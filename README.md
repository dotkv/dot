# Dot

A small log-backed key-value embedded store in Go.

## Recovery

`Open` replays each shard's log segments in order to rebuild its index, applying
sets, deletes, and expirations. It resumes appending at the end of the newest
segment. An incomplete final record in that segment is truncated; checksum
failures, invalid records, or incomplete records in older segments fail opening.
Recovery currently scans all log records; checkpoints are not implemented yet.

Failed writes that made partial progress are truncated back to the last complete
record, and the repair is synced. If rollback fails, further appends fail until
the database is reopened. Successful writes are not synced individually.

## Atomic updates

`Update` reads an existing value and writes the callback's result atomically:

```go
err := db.Update([]byte("counter"), func(v uint64) uint64 { return v + 1 })
err = db.Update([]byte("message"), func(v []byte) []byte {
	return append(v, '!')
})
```

It supports the same types as `Get` and `Set`, preserves expiration, and returns
`ErrNotFound` for missing or expired keys without calling the callback. An invalid
scalar encoding also fails before the callback runs. Unchanged encodings skip the
log append; returning nil bytes stores an empty value.

The callback runs once under an exclusive shard lock. Keep it short and do not
call methods on the same database from it. Byte slices passed to the callback
are owned by the caller and may be mutated or retained; returned bytes are copied
into the log. A panic leaves the stored value unchanged and releases the lock.
A failed write cannot undo external effects performed by the callback.

## Benchmarks

The [engine comparison suite](benchmarks/README.md) compares Dot with Pebble across
read-heavy, mixed, and write-heavy workloads, using 8-byte scalars and 1 KiB
values with 16 workers by default. It reports latency, throughput, RSS, and disk
usage. Its dependencies live in a separate module.

Root benchmarks include serial, unchanged, and parallel `uint64` updates:

```sh
go test -run '^$' -bench '^BenchmarkUpdateUint64' -benchmem
```

`BenchmarkOpen` starts with 4,096 keys and 128-byte values, then repeatedly updates
keys in one shard until it has two full 64 MiB segments and a nonempty active
head. It verifies the file sizes and reports full segments per open and replay
throughput. This measures full replay of over 128 MiB of retained history with
warm filesystem caches. Setup, validation, and close are untimed:

```sh
go test -run '^$' -bench '^BenchmarkOpen$' -benchmem
```
