# Dot

A small log-backed key-value embedded store in Go.

## Recovery

`Open` replays each shard's log segments in order to rebuild its index, applying
sets, deletes, and expirations. It resumes appending at the end of the newest
segment. An incomplete final record in that segment is truncated; checksum
failures, invalid records, or incomplete records in older segments fail opening.
Segment IDs wrap after `1048575`. Published filenames must form one contiguous
run around that ring, leaving at least one vacant ID. `Open` infers the newest
segment from this gap and rejects layouts with multiple gaps or no vacant ID.
Recovery scans all retained log records; checkpoints and a manifest are not used.

Failed writes that made partial progress are truncated back to the last complete
record, and the repair is synced. If rollback fails, further appends fail until
the database is reopened. Successful writes are not synced individually.

## Compaction

Every ten seconds, shards with more than 20% obsolete records are compacted.
Compaction holds one shard's exclusive lock for its pass; other shards can
continue serving requests. Expired index entries are pruned before counting.
Interrupted passes are retried even below that threshold. Recovery schedules a
pass for underfilled sealed segments or sealed segments with obsolete records.

Pass `WithCompaction(false)` to `Open` to disable background compaction for that
database handle. Compaction is enabled by default. Disabling it retains obsolete
history and defers expired-entry reclamation; expired keys still read as missing.
The option has no effect when passed to `Set`.

The pass first removes obsolete history from oldest to newest, preserving delete
and expiration semantics. It then packs current records toward the existing head
and removes empty files only from the oldest boundary, keeping one filename gap.
Values retain their encodings and expiration times.

Each rewritten segment is synced as a temporary `NNNNNNN.log.compact` file, then
atomically renamed over its published log. The directory is synced before
retiring any sources. An interrupted pass can leave identical records in source
and destination files; replay handles those normally. Unpublished temporary files
are discarded during recovery. Compaction needs temporary space for one output
segment and holds relocation information proportional to the shard's index size.

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

The [engine comparison suite](benchmarks/README.md) compares Dot with Pebble using
16-byte keys and 16 workers by default. Its fast profile disables automatic
compaction and covers five read/write ratios with 8-byte scalars and 1 KiB values.
Its slow profile enables normal compaction for a 50/50 workload with 128-byte
values. A short Dot probe estimates fixed operation budgets targeting about
3 seconds for fast Dot cases and 45 seconds for slow Dot cases. Every engine
runs the same operation count; other engines may take longer. It reports latency,
throughput, RSS, and disk usage. Dependencies live in a separate module.

Root benchmarks include serial, unchanged, and parallel `uint64` updates.
Their primitive fixtures stop background maintenance to isolate foreground work
and keep direct file/index resets deterministic.

```sh
go test -run '^$' -bench '^BenchmarkUpdateUint64' -benchmem
```
