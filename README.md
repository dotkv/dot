# Dot

A small log-backed key-value embedded store in Go.

## Typed values

`Set`, `Get`, and `Update` support `[]byte`, `int64`, `uint64`, `float64`, and
`bool`. Each record stores its value type. `Set` may replace an existing key
with a different type; `Get[T]` and `Update[T]` require the stored type and return
an error wrapping `ErrTypeMismatch` otherwise. Use `errors.Is` to check it.
There are no implicit numeric conversions, and `Get[[]byte]` only reads byte
values. Missing or expired keys return `ErrNotFound` before checking the type.
A mismatched update leaves the value unchanged without invoking its callback.

```go
err := db.Set([]byte("counter"), uint64(42))
value, err := db.Get[uint64]([]byte("counter"))
_, err = db.Get[int64]([]byte("counter")) // errors.Is(err, dot.ErrTypeMismatch)
```

The 21-byte record header is `[crc32 4][type 1][keylen 4][vallen 4][expiration 8]`,
followed by the key and value. Header integers are big-endian. The checksum
covers the type, lengths, expiration, key, and value. Type tags are fixed:

| Tag | Type | Value payload |
| --- | --- | --- |
| `0x01` | Delete | Empty; expiration must be zero |
| `0x02` | `[]byte` | Any length, including empty |
| `0x03` | `int64` | 8 little-endian bytes |
| `0x04` | `uint64` | 8 little-endian bytes |
| `0x05` | `float64` | 8 little-endian bytes containing its IEEE 754 bits |
| `0x06` | `bool(false)` | Empty |
| `0x07` | `bool(true)` | Empty |

Both boolean tags have the logical type `bool`. Empty bytes, false, true, and
deletes remain distinct records. Invalid tags or payload lengths return an error
wrapping `ErrInvalidValue`. Compaction and recovery preserve value types and
exact floating-point bits. Unchanged writes compare type, payload, and expiration.

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

Successful mutations queue background compaction when more than 33% of a shard's
encoded log bytes are obsolete and at least 1 MiB is obsolete. A single worker
serves a bounded queue with at most one queued request per shard. Checks use
incremental byte counters, without scanning segments or adding reads to the write
path. Compaction holds one shard's exclusive lock for its pass; other shards can
continue serving requests.

Pass `WithCompactionThreshold(ratio, minObsoleteBytes)` to `Open` to configure the
state trigger. For example, `WithCompactionThreshold(0.4, 1<<20)` requires more
than 40% obsolete bytes and at least 1 MiB of obsolete data in that shard. The
second argument is **bytes**, replacing the previous record-count unit. The ratio
must be finite and in `[0, 1)`; a zero minimum is allowed. The option has no effect
when passed to `Set`.

Both thresholds count complete encoded records: the 21-byte header, key, and
value. Obsolete bytes are total retained record bytes minus bytes referenced by
the index. Deletes count toward retained history until compaction removes them.
Expired records become obsolete when pruned from the index. Counters are rebuilt
from existing logs during recovery; the log format and eight-byte index slots do
not change.

A larger minimum trades more retained history for fewer compaction passes and
sync operations. The minimum applies per shard: 1 MiB across 64 shards amounts
to roughly 64 MiB of possible obsolete history before considering the ratio,
queued work, and live data. This is a scheduling threshold, not a disk-space cap.

Every ten seconds, the worker also checks shards with tracked expirations or
pending cleanup. It prunes expired index entries, then applies the configured
obsolete-byte ratio without the state trigger's minimum byte count. Other shards
are skipped. Failed passes wait for this ticker to retry, including when pruning
removed the last expiration entry. Recovery queues eligible history, already
expired records, and unfinished packing without waiting for another write.
Pending passes complete even below the configured thresholds.

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
`ErrNotFound` for missing or expired keys without calling the callback. A type
mismatch or malformed encoding also fails before the callback runs. Unchanged
encodings skip the log append; returning nil bytes stores an empty value.

The callback runs once under an exclusive shard lock. Keep it short and do not
call methods on the same database from it. Byte slices passed to the callback
are owned by the caller and may be mutated or retained; returned bytes are copied
into the log. A panic leaves the stored value unchanged and releases the lock.
A failed write cannot undo external effects performed by the callback.

## Benchmarks

The [engine comparison suite](benchmarks/README.md) compares Dot, Pebble, and an
in-memory Go map using 16-byte keys and 16 workers by default. It covers 80/20 and
20/80 read/write ratios with 8-byte scalars and 1 KiB values. Dot and Pebble use
normal automatic compaction. A short Dot probe estimates fixed operation budgets
targeting about 15 seconds per Dot case. Every engine runs the same operation
count. The suite reports latency, throughput, RSS, and disk usage, and always
writes results to its README after a successful run. Dependencies live in a
separate module.

Root benchmarks include serial, unchanged, and parallel `uint64` updates.
Their primitive fixtures stop background maintenance to isolate foreground work
and keep direct file/index resets deterministic.

```sh
go test -run '^$' -bench '^BenchmarkUpdateUint64' -benchmem
```
