# Dot

An embedded, on-disk key-value database engine for Go applications. Dot supports
typed values, atomic updates, expiration, background compaction, and an embedded
metrics dashboard. Database paths have exclusive single-process access;
goroutines share a database handle.

[Documentation](https://dotkv.github.io/dot/) ·
[Usage](https://dotkv.github.io/dot/usage.html) ·
[Metrics](https://dotkv.github.io/dot/metrics.html) · [Benchmarks](benchmarks/README.md)

## Get started

Add Dot to your application using the Go toolchain required by [go.mod](go.mod):

```sh
go get github.com/dotkv/dot
```

```go
package main

import (
	"fmt"
	"log"

	"github.com/dotkv/dot"
)

func main() {
	db, err := dot.Open("data")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Set("greeting", "hello"); err != nil {
		log.Fatal(err)
	}
	value, err := db.Get[string]("greeting")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(value) // hello
}
```

Keys accept `string` or `[]byte`. Values support `[]byte`, `string`, `bool`,
`int`, `int8`, `int16`, `int32`, `int64`, `uint`, `uint8`, `uint16`, `uint32`,
`uint64`, `float32`, and `float64`. Reads require the stored type. Check missing or
expired keys with `err == dot.ErrNotFound`; use `errors.Is(err, dot.ErrTypeMismatch)`
for type mismatches.

Fixed-width numbers use their natural byte widths. `int` and `uint` always use
eight bytes on disk and retain distinct types from `int64` and `uint64`. Reading
an `int` or `uint` that cannot fit on a 32-bit host returns an error wrapping
`dot.ErrValueOverflow`.

## Update atomically

```go
err := db.Update("counter", func(current uint64, exists bool) (uint64, error) {
	return current + 1, nil
})
```

A missing or expired key passes the type's zero value with `exists == false`:
`0` for numbers, `""` for strings, `false` for booleans, or `nil` for byte slices.
Returning an error aborts the write. Keep the callback short and do not call
methods on the same database from it.

## Expire a value and sync the write

```go
err := db.Set("session", "active",
	dot.WithExpiration(time.Now().Add(15*time.Minute)),
	dot.WithDurability(dot.Sync),
)
```

The key becomes unreadable at the given time. Compute `time.Now().Add(ttl)` on
each write for a fresh TTL; a deadline passed to `dot.Open` is shared by all
writes that use that default.

`WithDurability` selects one mode for `Open`, or overrides it for one `Set`,
`Update`, or `Delete`:

| Mode | Before returning |
| --- | --- |
| `dot.Write` | Writes the record to the file, using the OS page cache. |
| `dot.Sync` | Writes and fsyncs the shard log. |
| `dot.Buffer` (default) | Copies the record into a bounded 64 KiB buffer per active shard. |

These are alternative modes, not flags to combine with `|`. Buffered records are
written to the file without fsync when the buffer fills or within 100 ms of the
first pending write, subject to scheduling and storage delays. Later writes do
not reset the deadline. Records larger than the buffer write directly. Close,
log rotation, compaction, and a `Write` or `Sync` operation on the same shard
also drain pending bytes. A `Sync` operation then fsyncs that shard, even if the
requested value was unchanged. A background flush error blocks further mutations
in that shard and is returned by subsequent mutations and `Close`.

All modes preserve immediate read-after-write visibility, including `Update`
and scans: the existing index points at the newest record, and reads use pending
bytes when its address is still buffered. There is no separate key lookup in the
buffer. Returned values remain caller-owned. A process crash can lose buffered
writes; `Write` and flushed `Buffer` records still lack a power-loss guarantee.
`Sync` provides durability before acknowledgment. Scans retain their existing
per-shard consistency and are not database-wide snapshots.

See [example/](example/) for a runnable workload and metrics dashboard.
`db.MetricsHandler()` collects only while viewers are connected and keeps live
history in the browser until reload. Persistent history defaults to zero
(disabled). Opt in with `db.MetricsHandler(dot.WithMetricsHistory(7*24*time.Hour))`
to retain seven days, including without viewers and across database restarts
(about 21.1 MiB per retained day).
