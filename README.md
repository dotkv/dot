# Dot

An embedded, on-disk key-value database engine for Go applications. Dot supports
typed values, atomic updates, expiration, background compaction, and an embedded
metrics dashboard. Database paths have exclusive single-process access;
goroutines share a database handle.

[Documentation](https://dotkv.github.io/dot/) ·
[Usage](https://dotkv.github.io/dot/usage.html) ·
[Storage](https://dotkv.github.io/dot/storage.html) ·
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
`uint64`, `float32`, `float64`, and `time.Duration`. Reads require the stored type.
Check missing or expired keys with `err == dot.ErrNotFound`; use
`errors.Is(err, dot.ErrTypeMismatch)` for type mismatches.

Fixed-width numbers use their natural byte widths. `int` and `uint` always use
eight bytes on disk and retain distinct types from `int64` and `uint64`. Reading
an `int` or `uint` that cannot fit on a 32-bit host returns an error wrapping
`dot.ErrValueOverflow`.

`time.Duration` stores signed nanoseconds in eight little-endian bytes and retains
its own type. Store with `db.Set("timeout", 5*time.Second)` and read with
`db.Get[time.Duration]("timeout")`.

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

`WithDurability` sets the default for `Open` or overrides it for a single `Set`,
`Update`, or `Delete`:

| Mode | Before returning |
| --- | --- |
| `dot.Buffer` (default) | Copies the record into a 64 KiB buffer per active shard. |
| `dot.Write` | Writes the record to the file, using the OS page cache. |
| `dot.Sync` | Writes and fsyncs the shard log. |

Buffered records flush without fsync when the buffer fills, within 100 ms of the
first pending write, on `Close`, or when a `Write` or `Sync` call runs on the
same shard. All modes provide immediate read-after-write visibility across
`Get`, `Update`, and `Scan`. A process crash can lose buffered writes, while
`dot.Sync` ensures writes reach storage before returning. See
[Storage & recovery](https://dotkv.github.io/dot/storage.html) for details.

See [example/](example/) for a runnable workload and embedded metrics dashboard.
By default, `db.MetricsHandler()` collects only while viewers are connected. Pass
`dot.WithMetricsHistory(7*24*time.Hour)` to retain seven days of one-second
snapshots across restarts (about 21.1 MiB per day).
