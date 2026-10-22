# Dot

A small, on-disk key-value store embedded in Go applications. Dot supports
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

Keys accept `string` or `[]byte`. Values support `[]byte`, `string`, `int64`,
`uint64`, `float64`, and `bool`. Reads require the stored type. Check missing or
expired keys with `err == dot.ErrNotFound`; use `errors.Is(err, dot.ErrTypeMismatch)`
for type mismatches.

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
	dot.WithSync(true),
)
```

The key becomes unreadable at the given time. Compute `time.Now().Add(ttl)` on
each write for a fresh TTL; a deadline passed to `dot.Open` is shared by all
writes that use that default. Writes use the OS page cache by default;
`WithSync(true)` syncs the log before returning.

See [example/](example/) for a runnable workload and metrics dashboard.
