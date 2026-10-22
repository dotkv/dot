# Dot

A small, log-backed key-value store embedded in Go applications. Dot supports
typed values, atomic updates, expiration, background compaction, and an embedded
metrics dashboard. Database paths have exclusive single-process access;
goroutines share a database handle.

[Documentation](https://dotkv.github.io/dot/) ·
[Usage](https://dotkv.github.io/dot/usage.html) ·
[Storage and recovery](https://dotkv.github.io/dot/storage.html) ·
[Metrics](https://dotkv.github.io/dot/metrics.html) · [Benchmarks](benchmarks/README.md)

## Get started

Use the Go toolchain required by [go.mod](go.mod), currently Go 1.27.2.
Run the demo from a checkout:

```sh
git clone https://github.com/dotkv/dot.git
cd dot
go run -buildvcs=true ./fiddle
```

Open <http://localhost:8080/dot/metrics/> for a dashboard and repeating workload
against a temporary database. See [fiddle](fiddle/README.md) for details.

Install Dot in an application with `go get github.com/dotkv/dot` and import
`github.com/dotkv/dot`. See the
[getting started guide](https://dotkv.github.io/dot/#open-a-database) for details.

## Store and read

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
`uint64`, `float64`, and `bool`. Reads require the stored type; use `errors.Is`
with `dot.ErrNotFound` or `dot.ErrTypeMismatch` to handle misses and mismatches.

## Update atomically

```go
err := db.Update("counter", func(current uint64, exists bool) (uint64, error) {
	return current + 1, nil
})
```

A missing or expired key starts at zero with `exists == false`. Returning an
error aborts the write. The callback runs under the shard lock; keep it short
and do not call back into the same database.

## Expire a value and sync the write

```go
err := db.Set("session", "active",
	dot.WithExpiration(time.Now().Add(15*time.Minute)),
	dot.WithSync(true),
)
```

Expiration is an absolute deadline. Writes use the OS page cache by default;
`WithSync(true)` syncs the log before returning. Options can also be passed to
`dot.Open` to set database defaults.

See [development](https://dotkv.github.io/dot/development.html) for checks, benchmarks, local docs
preview, and GitHub Pages setup. The site lives in `docs/` as plain HTML, CSS,
and JavaScript.

## Releases

Tag the release commit and push the tag:

```sh
git tag v0.1.0
git push origin v0.1.0
```

Go resolves module versions directly from pushed semantic version tags. The
release workflow runs the Go tests, benchmark adapter tests, and dashboard tests,
then creates a GitHub Release with generated notes. Tags such as `v0.1.0-rc.1`
are marked as prereleases.

The metrics dashboard shows the compiled Dot dependency's version automatically,
without build flags. Untagged commits show `dev · <short hash>`; local builds with
uncommitted changes append `· dirty`. Local replacements or missing metadata
show `dev`. Use `go run -buildvcs=true ./fiddle` to include VCS metadata in the demo.
