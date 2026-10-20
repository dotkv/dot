# Engine comparisons

Compare Dot and Pebble on Linux. This directory is a separate Go module: its
engine and histogram dependencies do not enter Dot's root `go.mod` or `go.sum`.
Run its commands from this directory. Root `go test ./...` does not run this suite.

```sh
cd benchmarks
go test ./...
go build -o dot-bench .

# Quick check: the same 12 cases with 90% fewer operations.
./dot-bench -ops 30000 -warmup 3000

# Default matrix: 12 fresh-process cases, producing six comparison tables.
./dot-bench -readme README.md

# Repeat runs and retain all measurements, including operation breakdowns.
./dot-bench -repeat 5 -readme README.md -json > results.jsonl

# Regenerate the README from saved results without rerunning benchmarks.
./dot-bench -from-json results.jsonl -readme README.md

# Exercise byte-slice APIs for the same eight stored bytes.
./dot-bench -sizes 8 -scalar=false
```

By default temporary databases live in the current directory, on the same
filesystem as this checkout. Use `-dir /path/on/test/ssd` to select another
existing directory. Each case is removed after reporting, including failed cases.
Avoid a tmpfs data directory for on-disk comparisons. The default 1 KiB cases
preload about 16 MiB of values. Cases run sequentially. Additional sizes and
worker counts are opt-in through `-sizes` and `-workers`. For example, 64 KiB
values preload about 1 GiB and can take much longer to benchmark.

The default matrix is 2 implementations × 3 workloads × 2 value sizes × 1 worker
count = **12 runs**, six times fewer than the original 72-case matrix. Each
repetition multiplies that count: `-repeat 3` runs 36 cases. Removing 64 KiB
values also avoids the slowest cases in the original sweep.

Use `-readme README.md` to update the result tables after a successful run. Each
workload/value-size/worker-count combination has its own table, with implementations
as rows and measurements as columns. Repetitions are summarized using the median
of each measurement, including the median of run p99s. Other README content is
preserved; a failed run leaves the existing tables intact.
Saved results can also include completed cases from an interrupted run; gaps in
the workload/value-size/worker-count matrix are marked in the generated section.

## Results

<!-- benchmark-results:start -->

Completed cases: **12**, covering **6 configurations**.

Configuration: **16,384 initial keys**, **300,000 measured operations**, **30,000 warmup operations**, seed **1**.

Environment: go1.27.2, linux/amd64, 16 logical CPUs, GOMAXPROCS=16.

CPU: AMD Ryzen 7 7840HS w/ Radeon 780M Graphics.

Pebble: `v2.1.7`. Eight-byte scalar cases use `uint64` in Dot and equivalent encoding/decoding in Pebble.

Columns summarize all operation types. Each cell is the median across the indicated runs; p99 is the median of run p99s, not a pooled percentile. Peak RSS includes preparation. Disk usage is measured after the final drain. These are closed-loop saturation results.

### read heavy · 8 B uint64 values · 16 workers

| Implementation | Runs | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Dot | 1 | 2.437 | 34.047 | 6,171,877 | 61.7 | 1.50 |
| Pebble | 1 | 3.063 | 28.287 | 4,795,546 | 36.6 | 18.54 |

### read heavy · 1,024 B values · 16 workers

| Implementation | Runs | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Dot | 1 | 6.280 | 86.015 | 2,398,767 | 70.9 | 28.11 |
| Pebble | 1 | 52.296 | 301.055 | 302,804 | 50.8 | 36.56 |

### mixed · 8 B uint64 values · 16 workers

| Implementation | Runs | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Dot | 1 | 5.538 | 45.567 | 2,793,480 | 62.2 | 7.49 |
| Pebble | 1 | 5.898 | 63.231 | 2,583,255 | 45.3 | 19.01 |

### mixed · 1,024 B values · 16 workers

| Implementation | Runs | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Dot | 1 | 10.420 | 96.767 | 1,486,959 | 65.4 | 129.95 |
| Pebble | 1 | 79.203 | 481.279 | 197,288 | 51.4 | 38.99 |

### write heavy · 8 B uint64 values · 16 workers

| Implementation | Runs | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Dot | 1 | 9.307 | 62.463 | 1,677,652 | 60.3 | 13.48 |
| Pebble | 1 | 9.624 | 72.703 | 1,622,606 | 44.4 | 18.99 |

### write heavy · 1,024 B values · 16 workers

| Implementation | Runs | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Dot | 1 | 17.861 | 102.399 | 879,443 | 63.5 | 231.73 |
| Pebble | 1 | 149.889 | 155.647 | 104,691 | 55.8 | 39.73 |


<!-- benchmark-results:end -->

## Workloads

| Setting | Default |
| --- | --- |
| Engines | `dot,pebble` |
| Value sizes | `8,1024` bytes |
| Workers | `16` |
| Initial live keys | 16,384 total, divided across workers |
| Measured operations | 300,000 total per case |
| Warmup operations | 30,000 total per case |
| Read heavy | 95% reads / 5% mutations |
| Mixed | 50% reads / 50% mutations |
| Write heavy | 5% reads / 95% mutations |
| Read outcomes | 95% hits / 5% guaranteed misses |
| Mutations | Equal inserts, updates, deletes |

Use `-help` for flags. The read percentages are seeded random draws; JSON contains
the actual operation counts. Each worker cycles insert/update/delete, keeping
their counts within one of each other over any measurement interval. Inserts
use fresh keys, updates change existing values, and deletes remove existing keys.
Live population remains between the initial count and that count plus workers.

Keys are 16 bytes with permuted IDs, so insertion order is unrelated to key order.
Each worker owns a disjoint partition of live keys and selects uniformly within
it. Misses use a separate, never-inserted key space. This avoids scheduling races
in the expected outcomes while exercising the same shared engine and locks.
This first suite does not simulate hot-key contention or skewed access.

Eight-byte values use Dot's `Set[uint64]` and `Get[uint64]` by default. Pebble stores
the same little-endian bytes and encodes/decodes the integer inside the timed
adapter call. Set `-scalar=false` to compare byte-slice APIs instead. Larger
values use byte slices. Their payloads are generated before measurement, with
pseudorandom bodies and a per-key revision in the first eight bytes. Every read
checks the returned size and revision outside latency measurement. Byte-slice
reads return caller-owned bytes in both engines; Pebble's copy and release are
included in latency. Scalar reads avoid that copy in both adapters.

## Measurement

Each case starts a fresh process and database. Preload and a workload warmup are
untimed. Pebble's memtables are flushed after preload and warmup, so the read
comparison exercises SSTables as well as caches. Measurements use fixed operation
counts; engine order rotates across cases and repetitions. The seed and per-worker
operation budgets are identical for corresponding Dot and Pebble cases.

This is a **closed-loop saturation benchmark**: workers issue the next operation
when the previous one finishes. Throughput is completed operations divided by
wall time, including workload generation, validation, and histogram recording.
Latency times the adapter call, including its internal lock waits and required
conversion/copying. It excludes trace generation, validation, and time between
requests. Mean is the arithmetic mean of observed operation latencies, not wall
time divided by operation count. p99 comes from merged per-worker HDR histograms
with two significant figures (about 1% quantization). No samples are dropped;
operations exceeding the histogram's one-hour bound fail the case.

JSON reports aggregate and separate hit/miss/insert/update/delete latency and
counts. The terminal table shows the aggregate. These p99 values describe the
achieved saturation load, not response time at a matched external request rate.
Increase keys and operations for longer runs, datasets exceeding available cache,
and runs that encounter log rotation and repeated LSM maintenance. Use repetitions
and compare their spread; smoke-run numbers are not stable performance estimates.

**Memory is RSS, not Go heap.** JSON includes RSS after harness allocation, after
preparation, and immediately after measurement. Peak is the process high-water
mark through measurement, including preparation. The terminal and README tables
show peak RSS. This captures Dot's anonymous mmap index and the engine's resident
process memory. It also includes runtime, worker state, and histogram overhead;
the harness baseline provides context rather than a precise subtractable engine
footprint. RSS excludes filesystem cache that is not mapped into the process.
No claim is made that it represents total system memory consumed by storage I/O.

**Disk usage** includes apparent regular-file bytes and allocated bytes
(`stat.st_blocks * 512`), before measurement and after a final flush and close.
JSON also includes space amplification: final allocated bytes / live key-and-value
bytes, including the 16-byte keys. Comparisons use the same initial dataset, warmup count, and
measured operation count. The final untimed drain is reported separately as
`drain_seconds`; it materializes pending writes for disk accounting, without
forcing a full database compaction. Normal Pebble maintenance stays enabled.
The RSS figure is captured before this drain. Final disk figures include any
file preallocation and engine metadata; directory metadata is excluded.

## Engine configuration and interpretation

Pebble is pinned in this module, with WAL and automatic compaction enabled,
compression disabled, and other defaults retained. Mutations use `pebble.NoSync`.
Dot uses its current public API, including syncing at log rotation. This compares
writes without a per-operation fsync guarantee. Acknowledgment semantics still
differ: Pebble may buffer an acknowledged WAL write in userspace; Dot issues a
file write before returning. See [Pebble's WriteOptions documentation](https://pkg.go.dev/github.com/cockroachdb/pebble/v2#WriteOptions).

Results compare the engines as implemented. They do not attribute every difference
to sortedness. Record the CPU, storage device, filesystem, available RAM, and
power settings when sharing results. JSON includes Go version, OS, architecture,
CPU count, GOMAXPROCS, dependency versions, configuration, and available build VCS
metadata. Results from a modified tree should identify that revision separately.

To add a contender, implement `engine` and register an opener in `engines.go`.
Keep it in this module. A future Go map adapter can use the same workload and
result format, with synchronization and its in-memory-only nature documented.
