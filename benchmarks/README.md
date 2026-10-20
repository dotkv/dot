# Engine comparisons

Compare Dot and Pebble on Linux. This directory is a separate Go module: its
engine and histogram dependencies do not enter Dot's root `go.mod` or `go.sum`.
Run its commands from this directory. Root `go test ./...` does not run this suite.

```sh
cd benchmarks
go test ./...
go build -o dot-bench .

# Fewer timed operations; each Open fixture still fills two 64 MiB segments.
./dot-bench -ops 30000 -warmup 3000

# Default matrix: 12 fresh-process cases, producing six comparison tables.
./dot-bench -readme README.md

# Repeat runs and retain all measurements, including operation breakdowns.
./dot-bench -repeat 5 -readme README.md -json > results.jsonl

# Regenerate the README from saved results without rerunning benchmarks.
./dot-bench -from-json results.jsonl -readme README.md

# Exercise byte-slice APIs for the same eight stored bytes.
./dot-bench -sizes 8 -scalar=false

# Compare operation performance and Open over at least two full log segments.
./dot-bench -workloads write-heavy -repeat 3

# Increase the retained history to at least four full segments in one Dot shard.
./dot-bench -workloads write-heavy -open-segments 4
```

By default temporary databases live in the current directory, on the same
filesystem as this checkout. Use `-dir /path/on/test/ssd` to select another
existing directory. Each case is removed after reporting, including failed cases.
Avoid a tmpfs data directory for on-disk comparisons. The default 1 KiB cases
preload about 16 MiB of values. Cases run sequentially. Additional sizes and
worker counts are opt-in through `-sizes` and `-workers`. For example, 64 KiB
values preload about 1 GiB and can take much longer to benchmark.

Open preparation adds at least 128 MiB of update history per case by default,
after the operation measurements. It fills two 64 MiB segments in one Dot shard
and starts a nonempty third segment. Both engines receive the same updates.
This is substantial untimed work even when `-ops` is small: eight-byte values
require about three million additional updates. Use `-open-segments N` (at least
2) to increase the history. Cases release their files before the next case starts.

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

Completed cases: **36**, covering **6 configurations**.

Configuration: **16,384 initial keys**, **300,000 measured operations**, **30,000 warmup operations**, seed **1**.

Environment: go1.27.2, linux/amd64, 16 logical CPUs, GOMAXPROCS=16.

CPU: AMD Ryzen 7 7840HS w/ Radeon 780M Graphics.

Pebble: `v2.1.7`. Eight-byte scalar cases use `uint64` in Dot and equivalent encoding/decoding in Pebble.

Columns summarize all operation types. Each cell is the median across the indicated runs; p99 is the median of run p99s, not a pooled percentile. Peak RSS includes preparation. Disk usage is measured after the final drain. These are closed-loop saturation results.

### read heavy · 8 B uint64 values · 16 workers

| Implementation | Runs | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Dot | 3 | 2.407 | 33.535 | 6,167,402 | 57.5 | 1.50 |
| Pebble | 3 | 3.065 | 26.239 | 4,795,192 | 38.6 | 18.54 |

### read heavy · 1,024 B values · 16 workers

| Implementation | Runs | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Dot | 3 | 5.738 | 78.847 | 2,652,177 | 70.8 | 28.11 |
| Pebble | 3 | 50.195 | 284.671 | 315,296 | 50.8 | 36.56 |

### mixed · 8 B uint64 values · 16 workers

| Implementation | Runs | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Dot | 3 | 5.764 | 47.615 | 2,667,896 | 60.2 | 7.49 |
| Pebble | 3 | 5.921 | 63.999 | 2,576,548 | 45.3 | 19.01 |

### mixed · 1,024 B values · 16 workers

| Implementation | Runs | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Dot | 3 | 10.831 | 92.159 | 1,431,149 | 65.7 | 129.95 |
| Pebble | 3 | 79.358 | 471.039 | 195,931 | 51.1 | 38.96 |

### write heavy · 8 B uint64 values · 16 workers

| Implementation | Runs | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Dot | 3 | 9.345 | 64.767 | 1,674,509 | 60.2 | 13.48 |
| Pebble | 3 | 9.705 | 71.679 | 1,605,759 | 44.3 | 18.98 |

### write heavy · 1,024 B values · 16 workers

| Implementation | Runs | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Dot | 3 | 17.551 | 110.591 | 891,505 | 65.1 | 231.73 |
| Pebble | 3 | 150.051 | 137.215 | 104,478 | 50.9 | 39.94 |


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
| Open history | At least two full 64 MiB Dot segments plus an active head |
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
The measured operation workloads do not simulate hot-key contention or skewed access.

Eight-byte values use Dot's `Set[uint64]` and `Get[uint64]` by default. Pebble stores
the same little-endian bytes and encodes/decodes the integer inside the timed
adapter call. Set `-scalar=false` to compare byte-slice APIs instead. Larger
values use byte slices. Their payloads are generated before measurement, with
pseudorandom bodies and a per-key revision in the first eight bytes. Every read
checks the returned size and revision outside latency measurement. Byte-slice
reads return caller-owned bytes in both engines; Pebble's copy and release are
included in latency. Scalar reads avoid that copy in both adapters.

## Measurement

Every case also measures one clean reopen with a large retained history, reported as
`OPEN ms` in the terminal, `Open (ms)` in generated tables, and `open_seconds` in
JSON. After capturing the operation and space metrics, the harness adds enough
changed-value updates to fill at least two 64 MiB segments in one Dot shard, plus
a nonempty active head. On-disk file sizes verify that sealed segments are full;
merely creating several partial files does not qualify. Keys cycle over a subset
of the existing live keys, keeping the live population fixed while retaining
overwritten history. Pebble receives the identical key/value update sequence.

The timer covers only the engine's open call, including Dot's log replay and
index rebuild. History creation, flush/close, correctness checks of every
surviving key, and the subsequent close are excluded. Each repetition starts fresh
and supplies one open sample; tables use their median.
Older saved results without this measurement display an em dash.

`HISTORY MiB` / `Added history (MiB)` reports the extra record bytes using Dot's
21-byte header, 16-byte key, and configured value size. It is write history, not
Pebble's physical disk size. JSON also records `open_history_records`,
`open_preparation_seconds`, and the verified `open_dot_full_segments` for Dot.
Saved runs with different `open_full_segments` settings cannot be combined.

This is a **warm-cache clean reopen**, with filesystem caches left intact. Pebble
has flushed its memtables to SSTables before closing, so this does not compare
crash recovery or recovery of an unflushed Pebble WAL. Dot still replays its full
log history, including overwritten and deleted records, without hint files or
checkpoints. Operation RSS and disk measurements precede the additional recovery
history; operation latency and throughput exclude all Open preparation and timing.

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
