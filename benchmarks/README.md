# Engine comparisons

Compare Dot, Pebble, and an in-memory Go map on Linux using 16-byte keys.
This directory is a separate Go module: its engine and histogram dependencies do
not enter Dot's root `go.mod` or `go.sum`. Root `go test ./...` does not run this
suite.

Run the command from this directory. Every successful run updates the generated
result section of `README.md`, including when emitting JSON.

```sh
cd benchmarks
go test ./...
go build -o dot-bench .

# Default matrix: all three engines, both read/write ratios, both value sizes.
./dot-bench

# Repeat with one shared operation budget per configuration and save JSON.
./dot-bench -repeat 3 -json > results.jsonl

# Regenerate README tables from saved results without rerunning benchmarks.
./dot-bench -from-json results.jsonl

# Fixed-operation smoke run; bypass Dot calibration.
./dot-bench -ops 30000 -warmup 3000

# Override the Dot target, or select part of the matrix.
./dot-bench -target 10s -workloads 80/20 -sizes 1024 -engines dot,map

# Compare byte-slice APIs rather than scalar APIs for 8-byte values.
./dot-bench -sizes 8 -scalar=false
```

## Matrix and operation budgets

Ratios are **reads/writes**. The only workloads are **80/20** and **20/80**.
Writes are equal shares of inserts, updates, and deletes.

| Setting                   | Default                          |
| ------------------------- | -------------------------------- |
| Engines                   | `dot,pebble,map`                 |
| Read/write ratios         | `80/20,20/80`                    |
| Value sizes               | 8 B scalars, 1,024 B byte slices |
| Workers                   | 16                               |
| Initial live keys         | 16,384                           |
| Untimed warmup operations | 30,000                           |
| Seed                      | 1                                |
| Dot time target           | 3 seconds                        |

This gives **12 cases** across four configurations before repetitions. Use
`-engines`, `-workloads`, `-sizes`, and `-workers` to select a subset or override
sizes and concurrency. Supported worker counts are 1, 16, and 128. Keys are always
16 bytes.

The target applies **only to Dot's estimated performance**. Each distinct
workload/value-size/API/worker configuration first runs a fresh-process Dot probe
with 500 ms of measured work. Its observed throughput estimates a fixed operation
budget for the Dot target. Budgets are rounded up to groups of three operations
per worker. Every engine and repetition then runs that **same fixed count**, with
identical seeded per-worker traces. Reported cases never stop on a timer. Faster
engines may finish in fractions of a second; the target does not impose a minimum
runtime on them. Dot is used for calibration even if omitted from `-engines`.

Calibration is an estimate, not a runtime guarantee or evidence of steady state.
A short probe may not capture later maintenance costs. Preparation, calibration,
drain, and process overhead are outside the Dot target. `-target` overrides the
3-second target; `-ops` bypasses calibration and supplies one explicit budget for
all configurations. These flags are mutually exclusive. Budgets are reused across
repetitions within a command. Independent commands can choose different budgets;
the report refuses to combine different budgets for the same configuration. Use
`-repeat` to collect comparable repeats.

Each case starts a fresh process and database, and cases run sequentially.
Engine order rotates across configurations and repetitions. Temporary databases
live on the current filesystem by default; use `-dir /path/on/test/ssd` to select
another existing directory. Avoid tmpfs for disk comparisons. Each database is
removed after reporting, including failed cases. `-timeout` limits each child
process, including preparation and drain, to 10 minutes by default.

## Results

<!-- benchmark-results:start -->

Completed cases: **12**, covering **4 configurations**.

Configuration: **16,384 initial keys**, **16-byte keys**, **30,000 warmup operations**, seed **1**. Operation budgets are fixed per configuration and shared across engines and repetitions.

Dot and Pebble use normal automatic compaction; budgets estimated from a short Dot probe targeting **3s for Dot**. The Go map is in memory with no persistence.

Environment: go1.27.2, linux/amd64, 16 logical CPUs, GOMAXPROCS=16.

CPU: AMD Ryzen 7 7840HS w/ Radeon 780M Graphics.

Pebble: `v2.1.7`. Eight-byte scalar cases use `uint64` in Dot and equivalent little-endian encoding/decoding in the other adapters.

Columns summarize all operation types. Each cell is the median across the indicated runs; p99 is the median of run p99s, not a pooled percentile. Peak RSS includes preparation. Disk usage is measured after the final drain. These are closed-loop saturation results.

### 80/20 · 8 B uint64 values · 16 workers

Measured operations per engine: **11,991,600**. Ratios are reads/writes.

| Implementation     | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| ------------------ | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot                |    1 |   3.953 |     5.124 |   41.471 |          3,033,391 |           64.4 |      13.31 |
| Go map (in memory) |    1 |   1.762 |     2.252 |    1.695 |          6,803,910 |           34.7 |       0.00 |
| Pebble             |    1 |   6.140 |     7.947 |  133.119 |          1,953,018 |           45.2 |      18.16 |

### 80/20 · 1,024 B values · 16 workers

Measured operations per engine: **6,008,400**. Ratios are reads/writes.

| Implementation     | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| ------------------ | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot                |    1 |   2.957 |     7.672 |   84.479 |          2,031,842 |           69.0 |     846.04 |
| Go map (in memory) |    1 |   2.262 |     5.852 |    6.655 |          2,656,734 |           69.1 |       0.00 |
| Pebble             |    1 |  22.434 |    59.360 |  358.399 |            267,820 |           49.9 |      35.50 |

### 20/80 · 8 B uint64 values · 16 workers

Measured operations per engine: **4,878,192**. Ratios are reads/writes.

| Implementation     | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| ------------------ | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot                |    1 |   3.788 |    12.222 |   72.191 |          1,287,915 |           64.5 |      74.63 |
| Go map (in memory) |    1 |   0.905 |     2.852 |    1.735 |          5,391,484 |           33.9 |       0.00 |
| Pebble             |    1 |   2.563 |     8.234 |   68.095 |          1,903,109 |           45.5 |      18.16 |

### 20/80 · 1,024 B values · 16 workers

Measured operations per engine: **3,159,504**. Ratios are reads/writes.

| Implementation     | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| ------------------ | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot                |    1 |   4.044 |    20.258 |   90.111 |            781,360 |           66.6 |     327.21 |
| Go map (in memory) |    1 |   1.067 |     5.209 |    2.255 |          2,961,824 |           69.6 |       0.00 |
| Pebble             |    1 |  26.167 |   131.663 |  387.071 |            120,744 |           52.2 |      39.90 |


<!-- benchmark-results:end -->

## Workload semantics

Each worker owns a disjoint partition of live keys and selects uniformly within
it. Reads are 95% hits and 5% guaranteed misses in a never-inserted key space.
Workers cycle insert/update/delete, keeping mutation counts within one of each
other. Inserts use fresh keys, updates change existing values, and deletes remove
existing keys. Live population stays between the initial key count and that count
plus workers. Ratios use seeded random draws; JSON includes actual operation
counts. The workload does not simulate hot-key contention or skewed access.

Keys contain permuted IDs, so insertion order is unrelated to sort order.
Eight-byte values use Dot's `Set[uint64]` and `Get[uint64]` by default. Other
adapters store the same little-endian bytes and encode/decode inside the timed
adapter call. Set `-scalar=false` for byte-slice APIs. Larger values use byte
slices with pseudorandom payloads generated before measurement and a per-key
revision in the first eight bytes. Every read checks size and revision outside
latency timing. Byte-slice reads return caller-owned bytes in every engine;
necessary copying and release are included in latency. Dot, Pebble, and the map
decode scalars without copying a result slice.

## Engine and maintenance configuration

**Dot** uses its public API with normal automatic compaction. Successful mutations
queue a shard when more than 20% of its records are obsolete and at least 8,192
records are obsolete. A single background worker coalesces requests per shard.
A 10-second ticker also handles tracked expirations and pending retries. The
flush adapter is a no-op. Closing cancels its compactor after finishing any
current shard; the drain does not reclaim all outstanding garbage.

**Pebble** uses normal automatic compaction and default L0 limits, with WAL
enabled, compression disabled, and `pebble.NoSync` mutations. Other defaults,
including cache and memtable sizes, remain in effect. Memtables are flushed after
preload and warmup so reads exercise SSTables as well as caches. The final drain
flushes and closes without forcing a full compaction.

**Go map** is one ordinary `map[string][]byte` shared by all workers and protected
by one `sync.RWMutex`. Writes copy keys and values; byte-slice reads copy values
while holding a read lock. Scalar reads decode under that lock. Synchronization
and copying are included in timing. It has no persistence, files, flush, or drain
work; disk usage and disk space amplification are zero. This is an in-memory
baseline, with different durability and maintenance costs from the databases.

Preload and workload warmup are untimed for every engine. The final untimed flush
and close materializes pending writes for disk accounting. `drain_seconds` is
reported separately from foreground throughput.

Dot syncs at log rotation and issues a file write before returning. Pebble may
buffer an acknowledged WAL write in userspace. These configurations have no
per-operation fsync guarantee and differ in acknowledgment and recovery behavior.
See [Pebble's WriteOptions](https://pkg.go.dev/github.com/cockroachdb/pebble/v2#WriteOptions).

## Measurement and reports

This is a **closed-loop saturation benchmark**: each worker issues its next
operation after the previous one finishes. Throughput is completed operations
divided by wall time, including generation, validation, and histogram recording.
Latency measures the adapter call, including lock waits and necessary conversion
and copying, and excludes generation, validation, and time between requests.
Mean is the arithmetic mean of observed latencies. p99 comes from merged
per-worker HDR histograms with two significant figures (about 1% quantization).
No samples are dropped; operations exceeding the one-hour histogram bound fail
the case. These p99s describe the achieved saturation load, not latency at a
matched external request rate.

JSON includes the 16-byte key size, Dot target in nanoseconds, actual fixed
operation budget, elapsed time, aggregate and per-type latencies and counts,
memory/disk measurements, and environment metadata. Calibration probes are
excluded. Terminal output includes actual elapsed seconds.

The README's generated section is always replaced after all cases succeed;
handwritten documentation is preserved. Each workload/value/API/worker
configuration has its own table and recorded operation budget. Repetitions use
the median of each measurement, including the median of run p99s rather than a
pooled percentile. Saved results can include completed cases from interrupted
runs; missing configurations within the observed axes are marked. `-from-json`
regenerates the same section without running benchmarks.

**Memory is RSS, not Go heap.** JSON records RSS after harness allocation, after
preparation, and after measurement. Peak is the process high-water mark through
measurement, including preparation. It captures Dot's anonymous mmap index as
well as runtime and harness memory. The harness baseline is context, not a
precisely subtractable engine footprint. RSS excludes
filesystem cache that is not mapped into the process. Calibration runs in a
separate process and does not inflate the measured case's peak.

**Disk usage** records apparent regular-file bytes and allocated bytes
(`stat.st_blocks * 512`), before measurement and after the final drain. Space
amplification is final allocated bytes / live key-and-value bytes, including the
16-byte keys. Results include preallocation and engine metadata, excluding
directory metadata. Engines in one configuration perform the same work, but
maintenance policies affect the final retained history and allocated space.

Record the storage device, filesystem, available RAM, and power settings when
sharing results. JSON includes Go version, OS, architecture, CPU model/count,
GOMAXPROCS, dependency versions, and available build VCS metadata. Identify
modified-tree results separately. Results compare the engines as configured;
they do not attribute every difference to sortedness.

To add a contender, implement `engine` and register an opener in `engines.go`.
Keep dependencies in this module and document synchronization, persistence, and
maintenance behavior.
