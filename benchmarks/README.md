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

Measured operations per engine: **11,503,872**. Ratios are reads/writes.

| Implementation     | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| ------------------ | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot                |    1 |   3.667 |     4.948 |   38.655 |          3,136,944 |           62.4 |      30.05 |
| Go map (in memory) |    1 |   1.620 |     2.158 |    1.535 |          7,102,359 |           34.6 |       0.00 |
| Pebble             |    1 |   5.630 |     7.563 |  127.487 |          2,043,388 |           45.4 |      18.16 |

### 80/20 · 1,024 B values · 16 workers

Measured operations per engine: **2,593,008**. Ratios are reads/writes.

| Implementation     | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| ------------------ | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot                |    1 |   5.151 |    31.554 |   87.039 |            503,437 |           67.9 |      56.43 |
| Go map (in memory) |    1 |   0.952 |     5.711 |    6.079 |          2,722,786 |           69.2 |       0.00 |
| Pebble             |    1 |   9.748 |    59.748 |  364.543 |            265,994 |           55.0 |      37.77 |

### 20/80 · 8 B uint64 values · 16 workers

Measured operations per engine: **5,031,600**. Ratios are reads/writes.

| Implementation     | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| ------------------ | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot                |    1 |   4.355 |    13.681 |   62.975 |          1,155,308 |           64.7 |      36.14 |
| Go map (in memory) |    1 |   0.881 |     2.673 |    1.655 |          5,712,498 |           34.1 |       0.00 |
| Pebble             |    1 |   2.645 |     8.237 |   67.583 |          1,902,628 |           46.7 |      18.17 |

### 20/80 · 1,024 B values · 16 workers

Measured operations per engine: **671,088**. Ratios are reads/writes.

| Implementation     | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| ------------------ | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot                |    1 |   4.552 |   106.082 |  147.455 |            147,424 |           65.4 |      54.85 |
| Go map (in memory) |    1 |   0.206 |     4.629 |    1.967 |          3,257,755 |           69.3 |       0.00 |
| Pebble             |    1 |   5.412 |   126.876 |  389.119 |            124,010 |           51.2 |      37.34 |


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
queue a shard when more than 33% of its encoded log bytes are obsolete and at
least 1 MiB is obsolete. Both thresholds include headers, keys, and values. A
single background worker coalesces requests per shard.
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

## Byte threshold comparison

The byte-based trigger was compared at ratio 0.33 with 16 workers, 16,384 initial
keys, 30,000 warmup operations, and the 20/80 workload. Every threshold ran the
same seeded trace: 4,878,192 measured operations for 8-byte scalars and 1,000,032
for 1 KiB values. Each row is one isolated run, with disk measured after drain.
These fixed budgets differ from the calibrated budgets in the main results.

| Minimum per shard | Values     | Throughput (ops/s) | Disk (MiB) |
| ----------------- | ---------- | -----------------: | ---------: |
| 256 KiB           | 8 B uint64 |            554,536 |      13.27 |
| 256 KiB           | 1 KiB      |             83,917 |      23.20 |
| 512 KiB           | 8 B uint64 |            907,975 |      27.84 |
| 512 KiB           | 1 KiB      |             86,483 |      32.84 |
| 1 MiB             | 8 B uint64 |          1,262,080 |      31.20 |
| 1 MiB             | 1 KiB      |            149,116 |      52.02 |

The default is **1 MiB per shard**: it gave the highest throughput of these three
settings. Smaller thresholds retained less history, with more maintenance cost.
Final disk usage depends on where a run ends in the compaction cycle; the minimum
is a scheduling threshold, not a hard limit. Compaction still holds a shard lock
while syncing files, so changing units does not remove that latency tradeoff.

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
