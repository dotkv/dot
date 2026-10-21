# Engine comparisons

Compare Dot and Pebble on Linux using 16-byte keys. This directory is a separate
Go module: its engine and histogram dependencies do not enter Dot's root
`go.mod` or `go.sum`. Root `go test ./...` does not run this suite.

```sh
cd benchmarks
go test ./...
go build -o dot-bench .

# Default: fast profile, automatic compaction disabled in both engines.
./dot-bench

# Slow profile, normal automatic compaction enabled.
./dot-bench -profile slow

# Both profiles, repeated with one shared budget per configuration.
./dot-bench -profile all -repeat 3 -readme README.md -json > results.jsonl

# Regenerate tables without rerunning benchmarks.
./dot-bench -from-json results.jsonl -readme README.md

# Fixed-operation smoke run; bypass Dot calibration.
./dot-bench -profile all -ops 30000 -warmup 3000

# Override the Dot target, or select part of the matrix.
./dot-bench -target 5s -workloads 10/90,90/10 -sizes 1024

# Compare byte-slice APIs rather than scalar APIs for 8-byte values.
./dot-bench -sizes 8 -scalar=false
```

## Profiles and operation budgets

Ratios are **reads/writes**. Writes are equal shares of inserts, updates, and
deletes; they are not all inserts or all overwrites.

| Profile          | Read/write ratios                 | Value sizes  | Automatic compaction | Dot time target                       |
| ---------------- | --------------------------------- | ------------ | -------------------- | ------------------------------------- |
| `fast` (default) | 0/100, 10/90, 50/50, 90/10, 100/0 | 8 B, 1,024 B | Disabled             | 3 seconds (aiming for 2–5 seconds)    |
| `slow`           | 50/50                             | 128 B        | Enabled              | 45 seconds (aiming for 30–60 seconds) |

The target applies **only to Dot's estimated performance**. Each distinct
profile/workload/value-size/API/worker configuration first runs a fresh-process
Dot probe with 500 ms of measured work. Its observed throughput estimates a fixed
operation budget for the Dot target. Budgets are rounded up to complete groups of
three operations per worker. Every engine and repetition then runs that **same
fixed count**, with identical seeded per-worker traces. Reported cases never
stop on a timer. A future slower engine may take minutes for a case that takes
Dot a few seconds. Dot is used for calibration even if omitted from `-engines`.

Calibration is an estimate, not a runtime guarantee. In particular, a short slow
profile probe does not capture repeated compaction. Preparation, calibration,
drain, and process overhead are outside the Dot target. `-target` overrides the
profile targets; `-ops` bypasses calibration and supplies one explicit budget for
all configurations. These flags are mutually exclusive. Budgets are reused across
repetitions within a command, not recalibrated for each repetition. Independent
commands can choose different budgets; the report refuses to combine different
budgets for the same configuration. Use `-repeat` to collect comparable repeats.

Defaults are two engines, 16 workers, 16,384 initial live keys, 30,000 untimed
warmup operations, and seed 1. This gives **20 fast cases**, **2 slow cases**, or
**22 cases** for `-profile all`, before repetitions. Workloads, sizes, and worker
counts can be overridden; overrides apply to every selected profile. Supported
worker counts are 1, 16, and 128. The legacy workload names `read-heavy` (95/5),
`mixed` (50/50), and `write-heavy` (5/95) remain available for saved results and
opt-in runs. Keys are always 16 bytes; there is no key-size sweep.

Each case starts a fresh process and database, and cases run sequentially.
Engine order rotates across configurations and repetitions. Temporary databases
live on the current filesystem by default; use `-dir /path/on/test/ssd` to select
another existing directory. Avoid tmpfs for disk comparisons. Each database is
removed after reporting, including failed cases. `-timeout` limits each child
process, including preparation and drain, to 10 minutes by default. Increase it
for slower contenders or larger opt-in datasets; it does not change the workload.

## Results

<!-- benchmark-results:start -->

Completed cases: **22**, covering **11 configurations**.

Configuration: **16,384 initial keys**, **16-byte keys**, **30,000 warmup operations**, seed **1**. Operation budgets are fixed per configuration and shared across engines and repetitions.

Profile **fast**: automatic compaction **disabled**; budgets estimated from a short Dot probe targeting **3s for Dot**; Pebble L0 write-stop threshold **1,073,741,824**.

Profile **slow**: automatic compaction **enabled**; budgets estimated from a short Dot probe targeting **45s for Dot**.

Environment: go1.27.2, linux/amd64, 16 logical CPUs, GOMAXPROCS=16.

CPU: AMD Ryzen 7 7840HS w/ Radeon 780M Graphics.

Pebble: `v2.1.7`. Eight-byte scalar cases use `uint64` in Dot and equivalent encoding/decoding in Pebble.

Columns summarize all operation types. Each cell is the median across the indicated runs; p99 is the median of run p99s, not a pooled percentile. Peak RSS includes preparation. Disk usage is measured after the final drain. These are closed-loop saturation results.

### fast · 0/100 · 8 B uint64 values · 16 workers

Measured operations per engine: **4,002,192**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |   3.050 |    11.990 |   88.575 |          1,312,065 |           60.1 |     163.63 |
| Pebble         |    1 |   2.288 |     9.004 |   68.607 |          1,749,394 |           42.3 |      82.05 |

### fast · 0/100 · 1,024 B values · 16 workers

Measured operations per engine: **2,980,560**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |   3.110 |    16.430 |  125.439 |            958,459 |           62.3 |    2082.92 |
| Pebble         |    1 |  20.142 |   107.293 |  117.759 |            147,979 |           61.9 |    1868.31 |

### fast · 10/90 · 8 B uint64 values · 16 workers

Measured operations per engine: **4,809,024**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |   3.386 |    11.076 |   79.871 |          1,420,394 |           62.3 |     176.62 |
| Pebble         |    1 |   3.353 |    10.864 |   69.631 |          1,434,109 |           45.8 |      86.85 |

### fast · 10/90 · 1,024 B values · 16 workers

Measured operations per engine: **3,152,640**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |   3.274 |    16.355 |  143.359 |            962,800 |           65.7 |    1982.31 |
| Pebble         |    1 |  21.343 |   105.883 |  856.063 |            147,714 |           95.6 |    1734.22 |

### fast · 50/50 · 8 B uint64 values · 16 workers

Measured operations per engine: **6,475,584**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |   2.959 |     7.129 |   55.807 |          2,188,536 |           62.2 |     132.11 |
| Pebble         |    1 |  12.727 |    31.013 | 1028.095 |            508,805 |           47.2 |      69.70 |

### fast · 50/50 · 1,024 B values · 16 workers

Measured operations per engine: **4,239,024**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |   3.191 |    11.800 |  103.423 |          1,328,331 |           69.7 |    1481.93 |
| Pebble         |    1 |  81.232 |   303.374 | 9568.255 |             52,184 |           60.4 |    1334.12 |

### fast · 90/10 · 8 B uint64 values · 16 workers

Measured operations per engine: **14,859,840**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |   3.006 |     3.090 |   37.375 |          4,942,715 |           62.3 |      60.88 |
| Pebble         |    1 |  28.819 |    30.550 |  626.687 |            515,630 |           46.9 |      41.76 |

### fast · 90/10 · 1,024 B values · 16 workers

Measured operations per engine: **6,813,552**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |   3.100 |     7.060 |   89.087 |          2,197,720 |           77.8 |     486.00 |
| Pebble         |    1 | 117.209 |   271.173 | 3588.095 |             58,131 |           49.4 |     451.11 |

### fast · 100/0 · 8 B uint64 values · 16 workers

Measured operations per engine: **31,703,376**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |   2.945 |     1.314 |    8.383 |         10,765,050 |           59.4 |       0.78 |
| Pebble         |    1 |   5.648 |     2.680 |   20.351 |          5,613,036 |           38.0 |      13.76 |

### fast · 100/0 · 1,024 B values · 16 workers

Measured operations per engine: **10,720,944**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |   2.938 |     4.179 |   25.727 |          3,649,057 |          117.5 |      16.71 |
| Pebble         |    1 |  90.132 |   128.283 |  456.703 |            118,948 |           50.8 |      34.21 |

### slow · 50/50 · 128 B values · 16 workers

Measured operations per engine: **101,716,128**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |  53.209 |     8.190 |   59.391 |          1,911,622 |           67.4 |     278.26 |
| Pebble         |    1 |  79.305 |    12.250 |  152.575 |          1,282,589 |           49.7 |      20.46 |


<!-- benchmark-results:end -->

## Workload semantics

Each worker owns a disjoint partition of live keys and selects uniformly within
it. Reads are 95% hits and 5% guaranteed misses in a never-inserted key space.
Workers cycle insert/update/delete, keeping mutation counts within one of each
other. Inserts use fresh keys, updates change existing values, and deletes remove
existing keys. Live population stays between the initial key count and that
count plus workers. Pure-read cases perform no measured mutations, and pure-write
cases perform no measured reads. Ratios use seeded random draws; JSON includes the
actual operation counts. The workload does not simulate hot-key contention or
skewed access.

Keys contain permuted IDs, so insertion order is unrelated to sort order.
Eight-byte values use Dot's `Set[uint64]` and `Get[uint64]` by default. Pebble
stores the same little-endian bytes and encodes/decodes inside the timed adapter
call. Set `-scalar=false` for byte-slice APIs. Larger values use byte slices with
pseudorandom payloads generated before measurement and a per-key revision in the
first eight bytes. Every read checks size and revision outside latency timing.
Byte-slice reads return caller-owned bytes in both engines; Pebble's copy and
release are included in latency. Scalar reads avoid that copy in both adapters.

## Maintenance configuration

The **fast profile** opens Dot with `dot.WithCompaction(false)` and Pebble with
`DisableAutomaticCompactions: true`. WAL writes, log rotation, and memtable
flushes still occur. Pebble's `L0StopWritesThreshold` is raised to **1,073,741,824**
for these finite cases: otherwise accumulated L0 sublevels can stop writes while
the compaction needed to unblock them is disabled. Other default limits, including
memtable flush backpressure, remain in effect. Results describe performance with
cleanup deferred, including any read amplification and disk growth that creates.
They do not measure sustained performance or reclaimed disk space.

The **slow profile** uses normal Dot compaction: successful mutations queue a
shard when more than 20% of its records are obsolete and at least 8,192 records
are obsolete. A single background worker coalesces requests per shard. A
10-second ticker additionally handles tracked expirations and pending retries.
Pebble uses its default compaction settings and L0 limit. The 45-second Dot target
is an estimate; elapsed time alone does not establish maintenance coverage or
steady state. Results from the earlier ticker-only scheduler must be rerun to
measure the new policy.

Preload and workload warmup are untimed. Pebble's memtables are flushed after
each so reads exercise SSTables as well as caches. A final untimed flush and
close materializes pending writes for disk accounting, without forcing a full
compaction. Dot's flush adapter is a no-op and closing cancels its compactor after
finishing any current shard; the drain does not reclaim outstanding garbage.
`drain_seconds` is reported separately from foreground throughput.

## Measurement and reports

This is a **closed-loop saturation benchmark**: each worker issues its next
operation after the previous one finishes. Throughput is fixed completed
operations divided by wall time, including generation, validation, and histogram
recording. Latency measures the adapter call, including lock waits and necessary
conversion/copying, and excludes generation, validation, and time between requests.
Mean is the arithmetic mean of observed latencies. p99 comes from merged
per-worker HDR histograms with two significant figures (about 1% quantization).
No samples are dropped; operations exceeding the one-hour histogram bound fail
the case. These p99s describe the achieved saturation load, not latency at a
matched external request rate.

JSON includes the profile, compaction setting, 16-byte key size, Dot target in
nanoseconds, actual fixed operation budget, elapsed time, aggregate and per-type
latencies and counts, memory/disk measurements, and environment metadata.
Calibration probes are excluded from results. Terminal output includes actual
elapsed seconds so differences between engines remain visible.

`-readme README.md` updates only the generated result section after all cases
succeed. Each profile/workload/value/API/worker configuration has its own table
and recorded operation budget. Repetitions use the median of each measurement,
including the median of run p99s, rather than a pooled percentile. Saved results
can include completed cases from interrupted runs; missing combinations within
each profile are marked. Fast and slow matrices are kept separate.

**Memory is RSS, not Go heap.** JSON records RSS after harness allocation, after
preparation, and after measurement. Peak is the process high-water mark through
measurement, including preparation. It captures Dot's anonymous mmap index as
well as runtime and harness memory. The harness baseline is context, not a
precisely subtractable engine footprint. RSS excludes filesystem cache that is
not mapped into the process. Calibration runs in a separate process and does not
inflate the measured case's peak.

**Disk usage** records apparent regular-file bytes and allocated bytes
(`stat.st_blocks * 512`), before measurement and after the final drain. Space
amplification is final allocated bytes / live key-and-value bytes, including the
16-byte keys. Engines compared in one configuration perform the same work.
Results include preallocation and engine metadata, excluding directory metadata.
Compaction-disabled disk figures include retained obsolete history.

## Engine configuration and interpretation

Pebble is pinned in this module, with WAL enabled, compression disabled, and
`pebble.NoSync` mutations. Dot uses its public API, including syncing at log
rotation. Writes have no per-operation fsync guarantee. Acknowledgment semantics
still differ: Pebble may buffer an acknowledged WAL write in userspace; Dot
issues a file write before returning. See
[Pebble's WriteOptions documentation](https://pkg.go.dev/github.com/cockroachdb/pebble/v2#WriteOptions).

Record the storage device, filesystem, available RAM, and power settings when
sharing results. JSON includes Go version, OS, architecture, CPU model/count,
GOMAXPROCS, dependency versions, and available build VCS metadata. Identify
modified-tree results separately. Results compare the engines as configured;
they do not attribute every difference to sortedness.

To add a contender, implement `engine` and register a configuration-aware opener
in `engines.go`. Keep dependencies in this module. An in-memory adapter should
document synchronization and lack of persistence. A future SQLite WAL adapter
should explicitly configure its checkpoint/vacuum behavior for each profile,
rather than treating checkpointing as identical to LSM compaction.
