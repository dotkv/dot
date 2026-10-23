# Engine comparisons

Dot and Pebble with 8 workers, comparing 80/20 and 20/80 reads/writes with
8-byte scalars and 1 KiB byte slices.

## Results

<!-- benchmark-results:start -->

Completed cases: **8**, covering **4 configurations**.

### 80/20 · 8 B uint64 values · 8 workers

Measured operations per engine: **93,041,472**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |  21.007 |     1.702 |   24.575 |          4,429,159 |           47.6 |      20.79 |
| Pebble         |    1 |  57.329 |     4.785 |   75.263 |          1,622,935 |           43.1 |      18.90 |

### 80/20 · 1,024 B values · 8 workers

Measured operations per engine: **12,859,488**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |  20.157 |    12.308 |   48.383 |            637,975 |           49.6 |      45.25 |
| Pebble         |    1 |  54.610 |    33.698 |  219.135 |            235,480 |           49.4 |      38.89 |

### 20/80 · 8 B uint64 values · 8 workers

Measured operations per engine: **50,801,952**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |  23.190 |     3.533 |   30.079 |          2,190,705 |           49.2 |      14.20 |
| Pebble         |    1 |  29.854 |     4.563 |   35.583 |          1,701,700 |           44.0 |      18.36 |

### 20/80 · 1,024 B values · 8 workers

Measured operations per engine: **3,182,304**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |  14.888 |    37.102 |   63.487 |            213,747 |           50.2 |      50.80 |
| Pebble         |    1 |  26.275 |    65.466 |  203.775 |            121,114 |           49.5 |      41.27 |

Setup: **16,384 initial keys**, **16-byte keys**, **30,000 warmup operations**, seed **1**.

Shared operation budgets target approximately **15s for Dot**.

Environment: go1.27.2, linux/amd64, 16 logical CPUs, GOMAXPROCS=16.

CPU: AMD Ryzen 7 7840HS w/ Radeon 780M Graphics.

Pebble: `v2.1.7`.

<!-- benchmark-results:end -->

## Run

```sh
cd benchmarks
go run .
```

Requires Linux. Each successful run updates the results above.

If linking fails with "No space left on device" or "disk quota exceeded", check
`/tmp`. A memory-backed `/tmp` can reach its capacity or per-user quota even when
the disk has free space. Use a temporary directory on disk for the build:

```sh
mkdir -p "$HOME/.cache/dot/tmp"
TMPDIR="$HOME/.cache/dot/tmp" go run .
```

`TMPDIR` also moves the external linker's temporary files; `GOTMPDIR` alone is
not enough. See [local setup](../tests/README.md#local-setup) to apply this to
terminal and editor Go commands through mise.

## How it works

Each case runs sequentially in a fresh process and temporary database. A 500 ms
Dot probe estimates a fixed operation count for roughly 15 seconds of Dot work;
both engines then execute that same count with identical seeded traces. Preload,
warmup, and final flush/close are untimed. Temporary databases are removed afterward.

Workers use separate key ranges with uniform access. Reads are 95% hits; writes
cycle through inserts, updates, and deletes. Eight-byte values use scalar APIs;
larger values use byte slices. Both engines use automatic compaction. Dot uses
its default buffered writes; Pebble uses an uncompressed WAL with `NoSync` writes.
Neither configuration fsyncs each operation.

Throughput includes workload generation and validation; latency measures the
engine call. Peak RSS includes preparation, and disk usage is measured after
flush/close. Each table row is one run.
