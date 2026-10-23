# Engine comparisons

Dot and Pebble with 8 workers, comparing 80/20 and 20/80 reads/writes with
8-byte scalars and 1 KiB byte slices.

## Results

<!-- benchmark-results:start -->

Completed cases: **8**, covering **4 configurations**.

### 80/20 · 8 B uint64 values · 8 workers

Measured operations per engine: **94,278,480**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |  20.440 |     1.635 |   23.295 |          4,612,541 |           46.9 |      32.12 |
| Pebble         |    1 |  57.052 |     4.715 |   74.751 |          1,652,512 |           43.6 |      18.75 |

### 80/20 · 1,024 B values · 8 workers

Measured operations per engine: **13,002,912**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |  19.135 |    11.591 |   48.127 |            679,527 |           50.6 |      47.18 |
| Pebble         |    1 |  55.592 |    33.907 |  221.183 |            233,898 |           47.8 |      37.44 |

### 20/80 · 8 B uint64 values · 8 workers

Measured operations per engine: **50,957,016**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |  23.071 |     3.502 |   29.055 |          2,208,715 |           49.3 |      12.87 |
| Pebble         |    1 |  30.439 |     4.643 |   35.327 |          1,674,079 |           44.8 |      18.23 |

### 20/80 · 1,024 B values · 8 workers

Measured operations per engine: **3,164,160**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |  14.684 |    36.877 |   63.487 |            215,479 |           48.8 |      54.98 |
| Pebble         |    1 |  26.556 |    66.613 |  212.991 |            119,153 |           49.9 |      38.95 |

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
