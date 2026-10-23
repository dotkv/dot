# Engine comparisons

Dot and Pebble with 8 workers, comparing 80/20 and 20/80 reads/writes with
8-byte scalars and 1 KiB byte slices.

## Results

<!-- benchmark-results:start -->

Completed cases: **8**, covering **4 configurations**.

### 80/20 · 8 B uint64 values · 8 workers

Measured operations per engine: **92,527,872**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |  21.186 |     1.729 |   24.703 |          4,367,345 |           44.8 |      16.08 |
| Pebble         |    1 |  56.957 |     4.795 |   75.775 |          1,624,517 |           41.8 |      18.64 |

### 80/20 · 1,024 B values · 8 workers

Measured operations per engine: **13,018,680**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |  20.383 |    12.313 |   48.895 |            638,691 |           51.0 |      48.51 |
| Pebble         |    1 |  55.100 |    33.582 |  220.159 |            236,273 |           50.5 |      34.52 |

### 20/80 · 8 B uint64 values · 8 workers

Measured operations per engine: **50,686,872**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |  23.112 |     3.513 |   29.951 |          2,193,094 |           47.4 |      25.96 |
| Pebble         |    1 |  29.331 |     4.491 |   35.327 |          1,728,101 |           43.8 |      18.98 |

### 20/80 · 1,024 B values · 8 workers

Measured operations per engine: **3,254,304**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |  15.443 |    37.655 |   66.047 |            210,728 |           50.5 |      48.70 |
| Pebble         |    1 |  27.002 |    65.937 |  204.799 |            120,520 |           51.3 |      41.96 |

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
