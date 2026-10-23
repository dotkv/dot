# Engine comparisons

Dot and Pebble with 8 workers, comparing 80/20 and 20/80 reads/writes with
8-byte scalars and 1 KiB byte slices.

## Results

<!-- benchmark-results:start -->

Completed cases: **8**, covering **4 configurations**.

### 80/20 · 8 B uint64 values · 8 workers

Measured operations per engine: **94,069,680**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |  21.346 |     1.711 |   24.319 |          4,406,937 |           50.3 |      30.20 |
| Pebble         |    1 |  54.258 |     4.501 |   72.703 |          1,733,755 |           44.3 |      18.20 |

### 80/20 · 1,024 B values · 8 workers

Measured operations per engine: **13,011,360**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |  19.846 |    12.006 |   46.335 |            655,604 |           53.5 |      47.45 |
| Pebble         |    1 |  53.160 |    32.440 |  215.039 |            244,759 |           51.4 |      34.99 |

### 20/80 · 8 B uint64 values · 8 workers

Measured operations per engine: **51,072,600**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |  22.245 |     3.385 |   27.903 |          2,295,891 |           51.5 |      16.10 |
| Pebble         |    1 |  28.707 |     4.367 |   34.047 |          1,779,096 |           44.3 |      18.24 |

### 20/80 · 1,024 B values · 8 workers

Measured operations per engine: **3,129,048**. Ratios are reads/writes.

| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | ------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 |  14.569 |    36.926 |   62.463 |            214,780 |           51.2 |      45.50 |
| Pebble         |    1 |  25.321 |    64.228 |  204.799 |            123,573 |           50.0 |      45.72 |

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
