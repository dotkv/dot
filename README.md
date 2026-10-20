# Dot

A small log-backed key-value embedded store in Go.

## Benchmarks

The [engine comparison suite](benchmarks/README.md) compares Dot with Pebble across
read-heavy, mixed, and write-heavy workloads, using 8-byte scalars and 1 KiB
values with 16 workers by default. It reports latency, throughput, RSS, and disk
usage. Its dependencies live in a separate module.
