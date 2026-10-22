# Metrics fiddle

From the repository root:

```sh
go run -buildvcs=true ./fiddle
```

Open http://localhost:8080/dot/metrics/. The example creates a fresh database in
`/tmp/dot-fiddle-*`, leaving any existing `fiddle/fiddle.dot` database alone.
Press Ctrl+C to stop.

The dashboard shows the release tag or a development commit and dirty status
from the binary's build metadata. `-buildvcs=true` includes this metadata for
`go run`; without it, the local demo shows `dev`. Applications importing a tagged
Dot dependency get its version automatically without a build flag.

The workload continuously sets, gets, updates, and deletes random keys from a
4,096-key space, using 1 KiB values and short expirations. The operations table
shows ops/sec, mean latency, and approximate p99 for a trailing ten-second window.
Click its values to graph operations. Select one or more
metrics with matching units and the last minute, last hour, or all
retained samples. Selecting a different unit replaces the current selection.
Index mmap shows this database's mapped index space, including pages not resident
in RAM.
Hover over the graph for exact values. The browser keeps at most 20,000 snapshots;
older samples are evicted and reloading starts a new history.

The standard-library HTTP integration is:

```go
mux := http.NewServeMux()
mux.Handle("/dot/metrics/", http.StripPrefix("/dot/metrics",
    db.MetricsHandler(dot.WithMetricsInterval(250*time.Millisecond))))
```

Calling `db.MetricsHandler()` without options defaults to one snapshot per second.
The handler serves embedded HTML, CSS, and JavaScript plus a `/ws` WebSocket using
`github.com/coder/websocket`. It shares collection across viewers, keeps no graph
history, and stops collecting when no viewers remain. `db.Close()` closes streams.
Operation instrumentation is shared across handlers and viewers. With no viewers,
calls only perform an enabled check. Use `dot.WithMetricsLatency(false)` for a
throughput-only handler; latency timing stops once no connected handler needs it.
Reloading after the last viewer leaves starts a fresh operation window. Missing
latency displays as a dash; p99 is rounded up within 12.5% by histogram buckets.
