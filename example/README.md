# Metrics example

From the repository root:

```sh
go run ./example
```

Open <http://localhost:8080/dot/metrics/>. The example opens `db/` in the current
directory and continuously reads, writes, updates, and deletes random keys,
using 1 KiB values and short expirations. Press Ctrl+C to stop.

To add the dashboard to your own application:

```go
mux := http.NewServeMux()
mux.Handle("/dot/metrics/", http.StripPrefix("/dot/metrics", db.MetricsHandler()))
```

Persistent history defaults to zero (disabled). The example collects metrics only
while viewers are connected; each browser keeps live history until reload.
Opt in with `db.MetricsHandler(dot.WithMetricsHistory(7*24*time.Hour))` to retain
seven days. When enabled, history collects once per second without viewers and
survives restarts when your application reopens the same database path. Storage
is bounded to about 21.1 MiB per retained day.

See the [metrics guide](https://dotkv.github.io/dot/metrics.html) for options
and an explanation of the readings.
