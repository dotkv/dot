# Metrics example

From the repository root:

```sh
go run ./example
```

Open <http://localhost:8080/dot/metrics/>. The example opens `db/` in the current
directory and runs 16 workers that continuously read, write, update, and delete
random `uint64` keys with 1–2 second expirations. Press Ctrl+C to stop.

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
is bounded to about 22.4 MiB per retained day.

See the [metrics guide](https://dotkv.github.io/dot/metrics.html) for options
and an explanation of the readings.
