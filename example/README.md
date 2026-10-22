# Metrics example

From the repository root:

```sh
go run ./example
```

Open <http://localhost:8080/dot/metrics/>. The example creates a temporary
database and continuously reads, writes, updates, and deletes random keys,
using 1 KiB values and short expirations. Press Ctrl+C to stop.

To add the dashboard to your own application:

```go
mux := http.NewServeMux()
mux.Handle("/dot/metrics/", http.StripPrefix("/dot/metrics", db.MetricsHandler()))
```

By default, one day of history is retained. History collects once per second
without viewers and survives restarts when
your application reopens the same database path. The example uses a temporary
database, so each run starts a new history. Storage is bounded to about 21 MiB.
Use `dot.WithMetricsHistory(7*24*time.Hour)`
to retain seven days, or `dot.WithMetricsHistory(0)` to disable persistence.

See the [metrics guide](https://dotkv.github.io/dot/metrics.html) for options
and an explanation of the readings.
