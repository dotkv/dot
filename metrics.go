package dot

import (
	"bytes"
	"context"
	"embed"
	"html/template"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// MetricsOption configures MetricsHandler independently of database operations.
type MetricsOption func(*metricsOptions)

type metricsOptions struct {
	interval time.Duration
	latency  bool
	history  time.Duration
}

// WithMetricsLatency controls latency timing for dashboard viewers. It defaults
// to true. If false, this handler requests only operation counts; timing stops
// when no connected handler or persistent history collector requests it.
func WithMetricsLatency(enabled bool) MetricsOption {
	return func(o *metricsOptions) { o.latency = enabled }
}

// WithMetricsHistory sets the duration of persistent, one-second snapshots.
// Zero (the default) disables history. Negative durations panic; positive
// durations round up to whole seconds. Collection starts when the
// handler is constructed with positive retention and continues until DB.Close,
// including without viewers.
// History-enabled handlers sharing a DB must request the same retention. Changing
// retention after reopening resizes the METRICS ring, preserving samples that fit.
// Storage is bounded to about 21.1 MiB per retained day.
// Latency recording stays enabled until Close once any history handler requests
// it. Writes are synced every minute and on Close; a crash can lose recent
// history. History failures are shown in the dashboard and do not fail DB calls.
func WithMetricsHistory(retention time.Duration) MetricsOption {
	if retention < 0 {
		panic("dot: metrics history retention must not be negative")
	}
	if retention > 0 {
		seconds := (retention-1)/time.Second + 1
		if seconds > time.Duration(1<<63-1)/time.Second {
			panic("dot: metrics history retention is too large")
		}
		retention = seconds * time.Second
	}
	return func(o *metricsOptions) { o.history = retention }
}

// WithMetricsInterval sets the interval between metric snapshots. The default
// is one second. Like time.NewTicker, it panics if interval is not positive.
// Persistent history always records at one-second intervals.
func WithMetricsInterval(interval time.Duration) MetricsOption {
	if interval <= 0 {
		panic("dot: metrics interval must be positive")
	}
	return func(o *metricsOptions) { o.interval = interval }
}

//go:embed metrics_ui/index.html metrics_ui/app.css metrics_ui/app.js metrics_ui/*.png
var metricsAssets embed.FS

// The dashboard and its build metadata are immutable for the binary's lifetime.
var metricsIndexHTML = sync.OnceValues(func() ([]byte, error) {
	info, _ := debug.ReadBuildInfo()
	return renderMetricsIndex(metricsBuildVersion(info))
})

func renderMetricsIndex(version string) ([]byte, error) {
	t, err := template.ParseFS(metricsAssets, "metrics_ui/index.html")
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	err = t.Execute(&out, struct{ Version string }{Version: version})
	return out.Bytes(), err
}

// MetricsHandler serves an embedded dashboard at / and current metrics over a
// WebSocket at /ws. Mount it on a standard http.ServeMux, using http.StripPrefix
// with the prefix excluding its trailing slash, so the handler receives paths
// beginning with /. By default persistent history is disabled: history stays in
// the browser and metrics collect only while viewers are connected. Opt in with
// WithMetricsHistory using positive retention to collect without viewers and
// serve bounded historical views at /history.
// Operation rates, means, and approximate p99 latencies cover a trailing ten
// seconds at one-second resolution (a shorter window during initial warm-up).
// All calls count, including misses, errors, and no-ops. Timings include lock
// waits, Update callbacks, and synchronous writes. Histograms round p99 up by
// at most 12.5%. Operation snapshots, like shard readings, are not transactional.
//
// On-disk size scans allocated filesystem blocks under the database path,
// including directories, metadata, and temporary files, excluding the METRICS
// history file, without following child symlinks. Obsolete bytes are retained
// log bytes no longer referenced by the index. Entry counts include expired
// entries until pruning. Snapshots skip busy shard locks and reuse their previous readings. Failed disk scans also reuse
// the previous reading; snapshots report stale or incomplete data.
// Index mapped bytes describe this database's index mappings, including pages
// not resident in RAM.
//
// Storage collection is shared by clients of this handler. Operation recording
// is shared across all handlers on this DB and stops when the last client
// disconnects or the database closes, unless persistent history is enabled.
// With no viewers or history, key operations only check whether recording is
// enabled. Origin checks use coder/websocket defaults;
// authentication and HTTP server lifecycle belong to the calling application.
func (db *DB) MetricsHandler(opts ...MetricsOption) http.Handler {
	o := metricsOptions{interval: time.Second, latency: true}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	h := &metricsHandler{db: db, interval: o.interval, latency: o.latency}
	if o.history > 0 {
		h.history = db.startMetricsHistory(o.history, o.latency)
	}
	return h
}

type metricsSnapshot struct {
	Time               int64            `json:"time"`
	IntervalMS         int64            `json:"interval_ms"`
	DiskBytes          uint64           `json:"disk_bytes"`
	ObsoleteBytes      uint64           `json:"obsolete_bytes"`
	EntryCount         uint64           `json:"entry_count"`
	IndexMappedBytes   uint64           `json:"index_mapped_bytes"`
	StaleShards        int              `json:"stale_shards"`
	DiskStale          bool             `json:"disk_stale"`
	Ready              bool             `json:"ready"`
	Operations         operationMetrics `json:"operations"`
	OperationWindowMS  float64          `json:"operation_window_ms"`
	LatencyEnabled     bool             `json:"latency_enabled"`
	HistoryRetentionMS int64            `json:"history_retention_ms,omitempty"`
	HistoryStart       int64            `json:"history_start,omitempty"`
	HistoryError       string           `json:"history_error,omitempty"`
	GapBefore          bool             `json:"gap_before,omitempty"`
}

type metricsShard struct {
	obsoleteBytes, entryCount, indexMappedBytes uint64
	ready                                       bool
}

type metricsCache struct {
	shards     []metricsShard
	diskBytes  uint64
	diskReady  bool
	operations *operationRecorder
}

type metricsStream struct {
	clients map[chan metricsSnapshot]struct{}
	stop    chan struct{}
	last    metricsSnapshot
	cache   metricsCache
}

type metricsHandler struct {
	db       *DB
	interval time.Duration
	latency  bool
	mu       sync.Mutex // Guards stream, its clients, and its last snapshot.
	stream   *metricsStream
	history  *metricsHistory
}

func (h *metricsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.URL.Path == "/ws" {
		h.serveWebSocket(w, r)
		return
	}
	if r.URL.Path == "/history" {
		h.serveHistory(w, r)
		return
	}
	var name, contentType string
	switch r.URL.Path {
	case "/":
		name, contentType = "index.html", "text/html; charset=utf-8"
	case "/app.css":
		name, contentType = "app.css", "text/css; charset=utf-8"
	case "/app.js":
		name, contentType = "app.js", "text/javascript; charset=utf-8"
	case "/dot128.png", "/dot512.png":
		name, contentType = r.URL.Path[1:], "image/png"
	default:
		http.NotFound(w, r)
		return
	}
	var data []byte
	var err error
	if name == "index.html" {
		data, err = metricsIndexHTML()
	} else {
		data, err = metricsAssets.ReadFile("metrics_ui/" + name)
	}
	if err != nil {
		http.Error(w, "dashboard asset unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	if r.Method != http.MethodHead {
		_, _ = w.Write(data)
	}
}

func (h *metricsHandler) serveWebSocket(w http.ResponseWriter, r *http.Request) {
	if h.db.metricsContext.Err() != nil {
		http.Error(w, "database is closed", http.StatusServiceUnavailable)
		return
	}
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer c.CloseNow()
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(h.db.metricsContext, cancel)
	defer stop()
	ctx = c.CloseRead(ctx)
	updates, unsubscribe := h.subscribe()
	defer unsubscribe()
	for {
		select {
		case <-ctx.Done():
			return
		case snapshot := <-updates:
			writeCtx, writeCancel := context.WithTimeout(ctx, 5*time.Second)
			err := wsjson.Write(writeCtx, c, snapshot)
			writeCancel()
			if err != nil {
				return
			}
		}
	}
}

func (h *metricsHandler) subscribe() (<-chan metricsSnapshot, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.stream
	if s == nil {
		s = &metricsStream{
			clients: make(map[chan metricsSnapshot]struct{}),
			stop:    make(chan struct{}),
			cache:   metricsCache{shards: make([]metricsShard, len(h.db.shards))},
		}
		h.stream = s
		s.cache.operations = h.db.acquireOperationMetrics(h.latency)
		if h.history != nil && h.interval == time.Second {
			go h.collectHistory(s)
		} else {
			go h.collect(s)
		}
	}
	ch := make(chan metricsSnapshot, 1)
	s.clients[ch] = struct{}{}
	if s.last.Time != 0 {
		ch <- s.last
	}
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			delete(s.clients, ch)
			if len(s.clients) == 0 {
				close(s.stop)
				h.stream = nil
				h.db.releaseOperationMetrics(s.cache.operations, h.latency)
			}
		})
	}
}

func (h *metricsHandler) collect(s *metricsStream) {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-h.db.metricsContext.Done():
			return
		default:
		}
		snapshot := h.snapshot(&s.cache)
		if h.history != nil {
			snapshot.HistoryStart, snapshot.HistoryError = h.history.status(time.Now())
			snapshot.HistoryRetentionMS = h.history.retention.Milliseconds()
		}
		h.publish(s, snapshot)
		select {
		case <-s.stop:
			return
		case <-h.db.metricsContext.Done():
			return
		case <-ticker.C:
		}
	}
}

// The default live stream uses the recorder's snapshots and cadence, including
// across disconnects. Refreshing must not create a second sampling clock whose
// readings disappear when the browser reloads history.
func (h *metricsHandler) collectHistory(s *metricsStream) {
	var lastTime int64
	for {
		h.history.mu.RLock()
		snapshot, updated := h.history.last, h.history.updated
		h.history.mu.RUnlock()
		if snapshot.Time != 0 && snapshot.Time != lastTime {
			lastTime = snapshot.Time
			if !h.latency {
				snapshot.LatencyEnabled = false
				for _, op := range []*operationMetric{&snapshot.Operations.Get, &snapshot.Operations.Set, &snapshot.Operations.Delete, &snapshot.Operations.Update} {
					op.LatencySamples, op.MeanNS, op.P99NS = 0, nil, nil
				}
			}
			snapshot.HistoryStart, snapshot.HistoryError = h.history.status(time.Now())
			snapshot.HistoryRetentionMS = h.history.retention.Milliseconds()
			h.publish(s, snapshot)
		}
		select {
		case <-s.stop:
			return
		case <-h.db.metricsContext.Done():
			return
		case <-updated:
		}
	}
}

func (h *metricsHandler) publish(s *metricsStream, snapshot metricsSnapshot) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s.last = snapshot
	for ch := range s.clients {
		// A slow viewer gets the latest snapshot without blocking collection.
		select {
		case <-ch:
		default:
		}
		ch <- snapshot
	}
}

func (h *metricsHandler) snapshot(cache *metricsCache) metricsSnapshot {
	result := metricsSnapshot{Time: time.Now().UnixMilli(), IntervalMS: h.interval.Milliseconds()}
	result.LatencyEnabled = h.latency
	result.Operations, result.OperationWindowMS = cache.operations.snapshot(time.Now(), h.latency)
	diskBytes, err := metricsDiskUsage(h.db.metricsContext, h.db.path)
	if err == nil {
		cache.diskBytes, cache.diskReady = diskBytes, true
	}
	result.DiskBytes, result.DiskStale = cache.diskBytes, err != nil
	result.Ready = cache.diskReady
	for i := range h.db.shards {
		s := &h.db.shards[i]
		if s.lock.TryRLock() {
			if !s.closed {
				cache.shards[i] = metricsShard{
					obsoleteBytes:    s.log.recordBytes - s.index.liveBytes,
					entryCount:       uint64(s.index.entryCount),
					indexMappedBytes: uint64(len(s.index.memory.current)) + uint64(len(s.index.memory.previous)),
					ready:            true,
				}
			}
			s.lock.RUnlock()
		} else {
			result.StaleShards++
		}
		result.ObsoleteBytes += cache.shards[i].obsoleteBytes
		result.EntryCount += cache.shards[i].entryCount
		result.IndexMappedBytes += cache.shards[i].indexMappedBytes
		result.Ready = result.Ready && cache.shards[i].ready
	}
	return result
}

// Scan outside shard locks: compaction and writes can continue during stat calls.
func metricsDiskUsage(ctx context.Context, root string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	// Open accepts a symlink as its database path; resolve that root only.
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		return 0, err
	}
	var total uint64
	hardlinks := make(map[[2]uint64]struct{})
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Compaction may remove a file after its directory has been read.
		if os.IsNotExist(err) && path != root {
			return nil
		}
		if err != nil {
			return err
		}
		if path == filepath.Join(root, metricsHistoryName) || path == filepath.Join(root, metricsHistoryTempName) {
			return nil
		}
		info, err := entry.Info() // Lstat: symlink targets outside the path are excluded.
		if os.IsNotExist(err) && path != root {
			return nil
		}
		if err != nil {
			return err
		}
		stat := info.Sys().(*syscall.Stat_t)
		if !info.IsDir() && stat.Nlink > 1 {
			key := [2]uint64{uint64(stat.Dev), uint64(stat.Ino)}
			if _, seen := hardlinks[key]; seen {
				return nil
			}
			hardlinks[key] = struct{}{}
		}
		total += uint64(stat.Blocks) * 512
		return nil
	})
	return total, err
}
