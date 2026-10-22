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
}

// WithMetricsLatency controls latency timing for dashboard viewers. It defaults
// to true. If false, this handler requests only operation counts; timing stops
// when no connected handler requests it. With no viewers, operations perform
// only an enabled check, regardless of this option.
func WithMetricsLatency(enabled bool) MetricsOption {
	return func(o *metricsOptions) { o.latency = enabled }
}

// WithMetricsInterval sets the interval between metric snapshots. The default
// is one second. Like time.NewTicker, it panics if interval is not positive.
func WithMetricsInterval(interval time.Duration) MetricsOption {
	if interval <= 0 {
		panic("dot: metrics interval must be positive")
	}
	return func(o *metricsOptions) { o.interval = interval }
}

//go:embed metrics_ui/index.html metrics_ui/app.css metrics_ui/app.js
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
// beginning with /. The browser keeps graph history; the handler keeps only the
// most recent reading for each shard. It samples only while connected.
// Operation rates, means, and approximate p99 latencies cover a trailing ten
// seconds at one-second resolution (a shorter window during initial warm-up).
// All calls count, including misses, errors, and no-ops. Timings include lock
// waits, Update callbacks, and synchronous writes. Histograms round p99 up by
// at most 12.5%. Operation snapshots, like shard readings, are not transactional.
//
// On-disk size scans allocated filesystem blocks under the database path,
// including directories, metadata, and temporary files, without following child
// symlinks. Obsolete bytes are retained log bytes no longer referenced by the
// index. Entry counts include expired entries until pruning. Snapshots skip busy
// shard locks and reuse their previous readings. Failed disk scans also reuse
// the previous reading; snapshots report stale or incomplete data.
// Index mapped bytes describe this database's index mappings, including pages
// not resident in RAM.
//
// Storage collection is shared by clients of this handler. Operation recording
// is shared across all handlers on this DB and stops when the last client
// disconnects or the database closes. With no viewers, key operations only check
// whether recording is enabled. Origin checks use coder/websocket defaults;
// authentication and HTTP server lifecycle belong to the calling application.
func (db *DB) MetricsHandler(opts ...MetricsOption) http.Handler {
	o := metricsOptions{interval: time.Second, latency: true}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return &metricsHandler{db: db, interval: o.interval, latency: o.latency}
}

type metricsSnapshot struct {
	Time              int64            `json:"time"`
	IntervalMS        int64            `json:"interval_ms"`
	DiskBytes         uint64           `json:"disk_bytes"`
	ObsoleteBytes     uint64           `json:"obsolete_bytes"`
	EntryCount        uint64           `json:"entry_count"`
	IndexMappedBytes  uint64           `json:"index_mapped_bytes"`
	StaleShards       int              `json:"stale_shards"`
	DiskStale         bool             `json:"disk_stale"`
	Ready             bool             `json:"ready"`
	Operations        operationMetrics `json:"operations"`
	OperationWindowMS float64          `json:"operation_window_ms"`
	LatencyEnabled    bool             `json:"latency_enabled"`
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
	var name, contentType string
	switch r.URL.Path {
	case "/":
		name, contentType = "index.html", "text/html; charset=utf-8"
	case "/app.css":
		name, contentType = "app.css", "text/css; charset=utf-8"
	case "/app.js":
		name, contentType = "app.js", "text/javascript; charset=utf-8"
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
		go h.collect(s)
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
		h.mu.Lock()
		s.last = snapshot
		for ch := range s.clients {
			// A slow viewer gets the latest snapshot without blocking collection.
			select {
			case <-ch:
			default:
			}
			ch <- snapshot
		}
		h.mu.Unlock()
		select {
		case <-s.stop:
			return
		case <-h.db.metricsContext.Done():
			return
		case <-ticker.C:
		}
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
