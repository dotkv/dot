package dot

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func TestMetricsAccounting(t *testing.T) {
	db := newInternalDB(t, WithShardCount(2))
	stopTestMaintenance(db)
	h := db.MetricsHandler().(*metricsHandler)
	cache := &metricsCache{shards: make([]metricsShard, len(db.shards))}
	if got := h.snapshot(cache); !got.Ready || got.DiskBytes != allocatedTreeBytes(t, db.path) || got.ObsoleteBytes != 0 || got.EntryCount != 0 {
		t.Fatalf("empty metrics: %+v", got)
	}
	for _, err := range []error{
		db.Set("a", "first"),
		db.Set("a", "first"), // Unchanged writes do not add disk bytes.
		db.Set("b", "second"),
		db.Set("a", strings.Repeat("x", 128)),
		db.Delete("b"),
		db.Delete("absent"),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	assert := func(wantObsolete uint64) {
		t.Helper()
		got := h.snapshot(cache)
		disk := allocatedTreeBytes(t, db.path)
		if !got.Ready || got.StaleShards != 0 || got.DiskBytes != disk || got.ObsoleteBytes != wantObsolete || got.EntryCount != 1 {
			t.Fatalf("metrics=%+v, want disk=%d obsolete=%d entries=1", got, disk, wantObsolete)
		}
	}
	obsolete := recordSize(1, len("first")) + recordSize(1, len("second")) + recordSize(1, 0)
	assert(obsolete)
	path := db.path
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	stopTestMaintenance(db)
	h = db.MetricsHandler().(*metricsHandler)
	assert(obsolete)
	for i := range db.shards {
		db.shards[i].log.compactPending = true
	}
	if err := db.compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	assert(0)
}

func TestMetricsBusyShard(t *testing.T) {
	db := newInternalDB(t, WithShardCount(1))
	stopTestMaintenance(db)
	h := db.MetricsHandler().(*metricsHandler)
	cache := &metricsCache{shards: make([]metricsShard, 1)}
	s := &db.shards[0]
	s.lock.Lock()
	locked := true
	defer func() {
		if locked {
			s.lock.Unlock()
		}
	}()
	result := make(chan metricsSnapshot, 1)
	go func() { result <- h.snapshot(cache) }()
	select {
	case got := <-result:
		if got.Ready || got.StaleShards != 1 {
			t.Fatalf("initial busy snapshot: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("snapshot blocked on a busy shard")
	}
	s.lock.Unlock()
	locked = false
	if err := db.Set("key", uint64(1), WithExpiration(time.Now().Add(-time.Hour))); err != nil {
		t.Fatal(err)
	}
	got := h.snapshot(cache)
	if !got.Ready || got.EntryCount != 1 || got.ObsoleteBytes != 0 {
		t.Fatalf("unpruned expired entry: %+v", got)
	}
	s.lock.Lock()
	locked = true
	stale := h.snapshot(cache)
	if !stale.Ready || stale.StaleShards != 1 || stale.DiskBytes != got.DiskBytes || stale.EntryCount != 1 {
		t.Fatalf("cached busy snapshot: %+v", stale)
	}
	s.index.prune()
	s.lock.Unlock()
	locked = false
	if got := h.snapshot(cache); got.EntryCount != 0 || got.ObsoleteBytes != s.log.recordBytes {
		t.Fatalf("pruned expired entry: %+v", got)
	}
}

func TestMetricsHTTP(t *testing.T) {
	db := newInternalDB(t)
	mux := http.NewServeMux()
	mux.Handle("/nested/dot/", http.StripPrefix("/nested/dot", db.MetricsHandler()))
	for _, tc := range []struct {
		method, path, contentType string
		status                    int
	}{
		{"GET", "/nested/dot", "", http.StatusTemporaryRedirect},
		{"GET", "/nested/dot/", "text/html", http.StatusOK},
		{"GET", "/nested/dot/app.css", "text/css", http.StatusOK},
		{"GET", "/nested/dot/app.js", "text/javascript", http.StatusOK},
		{"HEAD", "/nested/dot/", "text/html", http.StatusOK},
		{"GET", "/nested/dot/missing", "", http.StatusNotFound},
		{"GET", "/nested/dot/metrics_ui/", "", http.StatusNotFound},
		{"POST", "/nested/dot/", "", http.StatusMethodNotAllowed},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
			if w.Code != tc.status || !strings.HasPrefix(w.Header().Get("Content-Type"), tc.contentType) {
				t.Fatalf("status=%d content-type=%q", w.Code, w.Header().Get("Content-Type"))
			}
			if tc.method == "HEAD" && w.Body.Len() != 0 {
				t.Fatal("HEAD returned a body")
			}
		})
	}
}

func TestMetricsPrefixedDashboard(t *testing.T) {
	db := newInternalDB(t, WithShardCount(1))
	mux := http.NewServeMux()
	// Match fiddle's mount: strip the prefix while preserving the path's slash.
	const prefix = "/dot/metrics"
	mux.Handle(prefix+"/", http.StripPrefix(prefix, db.MetricsHandler()))
	server := httptest.NewServer(mux)
	defer server.Close()
	for _, asset := range []string{"", "app.css", "app.js"} {
		response, err := server.Client().Get(server.URL + prefix + "/" + asset)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Errorf("%s: status=%d, want 200", prefix+"/"+asset, response.StatusCode)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, server.URL+prefix+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	var snapshot metricsSnapshot
	if err := wsjson.Read(ctx, c, &snapshot); err != nil {
		t.Fatal(err)
	}
	if !snapshot.Ready {
		t.Fatalf("incomplete snapshot: %+v", snapshot)
	}
}

func TestMetricsWebSocket(t *testing.T) {
	for _, custom := range []bool{false, true} {
		t.Run(fmt.Sprintf("custom=%v", custom), func(t *testing.T) {
			db := newInternalDB(t, WithShardCount(2))
			opts := []MetricsOption(nil)
			wantInterval := int64(1000)
			if custom {
				opts = append(opts, WithMetricsInterval(20*time.Millisecond))
				wantInterval = 20
			}
			h := db.MetricsHandler(opts...).(*metricsHandler)
			mux := http.NewServeMux()
			mux.Handle("/prefix/", http.StripPrefix("/prefix", h))
			server := httptest.NewServer(mux)
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, _, err := websocket.Dial(ctx, server.URL+"/prefix/ws", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer c.CloseNow()
			var first metricsSnapshot
			if err := wsjson.Read(ctx, c, &first); err != nil {
				t.Fatal(err)
			}
			if !first.Ready || first.IntervalMS != wantInterval || first.EntryCount != 0 || !first.LatencyEnabled || first.Operations.Get.MeanNS != nil {
				t.Fatalf("initial snapshot: %+v", first)
			}
			// A second viewer shares the same collector and current snapshot.
			c2, _, err := websocket.Dial(ctx, server.URL+"/prefix/ws", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer c2.CloseNow()
			var shared metricsSnapshot
			if err := wsjson.Read(ctx, c2, &shared); err != nil {
				t.Fatal(err)
			}
			h.mu.Lock()
			clients := len(h.stream.clients)
			h.mu.Unlock()
			if clients != 2 {
				t.Fatalf("shared clients=%d, want 2", clients)
			}
			if custom {
				if err := db.Set("hello", "world"); err != nil {
					t.Fatal(err)
				}
				updateCtx, stop := context.WithTimeout(ctx, 500*time.Millisecond)
				defer stop()
				for shared.EntryCount != 1 || shared.Operations.Set.Count != 1 {
					if err := wsjson.Read(updateCtx, c, &shared); err != nil {
						t.Fatalf("configured push did not deliver changed metrics: %v", err)
					}
				}
				if shared.Operations.Set.MeanNS == nil || shared.Operations.Set.P99NS == nil || shared.Operations.Set.LatencySamples != 1 {
					t.Fatalf("missing operation metrics in WebSocket update: %+v", shared.Operations.Set)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			for wsjson.Read(ctx, c, &shared) == nil {
			}
			if ctx.Err() != nil {
				t.Fatal("DB.Close did not close the WebSocket promptly")
			}
			_, response, err := websocket.Dial(ctx, server.URL+"/prefix/ws", nil)
			if err == nil || response == nil || response.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("closed database: response=%v err=%v", response, err)
			}
		})
	}
}

func TestMetricsOriginAndDisconnect(t *testing.T) {
	db := newInternalDB(t)
	h := db.MetricsHandler(WithMetricsInterval(10 * time.Millisecond)).(*metricsHandler)
	server := httptest.NewServer(h)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, response, err := websocket.Dial(ctx, server.URL+"/ws", &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": {"https://unrelated.invalid"}},
	})
	if err == nil || response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin connection: response=%v err=%v", response, err)
	}
	c, _, err := websocket.Dial(ctx, server.URL+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	var snapshot metricsSnapshot
	if err := wsjson.Read(ctx, c, &snapshot); err != nil {
		t.Fatal(err)
	}
	c.CloseNow()
	for {
		h.mu.Lock()
		stopped := h.stream == nil
		h.mu.Unlock()
		if stopped {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("collector did not stop after the last viewer disconnected")
		case <-time.After(time.Millisecond):
		}
	}
	// Assets remain ordinary HTTP responses after a viewer leaves.
	res, err := server.Client().Get(server.URL + "/app.js")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if _, err := io.Copy(io.Discard, res.Body); err != nil {
		t.Fatal(err)
	}
}

func TestMetricsInvalidInterval(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second} {
		t.Run(interval.String(), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("nonpositive interval did not panic")
				}
			}()
			WithMetricsInterval(interval)
		})
	}
}

// Count the whole tree independently of the collector's directory walker.
func allocatedTreeBytes(t *testing.T, root string) uint64 {
	t.Helper()
	var total uint64
	err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err == nil {
			total += uint64(info.Sys().(*syscall.Stat_t).Blocks) * 512
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return total
}

func TestMetricsDiskUsage(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "nested")
	if err := os.Mkdir(nested, 0700); err != nil {
		t.Fatal(err)
	}
	meta := filepath.Join(root, "META")
	if err := os.WriteFile(meta, []byte("metadata"), 0600); err != nil {
		t.Fatal(err)
	}
	// A temporary compaction file with a large sparse hole: measure allocation,
	// rather than its apparent length, even though it is not a published log.
	temporary := filepath.Join(nested, "compact.tmp")
	f, err := os.Create(temporary)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(16 << 20); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("data"), 0); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "unrelated"), make([]byte, 64<<10), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "outside")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(temporary, filepath.Join(root, "same-file")); err != nil {
		t.Fatal(err)
	}
	var want uint64
	for _, path := range []string{root, nested, meta, temporary, link} {
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		want += uint64(info.Sys().(*syscall.Stat_t).Blocks) * 512
	}
	got, err := metricsDiskUsage(context.Background(), root)
	if err != nil || got != want {
		t.Fatalf("disk usage=%d err=%v, want %d", got, err, want)
	}
	alias := filepath.Join(t.TempDir(), "database")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	if got, err := metricsDiskUsage(context.Background(), alias); err != nil || got != want {
		t.Fatalf("symlinked database usage=%d err=%v, want %d", got, err, want)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := metricsDiskUsage(ctx, root); err != context.Canceled {
		t.Fatalf("canceled scan: %v", err)
	}
}

func TestMetricsDiskScanFailure(t *testing.T) {
	db := newInternalDB(t, WithShardCount(1))
	stopTestMaintenance(db)
	// A separate collector path lets us simulate a failed scan without changing
	// the database's own path or touching its open files.
	view := &DB{path: filepath.Join(t.TempDir(), "missing"), shards: db.shards, metricsContext: db.metricsContext}
	h := &metricsHandler{db: view, interval: time.Second}
	cache := &metricsCache{shards: make([]metricsShard, 1)}
	if got := h.snapshot(cache); got.Ready || !got.DiskStale {
		t.Fatalf("initial scan failure: %+v", got)
	}
	view.path = db.path
	good := h.snapshot(cache)
	if !good.Ready || good.DiskStale || good.DiskBytes != allocatedTreeBytes(t, db.path) {
		t.Fatalf("scan recovery: %+v", good)
	}
	view.path = filepath.Join(t.TempDir(), "missing-again")
	if got := h.snapshot(cache); !got.Ready || !got.DiskStale || got.DiskBytes != good.DiskBytes {
		t.Fatalf("failed scan lost previous reading: %+v, previous %+v", got, good)
	}
}
