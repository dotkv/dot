package dot

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime/debug"
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
	h := db.MetricsHandler(WithMetricsHistory(0)).(*metricsHandler)
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
	h = db.MetricsHandler(WithMetricsHistory(0)).(*metricsHandler)
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
	h := db.MetricsHandler(WithMetricsHistory(0)).(*metricsHandler)
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

func TestMetricsOriginAndDisconnect(t *testing.T) {
	db := newInternalDB(t)
	h := db.MetricsHandler(WithMetricsHistory(0), WithMetricsInterval(10*time.Millisecond)).(*metricsHandler)
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

func TestMetricsIndexMappedBytes(t *testing.T) {
	db := newInternalDB(t, WithShardCount(1))
	stopTestMaintenance(db)
	h := db.MetricsHandler(WithMetricsHistory(0)).(*metricsHandler)
	cache := &metricsCache{shards: make([]metricsShard, 1)}
	s := &db.shards[0]
	initial := uint64(len(s.index.memory.current))
	if got := h.snapshot(cache); !got.Ready || got.IndexMappedBytes != initial || initial == 0 {
		t.Fatalf("initial mappings: %+v", got)
	}
	s.lock.Lock()
	// Retain both mappings as can happen when unmapping after growth fails.
	slots, err := s.index.memory.allocate(2 * len(s.index.slots))
	if err != nil {
		s.lock.Unlock()
		t.Fatal(err)
	}
	s.index.slots = slots
	busy := h.snapshot(cache)
	s.lock.Unlock()
	if busy.StaleShards != 1 || busy.IndexMappedBytes != initial {
		t.Fatalf("busy shard should retain its last mapping size: %+v", busy)
	}
	if got := h.snapshot(cache); got.IndexMappedBytes != 3*initial {
		t.Fatalf("current and retained previous mappings: %+v", got)
	}
	s.lock.Lock()
	err = s.index.memory.releasePrevious()
	s.lock.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if got := h.snapshot(cache); got.IndexMappedBytes != 2*initial {
		t.Fatalf("released previous mapping: %+v", got)
	}
	if err := s.compactExpirations(); err != nil {
		t.Fatal(err)
	}
	if got := h.snapshot(cache); got.IndexMappedBytes != initial {
		t.Fatalf("shrunk index mapping: %+v", got)
	}
}

func TestMetricsBuildVersion(t *testing.T) {
	const revision = "605b593ab9c6546e798b5b9aec099615fbfdf97f"
	const pseudo = "v0.0.0-20261022182138-605b593ab9c6"
	vcs := []debug.BuildSetting{
		{Key: "vcs.revision", Value: revision},
		{Key: "vcs.modified", Value: "true"},
	}
	for _, tc := range []struct {
		name string
		info *debug.BuildInfo
		want string
	}{
		{name: "unavailable", want: "dev"},
		{name: "missing module", info: &debug.BuildInfo{}, want: "dev"},
		{
			name: "main release",
			info: &debug.BuildInfo{Main: debug.Module{Path: dotModulePath, Version: "v0.1.0"}},
			want: "v0.1.0",
		},
		{
			name: "main prerelease",
			info: &debug.BuildInfo{Main: debug.Module{Path: dotModulePath, Version: "v0.1.0-rc.1"}},
			want: "v0.1.0-rc.1",
		},
		{
			name: "main pseudo-version",
			info: &debug.BuildInfo{Main: debug.Module{Path: dotModulePath, Version: pseudo}},
			want: "dev · 605b593",
		},
		{
			name: "main devel",
			info: &debug.BuildInfo{Main: debug.Module{Path: dotModulePath, Version: "(devel)"}},
			want: "dev",
		},
		{
			name: "main devel with revision",
			info: &debug.BuildInfo{Main: debug.Module{Path: dotModulePath, Version: "(devel)"}, Settings: vcs[:1]},
			want: "dev · 605b593",
		},
		{
			name: "main dirty release",
			info: &debug.BuildInfo{Main: debug.Module{Path: dotModulePath, Version: "v0.1.0"}, Settings: vcs},
			want: "dev · 605b593 · dirty",
		},
		{
			name: "main dirty pseudo-version without settings",
			info: &debug.BuildInfo{Main: debug.Module{Path: dotModulePath, Version: pseudo + "+dirty"}},
			want: "dev · 605b593 · dirty",
		},
		{
			name: "main dirty tag without revision",
			info: &debug.BuildInfo{Main: debug.Module{Path: dotModulePath, Version: "v0.1.0+dirty"}},
			want: "dev · dirty",
		},
		{
			name: "short revision",
			info: &debug.BuildInfo{Main: debug.Module{Path: dotModulePath}, Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc"}}},
			want: "dev · abc",
		},
		{
			name: "dependency release ignores host version and dirty state",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "example.com/app", Version: "v9.0.0"}, Settings: vcs,
				Deps: []*debug.Module{
					{Path: "example.com/other", Version: "v1.0.0"},
					{Path: dotModulePath, Version: "v0.1.0"},
				},
			},
			want: "v0.1.0",
		},
		{
			name: "dependency pseudo-version",
			info: &debug.BuildInfo{Deps: []*debug.Module{{Path: dotModulePath, Version: pseudo}}},
			want: "dev · 605b593",
		},
		{
			name: "pseudo-version after release",
			info: &debug.BuildInfo{Deps: []*debug.Module{{Path: dotModulePath, Version: "v0.1.1-0.20261022182138-605b593ab9c6"}}},
			want: "dev · 605b593",
		},
		{
			name: "pseudo-version after prerelease",
			info: &debug.BuildInfo{Deps: []*debug.Module{{Path: dotModulePath, Version: "v0.1.0-rc.1.0.20261022182138-605b593ab9c6"}}},
			want: "dev · 605b593",
		},
		{
			name: "local replacement ignores original version and host revision",
			info: &debug.BuildInfo{
				Settings: vcs,
				Deps:     []*debug.Module{{Path: dotModulePath, Version: "v0.1.0", Replace: &debug.Module{Path: "../dot"}}},
			},
			want: "dev",
		},
		{
			name: "versioned replacement",
			info: &debug.BuildInfo{Deps: []*debug.Module{{Path: dotModulePath, Version: "v0.1.0", Replace: &debug.Module{Path: "example.com/fork", Version: "v0.2.0"}}}},
			want: "v0.2.0",
		},
		{
			name: "unrelated host revision",
			info: &debug.BuildInfo{Main: debug.Module{Path: "example.com/app"}, Settings: vcs},
			want: "dev",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := metricsBuildVersion(tc.info); got != tc.want {
				t.Fatalf("version = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMetricsIndexVersion(t *testing.T) {
	for _, tc := range []struct{ version, escaped string }{
		{"v0.1.0", "v0.1.0"},
		{"dev · 605b593 · dirty", "dev · 605b593 · dirty"},
		{`<script>alert("version")</script>`, "&lt;script&gt;alert(&#34;version&#34;)&lt;/script&gt;"},
	} {
		t.Run(tc.version, func(t *testing.T) {
			data, err := renderMetricsIndex(tc.version)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(data), `<span class="version">`+tc.escaped+`</span>`) {
				t.Fatalf("dashboard did not render escaped version %q", tc.escaped)
			}
		})
	}
}

func TestMetricsHTTPVersion(t *testing.T) {
	// Serving the version needs neither an open database nor a WebSocket client.
	var db DB
	w := httptest.NewRecorder()
	db.MetricsHandler(WithMetricsHistory(0)).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	info, _ := debug.ReadBuildInfo()
	want := `<span class="version">` + metricsBuildVersion(info) + `</span>`
	if !strings.Contains(w.Body.String(), want) {
		t.Fatalf("dashboard did not render %q", want)
	}
}
