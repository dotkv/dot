package dot_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/dotkv/dot"
)

func TestMetricsHTTP(t *testing.T) {
	db := newDB(t)
	mux := http.NewServeMux()
	mux.Handle("/nested/dot/", http.StripPrefix("/nested/dot", db.MetricsHandler(dot.WithMetricsHistory(0))))
	for _, tc := range []struct {
		method, path, contentType string
		status                    int
	}{
		{"GET", "/nested/dot", "", http.StatusTemporaryRedirect},
		{"GET", "/nested/dot/", "text/html", http.StatusOK},
		{"GET", "/nested/dot/app.css", "text/css", http.StatusOK},
		{"GET", "/nested/dot/app.js", "text/javascript", http.StatusOK},
		{"GET", "/nested/dot/dot128.png", "image/png", http.StatusOK},
		{"GET", "/nested/dot/dot512.png", "image/png", http.StatusOK},
		{"HEAD", "/nested/dot/dot128.png", "image/png", http.StatusOK},
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
	db := newDB(t, dot.WithShardCount(1))
	mux := http.NewServeMux()
	// Match the example's mount: strip the prefix while preserving the path's slash.
	const prefix = "/dot/metrics"
	mux.Handle(prefix+"/", http.StripPrefix(prefix, db.MetricsHandler(dot.WithMetricsHistory(0))))
	server := httptest.NewServer(mux)
	defer server.Close()
	for _, asset := range []string{"", "app.css", "app.js", "dot128.png", "dot512.png"} {
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
	var snapshot struct {
		Ready bool `json:"ready"`
	}
	if err := wsjson.Read(ctx, c, &snapshot); err != nil {
		t.Fatal(err)
	}
	if !snapshot.Ready {
		t.Fatalf("incomplete snapshot: %+v", snapshot)
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
			dot.WithMetricsInterval(interval)
		})
	}
}

// Decode the public JSON protocol without sharing implementation structs.
type wireOperation struct {
	Count          uint64   `json:"count"`
	LatencySamples uint64   `json:"latency_samples"`
	MeanNS         *float64 `json:"mean_ns"`
	P99NS          *float64 `json:"p99_ns"`
}
type wireSnapshot struct {
	Ready          bool   `json:"ready"`
	IntervalMS     int64  `json:"interval_ms"`
	EntryCount     uint64 `json:"entry_count"`
	LatencyEnabled bool   `json:"latency_enabled"`
	Operations     struct {
		Get  wireOperation `json:"get"`
		Set  wireOperation `json:"set"`
		Scan wireOperation `json:"scan"`
	} `json:"operations"`
}

func TestMetricsWebSocket(t *testing.T) {
	for _, custom := range []bool{false, true} {
		t.Run(fmt.Sprintf("custom=%v", custom), func(t *testing.T) {
			db := newDB(t, dot.WithShardCount(2))
			opts := []dot.MetricsOption{dot.WithMetricsHistory(0)}
			wantInterval := int64(1000)
			if custom {
				opts = append(opts, dot.WithMetricsInterval(20*time.Millisecond))
				wantInterval = 20
			}
			h := db.MetricsHandler(opts...)
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
			var first wireSnapshot
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
			var shared wireSnapshot
			if err := wsjson.Read(ctx, c2, &shared); err != nil {
				t.Fatal(err)
			}
			if custom {
				if err := db.Set("hello", "world"); err != nil {
					t.Fatal(err)
				}
				for _, err := range db.Scan() {
					if err != nil {
						t.Fatal(err)
					}
				}
				updateCtx, stop := context.WithTimeout(ctx, 500*time.Millisecond)
				defer stop()
				for shared.EntryCount != 1 || shared.Operations.Set.Count != 1 || shared.Operations.Scan.Count != 1 {
					if err := wsjson.Read(updateCtx, c, &shared); err != nil {
						t.Fatalf("configured push did not deliver changed metrics: %v", err)
					}
				}
				if shared.Operations.Set.MeanNS == nil || shared.Operations.Set.P99NS == nil || shared.Operations.Set.LatencySamples != 1 {
					t.Fatalf("missing operation metrics in WebSocket update: %+v", shared.Operations.Set)
				}
				if shared.Operations.Scan.MeanNS == nil || shared.Operations.Scan.P99NS == nil || shared.Operations.Scan.LatencySamples != 1 {
					t.Fatalf("missing scan metrics in WebSocket update: %+v", shared.Operations.Scan)
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
