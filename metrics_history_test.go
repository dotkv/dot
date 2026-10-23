package dot

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func historySample(at time.Time, value uint64) metricsSnapshot {
	mean, p99 := 1234.5, 2047.0
	scanMean, scanP99 := 123456.0, 131071.0
	return metricsSnapshot{
		Time: at.UnixMilli(), IntervalMS: 1000, DiskBytes: value, ObsoleteBytes: 12,
		EntryCount: 34, IndexMappedBytes: 56, Ready: true, StaleShards: 2,
		DiskStale: true, LatencyEnabled: true, OperationWindowMS: 10_000,
		Operations: operationMetrics{
			Get:  operationMetric{Count: 100, OpsPerSecond: 10, LatencySamples: 100, MeanNS: &mean, P99NS: &p99},
			Scan: operationMetric{Count: 3, OpsPerSecond: 0.3, LatencySamples: 3, MeanNS: &scanMean, P99NS: &scanP99},
		},
	}
}

func testHistory(t testing.TB, created time.Time) *metricsHistory {
	t.Helper()
	h := openMetricsHistory(filepath.Join(t.TempDir(), metricsHistoryName), created, 7*24*time.Hour)
	if h.err != nil {
		t.Fatal(h.err)
	}
	t.Cleanup(func() {
		if h.file != nil {
			h.file.Close()
		}
	})
	return h
}

func TestMetricsHistoryEncoding(t *testing.T) {
	want := historySample(time.Now(), 12345)
	record := encodeMetricsHistory(want)
	got, ok := decodeMetricsHistory(record[:])
	if !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded=%+v valid=%t, want %+v", got, ok, want)
	}
	record[24] ^= 1
	if _, ok := decodeMetricsHistory(record[:]); ok {
		t.Fatal("corrupt record passed checksum validation")
	}
	record = encodeMetricsHistory(want)
	record[64+operationScan*40+32] ^= 1
	if _, ok := decodeMetricsHistory(record[:]); ok {
		t.Fatal("corrupt scan latency passed checksum validation")
	}
}

func TestMetricsHistoryDefaultAndDisabled(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []MetricsOption
	}{
		{name: "default"},
		{name: "explicit zero", opts: []MetricsOption{WithMetricsHistory(0)}},
		{name: "override", opts: []MetricsOption{WithMetricsHistory(time.Hour), WithMetricsHistory(0)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newInternalDB(t, WithShardCount(1))
			h := db.MetricsHandler(tc.opts...).(*metricsHandler)
			if h.history != nil || db.metricsHistory != nil || db.operationMetrics.Load() != nil {
				t.Fatal("disabled history started collection without a viewer")
			}
			if !h.latency || h.interval != time.Second {
				t.Fatal("disabled history changed live collection defaults")
			}
			if _, err := os.Stat(filepath.Join(db.path, metricsHistoryName)); !os.IsNotExist(err) {
				t.Fatalf("disabled history created a file: %v", err)
			}
			response := httptest.NewRecorder()
			h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/history", nil))
			if response.Code != http.StatusNotFound {
				t.Fatalf("disabled history endpoint status: %d", response.Code)
			}
		})
	}
}

func TestMetricsHistoryOptIn(t *testing.T) {
	db := newInternalDB(t, WithShardCount(1))
	h := db.MetricsHandler(WithMetricsHistory(24 * time.Hour)).(*metricsHandler)
	if h.history == nil || h.history.retention != 24*time.Hour || !h.history.latency.Load() {
		t.Fatal("explicit retention did not enable one-day history with latency")
	}
	h.history.mu.RLock()
	info, err := h.history.file.Stat()
	var signature [8]byte
	_, signatureErr := h.history.file.ReadAt(signature[:], 0)
	h.history.mu.RUnlock()
	if signatureErr != nil || string(signature[:]) != "DOTMETRX" {
		t.Fatalf("history signature=%q err=%v", signature, signatureErr)
	}
	if err != nil || info.Size() != metricsHistoryFileSize(24*60*60) {
		t.Fatalf("opt-in history file: info=%v err=%v", info, err)
	}
	other := newInternalDB(t, WithShardCount(1))
	short := other.MetricsHandler(WithMetricsHistory(1500*time.Millisecond), WithMetricsLatency(false)).(*metricsHandler)
	if short.history.retention != 2*time.Second || short.history.slots != 2 || short.history.latency.Load() {
		t.Fatal("custom retention did not round up, or enabled unwanted latency")
	}
}

func TestMetricsHistoryInvalidRetention(t *testing.T) {
	for _, retention := range []time.Duration{-time.Second, time.Duration(1<<63 - 1)} {
		t.Run(retention.String(), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("invalid retention did not panic")
				}
			}()
			WithMetricsHistory(retention)
		})
	}
	db := newInternalDB(t, WithShardCount(1))
	db.MetricsHandler(WithMetricsHistory(time.Hour))
	defer func() {
		if recover() == nil {
			t.Fatal("conflicting retention for the same DB did not panic")
		}
	}()
	db.MetricsHandler(WithMetricsHistory(2 * time.Hour))
}

func TestMetricsHistoryRejectsUnsupportedFormats(t *testing.T) {
	for _, signature := range []string{"DOTMET01", "DOTMET02", "DOTMX001", "OTHERFMT"} {
		t.Run(signature, func(t *testing.T) {
			created := time.Now().Add(-time.Hour)
			h := testHistory(t, created)
			h.append(historySample(created.Add(time.Minute), 42))
			path := h.file.Name()
			var header [metricsHistoryHeaderSize]byte
			if _, err := h.file.ReadAt(header[:], 0); err != nil {
				t.Fatal(err)
			}
			copy(header[:8], signature)
			binary.LittleEndian.PutUint32(header[60:], crc32.ChecksumIEEE(header[:60]))
			if _, err := h.file.WriteAt(header[:], 0); err != nil {
				t.Fatal(err)
			}
			h.file.Close()
			h.file = nil
			rejected := openMetricsHistory(path, time.Now(), h.retention)
			if rejected.err == nil || rejected.file != nil {
				t.Fatal("unsupported metrics format was accepted")
			}
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			var after [metricsHistoryHeaderSize]byte
			if _, err := file.ReadAt(after[:], 0); err != nil || !bytes.Equal(header[:], after[:]) {
				t.Fatalf("rejected format was rewritten: err=%v", err)
			}
		})
	}
}

func TestMetricsHistoryRejectsInvalidHeaderFields(t *testing.T) {
	for _, tc := range []struct {
		name   string
		offset int
		value  byte
	}{
		{"version", 8, 1},
		{"reserved-before-time", 12, 1},
		{"reserved-after-slots", 32, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := testHistory(t, time.Now().Add(-time.Hour))
			path := h.file.Name()
			var header [metricsHistoryHeaderSize]byte
			if _, err := h.file.ReadAt(header[:], 0); err != nil {
				t.Fatal(err)
			}
			header[tc.offset] = tc.value
			binary.LittleEndian.PutUint32(header[60:], crc32.ChecksumIEEE(header[:60]))
			if _, err := h.file.WriteAt(header[:], 0); err != nil {
				t.Fatal(err)
			}
			h.file.Close()
			h.file = nil
			invalid := openMetricsHistory(path, time.Now(), h.retention)
			if invalid.err == nil || invalid.file != nil {
				t.Fatal("invalid header field was accepted with a valid checksum")
			}
		})
	}
}

func TestMetricsHistoryResize(t *testing.T) {
	now := time.Now().Truncate(time.Second).Add(-time.Second)
	created := now.Add(-8 * 24 * time.Hour)
	h := testHistory(t, created)
	samples := []metricsSnapshot{historySample(now.Add(-6*24*time.Hour), 6), historySample(now.Add(-2*24*time.Hour), 2), historySample(now, 1)}
	for _, sample := range samples {
		h.append(sample)
	}
	path := h.file.Name()

	h.file.Close()
	h.file = nil
	grown := openMetricsHistory(path, time.Now(), 30*24*time.Hour)
	if grown.err != nil {
		t.Fatal(grown.err)
	}
	got, err := grown.query(context.Background(), created.UnixMilli(), now.UnixMilli(), []int{0})
	if err != nil || len(got.Samples) != 3 {
		grown.file.Close()
		t.Fatalf("growth lost readings: samples=%+v err=%v", got.Samples, err)
	}
	for i := range samples {
		if got.Samples[i].Time != samples[i].Time || got.Samples[i].DiskBytes != samples[i].DiskBytes || !reflect.DeepEqual(got.Samples[i].Operations, samples[i].Operations) {
			t.Fatalf("growth changed a reading: %+v", got.Samples)
		}
	}
	grown.file.Close()
	// A leftover staging file never replaces the published history.
	if err := os.WriteFile(filepath.Join(filepath.Dir(path), metricsHistoryTempName), []byte("interrupted resize"), 0600); err != nil {
		t.Fatal(err)
	}
	shrunk := openMetricsHistory(path, time.Now(), 3*24*time.Hour)
	if shrunk.err != nil {
		t.Fatal(shrunk.err)
	}
	defer shrunk.file.Close()
	got, err = shrunk.query(context.Background(), created.UnixMilli(), now.UnixMilli(), []int{0})
	if err != nil || len(got.Samples) != 2 || got.Samples[0].DiskBytes != 2 || got.Samples[1].DiskBytes != 1 {
		t.Fatalf("shrink retained the wrong readings: samples=%+v err=%v", got.Samples, err)
	}
	info, err := shrunk.file.Stat()
	if err != nil || info.Size() != metricsHistoryFileSize(shrunk.slots) {
		t.Fatalf("shrunk file size: info=%v err=%v", info, err)
	}
}

func TestMetricsHistoryResizeFailurePreservesOriginal(t *testing.T) {
	created := time.Now().Add(-time.Hour)
	h := testHistory(t, created)
	sample := historySample(created.Add(time.Minute), 42)
	h.append(sample)
	path, slots := h.file.Name(), h.slots
	h.file.Close()
	h.file = nil
	// Prevent staging creation without touching the published ring.
	if err := os.Mkdir(filepath.Join(filepath.Dir(path), metricsHistoryTempName), 0700); err != nil {
		t.Fatal(err)
	}
	failed := openMetricsHistory(path, time.Now(), 24*time.Hour)
	if failed.err == nil || failed.file != nil {
		t.Fatal("blocked resize did not report a history failure")
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() != metricsHistoryFileSize(slots) {
		t.Fatalf("failed resize changed published capacity: info=%v err=%v", info, err)
	}
	original := openMetricsHistory(path, time.Now(), 7*24*time.Hour)
	if original.err != nil {
		t.Fatal(original.err)
	}
	defer original.file.Close()
	got, err := original.query(context.Background(), sample.Time, sample.Time, []int{0})
	if err != nil || len(got.Samples) != 1 || got.Samples[0].DiskBytes != sample.DiskBytes {
		t.Fatalf("failed resize lost the published reading: %+v err=%v", got.Samples, err)
	}
}

func TestMetricsHistoryRetentionAndRestart(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	h := testHistory(t, now.Add(-8*24*time.Hour))
	// The first two rows own the same slot, exactly seven days apart.
	h.append(historySample(now.Add(-h.retention), 1))
	h.append(historySample(now, 99))
	// Keep this row well inside retention: querying the ring can cross a second
	// boundary, so a row just one second inside retention can expire mid-test.
	h.append(historySample(now.Add(-h.retention/2), 2))
	h.append(historySample(now.Add(-8*24*time.Hour), 3))
	path := h.file.Name()
	if err := h.file.Close(); err != nil {
		t.Fatal(err)
	}
	h.file = nil
	h = openMetricsHistory(path, time.Now(), h.retention)
	if h.err != nil {
		t.Fatal(h.err)
	}
	defer h.file.Close()
	got, err := h.query(context.Background(), now.Add(-8*24*time.Hour).UnixMilli(), now.UnixMilli(), []int{0})
	if err != nil || len(got.Samples) != 2 || got.Samples[0].DiskBytes != 2 || got.Samples[1].DiskBytes != 99 {
		t.Fatalf("retained history=%+v err=%v", got.Samples, err)
	}
	info, err := h.file.Stat()
	if err != nil || info.Size() != metricsHistoryFileSize(h.slots) {
		t.Fatalf("file size: info=%v err=%v", info, err)
	}
	// A torn record is omitted; the other retained row still loads.
	offset := int64(metricsHistoryHeaderSize) + now.Unix()%h.slots*metricsHistoryRecordSize
	if _, err := h.file.WriteAt([]byte{0xff}, offset+24); err != nil {
		t.Fatal(err)
	}
	got, err = h.query(context.Background(), now.Add(-h.retention).UnixMilli(), now.UnixMilli(), []int{0})
	if err != nil || len(got.Samples) != 1 || got.Samples[0].DiskBytes != 2 {
		t.Fatalf("torn record handling: samples=%+v err=%v", got.Samples, err)
	}
}

func TestMetricsHistoryExtremaAndDetail(t *testing.T) {
	now := time.Now().Truncate(time.Second).Add(-time.Second)
	start := now.Add(-time.Hour)
	h := testHistory(t, start)
	for i := range 3601 {
		sample := historySample(start.Add(time.Duration(i)*time.Second), 100)
		if i == 13 {
			sample.DiskBytes = 1_000_000
		}
		if i == 14 {
			sample.DiskBytes = 1
		}
		h.append(sample)
	}
	got, err := h.query(context.Background(), start.UnixMilli(), now.UnixMilli(), []int{0})
	if err != nil {
		t.Fatal(err)
	}
	var high, low bool
	for i, sample := range got.Samples {
		high = high || sample.DiskBytes == 1_000_000
		low = low || sample.DiskBytes == 1
		if i > 0 && sample.Time <= got.Samples[i-1].Time {
			t.Fatal("history is not strictly ordered")
		}
	}
	if !high || !low || len(got.Samples) > metricsHistoryBuckets*4 {
		t.Fatalf("extrema: high=%t low=%t count=%d", high, low, len(got.Samples))
	}
	detail, err := h.query(context.Background(), start.UnixMilli(), start.Add(time.Minute).UnixMilli(), []int{0})
	if err != nil || len(detail.Samples) != 61 || detail.ResolutionMS != 1000 {
		t.Fatalf("detail: count=%d resolution=%d err=%v", len(detail.Samples), detail.ResolutionMS, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := h.query(ctx, start.UnixMilli(), now.UnixMilli(), []int{0}); err != context.Canceled {
		t.Fatalf("canceled query: %v", err)
	}
}

func TestMetricsHistoryScanExtrema(t *testing.T) {
	now := time.Now().Truncate(time.Second).Add(-time.Second)
	start := now.Add(-time.Hour)
	h := testHistory(t, start)
	extrema := map[int]bool{13: false, 14: false, 19: false, 20: false, 26: false, 27: false}
	for i := range 60 {
		sample := historySample(start.Add(time.Duration(i)*time.Second), 100)
		mean, p99 := 100.0, 1000.0
		sample.Operations.Scan.OpsPerSecond = 10
		switch i {
		case 13:
			sample.Operations.Scan.OpsPerSecond = 1000
		case 14:
			sample.Operations.Scan.OpsPerSecond = 0.01
		case 19:
			mean = 1_000_000
		case 20:
			mean = 1
		case 26:
			p99 = 1_000_000_000
		case 27:
			p99 = 2
		}
		sample.Operations.Scan.MeanNS, sample.Operations.Scan.P99NS = &mean, &p99
		h.append(sample)
	}
	// Select every series to exercise the expanded extrema arrays as well as
	// each Scan field's position in the encoded record.
	keys := make([]int, len(metricsHistoryKeys))
	for key := range keys {
		keys[key] = key
	}
	got, err := h.query(context.Background(), start.UnixMilli(), now.UnixMilli(), keys)
	if err != nil {
		t.Fatal(err)
	}
	for _, sample := range got.Samples {
		second := int((sample.Time - start.UnixMilli()) / 1000)
		if _, ok := extrema[second]; ok {
			extrema[second] = true
		}
	}
	for second, retained := range extrema {
		if !retained {
			t.Errorf("scan extremum at second %d was lost during downsampling", second)
		}
	}
}

func TestMetricsHistoryGapsSurviveDownsampling(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	start := now.Add(-6 * 24 * time.Hour)
	h := testHistory(t, start)
	// A ten-second restart and a three-hour crash, with no synthetic zero rows.
	for _, offset := range []time.Duration{0, time.Second, 12 * time.Second, 13 * time.Second, 3 * time.Hour, 3*time.Hour + time.Second} {
		h.append(historySample(start.Add(offset), 100))
	}
	overview, err := h.query(context.Background(), start.UnixMilli(), now.UnixMilli(), []int{0})
	if err != nil {
		t.Fatal(err)
	}
	var gaps int
	for _, sample := range overview.Samples {
		if sample.GapBefore {
			gaps++
		}
		if sample.DiskBytes != 100 {
			t.Fatal("missing time became a synthetic value")
		}
	}
	if gaps != 2 {
		t.Fatalf("overview preserved %d gaps, want 2", gaps)
	}
	detail, err := h.query(context.Background(), start.UnixMilli(), start.Add(time.Minute).UnixMilli(), []int{0})
	if err != nil || len(detail.Samples) != 4 || !detail.Samples[2].GapBefore || detail.Samples[3].GapBefore {
		t.Fatalf("restart gap detail: %+v err=%v", detail.Samples, err)
	}
}

func TestMetricsHistoryStreamSurvivesRefresh(t *testing.T) {
	db := newInternalDB(t, WithShardCount(1))
	start := time.Now().Add(-10 * time.Second)
	history := testHistory(t, start)
	h := &metricsHandler{db: db, interval: time.Second, latency: true, history: history}
	read := func(updates <-chan metricsSnapshot, want metricsSnapshot) {
		t.Helper()
		select {
		case got := <-updates:
			if got.Time != want.Time || got.DiskBytes != want.DiskBytes || !reflect.DeepEqual(got.Operations, want.Operations) {
				t.Fatalf("streamed snapshot differs from history: got %+v, want %+v", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("persisted snapshot was not streamed")
		}
	}
	first := historySample(start.Add(time.Second), 1)
	history.append(first)
	updates, stop := h.subscribe()
	defer stop()
	read(updates, first)
	stop()
	// The recorder keeps its cadence while the page reloads. The replacement
	// connection starts at the same saved reading rather than a fresh sample.
	second := historySample(start.Add(2*time.Second), 2)
	history.append(second)
	updates, stopRefresh := h.subscribe()
	defer stopRefresh()
	read(updates, second)
	third := historySample(start.Add(3*time.Second), 3)
	history.append(third)
	read(updates, third)
	counts := &metricsHandler{db: db, interval: time.Second, history: history}
	countsUpdates, stopCounts := counts.subscribe()
	defer stopCounts()
	select {
	case sample := <-countsUpdates:
		if sample.Time != third.Time || sample.LatencyEnabled || sample.Operations.Get.Count != third.Operations.Get.Count || sample.Operations.Get.MeanNS != nil || sample.Operations.Get.P99NS != nil || sample.Operations.Get.LatencySamples != 0 {
			t.Fatalf("counts-only view of shared snapshot: %+v", sample)
		}
		if sample.Operations.Scan.Count != third.Operations.Scan.Count || sample.Operations.Scan.LatencySamples != 0 || sample.Operations.Scan.MeanNS != nil || sample.Operations.Scan.P99NS != nil {
			t.Fatalf("counts-only view retained scan latency: %+v", sample.Operations.Scan)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("counts-only viewer did not receive the shared snapshot")
	}
	saved, err := history.query(context.Background(), first.Time, third.Time, []int{0})
	if err != nil || len(saved.Samples) != 3 {
		t.Fatalf("history across refresh: %+v err=%v", saved.Samples, err)
	}
	for i, sample := range saved.Samples {
		if sample.Time != first.Time+int64(i)*1000 || sample.DiskBytes != uint64(i+1) || !reflect.DeepEqual(sample.Operations, third.Operations) {
			t.Fatalf("missing or shifted reading after refresh: %+v", saved.Samples)
		}
	}
}

func TestMetricsHistoryLifecycleAndHTTP(t *testing.T) {
	db := newInternalDB(t, WithShardCount(1))
	h := db.MetricsHandler(WithMetricsHistory(7*24*time.Hour), WithMetricsLatency(false)).(*metricsHandler)
	h2 := db.MetricsHandler(WithMetricsHistory(7 * 24 * time.Hour)).(*metricsHandler)
	if h.history == nil || h.history != h2.history || db.operationMetrics.Load() == nil || !h.history.latency.Load() {
		t.Fatal("history handlers did not share an active, latency-enabled collector")
	}
	// No browser has connected: operations must already be recorded.
	if err := db.Set("before-viewer", uint64(1)); err != nil {
		t.Fatal(err)
	}
	for _, err := range db.Scan() {
		if err != nil {
			t.Fatal(err)
		}
	}
	got, _ := db.operationMetrics.Load().snapshot(time.Now(), true)
	if got.Set.Count != 1 || got.Set.LatencySamples != 1 || got.Scan.Count != 1 || got.Scan.LatencySamples != 1 {
		t.Fatalf("unattended recording: %+v", got)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		history, err := h.history.query(context.Background(), h.history.created, time.Now().UnixMilli(), []int{0})
		if err != nil {
			t.Fatal(err)
		}
		if len(history.Samples) > 0 && history.Samples[len(history.Samples)-1].Operations.Set.Count == 1 && history.Samples[len(history.Samples)-1].Operations.Scan.Count == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("collector did not persist unattended operations")
		}
		time.Sleep(10 * time.Millisecond)
	}
	server := httptest.NewServer(http.StripPrefix("/prefix", h))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, server.URL+"/prefix/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	var sample metricsSnapshot
	if err := wsjson.Read(ctx, c, &sample); err != nil || sample.HistoryStart <= 0 || sample.HistoryRetentionMS != (7*24*time.Hour).Milliseconds() || sample.HistoryError != "" {
		t.Fatalf("history advertisement: %+v err=%v", sample, err)
	}
	c.CloseNow()
	if db.operationMetrics.Load() == nil {
		t.Fatal("disconnect stopped persistent recording")
	}
	url := fmt.Sprintf("/history?start=%d&end=%d&metrics=disk_bytes,get_p99_ns,scan_ops,scan_mean_ns,scan_p99_ns", time.Now().Add(-time.Minute).UnixMilli(), time.Now().UnixMilli())
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest("GET", url, nil))
	var history metricsHistoryResponse
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &history) != nil {
		t.Fatalf("history response: %d %s", response.Code, response.Body.String())
	}
	var recordedScan bool
	for _, sample := range history.Samples {
		recordedScan = recordedScan || (sample.Operations.Scan.Count == 1 && sample.Operations.Scan.MeanNS != nil && sample.Operations.Scan.P99NS != nil)
	}
	if !recordedScan {
		t.Fatal("history response omitted scan metrics")
	}
	for _, query := range []string{"start=no&end=1&metrics=disk_bytes", "start=2&end=1&metrics=disk_bytes", "start=0&end=1&metrics=unknown"} {
		response = httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest("GET", "/history?"+query, nil))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid query status: %d", response.Code)
		}
	}
	path := db.path
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.history.done:
	default:
		t.Fatal("Close did not wait for history flushing")
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restarted := reopened.MetricsHandler(WithMetricsHistory(7 * 24 * time.Hour)).(*metricsHandler)
	if _, message := restarted.history.status(time.Now()); message != "" || restarted.history.created != h.history.created {
		t.Fatal("database restart did not retain the history file")
	}
}

func TestMetricsHistoryFailureAndAccounting(t *testing.T) {
	db := newInternalDB(t, WithShardCount(1))
	before, err := metricsDiskUsage(context.Background(), db.path)
	if err != nil {
		t.Fatal(err)
	}
	h := db.MetricsHandler(WithMetricsHistory(7 * 24 * time.Hour)).(*metricsHandler)
	// Allocate a history slot without changing the database storage metric.
	h.history.append(historySample(time.Now(), 1))
	after, err := metricsDiskUsage(context.Background(), db.path)
	if err != nil || before != after {
		t.Fatalf("history affected database disk usage: before=%d after=%d err=%v", before, after, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(db.path, metricsHistoryName), []byte("interrupted header"), 0600); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(db.path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	failed := reopened.MetricsHandler(WithMetricsHistory(7 * 24 * time.Hour)).(*metricsHandler)
	if _, message := failed.history.status(time.Now()); message == "" {
		t.Fatal("history failure was not reported")
	}
	if err := reopened.Set("still-works", uint64(1)); err != nil {
		t.Fatalf("telemetry failure affected database operations: %v", err)
	}
	response := httptest.NewRecorder()
	failed.ServeHTTP(response, httptest.NewRequest("GET", fmt.Sprintf("/history?start=0&end=%d&metrics=disk_bytes", time.Now().UnixMilli()), nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("failed history status: %d", response.Code)
	}
}

func BenchmarkMetricsHistoryWeek(b *testing.B) {
	now := time.Now().Truncate(time.Second)
	start := now.Add(-7*24*time.Hour + time.Second)
	h := testHistory(b, start)
	for i := int64(0); i < h.slots; i++ {
		h.append(historySample(start.Add(time.Duration(i)*time.Second), uint64(i%100)))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := h.query(context.Background(), start.UnixMilli(), now.UnixMilli(), []int{0}); err != nil {
			b.Fatal(err)
		}
	}
}
