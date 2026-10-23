package dot

import (
	"errors"
	"fmt"
	"math"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestOperationMetricsLifecycle(t *testing.T) {
	db := newInternalDB(t, WithShardCount(2))
	h := db.MetricsHandler().(*metricsHandler)
	counts := db.MetricsHandler(WithMetricsLatency(false)).(*metricsHandler)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
	if db.operationMetrics.Load() != nil {
		t.Fatal("constructing or serving dashboard assets enabled operation metrics")
	}
	_, stop := h.subscribe()
	defer stop()
	m := db.operationMetrics.Load()
	if m == nil || !m.latency.Load() {
		t.Fatal("viewer did not enable operation timing")
	}
	_, stopSecond := h.subscribe()
	defer stopSecond()
	_, stopCounts := counts.subscribe()
	defer stopCounts()
	if db.operationMetrics.Load() != m || m.users != 2 {
		t.Fatal("handlers did not share one operation recorder")
	}
	stop()
	if !m.latency.Load() {
		t.Fatal("timing stopped while another viewer remained")
	}
	stopSecond()
	if db.operationMetrics.Load() != m || m.latency.Load() {
		t.Fatal("counts-only handler did not keep counts while stopping timing")
	}
	if _, err := db.Get[uint64]("absent"); err != ErrNotFound {
		t.Fatal(err)
	}
	got, _ := m.snapshot(time.Now(), true)
	if got.Get.Count != 1 || got.Get.LatencySamples != 0 || got.Get.MeanNS != nil || got.Get.P99NS != nil {
		t.Fatalf("counts-only operation: %+v", got.Get)
	}
	stopCounts()
	if db.operationMetrics.Load() != nil {
		t.Fatal("last viewer did not disable instrumentation")
	}
	select {
	case <-m.done:
	case <-time.After(time.Second):
		t.Fatal("operation sampler did not stop")
	}
	db.Get[uint64]("absent")
	got, _ = m.snapshot(time.Now(), true)
	if got.Get.Count != 1 {
		t.Fatal("a call after disconnect was recorded")
	}
	_, stopNew := h.subscribe()
	defer stopNew()
	next := db.operationMetrics.Load()
	if next == nil || next == m {
		t.Fatal("reconnecting did not create a fresh session")
	}
	got, _ = next.snapshot(time.Now(), true)
	if got.Get.Count != 0 {
		t.Fatal("reconnecting inherited counts from a previous session")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if db.operationMetrics.Load() != nil {
		t.Fatal("Close did not disable operation metrics")
	}
	select {
	case <-next.done:
	default:
		t.Fatal("Close did not wait for the operation sampler")
	}
}

func TestOperationMetricsCalls(t *testing.T) {
	db := newInternalDB(t, WithShardCount(4))
	h := db.MetricsHandler(WithMetricsHistory(0)).(*metricsHandler)
	_, stop := h.subscribe()
	defer stop()
	abort := errors.New("abort callback")
	for _, err := range []error{db.Set("a", uint64(1)), db.Set("a", uint64(1))} {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Get[uint64]("a"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get[uint64]("missing"); err != ErrNotFound {
		t.Fatal(err)
	}
	if _, err := db.Get[string]("a"); !errors.Is(err, ErrTypeMismatch) {
		t.Fatal(err)
	}
	if err := db.Update("a", func(v uint64, _ bool) (uint64, error) { return v + 1, nil }); err != nil {
		t.Fatal(err)
	}
	if err := db.Update("a", func(v uint64, _ bool) (uint64, error) { return v, nil }); err != nil {
		t.Fatal(err)
	}
	if err := db.Update("a", func(v uint64, _ bool) (uint64, error) { return v, abort }); err != abort {
		t.Fatal(err)
	}
	if err := db.Update("a", func(v string, _ bool) (string, error) { return v, nil }); !errors.Is(err, ErrTypeMismatch) {
		t.Fatal(err)
	}
	for _, err := range []error{db.Delete("a"), db.Delete("a")} {
		if err != nil {
			t.Fatal(err)
		}
	}
	got, window := db.operationMetrics.Load().snapshot(time.Now(), true)
	if window <= 0 || window > 1000 {
		t.Fatalf("unexpected initial window: %f ms", window)
	}
	for name, check := range map[string]struct {
		metric operationMetric
		count  uint64
	}{"get": {got.Get, 3}, "set": {got.Set, 2}, "delete": {got.Delete, 2}, "update": {got.Update, 4}} {
		if check.metric.Count != check.count || check.metric.LatencySamples != check.count || check.metric.MeanNS == nil || check.metric.P99NS == nil {
			t.Errorf("%s: %+v, want %d calls including misses, errors, and no-ops", name, check.metric, check.count)
		}
		wantRate := float64(check.count) / (window / 1000)
		if math.Abs(check.metric.OpsPerSecond-wantRate) > 1e-6 {
			t.Errorf("%s: rate=%f, want %f", name, check.metric.OpsPerSecond, wantRate)
		}
	}
}

func TestOperationLatencyIncludesCallback(t *testing.T) {
	db := newInternalDB(t, WithShardCount(1))
	h := db.MetricsHandler(WithMetricsHistory(0)).(*metricsHandler)
	_, stop := h.subscribe()
	defer stop()
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- db.Update("a", func(v uint64, _ bool) (uint64, error) {
			close(entered)
			<-release
			return v + 1, nil
		})
	}()
	<-entered
	before := time.Now()
	time.Sleep(10 * time.Millisecond)
	wait := time.Since(before)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got, _ := db.operationMetrics.Load().snapshot(time.Now(), true)
	if got.Update.MeanNS == nil || *got.Update.MeanNS < float64(wait) {
		t.Fatalf("callback wait missing from latency: %+v, waited %s", got.Update, wait)
	}
}

func TestOperationScanMetrics(t *testing.T) {
	for _, latency := range []bool{false, true} {
		t.Run(fmt.Sprintf("latency=%t", latency), func(t *testing.T) {
			db := newInternalDB(t, WithShardCount(4))
			keys := make([][]byte, len(db.shards))
			for i := range db.shards {
				keys[i] = keysForShard(db, &db.shards[i], "metrics-scan", 1)[0]
				if err := db.Set(keys[i], uint64(i)); err != nil {
					t.Fatal(err)
				}
			}
			// Recording is selected when iteration starts, even for an iterator
			// created before collection was enabled.
			scan := db.Scan()
			m := db.acquireOperationMetrics(latency)
			defer db.releaseOperationMetrics(m, latency)
			check := func(count uint64) {
				t.Helper()
				got, window := m.snapshot(time.Now(), true)
				metric := got.Scan
				samples := uint64(0)
				if latency {
					samples = count
				}
				if metric.Count != count || metric.LatencySamples != samples ||
					(metric.MeanNS != nil) != (samples > 0) || (metric.P99NS != nil) != (samples > 0) {
					t.Fatalf("scan metrics: %+v, want count=%d samples=%d", metric, count, samples)
				}
				if window <= 0 || math.Abs(metric.OpsPerSecond-float64(count)/(window/1000)) > 1e-6 {
					t.Fatalf("scan rate=%f count=%d window=%f", metric.OpsPerSecond, count, window)
				}
				if got.Get.Count != 0 {
					t.Fatalf("scanned entries counted as gets: %+v", got.Get)
				}
			}
			_ = db.Scan() // Constructing another unused iterator must not count.
			check(0)
			for run := range 3 {
				entries := 0
				for _, err := range scan {
					if err != nil {
						t.Fatal(err)
					}
					entries++
					if run == 1 {
						break
					}
				}
				wantEntries := len(keys)
				if run == 1 {
					wantEntries = 1
				}
				if entries != wantEntries {
					t.Fatalf("scan yielded %d entries, want %d", entries, wantEntries)
				}
				check(uint64(run + 1))
			}
			for _, key := range keys {
				if err := db.Delete(key); err != nil {
					t.Fatal(err)
				}
			}
			for _, err := range scan {
				t.Fatalf("empty scan yielded an entry or error: %v", err)
			}
			check(4)
		})
	}
}

func TestOperationScanLatencyIncludesLoopBody(t *testing.T) {
	db := newInternalDB(t, WithShardCount(1))
	if err := db.Set("a", uint64(1)); err != nil {
		t.Fatal(err)
	}
	m := db.acquireOperationMetrics(true)
	defer db.releaseOperationMetrics(m, true)
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		for _, err := range db.Scan() {
			if err != nil {
				done <- err
				return
			}
			close(entered)
			<-release
		}
		done <- nil
	}()
	<-entered
	inProgress, _ := m.snapshot(time.Now(), true)
	before := time.Now()
	time.Sleep(10 * time.Millisecond)
	wait := time.Since(before)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if inProgress.Scan.Count != 0 || inProgress.Scan.LatencySamples != 0 {
		t.Fatalf("unfinished scan was recorded: %+v", inProgress.Scan)
	}
	got, _ := m.snapshot(time.Now(), true)
	if got.Scan.Count != 1 || got.Scan.LatencySamples != 1 || got.Scan.MeanNS == nil || *got.Scan.MeanNS < float64(wait) {
		t.Fatalf("loop body wait missing from scan latency: %+v, waited %s", got.Scan, wait)
	}
}

func TestOperationMetricsWindow(t *testing.T) {
	start := time.Unix(100, 0)
	m := newOperationRecorder(1, start)
	add := func(count, ns uint64) {
		c := &m.shards[0][operationGet]
		c.count.Add(count)
		c.sumNS.Add(count * ns)
		c.histogram[operationBucket(ns)].Add(count)
	}
	add(100, 1000)
	m.sample(start.Add(time.Second))
	add(900, 3000)
	for second := 2; second <= 10; second++ {
		m.sample(start.Add(time.Duration(second) * time.Second))
	}
	got, window := m.snapshot(start.Add(10*time.Second), true)
	if window != 10_000 || got.Get.Count != 1000 || got.Get.OpsPerSecond != 100 || *got.Get.MeanNS != 2800 {
		t.Fatalf("weighted ten-second metrics: %+v, window=%f", got.Get, window)
	}
	if *got.Get.P99NS < 3000 || *got.Get.P99NS > 3375 {
		t.Fatalf("merged p99=%f, want upper bucket bound near 3000", *got.Get.P99NS)
	}
	m.sample(start.Add(11 * time.Second))
	got, window = m.snapshot(start.Add(11*time.Second), true)
	if window != 10_000 || got.Get.Count != 900 || got.Get.OpsPerSecond != 90 || *got.Get.MeanNS != 3000 {
		t.Fatalf("expired first second: %+v, window=%f", got.Get, window)
	}
	for second := 12; second <= 22; second++ {
		m.sample(start.Add(time.Duration(second) * time.Second))
	}
	got, _ = m.snapshot(start.Add(22*time.Second), true)
	if got.Get.Count != 0 || got.Get.OpsPerSecond != 0 || got.Get.LatencySamples != 0 || got.Get.MeanNS != nil || got.Get.P99NS != nil {
		t.Fatalf("idle window retained old measurements: %+v", got.Get)
	}
	got, _ = m.snapshot(start.Add(22*time.Second), false)
	if got.Get.MeanNS != nil || got.Get.P99NS != nil {
		t.Fatal("disabled latency returned values")
	}
}

func TestOperationP99(t *testing.T) {
	start := time.Unix(100, 0)
	m := newOperationRecorder(1, start)
	c := &m.shards[0][operationGet]
	c.count.Store(100)
	c.sumNS.Store(99*100 + 10_000)
	c.histogram[operationBucket(100)].Store(99)
	c.histogram[operationBucket(10_000)].Store(1)
	got, _ := m.snapshot(start.Add(time.Second), true)
	if *got.Get.P99NS < 100 || *got.Get.P99NS > 112 {
		t.Fatalf("p99 included the top 1%%: %f", *got.Get.P99NS)
	}
	c.count.Add(1)
	c.sumNS.Add(10_000)
	c.histogram[operationBucket(10_000)].Add(1)
	got, _ = m.snapshot(start.Add(time.Second), true)
	if *got.Get.P99NS < 10_000 || *got.Get.P99NS > 11_250 {
		t.Fatalf("p99 missed the slow tail: %f", *got.Get.P99NS)
	}
}

func TestOperationHistogramPrecision(t *testing.T) {
	values := []uint64{0, 1, 15, 16, math.MaxInt64}
	for exponent := range 60 {
		for mantissa := uint64(8); mantissa < 16; mantissa++ {
			values = append(values, mantissa<<exponent, (mantissa+1)<<exponent-1)
		}
	}
	for _, ns := range values {
		bucket := operationBucket(ns)
		upper := operationBucketUpper(bucket)
		if bucket >= operationHistogramBuckets || upper < ns || upper-ns > ns/8 {
			t.Fatalf("duration %d mapped to bucket %d with upper bound %d", ns, bucket, upper)
		}
	}
}

func TestOperationMetricsConcurrent(t *testing.T) {
	db := newInternalDB(t, WithShardCount(4))
	h := db.MetricsHandler(WithMetricsHistory(0)).(*metricsHandler)
	_, stop := h.subscribe()
	defer stop()
	m := db.operationMetrics.Load()
	const workers, iterations = 8, 100
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			key := fmt.Sprintf("worker-%d", worker)
			for range iterations {
				if err := db.Update(key, func(v uint64, _ bool) (uint64, error) { return v + 1, nil }); err != nil {
					t.Error(err)
					return
				}
				if _, err := db.Get[uint64](key); err != nil {
					t.Error(err)
					return
				}
				if err := db.Delete(key); err != nil {
					t.Error(err)
					return
				}
				if err := db.Set(key, uint64(0)); err != nil {
					t.Error(err)
					return
				}
				for _, err := range db.Scan() {
					if err != nil {
						t.Error(err)
						return
					}
				}
			}
		})
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	for {
		select {
		case <-done:
			got, _ := m.snapshot(time.Now(), true)
			for _, metric := range []operationMetric{got.Get, got.Set, got.Delete, got.Update, got.Scan} {
				if metric.Count != workers*iterations || metric.LatencySamples != workers*iterations {
					t.Fatalf("concurrent calls lost: %+v", metric)
				}
			}
			return
		default:
			m.snapshot(time.Now(), true)
		}
	}
}

func TestOperationMetricsConcurrentViewers(t *testing.T) {
	db := newInternalDB(t, WithShardCount(2))
	handlers := []*metricsHandler{
		db.MetricsHandler(WithMetricsHistory(0), WithMetricsInterval(time.Millisecond)).(*metricsHandler),
		db.MetricsHandler(WithMetricsHistory(0), WithMetricsLatency(false)).(*metricsHandler),
	}
	stopCalls, callsDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(callsDone)
		for {
			select {
			case <-stopCalls:
				return
			default:
				db.Get[uint64]("missing")
			}
		}
	}()
	var viewers sync.WaitGroup
	for worker := range 4 {
		viewers.Go(func() {
			for range 20 {
				_, stop := handlers[worker%len(handlers)].subscribe()
				stop()
			}
		})
	}
	viewers.Wait()
	close(stopCalls)
	<-callsDone
	if db.operationMetrics.Load() != nil {
		t.Fatal("concurrent viewers left instrumentation enabled")
	}
}

func BenchmarkOperationMetrics(b *testing.B) {
	for _, mode := range []string{"disabled", "idle", "counts", "latency"} {
		for _, hit := range []bool{true, false} {
			b.Run(fmt.Sprintf("%s/hit=%t", mode, hit), func(b *testing.B) {
				f := newPrimitiveFixture(b)
				f.resetValue(b, true, uint64(42))
				if mode == "idle" {
					f.db.MetricsHandler() // Construct the default handler without viewers.
				} else if mode != "disabled" {
					m := f.db.acquireOperationMetrics(mode == "latency")
					defer f.db.releaseOperationMetrics(m, mode == "latency")
				}
				offset := 0
				var wantErr error
				if !hit {
					offset, wantErr = benchmarkKeyCount, ErrNotFound
				}
				b.StartTimer()
				for i := 0; b.Loop(); i++ {
					_, err := f.db.Get[uint64](f.keys[offset+i%benchmarkKeyCount][:])
					if err != wantErr {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
