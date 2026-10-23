package dot

import (
	"math/bits"
	"sync"
	"sync/atomic"
	"time"
)

const (
	operationGet = iota
	operationSet
	operationDelete
	operationUpdate
	operationScan
	operationCount

	operationHistogramBuckets = 512
	operationWindow           = 10 * time.Second
)

type operationMetric struct {
	Count          uint64   `json:"count"`
	OpsPerSecond   float64  `json:"ops_per_second"`
	LatencySamples uint64   `json:"latency_samples"`
	MeanNS         *float64 `json:"mean_ns"`
	P99NS          *float64 `json:"p99_ns"`
}

type operationMetrics struct {
	Get    operationMetric `json:"get"`
	Set    operationMetric `json:"set"`
	Delete operationMetric `json:"delete"`
	Update operationMetric `json:"update"`
	Scan   operationMetric `json:"scan"`
}

type operationCounter struct {
	count     atomic.Uint64
	sumNS     atomic.Uint64
	histogram [operationHistogramBuckets]atomic.Uint64
	// Separate hot counters for different operations and shards on cache lines.
	_ [48]byte
}

type operationTotals struct {
	count, sumNS uint64
	histogram    [operationHistogramBuckets]uint64
}

type operationReading struct {
	at     time.Time
	totals [operationCount]operationTotals
}

// Foreground calls update cumulative counters. Readers subtract an older
// reading, so rotating the window never resets a counter beneath a writer.
type operationRecorder struct {
	shards  [][operationCount]operationCounter
	latency atomic.Bool
	stop    chan struct{}
	done    chan struct{}

	users, latencyUsers int // Guarded by DB.metricsMu.
	mu                  sync.Mutex
	history             [12]operationReading // Ten seconds plus boundary readings.
	head, size          int
}

func newOperationRecorder(shards int, now time.Time) *operationRecorder {
	m := &operationRecorder{
		shards: make([][operationCount]operationCounter, max(1, shards)),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		size:   1,
	}
	m.history[0].at = now
	return m
}

func (db *DB) acquireOperationMetrics(latency bool) *operationRecorder {
	db.metricsMu.Lock()
	defer db.metricsMu.Unlock()
	if db.metricsContext.Err() != nil {
		return nil
	}
	m := db.operationMetrics.Load()
	if m == nil {
		m = newOperationRecorder(len(db.shards), time.Now())
		m.latency.Store(latency)
		db.operationMetrics.Store(m)
		go m.run()
	}
	m.users++
	if latency {
		m.latencyUsers++
		m.latency.Store(true)
	}
	return m
}

func (db *DB) releaseOperationMetrics(m *operationRecorder, latency bool) {
	db.metricsMu.Lock()
	defer db.metricsMu.Unlock()
	if m == nil || db.operationMetrics.Load() != m {
		return // Close already stopped this session.
	}
	m.users--
	if latency {
		m.latencyUsers--
		m.latency.Store(m.latencyUsers > 0)
	}
	if m.users == 0 {
		db.operationMetrics.Store(nil)
		close(m.stop)
	}
}

func (db *DB) stopOperationMetrics() {
	db.metricsMu.Lock()
	m := db.operationMetrics.Swap(nil)
	if m != nil {
		close(m.stop)
	}
	db.metricsMu.Unlock()
	if m != nil {
		<-m.done
	}
}

func (m *operationRecorder) begin() time.Time {
	if m.latency.Load() {
		return time.Now()
	}
	return time.Time{}
}

func (m *operationRecorder) record(kind int, shard *Shard, started time.Time) {
	index := 0 // Encoding errors can return before a shard is selected.
	if shard != nil {
		index = shard.metricsIndex
	}
	c := &m.shards[index][kind]
	if !started.IsZero() {
		elapsed := uint64(max(0, time.Since(started)))
		c.sumNS.Add(elapsed)
		c.histogram[operationBucket(elapsed)].Add(1)
	}
	c.count.Add(1)
}

// Eight subdivisions per power of two bound p99's rounding up to 12.5%.
// Durations below 16 ns have exact buckets. No duration samples are retained.
func operationBucket(ns uint64) int {
	exponent := max(0, bits.Len64(ns)-4)
	return exponent*8 + int(ns>>uint(exponent))
}

func operationBucketUpper(bucket int) uint64 {
	if bucket < 16 {
		return uint64(bucket)
	}
	return (uint64(bucket%8+9) << uint(bucket/8-1)) - 1
}

func (m *operationRecorder) read(now time.Time) operationReading {
	r := operationReading{at: now}
	for i := range m.shards {
		for kind := range r.totals {
			c, total := &m.shards[i][kind], &r.totals[kind]
			total.count += c.count.Load()
			total.sumNS += c.sumNS.Load()
			for bucket := range total.histogram {
				total.histogram[bucket] += c.histogram[bucket].Load()
			}
		}
	}
	return r
}

func (m *operationRecorder) run() {
	defer close(m.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-ticker.C:
			m.sample(time.Now())
		}
	}
}

func (m *operationRecorder) sample(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	i := (m.head + m.size) % len(m.history)
	m.history[i] = m.read(now)
	if m.size < len(m.history) {
		m.size++
	} else {
		m.head = (m.head + 1) % len(m.history)
	}
}

func (m *operationRecorder) snapshot(now time.Time, latency bool) (operationMetrics, float64) {
	if m == nil {
		return operationMetrics{}, 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.read(now)
	baseline := &m.history[m.head]
	cutoff := now.Add(-operationWindow)
	for i := 1; i < m.size; i++ {
		reading := &m.history[(m.head+i)%len(m.history)]
		if reading.at.After(cutoff) {
			break
		}
		baseline = reading
	}
	window := max(0, now.Sub(baseline.at))
	var result operationMetrics
	for kind, metric := range []*operationMetric{&result.Get, &result.Set, &result.Delete, &result.Update, &result.Scan} {
		total, before := &current.totals[kind], &baseline.totals[kind]
		metric.Count = total.count - before.count
		if window > 0 {
			metric.OpsPerSecond = float64(metric.Count) / window.Seconds()
		}
		if !latency {
			continue
		}
		for bucket := range total.histogram {
			total.histogram[bucket] -= before.histogram[bucket]
			metric.LatencySamples += total.histogram[bucket]
		}
		if metric.LatencySamples == 0 {
			continue // Empty windows have unknown latency, not zero latency.
		}
		mean := float64(total.sumNS-before.sumNS) / float64(metric.LatencySamples)
		metric.MeanNS = &mean
		rank := metric.LatencySamples - metric.LatencySamples/100 // ceil(0.99*n)
		var seen uint64
		for bucket, count := range total.histogram {
			seen += count
			if seen >= rank {
				p99 := float64(operationBucketUpper(bucket))
				metric.P99NS = &p99
				break
			}
		}
	}
	return result, float64(window) / float64(time.Millisecond)
}
