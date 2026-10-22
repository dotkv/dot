package dot

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	metricsHistoryName             = "METRICS"
	metricsHistoryMagic            = "DOTMETRX"
	metricsHistoryFormatVersion    = 1
	defaultMetricsHistoryRetention = 24 * time.Hour
	metricsHistoryTempName         = "METRICS.tmp"
	metricsHistoryHeaderSize       = 64
	metricsHistoryRecordSize       = 256
	metricsHistoryBuckets          = 500 // At most 17,000 samples for all sixteen series.
)

// Each wall-clock second owns one slot. Timestamp and checksum validation let
// readers skip unwritten, expired, or torn records without replaying the file.
// This file is telemetry, independent of database recovery and durability.
type metricsHistory struct {
	retention time.Duration
	slots     int64
	mu        sync.RWMutex
	file      *os.File
	created   int64
	err       error
	last      metricsSnapshot
	updated   chan struct{} // Closed and replaced after each recorded snapshot.
	done      chan struct{}
	latency   atomic.Bool
}

func metricsHistoryFileSize(slots int64) int64 {
	return metricsHistoryHeaderSize + slots*metricsHistoryRecordSize
}

// The 64-byte header uses little-endian fields: [0:8] signature, [8:12]
// version, [16:24] creation time, [24:32] slots, [60:64] CRC32-IEEE.
// Unused bytes are reserved zeros. The version covers snapshot records too.
func initializeMetricsHistory(f *os.File, created, slots int64) error {
	var header [metricsHistoryHeaderSize]byte
	copy(header[:8], metricsHistoryMagic)
	binary.LittleEndian.PutUint32(header[8:12], metricsHistoryFormatVersion)
	binary.LittleEndian.PutUint64(header[16:24], uint64(created))
	binary.LittleEndian.PutUint64(header[24:32], uint64(slots))
	binary.LittleEndian.PutUint32(header[60:], crc32.ChecksumIEEE(header[:60]))
	if err := f.Truncate(metricsHistoryFileSize(slots)); err != nil {
		return err
	}
	_, err := f.WriteAt(header[:], 0)
	return err
}

func syncMetricsHistoryDirectory(path string) error {
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

// A changed ring capacity changes every record's offset. Rehash retained rows
// into a staging file, then publish it atomically so a crash keeps the old ring.
func resizeMetricsHistory(path string, source *os.File, oldSlots, slots, created int64, now time.Time) (*os.File, error) {
	staging := filepath.Join(filepath.Dir(path), metricsHistoryTempName)
	f, err := os.OpenFile(staging, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	published := false
	defer func() {
		if !published {
			f.Close()
			os.Remove(staging)
		}
	}()
	if err := initializeMetricsHistory(f, created, slots); err != nil {
		return nil, err
	}
	buffer := make([]byte, 3600*metricsHistoryRecordSize)
	cutoff := now.UnixMilli() - min(oldSlots, slots)*1000
	for second := max(created/1000, cutoff/1000); second <= now.Unix(); {
		slot := second % oldSlots
		n := min(int64(3600), oldSlots-slot, now.Unix()-second+1)
		data := buffer[:n*metricsHistoryRecordSize]
		if _, err := source.ReadAt(data, metricsHistoryHeaderSize+slot*metricsHistoryRecordSize); err != nil {
			return nil, err
		}
		for i := int64(0); i < n; i++ {
			record := data[i*metricsHistoryRecordSize : (i+1)*metricsHistoryRecordSize]
			at := int64(binary.LittleEndian.Uint64(record))
			if at/1000 != second+i || at < cutoff || at > now.UnixMilli() || binary.LittleEndian.Uint32(record[252:]) != crc32.ChecksumIEEE(record[:252]) {
				continue
			}
			offset := metricsHistoryHeaderSize + (at/1000%slots)*metricsHistoryRecordSize
			if _, err := f.WriteAt(record, offset); err != nil {
				return nil, err
			}
		}
		second += n
	}
	if err := f.Sync(); err != nil {
		return nil, err
	}
	if err := os.Rename(staging, path); err != nil {
		return nil, err
	}
	if err := syncMetricsHistoryDirectory(path); err != nil {
		return nil, err
	}
	published = true
	return f, nil
}

func openMetricsHistory(path string, now time.Time, retention time.Duration) *metricsHistory {
	h := &metricsHistory{retention: retention, slots: int64(retention / time.Second), created: now.UnixMilli(), updated: make(chan struct{}), done: make(chan struct{})}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		h.err = err
		return h
	}
	h.file = f
	var header [metricsHistoryHeaderSize]byte
	info, err := f.Stat()
	if err == nil {
		if info.Size() == 0 {
			err = initializeMetricsHistory(f, h.created, h.slots)
			if err == nil {
				err = f.Sync()
			}
			if err == nil {
				err = syncMetricsHistoryDirectory(path)
			}
		} else if _, err = f.ReadAt(header[:], 0); err == nil {
			version := binary.LittleEndian.Uint32(header[8:12])
			oldSlots := int64(binary.LittleEndian.Uint64(header[24:32]))
			if string(header[:8]) != metricsHistoryMagic || binary.LittleEndian.Uint32(header[60:]) != crc32.ChecksumIEEE(header[:60]) {
				err = errors.New("invalid metrics history header")
			} else if version != metricsHistoryFormatVersion {
				err = fmt.Errorf("unsupported metrics format version %d (want %d)", version, metricsHistoryFormatVersion)
			} else if binary.LittleEndian.Uint32(header[12:16]) != 0 || !bytes.Equal(header[32:60], make([]byte, 28)) {
				err = errors.New("invalid metrics history header: nonzero reserved bytes")
			} else if oldSlots < 1 || oldSlots > int64(time.Duration(1<<63-1)/time.Second) || info.Size() != metricsHistoryFileSize(oldSlots) {
				err = errors.New("invalid metrics history file size")
			} else {
				h.created = int64(binary.LittleEndian.Uint64(header[16:24]))
				if h.created <= 0 || h.created > now.UnixMilli() {
					err = errors.New("invalid metrics history creation time")
				} else if oldSlots != h.slots {
					var resized *os.File
					resized, err = resizeMetricsHistory(path, f, oldSlots, h.slots, h.created, now)
					if err == nil {
						f.Close()
						f = resized
						h.file = f
					}
				}
			}
		}
	}
	if err != nil {
		h.err = errors.Join(err, f.Close())
		h.file = nil
		h.created = now.UnixMilli()
	}
	return h
}

func (db *DB) startMetricsHistory(retention time.Duration, latency bool) *metricsHistory {
	db.metricsHistoryMu.Lock()
	defer db.metricsHistoryMu.Unlock()
	if db.metricsContext.Err() != nil {
		return nil
	}
	h := db.metricsHistory
	if h == nil {
		h = openMetricsHistory(filepath.Join(db.path, metricsHistoryName), time.Now(), retention)
		db.metricsHistory = h
		// These registrations belong to the DB lifetime, rather than a viewer.
		m := db.acquireOperationMetrics(false)
		if latency {
			db.acquireOperationMetrics(true)
			h.latency.Store(true)
		}
		go h.run(db, m)
	}
	if h.retention != retention {
		panic("dot: history-enabled handlers must use the same retention for a database")
	}
	if latency && !h.latency.Load() {
		db.acquireOperationMetrics(true)
		h.latency.Store(true)
	}
	return h
}

func (h *metricsHistory) run(db *DB, operations *operationRecorder) {
	defer close(h.done)
	defer func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.file != nil {
			h.err = errors.Join(h.err, h.file.Sync(), h.file.Close())
			h.file = nil
		}
	}()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	cache := metricsCache{shards: make([]metricsShard, len(db.shards)), operations: operations}
	lastSync := time.Now()
	for {
		if db.metricsContext.Err() != nil {
			return
		}
		sampler := metricsHandler{db: db, interval: time.Second, latency: h.latency.Load()}
		snapshot := sampler.snapshot(&cache)
		h.append(snapshot)
		if time.Since(lastSync) >= time.Minute {
			h.mu.Lock()
			if h.file != nil {
				h.err = h.file.Sync()
			}
			h.mu.Unlock()
			lastSync = time.Now()
		}
		select {
		case <-db.metricsContext.Done():
			return
		case <-ticker.C:
		}
	}
}

func (h *metricsHistory) status(now time.Time) (int64, string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var message string
	if h.err != nil {
		message = "Metrics history is unavailable: " + h.err.Error()
	}
	return max(h.created, now.Add(-h.retention).UnixMilli()), message
}

func encodeMetricsHistory(s metricsSnapshot) [metricsHistoryRecordSize]byte {
	var record [metricsHistoryRecordSize]byte
	fields := []uint64{uint64(s.Time), uint64(s.IntervalMS), s.DiskBytes, s.ObsoleteBytes, s.EntryCount, s.IndexMappedBytes, math.Float64bits(s.OperationWindowMS)}
	for i, field := range fields {
		binary.LittleEndian.PutUint64(record[i*8:], field)
	}
	binary.LittleEndian.PutUint32(record[56:], uint32(s.StaleShards))
	for i, flag := range []bool{s.DiskStale, s.Ready, s.LatencyEnabled} {
		if flag {
			record[60] |= 1 << i
		}
	}
	for i, op := range []operationMetric{s.Operations.Get, s.Operations.Set, s.Operations.Delete, s.Operations.Update} {
		mean, p99 := math.NaN(), math.NaN()
		if op.MeanNS != nil {
			mean = *op.MeanNS
		}
		if op.P99NS != nil {
			p99 = *op.P99NS
		}
		for j, field := range []uint64{op.Count, math.Float64bits(op.OpsPerSecond), op.LatencySamples, math.Float64bits(mean), math.Float64bits(p99)} {
			binary.LittleEndian.PutUint64(record[64+i*40+j*8:], field)
		}
	}
	binary.LittleEndian.PutUint32(record[252:], crc32.ChecksumIEEE(record[:252]))
	return record
}

func decodeMetricsHistory(record []byte) (metricsSnapshot, bool) {
	var s metricsSnapshot
	if binary.LittleEndian.Uint32(record[252:]) != crc32.ChecksumIEEE(record[:252]) {
		return s, false
	}
	s.Time = int64(binary.LittleEndian.Uint64(record))
	s.IntervalMS = int64(binary.LittleEndian.Uint64(record[8:]))
	s.DiskBytes = binary.LittleEndian.Uint64(record[16:])
	s.ObsoleteBytes = binary.LittleEndian.Uint64(record[24:])
	s.EntryCount = binary.LittleEndian.Uint64(record[32:])
	s.IndexMappedBytes = binary.LittleEndian.Uint64(record[40:])
	s.OperationWindowMS = math.Float64frombits(binary.LittleEndian.Uint64(record[48:]))
	s.StaleShards = int(binary.LittleEndian.Uint32(record[56:]))
	s.DiskStale, s.Ready, s.LatencyEnabled = record[60]&1 != 0, record[60]&2 != 0, record[60]&4 != 0
	validNumber := func(f float64) bool { return !math.IsNaN(f) && !math.IsInf(f, 0) && f >= 0 }
	if s.Time <= 0 || s.IntervalMS != 1000 || !validNumber(s.OperationWindowMS) {
		return s, false
	}
	for i, op := range []*operationMetric{&s.Operations.Get, &s.Operations.Set, &s.Operations.Delete, &s.Operations.Update} {
		data := record[64+i*40:]
		op.Count = binary.LittleEndian.Uint64(data)
		op.OpsPerSecond = math.Float64frombits(binary.LittleEndian.Uint64(data[8:]))
		op.LatencySamples = binary.LittleEndian.Uint64(data[16:])
		if !validNumber(op.OpsPerSecond) {
			return s, false
		}
		for j, dest := range []**float64{&op.MeanNS, &op.P99NS} {
			value := math.Float64frombits(binary.LittleEndian.Uint64(data[24+j*8:]))
			if !math.IsNaN(value) {
				if !validNumber(value) {
					return s, false
				}
				*dest = &value
			}
		}
	}
	return s, true
}

func (h *metricsHistory) append(s metricsSnapshot) {
	record := encodeMetricsHistory(s)
	offset := int64(metricsHistoryHeaderSize) + (s.Time/1000%h.slots)*metricsHistoryRecordSize
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.file != nil {
		if _, err := h.file.WriteAt(record[:], offset); err != nil {
			h.err = err
		}
	}
	// Publish only after the write, so a viewer can find this exact reading in
	// history immediately. Continue publishing if persistence is unavailable.
	h.last = s
	close(h.updated)
	h.updated = make(chan struct{})
}

type metricsHistoryResponse struct {
	Start        int64             `json:"start"`
	End          int64             `json:"end"`
	ResolutionMS int64             `json:"resolution_ms"`
	Samples      []metricsSnapshot `json:"samples"`
}

// historyRecordValue follows the sixteen series in the UI. Compare encoded
// values so a week-wide query only allocates latency pointers for retained rows.
func historyRecordValue(record []byte, key int) float64 {
	if key < 4 {
		if record[60]&2 == 0 {
			return math.NaN()
		}
		return float64(binary.LittleEndian.Uint64(record[16+key*8:]))
	}
	op, field := (key-4)/3, (key-4)%3
	offset := 64 + op*40 + []int{8, 24, 32}[field]
	return math.Float64frombits(binary.LittleEndian.Uint64(record[offset:]))
}

var metricsHistoryKeys = []string{"disk_bytes", "obsolete_bytes", "entry_count", "index_mapped_bytes", "get_ops", "get_mean_ns", "get_p99_ns", "set_ops", "set_mean_ns", "set_p99_ns", "delete_ops", "delete_mean_ns", "delete_p99_ns", "update_ops", "update_mean_ns", "update_p99_ns"}

func (h *metricsHandler) serveHistory(w http.ResponseWriter, r *http.Request) {
	if h.db.metricsContext.Err() != nil {
		http.Error(w, "database is closed", http.StatusServiceUnavailable)
		return
	}
	if h.history == nil {
		http.NotFound(w, r)
		return
	}
	now := time.Now().UnixMilli()
	start, errStart := strconv.ParseInt(r.URL.Query().Get("start"), 10, 64)
	end, errEnd := strconv.ParseInt(r.URL.Query().Get("end"), 10, 64)
	if errStart != nil || errEnd != nil || start < 0 || end < start {
		http.Error(w, "invalid history range", http.StatusBadRequest)
		return
	}
	start, end = max(start, now-h.history.retention.Milliseconds()), min(end, now)
	var keys []int
	for _, name := range strings.Split(r.URL.Query().Get("metrics"), ",") {
		key := slices.Index(metricsHistoryKeys, name)
		if key < 0 {
			http.Error(w, "invalid history metric", http.StatusBadRequest)
			return
		}
		if !slices.Contains(keys, key) {
			keys = append(keys, key)
		}
	}
	result, err := h.history.query(r.Context(), start, end, keys)
	if err != nil {
		http.Error(w, "metrics history unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodHead {
		_ = json.NewEncoder(w).Encode(result)
	}
}

func (h *metricsHistory) query(ctx context.Context, start, end int64, keys []int) (metricsHistoryResponse, error) {
	start = max(start, time.Now().Add(-h.retention).UnixMilli(), h.created)
	end = min(end, time.Now().UnixMilli())
	result := metricsHistoryResponse{Start: start, End: end, ResolutionMS: max(1000, (end-start)/metricsHistoryBuckets+1), Samples: []metricsSnapshot{}}
	type reading struct {
		record [metricsHistoryRecordSize]byte
		gaps   uint64
	}
	var bucket []reading
	var minima, maxima [16]int
	var lowValues, highValues [16]float64
	var previousTime int64
	var gaps, previousGaps uint64
	bucketID := int64(-1)
	flush := func() {
		if len(bucket) == 0 {
			return
		}
		indices := []int{0, len(bucket) - 1}
		for _, key := range keys {
			indices = append(indices, minima[key], maxima[key])
		}
		slices.Sort(indices)
		for _, i := range slices.Compact(indices) {
			if sample, ok := decodeMetricsHistory(bucket[i].record[:]); ok {
				// Detect missing time before downsampling. If a bucket contains an
				// outage, never connect retained points across it. The displayed
				// break can be wider by up to one bucket in an overview.
				sample.GapBefore = bucket[i].gaps > previousGaps
				previousGaps = bucket[i].gaps
				result.Samples = append(result.Samples, sample)
			}
		}
		bucket = bucket[:0]
	}
	buffer := make([]byte, 3600*metricsHistoryRecordSize)
	for second := start / 1000; second <= end/1000; {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		slot := second % h.slots
		n := min(int64(3600), h.slots-slot, end/1000-second+1)
		data := buffer[:n*metricsHistoryRecordSize]
		h.mu.RLock()
		var err error
		if h.file == nil {
			err = errors.New("metrics history is closed or unavailable")
		} else {
			_, err = h.file.ReadAt(data, metricsHistoryHeaderSize+slot*metricsHistoryRecordSize)
		}
		h.mu.RUnlock()
		if err != nil {
			return result, err
		}
		for i := int64(0); i < n; i++ {
			record := data[i*metricsHistoryRecordSize : (i+1)*metricsHistoryRecordSize]
			at := int64(binary.LittleEndian.Uint64(record))
			if at/1000 != second+i || at < start || at > end || binary.LittleEndian.Uint32(record[252:]) != crc32.ChecksumIEEE(record[:252]) {
				continue
			}
			if previousTime > 0 && at-previousTime > 3000 {
				gaps++
			}
			previousTime = at
			id := (at - start) / result.ResolutionMS
			if id != bucketID {
				flush()
				bucketID = id
				clear(minima[:])
				clear(maxima[:])
			}
			for _, key := range keys {
				value := historyRecordValue(record, key)
				if len(bucket) == 0 {
					lowValues[key], highValues[key] = value, value
				} else {
					if value < lowValues[key] || math.IsNaN(lowValues[key]) {
						minima[key], lowValues[key] = len(bucket), value
					}
					if value > highValues[key] || math.IsNaN(highValues[key]) {
						maxima[key], highValues[key] = len(bucket), value
					}
				}
			}
			bucket = append(bucket, reading{record: [metricsHistoryRecordSize]byte(record), gaps: gaps})
		}
		second += n
	}
	flush()
	return result, nil
}
