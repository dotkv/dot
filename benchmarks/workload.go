package main

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
)

var workloads = map[string]int{"read-heavy": 95, "mixed": 50, "write-heavy": 5}

type operationKind int

const (
	readHit operationKind = iota
	readMiss
	insert
	update
	remove
	kindCount
)

var operationNames = [kindCount]string{"read_hit", "read_miss", "insert", "update", "delete"}

type liveKey struct{ id, version uint64 }
type operation struct {
	kind  operationKind
	entry liveKey
}

type latency struct {
	count uint64
	total time.Duration
	hist  *hdr.Histogram
}

func newLatency() latency {
	// Two significant figures bound percentile quantization to about 1% while
	// keeping per-worker, per-operation histograms small. Never drop samples.
	return latency{hist: hdr.New(1, int64(time.Hour), 2)}
}

func (l *latency) record(elapsed time.Duration) error {
	if err := l.hist.RecordValue(max(1, elapsed.Nanoseconds())); err != nil {
		return fmt.Errorf("record latency %s: %w", elapsed, err)
	}
	l.count++
	l.total += elapsed
	return nil
}

type worker struct {
	rng         *rand.Rand
	live        []liveKey
	nextID      uint64
	idStride    uint64
	seed        uint64
	writeCount  uint64
	readPercent int
	scalar      bool
	key         [16]byte
	payload     []byte
	stats       [kindCount]latency
}

func newWorker(c caseConfig, id int) *worker {
	n := share(c.Keys, c.Workers, id)
	w := &worker{
		rng:         rand.New(rand.NewPCG(c.Seed, uint64(id)+1)),
		live:        make([]liveKey, n, n+1),
		nextID:      uint64(n*c.Workers + id),
		idStride:    uint64(c.Workers),
		seed:        c.Seed,
		readPercent: workloads[c.Workload],
		scalar:      c.Scalar && c.ValueBytes == 8,
		payload:     make([]byte, c.ValueBytes),
	}
	for i := range w.live {
		w.live[i].id = uint64(i*c.Workers + id)
	}
	// Generate once outside measurement. Values are not compressible runs of
	// zeros; their first eight bytes identify each key and revision.
	for i := 0; i < len(w.payload); i += 8 {
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], mix64(c.Seed+uint64(id)+uint64(i)))
		copy(w.payload[i:], encoded[:])
	}
	for i := range w.stats {
		w.stats[i] = newLatency()
	}
	return w
}

func share(total, workers, id int) int {
	n := total / workers
	if id < total%workers {
		n++
	}
	return n
}

func mix64(x uint64) uint64 {
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func (w *worker) prepare(op operation) uint64 {
	// The second word separates guaranteed misses from the live key space.
	// The first word permutes IDs so keys are not inserted in sorted order.
	binary.LittleEndian.PutUint64(w.key[:8], mix64(op.entry.id^w.seed))
	clear(w.key[8:])
	if op.kind == readMiss {
		w.key[8] = 1
	}
	value := mix64(op.entry.id^w.seed) + op.entry.version
	binary.LittleEndian.PutUint64(w.payload[:8], value)
	return value
}

func (w *worker) next() operation {
	if w.rng.IntN(100) < w.readPercent {
		op := operation{kind: readHit, entry: w.live[w.rng.IntN(len(w.live))]}
		if w.rng.IntN(100) >= 95 {
			op.kind = readMiss
		}
		return op
	}
	// Cycling mutations keeps insert/update/delete counts within one of each
	// other, guarantees real mutations, and bounds live keys to n or n+1.
	kind := insert + operationKind(w.writeCount%3)
	w.writeCount++
	if kind == insert {
		entry := liveKey{id: w.nextID}
		w.nextID += w.idStride
		w.live = append(w.live, entry)
		return operation{kind: kind, entry: entry}
	}
	i := w.rng.IntN(len(w.live))
	if kind == update {
		w.live[i].version++
		return operation{kind: kind, entry: w.live[i]}
	}
	op := operation{kind: kind, entry: w.live[i]}
	w.live[i] = w.live[len(w.live)-1]
	w.live = w.live[:len(w.live)-1]
	return op
}

// execute includes conversion/copying required by the adapter's API contract.
// Trace generation and correctness checks happen outside latency measurement.
func (w *worker) execute(db engine, op operation) (uint64, bool, int, error) {
	switch op.kind {
	case readHit, readMiss:
		if w.scalar {
			v, found, err := db.GetUint64(w.key[:])
			return v, found, 8, err
		}
		v, found, err := db.Get(w.key[:])
		var bits uint64
		if len(v) >= 8 {
			bits = binary.LittleEndian.Uint64(v)
		}
		return bits, found, len(v), err
	case insert, update:
		if w.scalar {
			return 0, false, 0, db.SetUint64(w.key[:], binary.LittleEndian.Uint64(w.payload[:8]))
		}
		return 0, false, 0, db.Set(w.key[:], w.payload)
	case remove:
		return 0, false, 0, db.Delete(w.key[:])
	default:
		panic("unknown operation")
	}
}

func (w *worker) step(db engine, measured bool) error {
	op := w.next()
	want := w.prepare(op)
	var start time.Time
	if measured {
		start = time.Now()
	}
	got, found, size, err := w.execute(db, op)
	if measured {
		if recordErr := w.stats[op.kind].record(time.Since(start)); recordErr != nil {
			return recordErr
		}
	}
	if err != nil {
		return fmt.Errorf("%s: %w", operationNames[op.kind], err)
	}
	if op.kind == readHit && (!found || got != want || size != len(w.payload)) {
		return fmt.Errorf("read hit: found=%v, value=%d, bytes=%d; want %d, %d bytes", found, got, size, want, len(w.payload))
	}
	if op.kind == readMiss && found {
		return fmt.Errorf("read miss unexpectedly found key")
	}
	return nil
}
