package dot

import "testing"

func TestMetricsIndexMappedBytes(t *testing.T) {
	db := newInternalDB(t, WithShardCount(1))
	stopTestMaintenance(db)
	h := db.MetricsHandler().(*metricsHandler)
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
