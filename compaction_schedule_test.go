package dot

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func waitForCompaction(t *testing.T, s *Shard, records uint64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		s.lock.RLock()
		done := !s.log.compactPending && !s.compactionQueued && s.log.recordCount == records
		s.lock.RUnlock()
		if done {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("background compaction did not complete before the expiry ticker")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestStateCompactionMutations(t *testing.T) {
	for _, mutation := range []string{"set", "update", "delete", "expired-delete"} {
		t.Run(mutation, func(t *testing.T) {
			db := newInternalDB(t, WithCompactionThreshold(0.2, 2))
			key := []byte("key")
			s, _ := db.shard(key)
			if err := db.Set(key, uint64(1)); err != nil {
				t.Fatal(err)
			}
			if mutation == "delete" {
				if err := db.Delete(key); err != nil {
					t.Fatal(err)
				}
				waitForCompaction(t, s, 0)
				if _, err := db.Get[uint64](key); err != ErrNotFound {
					t.Fatalf("deleted key: %v", err)
				}
				return
			}
			for i := uint64(2); i <= 3; i++ {
				var err error
				switch mutation {
				case "set":
					err = db.Set(key, i)
				case "update":
					err = db.Update(key, func(v uint64) uint64 { return v + 1 })
				case "expired-delete":
					if i == 2 {
						err = db.Set(key, i, WithExpiration(time.Now().Add(-time.Second)))
					} else {
						err = db.Delete(key)
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				if i == 2 {
					s.lock.RLock()
					queued, count := s.compactionQueued, s.log.recordCount
					s.lock.RUnlock()
					if queued || count != 2 {
						t.Fatal("compaction ran below the configured obsolete-record minimum")
					}
				}
			}
			records := uint64(1)
			if mutation == "expired-delete" {
				records = 0
			}
			waitForCompaction(t, s, records)
			if mutation != "expired-delete" {
				if got, err := db.Get[uint64](key); err != nil || got != 3 {
					t.Fatalf("compacted value=%d error=%v", got, err)
				}
			}
			s.lock.RLock()
			checkSegmentAccounting(t, s)
			s.lock.RUnlock()
		})
	}
}

func TestStateCompactionRatio(t *testing.T) {
	db := newInternalDB(t, WithCompactionThreshold(0.8, 2))
	key := []byte("key")
	s, _ := db.shard(key)
	for value := uint64(0); value < 5; value++ {
		if err := db.Set(key, value); err != nil {
			t.Fatal(err)
		}
	}
	s.lock.RLock()
	queued, records := s.compactionQueued, s.log.recordCount
	s.lock.RUnlock()
	if queued || records != 5 {
		t.Fatal("compaction ran before exceeding the configured ratio")
	}
	// Four of five records are obsolete: exactly 80% is below the strict trigger.
	if err := db.Set(key, uint64(5)); err != nil {
		t.Fatal(err)
	}
	waitForCompaction(t, s, 1)
}

func TestCompactionThresholdValidation(t *testing.T) {
	for _, ratio := range []float64{-0.1, 1, 1.1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		path := filepath.Join(t.TempDir(), "uncreated")
		if db, err := Open(path, WithCompactionThreshold(ratio, 1)); err == nil || db != nil {
			if db != nil {
				db.Close()
			}
			t.Fatalf("accepted ratio %v", ratio)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("invalid options modified the filesystem: %v", err)
		}
	}
	// Last option wins, and a zero floor is supported.
	db := newInternalDB(t, WithCompactionThreshold(0.8, 100), WithCompactionThreshold(0, 0))
	key := []byte("key")
	for _, value := range []uint64{1, 2} {
		if err := db.Set(key, value, WithCompactionThreshold(0.99, 1<<60)); err != nil {
			t.Fatal(err)
		}
	}
	s, _ := db.shard(key)
	waitForCompaction(t, s, 1)
}

func TestCompactionDisabledOverridesThreshold(t *testing.T) {
	db := newInternalDB(t, WithCompaction(false), WithCompactionThreshold(0, 0))
	key := []byte("key")
	for i := range 10 {
		if err := db.Set(key, uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	s, _ := db.shard(key)
	if s.log.recordCount != 10 || s.compactionQueued || s.log.compactPending {
		t.Fatal("disabled database scheduled automatic compaction")
	}
}

func TestTickerCompactionRequiresExpirations(t *testing.T) {
	db := newInternalDB(t, WithCompaction(false))
	var shards [3]*Shard
	for i := range shards {
		s := &db.shards[i]
		shards[i] = s
		key := keysForShard(db, s, "sweep", 1)[0]
		if err := db.Set(key, uint64(1)); err != nil {
			t.Fatal(err)
		}
		var opts []Option
		if i == 1 {
			opts = []Option{WithExpiration(time.Now().Add(-time.Second))}
		}
		if err := db.Set(key, uint64(2), opts...); err != nil {
			t.Fatal(err)
		}
	}
	// Recovery and failed passes remain retryable even without any TTL entries.
	shards[2].log.compactPending = true
	if err := db.compactExpirations(context.Background()); err != nil {
		t.Fatal(err)
	}
	if shards[0].log.recordCount != 2 || shards[1].log.recordCount != 0 || len(shards[1].index.expirations) != 0 || shards[2].log.recordCount != 1 {
		t.Fatal("ticker did not skip ordinary history and clean expiry/pending work")
	}
}

func TestCompactionRecoverySchedulesWithoutWrites(t *testing.T) {
	for _, history := range []string{"head", "packing", "expired"} {
		t.Run(history, func(t *testing.T) {
			path := t.TempDir()
			db, err := Open(path, WithCompaction(false))
			if err != nil {
				t.Fatal(err)
			}
			key := []byte("key")
			s, hash := db.shard(key)
			id := int(hash >> (64 - shardBits))
			for _, value := range []uint64{1, 2, 3} {
				var opts []Option
				if history == "expired" {
					opts = []Option{WithExpiration(time.Now().Add(-time.Second))}
				}
				if err := db.Set(key, value, opts...); err != nil {
					t.Fatal(err)
				}
			}
			if history == "packing" {
				if err := s.log.openHead(1); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			minimum := uint64(2)
			if history != "head" {
				minimum = 100 // Pending cleanup overrides the state-based floor.
			}
			reopened, err := Open(path, WithCompactionThreshold(0.2, minimum))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			})
			want := uint64(1)
			if history == "expired" {
				want = 0
			}
			waitForCompaction(t, &reopened.shards[id], want)
		})
	}
}

func TestCompactionFailureRetriesOnTicker(t *testing.T) {
	db := newInternalDB(t, WithCompaction(false), WithCompactionThreshold(0, 1))
	// Capture queued jobs without starting a worker, so the failure and retry
	// can be driven deterministically through the production worker paths.
	queue := make(chan *Shard, shardCount)
	key := []byte("key")
	s, _ := db.shard(key)
	s.compactionQueue = queue
	if err := db.Set(key, uint64(1)); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(key, uint64(2)); err != nil {
		t.Fatal(err)
	}
	blocker := s.log.segmentPath(0) + ".compact"
	if err := os.Mkdir(blocker, 0700); err != nil {
		t.Fatal(err)
	}
	queued := <-queue
	if err := queued.compactQueued(); err == nil {
		t.Fatal("compaction unexpectedly succeeded")
	}
	if !s.log.compactPending || !s.compactionRetry || s.compactionQueued {
		t.Fatal("failed pass did not enter deferred retry state")
	}
	if err := db.Set(key, uint64(3)); err != nil {
		t.Fatal(err)
	}
	if len(queue) != 0 {
		t.Fatal("write retried a failed compaction immediately")
	}
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	if err := db.compactExpirations(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.log.recordCount != 1 || s.log.compactPending || s.compactionRetry {
		t.Fatal("ticker did not finish pending cleanup without expirations")
	}
}

func TestCompactionQueueCoalesces(t *testing.T) {
	db := newInternalDB(t, WithCompaction(false), WithCompactionThreshold(0, 0))
	queue := make(chan *Shard, shardCount)
	var wg sync.WaitGroup
	for i := range db.shards {
		s := &db.shards[i]
		s.compactionQueue = queue
		key := keysForShard(db, s, "queued", 1)[0]
		wg.Go(func() {
			for j := range 10 {
				if err := db.Set(key, uint64(j)); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	if len(queue) != shardCount {
		t.Fatalf("queued %d jobs, want one per shard", len(queue))
	}
	// The timer must not clear queued flags or duplicate work before dequeue.
	if err := db.compactExpirations(context.Background()); err != nil {
		t.Fatal(err)
	}
	for range shardCount {
		s := <-queue
		if err := s.compactQueued(); err != nil {
			t.Fatal(err)
		}
		checkSegmentAccounting(t, s)
	}
	if len(queue) != 0 {
		t.Fatal("duplicate compaction requests remained")
	}
}

func TestStateCompactionConcurrentUpdatesAndClose(t *testing.T) {
	db := newInternalDB(t, WithCompactionThreshold(0.2, 8))
	const shards, workers, updates = 4, 4, 100
	keys := make([][]byte, shards)
	var wg sync.WaitGroup
	for i := range shards {
		key := keysForShard(db, &db.shards[i], "counter", 1)[0]
		keys[i] = key
		if err := db.Set(key, uint64(0)); err != nil {
			t.Fatal(err)
		}
		for range workers {
			wg.Go(func() {
				for range updates {
					if err := db.Update(key, func(v uint64) uint64 { return v + 1 }); err != nil {
						t.Error(err)
						return
					}
				}
			})
		}
	}
	wg.Wait()
	// Close may leave queued passes unprocessed; all acknowledged updates must
	// remain recoverable after the running pass finishes and the worker exits.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(db.path, WithCompaction(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	var records uint64
	for i, key := range keys {
		if got, err := reopened.Get[uint64](key); err != nil || got != workers*updates {
			t.Fatalf("recovered counter %d: value=%d error=%v", i, got, err)
		}
		s := &reopened.shards[i]
		records += s.log.recordCount
		checkSegmentAccounting(t, s)
	}
	if records >= shards*(1+workers*updates) {
		t.Fatal("concurrent updates never ran a background compaction")
	}
}
