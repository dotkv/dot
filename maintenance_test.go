package dot

import (
	"bytes"
	"context"
	"fmt"
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
			minimum := 2 * recordSize(len("key"), 8)
			if mutation == "delete" {
				minimum = recordSize(len("key"), 8) + recordSize(len("key"), 0)
			}
			db := newInternalDB(t, WithCompactionThreshold(0.2, minimum))
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
					err = db.Update(key, func(v uint64, _ bool) (uint64, error) { return v + 1, nil })
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
						t.Fatal("compaction ran below the configured obsolete-byte minimum")
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
	db := newInternalDB(t, WithCompactionThreshold(0.8, 2*recordSize(len("key"), 8)))
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
	// Four of five equal-size records are obsolete: exactly 80% is below the strict trigger.
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

func TestTickerCompactionRequiresExpirations(t *testing.T) {
	db := newInternalDB(t)
	stopTestMaintenance(db)
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
			db, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			stopTestMaintenance(db)
			key := []byte("key")
			s, hash := db.shard(key)
			id := int(hash >> (64 - defaultShardCountBits))
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
			minimum := 2 * recordSize(len(key), 8)
			if history != "head" {
				minimum = 100 * recordSize(len(key), 8) // Pending cleanup overrides the state-based floor.
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
	db := newInternalDB(t, WithCompactionThreshold(0, 1))
	stopTestMaintenance(db)
	// Capture queued jobs without starting a worker, so the failure and retry
	// can be driven deterministically through the production worker paths.
	queue := make(chan *Shard, defaultShardCount)
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
	db := newInternalDB(t, WithCompactionThreshold(0, 0))
	stopTestMaintenance(db)
	queue := make(chan *Shard, defaultShardCount)
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
	if len(queue) != defaultShardCount {
		t.Fatalf("queued %d jobs, want one per shard", len(queue))
	}
	// The timer must not clear queued flags or duplicate work before dequeue.
	if err := db.compactExpirations(context.Background()); err != nil {
		t.Fatal(err)
	}
	for range defaultShardCount {
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
	db := newInternalDB(t, WithCompactionThreshold(0.2, 256))
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
					if err := db.Update(key, func(v uint64, _ bool) (uint64, error) { return v + 1, nil }); err != nil {
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
	reopened, err := Open(db.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	stopTestMaintenance(reopened)
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

func TestCompactionUsesEncodedBytes(t *testing.T) {
	for _, largeObsolete := range []bool{false, true} {
		name := "small obsolete records"
		if largeObsolete {
			name = "large obsolete record"
		}
		t.Run(name, func(t *testing.T) {
			db := newInternalDB(t, WithCompactionThreshold(0.33, 128))
			stopTestMaintenance(db)
			s := &db.shards[0]
			queue := make(chan *Shard, 1)
			s.compactionQueue = queue
			keys := keysForShard(db, s, "byte-trigger", 21)
			if largeObsolete {
				for _, key := range keys[:20] {
					if err := db.Set(key, uint64(1)); err != nil {
						t.Fatal(err)
					}
				}
			} else {
				keys = keys[:2]
			}
			large := keys[len(keys)-1]
			if err := db.Set(large, make([]byte, 4096)); err != nil {
				t.Fatal(err)
			}
			if largeObsolete {
				if err := db.Set(large, false); err != nil {
					t.Fatal(err)
				}
			} else {
				for i := range 11 {
					if err := db.Set(keys[0], uint64(i)); err != nil {
						t.Fatal(err)
					}
				}
			}
			if queued := len(queue) == 1; queued != largeObsolete {
				t.Fatalf("queued=%v, want %v; total=%d live=%d", queued, largeObsolete, s.log.recordBytes, s.index.liveBytes)
			}
			checkSegmentAccounting(t, s)
		})
	}
}

func TestCompactionByteMinimum(t *testing.T) {
	minimum := 2 * recordSize(len("key"), 8)
	db := newInternalDB(t, WithCompactionThreshold(0, minimum))
	stopTestMaintenance(db)
	s, _ := db.shard([]byte("key"))
	queue := make(chan *Shard, 1)
	s.compactionQueue = queue
	for i := range 3 {
		if err := db.Set([]byte("key"), uint64(i)); err != nil {
			t.Fatal(err)
		}
		if queued := len(queue) == 1; queued != (i == 2) {
			t.Fatalf("write %d: queued=%v; minimum=%d obsolete=%d", i+1, queued, minimum, s.log.recordBytes-s.index.liveBytes)
		}
	}
	checkSegmentAccounting(t, s)
}

func TestByteAccountingMutationsAndRecovery(t *testing.T) {
	db := newInternalDB(t)
	stopTestMaintenance(db)
	s := &db.shards[0]
	keys := keysForShard(db, s, "accounting", 5)
	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		checkSegmentAccounting(t, s)
	}
	check(db.Set(keys[0], bytes.Repeat([]byte{'a'}, 128)))
	before := s.log.recordBytes
	check(db.Set(keys[0], bytes.Repeat([]byte{'a'}, 128)))
	if s.log.recordBytes != before {
		t.Fatal("unchanged Set changed byte accounting")
	}
	check(db.Update(keys[0], func(value []byte, exists bool) ([]byte, error) { return append(value, make([]byte, 1024)...), nil }))
	check(db.Update(keys[0], func(value []byte, exists bool) ([]byte, error) { return value[:3], nil }))
	check(db.Set(keys[0], uint64(7)))
	check(db.Update(keys[0], func(value uint64, exists bool) (uint64, error) { return value + 1, nil }))
	check(db.Set(keys[0], true))
	check(db.Update(keys[0], func(value bool, exists bool) (bool, error) { return !value, nil }))
	check(db.Set(keys[0], []byte(nil)))
	check(db.Delete(keys[0]))
	before = s.log.recordBytes
	check(db.Delete(keys[0]))
	if s.log.recordBytes != before {
		t.Fatal("missing Delete changed byte accounting")
	}
	future := WithExpiration(time.Now().Add(time.Hour))
	past := WithExpiration(time.Now().Add(-time.Hour))
	check(db.Set(keys[1], []byte("future"), future))
	check(db.Set(keys[1], bytes.Repeat([]byte{'b'}, 100), future))
	check(db.Set(keys[2], make([]byte, 4096), past))
	before = s.log.recordBytes
	check(db.Delete(keys[2]))
	if s.log.recordBytes != before {
		t.Fatal("expired Delete appended a record")
	}
	check(db.Set(keys[3], false, past))
	s.index.prune()
	check(nil)
	check(db.Set(keys[0], int64(-7)))
	check(db.Set(keys[4], float64(1.5)))
	check(s.log.openHead(incrementSegment(s.log.headSegment)))
	check(db.Set(keys[0], int64(-8)))
	check(db.Delete(keys[4]))
	total, live, path, head := s.log.recordBytes, s.index.liveBytes, db.path, s.log.headFile.Name()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// A torn append must not contribute bytes after recovery truncates it.
	f, err := os.OpenFile(head, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("torn tail")); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	recovered := openRecoveryFixture(t, path)
	s = &recovered.shards[0]
	check(nil)
	if s.log.recordBytes != total || s.index.liveBytes != live {
		t.Fatalf("recovery changed bytes: total=%d/%d live=%d/%d", s.log.recordBytes, total, s.index.liveBytes, live)
	}
	check(recovered.compact(context.Background()))
	if s.log.recordBytes != s.index.liveBytes {
		t.Fatalf("compaction left obsolete bytes: total=%d live=%d", s.log.recordBytes, s.index.liveBytes)
	}
	if got, err := recovered.Get[int64](keys[0]); err != nil || got != -8 {
		t.Fatalf("compacted value=%d error=%v", got, err)
	}
}

func TestByteAccountingFailedWrite(t *testing.T) {
	db := newInternalDB(t)
	stopTestMaintenance(db)
	key := []byte("failed-write")
	if err := db.Set(key, []byte("original")); err != nil {
		t.Fatal(err)
	}
	s, _ := db.shard(key)
	total, live := s.log.recordBytes, s.index.liveBytes
	if err := s.log.headFile.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(key, make([]byte, 4096)); err == nil {
		t.Fatal("write to a closed log succeeded")
	}
	if s.log.recordBytes != total || s.index.liveBytes != live {
		t.Fatal("failed write changed byte accounting")
	}
	checkSegmentAccounting(t, s)
}

func TestIndexShrinkMaintenance(t *testing.T) {
	for _, mode := range []string{"deleted", "expired", "queued compaction"} {
		t.Run(mode, func(t *testing.T) {
			db := newInternalDB(t, WithShardCount(1), WithCompactionThreshold(0.999999, ^uint64(0)))
			stopTestMaintenance(db)
			s := &db.shards[0]
			minimum := len(s.index.slots)
			const keep = 32
			keys := make([]string, minimum*3/4+1)
			for i := range keys {
				keys[i] = fmt.Sprintf("shrink-%d", i)
				var opts []Option
				if mode == "expired" && i < keep {
					opts = []Option{WithExpiration(time.Now().Add(time.Hour))}
				}
				if err := db.Set(keys[i], uint64(i), opts...); err != nil {
					t.Fatal(err)
				}
			}
			if len(s.index.slots) != 2*minimum {
				t.Fatal("fixture did not grow")
			}
			for i := keep; i < len(keys); i++ {
				var err error
				if mode == "expired" {
					err = db.Set(keys[i], uint64(i), WithExpiration(time.Now().Add(-time.Hour)))
				} else {
					err = db.Delete(keys[i])
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if len(s.index.slots) != 2*minimum {
				t.Fatal("a foreground mutation shrank the index")
			}
			records := s.log.recordCount
			if mode == "queued compaction" {
				s.compactionQueued, s.log.compactPending = true, true
				if err := db.compactExpirations(context.Background()); err != nil {
					t.Fatal(err)
				}
				if len(s.index.slots) != 2*minimum || !s.compactionQueued {
					t.Fatal("ticker ran a queued shard's maintenance")
				}
				if err := s.compactQueued(); err != nil {
					t.Fatal(err)
				}
				if s.log.recordCount != keep {
					t.Fatal("queued pass did not compact disk history")
				}
			} else {
				if err := db.compactExpirations(context.Background()); err != nil {
					t.Fatal(err)
				}
				if s.log.recordCount != records {
					t.Fatal("shrinking below the disk threshold rewrote history")
				}
			}
			if len(s.index.slots) != minimum || s.index.entryCount != keep {
				t.Fatalf("maintenance capacity=%d count=%d, want %d/%d", len(s.index.slots), s.index.entryCount, minimum, keep)
			}
			if mode == "expired" && len(s.index.expirations) != keep {
				t.Fatal("shrink changed remaining expirations")
			}
			checkSegmentAccounting(t, s)
			for i := range keep {
				if got, err := db.Get[uint64](keys[i]); err != nil || got != uint64(i) {
					t.Fatalf("retained key %d=%d, %v", i, got, err)
				}
			}
			if _, err := db.Get[uint64](keys[keep]); err != ErrNotFound {
				t.Fatalf("removed key returned %v", err)
			}
			if err := db.Set(keys[0], uint64(99)); err != nil {
				t.Fatal(err)
			}
			if err := db.Update(keys[1], func(value uint64, exists bool) (uint64, error) { return value + 10, nil }); err != nil {
				t.Fatal(err)
			}
			if err := db.Delete(keys[2]); err != nil {
				t.Fatal(err)
			}
			checkSegmentAccounting(t, s)
			path := db.path
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			})
			stopTestMaintenance(reopened)
			for i := range keep {
				want := uint64(i)
				switch i {
				case 0:
					want = 99
				case 1:
					want += 10
				case 2:
					if _, err := reopened.Get[uint64](keys[i]); err != ErrNotFound {
						t.Fatalf("deleted key after recovery: %v", err)
					}
					continue
				}
				if got, err := reopened.Get[uint64](keys[i]); err != nil || got != want {
					t.Fatalf("recovered key %d=%d, %v; want %d", i, got, err, want)
				}
			}
			checkSegmentAccounting(t, &reopened.shards[0])
		})
	}
}
