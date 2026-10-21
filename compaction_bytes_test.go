package dot

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"
)

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
	check(db.Update(keys[0], func(value []byte) []byte { return append(value, make([]byte, 1024)...) }))
	check(db.Update(keys[0], func(value []byte) []byte { return value[:3] }))
	check(db.Set(keys[0], uint64(7)))
	check(db.Update(keys[0], func(value uint64) uint64 { return value + 1 }))
	check(db.Set(keys[0], true))
	check(db.Update(keys[0], func(value bool) bool { return !value }))
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
