package dot

import (
	"context"
	"fmt"
	"testing"
	"time"
)

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
