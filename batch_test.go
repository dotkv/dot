package dot

import (
	"bytes"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

func TestBatchSetGetCommit(t *testing.T) {
	db := newInternalDB(t)
	b := db.NewBatch()
	if err := b.Set([]byte("k1"), uint64(1)); err != nil {
		t.Fatal(err)
	}
	if err := b.Set([]byte("k2"), "v2"); err != nil {
		t.Fatal(err)
	}
	if err := b.Set("k3", int64(-3)); err != nil {
		t.Fatal(err)
	}
	// Nothing visible before commit.
	if _, err := db.Get[uint64]([]byte("k1")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("pre-commit get err = %v, want ErrNotFound", err)
	}
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get[uint64]([]byte("k1")); err != nil || v != 1 {
		t.Fatalf("k1 = %d, %v; want 1, nil", v, err)
	}
	if v, err := db.Get[string]([]byte("k2")); err != nil || v != "v2" {
		t.Fatalf("k2 = %q, %v; want v2, nil", v, err)
	}
	if v, err := db.Get[int64]("k3"); err != nil || v != -3 {
		t.Fatalf("k3 = %d, %v; want -3, nil", v, err)
	}
}

func TestBatchReadYourWrites(t *testing.T) {
	db := newInternalDB(t)
	if err := db.Set([]byte("committed"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	b := db.NewBatch()
	if err := b.Set([]byte("pending"), uint64(7)); err != nil {
		t.Fatal(err)
	}
	// Pending write visible through the batch.
	if v, err := b.Get[uint64]([]byte("pending")); err != nil || v != 7 {
		t.Fatalf("pending get = %d, %v; want 7, nil", v, err)
	}
	// Type mismatch against a pending write, as with DB.Get.
	if _, err := b.Get[string]([]byte("pending")); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("pending type mismatch err = %v, want ErrTypeMismatch", err)
	}
	// Non-pending keys fall through to committed state.
	if v, err := b.Get[[]byte]([]byte("committed")); err != nil || !bytes.Equal(v, []byte("old")) {
		t.Fatalf("fallthrough get = %q, %v; want old, nil", v, err)
	}
	// Pending delete reads as not found.
	b.Delete([]byte("committed"))
	if _, err := b.Get[[]byte]([]byte("committed")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted get err = %v, want ErrNotFound", err)
	}
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get[[]byte]([]byte("committed")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("post-commit get err = %v, want ErrNotFound", err)
	}
}

func TestBatchLastWriteWins(t *testing.T) {
	db := newInternalDB(t)
	b := db.NewBatch()
	if err := b.Set([]byte("k"), uint64(1)); err != nil {
		t.Fatal(err)
	}
	if err := b.Set([]byte("k"), uint64(2)); err != nil {
		t.Fatal(err)
	}
	b.Delete([]byte("k"))
	if err := b.Set([]byte("k"), uint64(3)); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get[uint64]([]byte("k")); err != nil || v != 3 {
		t.Fatalf("k = %d, %v; want 3, nil", v, err)
	}
	// Set then delete ends deleted.
	b2 := db.NewBatch()
	if err := b2.Set([]byte("k"), uint64(9)); err != nil {
		t.Fatal(err)
	}
	b2.Delete([]byte("k"))
	if err := b2.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get[uint64]([]byte("k")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after delete err = %v, want ErrNotFound", err)
	}
}

func TestBatchEmptyCommit(t *testing.T) {
	db := newInternalDB(t)
	if err := db.NewBatch().Commit(); err != nil {
		t.Fatalf("empty commit err = %v", err)
	}
}

func TestBatchResetReuse(t *testing.T) {
	db := newInternalDB(t)
	b := db.NewBatch()
	if err := b.Set([]byte("a"), uint64(1)); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	// A committed batch is reset: a second commit is a no-op.
	if err := b.Commit(); err != nil {
		t.Fatalf("recommit err = %v", err)
	}
	if err := b.Set([]byte("b"), uint64(2)); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get[uint64]([]byte("b")); err != nil || v != 2 {
		t.Fatalf("b = %d, %v; want 2, nil", v, err)
	}
	b.Reset()
	// Reset clears the buffer: a never-committed key is gone from the batch.
	b2 := db.NewBatch()
	if err := b2.Set([]byte("never"), uint64(1)); err != nil {
		t.Fatal(err)
	}
	b2.Reset()
	if _, err := b2.Get[uint64]([]byte("never")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("post-reset get err = %v, want ErrNotFound", err)
	}
}

func TestBatchCopiesKeyAndValue(t *testing.T) {
	db := newInternalDB(t)
	b := db.NewBatch()
	key := []byte("mutable")
	val := []byte("v1")
	if err := b.Set(key, val); err != nil {
		t.Fatal(err)
	}
	key[0] = 'X'
	val[0] = 'X'
	if _, err := b.Get[[]byte]([]byte("mutable")); err != nil {
		t.Fatalf("get err = %v", err)
	}
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get[[]byte]([]byte("mutable")); err != nil || !bytes.Equal(v, []byte("v1")) {
		t.Fatalf("k = %q, %v; want v1, nil", v, err)
	}
}

func TestBatchExpiration(t *testing.T) {
	db := newInternalDB(t)
	b := db.NewBatch()
	past := time.Now().Add(-time.Hour)
	if err := b.Set([]byte("exp"), uint64(1), WithExpiration(past)); err != nil {
		t.Fatal(err)
	}
	// An expired pending write reads as not found, matching post-commit state.
	if _, err := b.Get[uint64]([]byte("exp")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired pending get err = %v, want ErrNotFound", err)
	}
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get[uint64]([]byte("exp")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("post-commit get err = %v, want ErrNotFound", err)
	}
}

func TestBatchCommitClosed(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	b := db.NewBatch()
	if err := b.Set([]byte("k"), uint64(1)); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("commit on closed db err = %v, want os.ErrClosed", err)
	}
}

// TestBatchConcurrentCommits commits batches with overlapping shards in
// opposite key order from many goroutines. Shard locks are taken in index
// order, so this must complete without deadlocking.
func TestBatchConcurrentCommits(t *testing.T) {
	db := newInternalDB(t)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				b := db.NewBatch()
				// Keys land on shards 3 and 5; order alternates to stress
				// lock ordering.
				k3 := keysForShard(db, &db.shards[3], "c3", 1)[0]
				k5 := keysForShard(db, &db.shards[5], "c5", 1)[0]
				if (w+i)%2 == 0 {
					k3, k5 = k5, k3
				}
				if err := b.Set(k3, uint64(w)); err != nil {
					t.Error(err)
					return
				}
				if err := b.Set(k5, uint64(i)); err != nil {
					t.Error(err)
					return
				}
				if err := b.Commit(); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
}
