package dot

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
)

func TestDBConcurrentScalarReads(t *testing.T) {
	db := newInternalDB(t)
	key := []byte("scalar")
	if err := db.Set(key, int64(0)); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			<-start
			for range 200 {
				value, err := db.Get[int64](key)
				if err != nil || (value != 0 && value != 1) {
					t.Errorf("got %d, error %v", value, err)
					return
				}
			}
		})
	}
	workers.Go(func() {
		<-start
		for i := range 200 {
			if err := db.Set(key, int64(i%2)); err != nil {
				t.Error(err)
				return
			}
		}
	})
	close(start)
	workers.Wait()
}

func TestDBConcurrentOperations(t *testing.T) {
	db := newInternalDB(t)
	const workerCount, keyCount = 2 * shardCount, 64
	var keys [workerCount][][]byte
	for worker := range keys {
		keys[worker] = keysForShard(db, &db.shards[worker%shardCount], fmt.Sprintf("worker-%d", worker), keyCount)
	}
	keyFor := func(worker, i int) []byte { return keys[worker][i] }
	initial, updated := []byte("initial"), []byte("updated")
	// Start at the growth threshold so concurrent writes must resize the index.
	s := &db.shards[0]
	size := len(s.index.slots)
	for _, key := range keysForShard(db, s, "existing", size*3/4) {
		if err := db.Set(key, initial); err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		workers.Go(func() {
			<-start
			for i := 0; i < keyCount; i++ {
				if err := db.Set(keyFor(worker, i), initial); err != nil {
					t.Error(err)
					return
				}
			}
			for i := 0; i < keyCount; i++ {
				key := keyFor(worker, i)
				if got, err := db.Get[[]byte](key); err != nil || !bytes.Equal(got, initial) {
					t.Errorf("key %q: got %q, want %q, error %v", key, got, initial, err)
					return
				}
				if err := db.Set(key, updated); err != nil {
					t.Error(err)
					return
				}
				if i%2 == 0 {
					if err := db.Delete(key); err != nil {
						t.Error(err)
						return
					}
				}
			}
		})
	}
	close(start)
	workers.Wait()
	if t.Failed() {
		return
	}
	if len(s.index.slots) <= size {
		t.Fatal("concurrent inserts did not exercise growth")
	}
	for worker := 0; worker < workerCount; worker++ {
		for i := 0; i < keyCount; i++ {
			want := updated
			var wantErr error
			if i%2 == 0 {
				want = nil
				wantErr = ErrNotFound
			}
			key := keyFor(worker, i)
			if got, err := db.Get[[]byte](key); err != wantErr || !bytes.Equal(got, want) {
				t.Fatalf("key %q: got %q, want %q, error %v", key, got, want, err)
			}
		}
	}
}

func TestDBConcurrentDestroy(t *testing.T) {
	testDBConcurrentShutdown(t, (*DB).Destroy)
}

func TestDBConcurrentClose(t *testing.T) {
	testDBConcurrentShutdown(t, (*DB).Close)
}

func testDBConcurrentShutdown(t *testing.T, shutdown func(*DB) error) {
	t.Helper()
	db := newInternalDB(t)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i := range db.shards {
		key := keysForShard(db, &db.shards[i], "destroy", 1)[0]
		if err := db.Set(key, []byte("value")); err != nil {
			t.Fatal(err)
		}
		workers.Go(func() {
			<-start
			for {
				if err := db.Set(key, []byte("updated")); err != nil {
					if !errors.Is(err, os.ErrClosed) {
						t.Error(err)
					}
					return
				}
				if _, err := db.Get[[]byte](key); err != nil && !errors.Is(err, os.ErrClosed) {
					t.Error(err)
					return
				}
				if err := db.Delete(key); err != nil && !errors.Is(err, os.ErrClosed) {
					t.Error(err)
					return
				}
			}
		})
	}
	for range 2 {
		workers.Go(func() {
			<-start
			if err := shutdown(db); err != nil {
				t.Error(err)
			}
		})
	}
	close(start)
	workers.Wait()
}
