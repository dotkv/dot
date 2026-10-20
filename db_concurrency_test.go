package dot

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestDBConcurrentOperations(t *testing.T) {
	db, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.log.head.Close(); err != nil {
			t.Error(err)
		}
	})
	const workerCount, keyCount = 8, 64
	keyFor := func(worker, i int) []byte {
		return fmt.Appendf(nil, "worker-%d-key-%d", worker, i)
	}
	initial, updated := []byte("initial"), []byte("updated")
	// Start at the growth threshold so concurrent writes must resize the index.
	size := len(db.index.slots)
	for i := 0; i < size*3/4; i++ {
		if err := db.Set(fmt.Appendf(nil, "existing-%d", i), initial); err != nil {
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
				if got, err := db.Get(key); err != nil || !bytes.Equal(got, initial) {
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
	if len(db.index.slots) <= size {
		t.Fatal("concurrent inserts did not exercise growth")
	}
	for worker := 0; worker < workerCount; worker++ {
		for i := 0; i < keyCount; i++ {
			want := updated
			if i%2 == 0 {
				want = nil
			}
			key := keyFor(worker, i)
			if got, err := db.Get(key); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("key %q: got %q, want %q, error %v", key, got, want, err)
			}
		}
	}
}
