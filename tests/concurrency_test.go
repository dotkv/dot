package dot_test

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/dotkv/dot"
)

func TestDBConcurrentScalarReads(t *testing.T) {
	db := newDB(t)
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

func TestDBConcurrentDestroy(t *testing.T) {
	testDBConcurrentShutdown(t, (*dot.DB).Destroy)
}

func TestDBConcurrentClose(t *testing.T) {
	testDBConcurrentShutdown(t, (*dot.DB).Close)
}

func testDBConcurrentShutdown(t *testing.T, shutdown func(*dot.DB) error) {
	t.Helper()
	db := newDB(t)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i := range 16 {
		key := []byte(fmt.Sprintf("shutdown-%d", i))
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

func TestDBUpdateConcurrentIncrements(t *testing.T) {
	db := newDB(t)
	key := []byte("counter")
	if err := db.Set(key, uint64(0)); err != nil {
		t.Fatal(err)
	}
	const workers, increments = 16, 200
	start := make(chan struct{})
	var done sync.WaitGroup
	for range workers {
		done.Go(func() {
			<-start
			for range increments {
				if err := db.Update(key, func(v uint64, exists bool) (uint64, error) { return v + 1, nil }); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	close(start)
	done.Wait()
	if got, err := db.Get[uint64](key); err != nil || got != workers*increments {
		t.Fatalf("got %d, want %d, error %v", got, workers*increments, err)
	}
}

func TestDBConcurrentOperations(t *testing.T) {
	for _, shards := range []int{1, 4} {
		t.Run(fmt.Sprintf("shards=%d", shards), func(t *testing.T) {
			path := t.TempDir()
			db, err := dot.Open(path, dot.WithShardCount(shards), dot.WithCompactionThreshold(0.1, 0))
			check(t, err)
			t.Cleanup(func() { check(t, db.Close()) })
			const workers, keys = 16, 64
			keyFor := func(worker, key int) string { return fmt.Sprintf("worker-%d/key-%d", worker, key) }
			start := make(chan struct{})
			var done sync.WaitGroup
			for worker := range workers {
				done.Go(func() {
					<-start
					for i := range keys {
						key := keyFor(worker, i)
						if err := db.Set(key, uint64(i)); err != nil {
							t.Error(err)
							return
						}
						if err := db.Update(key, func(v uint64, exists bool) (uint64, error) {
							if !exists || v != uint64(i) {
								t.Errorf("Update(%q): value=%d exists=%t", key, v, exists)
							}
							return v + 1, nil
						}); err != nil {
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
			done.Wait()
			check(t, db.Close())
			next, err := dot.Open(path)
			check(t, err)
			db = next
			for worker := range workers {
				for i := range keys {
					value, err := db.Get[uint64](keyFor(worker, i))
					if i%2 == 0 {
						if err != dot.ErrNotFound || value != 0 {
							t.Fatalf("deleted key reappeared: value=%d error=%v", value, err)
						}
					} else if err != nil || value != uint64(i+1) {
						t.Fatalf("worker=%d key=%d: value=%d error=%v", worker, i, value, err)
					}
				}
			}
		})
	}
}
