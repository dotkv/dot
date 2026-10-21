package dot

import (
	"testing"
	"time"
)

func TestOpenWithCompaction(t *testing.T) {
	for _, mode := range []struct {
		name    string
		opts    []Option
		enabled bool
	}{
		{"default", nil, true},
		{"disabled", []Option{WithCompaction(false)}, false},
		{"reenabled", []Option{WithCompaction(false), WithCompaction(true)}, true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			db := newInternalDB(t, mode.opts...)
			done := make(chan struct{})
			go func() {
				db.backgroundWG.Wait()
				close(done)
			}()
			if mode.enabled {
				select {
				case <-done:
					t.Fatal("background worker did not start")
				case <-time.After(20 * time.Millisecond):
				}
			} else {
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("background worker started with compaction disabled")
				}
			}
			// Expiration and recovery must work without a compactor pruning keys.
			key := []byte("expired")
			if err := db.Set(key, uint64(1), WithExpiration(time.Now().Add(-time.Second))); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Get[uint64](key); err != ErrNotFound {
				t.Fatalf("expired read: %v", err)
			}
			live := []byte("live")
			if err := db.Set(live, uint64(42)); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			<-done
			reopened, err := Open(db.path, WithCompaction(false))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			}()
			if got, err := reopened.Get[uint64](live); err != nil || got != 42 {
				t.Fatalf("recovered value: %d, %v", got, err)
			}
			if _, err := reopened.Get[uint64](key); err != ErrNotFound {
				t.Fatalf("recovered expired read: %v", err)
			}
		})
	}
}
