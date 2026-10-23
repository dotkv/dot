package dot

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func newBufferedDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(t.TempDir(), WithShardCount(1))
	if err != nil {
		t.Fatal(err)
	}
	if db.durability != Buffer {
		t.Fatal("default durability is not Buffer")
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func reopenBufferedDB(t *testing.T, db *DB) *DB {
	t.Helper()
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
	return reopened
}

func TestBufferReadVisibilityAndOwnership(t *testing.T) {
	db := newBufferedDB(t)
	s := &db.shards[0]
	key, value := []byte("bytes"), []byte("original")
	if err := db.Set(key, value); err != nil {
		t.Fatal(err)
	}
	// The record must be readable before any file write, and cannot retain
	// caller-owned keys or values.
	s.lock.RLock()
	info, err := s.log.headFile.Stat()
	pending := len(s.log.pending)
	s.lock.RUnlock()
	if err != nil || info.Size() != 0 || pending == 0 {
		t.Fatalf("record was not buffered: stat=%v pending=%d error=%v", info, pending, err)
	}
	clear(key)
	clear(value)
	got, err := db.Get[[]byte]("bytes")
	if err != nil || string(got) != "original" {
		t.Fatalf("buffered Get=%q error=%v", got, err)
	}
	clear(got)
	if got, err := db.Get[[]byte]("bytes"); err != nil || string(got) != "original" {
		t.Fatalf("Get exposed buffer: %q error=%v", got, err)
	}
	if err := db.Update("bytes", func(v []byte, exists bool) ([]byte, error) {
		if !exists || string(v) != "original" {
			t.Fatalf("Update did not see pending value: %q exists=%v", v, exists)
		}
		v[0] = 'O'
		return v, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("string", "hello"); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get[string]("string"); err != nil || got != "hello" {
		t.Fatalf("string=%q error=%v", got, err)
	}
	if _, err := db.Get[uint64]("string"); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("type mismatch=%v", err)
	}
	if err := db.Set("scalar", uint64(42)); err != nil {
		t.Fatal(err)
	}
	if err := db.Update("scalar", func(v uint64, exists bool) (uint64, error) {
		if !exists || v != 42 {
			t.Fatalf("scalar Update saw %d, exists=%v", v, exists)
		}
		return v + 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get[uint64]("scalar"); err != nil || got != 43 {
		t.Fatalf("scalar=%d error=%v", got, err)
	}
	if err := db.Set("expired", true, WithExpiration(time.Now().Add(-time.Hour))); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get[bool]("expired"); err != ErrNotFound {
		t.Fatalf("expired value: %v", err)
	}
	if err := db.Delete("string"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get[string]("string"); err != ErrNotFound {
		t.Fatalf("buffered delete: %v", err)
	}
	seen := make(map[string]string)
	for k, v := range db.Scan() {
		seen[string(k)] = string(v)
	}
	if len(seen) != 2 || seen["bytes"] != "Original" {
		t.Fatalf("Scan did not see current buffered records: %v", seen)
	}
	db = reopenBufferedDB(t, db)
	if got, err := db.Get[uint64]("scalar"); err != nil || got != 43 {
		t.Fatalf("Close did not flush: scalar=%d error=%v", got, err)
	}
	if _, err := db.Get[string]("string"); err != ErrNotFound {
		t.Fatalf("delete lost on reopen: %v", err)
	}
}

func TestBufferSizeTriggers(t *testing.T) {
	for _, size := range []int{logPendingSize - logAlignment, logPendingSize, logPendingSize + logAlignment} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			log, _ := openRecoveryLog(t, t.TempDir())
			key := []byte("key")
			value := bytes.Repeat([]byte{'v'}, size-logHeaderSize-len(key))
			if _, _, err := log.writeDurability(typeBytes, key, value, 0, Buffer); err != nil {
				t.Fatal(err)
			}
			info, err := log.headFile.Stat()
			if err != nil {
				t.Fatal(err)
			}
			wantFile := int64(size)
			if size < logPendingSize {
				wantFile = 0
			}
			if info.Size() != wantFile || log.size != uint64(size) {
				t.Fatalf("file=%d logical=%d, want file=%d logical=%d", info.Size(), log.size, wantFile, size)
			}
			// A following record either starts a new buffer, or flushes a
			// partially full buffer before reserving its own complete record.
			if _, off, err := log.writeDurability(typeBytes, key, []byte("next"), 0, Buffer); err != nil || off != uint64(size) {
				t.Fatalf("next offset=%d error=%v", off, err)
			}
			if info, err := log.headFile.Stat(); err != nil || info.Size() != int64(size) {
				t.Fatalf("capacity did not flush earlier record: %v error=%v", info, err)
			}
			if _, got, _, _, err := log.readPayload(0, 0, nil); err != nil || !bytes.Equal(got, value) {
				t.Fatalf("first record corrupted: length=%d error=%v", len(got), err)
			}
			if _, got, _, _, err := log.readPayload(0, uint64(size), nil); err != nil || string(got) != "next" {
				t.Fatalf("pending record=%q error=%v", got, err)
			}
			if err := log.flush(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBufferIdleDeadline(t *testing.T) {
	db := newBufferedDB(t)
	if err := db.Set("key", uint64(1)); err != nil {
		t.Fatal(err)
	}
	s := &db.shards[0]
	// No additional mutations are needed to trigger the timer.
	deadline := time.Now().Add(2 * time.Second)
	for {
		s.lock.RLock()
		pending, flushErr := len(s.log.pending), s.log.writeErr
		s.lock.RUnlock()
		if flushErr != nil {
			t.Fatal(flushErr)
		}
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("idle buffered write did not flush")
		}
		time.Sleep(time.Millisecond)
	}
	if info, err := s.log.headFile.Stat(); err != nil || info.Size() == 0 {
		t.Fatalf("timer did not write file: %v error=%v", info, err)
	}
}

func TestDurabilityOverridesFlushPending(t *testing.T) {
	for _, mode := range []Durability{Write, Sync} {
		for _, operation := range []string{"set", "unchanged-set", "update", "unchanged-update", "insert", "add", "unchanged-add", "sub", "unchanged-sub", "delete", "missing-delete"} {
			t.Run(fmt.Sprintf("%d/%s", mode, operation), func(t *testing.T) {
				db := newBufferedDB(t)
				if err := db.Set("key", uint64(1)); err != nil {
					t.Fatal(err)
				}
				opt := WithDurability(mode)
				var err error
				switch operation {
				case "set":
					err = db.Set("key", uint64(2), opt)
				case "unchanged-set":
					err = db.Set("key", uint64(1), opt)
				case "update", "unchanged-update", "insert":
					key := "key"
					if operation == "insert" {
						key = "other"
					}
					err = db.Update(key, func(v uint64, exists bool) (uint64, error) {
						if operation == "unchanged-update" {
							return v, nil
						}
						return v + 1, nil
					}, opt)
				case "delete":
					err = db.Delete("key", opt)
				case "missing-delete":
					err = db.Delete("missing", opt)
				case "add", "unchanged-add":
					delta := uint64(1)
					if operation == "unchanged-add" {
						delta = 0
					}
					_, err = db.Add("key", delta, opt)
				case "sub", "unchanged-sub":
					delta := uint64(1)
					if operation == "unchanged-sub" {
						delta = 0
					}
					_, err = db.Sub("key", delta, opt)
				}
				if err != nil {
					t.Fatal(err)
				}
				s := &db.shards[0]
				s.lock.RLock()
				defer s.lock.RUnlock()
				info, err := s.log.headFile.Stat()
				if err != nil || len(s.log.pending) != 0 || info.Size() != int64(s.log.size) || db.durability != Buffer {
					t.Fatalf("override did not drain buffer or changed default: stat=%v error=%v", info, err)
				}
			})
		}
	}
}

func TestConditionRejectionPreservesPendingWrite(t *testing.T) {
	for name, mutate := range map[string]func(*DB, ...Option) error{
		"set": func(db *DB, opts ...Option) error { return db.Set("key", uint64(8), opts...) },
		"update": func(db *DB, opts ...Option) error {
			return db.Update("key", func(v uint64, _ bool) (uint64, error) { return v + 1, nil }, opts...)
		},
		"add":    func(db *DB, opts ...Option) error { _, err := db.Add("key", uint64(1), opts...); return err },
		"sub":    func(db *DB, opts ...Option) error { _, err := db.Sub("key", uint64(1), opts...); return err },
		"delete": func(db *DB, opts ...Option) error { return db.Delete("key", opts...) },
	} {
		t.Run(name, func(t *testing.T) {
			db := newBufferedDB(t)
			stopTestMaintenance(db)
			s := &db.shards[0]
			// Keep the idle flush timer out of these exact storage assertions.
			s.flushTimer = time.AfterFunc(time.Hour, func() {})
			deadline := time.Now().Add(time.Hour)
			if err := db.Set("key", uint64(7), WithExpiration(deadline)); err != nil {
				t.Fatal(err)
			}
			size, pending := s.log.size, len(s.log.pending)
			err := mutate(db, WithTTL(-time.Hour), WithDurability(Sync), WithCondition(func(uint64, bool) bool { return false }))
			if err != ErrConditionFailed {
				t.Fatalf("rejected mutation: %v", err)
			}
			info, err := s.log.headFile.Stat()
			if err != nil || info.Size() != 0 || s.log.size != size || len(s.log.pending) != pending {
				t.Fatalf("rejected mutation appended or flushed: stat=%v size=%d pending=%d error=%v", info, s.log.size, len(s.log.pending), err)
			}
			_, hash := db.shard([]byte("key"))
			_, _, expiration, err := s.index.findScalar(s.log, []byte("key"), hash)
			if err != nil || expiration != deadline.UnixNano() {
				t.Fatalf("rejected mutation changed expiration: %d, %v", expiration, err)
			}
			// An approved no-op still honors its explicit durability request.
			if _, err := db.Add("key", uint64(0), WithDurability(Sync), WithCondition(func(uint64, bool) bool { return true })); err != nil {
				t.Fatal(err)
			}
			info, err = s.log.headFile.Stat()
			if err != nil || info.Size() != int64(size) || s.log.size != size || len(s.log.pending) != 0 {
				t.Fatalf("approved no-op did not flush: stat=%v size=%d pending=%d error=%v", info, s.log.size, len(s.log.pending), err)
			}
		})
	}
}

func TestBufferCompaction(t *testing.T) {
	db := newBufferedDB(t)
	for _, value := range []string{"old", "new"} {
		if err := db.Set("key", value); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Set("deleted", "value"); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete("deleted"); err != nil {
		t.Fatal(err)
	}
	if err := db.compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	db = reopenBufferedDB(t, db)
	if value, err := db.Get[string]("key"); err != nil || value != "new" {
		t.Fatalf("compaction lost buffered record: %q error=%v", value, err)
	}
	if _, err := db.Get[string]("deleted"); err != ErrNotFound {
		t.Fatalf("compaction resurrected delete: %v", err)
	}
}

func TestBufferConcurrentReadAfterWrite(t *testing.T) {
	db := newBufferedDB(t)
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Go(func() {
			key := fmt.Sprintf("key-%d", worker)
			for i := range 500 {
				if err := db.Set(key, uint64(i)); err != nil {
					t.Error(err)
					return
				}
				if got, err := db.Get[uint64](key); err != nil || got != uint64(i) {
					t.Errorf("read after write: got=%d want=%d error=%v", got, i, err)
					return
				}
			}
		})
	}
	wg.Wait()
}

func TestBufferDeadlineUnderContinuousWrites(t *testing.T) {
	db := newBufferedDB(t)
	s := &db.shards[0]
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.Now().Add(2 * time.Second)
	for i := uint64(0); ; i++ {
		if err := db.Set("key", i); err != nil {
			t.Fatal(err)
		}
		s.lock.RLock()
		info, err := s.log.headFile.Stat()
		s.lock.RUnlock()
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("continuous writes postponed the flush deadline")
		}
		<-ticker.C
	}
}

func TestBufferBackgroundFailure(t *testing.T) {
	db, err := Open(t.TempDir(), WithShardCount(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.Set("key", uint64(42)); err != nil {
		t.Fatal(err)
	}
	s := &db.shards[0]
	s.lock.Lock()
	err = s.log.headFile.Close()
	s.lock.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		s.lock.RLock()
		flushErr := s.log.writeErr
		s.lock.RUnlock()
		if flushErr != nil {
			if !errors.Is(flushErr, os.ErrClosed) {
				t.Fatalf("background flush error=%v", flushErr)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background failure was not recorded")
		}
		time.Sleep(time.Millisecond)
	}
	if got, err := db.Get[uint64]("key"); err != nil || got != 42 {
		t.Fatalf("flush failure lost pending value: %d error=%v", got, err)
	}
	if err := db.Set("key", uint64(42)); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Set did not report flush failure: %v", err)
	}
	if err := db.Update("key", func(v uint64, exists bool) (uint64, error) {
		t.Fatal("Update invoked callback after flush failure")
		return v, nil
	}); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Update did not report flush failure: %v", err)
	}
	if err := db.Delete("missing"); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Delete did not report flush failure: %v", err)
	}
	if err := db.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Close did not report flush failure: %v", err)
	}
}

func TestBuffersArePerShard(t *testing.T) {
	db := newInternalDB(t, WithShardCount(2), WithDurability(Buffer))
	first, second := &db.shards[0], &db.shards[1]
	k1 := keysForShard(db, first, "first", 1)[0]
	k2 := keysForShard(db, second, "second", 1)[0]
	if err := db.Set(k1, uint64(1)); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(k2, uint64(2)); err != nil {
		t.Fatal(err)
	}
	// Flushing shard zero must neither need shard one's lock nor drain it.
	second.lock.Lock()
	defer second.lock.Unlock()
	if err := db.Set(k1, uint64(1), WithDurability(Sync)); err != nil {
		t.Fatal(err)
	}
	if len(second.log.pending) == 0 {
		t.Fatal("Sync drained another shard")
	}
	first.lock.RLock()
	defer first.lock.RUnlock()
	if len(first.log.pending) != 0 || cap(first.log.pending) != logPendingSize || cap(second.log.pending) != logPendingSize {
		t.Fatal("shards do not own independent bounded buffers")
	}
}

func TestBufferPerOperation(t *testing.T) {
	for _, defaultMode := range []Durability{Write, Sync} {
		t.Run(fmt.Sprint(defaultMode), func(t *testing.T) {
			db := newInternalDB(t, WithShardCount(1), WithDurability(defaultMode))
			if err := db.Set("key", uint64(1), WithDurability(Buffer)); err != nil {
				t.Fatal(err)
			}
			if err := db.Update("key", func(v uint64, exists bool) (uint64, error) {
				return v + 1, nil
			}, WithDurability(Buffer)); err != nil {
				t.Fatal(err)
			}
			if err := db.Delete("key", WithDurability(Buffer)); err != nil {
				t.Fatal(err)
			}
			s := &db.shards[0]
			s.lock.RLock()
			defer s.lock.RUnlock()
			info, err := s.log.headFile.Stat()
			if err != nil || info.Size() != 0 || len(s.log.pending) == 0 || db.durability != defaultMode {
				t.Fatalf("Buffer override changed default or wrote file: stat=%v error=%v", info, err)
			}
		})
	}
}

func TestBufferRotation(t *testing.T) {
	log, _ := openRecoveryLog(t, t.TempDir())
	const firstOffset = logSegmentSize - logAlignment
	padLog(t, log, firstOffset)
	key, value := []byte("key"), []byte("record crosses the segment boundary")
	if seg, off, err := log.writeDurability(typeBytes, key, value, 0, Buffer); err != nil || seg != 0 || off != firstOffset {
		t.Fatalf("boundary write: segment=%d offset=%d error=%v", seg, off, err)
	}
	if seg, off, err := log.writeDurability(typeBytes, key, []byte("next"), 0, Buffer); err != nil || seg != 1 || off != 0 {
		t.Fatalf("rotation: segment=%d offset=%d error=%v", seg, off, err)
	}
	if _, got, _, _, err := log.readPayload(0, firstOffset, nil); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("sealed segment lost buffered tail: %q error=%v", got, err)
	}
	if _, got, _, _, err := log.readPayload(1, 0, nil); err != nil || string(got) != "next" {
		t.Fatalf("new head pending value: %q error=%v", got, err)
	}
	if err := log.flush(); err != nil {
		t.Fatal(err)
	}
}

func TestBufferCompactionAcrossSegments(t *testing.T) {
	db := newBufferedDB(t)
	for _, key := range []string{"updated", "deleted"} {
		if err := db.Set(key, "old", WithDurability(Write)); err != nil {
			t.Fatal(err)
		}
	}
	s := &db.shards[0]
	s.lock.Lock()
	err := s.log.openHead(1)
	s.lock.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set("updated", "new"); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete("deleted"); err != nil {
		t.Fatal(err)
	}
	if err := db.compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	db = reopenBufferedDB(t, db)
	if got, err := db.Get[string]("updated"); err != nil || got != "new" {
		t.Fatalf("compaction lost head update: %q error=%v", got, err)
	}
	if _, err := db.Get[string]("deleted"); err != ErrNotFound {
		t.Fatalf("compaction resurrected older value: %v", err)
	}
}
