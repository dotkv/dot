package dot

import (
	"bytes"
	"errors"
	"math"
	"os"
	"sync"
	"testing"
	"time"
)

func checkUpdate[T Value](t *testing.T, db *DB, key []byte, initial, want T, fn func(T, bool) (error, T)) {
	t.Helper()
	if err := db.Set(key, initial); err != nil {
		t.Fatal(err)
	}
	calls := 0
	if err := db.Update(key, func(value T, exists bool) (error, T) {
		calls++
		if !exists {
			t.Fatal("existing key reported exists=false")
		}
		return fn(value, exists)
	}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("callback called %d times, want 1", calls)
	}
	got, err := db.Get[T](key)
	var gotBuf, wantBuf [8]byte
	gotType, gotBytes, _ := encodeValue(got, &gotBuf)
	wantType, wantBytes, _ := encodeValue(want, &wantBuf)
	if err != nil || gotType != wantType || !bytes.Equal(gotBytes, wantBytes) {
		t.Fatalf("got %v, want %v, error %v", got, want, err)
	}
}

func TestDBUpdateScalars(t *testing.T) {
	db := newInternalDB(t)
	key := []byte("scalar")
	checkUpdate(t, db, key, uint64(41), uint64(42), func(v uint64, _ bool) (error, uint64) { return nil, v + 1 })
	checkUpdate(t, db, key, ^uint64(0), uint64(0), func(v uint64, _ bool) (error, uint64) { return nil, v + 1 })
	checkUpdate(t, db, key, int64(-42), int64(42), func(v int64, _ bool) (error, int64) { return nil, -v })
	checkUpdate(t, db, key, 1.5, 3.0, func(v float64, _ bool) (error, float64) { return nil, v * 2 })
	checkUpdate(t, db, key, false, true, func(v bool, _ bool) (error, bool) { return nil, !v })
	// No-op detection compares encodings, including NaN payloads and signed zero.
	for _, bits := range []uint64{0x7ff8000000000042, 1 << 63} {
		value := math.Float64frombits(bits)
		if err := db.Set(key, value); err != nil {
			t.Fatal(err)
		}
		s, _ := db.shard(key)
		size := s.log.size
		checkUpdate(t, db, key, value, value, func(v float64, _ bool) (error, float64) { return nil, v })
		if s.log.size != size {
			t.Fatal("unchanged scalar appended a record")
		}
	}
}

func TestDBUpdateBytes(t *testing.T) {
	db := newInternalDB(t)
	key := []byte("bytes")
	for _, size := range []int{0, 128, 2 * logWriteBufferSize} {
		initial := bytes.Repeat([]byte{'a'}, size)
		want := append(bytes.Clone(initial), 'b')
		var retained []byte
		checkUpdate(t, db, key, initial, want, func(v []byte, _ bool) (error, []byte) {
			retained = append(v, 'b')
			return nil, retained
		})
		clear(retained)
		if got, err := db.Get[[]byte](key); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("callback result aliases stored value: %q, %v", got, err)
		}
	}
	checkUpdate(t, db, key, []byte("abc"), []byte("Abc"), func(v []byte, _ bool) (error, []byte) {
		v[0] = 'A'
		return nil, v
	})
	s, _ := db.shard(key)
	size := s.log.size
	if err := db.Update(key, func(v []byte, _ bool) (error, []byte) { return nil, bytes.Clone(v) }); err != nil {
		t.Fatal(err)
	}
	if s.log.size != size {
		t.Fatal("unchanged bytes appended a record")
	}
	checkUpdate(t, db, key, []byte("abc"), []byte{}, func(v []byte, _ bool) (error, []byte) { return nil, nil })
	size = s.log.size
	if err := db.Update(key, func(v []byte, _ bool) (error, []byte) { return nil, []byte{} }); err != nil {
		t.Fatal(err)
	}
	if s.log.size != size {
		t.Fatal("nil-to-empty bytes appended a record")
	}
}

func TestDBUpdateMissingInvalidAndClosed(t *testing.T) {
	db := newInternalDB(t)
	key := []byte("key")
	// A missing key calls fn with the zero value and exists=false; returning
	// a value inserts it.
	calls := 0
	if err := db.Update(key, func(v uint64, exists bool) (error, uint64) {
		calls++
		if exists || v != 0 {
			t.Fatalf("missing key: v=%d exists=%v", v, exists)
		}
		return nil, 7
	}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("callback called %d times, want 1", calls)
	}
	if got, err := db.Get[uint64](key); err != nil || got != 7 {
		t.Fatalf("insert: got %d error=%v", got, err)
	}
	// A callback error aborts without writing.
	sentinel := errors.New("abort")
	if err := db.Update(key, func(v uint64, exists bool) (error, uint64) {
		return sentinel, 0
	}); !errors.Is(err, sentinel) {
		t.Fatalf("callback error: %v", err)
	}
	if got, err := db.Get[uint64](key); err != nil || got != 7 {
		t.Fatalf("aborted update changed value: got %d error=%v", got, err)
	}
	// Update-only callers return ErrNotFound themselves for missing keys.
	if err := db.Delete(key); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(key, func(v uint64, exists bool) (error, uint64) {
		if !exists {
			return ErrNotFound, 0
		}
		return nil, v + 1
	}); err != ErrNotFound {
		t.Fatalf("missing key with guard: %v", err)
	}
	// Type mismatch fails without calling fn.
	unexpected := func(v uint64, exists bool) (error, uint64) {
		t.Fatal("callback must not run")
		return nil, v
	}
	if err := db.Set(key, []byte("invalid")); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(key, unexpected); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("invalid scalar: %v", err)
	}
	// Expired keys look absent: fn sees exists=false and replaces the value.
	if err := db.Set(key, []byte("invalid"), WithExpiration(time.Now().Add(-time.Second))); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(key, func(v uint64, exists bool) (error, uint64) {
		if exists || v != 0 {
			t.Fatalf("expired key: v=%d exists=%v", v, exists)
		}
		return nil, 42
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get[uint64](key); err != nil || got != 42 {
		t.Fatalf("replaced expired: got %d error=%v", got, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(key, unexpected); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed database: %v", err)
	}
}

func TestDBUpdatePreservesExpiration(t *testing.T) {
	db := newInternalDB(t, WithExpiration(time.Now().Add(-time.Hour)))
	key := []byte("key")
	for _, expiration := range []time.Time{{}, time.Now().Add(time.Hour)} {
		if err := db.Set(key, uint64(1), WithExpiration(expiration)); err != nil {
			t.Fatal(err)
		}
		if err := db.Update(key, func(v uint64, exists bool) (error, uint64) { return nil, v + 1 }); err != nil {
			t.Fatal(err)
		}
		s, hash := db.shard(key)
		_, _, gotExp, err := s.index.findScalar(s.log, key, hash)
		wantExp := int64(0)
		if !expiration.IsZero() {
			wantExp = expiration.UnixNano()
		}
		if err != nil || gotExp != wantExp {
			t.Fatalf("expiration: got %d, want %d, error %v", gotExp, wantExp, err)
		}
	}
}

func TestDBUpdatePanicAndWriteFailure(t *testing.T) {
	db := newInternalDB(t)
	key := []byte("key")
	if err := db.Set(key, []byte("original")); err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recovered := recover(); recovered != "callback panic" {
				t.Fatalf("unexpected panic: %v", recovered)
			}
		}()
		_ = db.Update(key, func(v []byte, exists bool) (error, []byte) {
			clear(v)
			panic("callback panic")
		})
	}()
	if got, err := db.Get[[]byte](key); err != nil || string(got) != "original" {
		t.Fatalf("panic changed value or retained lock: %q, %v", got, err)
	}
	s, _ := db.shard(key)
	if err := s.log.headFile.Truncate(logSegmentSize); err != nil {
		t.Fatal(err)
	}
	s.log.size = logSegmentSize
	blocker := s.log.segmentPath(s.log.headSegment + 1)
	if err := os.Mkdir(blocker, 0700); err != nil {
		t.Fatal(err)
	}
	calls := 0
	fn := func(v []byte, exists bool) (error, []byte) {
		calls++
		v[0] = 'O'
		return nil, v
	}
	if err := db.Update(key, fn); err == nil {
		t.Fatal("expected log rotation failure")
	}
	if got, err := db.Get[[]byte](key); err != nil || string(got) != "original" {
		t.Fatalf("failed append changed value: %q, %v", got, err)
	}
	if calls != 1 {
		t.Fatalf("failed write invoked callback %d times", calls)
	}
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(key, fn); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get[[]byte](key); err != nil || string(got) != "Original" {
		t.Fatalf("update after rotation: %q, %v", got, err)
	}
}

func TestDBUpdateConcurrentIncrements(t *testing.T) {
	db := newInternalDB(t)
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
				if err := db.Update(key, func(v uint64, exists bool) (error, uint64) { return nil, v + 1 }); err != nil {
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
