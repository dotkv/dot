package dot

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestStringValues(t *testing.T) {
	db := newInternalDB(t)
	key := []byte("strkey")
	if err := db.Set(key, "hello"); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get[string](key); err != nil || got != "hello" {
		t.Fatalf("Get[string]=%q error=%v", got, err)
	}
	if err := db.Set(key, ""); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get[string](key); err != nil || got != "" {
		t.Fatalf("empty string: got %q error=%v", got, err)
	}
	// String and []byte are distinct value types.
	if _, err := db.Get[[]byte](key); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("Get[[]byte] on string: error=%v, want ErrTypeMismatch", err)
	}
	if err := db.Set([]byte("byteskey"), []byte("raw")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get[string]([]byte("byteskey")); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("Get[string] on []byte: error=%v, want ErrTypeMismatch", err)
	}
	if err := db.Update(key, func(cur string, exists bool) (error, string) { return nil, cur + " world" }); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get[string](key); err != nil || got != " world" {
		t.Fatalf("after Update: got %q error=%v", got, err)
	}
}

func TestStringKeys(t *testing.T) {
	db := newInternalDB(t)
	if err := db.Set("skey", uint64(42)); err != nil {
		t.Fatal(err)
	}
	// Key identity ignores the key's Go type.
	if got, err := db.Get[uint64]([]byte("skey")); err != nil || got != 42 {
		t.Fatalf("Get with []byte key: got %d error=%v", got, err)
	}
	if err := db.Update("skey", func(cur uint64, exists bool) (error, uint64) { return nil, cur + 1 }); err != nil {
		t.Fatal(err)
	}
	if err := db.Update("skey", func(cur uint64, exists bool) (error, uint64) {
		if !exists || cur != 43 {
			t.Errorf("update saw cur=%d exists=%v", cur, exists)
		}
		return nil, cur + 1
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get[uint64]("skey"); err != nil || got != 44 {
		t.Fatalf("final: got %d error=%v", got, err)
	}
	if err := db.Delete("skey"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get[uint64]([]byte("skey")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after Delete: error=%v, want ErrNotFound", err)
	}
}

func TestUpdate(t *testing.T) {
	db := newInternalDB(t)
	key := []byte("upsert-counter")

	// Insert path: missing key reports exists=false and zero value.
	if err := db.Update(key, func(cur uint64, exists bool) (error, uint64) {
		if exists || cur != 0 {
			t.Errorf("insert: cur=%d exists=%v", cur, exists)
		}
		return nil, 1
	}); err != nil {
		t.Fatal(err)
	}
	// Update path: existing key reports exists=true and current value.
	if err := db.Update(key, func(cur uint64, exists bool) (error, uint64) {
		if !exists || cur != 1 {
			t.Errorf("update: cur=%d exists=%v", cur, exists)
		}
		return nil, cur + 1
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get[uint64](key); err != nil || got != 2 {
		t.Fatalf("counter=%d error=%v, want 2", got, err)
	}

	// A callback error aborts the write and propagates as-is.
	sentinel := errors.New("abort")
	called := false
	if err := db.Update(key, func(cur uint64, exists bool) (error, uint64) {
		called = true
		return sentinel, 0
	}); !errors.Is(err, sentinel) {
		t.Fatalf("callback error: error=%v, want %v", err, sentinel)
	}
	if !called {
		t.Fatal("callback was not called")
	}
	if got, err := db.Get[uint64](key); err != nil || got != 2 {
		t.Fatalf("aborted update changed value: got %d error=%v", got, err)
	}

	// Unchanged encoding appends no record.
	s, _ := db.shard(key)
	before := s.log.size
	if err := db.Update(key, func(cur uint64, exists bool) (error, uint64) { return nil, cur }); err != nil {
		t.Fatal(err)
	}
	if s.log.size != before {
		t.Fatal("unchanged update appended a record")
	}

	// Type mismatch fails without calling fn.
	if err := db.Set([]byte("typed"), []byte("bytes")); err != nil {
		t.Fatal(err)
	}
	called = false
	err := db.Update([]byte("typed"), func(cur uint64, exists bool) (error, uint64) {
		called = true
		return nil, cur
	})
	if !errors.Is(err, ErrTypeMismatch) || called {
		t.Fatalf("mismatch: error=%v called=%v", err, called)
	}

	// Expired keys look absent and are replaced.
	past := time.Now().Add(-time.Hour)
	if err := db.Set([]byte("exp"), uint64(7), WithExpiration(past)); err != nil {
		t.Fatal(err)
	}
	if err := db.Update([]byte("exp"), func(cur uint64, exists bool) (error, uint64) {
		if exists || cur != 0 {
			t.Errorf("expired: cur=%d exists=%v", cur, exists)
		}
		return nil, 99
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get[uint64]([]byte("exp")); err != nil || got != 99 {
		t.Fatalf("replaced expired: got %d error=%v", got, err)
	}

	// Without options, an existing key keeps its expiration.
	future := time.Now().Add(time.Hour)
	if err := db.Set([]byte("ttl"), "v", WithExpiration(future)); err != nil {
		t.Fatal(err)
	}
	if err := db.Update([]byte("ttl"), func(cur string, exists bool) (error, string) { return nil, "v2" }); err != nil {
		t.Fatal(err)
	}
	s2, hash := db.shard([]byte("ttl"))
	pos, _, _, expiration, err := s2.index.findPayload(s2.log, []byte("ttl"), hash, nil)
	if err != nil || pos < 0 || expiration != future.UnixNano() {
		t.Fatalf("expiration not preserved: expiration=%d error=%v", expiration, err)
	}

	// WithExpiration on Update sets the expiration of an existing key.
	later := future.Add(time.Hour)
	if err := db.Update([]byte("ttl"), func(cur string, exists bool) (error, string) { return nil, "v3" },
		WithExpiration(later)); err != nil {
		t.Fatal(err)
	}
	pos, _, _, expiration, err = s2.index.findPayload(s2.log, []byte("ttl"), hash, nil)
	if err != nil || pos < 0 || expiration != later.UnixNano() {
		t.Fatalf("expiration not updated: expiration=%d error=%v", expiration, err)
	}
	if got, err := db.Get[string]([]byte("ttl")); err != nil || got != "v3" {
		t.Fatalf("value after expiration update: got %q error=%v", got, err)
	}

	// Options apply on insert.
	ttl := time.Now().Add(2 * time.Hour)
	if err := db.Update([]byte("ttl-new"), func(cur string, exists bool) (error, string) { return nil, "n" },
		WithExpiration(ttl)); err != nil {
		t.Fatal(err)
	}
	s3, hash3 := db.shard([]byte("ttl-new"))
	pos, _, _, expiration, err = s3.index.findPayload(s3.log, []byte("ttl-new"), hash3, nil)
	if err != nil || pos < 0 || expiration != ttl.UnixNano() {
		t.Fatalf("insert expiration: expiration=%d error=%v", expiration, err)
	}
}

func TestScan(t *testing.T) {
	db := newInternalDB(t)
	// Empty database yields nothing.
	for range db.Scan() {
		t.Fatal("empty scan yielded a pair")
	}
	const n = 200
	want := make(map[string]string, n)
	for i := range n {
		key := fmt.Sprintf("scan-%04d", i)
		value := fmt.Sprintf("value-%04d", i)
		if err := db.Set(key, value); err != nil {
			t.Fatal(err)
		}
		want[key] = value
	}
	// Scalars and expirations exercise the skip paths.
	if err := db.Set("scan-scalar", uint64(7)); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("scan-expired", "gone", WithExpiration(time.Now().Add(-time.Hour))); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete("scan-0000"); err != nil {
		t.Fatal(err)
	}
	delete(want, "scan-0000")

	got := make(map[string]string)
	for k, v := range db.Scan() {
		got[string(k)] = string(v)
	}
	// The scalar is stored as 8 little-endian bytes; check presence only.
	if len(got) != len(want)+1 {
		t.Fatalf("scan yielded %d pairs, want %d", len(got), len(want)+1)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("key %q: got %q want %q", k, got[k], v)
		}
	}
	if _, ok := got["scan-expired"]; ok {
		t.Fatal("scan yielded an expired key")
	}

	// Early termination stops the scan.
	count := 0
	for range db.Scan() {
		count++
		if count == 3 {
			break
		}
	}
	if count != 3 {
		t.Fatalf("early break scanned %d pairs, want 3", count)
	}

	// A second scan sees the same content: no state leaks between scans.
	again := make(map[string]string)
	for k, v := range db.Scan() {
		again[string(k)] = string(v)
	}
	if len(again) != len(got) {
		t.Fatalf("second scan: got %d pairs, want %d", len(again), len(got))
	}
}
