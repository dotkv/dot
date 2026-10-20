package dot_test

import (
	"bytes"
	"dot"
	"fmt"
	"testing"
)

func newDB(t *testing.T) *dot.DB {
	t.Helper()
	db, err := dot.New(t.TempDir())
	check(t, err)
	t.Cleanup(func() {
		check(t, db.Destroy())
	})
	return db
}

func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
}

func compare(t *testing.T, a []byte, b []byte) {
	t.Helper()
	if !bytes.Equal(a, b) {
		t.Fatalf("expected '%s' got '%s'", b, a)
	}
}

func equal(t *testing.T, db *dot.DB, key []byte, val []byte) {
	t.Helper()
	v, err := db.Get[[]byte](key)
	check(t, err)
	compare(t, v, val)
}

func expectMissing(t *testing.T, db *dot.DB, key []byte) {
	t.Helper()
	value, err := db.Get[[]byte](key)
	if err != dot.ErrNotFound || value != nil {
		t.Fatalf("missing key: got %q, error %v; want nil and ErrNotFound", value, err)
	}
}

func TestDBBasic(t *testing.T) {
	db := newDB(t)
	key, val := []byte("foo"), []byte("bar")
	check(t, db.Set(key, val))
	equal(t, db, key, val)
	val = []byte("baz")
	check(t, db.Set(key, val))
	equal(t, db, key, val)
	val = []byte("bar")
	check(t, db.Set(key, val))
	equal(t, db, key, val)
	expectMissing(t, db, []byte("bar"))
	check(t, db.Delete(key))
	expectMissing(t, db, key)
	expectMissing(t, db, []byte("missing"))
}

func TestDBSetSameValueAfterDelete(t *testing.T) {
	db := newDB(t)
	key, val := []byte("foo"), []byte("bar")
	check(t, db.Set(key, val))
	check(t, db.Delete(key))
	expectMissing(t, db, key)
	check(t, db.Set(key, val))
	equal(t, db, key, val)
}

func TestDBDeleteMissingKey(t *testing.T) {
	db := newDB(t)
	key, val := []byte("foo"), []byte("bar")
	missing := []byte("missing")
	check(t, db.Set(key, val))
	check(t, db.Delete(missing))
	check(t, db.Delete(missing))
	expectMissing(t, db, missing)
	equal(t, db, key, val)
}

func TestDBDeleteTwice(t *testing.T) {
	db := newDB(t)
	key, val := []byte("foo"), []byte("bar")
	check(t, db.Set(key, val))
	check(t, db.Delete(key))
	expectMissing(t, db, key)
	check(t, db.Delete(key))
	expectMissing(t, db, key)
}

func TestDBMultipleKeys(t *testing.T) {
	db := newDB(t)
	const count = 4096
	keys, vals := make([][]byte, count), make([][]byte, count)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "key-%d", i)
		vals[i] = fmt.Appendf(nil, "value-%d", i)
		check(t, db.Set(keys[i], vals[i]))
	}
	for i := range keys {
		equal(t, db, keys[i], vals[i])
	}

	// Deleting some keys must leave all the remaining keys reachable.
	for i := 0; i < count; i += 2 {
		check(t, db.Delete(keys[i]))
	}
	for i := range keys {
		if i%2 == 0 {
			expectMissing(t, db, keys[i])
		} else {
			equal(t, db, keys[i], vals[i])
		}
	}

	// Update the remaining keys while deleted positions still exist.
	for i := 1; i < count; i += 2 {
		vals[i] = fmt.Appendf(nil, "updated-%d", i)
		check(t, db.Set(keys[i], vals[i]))
		equal(t, db, keys[i], vals[i])
	}
	for i := 1; i < count; i += 2 {
		check(t, db.Delete(keys[i]))
	}
	// No old value should become visible after deleting an updated key.
	for i := range keys {
		expectMissing(t, db, keys[i])
	}

	for i := range keys {
		check(t, db.Set(keys[i], vals[i]))
	}
	for i := range keys {
		equal(t, db, keys[i], vals[i])
	}
}
