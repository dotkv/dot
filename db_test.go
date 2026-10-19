package dot_test

import (
	"bytes"
	"dot"
	"testing"
)

func newDB(t *testing.T) *dot.DB {
	db, err := dot.New("./db")
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
	v, err := db.Get(key)
	check(t, err)
	compare(t, v, val)
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
	equal(t, db, []byte("bar"), nil)
	check(t, db.Delete(key))
	equal(t, db, key, nil)
	equal(t, db, []byte("missing"), nil)
}
