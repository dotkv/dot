package dot

import (
	"testing"
)

// WithSync is a durability switch; these tests exercise the option plumbing
// on every write path. Actual crash durability is not testable without fault
// injection.
func TestSyncOpenDefault(t *testing.T) {
	db := newSyncInternalDB(t)
	if !db.syncWrites {
		t.Fatal("syncWrites = false, want true")
	}
	if err := db.Set([]byte("k"), uint64(1)); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get[uint64]([]byte("k")); err != nil || v != 1 {
		t.Fatalf("k = %d, %v; want 1, nil", v, err)
	}
	if err := db.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	if err := db.Update([]byte("u"), func(v uint64) uint64 { return v + 1 }); err == nil {
		t.Fatal("update of missing key succeeded, want ErrNotFound")
	}
	if err := db.Set([]byte("u"), uint64(1)); err != nil {
		t.Fatal(err)
	}
	if err := db.Update([]byte("u"), func(v uint64) uint64 { return v + 1 }); err != nil {
		t.Fatal(err)
	}
	if err := db.Upsert([]byte("w"), func(v uint64, exists bool) uint64 { return 7 }); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get[uint64]([]byte("u")); err != nil || v != 2 {
		t.Fatalf("u = %d, %v; want 2, nil", v, err)
	}
	if v, err := db.Get[uint64]([]byte("w")); err != nil || v != 7 {
		t.Fatalf("w = %d, %v; want 7, nil", v, err)
	}
}

func newSyncInternalDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(t.TempDir(), WithSync(true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Destroy(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func TestSyncPerOperation(t *testing.T) {
	db := newInternalDB(t)
	if db.syncWrites {
		t.Fatal("syncWrites = true, want false")
	}
	// Per-operation opt-in on a non-sync database.
	if err := db.Set([]byte("k"), uint64(1), WithSync(true)); err != nil {
		t.Fatal(err)
	}
	if err := db.Update([]byte("k"), func(v uint64) uint64 { return v + 1 }, WithSync(true)); err != nil {
		t.Fatal(err)
	}
	if err := db.Upsert([]byte("k"), func(v uint64, exists bool) uint64 { return v + 1 }, WithSync(true)); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get[uint64]([]byte("k")); err != nil || v != 3 {
		t.Fatalf("k = %d, %v; want 3, nil", v, err)
	}
	if err := db.Delete([]byte("k"), WithSync(true)); err != nil {
		t.Fatal(err)
	}
}

func TestSyncPerOperationOverride(t *testing.T) {
	db := newSyncInternalDB(t)
	// WithSync(false) disables the database default for a single call.
	if err := db.Set([]byte("k"), uint64(1), WithSync(false)); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete([]byte("k"), WithSync(false)); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get[uint64]([]byte("k")); err == nil {
		t.Fatalf("k = %d, want ErrNotFound", v)
	}
}
