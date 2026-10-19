package dot

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func newIndexTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.log.head.Close(); err != nil {
			t.Errorf("close log: %v", err)
		}
	})
	// Keep collisions easy to find and stay below the growth threshold.
	db.index.slots = make([]uint64, 8)
	return db
}

func collidingIndexTestKeys(t *testing.T, idx *Index, count int) [][]byte {
	t.Helper()
	buckets := make(map[uint64][][]byte)
	// Find actual collisions for this index's random seed. This many keys
	// guarantees that at least one bucket contains count keys.
	limit := (count-1)*len(idx.slots) + 1
	for i := 0; i < limit; i++ {
		key := fmt.Appendf(nil, "key-%d", i)
		pos := idx.hash(key) % uint64(len(idx.slots))
		buckets[pos] = append(buckets[pos], key)
		if len(buckets[pos]) == count {
			return buckets[pos]
		}
	}
	t.Fatal("could not find colliding keys")
	return nil
}

func setIndexTestKey(t *testing.T, db *DB, key []byte, value string) {
	t.Helper()
	if err := db.Set(key, []byte(value)); err != nil {
		t.Fatalf("Set(%q, %q): %v", key, value, err)
	}
}

func deleteIndexTestKey(t *testing.T, db *DB, key []byte) {
	t.Helper()
	if err := db.Delete(key); err != nil {
		t.Fatalf("Delete(%q): %v", key, err)
	}
}

func expectIndexTestValue(t *testing.T, db *DB, key []byte, want []byte) {
	t.Helper()
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Get(%q) = %q, want %q", key, got, want)
	}
}

func TestGetPastTombstone(t *testing.T) {
	db := newIndexTestDB(t)
	keys := collidingIndexTestKeys(t, db.index, 2)
	a, b := keys[0], keys[1]

	setIndexTestKey(t, db, a, "A")
	setIndexTestKey(t, db, b, "B")
	deleteIndexTestKey(t, db, a)

	// Deleting A must not hide B, which occupies the next probe position.
	expectIndexTestValue(t, db, b, []byte("B"))
}

func TestGetSkipsDeletedRecord(t *testing.T) {
	db := newIndexTestDB(t)
	keys := collidingIndexTestKeys(t, db.index, 2)
	a, b := keys[0], keys[1]
	setIndexTestKey(t, db, a, "A")
	setIndexTestKey(t, db, b, "B")
	deleteIndexTestKey(t, db, a)

	// Corrupt only deleted A's checksum at offset zero. B's record is
	// intact, and probing past a tombstone should not read A at all.
	contents, err := os.ReadFile(db.log.head.Name())
	if err != nil {
		t.Fatal(err)
	}
	contents[0] ^= 1
	if err := os.WriteFile(db.log.head.Name(), contents, 0600); err != nil {
		t.Fatal(err)
	}

	expectIndexTestValue(t, db, b, []byte("B"))
}

func TestSetSameValueAfterDelete(t *testing.T) {
	db := newIndexTestDB(t)
	key := []byte("key")

	setIndexTestKey(t, db, key, "value")
	deleteIndexTestKey(t, db, key)
	expectIndexTestValue(t, db, key, nil)
	setIndexTestKey(t, db, key, "value")

	// Matching the old value must not preserve the deleted slot state.
	expectIndexTestValue(t, db, key, []byte("value"))
}

func TestSetReusesTombstone(t *testing.T) {
	db := newIndexTestDB(t)
	keys := collidingIndexTestKeys(t, db.index, 2)
	a, b := keys[0], keys[1]

	setIndexTestKey(t, db, a, "A")
	deleteIndexTestKey(t, db, a)
	setIndexTestKey(t, db, b, "B")
	expectIndexTestValue(t, db, b, []byte("B"))

	// B can use A's tombstone instead of consuming another empty slot.
	used := 0
	for _, slot := range db.index.slots {
		if slot != 0 {
			used++
		}
	}
	if used != 1 {
		t.Fatalf("nonempty slots after reusing a deleted key's position = %d, want 1", used)
	}
}

func TestPrepareSetUpdatesExistingSlotPastTombstone(t *testing.T) {
	db := newIndexTestDB(t)
	keys := collidingIndexTestKeys(t, db.index, 2)
	a, b := keys[0], keys[1]
	setIndexTestKey(t, db, a, "A")
	setIndexTestKey(t, db, b, "old B")

	home := db.index.hash(a) % uint64(len(db.index.slots))
	existing := (home + 1) % uint64(len(db.index.slots))
	// Install a valid tombstone directly to isolate Set from Delete's logic.
	db.index.commit(home, occupiedMask|tombstoneMask)

	pos, _, err := db.index.prepareSet(db.log, b, []byte("new B"))
	if err != nil {
		t.Fatal(err)
	}
	if pos != existing {
		t.Fatalf("updating an existing key chose slot %d, want its existing slot %d", pos, existing)
	}
}

func TestDeletePastTombstone(t *testing.T) {
	db := newIndexTestDB(t)
	keys := collidingIndexTestKeys(t, db.index, 3)
	a, b, c := keys[0], keys[1], keys[2]

	setIndexTestKey(t, db, a, "A")
	setIndexTestKey(t, db, b, "B")
	deleteIndexTestKey(t, db, a)
	deleteIndexTestKey(t, db, b)

	// Reuse A's deleted slot so a Get that stops at tombstones cannot mask
	// the failure to delete B farther along the probe chain.
	setIndexTestKey(t, db, c, "C")
	expectIndexTestValue(t, db, b, nil)
}

func TestUpdatePastTombstoneDoesNotResurrectDeletedValue(t *testing.T) {
	db := newIndexTestDB(t)
	keys := collidingIndexTestKeys(t, db.index, 3)
	a, b, c := keys[0], keys[1], keys[2]

	setIndexTestKey(t, db, a, "A")
	setIndexTestKey(t, db, b, "old B")
	deleteIndexTestKey(t, db, a)
	setIndexTestKey(t, db, b, "new B")
	expectIndexTestValue(t, db, b, []byte("new B"))
	deleteIndexTestKey(t, db, b)

	// Updating B must replace its existing entry, not leave an old copy
	// that becomes visible when another key reuses the tombstone.
	setIndexTestKey(t, db, c, "C")
	expectIndexTestValue(t, db, b, nil)
}

func TestPopulation(t *testing.T) {
	// Population counts nonempty slots: live entries plus tombstones.
	t.Run("delete_missing_key", func(t *testing.T) {
		db := newIndexTestDB(t)
		deleteIndexTestKey(t, db, []byte("missing"))

		if got := db.index.population; got != 0 {
			t.Fatalf("population after deleting a missing key = %d, want 0", got)
		}
	})

	t.Run("reuse_tombstone", func(t *testing.T) {
		db := newIndexTestDB(t)
		key := []byte("key")
		setIndexTestKey(t, db, key, "old")
		deleteIndexTestKey(t, db, key)
		setIndexTestKey(t, db, key, "new")

		if got := db.index.population; got != 1 {
			t.Fatalf("population after reusing one tombstone = %d, want 1", got)
		}
	})

	t.Run("delete_same_key_twice", func(t *testing.T) {
		db := newIndexTestDB(t)
		key := []byte("key")
		setIndexTestKey(t, db, key, "value")
		deleteIndexTestKey(t, db, key)
		deleteIndexTestKey(t, db, key)

		if got := db.index.population; got != 1 {
			t.Fatalf("population after deleting the same key twice = %d, want 1", got)
		}
	})

	t.Run("delete_after_reinsertion", func(t *testing.T) {
		db := newIndexTestDB(t)
		key := []byte("key")
		setIndexTestKey(t, db, key, "old")
		deleteIndexTestKey(t, db, key)
		setIndexTestKey(t, db, key, "new")
		deleteIndexTestKey(t, db, key)

		if got := db.index.population; got != 1 {
			t.Fatalf("population after set/delete/set/delete of one key = %d, want 1", got)
		}
	})
}
