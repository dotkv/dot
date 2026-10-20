package dot

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"testing"
)

type indexEntry struct {
	hash uint64
	live bool
}

// Give every synthetic entry a unique offset and exercise segment bits too.
func entrySlot(id int, hash uint64) uint64 {
	return uint64(id) | uint64(id%1024)<<26 | hashtag(hash)
}

func checkIndex(t *testing.T, idx *Index, entries []indexEntry) {
	t.Helper()
	mask := len(idx.slots) - 1
	counts := make([]int, len(idx.slots))
	seen := make([]bool, len(entries))
	live := 0
	for _, entry := range entries {
		if entry.live {
			counts[entry.hash&uint64(mask)]++
			live++
		}
	}
	for pos, slot := range idx.slots {
		if (slot&occupiedMask != 0) != (counts[pos] != 0) {
			t.Fatalf("bucket %d: wrong occupancy bit", pos)
		}
		if empty(slot) {
			if slot != 0 {
				t.Fatalf("bucket %d: empty slot retains entry bits", pos)
			}
			continue
		}
		id := int(slot & offsetMask)
		if id >= len(entries) || !entries[id].live || seen[id] {
			t.Fatalf("bucket %d: unexpected or duplicate entry %d", pos, id)
		}
		seen[id] = true
		home := int(entries[id].hash & uint64(mask))
		if slot&^metadataMask != entrySlot(id, entries[id].hash) {
			t.Fatalf("entry %d: address or fingerprint changed", id)
		}
		if (slot&shiftedMask != 0) != (pos != home) {
			t.Fatalf("entry %d: wrong shifted bit", id)
		}
		previous := idx.slots[(pos-1)&mask]
		continuation := !empty(previous) && entries[previous&offsetMask].hash&uint64(mask) == uint64(home)
		if (slot&continuationMask != 0) != continuation {
			t.Fatalf("entry %d: wrong continuation bit", id)
		}
	}
	for id, entry := range entries {
		if entry.live != seen[id] {
			t.Fatalf("entry %d: live=%t, seen=%t", id, entry.live, seen[id])
		}
	}
	for home, count := range counts {
		if count == 0 {
			continue
		}
		pos := idx.run(home)
		for i := 0; i < count; i++ {
			slot := idx.slots[pos]
			if empty(slot) || int(entries[slot&offsetMask].hash&uint64(mask)) != home {
				t.Fatalf("bucket %d: run contains an entry from another bucket", home)
			}
			if (slot&continuationMask != 0) != (i != 0) {
				t.Fatalf("bucket %d: wrong run length or start", home)
			}
			pos = (pos + 1) & mask
		}
		if idx.slots[pos]&continuationMask != 0 {
			t.Fatalf("bucket %d: run is longer than expected", home)
		}
	}
	if idx.population != live {
		t.Fatalf("population: got %d, want %d", idx.population, live)
	}
}

func removeEntry(t *testing.T, idx *Index, entries []indexEntry, id int) {
	t.Helper()
	home := int(entries[id].hash & uint64(len(idx.slots)-1))
	pos := idx.run(home)
	for i := 0; i < len(idx.slots); i++ {
		if int(idx.slots[pos]&offsetMask) == id {
			idx.remove(home, pos)
			entries[id].live = false
			return
		}
		pos = (pos + 1) & (len(idx.slots) - 1)
	}
	t.Fatalf("entry %d not found", id)
}

func TestIndexRuns(t *testing.T) {
	for _, homes := range [][]int{
		{1, 1, 2, 0, 1, 3, 2, 0, 4},
		{31, 31, 0, 30, 31, 1, 0, 30, 2},
		{0, 1, 2, 3, 4, 5, 6, 7, 8},
	} {
		idx := &Index{slots: make([]uint64, 32)}
		entries := make([]indexEntry, len(homes))
		for id, home := range homes {
			entries[id] = indexEntry{hash: uint64(home), live: true}
			idx.insert(home, entrySlot(id, uint64(home)))
			checkIndex(t, idx, entries[:id+1])
		}
		// Remove run heads, interiors and tails, including across slot zero.
		for _, id := range []int{0, 4, 2, 8, 7, 1, 3, 6, 5} {
			removeEntry(t, idx, entries, id)
			checkIndex(t, idx, entries)
		}
	}
}

func TestIndexRandomMutations(t *testing.T) {
	random := rand.New(rand.NewPCG(1, 2))
	for _, size := range []int{8, 32, 512} {
		idx := &Index{slots: make([]uint64, size)}
		var entries []indexEntry
		var active []int
		for step := 0; step < 2000; step++ {
			if len(active) == 0 || len(active) < size*3/4 && random.IntN(2) == 0 {
				id := len(entries)
				hash := random.Uint64()
				entries = append(entries, indexEntry{hash: hash, live: true})
				active = append(active, id)
				idx.insert(int(hash&uint64(size-1)), entrySlot(id, hash))
			} else {
				i := random.IntN(len(active))
				removeEntry(t, idx, entries, active[i])
				active[i] = active[len(active)-1]
				active = active[:len(active)-1]
			}
			checkIndex(t, idx, entries)
		}
	}
}

func FuzzIndexMutations(f *testing.F) {
	f.Add([]byte{30, 30, 0, 28, 30, 2, 0, 28, 4, 1, 3, 5, 7, 9, 11})
	f.Add(bytes.Repeat([]byte{0, 30, 0, 2, 4, 1, 3}, 16))
	f.Fuzz(func(t *testing.T, data []byte) {
		idx := &Index{slots: make([]uint64, 16)}
		var entries []indexEntry
		var active []int
		for step, op := range data[:min(len(data), 256)] {
			if len(active) == 0 || op&1 == 0 && len(active) < 12 {
				id := len(entries)
				hash := uint64(op>>1) | uint64(step)<<indexBits
				entries = append(entries, indexEntry{hash: hash, live: true})
				active = append(active, id)
				idx.insert(int(hash&15), entrySlot(id, hash))
			} else {
				i := int(op>>1) % len(active)
				removeEntry(t, idx, entries, active[i])
				active[i] = active[len(active)-1]
				active = active[:len(active)-1]
			}
			checkIndex(t, idx, entries)
		}
	})
}

func TestIndexGrow(t *testing.T) {
	idx := newIndex()
	n := len(idx.slots)
	random := rand.New(rand.NewPCG(3, 4))
	var entries []indexEntry
	add := func(hash uint64) {
		id := len(entries)
		entries = append(entries, indexEntry{hash: hash, live: true})
		idx.insert(int(hash&uint64(len(idx.slots)-1)), entrySlot(id, hash))
	}
	// A wrapping run with displacement beyond an 8-bit counter, mixed
	// split bits, and neighboring runs inserted out of home-bucket order.
	for i := 0; i < 600; i++ {
		add(uint64(n-2) | uint64(i%8)<<indexBits)
	}
	for _, home := range []int{1, n - 1, 0, 2, n - 3} {
		add(uint64(home) | uint64(home%8)<<indexBits)
	}
	for idx.population < n*3/4 {
		add(random.Uint64())
	}
	checkIndex(t, idx, entries)
	for id := 0; id < 600; id += 3 {
		removeEntry(t, idx, entries, id)
	}
	// Deletions count as free space and do not cause premature growth.
	idx.grow()
	if len(idx.slots) != n {
		t.Fatal("grew below the load threshold")
	}
	for growth := 0; growth < 3; growth++ {
		for idx.population < len(idx.slots)*3/4 {
			add(random.Uint64())
		}
		size, population := len(idx.slots), idx.population
		idx.grow()
		if len(idx.slots) != size*2 || idx.population != population {
			t.Fatal("grow changed population or did not double capacity")
		}
		checkIndex(t, idx, entries)
	}
}

func TestIndexAllocations(t *testing.T) {
	idx := newIndex()
	for i := 0; i < len(idx.slots)*3/4; i++ {
		idx.insert(i, entrySlot(i, uint64(i)))
	}
	slots, population := idx.slots, idx.population
	allocations := testing.AllocsPerRun(5, func() {
		idx.slots, idx.population = slots, population
		idx.grow()
	})
	if allocations != 1 {
		t.Fatalf("grow: got %g allocations, want just the new table", allocations)
	}
	idx = &Index{slots: make([]uint64, 32)}
	allocations = testing.AllocsPerRun(1000, func() {
		idx.insert(31, 0)
		idx.insert(31, 1)
		idx.insert(0, 2)
		idx.remove(31, 0)
		idx.remove(31, 31)
		idx.remove(0, 0)
		idx.grow()
	})
	if allocations != 0 {
		t.Fatalf("insert/remove/below-threshold grow: got %g allocations", allocations)
	}
	idx = newIndex()
	log := &Log{}
	key := []byte("key")
	allocations = testing.AllocsPerRun(1000, func() {
		change, err := idx.prepareSet(log, key, nil)
		if err != nil {
			t.Fatal(err)
		}
		idx.commit(change)
		idx.remove(change.home, change.home)
		if _, err := idx.prepareDelete(log, key); err != nil {
			t.Fatal(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("prepare/commit: got %g allocations", allocations)
	}
}

func TestDBGrow(t *testing.T) {
	db, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.log.headFile.Close() })
	n := len(db.index.slots)
	var key [8]byte
	keyFor := func(i int) []byte { return fmt.Appendf(key[:0], "%08d", i) }
	value := []byte("value")
	for i := 0; i < n*3/4; i++ {
		if err := db.Set(keyFor(i), value); err != nil {
			t.Fatal(err)
		}
	}
	// Updating at the load threshold must not allocate a larger table.
	for _, val := range [][]byte{value, []byte("updated"), value} {
		if err := db.Set(keyFor(0), val); err != nil {
			t.Fatal(err)
		}
		if len(db.index.slots) != n {
			t.Fatal("updating an existing key grew the table")
		}
	}
	// Growing must work with the log closed; reopen it to check every value.
	name := db.log.headFile.Name()
	if err := db.log.headFile.Close(); err != nil {
		t.Fatal(err)
	}
	db.index.grow()
	if len(db.index.slots) != 2*n {
		t.Fatal("grow with the log closed did not double capacity")
	}
	db.log.headFile, err = os.OpenFile(name, os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	// Cross another threshold through the public Set path.
	for i := n * 3 / 4; i < 2*n; i++ {
		if err := db.Set(keyFor(i), value); err != nil {
			t.Fatal(err)
		}
	}
	if len(db.index.slots) != 4*n {
		t.Fatalf("capacity: got %d, want %d", len(db.index.slots), 4*n)
	}
	for i := 0; i < 2*n; i++ {
		if i%3 == 0 {
			err = db.Delete(keyFor(i))
		} else if i%3 == 1 {
			err = db.Set(keyFor(i), []byte("updated"))
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2*n; i++ {
		want := value
		if i%3 == 0 {
			want = nil
		} else if i%3 == 1 {
			want = []byte("updated")
		}
		got, err := db.Get(keyFor(i))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("key %d: got %q, want %q, error %v", i, got, want, err)
		}
	}
}

func TestDBDeleteMissDoesNotWriteLog(t *testing.T) {
	db, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.log.headFile.Close(); err != nil {
			t.Fatal(err)
		}
	})
	checkMissingDelete := func(key []byte) {
		t.Helper()
		before, err := db.log.headFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		size := db.log.size
		for range 2 {
			if err := db.Delete(key); err != nil {
				t.Fatal(err)
			}
		}
		after, err := db.log.headFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if after.Size() != before.Size() || db.log.size != size {
			t.Fatalf("deleting missing key %q grew log: file %d -> %d, counter %d -> %d",
				key, before.Size(), after.Size(), size, db.log.size)
		}
	}
	key, value := []byte("key"), []byte("value")
	checkMissingDelete(key)
	if err := db.Set(key, value); err != nil {
		t.Fatal(err)
	}
	checkMissingDelete([]byte("missing"))
	if err := db.Delete(key); err != nil {
		t.Fatal(err)
	}
	checkMissingDelete(key)
}

func TestIndexLogFailure(t *testing.T) {
	db, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.log.headFile.Close() })
	db.index.slots = make([]uint64, 32)
	nonce := 0
	keyFor := func(home uint64) []byte {
		for {
			key := fmt.Appendf(nil, "key-%d", nonce)
			nonce++
			if db.index.hash(key)&31 == home {
				return key
			}
		}
	}
	keys := [][]byte{keyFor(31), keyFor(31), keyFor(0)}
	value := []byte("value")
	for _, key := range keys {
		if err := db.Set(key, value); err != nil {
			t.Fatal(err)
		}
	}
	before := slices.Clone(db.index.slots)
	name := db.log.headFile.Name()
	if err := db.log.headFile.Close(); err != nil {
		t.Fatal(err)
	}
	// Reads succeed, but committing the log record fails after preparation.
	db.log.headFile, err = os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range []func() error{
		func() error { return db.Set(keyFor(30), value) },
		func() error { return db.Set(keys[0], []byte("updated")) },
		func() error { return db.Delete(keys[0]) },
	} {
		if err := operation(); err == nil {
			t.Fatal("write to read-only log succeeded")
		}
		if !slices.Equal(before, db.index.slots) || db.index.population != len(keys) {
			t.Fatal("failed log write changed the index")
		}
	}
	for _, key := range keys {
		got, err := db.Get(key)
		if err != nil || !bytes.Equal(got, value) {
			t.Fatalf("key %q: got %q, error %v", key, got, err)
		}
	}
	if err := db.log.headFile.Close(); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []func() error{
		func() error { _, err := db.Get(keys[0]); return err },
		func() error { return db.Set(keys[0], value) },
		func() error { return db.Delete(keys[0]) },
	} {
		if err := operation(); err == nil {
			t.Fatal("read from closed log succeeded")
		}
		if !slices.Equal(before, db.index.slots) || db.index.population != len(keys) {
			t.Fatal("failed log read changed the index")
		}
	}
}
