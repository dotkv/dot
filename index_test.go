package dot

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"testing"
	"time"
)

type indexEntry struct {
	hash uint64
	live bool
}

func newInternalIndex(t testing.TB) *Index {
	t.Helper()
	idx, err := newIndex()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := idx.close(); err != nil {
			t.Error(err)
		}
	})
	return idx
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
	if idx.entryCount != live {
		t.Fatalf("entryCount: got %d, want %d", idx.entryCount, live)
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
	idx := newInternalIndex(t)
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
	for idx.entryCount < n*3/4 {
		add(random.Uint64())
	}
	checkIndex(t, idx, entries)
	for id := 0; id < 600; id += 3 {
		removeEntry(t, idx, entries, id)
	}
	// Deletions count as free space and do not cause premature growth.
	if err := idx.grow(); err != nil {
		t.Fatal(err)
	}
	if len(idx.slots) != n {
		t.Fatal("grew below the load threshold")
	}
	for growth := 0; growth < 3; growth++ {
		for idx.entryCount < len(idx.slots)*3/4 {
			add(random.Uint64())
		}
		size, entryCount := len(idx.slots), idx.entryCount
		if err := idx.grow(); err != nil {
			t.Fatal(err)
		}
		if len(idx.slots) != size*2 || idx.entryCount != entryCount {
			t.Fatal("grow changed entryCount or did not double capacity")
		}
		checkIndex(t, idx, entries)
	}
}

func TestIndexAllocations(t *testing.T) {
	idx := newInternalIndex(t)
	for i := 0; i < len(idx.slots)*3/4; i++ {
		idx.insert(i, entrySlot(i, uint64(i)))
	}
	// Keep a heap-backed fixture: each growth unmaps its previous storage.
	slots, entryCount := slices.Clone(idx.slots), idx.entryCount
	if err := idx.close(); err != nil {
		t.Fatal(err)
	}
	allocations := testing.AllocsPerRun(5, func() {
		idx.slots, idx.entryCount = slots, entryCount
		if err := idx.grow(); err != nil {
			t.Fatal(err)
		}
		if err := idx.close(); err != nil {
			t.Fatal(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("grow: got %g Go heap allocations, want 0", allocations)
	}
	idx = &Index{slots: make([]uint64, 32)}
	allocations = testing.AllocsPerRun(1000, func() {
		idx.insert(31, 0)
		idx.insert(31, 1)
		idx.insert(0, 2)
		idx.remove(31, 0)
		idx.remove(31, 31)
		idx.remove(0, 0)
		if err := idx.grow(); err != nil {
			t.Fatal(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("insert/remove/below-threshold grow: got %g allocations", allocations)
	}
	idx = newInternalIndex(t)
	log := &Log{}
	key := []byte("key")
	const hash = uint64(42)
	allocations = testing.AllocsPerRun(1000, func() {
		change, err := idx.prepareSet(log, key, nil, hash, 0)
		if err != nil {
			t.Fatal(err)
		}
		idx.commit(change)
		idx.remove(change.home, change.home)
		if _, err := idx.prepareDelete(log, key, hash); err != nil {
			t.Fatal(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("prepare/commit: got %g allocations", allocations)
	}
}

func TestDBGrow(t *testing.T) {
	db := newInternalDB(t)
	s := &db.shards[0]
	n := len(s.index.slots)
	keys := keysForShard(db, s, "grow", 2*n)
	keyFor := func(i int) []byte { return keys[i] }
	var err error
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
		if len(s.index.slots) != n {
			t.Fatal("updating an existing key grew the table")
		}
	}
	// Growing must work with the log closed; reopen it to check every value.
	name := s.log.headFile.Name()
	if err := s.log.headFile.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.index.grow(); err != nil {
		t.Fatal(err)
	}
	if len(s.index.slots) != 2*n {
		t.Fatal("grow with the log closed did not double capacity")
	}
	s.log.headFile, err = os.OpenFile(name, os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	// Cross another threshold through the public Set path.
	for i := n * 3 / 4; i < 2*n; i++ {
		if err := db.Set(keyFor(i), value); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.index.slots) != 4*n {
		t.Fatalf("capacity: got %d, want %d", len(s.index.slots), 4*n)
	}
	for i := 0; i < 2*n; i++ {
		switch i % 3 {
		case 0:
			err = db.Delete(keyFor(i))
		case 1:
			err = db.Set(keyFor(i), []byte("updated"))
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2*n; i++ {
		want := value
		var wantErr error
		switch i % 3 {
		case 0:
			want = nil
			wantErr = ErrNotFound
		case 1:
			want = []byte("updated")
		}
		got, err := db.Get[[]byte](keyFor(i))
		if err != wantErr || !bytes.Equal(got, want) {
			t.Fatalf("key %d: got %q, want %q, error %v", i, got, want, err)
		}
	}
	checkSegmentAccounting(t, s)
}

func TestDBSetSameValueDoesNotWriteLog(t *testing.T) {
	for _, tc := range []struct {
		name            string
		value, repeated []byte
		boundary        bool
	}{
		{"nonempty", []byte("value"), []byte("value"), false},
		{"nil-to-empty", nil, []byte{}, false},
		{"empty-to-nil", []byte{}, nil, false},
		{"rotation-boundary", []byte("value"), []byte("value"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newInternalDB(t)
			key := []byte("key")
			if err := db.Set(key, tc.value); err != nil {
				t.Fatal(err)
			}
			s, _ := db.shard(key)
			if tc.boundary {
				padLog(t, s.log, logSegmentSize)
			}
			head, size := s.log.headFile, s.log.size
			before, err := head.Stat()
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if err := db.Set(key, tc.repeated); err != nil {
					t.Fatal(err)
				}
			}
			if s.log.headFile != head || s.log.headSegment != 0 || s.log.size != size {
				t.Fatal("setting an unchanged value appended to or rotated the log")
			}
			after, err := head.Stat()
			if err != nil {
				t.Fatal(err)
			}
			if after.Size() != before.Size() {
				t.Fatalf("unchanged value grew log: %d -> %d", before.Size(), after.Size())
			}
			if s.index.entryCount != 1 {
				t.Fatalf("index entryCount: got %d, want 1", s.index.entryCount)
			}
			if got, err := db.Get[[]byte](key); err != nil || !bytes.Equal(got, tc.value) {
				t.Fatalf("unchanged value: got %q, want %q, error %v", got, tc.value, err)
			}
		})
	}
}

func TestDBDeleteMissDoesNotWriteLog(t *testing.T) {
	db := newInternalDB(t)
	checkMissingDelete := func(key []byte) {
		t.Helper()
		s, _ := db.shard(key)
		before, err := s.log.headFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		size := s.log.size
		for range 2 {
			if err := db.Delete(key); err != nil {
				t.Fatal(err)
			}
		}
		after, err := s.log.headFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if after.Size() != before.Size() || s.log.size != size {
			t.Fatalf("deleting missing key %q grew log: file %d -> %d, counter %d -> %d",
				key, before.Size(), after.Size(), size, s.log.size)
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

	// Deleting an expired key drops its index slot without writing to the log.
	if err := db.Set(key, value, WithExpiration(time.Now().Add(-time.Minute))); err != nil {
		t.Fatal(err)
	}
	s, _ := db.shard(key)
	if s.index.entryCount != 1 {
		t.Fatalf("entryCount after expired set: got %d, want 1", s.index.entryCount)
	}
	checkMissingDelete(key)
	if s.index.entryCount != 0 {
		t.Fatalf("entryCount after deleting expired key: got %d, want 0", s.index.entryCount)
	}
}

func TestDBExpirationLogAndIndexBehavior(t *testing.T) {
	db := newInternalDB(t)
	key, value := []byte("key"), []byte("value")
	s, _ := db.shard(key)
	future1 := time.Now().Add(time.Hour)
	future2 := future1.Add(time.Hour)
	past := time.Now().Add(-time.Hour)

	if err := db.Set(key, value, WithExpiration(future1)); err != nil {
		t.Fatal(err)
	}
	size := s.log.size
	// Repeating the same unexpired value and expiration skips the log write.
	if err := db.Set(key, value, WithExpiration(future1)); err != nil {
		t.Fatal(err)
	}
	if s.log.size != size {
		t.Fatalf("unchanged set with same expiration grew log: %d -> %d", size, s.log.size)
	}

	// Changing expiration with the same value writes a new record and reuses the slot.
	for _, opts := range [][]Option{
		{WithExpiration(future2)},
		nil,
		{WithExpiration(past)},
		{WithExpiration(past)},
		nil,
	} {
		before := s.log.size
		if err := db.Set(key, value, opts...); err != nil {
			t.Fatal(err)
		}
		if s.log.size <= before {
			t.Fatalf("set with opts %v did not write to log", opts)
		}
		if s.index.entryCount != 1 {
			t.Fatalf("entryCount: got %d, want 1", s.index.entryCount)
		}
	}
	if got, err := db.Get[[]byte](key); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("got %q, error %v; want %q", got, err, value)
	}
}

func TestDBExpirationPruneOnGrow(t *testing.T) {
	// Verify that Get on an expired key skips log reads via the sidecar, even
	// when the underlying log file is closed.
	db := newInternalDB(t)
	key, value := []byte("expired"), []byte("value")
	past := time.Now().Add(-time.Minute)
	future := time.Now().Add(time.Hour)
	if err := db.Set(key, value, WithExpiration(past)); err != nil {
		t.Fatal(err)
	}
	s, _ := db.shard(key)
	if err := s.log.headFile.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get[[]byte](key); err != ErrNotFound || got != nil {
		t.Fatalf("expired byte Get with closed log: got %q, error %v", got, err)
	}
	if got, err := db.Get[int64](key); err != ErrNotFound || got != 0 {
		t.Fatalf("expired scalar Get with closed log: got %d, error %v", got, err)
	}

	// Verify prune-on-grow hysteresis (62.5% = n - n/4 - n/8) and invariants.
	idx := newInternalIndex(t)
	n := len(idx.slots)
	threshold := n - n/4
	watermark := threshold - n/8
	random := rand.New(rand.NewPCG(5, 6))
	var entries []indexEntry
	add := func(at int64) int {
		id := len(entries)
		hash := random.Uint64()
		entries = append(entries, indexEntry{hash: hash, live: true})
		slot := entrySlot(id, hash)
		idx.insert(int(hash&uint64(len(idx.slots)-1)), slot)
		if at != 0 {
			idx.setExpiration(slot&addressMask, at, hash)
		}
		return id
	}

	// Fill to 75% (threshold) with enough expired entries (> n/8) so pruning
	// drops entryCount below the 62.5% watermark and skips doubling.
	liveTarget := watermark - 1
	for idx.entryCount < liveTarget {
		add(future.UnixNano())
	}
	var expiredIDs []int
	for idx.entryCount < threshold {
		expiredIDs = append(expiredIDs, add(past.UnixNano()))
	}
	if err := idx.grow(); err != nil {
		t.Fatal(err)
	}
	for _, id := range expiredIDs {
		entries[id].live = false
	}
	if len(idx.slots) != n {
		t.Fatalf("table doubled despite pruning below 62.5%%: got %d, want %d", len(idx.slots), n)
	}
	if idx.entryCount != liveTarget {
		t.Fatalf("entryCount after prune-without-grow: got %d, want %d", idx.entryCount, liveTarget)
	}
	checkIndex(t, idx, entries)

	// Refill to 75% with only a few expired entries so pruning leaves entryCount
	// at or above the 62.5% watermark and doubles the table.
	expiredIDs = expiredIDs[:0]
	for idx.entryCount < threshold-4 {
		add(0)
	}
	for idx.entryCount < threshold {
		expiredIDs = append(expiredIDs, add(past.UnixNano()))
	}
	if err := idx.grow(); err != nil {
		t.Fatal(err)
	}
	for _, id := range expiredIDs {
		entries[id].live = false
	}
	if len(idx.slots) != 2*n {
		t.Fatalf("table did not double when prune stayed >= 62.5%%: got %d, want %d", len(idx.slots), 2*n)
	}
	if idx.entryCount != threshold-4 {
		t.Fatalf("entryCount after prune-and-grow: got %d, want %d", idx.entryCount, threshold-4)
	}
	checkIndex(t, idx, entries)
}

func TestIndexLogFailure(t *testing.T) {
	db := newInternalDB(t)
	s := &db.shards[0]
	if err := s.index.close(); err != nil {
		t.Fatal(err)
	}
	var mapErr error
	s.index.slots, mapErr = s.index.memory.allocate(32)
	if mapErr != nil {
		t.Fatal(mapErr)
	}
	nonce := 0
	keyFor := func(home uint64) []byte {
		for {
			key := fmt.Appendf(nil, "key-%d", nonce)
			nonce++
			if candidate, hash := db.shard(key); candidate == s && hash&31 == home {
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
	before := slices.Clone(s.index.slots)
	name := s.log.headFile.Name()
	if err := s.log.headFile.Close(); err != nil {
		t.Fatal(err)
	}
	// Reads succeed, but committing the log record fails after preparation.
	var err error
	s.log.headFile, err = os.Open(name)
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
		if !slices.Equal(before, s.index.slots) || s.index.entryCount != len(keys) {
			t.Fatal("failed log write changed the index")
		}
	}
	for _, key := range keys {
		got, err := db.Get[[]byte](key)
		if err != nil || !bytes.Equal(got, value) {
			t.Fatalf("key %q: got %q, error %v", key, got, err)
		}
	}
	if err := s.log.headFile.Close(); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []func() error{
		func() error { _, err := db.Get[[]byte](keys[0]); return err },
		func() error { return db.Set(keys[0], value) },
		func() error { return db.Delete(keys[0]) },
	} {
		if err := operation(); err == nil {
			t.Fatal("read from closed log succeeded")
		}
		if !slices.Equal(before, s.index.slots) || s.index.entryCount != len(keys) {
			t.Fatal("failed log read changed the index")
		}
	}
}
