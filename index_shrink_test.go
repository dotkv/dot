package dot

import (
	"maps"
	"math/rand/v2"
	"slices"
	"testing"
)

func newSizedInternalIndex(t testing.TB, capacity int) *Index {
	t.Helper()
	idx := &Index{}
	var err error
	idx.slots, err = idx.memory.allocate(capacity)
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

func TestIndexShrink(t *testing.T) {
	const n = 2 << indexBits
	for _, pattern := range []string{"random", "merged wrapping runs", "crossing halves"} {
		t.Run(pattern, func(t *testing.T) {
			idx := newSizedInternalIndex(t, n)
			idx.segmentRefCounts = make(map[uint64]uint32)
			idx.expirations = make(map[uint64]indexExpiration)
			random := rand.New(rand.NewPCG(14, 22))
			entries := make([]indexEntry, n/4-1)
			for id := range entries {
				hash := random.Uint64()
				switch pattern {
				case "merged wrapping runs":
					hash = hash&^uint64(n-1) | uint64(n/2-1+(id%2)*(n/2))
				case "crossing halves":
					home := (n - 32 + id%64) & (n - 1)
					if id%3 == 0 {
						home ^= n / 2
					}
					hash = hash&^uint64(n-1) | uint64(home)
				}
				entries[id] = indexEntry{hash: hash, live: true}
				slot := entrySlot(id, hash)
				idx.insert(int(hash&uint64(n-1)), slot)
				if id%257 == 0 {
					idx.expirations[slot&addressMask] = indexExpiration{at: 1 << 62, hash: hash, recordBytes: 48}
				}
			}
			idx.liveBytes = uint64(len(entries)) * 48
			refs, expirations := maps.Clone(idx.segmentRefCounts), maps.Clone(idx.expirations)
			checkIndex(t, idx, entries)
			if err := idx.shrink(); err != nil {
				t.Fatal(err)
			}
			if len(idx.slots) != n/2 || len(idx.memory.current) != n/2*8 || idx.memory.previous != nil {
				t.Fatal("shrink did not halve and release the old mapping")
			}
			if idx.liveBytes != uint64(len(entries))*48 || !maps.Equal(refs, idx.segmentRefCounts) || !maps.Equal(expirations, idx.expirations) {
				t.Fatal("shrink changed live-byte, segment-reference, or expiration accounting")
			}
			checkIndex(t, idx, entries)
			for id := 0; id < len(entries); id += 137 {
				removeEntry(t, idx, entries, id)
			}
			checkIndex(t, idx, entries)
		})
	}
}

func TestIndexShrinkThresholdsAndRegrowth(t *testing.T) {
	const minimum = 1 << indexBits
	idx := newSizedInternalIndex(t, 4*minimum)
	random := rand.New(rand.NewPCG(32, 56))
	var entries []indexEntry
	add := func() {
		hash := random.Uint64()
		id := len(entries)
		entries = append(entries, indexEntry{hash: hash, live: true})
		idx.insert(int(hash&uint64(len(idx.slots)-1)), entrySlot(id, hash))
	}
	for idx.entryCount < len(idx.slots)/4 {
		add()
	}
	if err := idx.shrink(); err != nil {
		t.Fatal(err)
	}
	if len(idx.slots) != 4*minimum {
		t.Fatal("shrank at exactly 25% occupancy")
	}
	removeEntry(t, idx, entries, 0)
	if err := idx.shrink(); err != nil {
		t.Fatal(err)
	}
	if len(idx.slots) != 2*minimum {
		t.Fatal("did not shrink below 25% occupancy")
	}
	for _, resize := range []func() error{idx.grow, idx.shrink} {
		if err := resize(); err != nil {
			t.Fatal(err)
		}
		if len(idx.slots) != 2*minimum {
			t.Fatal("immediate resizing reversed a shrink")
		}
	}
	for idx.entryCount < len(idx.slots)*3/4 {
		add()
	}
	if err := idx.grow(); err != nil {
		t.Fatal(err)
	}
	if len(idx.slots) != 4*minimum {
		t.Fatal("did not regrow at 75% occupancy")
	}
	checkIndex(t, idx, entries)
	for id, entry := range entries {
		if entry.live {
			removeEntry(t, idx, entries, id)
		}
	}
	for _, want := range []int{2 * minimum, minimum, minimum} {
		if err := idx.shrink(); err != nil {
			t.Fatal(err)
		}
		if len(idx.slots) != want {
			t.Fatalf("empty index capacity=%d, want %d", len(idx.slots), want)
		}
		checkIndex(t, idx, entries)
	}
}

func FuzzIndexShrink(f *testing.F) {
	f.Add([]byte{0, 0, 1, 1, 2, 127, 128, 255, 30, 31, 32, 33})
	f.Add(make([]byte, 256))
	f.Fuzz(func(t *testing.T, data []byte) {
		const n = 2 << indexBits
		idx := newSizedInternalIndex(t, n)
		entries := make([]indexEntry, min(len(data), 256))
		for id := range entries {
			home := (n - 128 + int(data[id])) & (n - 1)
			if data[id]&1 != 0 {
				home ^= n / 2
			}
			hash := uint64(home) | uint64(id)<<20
			entries[id] = indexEntry{hash: hash, live: true}
			idx.insert(home, entrySlot(id, hash))
		}
		if err := idx.shrink(); err != nil {
			t.Fatal(err)
		}
		checkIndex(t, idx, entries)
		for id := 0; id < len(entries); id += 3 {
			removeEntry(t, idx, entries, id)
		}
		checkIndex(t, idx, entries)
	})
}

// Compare resize costs at their respective load thresholds with the same source
// capacity. The fixtures contain no log or keys, so neither path can read disk.
func BenchmarkIndexResize(b *testing.B) {
	for _, pattern := range []string{"random", "clustered"} {
		for _, operation := range []string{"grow", "shrink"} {
			b.Run(pattern+"/"+operation, func(b *testing.B) {
				const n = 4 << indexBits
				count := n * 3 / 4
				if operation == "shrink" {
					count = n/4 - 1
				}
				idx := newSizedInternalIndex(b, n)
				random := rand.New(rand.NewPCG(24, 41))
				for id := range count {
					hash := random.Uint64()
					if pattern == "random" {
						idx.insert(int(hash&uint64(n-1)), entrySlot(id, hash))
					} else {
						// Two long, disjoint source runs that merge during shrink.
						home := n/2 - 1 + (id%2)*(n/2)
						pos := (home + id/2) & (n - 1)
						hash = hash&^uint64(n-1) | uint64(home)
						slot := entrySlot(id, hash)
						if id < 2 {
							slot |= occupiedMask
						} else {
							slot |= continuationMask | shiftedMask
						}
						idx.slots[pos] = slot
					}
				}
				fixture := slices.Clone(idx.slots)
				if err := idx.close(); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				b.StopTimer()
				for range b.N {
					var err error
					idx.slots, err = idx.memory.allocate(n)
					if err != nil {
						b.Fatal(err)
					}
					copy(idx.slots, fixture)
					idx.entryCount = count
					b.StartTimer()
					if operation == "grow" {
						err = idx.grow()
					} else {
						err = idx.shrink()
					}
					b.StopTimer()
					if err != nil {
						b.Fatal(err)
					}
					if err := idx.close(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
