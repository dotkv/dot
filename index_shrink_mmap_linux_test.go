package dot

import (
	"errors"
	"syscall"
	"testing"
	"unsafe"
)

func TestIndexShrinkMappingLifecycle(t *testing.T) {
	idx := newSizedInternalIndex(t, 2<<indexBits)
	address := uintptr(unsafe.Pointer(&idx.memory.current[0]))
	checkIndexMapping(t, address, true)
	if err := idx.shrink(); err != nil {
		t.Fatal(err)
	}
	checkIndexMapping(t, address, false)
	address = uintptr(unsafe.Pointer(&idx.memory.current[0]))
	checkIndexMapping(t, address, true)
	if err := idx.close(); err != nil {
		t.Fatal(err)
	}
	checkIndexMapping(t, address, false)
}

func TestIndexShrinkRetainsFailedUnmap(t *testing.T) {
	const n = 2 << indexBits
	idx := newSizedInternalIndex(t, n)
	entries := make([]indexEntry, 100)
	for id := range entries {
		hash := uint64(n/2-1+(id%2)*(n/2)) | uint64(id)<<20
		entries[id] = indexEntry{hash: hash, live: true}
		idx.insert(int(hash&uint64(n-1)), entrySlot(id, hash))
	}
	mapping := idx.memory.current
	address := uintptr(unsafe.Pointer(&mapping[0]))
	t.Cleanup(func() {
		if len(idx.memory.previous) != 0 && &idx.memory.previous[0] == &mapping[0] {
			idx.memory.previous = mapping
		} else if len(idx.memory.current) != 0 && &idx.memory.current[0] == &mapping[0] {
			idx.memory.current = mapping
		}
	})
	idx.memory.current = mapping[:len(mapping)-1]
	if err := idx.shrink(); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("shrink with failed unmap: got %v, want EINVAL", err)
	}
	if len(idx.slots) != n/2 || idx.memory.previous == nil {
		t.Fatal("failed unmap lost the old mapping or rebuilt index")
	}
	checkIndex(t, idx, entries)
	checkIndexMapping(t, address, true)
	current := &idx.memory.current[0]
	if err := idx.shrink(); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("pending cleanup: got %v, want EINVAL", err)
	}
	if &idx.memory.current[0] != current {
		t.Fatal("allocated another replacement before releasing the previous mapping")
	}
	idx.memory.previous = mapping
	if err := idx.shrink(); err != nil {
		t.Fatal(err)
	}
	if len(idx.slots) != n/2 || idx.memory.previous != nil {
		t.Fatal("cleanup at minimum capacity retained the old mapping or resized again")
	}
	checkIndexMapping(t, address, false)
	checkIndex(t, idx, entries)
}
