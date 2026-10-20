package dot

import (
	"encoding/binary"
	"errors"
	"os"
	"syscall"
	"testing"
	"unsafe"
)

// Probe the mapping without dereferencing freed memory. mincore reports ENOMEM
// for an unmapped page, even when the old Go slice still has a nonzero length.
func checkIndexMapping(t *testing.T, address uintptr, mapped bool) {
	t.Helper()
	var residency byte
	_, _, errno := syscall.Syscall(syscall.SYS_MINCORE, address,
		uintptr(os.Getpagesize()), uintptr(unsafe.Pointer(&residency)))
	want := syscall.Errno(0)
	if !mapped {
		want = syscall.ENOMEM
	}
	if errno != want {
		t.Fatalf("mapping at %#x: got %v, want %v", address, errno, want)
	}
}

func TestIndexMappingLifecycle(t *testing.T) {
	idx := newInternalIndex(t)
	n := len(idx.slots)
	oldAddress := uintptr(unsafe.Pointer(&idx.memory.current[0]))
	checkIndexMapping(t, oldAddress, true)
	for i := 0; i < n*3/4; i++ {
		idx.insert(i, entrySlot(i, uint64(i)))
	}
	if binary.NativeEndian.Uint64(idx.memory.current[8:16]) != idx.slots[1] {
		t.Fatal("slots do not share the mapped storage")
	}
	if err := idx.grow(); err != nil {
		t.Fatal(err)
	}
	checkIndexMapping(t, oldAddress, false)
	if len(idx.slots) != 2*n || len(idx.memory.current) != 2*n*8 {
		t.Fatal("growth did not double the mapped storage")
	}
	for i := 0; i < n*3/4; i++ {
		if idx.slots[i] != entrySlot(i, uint64(i))|occupiedMask {
			t.Fatalf("entry %d changed during mapped growth", i)
		}
	}
	address := uintptr(unsafe.Pointer(&idx.memory.current[0]))
	checkIndexMapping(t, address, true)
	for range 2 {
		if err := idx.close(); err != nil {
			t.Fatal(err)
		}
	}
	checkIndexMapping(t, address, false)
	if idx.slots != nil || idx.memory.current != nil || idx.memory.previous != nil || idx.population != 0 {
		t.Fatal("closed index retains storage or population")
	}
}

func TestIndexMemoryInvalidCapacity(t *testing.T) {
	for _, n := range []int{0, -1, int(^uint(0) >> 1)} {
		var memory indexMemory
		slots, err := memory.allocate(n)
		if err == nil || slots != nil || memory.current != nil || memory.previous != nil {
			memory.close()
			t.Fatalf("capacity %d: got slots=%d error=%v", n, len(slots), err)
		}
	}
}

func TestIndexCloseRetriesUnmap(t *testing.T) {
	idx := newInternalIndex(t)
	mapping := idx.memory.current
	address := uintptr(unsafe.Pointer(&mapping[0]))
	// A shortened slice is rejected by Munmap without releasing the memory.
	// Restore it before cleanup even if an assertion fails.
	t.Cleanup(func() {
		if idx.memory.current != nil {
			idx.memory.current = mapping
		}
	})
	idx.memory.current = mapping[:len(mapping)-1]
	if err := idx.close(); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("close with invalid mapping: got %v, want EINVAL", err)
	}
	if idx.memory.current == nil || idx.slots != nil {
		t.Fatal("failed close did not retain the mapping or invalidate the slots")
	}
	checkIndexMapping(t, address, true)
	idx.memory.current = mapping
	if err := idx.close(); err != nil {
		t.Fatal(err)
	}
	checkIndexMapping(t, address, false)
}

func TestIndexGrowRetainsFailedUnmap(t *testing.T) {
	idx := newInternalIndex(t)
	mapping := idx.memory.current
	address := uintptr(unsafe.Pointer(&mapping[0]))
	n := len(idx.slots)
	for i := 0; i < n*3/4; i++ {
		idx.insert(i, entrySlot(i, uint64(i)))
	}
	t.Cleanup(func() {
		if len(idx.memory.previous) != 0 && &idx.memory.previous[0] == &mapping[0] {
			idx.memory.previous = mapping
		} else if len(idx.memory.current) != 0 && &idx.memory.current[0] == &mapping[0] {
			idx.memory.current = mapping
		}
	})
	idx.memory.current = mapping[:len(mapping)-1]
	if err := idx.grow(); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("grow with failed unmap: got %v, want EINVAL", err)
	}
	if len(idx.slots) != 2*n || idx.memory.previous == nil {
		t.Fatal("failed unmap lost the old mapping or the rebuilt index")
	}
	for i := 0; i < n*3/4; i++ {
		if idx.slots[i] != entrySlot(i, uint64(i))|occupiedMask {
			t.Fatalf("entry %d changed after failed unmap", i)
		}
	}
	checkIndexMapping(t, address, true)
	currentAddress := uintptr(unsafe.Pointer(&idx.memory.current[0]))
	// At the next threshold, failed cleanup must stop growth before a third
	// mapping is allocated. Existing slots must remain usable.
	for i := n * 3 / 4; i < 2*n*3/4; i++ {
		idx.insert(i, entrySlot(i, uint64(i)))
	}
	if err := idx.grow(); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("grow with pending unmap: got %v, want EINVAL", err)
	}
	if len(idx.slots) != 2*n || uintptr(unsafe.Pointer(&idx.memory.current[0])) != currentAddress {
		t.Fatal("allocated replacement before releasing the previous mapping")
	}
	for i := 0; i < 2*n*3/4; i++ {
		if idx.slots[i] != entrySlot(i, uint64(i))|occupiedMask {
			t.Fatalf("entry %d changed after blocked growth", i)
		}
	}
	idx.memory.previous = mapping
	if err := idx.grow(); err != nil {
		t.Fatalf("retry growth after failed unmap: %v", err)
	}
	if len(idx.slots) != 4*n || idx.memory.previous != nil {
		t.Fatal("growth retry did not release both earlier mappings")
	}
	checkIndexMapping(t, currentAddress, false)
	currentAddress = uintptr(unsafe.Pointer(&idx.memory.current[0]))
	if err := idx.close(); err != nil {
		t.Fatal(err)
	}
	// The replacement could reuse the first address; probe it after close.
	checkIndexMapping(t, address, false)
	checkIndexMapping(t, currentAddress, false)
}
