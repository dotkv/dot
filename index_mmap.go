package dot

import (
	"errors"
	"fmt"
	"syscall"
	"unsafe"
)

// indexMemory owns the mappings behind an index's pointer-free slots. The shard's
// exclusive lock protects allocation and release; returned views must not escape
// that lifetime. Keep the original byte slices intact for Munmap.
type indexMemory struct {
	current  []byte
	previous []byte
}

// allocate keeps the current slots alive while their replacement is populated.
// A failed release must be retried before another allocation, so at most one
// previous mapping is retained. Allocation failure leaves the current slots valid.
func (m *indexMemory) allocate(n int) ([]uint64, error) {
	const slotSize = int(unsafe.Sizeof(uint64(0)))
	if n <= 0 || n > int(^uint(0)>>1)/slotSize {
		return nil, fmt.Errorf("invalid index capacity %d", n)
	}
	if err := m.releasePrevious(); err != nil {
		return nil, err
	}
	mapping, err := syscall.Mmap(-1, 0, n*slotSize,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
	if err != nil {
		return nil, err
	}
	m.previous, m.current = m.current, mapping
	return unsafe.Slice((*uint64)(unsafe.Pointer(&mapping[0])), n), nil
}

// releasePrevious is called after the index has finished rebuilding its slots.
func (m *indexMemory) releasePrevious() error {
	return unmapIndexMemory(&m.previous)
}

func (m *indexMemory) close() error {
	return errors.Join(unmapIndexMemory(&m.current), m.releasePrevious())
}

// Keep ownership on failure so a later allocation or close can retry the unmap.
func unmapIndexMemory(mapping *[]byte) error {
	if *mapping == nil {
		return nil
	}
	if err := syscall.Munmap(*mapping); err != nil {
		return fmt.Errorf("unmap index: %w", err)
	}
	*mapping = nil
	return nil
}
