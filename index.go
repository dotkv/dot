package dot

import (
	"bytes"
	"fmt"
	"hash/maphash"
)

const (
	offsetMask    uint64 = (1 << 26) - 1
	segmentMask   uint64 = ((1 << 20) - 1) << 26
	occupiedMask  uint64 = 1 << 63
	tombstoneMask uint64 = 1 << 62
)

type Index struct {
	slots      []uint64 // [type 2][hashtag 16][segment 20][offset 26]
	population int      // number of occupied (live + tombstones)
	seed       maphash.Seed
}

func newIndex() *Index {
	return &Index{
		slots:      make([]uint64, 1<<16),
		population: 0,
		seed:       maphash.MakeSeed(),
	}
}

func (idx *Index) hash(key []byte) uint64 {
	return maphash.Bytes(idx.seed, key)
}

func (idx *Index) grow() {
	if float64(idx.population)/float64(len(idx.slots)) > 0.75 {
		panic("unimplemented index grow")
	}
}

func (idx *Index) prepareSet(log *Log, key []byte, val []byte) (uint64, uint64, error) {
	idx.grow()
	n := uint64(len(idx.slots))
	pos := idx.hash(key) % n
	slot := idx.slots[pos]
	tombstone := n
	for slot&occupiedMask > 0 {
		if slot&tombstoneMask == 0 {
			k, v, err := log.read(slot&segmentMask>>26, slot&offsetMask)
			if err != nil {
				return 0, 0, fmt.Errorf("log read: %w", err)
			}
			if bytes.Equal(k, key) {
				if bytes.Equal(val, v) {
					return pos, slot, nil
				}
				return pos, log.size | occupiedMask, nil
			}
		} else if tombstone == n {
			tombstone = pos
		}
		pos = (pos + 1) % n
		slot = idx.slots[pos]
	}
	if tombstone != n {
		return tombstone, log.size | occupiedMask, nil
	}
	return pos, log.size | occupiedMask, nil
}

func (idx *Index) prepareDelete(log *Log, key []byte) (uint64, uint64, error) {
	n := uint64(len(idx.slots))
	pos := idx.hash(key) % n
	originalPos := pos
	slot := idx.slots[pos]
	originalSlot := slot
	for slot&occupiedMask > 0 {
		if slot&tombstoneMask == 0 {
			k, _, err := log.read(slot&segmentMask>>26, slot&offsetMask)
			if err != nil {
				return 0, 0, fmt.Errorf("log read: %w", err)
			}
			if bytes.Equal(k, key) {
				return pos, tombstoneMask | occupiedMask, nil
			}
		}
		pos = (pos + 1) % n
		slot = idx.slots[pos]
	}
	return originalPos, originalSlot, nil
}

func (idx *Index) get(log *Log, key []byte) ([]byte, error) {
	n := uint64(len(idx.slots))
	pos := idx.hash(key) % n
	slot := idx.slots[pos]
	for slot&occupiedMask > 0 {
		if slot&tombstoneMask == 0 {
			k, v, err := log.read(slot&segmentMask>>26, slot&offsetMask)
			if err != nil {
				return nil, fmt.Errorf("log read: %w", err)
			}
			if bytes.Equal(k, key) {
				return v, nil
			}
		}
		pos = (pos + 1) % n
		slot = idx.slots[pos]
	}
	return nil, nil
}

func (idx *Index) commit(i uint64, slot uint64) {
	if idx.slots[i] != slot {
		if idx.slots[i]&occupiedMask == 0 && slot&occupiedMask > 0 {
			idx.population++
		}
		idx.slots[i] = slot
	}
}
