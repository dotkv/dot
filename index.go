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
	slots []uint64 // [type 2][hashtag 16][segment 20][offset 26]
	seed  maphash.Seed
}

func newIndex() *Index {
	return &Index{
		slots: make([]uint64, 1<<16),
		seed:  maphash.MakeSeed(),
	}
}

func (idx *Index) hash(key []byte) uint64 {
	return maphash.Bytes(idx.seed, key)
}

func (idx *Index) prepareSet(log *Log, key []byte, val []byte) (uint64, uint64, error) {
	pos := idx.hash(key) % uint64(len(idx.slots))
	slot := idx.slots[pos]
	for slot&occupiedMask > 0 {
		k, v, err := log.read(slot&segmentMask>>26, slot&offsetMask)
		if err != nil {
			return 0, 0, fmt.Errorf("log read: %w", err)
		}
		if bytes.Equal(k, key) {
			if bytes.Equal(val, v) {
				return pos, slot, nil
			}
			break
		}
		pos = (pos + 1) % uint64(len(idx.slots))
		slot = idx.slots[pos]
	}
	slot = log.size | occupiedMask
	return pos, slot, nil
}

func (idx *Index) prepareDelete(log *Log, key []byte) (uint64, uint64, error) {
	pos := idx.hash(key) % uint64(len(idx.slots))
	slot := idx.slots[pos]
	for slot&occupiedMask > 0 {
		k, _, err := log.read(slot&segmentMask>>26, slot&offsetMask)
		if err != nil {
			return 0, 0, fmt.Errorf("log read: %w", err)
		}
		if bytes.Equal(k, key) {
			return pos, tombstoneMask, nil
		}
		pos = (pos + 1) % uint64(len(idx.slots))
		slot = idx.slots[pos]
	}
	return pos, 0, nil
}

func (idx *Index) get(log *Log, key []byte) ([]byte, error) {
	pos := idx.hash(key) % uint64(len(idx.slots))
	slot := idx.slots[pos]
	for slot&occupiedMask > 0 {
		k, v, err := log.read(slot&segmentMask>>26, slot&offsetMask)
		if err != nil {
			return nil, fmt.Errorf("log read: %w", err)
		}
		if bytes.Equal(k, key) {
			return v, nil
		}
		pos = (pos + 1) % uint64(len(idx.slots))
		slot = idx.slots[pos]
	}
	return nil, nil
}

func (idx *Index) commit(i uint64, slot uint64) {
	idx.slots[i] = slot
}
