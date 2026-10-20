package dot

import (
	"bytes"
	"errors"
	"fmt"
)

const (
	indexBits               = 16
	offsetMask       uint64 = (1 << 26) - 1
	segmentMask      uint64 = ((1 << 20) - 1) << 26
	hashtagMask      uint64 = ((1 << 15) - 1) << 46
	occupiedMask     uint64 = 1 << 63
	continuationMask uint64 = 1 << 62
	shiftedMask      uint64 = 1 << 61
	metadataMask            = occupiedMask | continuationMask | shiftedMask
)

type Index struct {
	// [occupied 1][continuation 1][shifted 1][hashtag 15][segment 20][offset 26]
	// Entries with the same home bucket form a run, ordered by home bucket.
	// occupied belongs to the bucket, even when its entries are displaced;
	// continuation and shifted belong to the entry. Together these bits encode
	// displacement without a per-entry counter or a limit on collision chains.
	slots      []uint64
	population int
	memory     indexMemory
}

// Carry changes by value so insertion/deletion shifts happen only after the
// log write succeeds.
type indexChange struct {
	home, pos int
	slot      uint64
	kind      uint8
}

const (
	indexUnchanged uint8 = iota
	indexInsert
	indexUpdate
	indexDelete
)

func newIndex() (*Index, error) {
	var memory indexMemory
	slots, err := memory.allocate(1 << indexBits)
	if err != nil {
		return nil, fmt.Errorf("map index: %w", err)
	}
	return &Index{
		slots:  slots,
		memory: memory,
	}, nil
}

// close requires the shard's exclusive lock, as does grow.
func (idx *Index) close() error {
	idx.slots = nil
	idx.population = 0
	return idx.memory.close()
}

func hashtag(hash uint64) uint64 {
	// Keep the bits immediately above the initial bucket bits, so growing
	// can split a bucket using its fingerprint instead of reading its keys.
	return (hash >> indexBits << 46) & hashtagMask
}

func empty(slot uint64) bool {
	return slot&metadataMask == 0
}

func (idx *Index) grow() error {
	n := len(idx.slots)
	if idx.population < n-n/4 {
		return nil
	}
	// The 15 fingerprint bits support tables up to 2^31 slots (16 GiB).
	if uint64(n) >= 1<<(indexBits+15) {
		return errors.New("index capacity exceeded")
	}
	slots, err := idx.memory.allocate(2 * n)
	if err != nil {
		return fmt.Errorf("grow index: %w", err)
	}
	old := idx.slots

	// An empty bucket gives both walks a common starting point, including
	// when a cluster wraps around the end of the table. Match each run to
	// the next occupied home bucket, then split it using the next hash bit.
	start := 0
	for !empty(old[start]) {
		start++
	}
	// Relative to this empty bucket, each new half receives an ordered
	// subset of the old runs. Append through two cursors rather than probe
	// again for every entry, keeping growth linear even for long runs.
	next := [2]int{0, n}
	last := [2]int{-1, -1}
	home := start
	for i := 1; i <= n; i++ {
		slot := old[(start+i)&(n-1)]
		if empty(slot) {
			continue
		}
		if slot&continuationMask == 0 {
			for {
				home = (home + 1) & (n - 1)
				if old[home]&occupiedMask != 0 {
					break
				}
			}
		}
		bucket := home | int((slot&hashtagMask)>>(46-indexBits))&n
		relative := (bucket - start - 1) & (2*n - 1)
		half := 0
		if relative >= n {
			half = 1
		}
		pos := max(next[half], relative)
		next[half] = pos + 1
		pos = (pos + start + 1) & (2*n - 1)
		slot &^= metadataMask
		if last[half] == bucket {
			slot |= continuationMask
		}
		if pos != bucket {
			slot |= shiftedMask
		}
		slots[bucket] |= occupiedMask
		slots[pos] = slot | slots[pos]&occupiedMask
		last[half] = bucket
	}
	idx.slots = slots
	return idx.memory.releasePrevious()
}

// run returns the first entry belonging to home. Its occupied bit must
// already be set. Walk back to the cluster start, then pair home buckets
// with runs until we reach the requested bucket.
func (idx *Index) run(home int) int {
	mask := len(idx.slots) - 1
	bucket := home
	for idx.slots[bucket]&shiftedMask != 0 {
		bucket = (bucket - 1) & mask
	}
	pos := bucket
	for bucket != home {
		for {
			pos = (pos + 1) & mask
			if idx.slots[pos]&continuationMask == 0 {
				break
			}
		}
		for {
			bucket = (bucket + 1) & mask
			if idx.slots[bucket]&occupiedMask != 0 {
				break
			}
		}
	}
	return pos
}

func (idx *Index) insert(home int, slot uint64) {
	idx.population++
	if empty(idx.slots[home]) {
		idx.slots[home] = slot | occupiedMask
		return
	}
	mask := len(idx.slots) - 1
	occupied := idx.slots[home]&occupiedMask != 0
	idx.slots[home] |= occupiedMask
	pos := idx.run(home)
	if occupied {
		for {
			pos = (pos + 1) & mask
			if idx.slots[pos]&continuationMask == 0 {
				break
			}
		}
		slot |= continuationMask
	}
	if pos != home {
		slot |= shiftedMask
	}
	for {
		previous := idx.slots[pos]
		idx.slots[pos] = slot&^occupiedMask | previous&occupiedMask
		if empty(previous) {
			return
		}
		slot = previous | shiftedMask
		pos = (pos + 1) & mask
	}
}

func (idx *Index) remove(home, pos int) {
	mask := len(idx.slots) - 1
	next := (pos + 1) & mask
	first := idx.slots[pos]&continuationMask == 0
	if first && idx.slots[next]&continuationMask == 0 {
		idx.slots[home] &^= occupiedMask
	}
	// Close the hole, preserving bucket occupancy at each position. At a
	// run boundary, advance home so we know when an entry stops being shifted.
	for idx.slots[next]&shiftedMask != 0 {
		slot := idx.slots[next]
		if slot&continuationMask == 0 {
			for {
				home = (home + 1) & mask
				if idx.slots[home]&occupiedMask != 0 {
					break
				}
			}
		}
		if first {
			slot &^= continuationMask
			first = false
		}
		slot &^= shiftedMask
		if pos != home {
			slot |= shiftedMask
		}
		idx.slots[pos] = slot&^occupiedMask | idx.slots[pos]&occupiedMask
		pos = next
		next = (next + 1) & mask
	}
	idx.slots[pos] = 0
	idx.population--
}

// find uses the hash computed by DB and may return a value backed by buf;
// consume it before reusing the buffer.
func (idx *Index) find(log *Log, key []byte, hash uint64, buf []byte) (int, []byte, error) {
	mask := len(idx.slots) - 1
	home := int(hash & uint64(mask))
	if idx.slots[home]&occupiedMask == 0 {
		return -1, nil, nil
	}
	tag := hashtag(hash)
	pos := idx.run(home)
	for {
		slot := idx.slots[pos]
		if slot&hashtagMask == tag {
			k, v, err := log.read(slot&segmentMask>>26, slot&offsetMask, buf)
			if err != nil {
				return 0, nil, fmt.Errorf("log read: %w", err)
			}
			if bytes.Equal(k, key) {
				return pos, v, nil
			}
			// Reuse this payload for later fingerprint matches, including when
			// read allocated because buf was nil or too small.
			buf = k[:cap(k)]
		}
		pos = (pos + 1) & mask
		if idx.slots[pos]&continuationMask == 0 {
			return -1, nil, nil
		}
	}
}

func (idx *Index) prepareSet(log *Log, key []byte, val []byte, hash uint64) (indexChange, error) {
	pos, previous, err := idx.find(log, key, hash, log.writeBuffer[:])
	if err != nil {
		return indexChange{}, err
	}
	if pos < 0 {
		if err := idx.grow(); err != nil {
			return indexChange{}, err
		}
		return indexChange{
			home: int(hash & uint64(len(idx.slots)-1)),
			slot: hashtag(hash),
			kind: indexInsert,
		}, nil
	}
	if bytes.Equal(val, previous) {
		return indexChange{}, nil
	}
	return indexChange{
		pos:  pos,
		slot: idx.slots[pos] &^ (segmentMask | offsetMask),
		kind: indexUpdate,
	}, nil
}

func (idx *Index) prepareDelete(log *Log, key []byte, hash uint64) (indexChange, error) {
	pos, _, err := idx.find(log, key, hash, log.writeBuffer[:])
	if err != nil {
		return indexChange{}, err
	}
	if pos < 0 {
		return indexChange{}, nil
	}
	return indexChange{
		home: int(hash & uint64(len(idx.slots)-1)),
		pos:  pos,
		kind: indexDelete,
	}, nil
}

func (idx *Index) get(log *Log, key []byte, hash uint64) ([]byte, error) {
	_, val, err := idx.find(log, key, hash, nil)
	return val, err
}

func (idx *Index) commit(change indexChange) {
	switch change.kind {
	case indexInsert:
		idx.insert(change.home, change.slot)
	case indexUpdate:
		idx.slots[change.pos] = change.slot
	case indexDelete:
		idx.remove(change.home, change.pos)
	}
}
