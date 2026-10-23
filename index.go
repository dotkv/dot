package dot

import (
	"bytes"
	"errors"
	"fmt"
	"time"
)

const (
	indexBits                = 14
	segmentOffsetBits        = segmentSizeBits - logAlignmentBits
	addressBits              = segmentOffsetBits + segmentIDBits
	fingerprintBits          = 61 - addressBits
	offsetMask        uint64 = (1 << segmentOffsetBits) - 1
	segmentMask       uint64 = ((1 << segmentIDBits) - 1) << segmentOffsetBits
	addressMask       uint64 = segmentMask | offsetMask
	fingerprintMask   uint64 = ((1 << fingerprintBits) - 1) << addressBits
	occupiedMask      uint64 = 1 << 63
	continuationMask  uint64 = 1 << 62
	shiftedMask       uint64 = 1 << 61
	metadataMask             = occupiedMask | continuationMask | shiftedMask
)

type indexExpiration struct {
	at          int64
	hash        uint64
	recordBytes uint64
}

type Index struct {
	// [occupied 1][continuation 1][shifted 1][fingerprint 19][segment 20][offset 22]
	// Offsets address 16-byte units within each segment.
	// Entries with the same home bucket form a run, ordered by home bucket.
	// occupied belongs to the bucket, even when its entries are displaced;
	// continuation and shifted belong to the entry. Together these bits encode
	// displacement without a per-entry counter or a limit on collision chains.
	slots      []uint64
	entryCount int
	// Bytes in records referenced by slots, including TTLs not yet pruned.
	liveBytes   uint64
	memory      indexMemory
	expirations map[uint64]indexExpiration
	// Shared with Log so every index mutation, including expiry pruning,
	// keeps the per-segment reference counts current. Nil for standalone indexes.
	segmentRefCounts map[uint64]uint32
}

// Carry changes by value so insertion/deletion shifts happen only after the
// log write succeeds.
type indexChange struct {
	home, pos   int
	slot        uint64
	prevAddr    uint64
	hash        uint64
	expiration  int64
	hasPrevExp  bool
	kind        uint8
	prevBytes   uint64
	recordBytes uint64
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

// close requires the shard's exclusive lock, as do grow and shrink.
func (idx *Index) close() error {
	idx.slots = nil
	idx.entryCount = 0
	idx.liveBytes = 0
	idx.expirations = nil
	clear(idx.segmentRefCounts)
	return idx.memory.close()
}

func (idx *Index) slotExpired(slot uint64) bool {
	if len(idx.expirations) == 0 {
		return false
	}
	exp, ok := idx.expirations[slot&addressMask]
	return ok && isExpired(exp.at)
}

func (idx *Index) setExpiration(addr uint64, at int64, hash uint64, recordBytes uint64) {
	if idx.expirations == nil {
		idx.expirations = make(map[uint64]indexExpiration)
	}
	idx.expirations[addr] = indexExpiration{at: at, hash: hash, recordBytes: recordBytes}
}

func (idx *Index) deleteExpiration(addr uint64) {
	delete(idx.expirations, addr)
	if len(idx.expirations) == 0 {
		clear(idx.expirations)
	}
}

func (idx *Index) prune() {
	if len(idx.expirations) == 0 {
		return
	}
	mask := len(idx.slots) - 1
	now := time.Now().UnixNano()
	for addr, exp := range idx.expirations {
		if exp.at <= now {
			delete(idx.expirations, addr)
			home := int(exp.hash & uint64(mask))
			if idx.slots[home]&occupiedMask == 0 {
				continue
			}
			pos := idx.run(home)
			for {
				if idx.slots[pos]&addressMask == addr {
					idx.liveBytes -= exp.recordBytes
					idx.remove(home, pos)
					break
				}
				pos = (pos + 1) & mask
				if idx.slots[pos]&continuationMask == 0 {
					break
				}
			}
		}
	}
	if len(idx.expirations) == 0 {
		clear(idx.expirations)
	}
}

func fingerprint(hash uint64) uint64 {
	// Keep the bits immediately above the initial bucket bits, so growing
	// can split a bucket using its fingerprint instead of reading its keys.
	return (hash >> indexBits << addressBits) & fingerprintMask
}

func logAddress(segment, offset uint64) uint64 {
	return segment<<segmentOffsetBits | offset>>logAlignmentBits
}

func addressSegment(address uint64) uint64 {
	return address & segmentMask >> segmentOffsetBits
}

func addressOffset(address uint64) uint64 {
	return (address & offsetMask) << logAlignmentBits
}

func isEmptySlot(slot uint64) bool {
	return slot&metadataMask == 0
}

func (idx *Index) grow() error {
	n := len(idx.slots)
	if idx.entryCount < n-n/4 {
		return nil
	}
	if len(idx.expirations) > 0 {
		idx.prune()
		if idx.entryCount < n-n/4-n/8 {
			return nil
		}
	}
	// The 19 fingerprint bits support tables up to 2^33 slots (64 GiB).
	if uint64(n) >= 1<<(indexBits+fingerprintBits) {
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
	for !isEmptySlot(old[start]) {
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
		if isEmptySlot(slot) {
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
		bucket := home | int((slot&fingerprintMask)>>(addressBits-indexBits))&n
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
	idx.entryCount++
	if idx.segmentRefCounts != nil {
		idx.segmentRefCounts[addressSegment(slot)]++
	}
	if isEmptySlot(idx.slots[home]) {
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
		if isEmptySlot(previous) {
			return
		}
		slot = previous | shiftedMask
		pos = (pos + 1) & mask
	}
}

func (idx *Index) remove(home, pos int) {
	if idx.segmentRefCounts != nil {
		idx.segmentRefCounts[addressSegment(idx.slots[pos])]--
	}
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
	idx.entryCount--
}

// findPayload returns the matching slot, value, type, and expiration, or position -1
// for a miss. A nil buf returns owned storage and skips expired entries for Get.
// Set, Delete, and replay lend scratch storage and must locate expired entries.
// Consume the value before reusing that buffer. The hash is computed by DB.
func (idx *Index) findPayload(log *Log, key []byte, hash uint64, buf []byte) (int, []byte, recordType, int64, error) {
	return idx.lookupPayload(log, key, hash, buf, buf == nil)
}

// lookupPayload can skip expired entries independently of buffer ownership.
// Update uses liveOnly with scratch storage to treat expired keys as absent.
func (idx *Index) lookupPayload(log *Log, key []byte, hash uint64, buf []byte, liveOnly bool) (int, []byte, recordType, int64, error) {
	mask := len(idx.slots) - 1
	home := int(hash & uint64(mask))
	if idx.slots[home]&occupiedMask == 0 {
		return -1, nil, 0, 0, nil
	}
	tag := fingerprint(hash)
	pos := idx.run(home)
	for {
		slot := idx.slots[pos]
		if slot&fingerprintMask == tag && (!liveOnly || !idx.slotExpired(slot)) {
			k, v, ty, expiration, err := log.readPayload(addressSegment(slot), addressOffset(slot), buf)
			if err != nil {
				return 0, nil, 0, 0, fmt.Errorf("log read: %w", err)
			}
			if bytes.Equal(k, key) {
				return pos, v, ty, expiration, nil
			}
			// Reuse this payload for later fingerprint matches, including when
			// read allocated because buf was nil or too small.
			buf = k[:cap(k)]
		}
		pos = (pos + 1) & mask
		if idx.slots[pos]&continuationMask == 0 {
			return -1, nil, 0, 0, nil
		}
	}
}

func (idx *Index) prepareSet(log *Log, ty recordType, key []byte, val []byte, hash uint64, expiration int64) (indexChange, error) {
	pos, previous, prevType, prevExp, err := idx.findPayload(log, key, hash, log.writeBuffer[:])
	if err != nil {
		return indexChange{}, err
	}
	if pos < 0 {
		if err := idx.grow(); err != nil {
			return indexChange{}, err
		}
		return indexChange{
			home:       int(hash & uint64(len(idx.slots)-1)),
			slot:       fingerprint(hash),
			hash:       hash,
			expiration: expiration,
			kind:       indexInsert,
		}, nil
	}
	if prevType == ty && prevExp == expiration && !isExpired(prevExp) && bytes.Equal(val, previous) {
		return indexChange{}, nil
	}
	return idx.prepareUpdate(pos, hash, expiration, prevExp, recordSize(len(key), len(previous))), nil
}

// replay applies an existing record at addr without writing or deduplicating it.
// An expired set must remove the previous value so recovery cannot resurrect it.
func (idx *Index) replay(log *Log, ty recordType, key []byte, hash, addr uint64, expiration int64, recordBytes uint64) error {
	expired := isExpired(expiration)
	if ty == typeDelete || expired {
		if expired {
			// Recovery prunes this TTL from the index immediately. Keep its
			// history eligible for cleanup even though the expiry map is empty.
			log.compactPending = true
		}
		change, err := idx.prepareDelete(log, key, hash)
		if err != nil {
			return err
		}
		idx.commit(change)
		return nil
	}
	pos, previous, _, prevExp, err := idx.findPayload(log, key, hash, log.writeBuffer[:])
	if err != nil {
		return err
	}
	var change indexChange
	if pos < 0 {
		if err := idx.grow(); err != nil {
			return err
		}
		change = indexChange{
			home:       int(hash & uint64(len(idx.slots)-1)),
			slot:       fingerprint(hash),
			hash:       hash,
			expiration: expiration,
			kind:       indexInsert,
		}
	} else {
		change = idx.prepareUpdate(pos, hash, expiration, prevExp, recordSize(len(key), len(previous)))
	}
	change.slot |= addr
	change.recordBytes = recordBytes
	idx.commit(change)
	return nil
}

// prepareUpdate reuses a successful lookup under the shard's exclusive lock.
func (idx *Index) prepareUpdate(pos int, hash uint64, expiration, prevExp int64, prevBytes uint64) indexChange {
	return indexChange{
		pos:        pos,
		slot:       idx.slots[pos] &^ addressMask,
		prevAddr:   idx.slots[pos] & addressMask,
		hash:       hash,
		expiration: expiration,
		hasPrevExp: prevExp != 0,
		prevBytes:  prevBytes,
		kind:       indexUpdate,
	}
}

func (idx *Index) prepareDelete(log *Log, key []byte, hash uint64) (indexChange, error) {
	pos, previous, _, expiration, err := idx.findPayload(log, key, hash, log.writeBuffer[:])
	if err != nil {
		return indexChange{}, err
	}
	if pos < 0 {
		return indexChange{}, nil
	}
	home := int(hash & uint64(len(idx.slots)-1))
	prevAddr := idx.slots[pos] & addressMask
	if isExpired(expiration) {
		idx.liveBytes -= recordSize(len(key), len(previous))
		idx.deleteExpiration(prevAddr)
		idx.remove(home, pos)
		return indexChange{}, nil
	}
	return indexChange{
		home:       home,
		pos:        pos,
		prevAddr:   prevAddr,
		hasPrevExp: expiration != 0,
		prevBytes:  recordSize(len(key), len(previous)),
		kind:       indexDelete,
	}, nil
}

// findScalar returns the matching slot, encoded scalar, and expiration, or
// position -1 for a miss. Log consumes its local record buffer; DB decodes the requested type.
func (idx *Index) findScalar(log *Log, key []byte, hash uint64) (int, scalarValue, int64, error) {
	mask := len(idx.slots) - 1
	home := int(hash & uint64(mask))
	if idx.slots[home]&occupiedMask == 0 {
		return -1, scalarValue{}, 0, nil
	}
	tag := fingerprint(hash)
	pos := idx.run(home)
	for {
		slot := idx.slots[pos]
		if slot&fingerprintMask == tag && !idx.slotExpired(slot) {
			raw, expiration, matched, err := log.readScalar(addressSegment(slot), addressOffset(slot), key)
			if err != nil {
				return 0, scalarValue{}, 0, fmt.Errorf("log read: %w", err)
			}
			if matched {
				return pos, raw, expiration, nil
			}
		}
		pos = (pos + 1) & mask
		if idx.slots[pos]&continuationMask == 0 {
			return -1, scalarValue{}, 0, nil
		}
	}
}

func (idx *Index) commit(change indexChange) {
	switch change.kind {
	case indexInsert:
		idx.liveBytes += change.recordBytes
		idx.insert(change.home, change.slot)
		if change.expiration != 0 {
			idx.setExpiration(change.slot&addressMask, change.expiration, change.hash, change.recordBytes)
		}
	case indexUpdate:
		idx.liveBytes = idx.liveBytes - change.prevBytes + change.recordBytes
		if idx.segmentRefCounts != nil {
			idx.segmentRefCounts[addressSegment(change.prevAddr)]--
			idx.segmentRefCounts[addressSegment(change.slot)]++
		}
		if change.hasPrevExp {
			idx.deleteExpiration(change.prevAddr)
		}
		idx.slots[change.pos] = change.slot
		if change.expiration != 0 {
			idx.setExpiration(change.slot&addressMask, change.expiration, change.hash, change.recordBytes)
		}
	case indexDelete:
		idx.liveBytes -= change.prevBytes
		if change.hasPrevExp {
			idx.deleteExpiration(change.prevAddr)
		}
		idx.remove(change.home, change.pos)
	}
}
