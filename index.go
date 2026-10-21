package dot

import (
	"bytes"
	"errors"
	"fmt"
	"time"
)

const (
	indexBits               = 16
	offsetMask       uint64 = (1 << 26) - 1
	segmentMask      uint64 = ((1 << 20) - 1) << 26
	addressMask      uint64 = segmentMask | offsetMask
	hashtagMask      uint64 = ((1 << 15) - 1) << 46
	occupiedMask     uint64 = 1 << 63
	continuationMask uint64 = 1 << 62
	shiftedMask      uint64 = 1 << 61
	metadataMask            = occupiedMask | continuationMask | shiftedMask
)

type indexExpiration struct {
	at   int64
	hash uint64
}

type Index struct {
	// [occupied 1][continuation 1][shifted 1][hashtag 15][segment 20][offset 26]
	// Entries with the same home bucket form a run, ordered by home bucket.
	// occupied belongs to the bucket, even when its entries are displaced;
	// continuation and shifted belong to the entry. Together these bits encode
	// displacement without a per-entry counter or a limit on collision chains.
	slots       []uint64
	entryCount  int
	memory      indexMemory
	expirations map[uint64]indexExpiration
	// Shared with Log so every index mutation, including expiry pruning,
	// keeps the per-segment reference counts current. Nil for standalone indexes.
	segmentRefCounts map[uint64]uint32
}

// Carry changes by value so insertion/deletion shifts happen only after the
// log write succeeds.
type indexChange struct {
	home, pos  int
	slot       uint64
	prevAddr   uint64
	hash       uint64
	expiration int64
	hasPrevExp bool
	kind       uint8
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
	idx.entryCount = 0
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

func (idx *Index) setExpiration(addr uint64, at int64, hash uint64) {
	if idx.expirations == nil {
		idx.expirations = make(map[uint64]indexExpiration)
	}
	idx.expirations[addr] = indexExpiration{at: at, hash: hash}
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
	if idx.entryCount < n-n/4 {
		return nil
	}
	if len(idx.expirations) > 0 {
		idx.prune()
		if idx.entryCount < n-n/4-n/8 {
			return nil
		}
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
	idx.entryCount++
	if idx.segmentRefCounts != nil {
		idx.segmentRefCounts[slot&segmentMask>>26]++
	}
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
	if idx.segmentRefCounts != nil {
		idx.segmentRefCounts[idx.slots[pos]&segmentMask>>26]--
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

// findPayload returns the matching slot, value, and expiration, or position -1
// for a miss. A nil buf gives Get owned storage; Set and Delete lend their
// write buffer. Consume the value before reusing that buffer. The hash is computed by DB.
func (idx *Index) findPayload(log *Log, key []byte, hash uint64, buf []byte) (int, []byte, int64, error) {
	return idx.lookupPayload(log, key, hash, buf, buf == nil)
}

// lookupPayload can skip expired entries even when a caller borrows scratch
// storage. Set and Delete must locate expired entries to replace or remove them.
func (idx *Index) lookupPayload(log *Log, key []byte, hash uint64, buf []byte, liveOnly bool) (int, []byte, int64, error) {
	mask := len(idx.slots) - 1
	home := int(hash & uint64(mask))
	if idx.slots[home]&occupiedMask == 0 {
		return -1, nil, 0, nil
	}
	tag := hashtag(hash)
	pos := idx.run(home)
	for {
		slot := idx.slots[pos]
		if slot&hashtagMask == tag && (!liveOnly || !idx.slotExpired(slot)) {
			k, v, expiration, err := log.readPayload(slot&segmentMask>>26, slot&offsetMask, buf)
			if err != nil {
				return 0, nil, 0, fmt.Errorf("log read: %w", err)
			}
			if bytes.Equal(k, key) {
				return pos, v, expiration, nil
			}
			// Reuse this payload for later fingerprint matches, including when
			// read allocated because buf was nil or too small.
			buf = k[:cap(k)]
		}
		pos = (pos + 1) & mask
		if idx.slots[pos]&continuationMask == 0 {
			return -1, nil, 0, nil
		}
	}
}

func (idx *Index) prepareSet(log *Log, key []byte, val []byte, hash uint64, expiration int64) (indexChange, error) {
	pos, previous, prevExp, err := idx.findPayload(log, key, hash, log.writeBuffer[:])
	if err != nil {
		return indexChange{}, err
	}
	if pos < 0 {
		if err := idx.grow(); err != nil {
			return indexChange{}, err
		}
		return indexChange{
			home:       int(hash & uint64(len(idx.slots)-1)),
			slot:       hashtag(hash),
			hash:       hash,
			expiration: expiration,
			kind:       indexInsert,
		}, nil
	}
	if prevExp == expiration && !isExpired(prevExp) && bytes.Equal(val, previous) {
		return indexChange{}, nil
	}
	return idx.prepareUpdate(pos, hash, expiration, prevExp), nil
}

// replay applies an existing record at addr without writing or deduplicating it.
// An expired set must remove the previous value so recovery cannot resurrect it.
func (idx *Index) replay(log *Log, ty byte, key []byte, hash, addr uint64, expiration int64) error {
	expired := isExpired(expiration)
	if ty == 'd' || expired {
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
	pos, _, prevExp, err := idx.findPayload(log, key, hash, log.writeBuffer[:])
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
			slot:       hashtag(hash),
			hash:       hash,
			expiration: expiration,
			kind:       indexInsert,
		}
	} else {
		change = idx.prepareUpdate(pos, hash, expiration, prevExp)
	}
	change.slot |= addr
	idx.commit(change)
	return nil
}

// prepareUpdate reuses a successful lookup under the shard's exclusive lock.
func (idx *Index) prepareUpdate(pos int, hash uint64, expiration, prevExp int64) indexChange {
	return indexChange{
		pos:        pos,
		slot:       idx.slots[pos] &^ addressMask,
		prevAddr:   idx.slots[pos] & addressMask,
		hash:       hash,
		expiration: expiration,
		hasPrevExp: prevExp != 0,
		kind:       indexUpdate,
	}
}

func (idx *Index) prepareDelete(log *Log, key []byte, hash uint64) (indexChange, error) {
	pos, _, expiration, err := idx.findPayload(log, key, hash, log.writeBuffer[:])
	if err != nil {
		return indexChange{}, err
	}
	if pos < 0 {
		return indexChange{}, nil
	}
	home := int(hash & uint64(len(idx.slots)-1))
	prevAddr := idx.slots[pos] & addressMask
	if isExpired(expiration) {
		idx.deleteExpiration(prevAddr)
		idx.remove(home, pos)
		return indexChange{}, nil
	}
	return indexChange{
		home:       home,
		pos:        pos,
		prevAddr:   prevAddr,
		hasPrevExp: expiration != 0,
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
	tag := hashtag(hash)
	pos := idx.run(home)
	for {
		slot := idx.slots[pos]
		if slot&hashtagMask == tag && !idx.slotExpired(slot) {
			raw, expiration, matched, err := log.readScalar(slot&segmentMask>>26, slot&offsetMask, key)
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
		idx.insert(change.home, change.slot)
		if change.expiration != 0 {
			idx.setExpiration(change.slot&addressMask, change.expiration, change.hash)
		}
	case indexUpdate:
		if idx.segmentRefCounts != nil {
			idx.segmentRefCounts[change.prevAddr>>26]--
			idx.segmentRefCounts[change.slot&segmentMask>>26]++
		}
		if change.hasPrevExp {
			idx.deleteExpiration(change.prevAddr)
		}
		idx.slots[change.pos] = change.slot
		if change.expiration != 0 {
			idx.setExpiration(change.slot&addressMask, change.expiration, change.hash)
		}
	case indexDelete:
		if change.hasPrevExp {
			idx.deleteExpiration(change.prevAddr)
		}
		idx.remove(change.home, change.pos)
	}
}
