package dot

import (
	"errors"
	"fmt"
	"os"
	"slices"
)

// compact holds the shard lock for a whole pass so every copied record belongs
// to the same index snapshot. Other shards remain available.
func (s *Shard) compact() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return nil
	}
	l, idx := s.log, s.index
	if l.writeErr != nil {
		return l.writeErr
	}
	idx.prune()
	var total uint64
	for _, count := range l.segmentRecordCounts {
		total += uint64(count)
	}
	if total == 0 || total-uint64(idx.entryCount) <= total/5 {
		return nil
	}
	// Older records may be omitted because of a newer set/delete in the
	// active head. Persist that snapshot before discarding any older history.
	if err := l.headFile.Sync(); err != nil {
		return fmt.Errorf("sync head before compaction: %w", err)
	}
	segments := make([]uint64, 0, len(l.segmentRecordCounts))
	for seg := range l.segmentRecordCounts {
		segments = append(segments, seg)
	}
	segments, err := orderSegments(segments)
	if err != nil {
		return err
	}
	positions := make(map[uint64][]int, len(segments))
	for pos, slot := range idx.slots {
		if !empty(slot) {
			seg := slot & segmentMask >> 26
			positions[seg] = append(positions[seg], pos)
		}
	}
	var pending []int
	rank := make(map[uint64]int, len(segments))
	for i, seg := range segments {
		rank[seg] = i
		slices.SortFunc(positions[seg], func(a, b int) int {
			return int(idx.slots[a]&offsetMask) - int(idx.slots[b]&offsetMask)
		})
		pending = append(pending, positions[seg]...)
		// Clean history oldest first. A delete or expired set is discarded
		// only after all earlier versions it could suppress are gone.
		if l.segmentRecordCounts[seg] != l.segmentRefCounts[seg] {
			if err := l.replaceSegment(idx, seg, positions[seg], nil); err != nil {
				return fmt.Errorf("clean segment %d: %w", seg, err)
			}
		}
	}

	// All remaining physical records now represent the snapshot's current
	// values. Pack them toward the head, publishing identical copies before
	// removing any source. Duplicate copies are harmless after a crash.
	cursor := 0
	for dest := len(segments) - 1; dest >= 0 && cursor < len(pending); dest-- {
		seg := segments[dest]
		own := make([]int, 0, len(positions[seg]))
		for _, pos := range positions[seg] {
			if idx.slots[pos]&segmentMask>>26 == seg {
				own = append(own, pos)
			}
		}
		hasEarlier := rank[idx.slots[pending[cursor]]&segmentMask>>26] < dest
		if !hasEarlier && l.segmentRecordCounts[seg] == l.segmentRefCounts[seg] {
			break
		}
		info, err := os.Stat(l.segmentPath(seg))
		if err != nil {
			return err
		}
		if info.Size() >= logSegmentSize && l.segmentRecordCounts[seg] == l.segmentRefCounts[seg] {
			continue
		}
		next := func() (int, bool) {
			if cursor == len(pending) || rank[idx.slots[pending[cursor]]&segmentMask>>26] >= dest {
				return 0, false
			}
			pos := pending[cursor]
			cursor++
			return pos, true
		}
		if err := l.replaceSegment(idx, seg, own, next); err != nil {
			return fmt.Errorf("pack segment %d: %w", seg, err)
		}
		if !hasEarlier {
			break
		}
	}

	// Only remove the oldest empty prefix. Sync each removal before the next
	// so a crash cannot persist an interior hole and lose the head boundary.
	for _, seg := range segments {
		if seg == l.headSegment || l.segmentRefCounts[seg] != 0 {
			break
		}
		if err := os.Remove(l.segmentPath(seg)); err != nil {
			return err
		}
		delete(l.segmentRecordCounts, seg)
		delete(l.segmentRefCounts, seg)
		if err := l.syncDirectory(); err != nil {
			l.writeErr = fmt.Errorf("persist segment removal; log requires recovery: %w", err)
			return l.writeErr
		}
	}
	return nil
}

type recordMove struct {
	pos                 int
	oldAddress, address uint64
	expiration          indexExpiration
	hasExpiration       bool
}

// replaceSegment atomically publishes a complete replacement. Before rename,
// the original file is authoritative; afterward, the replacement is. Sources
// copied from other segments stay intact until their duplicates can be retired.
// next supplies older records to fill spare space after the segment's own keys.
func (l *Log) replaceSegment(idx *Index, seg uint64, own []int, next func() (int, bool)) (err error) {
	path := l.segmentPath(seg)
	staging := path + ".compact"
	f, err := os.OpenFile(staging, os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if f != nil {
			err = errors.Join(err, f.Close())
		}
		if !published {
			err = errors.Join(err, os.Remove(staging))
		}
	}()
	output := Log{headFile: f, headSegment: seg, segmentRecordCounts: map[uint64]uint32{seg: 0}}
	var moves []recordMove
	buffer := l.writeBuffer[:]
	copyRecord := func(pos int) error {
		old := idx.slots[pos] & addressMask
		key, value, expiration, err := l.readPayload(old>>26, old&offsetMask, buffer)
		if err != nil {
			return err
		}
		buffer = key[:cap(key)]
		if output.size >= logSegmentSize {
			return fmt.Errorf("compaction record exceeds segment capacity")
		}
		_, off, err := output.appendRecord('s', key, value, expiration)
		if err != nil {
			return err
		}
		exp, hasExp := idx.expirations[old]
		moves = append(moves, recordMove{pos: pos, oldAddress: old, address: seg<<26 | off, expiration: exp, hasExpiration: hasExp})
		return nil
	}
	for _, pos := range own {
		if err := copyRecord(pos); err != nil {
			return err
		}
	}
	for next != nil && output.size < logSegmentSize {
		pos, ok := next()
		if !ok {
			break
		}
		if err := copyRecord(pos); err != nil {
			return err
		}
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := os.Rename(staging, path); err != nil {
		return err
	}
	published = true
	// Remove all old expiry addresses first: new offsets may reuse another
	// moved entry's old address in this same segment.
	for _, move := range moves {
		delete(idx.expirations, move.oldAddress)
	}
	for _, move := range moves {
		idx.slots[move.pos] = idx.slots[move.pos]&^addressMask | move.address
		l.segmentRefCounts[move.oldAddress>>26]--
		l.segmentRefCounts[seg]++
		if move.hasExpiration {
			idx.expirations[move.address] = move.expiration
		}
	}
	l.segmentRecordCounts[seg] = uint32(len(moves))
	if seg == l.headSegment {
		// Reopen under the published filename so headFile.Name remains useful.
		// Keep the staging descriptor usable even if reopening fails.
		replacement, openErr := l.openFile(seg, os.O_RDWR|os.O_APPEND)
		if openErr != nil {
			replacement, f = f, nil
		}
		old := l.headFile
		l.headFile, l.size = replacement, output.size
		err = errors.Join(openErr, old.Close())
	}
	err = errors.Join(err, l.syncDirectory())
	if err != nil {
		l.writeErr = fmt.Errorf("persist compaction; log requires recovery: %w", err)
		return l.writeErr
	}
	return nil
}
