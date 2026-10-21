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
	err := s.compactLocked()
	s.compactionRetry = err != nil
	return err
}

func (s *Shard) compactQueued() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	err := s.compactLocked()
	s.compactionQueued = false
	s.compactionRetry = err != nil
	return err
}

func (s *Shard) compactExpirations() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	// Let an already queued job own its pass. Clearing its queue flag here
	// would permit duplicate entries before the worker consumes the first one.
	if s.closed || s.compactionQueued || (len(s.index.expirations) == 0 && !s.log.compactPending) {
		return nil
	}
	err := s.compactLocked()
	s.compactionRetry = err != nil
	return err
}

// compactLocked requires the shard's exclusive lock for the entire pass.
func (s *Shard) compactLocked() error {
	if s.closed {
		return nil
	}
	l, idx := s.log, s.index
	if l.writeErr != nil {
		return l.writeErr
	}
	idx.prune()
	if !l.compactPending && !s.needsCompaction(0) {
		return nil
	}
	l.compactPending = true
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
	segments, err = l.removeUnusedPrefix(segments)
	if err != nil {
		return err
	}
	rank := make(map[uint64]int, len(segments))
	for i, seg := range segments {
		rank[seg] = i
	}
	positions := make([][]int, len(segments))
	for pos, slot := range idx.slots {
		if !empty(slot) {
			seg := slot & segmentMask >> 26
			positions[rank[seg]] = append(positions[rank[seg]], pos)
		}
	}
	// Clean oldest first; defer head cleanup to packing so it is written once.
	for i, seg := range segments {
		slices.SortFunc(positions[i], func(a, b int) int {
			return int(idx.slots[a]&offsetMask) - int(idx.slots[b]&offsetMask)
		})
		if i < len(segments)-1 && l.segmentRecordCounts[seg] != l.segmentRefCounts[seg] {
			if err := l.replaceSegment(idx, seg, positions[i], nil); err != nil {
				return fmt.Errorf("clean segment %d: %w", seg, err)
			}
		}
	}
	// Consume each source list as its records move toward the head.
	src := 0
	for dest := len(segments) - 1; dest >= 0; dest-- {
		for src < dest && len(positions[src]) == 0 {
			src++
		}
		seg := segments[dest]
		hasEarlier := src < dest
		if !hasEarlier {
			if (dest < len(segments)-1 && len(positions[dest]) == 0) || l.segmentRecordCounts[seg] == l.segmentRefCounts[seg] {
				break
			}
		}
		info, err := os.Stat(l.segmentPath(seg))
		if err != nil {
			return err
		}
		if info.Size() >= logSegmentSize && l.segmentRecordCounts[seg] == l.segmentRefCounts[seg] {
			continue
		}
		next := func() (int, bool) {
			for src < dest && len(positions[src]) == 0 {
				src++
			}
			if src >= dest {
				return 0, false
			}
			pos := positions[src][0]
			positions[src] = positions[src][1:]
			return pos, true
		}
		if err := l.replaceSegment(idx, seg, positions[dest], next); err != nil {
			return fmt.Errorf("pack segment %d: %w", seg, err)
		}
		if !hasEarlier {
			break
		}
	}

	if _, err := l.removeUnusedPrefix(segments); err != nil {
		return err
	}
	l.compactPending = false
	return nil
}

// The caller has synced the head snapshot. Only remove the oldest unused prefix,
// syncing each removal before the next so recovery never sees an interior hole.
func (l *Log) removeUnusedPrefix(segments []uint64) ([]uint64, error) {
	for len(segments) > 1 && l.segmentRefCounts[segments[0]] == 0 {
		seg := segments[0]
		if err := os.Remove(l.segmentPath(seg)); err != nil {
			return segments, err
		}
		l.recordCount -= uint64(l.segmentRecordCounts[seg])
		delete(l.segmentRecordCounts, seg)
		delete(l.segmentRefCounts, seg)
		segments = segments[1:]
		if err := l.syncDirectory(); err != nil {
			l.writeErr = fmt.Errorf("persist segment removal; log requires recovery: %w", err)
			return segments, l.writeErr
		}
	}
	return segments, nil
}

type recordMove struct {
	pos     int
	address uint64
}

// replaceSegment atomically publishes a complete replacement. Before rename,
// the original file is authoritative; afterward, the replacement is. Sources
// copied from other segments stay intact until their duplicates can be retired.
// own must be sorted by source offset; next supplies only older segments.
// Each record starts below logSegmentSize, but its end may cross that boundary.
func (l *Log) replaceSegment(idx *Index, seg uint64, own []int, next func() (int, bool)) (err error) {
	path := l.segmentPath(seg)
	staging := path + ".compact"
	const flags = os.O_CREATE | os.O_EXCL | os.O_RDWR | os.O_APPEND
	f, err := os.OpenFile(staging, flags, 0600)
	if errors.Is(err, os.ErrExist) {
		info, statErr := os.Lstat(staging)
		if statErr != nil {
			return statErr
		}
		if !info.Mode().IsRegular() {
			return err
		}
		// A previous failed cleanup may have left this file. Unlink and create
		// exclusively so a stale hard link cannot truncate a published source.
		if err := os.Remove(staging); err != nil {
			return err
		}
		f, err = os.OpenFile(staging, flags, 0600)
	}
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
		if output.size >= logSegmentSize {
			return fmt.Errorf("compaction record start exceeds segment offset range")
		}
		old := idx.slots[pos] & addressMask
		key, value, ty, expiration, err := l.readPayload(old>>26, old&offsetMask, buffer)
		if err != nil {
			return err
		}
		buffer = key[:cap(key)]
		_, off, err := output.appendRecord(ty, key, value, expiration)
		if err != nil {
			return err
		}
		moves = append(moves, recordMove{pos: pos, address: seg<<26 | off})
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
	// Own records move backward in offset order; imported records come last.
	// A new address can reuse an earlier old address, never a later one.
	for _, move := range moves {
		old := idx.slots[move.pos] & addressMask
		idx.slots[move.pos] = idx.slots[move.pos]&^addressMask | move.address
		l.segmentRefCounts[old>>26]--
		l.segmentRefCounts[seg]++
		if old != move.address && len(idx.expirations) > 0 {
			if exp, ok := idx.expirations[old]; ok {
				delete(idx.expirations, old)
				idx.expirations[move.address] = exp
			}
		}
	}
	l.recordCount = l.recordCount - uint64(l.segmentRecordCounts[seg]) + uint64(len(moves))
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
