package dot

import (
	"fmt"
	"sync"
)

// Shard owns a log, an index, and the lock that coordinates access to them.
// Shards are created and managed by DB and must not be copied after first use.
type Shard struct {
	log    *Log
	index  *Index
	lock   sync.RWMutex
	closed bool

	compactionThreshold *compactionThreshold
	compactionQueue     chan<- *Shard
	compactionQueued    bool
	// A failed pass waits for the ticker instead of retrying on every write.
	compactionRetry bool
}

// appendSet requires the shard's exclusive lock. Commit only after the append
// succeeds, using its actual address because writing may rotate the log.
func (s *Shard) appendSet(key, value []byte, change indexChange) error {
	if change.kind == indexUnchanged {
		return nil
	}
	segment, off, err := s.log.write('s', key, value, change.expiration)
	if err != nil {
		return fmt.Errorf("write log: %w", err)
	}
	change.slot |= segment<<26 | off
	s.index.commit(change)
	s.maybeScheduleCompaction()
	return nil
}

type compactionThreshold struct {
	ratio              float64
	minObsoleteRecords uint64
}

func defaultCompactionThreshold() compactionThreshold {
	return compactionThreshold{ratio: 0.2, minObsoleteRecords: 8192}
}

func (s *Shard) threshold() compactionThreshold {
	if s.compactionThreshold != nil {
		return *s.compactionThreshold
	}
	return defaultCompactionThreshold()
}

// needsCompaction requires the shard lock. recordCount avoids scanning the
// segment map on the foreground write path.
func (s *Shard) needsCompaction(minObsoleteRecords uint64) bool {
	total := s.log.recordCount
	if total == 0 {
		return false
	}
	obsolete := total - uint64(s.index.entryCount)
	return obsolete >= minObsoleteRecords && float64(obsolete)/float64(total) > s.threshold().ratio
}

// maybeScheduleCompaction requires the shard lock, except during Open before
// the worker starts. A bounded, deduplicated queue never blocks a writer.
func (s *Shard) maybeScheduleCompaction() {
	if s.compactionQueue == nil || s.compactionQueued || s.compactionRetry || s.closed || s.log.writeErr != nil {
		return
	}
	if !s.log.compactPending && !s.needsCompaction(s.threshold().minObsoleteRecords) {
		return
	}
	s.log.compactPending = true
	select {
	case s.compactionQueue <- s:
		s.compactionQueued = true
	default:
		// One entry per shard fits in the queue. Retain compactPending even if
		// a future queue policy runs out of room, so the ticker can retry.
	}
}
