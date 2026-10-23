package dot

import (
	"fmt"
	"sync"
	"time"
)

// Shard owns a log, an index, and the lock that coordinates access to them.
// Shards are created and managed by DB and must not be copied after first use.
type Shard struct {
	log          *Log
	index        *Index
	lock         sync.RWMutex
	closed       bool
	metricsIndex int         // Routes operation metrics without hashing the key again.
	flushTimer   *time.Timer // Guarded by lock, including its callback.

	compactionThreshold *compactionThreshold
	compactionQueue     chan<- *Shard
	compactionQueued    bool
	// A failed pass waits for the ticker instead of retrying on every write.
	compactionRetry bool
}

// appendSet requires the shard's exclusive lock. Commit only after the append
// succeeds, using its actual address because writing may rotate the log.
func (s *Shard) appendSet(ty recordType, key, value []byte, change indexChange, durability Durability) error {
	if change.kind == indexUnchanged {
		return nil
	}
	segment, off, err := s.log.writeDurability(ty, key, value, change.expiration, durability)
	if err != nil {
		return fmt.Errorf("write log: %w", err)
	}
	change.slot |= logAddress(segment, off)
	change.recordBytes = recordSize(len(key), len(value))
	s.index.commit(change)
	s.maybeScheduleCompaction()
	return nil
}

// finishDurability requires the shard's exclusive lock. A timer is armed by
// the first pending write; later writes never postpone its deadline.
func (s *Shard) finishDurability(durability Durability) error {
	if s.log.writeErr != nil {
		return s.log.writeErr
	}
	if durability == Buffer {
		if len(s.log.pending) != 0 && s.flushTimer == nil {
			s.flushTimer = time.AfterFunc(logFlushInterval, func() {
				s.lock.Lock()
				defer s.lock.Unlock()
				s.flushTimer = nil
				if !s.closed {
					if err := s.log.flush(); err != nil {
						s.log.writeErr = fmt.Errorf("background buffer flush: %w", err)
					}
				}
			})
		}
		return nil
	}
	if err := s.log.flush(); err != nil {
		return err
	}
	if durability == Sync {
		if err := s.log.headFile.Sync(); err != nil {
			return fmt.Errorf("sync log: %w", err)
		}
	}
	return nil
}

type compactionThreshold struct {
	ratio            float64
	minObsoleteBytes uint64
}

func defaultCompactionThreshold() compactionThreshold {
	return compactionThreshold{ratio: 0.33, minObsoleteBytes: 1 << 20}
}

func (s *Shard) threshold() compactionThreshold {
	if s.compactionThreshold != nil {
		return *s.compactionThreshold
	}
	return defaultCompactionThreshold()
}

// needsCompaction requires the shard lock. Byte counters avoid scanning the
// segment map on the foreground write path.
func (s *Shard) needsCompaction(minObsoleteBytes uint64) bool {
	total := s.log.recordBytes
	if total == 0 {
		return false
	}
	obsolete := total - s.index.liveBytes
	return obsolete >= minObsoleteBytes && float64(obsolete)/float64(total) > s.threshold().ratio
}

// maybeScheduleCompaction requires the shard lock, except during Open before
// the worker starts. A bounded, deduplicated queue never blocks a writer.
func (s *Shard) maybeScheduleCompaction() {
	if s.compactionQueue == nil || s.compactionQueued || s.compactionRetry || s.closed || s.log.writeErr != nil {
		return
	}
	if !s.log.compactPending && !s.needsCompaction(s.threshold().minObsoleteBytes) {
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
