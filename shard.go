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
	return nil
}
