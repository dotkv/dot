package dot

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/cespare/xxhash/v2"
)

const (
	shardBits  = 6
	shardCount = 1 << shardBits
)

type DB struct {
	path   string
	shards [shardCount]shard
}

type shard struct {
	log    *Log
	index  *Index
	lock   sync.RWMutex
	closed bool
}

// New creates a database with 64 key shards in directories 00 through 63.
func New(path string) (*DB, error) {
	db := &DB{path: path}
	for i := range db.shards {
		log, err := newLog(filepath.Join(path, fmt.Sprintf("%02d", i)))
		if err != nil {
			return nil, errors.Join(fmt.Errorf("open shard %02d: %w", i, err), db.Close())
		}
		db.shards[i].log = log
		db.shards[i].index, err = newIndex()
		if err != nil {
			return nil, errors.Join(fmt.Errorf("create shard %02d index: %w", i, err), db.Close())
		}
	}
	return db, nil
}

func (db *DB) shard(key []byte) (*shard, uint64) {
	// XXH64 with seed zero is part of the shard layout: keep it stable so
	// future replay can rebuild each index from its existing shard's logs.
	hash := xxhash.Sum64(key)
	// Keep shard bits separate from the low bits used by index buckets and
	// fingerprints, including the bits used to split buckets during growth.
	return &db.shards[hash>>(64-shardBits)], hash
}

func (db *DB) Set(key []byte, val []byte) error {
	s, hash := db.shard(key)
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	change, err := s.index.prepareSet(s.log, key, val, hash)
	if err != nil {
		return fmt.Errorf("prepare set: %w", err)
	}
	if change.kind == indexUnchanged {
		return nil
	}
	segment, off, err := s.log.write('s', key, val)
	if err != nil {
		return fmt.Errorf("write log: %w", err)
	}
	// The append may rotate, so use its actual address when committing.
	change.slot |= segment<<26 | off
	s.index.commit(change)
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	s, hash := db.shard(key)
	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.closed {
		return nil, os.ErrClosed
	}
	val, err := s.index.get(s.log, key, hash)
	if err != nil {
		return nil, fmt.Errorf("index get: %w", err)
	}
	return val, nil
}

func (db *DB) Delete(key []byte) error {
	s, hash := db.shard(key)
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	change, err := s.index.prepareDelete(s.log, key, hash)
	if err != nil {
		return fmt.Errorf("prepare delete: %w", err)
	}
	if change.kind == indexUnchanged {
		return nil
	}
	if _, _, err := s.log.write('d', key, nil); err != nil {
		return fmt.Errorf("write log: %w", err)
	}
	s.index.commit(change)
	return nil
}

// Close waits for active operations and releases the logs and index mappings
// without removing the database files. Subsequent operations return os.ErrClosed.
// Close may be called again to retry an unsuccessful unmap.
func (db *DB) Close() error {
	return db.close(false)
}

// Destroy closes the database and removes its directory.
func (db *DB) Destroy() error {
	return db.close(true)
}

func (db *DB) close(removeFiles bool) error {
	// Lock in directory order to wait for in-flight operations on every shard.
	for i := range db.shards {
		db.shards[i].lock.Lock()
		defer db.shards[i].lock.Unlock()
	}
	var err error
	for i := range db.shards {
		s := &db.shards[i]
		if !s.closed && s.log != nil {
			if closeErr := s.log.headFile.Close(); closeErr != nil && !errors.Is(closeErr, os.ErrClosed) {
				err = errors.Join(err, fmt.Errorf("close shard %02d: %w", i, closeErr))
			}
		}
		s.closed = true
		if s.index != nil {
			if unmapErr := s.index.close(); unmapErr != nil {
				err = errors.Join(err, fmt.Errorf("close shard %02d index: %w", i, unmapErr))
			}
		}
	}
	if removeFiles {
		err = errors.Join(err, os.RemoveAll(db.path))
	}
	return err
}
