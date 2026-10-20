package dot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/cespare/xxhash/v2"
)

const (
	shardBits  = 6
	shardCount = 1 << shardBits
)

// ErrNotFound is returned directly by Get when the key does not exist.
var ErrNotFound = errors.New("key not found")

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

// Set stores val using the encoding described by Value. It does not retain key
// or val, so callers may reuse their byte slices after Set returns.
func (db *DB) Set[T Value](key []byte, val T) error {
	var encoded [8]byte
	var value []byte
	switch v := any(val).(type) {
	case []byte:
		value = v
	case int64:
		binary.LittleEndian.PutUint64(encoded[:], uint64(v))
		value = encoded[:]
	case uint64:
		binary.LittleEndian.PutUint64(encoded[:], v)
		value = encoded[:]
	case float64:
		binary.LittleEndian.PutUint64(encoded[:], math.Float64bits(v))
		value = encoded[:]
	case bool:
		if v {
			encoded[0] = 1
		}
		value = encoded[:1]
	default:
		return ErrInvalidValue
	}
	s, hash := db.shard(key)
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	change, err := s.index.prepareSet(s.log, key, value, hash)
	if err != nil {
		return fmt.Errorf("prepare set: %w", err)
	}
	if change.kind == indexUnchanged {
		return nil
	}
	segment, off, err := s.log.write('s', key, value)
	if err != nil {
		return fmt.Errorf("write log: %w", err)
	}
	// The append may rotate, so use its actual address when committing.
	change.slot |= segment<<26 | off
	s.index.commit(change)
	return nil
}

// Get returns the value decoded as T. A missing key returns the zero value of T
// and ErrNotFound directly, without wrapping the sentinel.
// Byte slices are owned by the caller. Scalar encodings are described by Value.
func (db *DB) Get[T Value](key []byte) (T, error) {
	var zero T
	s, hash := db.shard(key)
	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.closed {
		return zero, os.ErrClosed
	}
	// Select the representation here; index lookups only locate and read it.
	var value T
	var pos int
	var err error
	if out, ok := any(&value).(*[]byte); ok {
		pos, *out, err = s.index.findPayload(s.log, key, hash, nil)
	} else {
		var raw scalarValue
		pos, raw, err = s.index.findScalar(s.log, key, hash)
		if err == nil && pos >= 0 {
			value, err = decodeScalar[T](raw)
		}
	}
	if err != nil {
		return zero, fmt.Errorf("index get: %w", err)
	}
	if pos < 0 {
		return zero, ErrNotFound
	}
	return value, nil
}

// Delete removes key. Deleting a missing key succeeds.
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
