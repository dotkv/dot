package dot

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
)

const (
	shardBits  = 6
	shardCount = 1 << shardBits
)

// ErrNotFound is returned directly by Get and Update when the key does not exist.
var ErrNotFound = errors.New("key not found")

type DB struct {
	path             string
	expiration       int64
	shards           [shardCount]Shard
	backgroundCancel context.CancelFunc
	backgroundWG     sync.WaitGroup
}

// Options configures database and key operations.
type Options struct {
	expiration int64
}

// Option configures Open or Set.
type Option func(*Options)

// WithExpiration sets the time at which a key expires. When passed to Open, it
// sets the default expiration for Set calls on that database; when passed to
// Set, it overrides the database default. A zero time.Time clears expiration.
func WithExpiration(t time.Time) Option {
	var ns int64
	if !t.IsZero() {
		ns = t.UnixNano()
		if ns == 0 {
			ns = 1
		}
	}
	return func(o *Options) {
		o.expiration = ns
	}
}

func isExpired(expiration int64) bool {
	return expiration != 0 && expiration <= time.Now().UnixNano()
}

// Open creates or recovers a database with 64 key shards in directories 00 through 63.
func Open(path string, opts ...Option) (*DB, error) {
	var options Options
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	db := &DB{path: path,
		expiration:       options.expiration,
		backgroundCancel: cancel,
	}
	for i := range db.shards {
		var err error
		db.shards[i].index, err = newIndex()
		if err != nil {
			return nil, errors.Join(fmt.Errorf("create shard %02d index: %w", i, err), db.Close())
		}
		db.shards[i].log, err = newLog(filepath.Join(path, fmt.Sprintf("%02d", i)), db.shards[i].index)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("open shard %02d: %w", i, err), db.Close())
		}
	}
	db.backgroundWG.Go(func() { db.background(ctx) })
	return db, nil
}

func (db *DB) background(ctx context.Context) {
	t := time.NewTicker(time.Second * 10)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if ctx.Err() != nil {
				return
			}
			if err := db.compact(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "compact: %v\n", err)
			}
		}
	}
}

func (db *DB) compact(ctx context.Context) error {
	var err error
	for i := range db.shards {
		// Finish the current shard, then stop before starting more shutdown work.
		if ctx.Err() != nil {
			break
		}
		if compactErr := db.shards[i].compact(); compactErr != nil {
			err = errors.Join(err, fmt.Errorf("shard %02d: %w", i, compactErr))
		}
	}
	return err
}

func (db *DB) shard(key []byte) (*Shard, uint64) {
	// XXH64 with seed zero is part of the shard layout: keep it stable so
	// replay can rebuild each index from its existing shard's logs.
	hash := xxhash.Sum64(key)
	// Keep shard bits separate from the low bits used by index buckets and
	// fingerprints, including the bits used to split buckets during growth.
	return &db.shards[hash>>(64-shardBits)], hash
}

// Set stores val using the encoding described by Value. It does not retain key
// or val, so callers may reuse their byte slices after Set returns.
func (db *DB) Set[T Value](key []byte, val T, opts ...Option) error {
	expiration := db.expiration
	if len(opts) > 0 {
		options := Options{expiration: expiration}
		for _, opt := range opts {
			if opt != nil {
				opt(&options)
			}
		}
		expiration = options.expiration
	}
	var encoded [8]byte
	value, err := encodeValue(val, &encoded)
	if err != nil {
		return err
	}
	s, hash := db.shard(key)
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	change, err := s.index.prepareSet(s.log, key, value, hash, expiration)
	if err != nil {
		return fmt.Errorf("prepare set: %w", err)
	}
	return s.appendSet(key, value, change)
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
	var expiration int64
	var err error
	if out, ok := any(&value).(*[]byte); ok {
		pos, *out, expiration, err = s.index.findPayload(s.log, key, hash, nil)
		if err == nil && pos >= 0 && isExpired(expiration) {
			pos = -1
		}
	} else {
		var raw scalarValue
		pos, raw, expiration, err = s.index.findScalar(s.log, key, hash)
		if err == nil && pos >= 0 {
			if isExpired(expiration) {
				pos = -1
			} else {
				value, err = decodeScalar[T](raw)
			}
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

// Update atomically replaces an existing value with fn's result, preserving its
// expiration. Missing or expired keys return ErrNotFound without calling fn;
// invalid scalar encodings also leave the value unchanged without calling fn.
// The callback runs once under the shard's exclusive lock. It must be short and
// must not call methods on this DB. A panic propagates without changing the value.
// Byte slices passed to fn are owned by the caller and may be mutated or retained.
// Returned slices are copied before Update returns; nil stores an empty value.
// An unchanged encoding does not append a record. A write error leaves the index
// unchanged, but cannot undo any external effects of the callback.
func (db *DB) Update[T Value](key []byte, fn func(T) T) error {
	s, hash := db.shard(key)
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	var value T
	var pos int
	var expiration int64
	var err error
	var previous []byte
	var raw scalarValue
	out, byteValue := any(&value).(*[]byte)
	if byteValue {
		// Keep the original in the write scratch space for no-op detection,
		// and give the callback a separate copy it can mutate or retain.
		pos, previous, expiration, err = s.index.lookupPayload(s.log, key, hash, s.log.writeBuffer[:], true)
	} else {
		pos, raw, expiration, err = s.index.findScalar(s.log, key, hash)
	}
	if err != nil {
		return fmt.Errorf("index update: %w", err)
	}
	if pos < 0 || isExpired(expiration) {
		return ErrNotFound
	}
	if byteValue {
		*out = bytes.Clone(previous)
	} else {
		value, err = decodeScalar[T](raw)
		if err != nil {
			return fmt.Errorf("index update: %w", err)
		}
	}
	var encoded [8]byte
	payload, err := encodeValue(fn(value), &encoded)
	if err != nil {
		return err
	}
	if byteValue {
		if bytes.Equal(payload, previous) {
			return nil
		}
	} else if scalarBytes(payload) == raw {
		return nil
	}
	return s.appendSet(key, payload, s.index.prepareUpdate(pos, hash, expiration, expiration))
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
	if _, _, err := s.log.write('d', key, nil, 0); err != nil {
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
	db.backgroundCancel()
	db.backgroundWG.Wait()
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
