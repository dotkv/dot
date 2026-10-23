package dot

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cespare/xxhash/v2"
)

const (
	defaultShardCountBits = 6
	defaultShardCount     = 1 << defaultShardCountBits
	maxShardCount         = 1 << 10
)

// ErrNotFound is returned directly by Get for a missing or expired key.
// Compare it with ==. Update treats these keys as absent; its callback may
// return ErrNotFound to reject an insert, and that error is returned unchanged.
var ErrNotFound = errors.New("key not found")

// ErrDatabaseLocked is returned by Open when another process holds the
// database lock.
var ErrDatabaseLocked = errors.New("database is locked by another process")

// lockName is the file in the database directory guarding single-process
// access. Open holds an exclusive flock on it for the database's lifetime;
// the kernel releases the lock if the process dies, so it can never go stale.
const lockName = "LOCK"

// Key lists the types accepted for keys. The key's type does not affect its
// identity: "a" and []byte("a") address the same record, unlike values, where
// string and []byte are distinct types. String keys are converted to []byte
// once per call; []byte keys are used directly without copying.
type Key interface {
	[]byte | string
}

type DB struct {
	path             string
	expiration       int64
	ttl              time.Duration
	durability       Durability
	shards           []Shard
	shardCountBits   uint
	backgroundCancel context.CancelFunc
	backgroundWG     sync.WaitGroup
	compactionQueue  chan *Shard
	lockFile         *os.File
	metricsContext   context.Context
	metricsCancel    context.CancelFunc
	metricsMu        sync.Mutex // Guards operation collector registration.
	operationMetrics atomic.Pointer[operationRecorder]
	metricsHistoryMu sync.Mutex // Guards history startup and latency registration.
	metricsHistory   *metricsHistory
}

// acquireLock takes an exclusive, non-blocking flock on path/LOCK. It fails
// with ErrDatabaseLocked when another process — or another Open in this
// process — already holds it.
func acquireLock(path string) (*os.File, error) {
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(path, lockName), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w (%s)", ErrDatabaseLocked, path)
		}
		return nil, fmt.Errorf("lock database: %w", err)
	}
	return f, nil
}

// Options configures database and key operations.
type Options struct {
	expiration    int64
	ttl           time.Duration
	expirationSet bool
	condition     func(scalarValue, []byte, bool) error
	durability    Durability
	compaction    compactionThreshold
	shardCount    int
	shardCountSet bool
}

// Option configures Open or a key operation.
type Option func(*Options)

// WithShardCount sets the number of key shards when Open creates a database.
// count must be a power of two between 1 and 1024; the default is 64.
// Existing databases use their stored count when this option is omitted.
// An explicit count must match the stored count or Open fails; changing it
// requires rewriting the database. This option has no effect on key operations.
func WithShardCount(count int) Option {
	return func(o *Options) {
		o.shardCount = count
		o.shardCountSet = true
	}
}

// WithCompactionThreshold configures state-based background compaction in Open.
// A shard is queued after a mutation when its obsolete-byte fraction exceeds
// ratio and it has at least minObsoleteBytes obsolete bytes. Bytes include record
// headers, keys, values, and padding. Defaults are 0.33 and 1 MiB per shard.
// ratio must be finite and in [0, 1); a zero minimum is allowed.
// Expiration sweeps use ratio without the minimum; unfinished passes always
// retry. This option has no effect when passed to Set.
func WithCompactionThreshold(ratio float64, minObsoleteBytes uint64) Option {
	return func(o *Options) {
		o.compaction = compactionThreshold{ratio, minObsoleteBytes}
	}
}

// WithExpiration sets a fixed deadline at which a key becomes unreadable.
// For a relative lifetime, use WithTTL. When passed to Open,
// the same deadline applies to every Set and to Update inserts unless overridden.
// Update, Add, and Sub preserve an existing key's deadline unless overridden.
// A zero time.Time clears expiration. If combined with WithTTL, the last option wins.
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
		o.ttl = 0
		o.expirationSet = true
	}
}

// WithTTL sets a key's lifetime relative to the write, after acquiring the shard
// lock and running any Update callback. The duration is evaluated on each write,
// so the option can be reused or passed to Open as a default. Update, Add, and
// Sub preserve an existing key's deadline unless WithTTL or WithExpiration is
// passed explicitly; inserts use the database default. A zero duration clears
// expiration; a negative duration expires immediately. If combined with
// WithExpiration, the last option wins. Deadlines beyond the disk format's range
// are clamped to its nearest limit. This option has no effect on Delete.
func WithTTL(ttl time.Duration) Option {
	return func(o *Options) {
		o.expiration = 0
		o.ttl = ttl
		o.expirationSet = true
	}
}

// Durability controls when an acknowledged mutation reaches storage.
// Select one mode; these values are alternatives, not bit flags.
type Durability uint8

const (
	// Write writes to the log file before returning, without fsync. The OS
	// may still hold the bytes in its page cache.
	Write Durability = iota
	// Sync writes and fsyncs the shard log before returning.
	Sync
	// Buffer is the default. It copies records into a 64 KiB buffer per active
	// shard. Pending bytes are written without fsync when full or after 100 ms.
	// Reads see buffered mutations immediately. A process crash can lose them.
	Buffer
)

func (d Durability) valid() bool { return d <= Buffer }

// WithDurability sets the default for Open or overrides it for one Set,
// Update, Add, Sub, or Delete. Write and Sync also flush earlier buffered writes
// in the same shard; Sync fsyncs them. This applies even to unchanged values and
// missing deletes. Other shards are unaffected. Close flushes pending buffers
// without fsync. Background flush failures block further mutations in that
// shard and are returned by its subsequent mutations and Close.
func WithDurability(durability Durability) Option {
	return func(o *Options) {
		o.durability = durability
	}
}

// resolveOptions applies opts over the database defaults without evaluating TTLs.
// expirationSet reports whether an operation explicitly set an expiration.
func (db *DB) resolveOptions(opts []Option) Options {
	defaults := Options{expiration: db.expiration, ttl: db.ttl, durability: db.durability}
	if len(opts) == 0 {
		return defaults
	}
	options := defaults
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	return options
}

func (o Options) deadline() int64 {
	if o.ttl == 0 {
		return o.expiration
	}
	now, ttl := time.Now().UnixNano(), int64(o.ttl)
	if ttl > 0 && now > math.MaxInt64-ttl {
		return math.MaxInt64
	}
	if ttl < 0 && now < math.MinInt64-ttl {
		return math.MinInt64
	}
	ns := now + ttl
	if ns == 0 {
		ns = 1 // Zero denotes no expiration in the disk format.
	}
	return ns
}

func isExpired(expiration int64) bool {
	return expiration != 0 && expiration <= time.Now().UnixNano()
}

// Open creates or recovers a database. New databases default to 64 key shards;
// WithShardCount selects another count. Existing databases load their layout
// from META and reject incompatible options before recovering any shard.
func Open(path string, opts ...Option) (*DB, error) {
	options := Options{durability: Buffer, compaction: defaultCompactionThreshold(), shardCount: defaultShardCount}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	if options.condition != nil {
		return nil, errors.New("WithCondition is only supported on mutations, not Open")
	}
	if !options.durability.valid() {
		return nil, fmt.Errorf("invalid durability %d", options.durability)
	}
	if ratio := options.compaction.ratio; math.IsNaN(ratio) || ratio < 0 || ratio >= 1 {
		return nil, fmt.Errorf("compaction ratio must be finite and in [0, 1)")
	}
	if !validShardCount(options.shardCount) {
		return nil, fmt.Errorf("shard count must be a power of two between 1 and %d", maxShardCount)
	}
	// Take the lock before touching any shard so two processes can never race
	// recovery or interleave writes to the same logs.
	lock, err := acquireLock(path)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	metricsContext, metricsCancel := context.WithCancel(context.Background())
	db := &DB{
		path:             path,
		expiration:       options.expiration,
		ttl:              options.ttl,
		durability:       options.durability,
		backgroundCancel: cancel,
		lockFile:         lock,
		metricsContext:   metricsContext,
		metricsCancel:    metricsCancel,
	}
	count, err := db.migrate(options)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("migration failed: %w", err), db.Close())
	}
	db.shardCountBits = uint(bits.TrailingZeros(uint(count)))
	db.shards = make([]Shard, count)
	db.compactionQueue = make(chan *Shard, count)
	for i := range db.shards {
		db.shards[i].metricsIndex = i
		db.shards[i].compactionThreshold = &options.compaction
		db.shards[i].compactionQueue = db.compactionQueue
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
	// Recovery may leave eligible history or unfinished packing without any
	// subsequent writes. Queue it before exposing the DB to callers.
	for i := range db.shards {
		db.shards[i].maybeScheduleCompaction()
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
		case s := <-db.compactionQueue:
			if ctx.Err() != nil {
				return
			}
			if err := s.compactQueued(); err != nil {
				fmt.Fprintf(os.Stderr, "compact %s: %v\n", s.log.path, err)
			}
		case <-t.C:
			if err := db.compactExpirations(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "compact: %v\n", err)
			}
		}
	}
}

// compactExpirations is the ticker path. Ordinary write history is handled by
// queued state triggers; pending passes are also retried when no TTLs remain.
// Every unqueued shard also checks for sparse indexes and retained mappings.
func (db *DB) compactExpirations(ctx context.Context) error {
	var err error
	for i := range db.shards {
		if ctx.Err() != nil {
			break
		}
		if compactErr := db.shards[i].compactExpirations(); compactErr != nil {
			err = errors.Join(err, fmt.Errorf("shard %02d: %w", i, compactErr))
		}
	}
	return err
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
	return &db.shards[hash>>(64-db.shardCountBits)], hash
}

func validShardCount(count int) bool {
	return count >= 1 && count <= maxShardCount && count&(count-1) == 0
}

// Set stores val using the encoding described by Value. It does not retain key
// or val, so callers may reuse their byte slices after Set returns. Set may
// replace an existing key with a different type. WithCondition checks the
// previous value atomically before writing.
func (db *DB) Set[T Value, K Key](key K, val T, opts ...Option) error {
	var metricsShard *Shard
	if m := db.operationMetrics.Load(); m != nil {
		started := m.begin()
		defer func() { m.record(operationSet, metricsShard, started) }()
	}
	options := db.resolveOptions(opts)
	durability := options.durability
	if !durability.valid() {
		return fmt.Errorf("invalid durability %d", durability)
	}
	var encoded [8]byte
	ty, value, err := encodeValue(val, &encoded)
	if err != nil {
		return err
	}
	kb := []byte(key)
	s, hash := db.shard(kb)
	metricsShard = s
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	if s.log.writeErr != nil {
		return s.log.writeErr
	}
	if err := options.checkCondition(s, kb, hash); err != nil {
		return err
	}
	change, err := s.index.prepareSet(s.log, ty, kb, value, hash, options.deadline())
	if err != nil {
		return fmt.Errorf("prepare set: %w", err)
	}
	if err := s.appendSet(ty, kb, value, change, durability); err != nil {
		return err
	}
	return s.finishDurability(durability)
}

// Get returns the value decoded as T. A missing or expired key returns the zero
// value of T and ErrNotFound directly; compare with ==. A different stored
// type returns an error wrapping ErrTypeMismatch, with no implicit conversion.
// Byte slices are owned by the caller. Scalar encodings are described by Value.
func (db *DB) Get[T Value, K Key](key K) (T, error) {
	var metricsShard *Shard
	if m := db.operationMetrics.Load(); m != nil {
		started := m.begin()
		defer func() { m.record(operationGet, metricsShard, started) }()
	}
	var zero T
	kb := []byte(key)
	s, hash := db.shard(kb)
	metricsShard = s
	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.closed {
		return zero, os.ErrClosed
	}
	// Select the representation here; index lookups only locate and read it.
	var value T
	var pos int
	var expiration int64
	var ty recordType
	var err error
	if out, ok := any(&value).(*[]byte); ok {
		pos, *out, ty, expiration, err = s.index.findPayload(s.log, kb, hash, nil)
		if err == nil && pos >= 0 && isExpired(expiration) {
			pos = -1
		} else if err == nil && pos >= 0 {
			err = checkType[T](ty)
		}
	} else if sout, ok := any(&value).(*string); ok {
		var raw []byte
		pos, raw, ty, expiration, err = s.index.findPayload(s.log, kb, hash, nil)
		if err == nil && pos >= 0 && isExpired(expiration) {
			pos = -1
		} else if err == nil && pos >= 0 {
			if err = checkType[T](ty); err == nil {
				*sout = string(raw)
			}
		}
	} else {
		var raw scalarValue
		pos, raw, expiration, err = s.index.findScalar(s.log, kb, hash)
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

// Update atomically reads the current value, calls fn, and stores fn's result,
// inserting when the key is missing. The callback receives the existing value
// and true, or the zero value of T and false for a missing or expired key.
// Expired keys are treated as absent and replaced. If fn returns a non-nil
// error, nothing is written and the error is returned as-is; the returned
// value is ignored. A stored type different from T fails with an error
// wrapping ErrTypeMismatch without calling fn, as do malformed encodings.
// The callback runs once under the shard's exclusive lock. It must be short
// and must not call methods on this DB. A panic propagates without changing
// the value. Byte slices passed to fn are owned by the caller and may be
// mutated or retained. Returned slices are copied before Update returns; nil
// stores an empty value. WithExpiration or WithTTL sets the expiration of the
// written record; otherwise an existing key keeps its expiration and
// an insert uses the database default. An unchanged value with an unchanged
// expiration does not append a record. A write error leaves the index
// unchanged, but cannot undo any external effects of the callback.
// WithCondition checks the previous value before fn; a rejected condition
// returns ErrConditionFailed without calling fn.
func (db *DB) Update[T Value, K Key](key K, fn func(v T, exists bool) (T, error), opts ...Option) error {
	var metricsShard *Shard
	if m := db.operationMetrics.Load(); m != nil {
		started := m.begin()
		defer func() { m.record(operationUpdate, metricsShard, started) }()
	}
	options := db.resolveOptions(opts)
	durability := options.durability
	if !durability.valid() {
		return fmt.Errorf("invalid durability %d", durability)
	}
	kb := []byte(key)
	s, hash := db.shard(kb)
	metricsShard = s
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	if s.log.writeErr != nil {
		return s.log.writeErr
	}
	var value T
	var pos int
	var ty recordType
	var err error
	var previous []byte
	var raw scalarValue
	var prevExp int64
	exists := false
	out, byteValue := any(&value).(*[]byte)
	sout, stringValue := any(&value).(*string)
	if byteValue || stringValue {
		// Keep the original in the write scratch space for no-op detection,
		// and give the callback a separate copy it can mutate or retain.
		// liveOnly treats expired keys as absent.
		pos, previous, ty, prevExp, err = s.index.lookupPayload(s.log, kb, hash, s.log.writeBuffer[:], true)
		if err != nil {
			return fmt.Errorf("index update: %w", err)
		}
		if pos >= 0 {
			if err := checkType[T](ty); err != nil {
				return fmt.Errorf("index update: %w", err)
			}
			if byteValue {
				*out = bytes.Clone(previous)
			} else {
				*sout = string(previous)
			}
			exists = true
		}
	} else {
		pos, raw, prevExp, err = s.index.findScalar(s.log, kb, hash)
		if err != nil {
			return fmt.Errorf("index update: %w", err)
		}
		if pos >= 0 {
			if isExpired(prevExp) {
				pos = -1
			} else {
				value, err = decodeScalar[T](raw)
				if err != nil {
					return fmt.Errorf("index update: %w", err)
				}
				exists = true
			}
		}
	}
	if options.condition != nil {
		conditionValue := raw
		if byteValue || stringValue {
			conditionValue = scalarValue{ty: ty, size: len(previous)}
		}
		if err := options.condition(conditionValue, previous, exists); err != nil {
			return err
		}
	}
	newValue, fnErr := fn(value, exists)
	if fnErr != nil {
		return fnErr
	}
	expiration := prevExp
	if !exists || options.expirationSet {
		expiration = options.deadline()
	}
	var encoded [8]byte
	ty, payload, err := encodeValue(newValue, &encoded)
	if err != nil {
		return err
	}
	var change indexChange
	if exists {
		// An unchanged value with an unchanged expiration does not append
		// a record.
		sameValue := false
		if byteValue || stringValue {
			sameValue = bytes.Equal(payload, previous)
		} else {
			sameValue = scalarBytes(ty, payload) == raw
		}
		if sameValue && expiration == prevExp {
			return s.finishDurability(durability)
		}
		previousSize := len(previous)
		if !byteValue && !stringValue {
			previousSize = raw.size
		}
		change = s.index.prepareUpdate(pos, hash, expiration, prevExp, recordSize(len(kb), previousSize))
	} else {
		change, err = s.index.prepareSet(s.log, ty, kb, payload, hash, expiration)
		if err != nil {
			return fmt.Errorf("prepare update: %w", err)
		}
	}
	if err := s.appendSet(ty, kb, payload, change, durability); err != nil {
		return err
	}
	return s.finishDurability(durability)
}

// Delete removes key. Deleting a missing key succeeds.
// WithCondition checks the previous value atomically before deleting, including
// for missing keys. Other options except WithDurability have no effect.
func (db *DB) Delete[K Key](key K, opts ...Option) error {
	var metricsShard *Shard
	if m := db.operationMetrics.Load(); m != nil {
		started := m.begin()
		defer func() { m.record(operationDelete, metricsShard, started) }()
	}
	options := db.resolveOptions(opts)
	durability := options.durability
	if !durability.valid() {
		return fmt.Errorf("invalid durability %d", durability)
	}
	kb := []byte(key)
	s, hash := db.shard(kb)
	metricsShard = s
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	if s.log.writeErr != nil {
		return s.log.writeErr
	}
	if err := options.checkCondition(s, kb, hash); err != nil {
		return err
	}
	change, err := s.index.prepareDelete(s.log, kb, hash)
	if err != nil {
		return fmt.Errorf("prepare delete: %w", err)
	}
	if change.kind == indexUnchanged {
		// Deleting an expired key can prune its index entry without a tombstone.
		s.maybeScheduleCompaction()
		return s.finishDurability(durability)
	}
	if _, _, err := s.log.writeDurability(typeDelete, kb, nil, 0, durability); err != nil {
		return fmt.Errorf("write log: %w", err)
	}
	s.index.commit(change)
	s.maybeScheduleCompaction()
	return s.finishDurability(durability)
}

// Scan returns an iterator over every live key/value pair in the database.
// Pairs are yielded in shard order; order within a shard is unspecified, and
// the scan is not a point-in-time snapshot: writes to shards not yet scanned
// become visible, while writes to scanned shards do not. Each shard's read
// lock is held while that shard is scanned, so writers on the shard block
// until its scan completes. Expired entries are skipped.
// The yielded slices are reused across iterations: copy them to retain.
// Iteration stops early on a read error, which cannot happen without
// underlying storage corruption.
func (db *DB) Scan() iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		var buf []byte
		for i := range db.shards {
			s := &db.shards[i]
			if !scanShard(s, &buf, yield) {
				return
			}
		}
	}
}

// scanShard scans one shard under its read lock, reusing buf across record
// reads. It reports whether iteration should continue.
func scanShard(s *Shard, buf *[]byte, yield func([]byte, []byte) bool) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.closed {
		return false
	}
	for _, slot := range s.index.slots {
		if isEmptySlot(slot) {
			continue
		}
		addr := slot & addressMask
		key, value, _, expiration, err := s.log.readPayload(addressSegment(addr), addressOffset(addr), *buf)
		if err != nil {
			return false
		}
		*buf = key[:cap(key)]
		if isExpired(expiration) {
			continue
		}
		if !yield(key, value) {
			return false
		}
	}
	return true
}

// Close waits for active operations and releases the logs and index mappings
// without removing the database files. Subsequent operations return os.ErrClosed.
// Pending buffers are written without fsync. Close may be called again to retry
// an unsuccessful unmap.
func (db *DB) Close() error {
	return db.close(false)
}

// Destroy closes the database and removes its directory.
func (db *DB) Destroy() error {
	return db.close(true)
}

func (db *DB) close(removeFiles bool) error {
	if db.metricsCancel != nil {
		db.metricsCancel()
	}
	db.metricsHistoryMu.Lock()
	history := db.metricsHistory
	db.metricsHistoryMu.Unlock()
	if history != nil {
		<-history.done
	}
	db.stopOperationMetrics()
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
			if s.flushTimer != nil {
				s.flushTimer.Stop()
			}
			if flushErr := s.log.flush(); flushErr != nil {
				err = errors.Join(err, fmt.Errorf("flush shard %02d: %w", i, flushErr))
			}
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
	// Release the lock after removal so no other process can open the path
	// between our close and the directory's deletion.
	if db.lockFile != nil {
		err = errors.Join(err, db.lockFile.Close())
		db.lockFile = nil
	}
	return err
}
