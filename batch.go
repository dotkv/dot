package dot

import (
	"bytes"
	"fmt"
	"os"
	"slices"

	"github.com/cespare/xxhash/v2"
)

// Batch buffers a group of writes and commits them atomically with respect to
// concurrent readers and writers: Commit locks every involved shard in index
// order, so no other operation can observe a partial batch.
//
// What Batch does not provide:
//
//   - Crash atomicity: a crash mid-commit can leave some shards' writes
//     applied and others not. Each shard still recovers consistently.
//   - Isolation: Get sees the batch's own writes plus committed database
//     state as of the call. Concurrent writers may interleave between a
//     batch Get and Commit. For read-modify-write that must be atomic, read
//     and write outside a batch under your own coordination.
//
// A Batch holds no resources; dropping it discards the buffered operations.
// A Batch must not be used concurrently.
type Batch struct {
	db      *DB
	ops     []batchOp
	pending map[string]int // string(key) -> index of the last op for that key
}

type batchOp struct {
	shard      int
	hash       uint64
	ty         recordType // typeDelete for deletes
	key        []byte     // owned copy
	keyStr     string     // string(key), for the pending map
	val        []byte     // owned encoded value; nil for deletes and bools
	expiration int64
}

// NewBatch returns an empty batch for db.
func (db *DB) NewBatch() *Batch {
	return &Batch{db: db, pending: make(map[string]int)}
}

// Set buffers a set of key to val, exactly as DB.Set would apply it, including
// option handling. The key and value are copied; callers may reuse them.
// Only the last operation per key takes effect at commit.
func (b *Batch) Set[T Value, K Key](key K, val T, opts ...Option) error {
	expiration := b.db.expiration
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
	ty, value, err := encodeValue(val, &encoded)
	if err != nil {
		return err
	}
	kb := []byte(key)
	if _, ok := any(key).([]byte); ok {
		kb = bytes.Clone(kb)
	}
	// encodeValue borrows its buffer and passes byte slices through; the
	// batch outlives this call, so retain an owned copy. Bool payloads stay nil.
	var owned []byte
	if value != nil {
		owned = bytes.Clone(value)
	}
	hash := xxhash.Sum64(kb)
	keyStr := string(kb)
	b.pending[keyStr] = len(b.ops)
	b.ops = append(b.ops, batchOp{
		shard:      int(hash >> (64 - shardBits)), // mirrors db.shard
		hash:       hash,
		ty:         ty,
		key:        kb,
		keyStr:     keyStr,
		val:        owned,
		expiration: expiration,
	})
	return nil
}

// Delete buffers a delete of key, exactly as DB.Delete would apply it.
// Only the last operation per key takes effect at commit.
func (b *Batch) Delete[K Key](key K) {
	kb := []byte(key)
	if _, ok := any(key).([]byte); ok {
		kb = bytes.Clone(kb)
	}
	hash := xxhash.Sum64(kb)
	keyStr := string(kb)
	b.pending[keyStr] = len(b.ops)
	b.ops = append(b.ops, batchOp{
		shard:  int(hash >> (64 - shardBits)), // mirrors db.shard
		hash:   hash,
		ty:     typeDelete,
		key:    kb,
		keyStr: keyStr,
	})
}

// Get returns the value for key as the batch will commit it: a pending write
// first (read-your-writes), otherwise the committed database state. A pending
// delete or an expired pending write reports ErrNotFound, matching what a
// read after Commit would observe. Type mismatches against a pending write
// return an error wrapping ErrTypeMismatch, as with DB.Get.
func (b *Batch) Get[T Value, K Key](key K) (T, error) {
	var zero T
	kb := []byte(key)
	if i, ok := b.pending[string(kb)]; ok {
		op := &b.ops[i]
		if op.ty == typeDelete || isExpired(op.expiration) {
			return zero, ErrNotFound
		}
		var value T
		if out, ok := any(&value).(*[]byte); ok {
			if err := checkType[T](op.ty); err != nil {
				return zero, err
			}
			*out = bytes.Clone(op.val)
			return value, nil
		}
		if sout, ok := any(&value).(*string); ok {
			if err := checkType[T](op.ty); err != nil {
				return zero, err
			}
			*sout = string(op.val)
			return value, nil
		}
		return decodeScalar[T](scalarBytes(op.ty, op.val))
	}
	return b.db.Get[T](key)
}

// Commit applies every buffered operation atomically with respect to
// concurrent database users: it locks all involved shards in index order,
// applies the operations in call order (only the last per key), and unlocks.
// An empty batch commits trivially. On success the batch is reset and may be
// reused. If the database is closed, Commit returns os.ErrClosed.
func (b *Batch) Commit() error {
	if len(b.ops) == 0 {
		return nil
	}
	involved := make([]int, 0, len(b.ops))
	seen := make(map[int]bool)
	for _, op := range b.ops {
		if !seen[op.shard] {
			seen[op.shard] = true
			involved = append(involved, op.shard)
		}
	}
	slices.Sort(involved)
	for _, i := range involved {
		b.db.shards[i].lock.Lock()
	}
	defer func() {
		for _, i := range involved {
			b.db.shards[i].lock.Unlock()
		}
	}()
	for _, i := range involved {
		if b.db.shards[i].closed {
			return os.ErrClosed
		}
	}
	for i := range b.ops {
		op := &b.ops[i]
		if b.pending[op.keyStr] != i {
			continue // superseded by a later operation on the same key
		}
		s := &b.db.shards[op.shard]
		if op.ty == typeDelete {
			change, err := s.index.prepareDelete(s.log, op.key, op.hash)
			if err != nil {
				return fmt.Errorf("prepare delete: %w", err)
			}
			if change.kind == indexUnchanged {
				s.maybeScheduleCompaction()
				continue
			}
			if _, _, err := s.log.write(typeDelete, op.key, nil, 0); err != nil {
				return fmt.Errorf("write log: %w", err)
			}
			s.index.commit(change)
			s.maybeScheduleCompaction()
			continue
		}
		change, err := s.index.prepareSet(s.log, op.ty, op.key, op.val, op.hash, op.expiration)
		if err != nil {
			return fmt.Errorf("prepare set: %w", err)
		}
		if err := s.appendSet(op.ty, op.key, op.val, change); err != nil {
			return err
		}
	}
	b.Reset()
	return nil
}

// Reset discards all buffered operations so the batch can be reused.
func (b *Batch) Reset() {
	b.ops = b.ops[:0]
	clear(b.pending)
}
