package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"git.mills.io/prologic/bitcask"
	"github.com/akrylysov/pogreb"
	"github.com/cockroachdb/pebble/v2"
	"github.com/dotkv/dot"
)

// Get returns caller-owned bytes. Scalar operations return the decoded value
// without allocating an intermediate result. New contenders implement this
// interface and add an opener to engines; the workload driver stays unchanged.
// Each workload uses one value type per key; cross-type reads are engine-specific.
type engine interface {
	Set([]byte, []byte) error
	Get([]byte) ([]byte, bool, error)
	SetUint64([]byte, uint64) error
	GetUint64([]byte) (uint64, bool, error)
	Delete([]byte) error
	Flush() error
	Close() error
}

var engines = map[string]func(caseConfig) (engine, error){
	"dot": func(c caseConfig) (engine, error) {
		db, err := dot.Open(c.Path)
		if err != nil {
			return nil, err
		}
		return &dotEngine{db}, nil
	},
	"pebble": func(c caseConfig) (engine, error) {
		db, err := pebble.Open(c.Path, pebbleOptions())
		if err != nil {
			return nil, err
		}
		return &pebbleEngine{db}, nil
	},
	"pogreb": func(c caseConfig) (engine, error) {
		// Keep the native mmap filesystem and maintenance defaults. Writes use
		// the OS page cache without per-write or periodic fsync.
		db, err := pogreb.Open(c.Path, &pogreb.Options{BackgroundSyncInterval: 0})
		if err != nil {
			return nil, err
		}
		return &pogrebEngine{db}, nil
	},
	"bitcask": func(c caseConfig) (engine, error) {
		// Like Pebble's NoSync, acknowledge writes without per-write fsync.
		// Bitcask still syncs when rotating or closing its active file.
		db, err := bitcask.Open(c.Path, bitcask.WithSync(false),
			bitcask.WithMaxValueSize(max(bitcask.DefaultMaxValueSize, uint64(c.ValueBytes))))
		if err != nil {
			return nil, err
		}
		return &bitcaskEngine{db}, nil
	},
}

func pebbleOptions() *pebble.Options {
	opts := &pebble.Options{}
	opts.EnsureDefaults()
	// WAL and flushes remain enabled. Disable compression to match Dot, and
	// retain other defaults, including the cache and memtable sizes.
	opts.ApplyCompressionSettings(func() pebble.DBCompressionSettings { return pebble.DBCompressionNone })
	return opts
}

type dotEngine struct{ db *dot.DB }

func (e *dotEngine) Set(key, value []byte) error { return e.db.Set(key, value) }
func (e *dotEngine) Get(key []byte) ([]byte, bool, error) {
	v, err := e.db.Get[[]byte](key)
	if err == dot.ErrNotFound {
		return nil, false, nil
	}
	return v, err == nil, err
}
func (e *dotEngine) SetUint64(key []byte, value uint64) error { return e.db.Set(key, value) }
func (e *dotEngine) GetUint64(key []byte) (uint64, bool, error) {
	v, err := e.db.Get[uint64](key)
	if err == dot.ErrNotFound {
		return 0, false, nil
	}
	return v, err == nil, err
}
func (e *dotEngine) Delete(key []byte) error { return e.db.Delete(key) }
func (e *dotEngine) Flush() error            { return nil }
func (e *dotEngine) Close() error            { return e.db.Close() }

type pebbleEngine struct{ db *pebble.DB }

func (e *pebbleEngine) Set(key, value []byte) error {
	return e.db.Set(key, value, pebble.NoSync)
}
func (e *pebbleEngine) Get(key []byte) ([]byte, bool, error) {
	v, closer, err := e.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	owned := bytes.Clone(v)
	return owned, true, closer.Close()
}
func (e *pebbleEngine) SetUint64(key []byte, value uint64) error {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], value)
	return e.Set(key, encoded[:])
}
func (e *pebbleEngine) GetUint64(key []byte) (uint64, bool, error) {
	v, closer, err := e.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if len(v) != 8 {
		return 0, false, errors.Join(fmt.Errorf("scalar has %d bytes, want 8", len(v)), closer.Close())
	}
	value := binary.LittleEndian.Uint64(v)
	return value, true, closer.Close()
}
func (e *pebbleEngine) Delete(key []byte) error { return e.db.Delete(key, pebble.NoSync) }
func (e *pebbleEngine) Flush() error            { return e.db.Flush() }
func (e *pebbleEngine) Close() error            { return e.db.Close() }

type pogrebEngine struct{ db *pogreb.DB }

func (e *pogrebEngine) Set(key, value []byte) error { return e.db.Put(key, value) }
func (e *pogrebEngine) Get(key []byte) ([]byte, bool, error) {
	// Pogreb already copies values out of its mmap before releasing the lock.
	v, err := e.db.Get(key)
	return v, v != nil && err == nil, err
}
func (e *pogrebEngine) SetUint64(key []byte, value uint64) error {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], value)
	return e.Set(key, encoded[:])
}
func (e *pogrebEngine) GetUint64(key []byte) (uint64, bool, error) {
	return decodeUint64(e.Get(key))
}
func (e *pogrebEngine) Delete(key []byte) error { return e.db.Delete(key) }

// Native writes are already in the OS page cache; Sync would force fsync.
func (e *pogrebEngine) Flush() error { return nil }
func (e *pogrebEngine) Close() error { return e.db.Close() }

type bitcaskEngine struct{ db *bitcask.Bitcask }

func (e *bitcaskEngine) Set(key, value []byte) error { return e.db.Put(key, value) }
func (e *bitcaskEngine) Get(key []byte) ([]byte, bool, error) {
	v, err := e.db.Get(key)
	if errors.Is(err, bitcask.ErrKeyNotFound) {
		return nil, false, nil
	}
	// Bitcask returns an owned allocation, including for mmap-backed reads.
	return v, err == nil, err
}
func (e *bitcaskEngine) SetUint64(key []byte, value uint64) error {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], value)
	return e.Set(key, encoded[:])
}
func (e *bitcaskEngine) GetUint64(key []byte) (uint64, bool, error) {
	return decodeUint64(e.Get(key))
}
func (e *bitcaskEngine) Delete(key []byte) error { return e.db.Delete(key) }

// The native encoder flushes each record to the OS page cache without fsync.
func (e *bitcaskEngine) Flush() error { return nil }
func (e *bitcaskEngine) Close() error { return e.db.Close() }

func decodeUint64(value []byte, found bool, err error) (uint64, bool, error) {
	if err != nil || !found {
		return 0, false, err
	}
	if len(value) != 8 {
		return 0, false, fmt.Errorf("scalar has %d bytes, want 8", len(value))
	}
	return binary.LittleEndian.Uint64(value), true, nil
}
