package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"dot"
	"github.com/cockroachdb/pebble/v2"
)

// Get returns caller-owned bytes. Scalar operations return the decoded value
// without allocating an intermediate result. New contenders implement this
// interface and add an opener to engines; the workload driver stays unchanged.
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
	"map": func(c caseConfig) (engine, error) {
		return &mapEngine{values: make(map[string][]byte, c.Keys+c.Workers)}, nil
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
	if errors.Is(err, dot.ErrNotFound) {
		return nil, false, nil
	}
	return v, err == nil, err
}
func (e *dotEngine) SetUint64(key []byte, value uint64) error { return e.db.Set(key, value) }
func (e *dotEngine) GetUint64(key []byte) (uint64, bool, error) {
	v, err := e.db.Get[uint64](key)
	if errors.Is(err, dot.ErrNotFound) {
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

// One ordinary Go map shared by all workers. Copies preserve the same ownership
// contract as the persistent adapters; the RWMutex is included in timing.
type mapEngine struct {
	mu     sync.RWMutex
	values map[string][]byte
}

func (e *mapEngine) Set(key, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.values[string(key)] = bytes.Clone(value)
	return nil
}

func (e *mapEngine) Get(key []byte) ([]byte, bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	value, found := e.values[string(key)]
	return bytes.Clone(value), found, nil
}

func (e *mapEngine) SetUint64(key []byte, value uint64) error {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], value)
	return e.Set(key, encoded[:])
}

func (e *mapEngine) GetUint64(key []byte) (uint64, bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	value, found := e.values[string(key)]
	if !found {
		return 0, false, nil
	}
	if len(value) != 8 {
		return 0, false, fmt.Errorf("scalar has %d bytes, want 8", len(value))
	}
	return binary.LittleEndian.Uint64(value), true, nil
}

func (e *mapEngine) Delete(key []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.values, string(key))
	return nil
}

func (e *mapEngine) Flush() error { return nil }
func (e *mapEngine) Close() error { return nil }
