package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

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

var engines = map[string]func(string) (engine, error){
	"dot": func(path string) (engine, error) {
		db, err := dot.New(path)
		if err != nil {
			return nil, err
		}
		return &dotEngine{db}, nil
	},
	"pebble": func(path string) (engine, error) {
		// WAL and automatic compaction remain enabled. Compression is disabled
		// to match Dot. Other settings, including the cache, use Pebble defaults.
		opts := &pebble.Options{}
		opts.EnsureDefaults()
		opts.ApplyCompressionSettings(func() pebble.DBCompressionSettings { return pebble.DBCompressionNone })
		db, err := pebble.Open(path, opts)
		if err != nil {
			return nil, err
		}
		return &pebbleEngine{db}, nil
	},
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
