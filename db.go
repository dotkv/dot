package dot

import (
	"fmt"
	"os"
)

type DB struct {
	path  string
	log   *Log
	index *Index
}

func New(path string) (*DB, error) {
	var err error
	db := &DB{
		path:  path,
		index: newIndex(),
	}
	db.log, err = newLog(path)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (db *DB) Set(key []byte, val []byte) error {
	change, err := db.index.prepareSet(db.log, key, val)
	if err != nil {
		return fmt.Errorf("prepare set: %s", err)
	}
	if err := db.log.write('s', key, val); err != nil {
		return fmt.Errorf("write log: %s", err)
	}
	db.index.commit(change)
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	val, err := db.index.get(db.log, key)
	if err != nil {
		return nil, fmt.Errorf("index get: %s", err)
	}
	return val, nil
}

func (db *DB) Delete(key []byte) error {
	change, err := db.index.prepareDelete(db.log, key)
	if err != nil {
		return fmt.Errorf("prepare delete: %s", err)
	}
	if change.kind == indexUnchanged {
		return nil
	}
	if err := db.log.write('d', key, nil); err != nil {
		return fmt.Errorf("write log: %s", err)
	}
	db.index.commit(change)
	return nil
}

func (db *DB) Destroy() error {
	return os.RemoveAll(db.path)
}
