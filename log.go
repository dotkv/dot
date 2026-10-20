package dot

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"syscall"
)

type Log struct {
	headFile    *os.File
	headSegment uint64
	size        uint64
}

func newLog(name string) (*Log, error) {
	var l Log
	var err error
	err = os.MkdirAll(name, 0700)
	if err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}
	name = path.Join(name, fmt.Sprintf("%07d.log", l.headSegment))
	l.headFile, err = os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND|syscall.O_CLOEXEC|syscall.O_NOATIME, 0600)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	return &l, nil
}

func (l *Log) crc(header [13]byte, key []byte, val []byte) uint32 {
	hash := crc32.NewIEEE()
	hash.Write(header[4:])
	hash.Write(key)
	hash.Write(val)
	return hash.Sum32()
}

func (l *Log) write(ty byte, key []byte, val []byte) error {
	var header [13]byte // [crc 4][type 1][keylen 4][vallen 4]
	header[4] = ty
	binary.BigEndian.PutUint32(header[5:], uint32(len(key)))
	binary.BigEndian.PutUint32(header[9:], uint32(len(val)))
	binary.BigEndian.PutUint32(header[:], l.crc(header, key, val))
	var err error
	var n int
	n, err = l.headFile.Write(header[:])
	if err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	l.size += uint64(n)
	n, err = l.headFile.Write(key)
	if err != nil {
		return fmt.Errorf("write key: %w", err)
	}
	l.size += uint64(n)
	n, err = l.headFile.Write(val)
	if err != nil {
		return fmt.Errorf("write val: %w", err)
	}
	l.size += uint64(n)
	return nil
}

func (l *Log) read(_ uint64, off uint64) ([]byte, []byte, error) {
	var header [13]byte
	if _, err := l.headFile.ReadAt(header[:], int64(off)); err != nil {
		return nil, nil, fmt.Errorf("read header: %w", err)
	}
	keylen := binary.BigEndian.Uint32(header[5:])
	vallen := binary.BigEndian.Uint32(header[9:])
	b := make([]byte, keylen+vallen)
	if _, err := l.headFile.ReadAt(b, int64(off)+13); err != nil {
		return nil, nil, fmt.Errorf("read header: %w", err)
	}
	binary.BigEndian.PutUint32(header[4:], l.crc(header, b[:keylen], b[keylen:]))
	if !bytes.Equal(header[:4], header[4:8]) {
		return nil, nil, fmt.Errorf("crc mismatch")
	}
	return b[:keylen], b[keylen:], nil
}
