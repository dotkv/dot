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

const logSegmentSize = 1 << 26

type Log struct {
	path        string
	headFile    *os.File
	headSegment uint64
	size        uint64
}

func newLog(name string) (*Log, error) {
	var l Log
	l.path = name
	var err error
	err = os.MkdirAll(l.path, 0700)
	if err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}
	if err := l.openHead(0); err != nil {
		return nil, fmt.Errorf("open head: %w", err)
	}
	return &l, nil
}

func (l *Log) openHead(segment uint64) error {
	name := path.Join(l.path, fmt.Sprintf("%07d.log", segment))
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND|syscall.O_CLOEXEC|syscall.O_NOATIME, 0600)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	old := l.headFile
	l.headFile = f
	l.headSegment = segment
	l.size = 0
	if old != nil {
		if err := old.Close(); err != nil {
			return fmt.Errorf("close sealed log: %w", err)
		}
	}
	return nil
}

func (l *Log) crc(header [13]byte, key []byte, val []byte) uint32 {
	hash := crc32.NewIEEE()
	hash.Write(header[4:])
	hash.Write(key)
	hash.Write(val)
	return hash.Sum32()
}

func (l *Log) write(ty byte, key []byte, val []byte) (uint64, uint64, error) {
	// Rotate before appending so a failed rotation leaves the record unwritten
	// and every record starts at an offset that fits in the index.
	if l.size >= logSegmentSize {
		if err := l.headFile.Sync(); err != nil {
			return 0, 0, fmt.Errorf("sync file: %w", err)
		}
		if err := l.openHead(l.headSegment + 1); err != nil {
			return 0, 0, fmt.Errorf("open head: %w", err)
		}
	}
	segment, off := l.headSegment, l.size
	var header [13]byte // [crc 4][type 1][keylen 4][vallen 4]
	header[4] = ty
	binary.BigEndian.PutUint32(header[5:], uint32(len(key)))
	binary.BigEndian.PutUint32(header[9:], uint32(len(val)))
	binary.BigEndian.PutUint32(header[:], l.crc(header, key, val))
	var err error
	var n int
	n, err = l.headFile.Write(header[:])
	if err != nil {
		return 0, 0, fmt.Errorf("write header: %w", err)
	}
	l.size += uint64(n)
	n, err = l.headFile.Write(key)
	if err != nil {
		return 0, 0, fmt.Errorf("write key: %w", err)
	}
	l.size += uint64(n)
	n, err = l.headFile.Write(val)
	if err != nil {
		return 0, 0, fmt.Errorf("write val: %w", err)
	}
	l.size += uint64(n)
	return segment, off, nil
}

func (l *Log) read(seg uint64, off uint64) ([]byte, []byte, error) {
	f := l.headFile
	if l.headSegment != seg {
		var err error
		f, err = os.OpenFile(path.Join(l.path, fmt.Sprintf("%07d.log", seg)), os.O_RDONLY|syscall.O_NOATIME|syscall.O_CLOEXEC, 0600)
		if err != nil {
			return nil, nil, fmt.Errorf("open sealed log: %w", err)
		}
		defer f.Close()
	}
	var header [13]byte
	if _, err := f.ReadAt(header[:], int64(off)); err != nil {
		return nil, nil, fmt.Errorf("read header: %w", err)
	}
	keylen := binary.BigEndian.Uint32(header[5:])
	vallen := binary.BigEndian.Uint32(header[9:])
	b := make([]byte, keylen+vallen)
	if _, err := f.ReadAt(b, int64(off)+13); err != nil {
		return nil, nil, fmt.Errorf("read payload: %w", err)
	}
	binary.BigEndian.PutUint32(header[4:], l.crc(header, b[:keylen], b[keylen:]))
	if !bytes.Equal(header[:4], header[4:8]) {
		return nil, nil, fmt.Errorf("crc mismatch")
	}
	return b[:keylen], b[keylen:], nil
}
