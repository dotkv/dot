package dot

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"syscall"
)

const (
	logSegmentSize     = 1 << 26
	logWriteBufferSize = 4 << 10
)

type Log struct {
	path        string
	headFile    *os.File
	headSegment uint64
	size        uint64
	// Writes and their index lookups share this buffer under the shard's
	// exclusive lock. Concurrent Get calls use their own payload allocations.
	writeBuffer [logWriteBufferSize]byte
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
	// Checksum the small header directly so it stays on the stack, then use
	// the optimized standard-library implementation for the payload.
	crc := ^uint32(0)
	for _, b := range header[4:] {
		crc = crc32.IEEETable[byte(crc)^b] ^ (crc >> 8)
	}
	crc = crc32.Update(^crc, crc32.IEEETable, key)
	return crc32.Update(crc, crc32.IEEETable, val)
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
	size := 13 + len(key) + len(val)
	var record []byte
	if size <= len(l.writeBuffer) {
		record = l.writeBuffer[:size]
	} else {
		// Keep oversized records temporary instead of growing the shard buffer.
		record = make([]byte, size)
	}
	// [crc 4][type 1][keylen 4][vallen 4][key][value]
	record[4] = ty
	binary.BigEndian.PutUint32(record[5:], uint32(len(key)))
	binary.BigEndian.PutUint32(record[9:], uint32(len(val)))
	copy(record[13:], key)
	copy(record[13+len(key):], val)
	binary.BigEndian.PutUint32(record[:], crc32.ChecksumIEEE(record[4:]))
	n, err := l.headFile.Write(record)
	l.size += uint64(n)
	if err != nil {
		return 0, 0, fmt.Errorf("write record: %w", err)
	}
	return segment, off, nil
}

// read borrows b when the payload fits; passing nil returns owned slices.
func (l *Log) read(seg uint64, off uint64, b []byte) ([]byte, []byte, error) {
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
	size := int(keylen) + int(vallen)
	if b == nil || cap(b) < size {
		b = make([]byte, size)
	} else {
		b = b[:size]
	}
	if _, err := f.ReadAt(b, int64(off)+13); err != nil {
		return nil, nil, fmt.Errorf("read payload: %w", err)
	}
	if binary.BigEndian.Uint32(header[:]) != l.crc(header, b[:keylen], b[keylen:]) {
		return nil, nil, fmt.Errorf("crc mismatch")
	}
	return b[:keylen], b[keylen:], nil
}
