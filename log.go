package dot

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
)

const (
	logSegmentSize     = 1 << 26
	logWriteBufferSize = 4 << 10
	logHeaderSize      = 21
)

type Log struct {
	path        string
	headFile    *os.File
	headSegment uint64
	size        uint64
	// Writes and their index lookups share this buffer under the shard's
	// exclusive lock. Concurrent Get calls use their own read buffers.
	writeBuffer [logWriteBufferSize]byte
}

func newLog(name string) (*Log, error) {
	var l Log
	if err := os.MkdirAll(name, 0700); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}
	l.path = filepath.Clean(name)
	switch {
	case l.path == ".":
		l.path = ""
	case !os.IsPathSeparator(l.path[len(l.path)-1]):
		l.path += string(os.PathSeparator)
	}
	if err := l.openHead(0); err != nil {
		return nil, fmt.Errorf("open head: %w", err)
	}
	return &l, nil
}

func (l *Log) segmentPath(seg uint64) string {
	var buf [20]byte
	digits := strconv.AppendUint(buf[:0], seg, 10)
	const zeros = "0000000"
	return l.path + zeros[:max(0, 7-len(digits))] + string(digits) + ".log"
}

func (l *Log) openFile(seg uint64, flag int) (*os.File, error) {
	return os.OpenFile(l.segmentPath(seg), flag|syscall.O_CLOEXEC|syscall.O_NOATIME, 0600)
}

func (l *Log) openHead(seg uint64) error {
	f, err := l.openFile(seg, os.O_CREATE|os.O_RDWR|os.O_APPEND)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	old := l.headFile
	l.headFile = f
	l.headSegment = seg
	l.size = 0
	if old != nil {
		if err := old.Close(); err != nil {
			return fmt.Errorf("close sealed log: %w", err)
		}
	}
	return nil
}

func (l *Log) crc(header []byte, key []byte, val []byte) uint32 {
	// Checksum the small header directly so it stays on the stack, then use
	// the optimized standard-library implementation for the payload.
	crc := crc32.Update(checksumSmall(header[4:]), crc32.IEEETable, key)
	return crc32.Update(crc, crc32.IEEETable, val)
}

// checksumSmall avoids the indirect calls in crc32.Update that make a local
// read buffer escape. Larger payloads still use the optimized standard library.
func checksumSmall(data []byte) uint32 {
	crc := ^uint32(0)
	for _, b := range data {
		crc = crc32.IEEETable[byte(crc)^b] ^ (crc >> 8)
	}
	return ^crc
}

func (l *Log) write(ty byte, key []byte, val []byte, expiration int64) (uint64, uint64, error) {
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
	size := logHeaderSize + len(key) + len(val)
	var record []byte
	if size <= len(l.writeBuffer) {
		record = l.writeBuffer[:size]
	} else {
		// Keep oversized records temporary instead of growing the shard buffer.
		record = make([]byte, size)
	}
	// [crc 4][type 1][keylen 4][vallen 4][expiration 8][key][value]
	record[4] = ty
	binary.BigEndian.PutUint32(record[5:], uint32(len(key)))
	binary.BigEndian.PutUint32(record[9:], uint32(len(val)))
	binary.BigEndian.PutUint64(record[13:], uint64(expiration))
	copy(record[logHeaderSize:], key)
	copy(record[logHeaderSize+len(key):], val)
	binary.BigEndian.PutUint32(record[:], crc32.ChecksumIEEE(record[4:]))
	n, err := l.headFile.Write(record)
	l.size += uint64(n)
	if err != nil {
		return 0, 0, fmt.Errorf("write record: %w", err)
	}
	return segment, off, nil
}

// readPayload reads and verifies a record, returning its key, value, and
// expiration. It borrows buf when the payload fits; passing nil returns owned slices.
func (l *Log) readPayload(seg uint64, off uint64, buf []byte) ([]byte, []byte, int64, error) {
	f := l.headFile
	if l.headSegment != seg {
		var err error
		f, err = l.openFile(seg, os.O_RDONLY)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("open sealed log: %w", err)
		}
		defer f.Close()
	}
	var prefix [1 << 10]byte
	n, err := f.ReadAt(prefix[:], int64(off))
	if err != nil && err != io.EOF {
		return nil, nil, 0, fmt.Errorf("read header: %w", err)
	}
	if n < logHeaderSize {
		return nil, nil, 0, fmt.Errorf("read header: %w", io.ErrUnexpectedEOF)
	}
	return l.readFromPrefix(f, off, prefix[:n], buf)
}

// readFromPrefix finishes and verifies a record after its header and
// possibly some payload bytes have been read. Both read paths use it, with
// readScalar falling back here only when the record exceeds its local buffer.
// prefix starts at off and must contain at least the 21-byte record header.
func (l *Log) readFromPrefix(f *os.File, off uint64, prefix []byte, buf []byte) ([]byte, []byte, int64, error) {
	keylen := binary.BigEndian.Uint32(prefix[5:])
	vallen := binary.BigEndian.Uint32(prefix[9:])
	expiration := int64(binary.BigEndian.Uint64(prefix[13:]))
	size := int(keylen) + int(vallen)

	if buf == nil || cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}
	copied := copy(buf, prefix[logHeaderSize:])
	if copied < size {
		if _, err := f.ReadAt(
			buf[copied:], int64(off)+logHeaderSize+int64(copied),
		); err != nil {
			return nil, nil, 0, fmt.Errorf("read payload: %w", err)
		}
	}
	if binary.BigEndian.Uint32(prefix[:]) != l.crc(prefix[:logHeaderSize], buf[:keylen], buf[keylen:]) {
		return nil, nil, 0, fmt.Errorf("crc mismatch")
	}
	return buf[:keylen], buf[keylen:], expiration, nil
}

// readScalar reads and verifies a record, returning its scalar bits, byte
// length, and expiration when the key matches. Key comparison stays here to keep
// record bytes on the stack. DB.Get validates the scalar type and decodes the returned bits.
func (l *Log) readScalar(seg uint64, off uint64, key []byte) (scalarValue, int64, bool, error) {
	f := l.headFile
	if l.headSegment != seg {
		var err error
		f, err = l.openFile(seg, os.O_RDONLY)
		if err != nil {
			return scalarValue{}, 0, false, fmt.Errorf("open sealed log: %w", err)
		}
		defer f.Close()
	}
	var record [1 << 10]byte
	// A matching scalar needs only the header, caller's key, and at most eight
	// value bytes. Keep the larger buffer for candidates with different sizes.
	readSize := min(len(record), logHeaderSize+len(key)+8)
	n, err := f.ReadAt(record[:readSize], int64(off))
	if err != nil && err != io.EOF {
		return scalarValue{}, 0, false, fmt.Errorf("read header: %w", err)
	}
	if n < logHeaderSize {
		return scalarValue{}, 0, false, fmt.Errorf("read header: %w", io.ErrUnexpectedEOF)
	}
	keylen := int(binary.BigEndian.Uint32(record[5:]))
	vallen := int(binary.BigEndian.Uint32(record[9:]))
	if keylen+vallen > len(record)-logHeaderSize {
		storedKey, value, expiration, err := l.readFromPrefix(f, off, record[:n], nil)
		if err != nil {
			return scalarValue{}, 0, false, err
		}
		if !bytes.Equal(storedKey, key) {
			return scalarValue{}, 0, false, nil
		}
		return scalarBytes(value), expiration, true, nil
	}
	end := logHeaderSize + keylen + vallen
	if n < end {
		if _, err := f.ReadAt(record[n:end], int64(off)+int64(n)); err != nil {
			return scalarValue{}, 0, false, fmt.Errorf("read payload: %w", err)
		}
	}
	if binary.BigEndian.Uint32(record[:]) != checksumSmall(record[4:end]) {
		return scalarValue{}, 0, false, fmt.Errorf("crc mismatch")
	}
	if !bytes.Equal(record[logHeaderSize:logHeaderSize+keylen], key) {
		return scalarValue{}, 0, false, nil
	}
	expiration := int64(binary.BigEndian.Uint64(record[13:]))
	return scalarBytes(record[logHeaderSize+keylen : end]), expiration, true, nil
}
