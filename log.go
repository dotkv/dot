package dot

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cespare/xxhash/v2"
)

const (
	// Record starts cover 2^segmentSizeBits bytes. Alignment removes four
	// zero bits from the offsets stored in index slots (segmentOffsetBits).
	segmentSizeBits    = 26
	logSegmentSize     = 1 << segmentSizeBits
	logAlignmentBits   = 4
	logAlignment       = 1 << logAlignmentBits
	logWriteBufferSize = 4 << 10
	logPendingSize     = 64 << 10
	logFlushInterval   = 100 * time.Millisecond
	logHeaderSize      = 21
	// Segment IDs form a ring independently of the byte range within a segment.
	segmentIDBits = 20
	segmentCount  = 1 << segmentIDBits
	maxSegment    = segmentCount - 1
)

type Log struct {
	path                string
	headFile            *os.File
	headSegment         uint64
	size                uint64
	recordCount         uint64
	recordBytes         uint64
	segmentRecordCounts map[uint64]uint32
	segmentBytes        map[uint64]uint64
	segmentRefCounts    map[uint64]uint32
	// Finish queued or interrupted maintenance even if cleanup lowers the ratio.
	compactPending bool

	// A failed rollback or metadata sync blocks appends until the log is reopened.
	writeErr error
	// Writes and their index lookups share this buffer under the shard's
	// exclusive lock. Concurrent Get calls use their own read buffers.
	writeBuffer [logWriteBufferSize]byte
	// Pending records have final log addresses and are visible through the
	// index before they reach the file. All access uses the shard lock.
	pending     []byte
	bufferStart uint64
}

func newLog(name string, idx *Index) (*Log, error) {
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
	entries, err := os.ReadDir(name)
	if err != nil {
		return nil, fmt.Errorf("read directory: %w", err)
	}
	var segments []uint64
	var unfinished []string
	for _, entry := range entries {
		// Unpublished replacements never contain the only copy of a value.
		// Remove them only after all published logs pass recovery validation.
		if strings.HasSuffix(entry.Name(), ".log.compact") {
			seg, err := strconv.ParseUint(strings.TrimSuffix(entry.Name(), ".log.compact"), 10, segmentIDBits)
			if err == nil && entry.Name() == filepath.Base(l.segmentPath(seg))+".compact" && entry.Type().IsRegular() {
				unfinished = append(unfinished, filepath.Join(name, entry.Name()))
			}
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}
		seg, err := strconv.ParseUint(strings.TrimSuffix(entry.Name(), ".log"), 10, segmentIDBits)
		if err != nil || entry.Name() != filepath.Base(l.segmentPath(seg)) || !entry.Type().IsRegular() {
			return nil, fmt.Errorf("invalid log segment %q", entry.Name())
		}
		segments = append(segments, seg)
	}
	segments, err = orderSegments(segments)
	if err != nil {
		return nil, err
	}
	l.segmentRecordCounts = make(map[uint64]uint32, len(segments))
	l.segmentBytes = make(map[uint64]uint64, len(segments))
	l.segmentRefCounts = make(map[uint64]uint32, len(segments))
	idx.segmentRefCounts = l.segmentRefCounts
	var head uint64
	for i, seg := range segments {
		l.segmentRecordCounts[seg] = 0
		l.segmentBytes[seg] = 0
		// Initialize only this segment; preserve counts from earlier replay.
		l.segmentRefCounts[seg] = 0
		if err := l.replaySegment(idx, seg, i == len(segments)-1); err != nil {
			return nil, fmt.Errorf("replay segment %d: %w", seg, err)
		}
		head = seg
	}
	for _, seg := range segments {
		// Packing may have published copies without retiring their sources yet.
		if seg != head && l.segmentRefCounts[seg] != l.segmentRecordCounts[seg] {
			l.compactPending = true
		}
	}
	if err := l.openHead(head); err != nil {
		return nil, fmt.Errorf("open head: %w", err)
	}
	for _, path := range unfinished {
		if err := os.Remove(path); err != nil {
			return nil, errors.Join(fmt.Errorf("remove unfinished compaction: %w", err), l.headFile.Close())
		}
	}
	if len(unfinished) > 0 {
		if err := l.syncDirectory(); err != nil {
			return nil, errors.Join(err, l.headFile.Close())
		}
	}
	return &l, nil
}

func incrementSegment(seg uint64) uint64 { return (seg + 1) & maxSegment }

// Published filenames form one occupied arc of the ring, leaving one nonempty
// gap. Its predecessor is the head; multiple gaps cannot identify a unique head.
func orderSegments(segments []uint64) ([]uint64, error) {
	if len(segments) == 0 {
		return segments, nil
	}
	if len(segments) >= segmentCount {
		return nil, fmt.Errorf("segment ring has no vacant ID")
	}
	slices.Sort(segments)
	start, gaps := 0, 0
	for i, seg := range segments {
		next := (i + 1) % len(segments)
		if incrementSegment(seg) != segments[next] {
			start = next
			gaps++
		}
	}
	if gaps != 1 {
		return nil, fmt.Errorf("segment ring has %d gaps; cannot infer head", gaps)
	}
	ordered := make([]uint64, 0, len(segments))
	ordered = append(ordered, segments[start:]...)
	return append(ordered, segments[:start]...), nil
}

func (l *Log) syncDirectory() error {
	path := l.path
	if path == "" {
		path = "."
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open log directory: %w", err)
	}
	return errors.Join(f.Sync(), f.Close())
}

// replaySegment streams records into the index without retaining their values.
// Make this file the temporary head so index lookups can reuse its descriptor.
func (l *Log) replaySegment(idx *Index, seg uint64, head bool) (err error) {
	flag := os.O_RDONLY
	if head {
		flag = os.O_RDWR
	}
	f, err := l.openFile(seg, flag)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	l.headFile, l.headSegment = f, seg
	defer func() {
		l.headFile = nil
		err = errors.Join(err, f.Close())
	}()
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("not a regular file")
	}
	size := uint64(info.Size())
	if !head && size < logSegmentSize {
		l.compactPending = true
	}
	r := bufio.NewReaderSize(f, 64<<10)
	var header [logHeaderSize]byte
	var padding [logAlignment - 1]byte
	var key []byte
	for off := uint64(0); off < size; {
		if off >= logSegmentSize {
			return fmt.Errorf("record offset %d exceeds segment capacity", off)
		}
		if size-off < logHeaderSize {
			return recoverLogTail(f, off, head)
		}
		if _, err := io.ReadFull(r, header[:]); err != nil {
			return fmt.Errorf("read header at offset %d: %w", off, err)
		}
		ty := recordType(header[4])
		keylen := uint64(binary.BigEndian.Uint32(header[5:]))
		vallen := uint64(binary.BigEndian.Uint32(header[9:]))
		expiration := int64(binary.BigEndian.Uint64(header[13:]))
		if err := ty.validate(vallen, expiration); err != nil {
			return fmt.Errorf("invalid record at offset %d: %w", off, err)
		}
		payloadBytes := uint64(logHeaderSize) + keylen + vallen
		recordBytes := alignRecordSize(payloadBytes)
		if recordBytes > size-off {
			return recoverLogTail(f, off, head)
		}
		if keylen > uint64(^uint(0)>>1) {
			return fmt.Errorf("key too large at offset %d", off)
		}
		if cap(key) < int(keylen) {
			key = make([]byte, int(keylen))
		} else {
			key = key[:int(keylen)]
		}
		if _, err := io.ReadFull(r, key); err != nil {
			return fmt.Errorf("read key at offset %d: %w", off, err)
		}
		crc := crc32.Update(checksumSmall(header[4:]), crc32.IEEETable, key)
		for remaining := vallen; remaining > 0; {
			n := min(remaining, uint64(len(l.writeBuffer)))
			buf := l.writeBuffer[:int(n)]
			if _, err := io.ReadFull(r, buf); err != nil {
				return fmt.Errorf("read value at offset %d: %w", off, err)
			}
			crc = crc32.Update(crc, crc32.IEEETable, buf)
			remaining -= n
		}
		if crc != binary.BigEndian.Uint32(header[:]) {
			return fmt.Errorf("crc mismatch at offset %d", off)
		}
		pad := padding[:recordBytes-payloadBytes]
		if _, err := io.ReadFull(r, pad); err != nil {
			return fmt.Errorf("read padding at offset %d: %w", off, err)
		}
		for _, b := range pad {
			if b != 0 {
				return fmt.Errorf("nonzero padding at offset %d", off)
			}
		}
		if err := idx.replay(l, ty, key, xxhash.Sum64(key), logAddress(seg, off), expiration, recordBytes); err != nil {
			return fmt.Errorf("index record at offset %d: %w", off, err)
		}
		l.segmentRecordCounts[seg]++
		l.recordCount++
		l.recordBytes += recordBytes
		l.segmentBytes[seg] += recordBytes
		off += recordBytes
	}
	return nil
}

// Only the newest segment may end in a partially written record. Persist the
// repair before any new records can be appended over that tail.
func recoverLogTail(f *os.File, off uint64, head bool) error {
	if !head {
		return fmt.Errorf("incomplete record at offset %d: %w", off, io.ErrUnexpectedEOF)
	}
	if err := f.Truncate(int64(off)); err != nil {
		return fmt.Errorf("truncate tail at offset %d: %w", off, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync repaired tail: %w", err)
	}
	return nil
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
	if seg > maxSegment {
		return fmt.Errorf("segment capacity exceeded")
	}
	_, exists := l.segmentRecordCounts[seg]
	if l.headFile != nil && (seg != incrementSegment(l.headSegment) || exists) {
		return fmt.Errorf("next segment %d is not vacant or adjacent", seg)
	}
	if !exists && len(l.segmentRecordCounts) >= segmentCount-1 {
		return fmt.Errorf("segment ring must retain one vacant ID")
	}
	flag := os.O_RDWR | os.O_APPEND
	if !exists {
		flag |= os.O_CREATE | os.O_EXCL
	}
	f, err := l.openFile(seg, flag)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	var size uint64
	if exists {
		info, err := f.Stat()
		if err != nil {
			return errors.Join(fmt.Errorf("stat file: %w", err), f.Close())
		}
		size = uint64(info.Size())
	} else {
		// O_EXCL created an empty file, so no Stat can strand an untracked head.
		if err := errors.Join(f.Sync(), l.syncDirectory()); err != nil {
			l.writeErr = fmt.Errorf("persist new head; log requires recovery: %w", err)
			return errors.Join(l.writeErr, f.Close())
		}
		l.segmentRecordCounts[seg] = 0
		l.segmentBytes[seg] = 0
		l.segmentRefCounts[seg] = 0
	}
	old := l.headFile
	l.headFile = f
	l.headSegment = seg
	l.size = size
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

func (l *Log) write(ty recordType, key []byte, val []byte, expiration int64) (uint64, uint64, error) {
	return l.writeDurability(ty, key, val, expiration, Write)
}

func (l *Log) writeDurability(ty recordType, key []byte, val []byte, expiration int64, durability Durability) (uint64, uint64, error) {
	if l.writeErr != nil {
		return 0, 0, l.writeErr
	}
	// Rotate before appending so a failed rotation leaves the record unwritten
	// and every record starts at an offset that fits in the index.
	if l.size >= logSegmentSize {
		if err := l.flush(); err != nil {
			return 0, 0, err
		}
		if err := l.headFile.Sync(); err != nil {
			return 0, 0, fmt.Errorf("sync file: %w", err)
		}
		if err := l.openHead(incrementSegment(l.headSegment)); err != nil {
			return 0, 0, fmt.Errorf("open head: %w", err)
		}
	}
	if durability == Buffer && recordSize(len(key), len(val)) <= logPendingSize {
		return l.bufferRecord(ty, key, val, expiration)
	}
	if err := l.flush(); err != nil {
		return 0, 0, err
	}
	return l.appendRecord(ty, key, val, expiration)
}

func (l *Log) bufferRecord(ty recordType, key, val []byte, expiration int64) (uint64, uint64, error) {
	if err := ty.validate(uint64(len(val)), expiration); err != nil {
		return 0, 0, err
	}
	segment, off := l.headSegment, l.size
	if off&(logAlignment-1) != 0 {
		return 0, 0, fmt.Errorf("unaligned record offset %d", off)
	}
	size := int(recordSize(len(key), len(val)))
	if l.pending == nil {
		l.pending = make([]byte, 0, logPendingSize)
	}
	if size > cap(l.pending)-len(l.pending) {
		if err := l.flush(); err != nil {
			return 0, 0, err
		}
	}
	if len(l.pending) == 0 {
		l.bufferStart = off
	}
	previous := len(l.pending)
	l.pending = l.pending[:previous+size]
	encodeRecord(l.pending[previous:], ty, key, val, expiration)
	if len(l.pending) == cap(l.pending) {
		if err := l.flush(); err != nil {
			// Earlier acknowledged records remain readable and queued. The new
			// record is not published after an unsuccessful full-buffer flush.
			l.pending = l.pending[:previous]
			return 0, 0, err
		}
	}
	l.recordAppend(uint64(size))
	return segment, off, nil
}

// flush preserves pending bytes on failure. Partial file writes are rolled back
// before retrying, so both the buffer and the published index remain valid.
func (l *Log) flush() error {
	if len(l.pending) == 0 {
		return nil
	}
	if l.writeErr != nil {
		return l.writeErr
	}
	if err := l.writeBytes(l.pending, l.bufferStart); err != nil {
		return fmt.Errorf("flush log buffer: %w", err)
	}
	l.pending = l.pending[:0]
	return nil
}

func (l *Log) recordAppend(size uint64) {
	l.size += size
	l.segmentRecordCounts[l.headSegment]++
	l.recordCount++
	l.recordBytes += size
	l.segmentBytes[l.headSegment] += size
}

// appendRecord writes directly to the head file. The caller ensures the next
// record starts within the segment's offset range.
func (l *Log) appendRecord(ty recordType, key []byte, val []byte, expiration int64) (uint64, uint64, error) {
	if err := ty.validate(uint64(len(val)), expiration); err != nil {
		return 0, 0, err
	}
	segment, off := l.headSegment, l.size
	if off&(logAlignment-1) != 0 {
		return 0, 0, fmt.Errorf("unaligned record offset %d", off)
	}
	alignedSize := int(recordSize(len(key), len(val)))
	var record []byte
	if alignedSize <= len(l.writeBuffer) {
		record = l.writeBuffer[:alignedSize]
	} else {
		// Keep oversized records temporary instead of growing the shard buffer.
		record = make([]byte, alignedSize)
	}
	encodeRecord(record, ty, key, val, expiration)
	if err := l.writeBytes(record, off); err != nil {
		return 0, 0, err
	}
	l.recordAppend(uint64(len(record)))
	return segment, off, nil
}

func (l *Log) writeBytes(record []byte, off uint64) error {
	n, err := l.headFile.Write(record)
	if err == nil && n != len(record) {
		err = io.ErrShortWrite
	}
	if err != nil {
		err = fmt.Errorf("write record: %w", err)
		if n > 0 {
			// Remove the failed record before allowing another append.
			// O_APPEND makes the next write use EOF after this repair.
			var rollbackErr error
			if truncateErr := l.headFile.Truncate(int64(off)); truncateErr != nil {
				rollbackErr = fmt.Errorf("truncate failed write: %w", truncateErr)
			} else if syncErr := l.headFile.Sync(); syncErr != nil {
				rollbackErr = fmt.Errorf("sync rollback: %w", syncErr)
			}
			if rollbackErr != nil {
				l.writeErr = errors.Join(err, fmt.Errorf("rollback failed; log requires recovery: %w", rollbackErr))
				return l.writeErr
			}
		}
		return err
	}
	return nil
}

// encodeRecord fills a complete, aligned record. Callers validate the type and
// size the destination before encoding; compaction validates records on read.
func encodeRecord(record []byte, ty recordType, key, val []byte, expiration int64) {
	size := logHeaderSize + len(key) + len(val)
	// [crc 4][type 1][keylen 4][vallen 4][expiration 8][key][value][zero padding]
	record[4] = byte(ty)
	binary.BigEndian.PutUint32(record[5:], uint32(len(key)))
	binary.BigEndian.PutUint32(record[9:], uint32(len(val)))
	binary.BigEndian.PutUint64(record[13:], uint64(expiration))
	copy(record[logHeaderSize:], key)
	copy(record[logHeaderSize+len(key):], val)
	clear(record[size:])
	binary.BigEndian.PutUint32(record[:], crc32.ChecksumIEEE(record[4:size]))
}

// recordSize includes alignment padding for both retained and live byte counts.
func recordSize(keyBytes, valueBytes int) uint64 {
	return alignRecordSize(uint64(logHeaderSize) + uint64(keyBytes) + uint64(valueBytes))
}

func alignRecordSize(size uint64) uint64 {
	return (size + logAlignment - 1) &^ (logAlignment - 1)
}

// bufferedPrefix locates pending bytes by their final address. There is no
// second key index, and no record straddles the file/buffer boundary.
func (l *Log) bufferedPrefix(seg, off uint64) []byte {
	if len(l.pending) != 0 && seg == l.headSegment && off >= l.bufferStart && off-l.bufferStart < uint64(len(l.pending)) {
		return l.pending[off-l.bufferStart:]
	}
	return nil
}

// readPayload reads and verifies a record, returning its key, value, type, and
// expiration. It borrows buf when the payload fits; passing nil returns owned slices.
func (l *Log) readPayload(seg uint64, off uint64, buf []byte) ([]byte, []byte, recordType, int64, error) {
	if off >= logSegmentSize {
		return nil, nil, 0, 0, fmt.Errorf("%w: record offset %d exceeds segment capacity", ErrInvalidValue, off)
	}
	if prefix := l.bufferedPrefix(seg, off); prefix != nil {
		if len(prefix) < logHeaderSize {
			return nil, nil, 0, 0, fmt.Errorf("read header: %w", io.ErrUnexpectedEOF)
		}
		return l.readFromPrefix(nil, off, prefix, buf)
	}
	f := l.headFile
	if l.headSegment != seg {
		var err error
		f, err = l.openFile(seg, os.O_RDONLY)
		if err != nil {
			return nil, nil, 0, 0, fmt.Errorf("open sealed log: %w", err)
		}
		defer f.Close()
	}
	var prefix [1 << 10]byte
	n, err := f.ReadAt(prefix[:], int64(off))
	if err != nil && err != io.EOF {
		return nil, nil, 0, 0, fmt.Errorf("read header: %w", err)
	}
	if n < logHeaderSize {
		return nil, nil, 0, 0, fmt.Errorf("read header: %w", io.ErrUnexpectedEOF)
	}
	return l.readFromPrefix(f, off, prefix[:n], buf)
}

// readFromPrefix finishes and verifies a record after its header and
// possibly some payload bytes have been read. Both read paths use it, with
// readScalar falling back here only when the record exceeds its local buffer.
// prefix starts at off and must contain at least the 21-byte record header.
func (l *Log) readFromPrefix(f *os.File, off uint64, prefix []byte, buf []byte) ([]byte, []byte, recordType, int64, error) {
	keylen := uint64(binary.BigEndian.Uint32(prefix[5:]))
	vallen := uint64(binary.BigEndian.Uint32(prefix[9:]))
	expiration := int64(binary.BigEndian.Uint64(prefix[13:]))
	ty := recordType(prefix[4])
	if err := ty.validate(vallen, expiration); err != nil {
		return nil, nil, 0, 0, err
	}
	// The uint32 fields cannot overflow uint64. Check their sum before any
	// conversion to int, allocation, or slice indexing, including on 32-bit hosts.
	end := uint64(logHeaderSize) + keylen + vallen
	if off >= logSegmentSize || end > uint64(^uint(0)>>1) {
		return nil, nil, 0, 0, fmt.Errorf("%w: record extent exceeds address space", ErrInvalidValue)
	}
	if end > uint64(len(prefix)) {
		if f == nil {
			return nil, nil, 0, 0, fmt.Errorf("read payload: %w", io.ErrUnexpectedEOF)
		}
		// Small records already fit in prefix and need no extra syscall. Larger
		// records must fit the actual file before we allocate their payload. A
		// final record may extend beyond the nominal segment boundary.
		var info syscall.Stat_t
		err := syscall.Fstat(int(f.Fd()), &info)
		runtime.KeepAlive(f)
		if err != nil {
			return nil, nil, 0, 0, fmt.Errorf("stat payload: %w", err)
		}
		if info.Size < 0 || off > uint64(info.Size) || end > uint64(info.Size)-off {
			return nil, nil, 0, 0, fmt.Errorf("read payload: %w", io.ErrUnexpectedEOF)
		}
	}
	size := int(keylen + vallen)

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
			return nil, nil, 0, 0, fmt.Errorf("read payload: %w", err)
		}
	}
	if binary.BigEndian.Uint32(prefix[:]) != l.crc(prefix[:logHeaderSize], buf[:keylen], buf[keylen:]) {
		return nil, nil, 0, 0, fmt.Errorf("crc mismatch")
	}
	return buf[:keylen], buf[keylen:], ty, expiration, nil
}

// readScalar reads and verifies a record, returning its scalar bits, byte
// length, type, and expiration when the key matches. Key comparison stays here to keep
// record bytes on the stack. DB.Get validates the scalar type and decodes the returned bits.
func (l *Log) readScalar(seg uint64, off uint64, key []byte) (scalarValue, int64, bool, error) {
	if off >= logSegmentSize {
		return scalarValue{}, 0, false, fmt.Errorf("%w: record offset %d exceeds segment capacity", ErrInvalidValue, off)
	}
	if record := l.bufferedPrefix(seg, off); record != nil {
		if len(record) < logHeaderSize {
			return scalarValue{}, 0, false, fmt.Errorf("read header: %w", io.ErrUnexpectedEOF)
		}
		keylen := uint64(binary.BigEndian.Uint32(record[5:]))
		vallen := uint64(binary.BigEndian.Uint32(record[9:]))
		expiration := int64(binary.BigEndian.Uint64(record[13:]))
		ty := recordType(record[4])
		if err := ty.validate(vallen, expiration); err != nil {
			return scalarValue{}, 0, false, err
		}
		end := logHeaderSize + keylen + vallen
		if end > uint64(len(record)) {
			return scalarValue{}, 0, false, fmt.Errorf("read payload: %w", io.ErrUnexpectedEOF)
		}
		storedKey, value := record[logHeaderSize:logHeaderSize+keylen], record[logHeaderSize+keylen:end]
		if binary.BigEndian.Uint32(record[:]) != l.crc(record[:logHeaderSize], storedKey, value) {
			return scalarValue{}, 0, false, fmt.Errorf("crc mismatch")
		}
		if !bytes.Equal(storedKey, key) {
			return scalarValue{}, 0, false, nil
		}
		return scalarBytes(ty, value), expiration, true, nil
	}
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
	readSize := logHeaderSize + min(len(key), len(record)-logHeaderSize-8) + 8
	n, err := f.ReadAt(record[:readSize], int64(off))
	if err != nil && err != io.EOF {
		return scalarValue{}, 0, false, fmt.Errorf("read header: %w", err)
	}
	if n < logHeaderSize {
		return scalarValue{}, 0, false, fmt.Errorf("read header: %w", io.ErrUnexpectedEOF)
	}
	keylen := uint64(binary.BigEndian.Uint32(record[5:]))
	vallen := uint64(binary.BigEndian.Uint32(record[9:]))
	expiration := int64(binary.BigEndian.Uint64(record[13:]))
	ty := recordType(record[4])
	if err := ty.validate(vallen, expiration); err != nil {
		return scalarValue{}, 0, false, err
	}
	end := uint64(logHeaderSize) + keylen + vallen
	if end > uint64(len(record)) {
		storedKey, value, ty, expiration, err := l.readFromPrefix(f, off, record[:n], nil)
		if err != nil {
			return scalarValue{}, 0, false, err
		}
		if !bytes.Equal(storedKey, key) {
			return scalarValue{}, 0, false, nil
		}
		return scalarBytes(ty, value), expiration, true, nil
	}
	if uint64(n) < end {
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
	return scalarBytes(ty, record[logHeaderSize+keylen:end]), expiration, true, nil
}
