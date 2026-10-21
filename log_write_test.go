package dot

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLogWriteRollbackFailure(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Close(); writer.Close() })
	readResult := make(chan error, 1)
	go func() {
		// Consume some bytes, then break a write larger than the pipe buffer.
		var prefix [10]byte
		_, err := io.ReadFull(reader, prefix[:])
		reader.Close()
		readResult <- err
	}()
	log := Log{headFile: writer}
	_, _, writeErr := log.write(typeBytes, []byte("key"), make([]byte, 2<<20), 0)
	if err := <-readResult; err != nil {
		t.Fatal(err)
	}
	// A pipe cannot be truncated, so the partial write cannot be rolled back.
	if writeErr == nil || log.writeErr == nil || !strings.Contains(writeErr.Error(), "rollback failed") {
		t.Fatalf("write error=%v, stored error=%v; want failed rollback", writeErr, log.writeErr)
	}
	if log.size != 0 {
		t.Fatalf("failed write advanced size to %d", log.size)
	}
	f, err := os.OpenFile(filepath.Join(t.TempDir(), "head.log"), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	log.headFile = f
	for _, size := range []uint64{0, logSegmentSize} {
		log.size = size
		if _, _, err := log.write(typeBytes, nil, nil, 0); err != writeErr {
			t.Fatalf("subsequent write error=%v, want %v", err, writeErr)
		}
		if info, err := f.Stat(); err != nil || info.Size() != 0 || log.headSegment != 0 {
			t.Fatalf("blocked write appended or rotated: stat=%v segment=%d error=%v", info, log.headSegment, err)
		}
	}
}

func TestLogWriteFailureWithoutProgress(t *testing.T) {
	path := t.TempDir()
	log, _ := openRecoveryLog(t, path)
	key, old := []byte("key"), []byte("old")
	if _, _, err := log.write(typeBytes, key, old, 0); err != nil {
		t.Fatal(err)
	}
	size, name := log.size, log.headFile.Name()
	beforeBytes := log.recordBytes
	if err := log.headFile.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	log.headFile, err = os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := log.write(typeBytes, key, []byte("failed"), 0); err == nil {
		t.Fatal("write to read-only log succeeded")
	}
	if log.size != size || log.writeErr != nil {
		t.Fatal("zero-progress failure changed the size or blocked future writes")
	}
	if log.recordBytes != beforeBytes || log.segmentBytes[log.headSegment] != beforeBytes {
		t.Fatal("zero-progress failure changed byte accounting")
	}
	if err := log.headFile.Close(); err != nil {
		t.Fatal(err)
	}
	log.headFile, err = os.OpenFile(name, os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, off, err := log.write(typeBytes, key, []byte("new"), 0); err != nil || off != size {
		t.Fatalf("retry: offset=%d error=%v, want offset %d", off, err, size)
	}
	if err := log.headFile.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
	log, idx := openRecoveryLog(t, path)
	pos := checkRecoveredValue(t, log, idx, key, []byte("new"))
	if addr := idx.slots[pos] & addressMask; addr != size {
		t.Fatalf("retry address=%d, want %d", addr, size)
	}
	if got, _, _, _, err := log.readPayload(0, 0, nil); err != nil || !bytes.Equal(got, key) {
		t.Fatalf("original record damaged: key=%q error=%v", got, err)
	}
}
