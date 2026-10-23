package dot

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"slices"
	"syscall"
	"testing"
)

func TestLogPartialWriteRollback(t *testing.T) {
	const helperEnv = "DOT_TEST_PARTIAL_WRITE"
	if os.Getenv(helperEnv) == "" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestLogPartialWriteRollback$")
		cmd.Env = append(os.Environ(), helperEnv+"=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%v\n%s", err, output)
		}
		return
	}
	// Limit file sizes only in this child, allowing real partial writes without
	// affecting other tests. The limit also triggers SIGXFSZ on the next write.
	signal.Ignore(syscall.SIGXFSZ)
	db := newInternalDB(t)
	key, old := []byte("key"), []byte("old")
	if err := db.Set(key, old); err != nil {
		t.Fatal(err)
	}
	s, _ := db.shard(key)
	size := s.log.size
	before, err := os.ReadFile(s.log.headFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	slots := slices.Clone(s.index.slots)
	value := bytes.Repeat([]byte{'v'}, 2*logWriteBufferSize)
	// Fail within the CRC, the rest of the header, the key, the value, and
	// padding after the complete checksummed payload has already been written.
	payloadBytes := uint64(logHeaderSize + len(key) + len(value))
	for _, progress := range []uint64{1, 3, 4, logHeaderSize - 1, logHeaderSize, logHeaderSize + 1, payloadBytes - 1, payloadBytes, payloadBytes + 1, recordSize(len(key), len(value)) - 1} {
		t.Run(fmt.Sprintf("progress=%d", progress), func(t *testing.T) {
			var original syscall.Rlimit
			if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &original); err != nil {
				t.Fatal(err)
			}
			limited := original
			limited.Cur = size + progress
			if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limited); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &original); err != nil {
					t.Fatal(err)
				}
			}()
			writeErr := db.Set(key, value)
			if !errors.Is(writeErr, syscall.EFBIG) {
				t.Fatalf("partial write: error=%v, want EFBIG", writeErr)
			}
			if s.log.size != size || s.log.writeErr != nil || !slices.Equal(slots, s.index.slots) {
				t.Fatal("partial write changed the committed size or index, or rollback failed")
			}
			if data, err := os.ReadFile(s.log.headFile.Name()); err != nil || !bytes.Equal(data, before) {
				t.Fatalf("partial write was not rolled back: length=%d error=%v", len(data), err)
			}
			if got, err := db.Get[[]byte](key); err != nil || !bytes.Equal(got, old) {
				t.Fatalf("partial write changed value: value=%q error=%v", got, err)
			}
			checkSegmentAccounting(t, s)
		})
	}
	if err := db.Set(key, value); err != nil {
		t.Fatalf("retry after partial write: %v", err)
	}
	_, hash := db.shard(key)
	pos, _, _, _, err := s.index.findPayload(s.log, key, hash, nil)
	if err != nil || pos < 0 || s.index.slots[pos]&addressMask != logAddress(0, size) {
		t.Fatalf("retry used wrong offset: position=%d error=%v, want offset %d", pos, err, size)
	}
	db = reopenDB(t, db)
	s, _ = db.shard(key)
	checkSegmentAccounting(t, s)
	if got, err := db.Get[[]byte](key); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("recovery after partial write and retry: length=%d error=%v", len(got), err)
	}
}

func TestBufferPartialFlushRollback(t *testing.T) {
	const helperEnv = "DOT_TEST_BUFFER_PARTIAL_WRITE"
	if os.Getenv(helperEnv) == "" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestBufferPartialFlushRollback$")
		cmd.Env = append(os.Environ(), helperEnv+"=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%v\n%s", err, output)
		}
		return
	}
	signal.Ignore(syscall.SIGXFSZ)
	for _, trigger := range []string{"flush", "full-buffer", "overflow", "oversized"} {
		t.Run(trigger, func(t *testing.T) {
			log, idx := openRecoveryLog(t, t.TempDir())
			s := &Shard{log: log, index: idx}
			key := []byte("key")
			const hash = uint64(42)
			old := []byte("old")
			change, err := idx.prepareSet(log, typeBytes, key, old, hash, 0)
			if err != nil {
				t.Fatal(err)
			}
			// Use appendSet directly to keep timers out of fault injection.
			if err := s.appendSet(typeBytes, key, old, change, Buffer); err != nil {
				t.Fatal(err)
			}
			slots, pending := slices.Clone(idx.slots), bytes.Clone(log.pending)
			size := log.size
			newSize := logPendingSize - int(size)
			if trigger == "overflow" {
				newSize += logAlignment
			} else if trigger == "oversized" {
				newSize = logPendingSize + logAlignment
			}
			value := bytes.Repeat([]byte{'v'}, newSize-logHeaderSize-len(key))
			change, err = idx.prepareSet(log, typeBytes, key, value, hash, 0)
			if err != nil {
				t.Fatal(err)
			}
			var original syscall.Rlimit
			if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &original); err != nil {
				t.Fatal(err)
			}
			func() {
				limited := original
				limited.Cur = 17 // Fail partway through the first acknowledged record.
				if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limited); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &original); err != nil {
						t.Fatal(err)
					}
				}()
				var err error
				if trigger == "flush" {
					err = log.flush()
				} else {
					err = s.appendSet(typeBytes, key, value, change, Buffer)
				}
				if !errors.Is(err, syscall.EFBIG) {
					t.Fatalf("partial buffer flush error=%v, want EFBIG", err)
				}
			}()
			if !slices.Equal(slots, idx.slots) || !bytes.Equal(pending, log.pending) || log.size != size || log.writeErr != nil {
				t.Fatal("failed flush changed the acknowledged index, bytes, or logical size")
			}
			if info, err := log.headFile.Stat(); err != nil || info.Size() != 0 {
				t.Fatalf("partial file write not rolled back: %v error=%v", info, err)
			}
			if _, got, _, _, err := idx.findPayload(log, key, hash, nil); err != nil || !bytes.Equal(got, old) {
				t.Fatalf("pending value lost after failure: %q error=%v", got, err)
			}
			checkSegmentAccounting(t, s)
			if err := log.flush(); err != nil {
				t.Fatalf("flush retry: %v", err)
			}
			if err := s.appendSet(typeBytes, key, value, change, Buffer); err != nil {
				t.Fatalf("append retry: %v", err)
			}
			if err := log.flush(); err != nil {
				t.Fatal(err)
			}
			checkSegmentAccounting(t, s)
			if err := log.headFile.Close(); err != nil {
				t.Fatal(err)
			}
			log, idx = openRecoveryLog(t, log.path)
			checkRecoveredValue(t, log, idx, key, value)
		})
	}
}
