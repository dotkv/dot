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
	// Fail within the CRC, the rest of the header, the key, and the value.
	for _, progress := range []uint64{1, 3, 4, logHeaderSize - 1, logHeaderSize, logHeaderSize + 1, uint64(logHeaderSize + len(key) + len(value) - 1)} {
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
	if err != nil || pos < 0 || s.index.slots[pos]&addressMask != size {
		t.Fatalf("retry used wrong offset: position=%d error=%v, want offset %d", pos, err, size)
	}
	db = reopenDB(t, db)
	s, _ = db.shard(key)
	checkSegmentAccounting(t, s)
	if got, err := db.Get[[]byte](key); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("recovery after partial write and retry: length=%d error=%v", len(got), err)
	}
}
