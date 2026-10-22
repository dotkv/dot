package dot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"slices"
	"syscall"
	"testing"
)

func TestCompactionBufferedWriteFailures(t *testing.T) {
	const helperEnv = "DOT_TEST_COMPACTION_WRITE"
	if os.Getenv(helperEnv) == "" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestCompactionBufferedWriteFailures$")
		cmd.Env = append(os.Environ(), helperEnv+"=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%v\n%s", err, output)
		}
		return
	}
	signal.Ignore(syscall.SIGXFSZ)
	for _, tc := range []struct {
		name                     string
		count, valueBytes, limit int
	}{
		{"final-flush", 4, 8, 100},
		{"full-buffer", 1600, 8, 65000},
		{"oversized-record", 1, 128 << 10, 65000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := t.TempDir()
			value := bytes.Repeat([]byte{'v'}, tc.valueBytes)
			var key [16]byte
			data := recoveryRecord(typeBytes, key[:], []byte("obsolete"), 0)
			for i := range tc.count {
				binary.LittleEndian.PutUint64(key[:], uint64(i))
				data = append(data, recoveryRecord(typeBytes, key[:], value, 0)...)
			}
			name := writeRecoverySegment(t, path, 0, data)
			log, idx := openRecoveryLog(t, path)
			log.compactPending = true
			shard := &Shard{log: log, index: idx}
			slots := slices.Clone(idx.slots)
			var original syscall.Rlimit
			if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &original); err != nil {
				t.Fatal(err)
			}
			func() {
				limited := original
				limited.Cur = uint64(tc.limit)
				if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limited); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &original); err != nil {
						t.Fatal(err)
					}
				}()
				if err := shard.compact(); !errors.Is(err, syscall.EFBIG) {
					t.Fatalf("compaction error=%v, want EFBIG", err)
				}
			}()
			if !slices.Equal(slots, idx.slots) || log.writeErr != nil || !log.compactPending {
				t.Fatal("unpublished write failure changed the index or recovery state")
			}
			if current, err := os.ReadFile(name); err != nil || !bytes.Equal(current, data) {
				t.Fatalf("failed compaction changed its source: %v", err)
			}
			if _, err := os.Stat(name + ".compact"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed compaction left staging output: %v", err)
			}
			checkSegmentAccounting(t, shard)
			if err := shard.compact(); err != nil {
				t.Fatalf("retry: %v", err)
			}
			if log.compactPending || log.recordCount != uint64(tc.count) {
				t.Fatal("retry did not reclaim obsolete records")
			}
			checkSegmentAccounting(t, shard)
			if err := log.headFile.Close(); err != nil {
				t.Fatal(err)
			}
			log, idx = openRecoveryLog(t, path)
			for i := range tc.count {
				binary.LittleEndian.PutUint64(key[:], uint64(i))
				checkRecoveredValue(t, log, idx, key[:], value)
			}
		})
	}
}
