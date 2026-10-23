package dot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"slices"
	"strconv"
	"syscall"
	"testing"
)

func TestLogReadRejectsInvalidLengths(t *testing.T) {
	const helperEnv = "DOT_TEST_INVALID_LENGTHS"
	if os.Getenv(helperEnv) == "" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestLogReadRejectsInvalidLengths$", "-test.v")
		cmd.Env = append(os.Environ(), helperEnv+"=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%v\n%s", err, output)
		}
		return
	}
	// A regression must fail in this subprocess rather than allocate several
	// GiB on the test host. Allow modest growth beyond the runtime's existing
	// mappings, including the race detector's much larger virtual reservation.
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		t.Fatal(err)
	}
	pages, err := strconv.ParseUint(string(bytes.Fields(data)[0]), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	var original syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_AS, &original); err != nil {
		t.Fatal(err)
	}
	limited := original
	limit := pages*uint64(os.Getpagesize()) + 256<<20
	if limited.Cur > limit {
		limited.Cur = limit
	}
	if err := syscall.Setrlimit(syscall.RLIMIT_AS, &limited); err != nil {
		t.Fatal(err)
	}
	defer syscall.Setrlimit(syscall.RLIMIT_AS, &original)

	key := []byte("key")
	for _, storage := range []string{"head", "sealed", "buffer"} {
		for _, tc := range []struct {
			name           string
			ty             recordType
			keylen, vallen uint32
			invalidType    bool
		}{
			{"truncated", typeBytes, 3, 9, false},
			{"beyond-prefix", typeBytes, 3, 1024, false},
			{"large-value", typeBytes, 3, 16 << 20, false},
			{"large-key", typeBytes, 16 << 20, 8, false},
			{"int32-conversion", typeBytes, 1 << 31, 8, false},
			{"int32-addition", typeBytes, 1<<31 - 1, 1<<31 - 1, false},
			{"uint32-max", typeBytes, ^uint32(0), ^uint32(0), false},
			{"scalar-size", typeInt64, 3, 16 << 20, true},
			{"bool-size", typeBoolTrue, 3, ^uint32(0), true},
			{"unknown-type", 0xff, ^uint32(0), ^uint32(0), true},
		} {
			t.Run(storage+"/"+tc.name, func(t *testing.T) {
				log, _ := openRecoveryLog(t, t.TempDir())
				record := recoveryRecord(typeBytes, key, make([]byte, 8), 0)
				record[4] = byte(tc.ty)
				binary.BigEndian.PutUint32(record[5:], tc.keylen)
				binary.BigEndian.PutUint32(record[9:], tc.vallen)
				if storage == "buffer" {
					log.pending = record
				} else {
					if err := os.WriteFile(log.headFile.Name(), record, 0600); err != nil {
						t.Fatal(err)
					}
					if storage == "sealed" {
						if err := log.openHead(1); err != nil {
							t.Fatal(err)
						}
					}
				}
				for _, read := range []struct {
					name string
					run  func() error
				}{
					{"payload", func() error { _, _, _, _, err := log.readPayload(0, 0, nil); return err }},
					{"scratch-payload", func() error { _, _, _, _, err := log.readPayload(0, 0, log.writeBuffer[:]); return err }},
					{"scalar", func() error { _, _, _, err := log.readScalar(0, 0, key); return err }},
				} {
					var before, after runtime.MemStats
					runtime.ReadMemStats(&before)
					err := read.run()
					runtime.ReadMemStats(&after)
					if tc.invalidType {
						if !errors.Is(err, ErrInvalidValue) {
							t.Fatalf("%s: type/length error=%v", read.name, err)
						}
					} else if !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) && !errors.Is(err, ErrInvalidValue) {
						t.Fatalf("%s: length error=%v", read.name, err)
					}
					if allocated := after.TotalAlloc - before.TotalAlloc; allocated >= 1<<20 {
						t.Fatalf("%s: allocated %d bytes before rejecting lengths", read.name, allocated)
					}
				}
			})
		}
	}
}

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
