package dot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cespare/xxhash/v2"
)

func recoveryRecord(ty byte, key, value []byte, expiration int64) []byte {
	record := make([]byte, logHeaderSize+len(key)+len(value))
	record[4] = ty
	binary.BigEndian.PutUint32(record[5:], uint32(len(key)))
	binary.BigEndian.PutUint32(record[9:], uint32(len(value)))
	binary.BigEndian.PutUint64(record[13:], uint64(expiration))
	copy(record[logHeaderSize:], key)
	copy(record[logHeaderSize+len(key):], value)
	binary.BigEndian.PutUint32(record, crc32.ChecksumIEEE(record[4:]))
	return record
}

func writeRecoverySegment(t *testing.T, path string, seg uint64, data []byte) string {
	t.Helper()
	name := filepath.Join(path, fmt.Sprintf("%07d.log", seg))
	if err := os.WriteFile(name, data, 0600); err != nil {
		t.Fatal(err)
	}
	return name
}

func openRecoveryLog(t *testing.T, path string) (*Log, *Index) {
	t.Helper()
	idx := newInternalIndex(t)
	log, err := newLog(path, idx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := log.headFile.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Error(err)
		}
	})
	return log, idx
}

func checkRecoveredValue(t *testing.T, log *Log, idx *Index, key, want []byte) int {
	t.Helper()
	pos, got, _, err := idx.findPayload(log, key, xxhash.Sum64(key), nil)
	if err != nil || pos < 0 || !bytes.Equal(got, want) {
		t.Fatalf("key %q: position=%d value=%q error=%v, want %q", key, pos, got, err, want)
	}
	return pos
}

func TestLogRecoveryOrderAndEmptyHead(t *testing.T) {
	path := t.TempDir()
	key := []byte("key")
	record := recoveryRecord('s', key, []byte("new"), 0)
	// The last two identical sets must still restore the latest address.
	writeRecoverySegment(t, path, 10, append(bytes.Clone(record), record...))
	writeRecoverySegment(t, path, 2, recoveryRecord('s', key, []byte("old"), 0))
	writeRecoverySegment(t, path, 11, nil)
	if err := os.WriteFile(filepath.Join(path, "unrelated"), []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	log, idx := openRecoveryLog(t, path)
	pos := checkRecoveredValue(t, log, idx, key, []byte("new"))
	if addr := idx.slots[pos] & addressMask; addr != 10<<26|uint64(len(record)) {
		t.Fatalf("got address %d, want latest record in segment 10", addr)
	}
	if log.headSegment != 11 || log.size != 0 {
		t.Fatalf("head: segment=%d size=%d, want 11 and 0", log.headSegment, log.size)
	}
	if seg, off, err := log.write('s', key, []byte("appended"), 0); err != nil || seg != 11 || off != 0 {
		t.Fatalf("append: segment=%d offset=%d error=%v", seg, off, err)
	}
}

func TestLogRecoveryIncompleteHead(t *testing.T) {
	key := []byte("key")
	old := recoveryRecord('s', key, []byte("old"), 0)
	update := recoveryRecord('s', key, []byte("new value"), 0)
	for _, prefix := range [][]byte{nil, old} {
		for cut := 0; cut < len(update); cut++ {
			t.Run(fmt.Sprintf("prefix=%d/cut=%d", len(prefix), cut), func(t *testing.T) {
				path := t.TempDir()
				name := writeRecoverySegment(t, path, 0, append(bytes.Clone(prefix), update[:cut]...))
				log, idx := openRecoveryLog(t, path)
				if len(prefix) > 0 {
					checkRecoveredValue(t, log, idx, key, []byte("old"))
				} else if idx.population != 0 {
					t.Fatal("incomplete first record populated the index")
				}
				if data, err := os.ReadFile(name); err != nil || !bytes.Equal(data, prefix) || log.size != uint64(len(prefix)) {
					t.Fatalf("tail repair: length=%d size=%d error=%v, want %d", len(data), log.size, err, len(prefix))
				}
				if _, off, err := log.write('s', key, []byte("appended"), 0); err != nil || off != uint64(len(prefix)) {
					t.Fatalf("append after repair: offset=%d error=%v", off, err)
				}
				if err := log.headFile.Close(); err != nil {
					t.Fatal(err)
				}
				log, idx = openRecoveryLog(t, path)
				checkRecoveredValue(t, log, idx, key, []byte("appended"))
			})
		}
	}
}

func TestLogRecoveryRejectsCorruption(t *testing.T) {
	valid := recoveryRecord('s', []byte("key"), []byte("value"), 0)
	badCRC := bytes.Clone(valid)
	badCRC[len(badCRC)-1] ^= 1
	for _, sealed := range []bool{false, true} {
		for _, tc := range []struct {
			name string
			data []byte
			want string
		}{
			{"checksum", badCRC, "crc mismatch"},
			{"type", recoveryRecord('x', nil, nil, 0), "invalid record"},
			{"delete-value", recoveryRecord('d', []byte("key"), []byte("value"), 0), "invalid record"},
			{"delete-expiration", recoveryRecord('d', []byte("key"), nil, 1), "invalid record"},
			{"short-header", valid[:logHeaderSize-1], "incomplete record"},
			{"short-payload", valid[:len(valid)-1], "incomplete record"},
		} {
			if !sealed && strings.HasPrefix(tc.name, "short-") {
				continue // Incomplete head records are covered by the repair test.
			}
			t.Run(fmt.Sprintf("sealed=%t/%s", sealed, tc.name), func(t *testing.T) {
				path := t.TempDir()
				name := writeRecoverySegment(t, path, 0, tc.data)
				if sealed {
					writeRecoverySegment(t, path, 1, nil)
				}
				log, err := newLog(path, newInternalIndex(t))
				if err == nil || log != nil || !strings.Contains(err.Error(), tc.want) {
					if log != nil {
						log.headFile.Close()
					}
					t.Fatalf("corrupt log: log=%v error=%v, want %s", log, err, tc.want)
				}
				if data, err := os.ReadFile(name); err != nil || !bytes.Equal(data, tc.data) {
					t.Fatalf("failed recovery changed data: %v", err)
				}
				if got := openFilesUnder(t, path); got != 0 {
					t.Fatalf("failed recovery leaked %d descriptors", got)
				}
			})
		}
	}
}

func TestLogRecoverySegmentNames(t *testing.T) {
	for _, name := range []string{".log", "0.log", "00000000.log", "not-a-number.log", "1048576.log"} {
		t.Run(name, func(t *testing.T) {
			path := t.TempDir()
			if err := os.WriteFile(filepath.Join(path, name), nil, 0600); err != nil {
				t.Fatal(err)
			}
			if log, err := newLog(path, newInternalIndex(t)); err == nil || log != nil {
				if log != nil {
					log.headFile.Close()
				}
				t.Fatalf("accepted invalid segment %q: %v", name, err)
			}
		})
	}
}

func TestLogRecoveryGrowsIndex(t *testing.T) {
	path := t.TempDir()
	log, _ := openRecoveryLog(t, path)
	const count = (1<<indexBits)*3/4 + 1
	for i := range count {
		key := fmt.Appendf(nil, "key-%d", i)
		if _, _, err := log.write('s', key, key, 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.headFile.Close(); err != nil {
		t.Fatal(err)
	}
	log, idx := openRecoveryLog(t, path)
	if idx.population != count || len(idx.slots) != 2<<indexBits {
		t.Fatalf("recovered index: population=%d capacity=%d", idx.population, len(idx.slots))
	}
	for i := range count {
		key := fmt.Appendf(nil, "key-%d", i)
		checkRecoveredValue(t, log, idx, key, key)
	}
}

func TestLogRecoveryRotationAtBoundary(t *testing.T) {
	path := t.TempDir()
	log, _ := openRecoveryLog(t, path)
	key := []byte("key")
	value := make([]byte, logWriteBufferSize-logHeaderSize-len(key))
	for range logSegmentSize/logWriteBufferSize - 1 {
		if _, _, err := log.write('s', key, value, 0); err != nil {
			t.Fatal(err)
		}
	}
	// The final record starts within the offset range and ends one byte past it.
	value = append(value, 1)
	if _, _, err := log.write('s', key, value, 0); err != nil {
		t.Fatal(err)
	}
	if err := log.headFile.Close(); err != nil {
		t.Fatal(err)
	}
	log, idx := openRecoveryLog(t, path)
	if log.size != logSegmentSize+1 {
		t.Fatalf("recovered size=%d, want %d", log.size, logSegmentSize+1)
	}
	checkRecoveredValue(t, log, idx, key, value)
	if seg, off, err := log.write('s', []byte("next"), nil, 0); err != nil || seg != 1 || off != 0 {
		t.Fatalf("rotation after recovery: segment=%d offset=%d error=%v", seg, off, err)
	}
}
