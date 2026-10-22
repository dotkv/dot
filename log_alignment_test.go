package dot

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

func TestIndexAlignedAddressLayout(t *testing.T) {
	// Pin the compressed address independently of the encoding constants,
	// including the final usable start offset and all 19 hashtag bits.
	for _, tc := range []struct {
		segment, offset, packed uint64
	}{
		{0, 0, 0},
		{0, 16, 1},
		{1, 0, 1 << 22},
		{1048575, (1 << 26) - 16, (1 << 42) - 1},
	} {
		packed := logAddress(tc.segment, tc.offset)
		if packed != tc.packed || addressSegment(packed) != tc.segment || addressOffset(packed) != tc.offset {
			t.Fatalf("address (%d, %d): packed=%#x, want %#x", tc.segment, tc.offset, packed, tc.packed)
		}
	}
	tag := hashtag(((1 << 19) - 1) << 14)
	slot := logAddress(1048575, (1<<26)-16) | tag | metadataMask
	if slot != ^uint64(0) || addressOffset(slot) != (1<<26)-16 || addressSegment(slot) != 1048575 {
		t.Fatalf("address, hashtag, and metadata fields overlap or leave gaps: %#x", slot)
	}
	if hashtag(1<<(14+19)) != 0 {
		t.Fatal("hashtag retained a bit outside the 19-bit budget")
	}
	idx := newInternalIndex(t)
	if len(idx.slots)*8*defaultShardCount != 8<<20 || indexBits+hashtagBits != 33 {
		t.Fatal("index does not provide 8 MiB initial mappings and the 2^33-slot growth limit")
	}
}

func TestLogAlignedRecordSizes(t *testing.T) {
	log, _ := openRecoveryLog(t, t.TempDir())
	key := make([]byte, 16)
	for _, tc := range []struct{ value, bytes int }{{8, 48}, {1024, 1072}} {
		before := log.size
		_, off, err := log.write(typeBytes, key, make([]byte, tc.value), 0)
		if err != nil || off != before || log.size-before != uint64(tc.bytes) {
			t.Fatalf("%d-byte value: offset=%d bytes=%d error=%v, want %d bytes", tc.value, off, log.size-before, err, tc.bytes)
		}
	}
}

func TestLogRecoveryRejectsNonzeroPadding(t *testing.T) {
	for _, sealed := range []bool{false, true} {
		t.Run(map[bool]string{false: "head", true: "sealed"}[sealed], func(t *testing.T) {
			path := t.TempDir()
			record := recoveryRecord(typeBytes, []byte("key"), []byte("value"), 0)
			record[len(record)-1] = 1 // Outside the checksum, inside required zero padding.
			name := writeRecoverySegment(t, path, 0, record)
			if sealed {
				writeRecoverySegment(t, path, 1, nil)
			}
			log, err := newLog(path, newInternalIndex(t))
			if log != nil {
				log.headFile.Close()
			}
			if log != nil || err == nil || !strings.Contains(err.Error(), "nonzero padding") {
				t.Fatalf("corrupt padding accepted: %v", err)
			}
			if got, err := os.ReadFile(name); err != nil || !bytes.Equal(got, record) {
				t.Fatalf("failed recovery changed the record: %v", err)
			}
		})
	}
}
