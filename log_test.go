package dot

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestLogRotationWraps(t *testing.T) {
	path := t.TempDir()
	writeRecoverySegment(t, path, maxSegment, nil)
	log, _ := openRecoveryLog(t, path)
	padLog(t, log, logSegmentSize)
	seg, off, err := log.write(typeBytes, []byte("key"), []byte("new"), 0)
	if err != nil || seg != 0 || off != 0 {
		t.Fatalf("wrapped rotation: segment=%d offset=%d error=%v", seg, off, err)
	}
	if len(log.segmentRecordCounts) != 2 || log.segmentRecordCounts[0] != 1 {
		t.Fatalf("rotated counts=%v", log.segmentRecordCounts)
	}
}

func TestLogRotationPreservesLastVacantID(t *testing.T) {
	log, _ := openRecoveryLog(t, t.TempDir())
	// Populate accounting without creating a million files: reservation must
	// reject the rotation before attempting any filesystem operation.
	for seg := uint64(2); seg <= maxSegment; seg++ {
		log.segmentRecordCounts[seg] = 0
	}
	if err := log.openHead(1); err == nil || log.headSegment != 0 {
		t.Fatalf("rotation consumed the last vacant ID: %v", err)
	}
	if _, err := os.Stat(log.segmentPath(1)); !os.IsNotExist(err) {
		t.Fatalf("rotation created the last vacant segment: %v", err)
	}
}

func TestNewLogCurrentDirectory(t *testing.T) {
	t.Chdir(t.TempDir())
	log, err := newLog(".", newInternalIndex(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := log.headFile.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := os.Stat("0000000.log"); err != nil {
		t.Fatal(err)
	}
}

func TestLogCRCCompatibility(t *testing.T) {
	var log Log
	random := rand.NewChaCha8([32]byte{1})
	for i := range 1000 {
		var header [logHeaderSize]byte
		random.Read(header[:])
		key, value := make([]byte, i%97), make([]byte, i*17%4097)
		random.Read(key)
		random.Read(value)
		original := crc32.NewIEEE()
		original.Write(header[4:])
		original.Write(key)
		original.Write(value)
		if got, want := log.crc(header[:], key, value), original.Sum32(); got != want {
			t.Fatalf("case %d: checksum %08x, want %08x", i, got, want)
		}
	}
}

func TestLogBufferedWrites(t *testing.T) {
	log, _ := openRecoveryLog(t, t.TempDir())
	key := []byte("key")
	records := []struct {
		ty         recordType
		key, value []byte
		expiration int64
		offset     uint64
	}{
		{ty: typeBytes},
		{ty: typeBytes, key: key, value: bytes.Repeat([]byte{'a'}, logWriteBufferSize-logHeaderSize-len(key)-1), expiration: 123456789},
		{ty: typeBytes, key: key, value: bytes.Repeat([]byte{'b'}, logWriteBufferSize-logHeaderSize-len(key))},
		{ty: typeBytes, key: key, value: bytes.Repeat([]byte{'c'}, logWriteBufferSize-logHeaderSize-len(key)+1), expiration: -42},
		{ty: typeBytes, key: bytes.Repeat([]byte{'k'}, 2*logWriteBufferSize), value: []byte("large key")},
		{ty: typeBytes, key: key, value: bytes.Repeat([]byte{'v'}, 16*logWriteBufferSize)},
		{ty: typeBytes, key: key, value: []byte("small again"), expiration: 987654321},
		{ty: typeDelete, key: key},
	}
	var size uint64
	for i := range records {
		r := &records[i]
		segment, off, err := log.write(r.ty, r.key, r.value, r.expiration)
		if err != nil {
			t.Fatalf("write record %d: %v", i, err)
		}
		if segment != 0 || off != size {
			t.Fatalf("record %d: got address (%d, %d), want (0, %d)", i, segment, off, size)
		}
		r.offset = off
		size += recordSize(len(r.key), len(r.value))
	}
	data, err := os.ReadFile(log.headFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	if log.size != size || uint64(len(data)) != size {
		t.Fatalf("log size: counter %d, file %d, want %d", log.size, len(data), size)
	}
	for i, r := range records {
		end := r.offset + uint64(logHeaderSize+len(r.key)+len(r.value))
		next := r.offset + recordSize(len(r.key), len(r.value))
		if r.offset%16 != 0 || next%16 != 0 {
			t.Fatalf("record %d: unaligned extent [%d, %d)", i, r.offset, next)
		}
		for _, b := range data[end:next] {
			if b != 0 {
				t.Fatalf("record %d: padding retained bytes from a previous write", i)
			}
		}
		k, v, ty, exp, err := log.readPayload(0, r.offset, nil)
		if err != nil || !bytes.Equal(k, r.key) || !bytes.Equal(v, r.value) || exp != r.expiration || ty != r.ty || recordType(data[r.offset+4]) != r.ty {
			t.Fatalf("record %d: key/value/expiration/type mismatch or read error: %v", i, err)
		}
	}
}

func TestLogReadRejectsCorruption(t *testing.T) {
	log, _ := openRecoveryLog(t, t.TempDir())
	key, value := []byte("key"), []byte("value")
	if _, _, err := log.write(typeBytes, key, value, 42); err != nil {
		t.Fatal(err)
	}
	record, err := os.ReadFile(log.headFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		offset int
	}{
		{"checksum", 0},
		{"type", 4},
		{"expiration", 13},
		{"key", logHeaderSize},
		{"value", logHeaderSize + len(key)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			corrupt := bytes.Clone(record)
			corrupt[tc.offset] ^= 1
			if err := os.WriteFile(log.headFile.Name(), corrupt, 0600); err != nil {
				t.Fatal(err)
			}
			for _, buf := range [][]byte{nil, log.writeBuffer[:]} {
				if _, _, _, _, err := log.readPayload(0, 0, buf); err == nil || err.Error() != "crc mismatch" {
					t.Fatalf("corrupt record: got error %v, want crc mismatch", err)
				}
			}
		})
	}
}

func TestLogBufferedReads(t *testing.T) {
	log, _ := openRecoveryLog(t, t.TempDir())
	for _, size := range []int{0, 144, logWriteBufferSize - 1, logWriteBufferSize, logWriteBufferSize + 1, 64 << 10, 144} {
		t.Run(fmt.Sprintf("%dB", size), func(t *testing.T) {
			key := bytes.Repeat([]byte{'k'}, min(16, size))
			value := bytes.Repeat([]byte{'v'}, size-len(key))
			seg, off, err := log.write(typeBytes, key, value, 12345)
			if err != nil {
				t.Fatal(err)
			}
			for _, buf := range [][]byte{nil, log.writeBuffer[:0]} {
				gotKey, gotValue, _, gotExp, err := log.readPayload(seg, off, buf)
				if err != nil || !bytes.Equal(gotKey, key) || !bytes.Equal(gotValue, value) || gotExp != 12345 {
					t.Fatalf("read: key=%q value=%q exp=%d error=%v", gotKey, gotValue, gotExp, err)
				}
			}
		})
	}
}

// Leave existing records intact and reach the boundary without writing 64 MiB.
func padLog(t *testing.T, log *Log, size uint64) {
	t.Helper()
	if err := log.headFile.Truncate(int64(size)); err != nil {
		t.Fatal(err)
	}
	log.size = size
}

func TestLogSealingBoundary(t *testing.T) {
	for _, extra := range []uint64{0, logAlignment} {
		t.Run(fmt.Sprintf("extra-%d", extra), func(t *testing.T) {
			log, _ := openRecoveryLog(t, t.TempDir())
			key, value := []byte("boundary"), []byte("value spanning the boundary")
			offset := logSegmentSize - recordSize(len(key), len(value)) + extra
			padLog(t, log, offset)
			segment, off, err := log.write(typeBytes, key, value, 0)
			if err != nil {
				t.Fatal(err)
			}
			if segment != 0 || off != offset {
				t.Fatalf("boundary record address: got (%d, %d), want (0, %d)", segment, off, offset)
			}
			old := log.headFile
			newKey, newValue := []byte("new"), []byte("v")
			newSegment, newOff, err := log.write(typeBytes, newKey, newValue, 0)
			if err != nil {
				t.Fatal(err)
			}
			if newSegment != 1 || newOff != 0 {
				t.Fatalf("next record address: got (%d, %d), want (1, 0)", newSegment, newOff)
			}
			if _, err := old.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("sealed head was not closed: %v", err)
			}
			for _, record := range []struct {
				segment, offset uint64
				key, value      []byte
			}{
				{segment, off, key, value},
				{newSegment, newOff, newKey, newValue},
			} {
				k, v, _, _, err := log.readPayload(record.segment, record.offset, nil)
				if err != nil || !bytes.Equal(k, record.key) || !bytes.Equal(v, record.value) {
					t.Fatalf("record (%d, %d): key=%q value=%q error=%v", record.segment, record.offset, k, v, err)
				}
			}
			info, err := os.Stat(old.Name())
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() != logSegmentSize+int64(extra) {
				t.Fatalf("sealed file grew: got %d bytes", info.Size())
			}
		})
	}
}

func TestDBLogSealing(t *testing.T) {
	db := newInternalDB(t)
	s := &db.shards[0]
	shardKeys := keysForShard(db, s, "seal", 5)
	keys := make(map[string][]byte)
	for i, name := range []string{"keep", "update", "delete", "new", "newest"} {
		keys[name] = shardKeys[i]
	}
	set := func(key, value string) {
		t.Helper()
		if err := db.Set(keys[key], []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	get := func(key, want string) {
		t.Helper()
		var wantErr error
		if want == "" {
			wantErr = ErrNotFound
		}
		got, err := db.Get[[]byte](keys[key])
		if err != wantErr || string(got) != want {
			t.Fatalf("key %q: got %q, want %q, error %v", key, got, want, err)
		}
	}
	set("keep", "a longer value from segment zero")
	set("update", "original")
	set("delete", "remove me")
	padLog(t, s.log, logSegmentSize)
	set("new", "segment one")
	get("keep", "a longer value from segment zero")
	get("update", "original")
	get("new", "segment one")
	set("update", "middle")
	if err := db.Delete(keys["delete"]); err != nil {
		t.Fatal(err)
	}
	get("delete", "")
	padLog(t, s.log, logSegmentSize)
	set("update", "latest")
	set("newest", "segment two")
	get("keep", "a longer value from segment zero")
	get("new", "segment one")
	get("update", "latest")
	get("newest", "segment two")
	// A delete can also trigger rotation, including when its key is sealed.
	padLog(t, s.log, logSegmentSize)
	for _, key := range []string{"keep", "new", "update"} {
		if err := db.Delete(keys[key]); err != nil {
			t.Fatal(err)
		}
		get(key, "")
	}
	get("newest", "segment two")
	if s.log.headSegment != 3 {
		t.Fatalf("got head segment %d, want 3", s.log.headSegment)
	}
}

func TestDBLogSealingOpenFailure(t *testing.T) {
	for _, operation := range []string{"insert", "update", "delete"} {
		t.Run(operation, func(t *testing.T) {
			db := newInternalDB(t)
			key, value := []byte("existing"), []byte("original")
			s, _ := db.shard(key)
			newKey := keysForShard(db, s, "new", 1)[0]
			if err := db.Set(key, value); err != nil {
				t.Fatal(err)
			}
			padLog(t, s.log, logSegmentSize)
			head := s.log.headFile
			before := slices.Clone(s.index.slots)
			blocker := filepath.Join(s.log.path, "0000001.log")
			if err := os.Mkdir(blocker, 0700); err != nil {
				t.Fatal(err)
			}
			write := func() error {
				switch operation {
				case "insert":
					return db.Set(newKey, []byte("inserted"))
				case "update":
					return db.Set(key, []byte("updated"))
				default:
					return db.Delete(key)
				}
			}
			if err := write(); err == nil {
				t.Fatal("rotation unexpectedly succeeded")
			}
			if s.log.headFile != head || s.log.headSegment != 0 || s.log.size != logSegmentSize {
				t.Fatal("failed rotation changed the head")
			}
			if !slices.Equal(before, s.index.slots) || s.index.entryCount != 1 {
				t.Fatal("failed rotation changed the index")
			}
			info, err := head.Stat()
			if err != nil || info.Size() != logSegmentSize {
				t.Fatalf("failed rotation changed the file: stat=%v, error=%v", info, err)
			}
			got, err := db.Get[[]byte](key)
			if err != nil || !bytes.Equal(got, value) {
				t.Fatalf("existing value after failed rotation: got %q, error %v", got, err)
			}
			if err := os.Remove(blocker); err != nil {
				t.Fatal(err)
			}
			if err := write(); err != nil {
				t.Fatalf("retry after removing blocker: %v", err)
			}
			want := value
			var wantErr error
			switch operation {
			case "insert":
				key, want = newKey, []byte("inserted")
			case "update":
				want = []byte("updated")
			default:
				want = nil
				wantErr = ErrNotFound
			}
			got, err = db.Get[[]byte](key)
			if err != wantErr || !bytes.Equal(got, want) {
				t.Fatalf("value after retry: got %q, want %q, error %v", got, want, err)
			}
		})
	}
}

func TestDBLogSealingSyncFailure(t *testing.T) {
	db := newInternalDB(t)
	key := []byte("existing")
	s, _ := db.shard(key)
	newKey := keysForShard(db, s, "new", 1)[0]
	if err := db.Set(key, []byte("value")); err != nil {
		t.Fatal(err)
	}
	padLog(t, s.log, logSegmentSize)
	before := slices.Clone(s.index.slots)
	head := s.log.headFile
	if err := head.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(newKey, []byte("value")); err == nil {
		t.Fatal("sync on closed head unexpectedly succeeded")
	}
	if s.log.headFile != head || s.log.headSegment != 0 || s.log.size != logSegmentSize {
		t.Fatal("failed sync changed the head")
	}
	if !slices.Equal(before, s.index.slots) || s.index.entryCount != 1 {
		t.Fatal("failed sync changed the index")
	}
	var err error
	s.log.headFile, err = os.OpenFile(head.Name(), os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set(newKey, []byte("value")); err != nil {
		t.Fatalf("retry after reopening head: %v", err)
	}
	for _, key := range [][]byte{key, newKey} {
		got, err := db.Get[[]byte](key)
		if err != nil || string(got) != "value" {
			t.Fatalf("key %q after retry: got %q, error %v", key, got, err)
		}
	}
}

func TestLogSealedReadsCloseFiles(t *testing.T) {
	log, _ := openRecoveryLog(t, t.TempDir())
	if _, _, err := log.write(typeBytes, []byte("old"), []byte("value"), 0); err != nil {
		t.Fatal(err)
	}
	sealedPath := log.headFile.Name()
	padLog(t, log, logSegmentSize)
	if _, _, err := log.write(typeBytes, []byte("new"), []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	countOpen := func() int {
		t.Helper()
		fds, err := os.ReadDir("/proc/self/fd")
		if os.IsNotExist(err) {
			t.Skip("file descriptor inspection requires /proc/self/fd")
		}
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for _, fd := range fds {
			name, err := os.Readlink(filepath.Join("/proc/self/fd", fd.Name()))
			if err == nil && name == sealedPath {
				count++
			}
		}
		return count
	}
	for range 8 {
		if _, _, _, _, err := log.readPayload(0, 0, nil); err != nil {
			t.Fatal(err)
		}
		if got := countOpen(); got != 0 {
			t.Fatalf("successful read left %d sealed descriptors open", got)
		}
		if _, _, _, _, err := log.readPayload(0, logSegmentSize, nil); err == nil {
			t.Fatal("reading past sealed EOF unexpectedly succeeded")
		}
		if got := countOpen(); got != 0 {
			t.Fatalf("failed read left %d sealed descriptors open", got)
		}
	}
}

func TestIndexAlignedAddressLayout(t *testing.T) {
	// Pin the compressed address independently of the encoding constants,
	// including the final usable start offset and all 19 fingerprint bits.
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
	tag := fingerprint(((1 << 19) - 1) << 14)
	slot := logAddress(1048575, (1<<26)-16) | tag | metadataMask
	if slot != ^uint64(0) || addressOffset(slot) != (1<<26)-16 || addressSegment(slot) != 1048575 {
		t.Fatalf("address, fingerprint, and metadata fields overlap or leave gaps: %#x", slot)
	}
	if fingerprint(1<<(14+19)) != 0 {
		t.Fatal("fingerprint retained a bit outside the 19-bit budget")
	}
	idx := newInternalIndex(t)
	if len(idx.slots)*8*defaultShardCount != 8<<20 || indexBits+fingerprintBits != 33 {
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

func TestLogRejectsInvalidTypedRecords(t *testing.T) {
	cases := []struct {
		name       string
		ty         recordType
		value      []byte
		expiration int64
	}{
		{"zero-type", 0, nil, 0},
		{"unknown-type", 0xff, nil, 0},
		{"false-payload", typeBoolFalse, []byte{0}, 0},
		{"true-payload", typeBoolTrue, []byte{1}, 0},
		{"delete-payload", typeDelete, []byte{0}, 0},
		{"delete-expiration", typeDelete, nil, 1},
	}
	for _, tc := range typedCases() {
		if tc.payload == 0 || tc.logical == "[]byte" {
			continue
		}
		sizes := []int{tc.payload - 1, tc.payload + 1}
		if tc.payload > 1 {
			sizes = append(sizes, 0)
		}
		for _, size := range sizes {
			cases = append(cases, struct {
				name       string
				ty         recordType
				value      []byte
				expiration int64
			}{fmt.Sprintf("%s-size-%d", tc.name, size), tc.ty, make([]byte, size), 0})
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Include long keys to exercise the scalar reader's fallback path.
			for _, keySize := range []int{3, 1100} {
				t.Run(fmt.Sprintf("key=%d", keySize), func(t *testing.T) {
					key := bytes.Repeat([]byte{'k'}, keySize)
					data := recoveryRecord(tc.ty, key, tc.value, tc.expiration)
					log, _ := openRecoveryLog(t, t.TempDir())
					if err := os.WriteFile(log.headFile.Name(), data, 0600); err != nil {
						t.Fatal(err)
					}
					for _, buf := range [][]byte{nil, make([]byte, 4096)} {
						if _, _, _, _, err := log.readPayload(0, 0, buf); !errors.Is(err, ErrInvalidValue) {
							t.Fatalf("payload read accepted invalid record: %v", err)
						}
					}
					if _, _, _, err := log.readScalar(0, 0, key); !errors.Is(err, ErrInvalidValue) {
						t.Fatalf("scalar read accepted invalid record: %v", err)
					}
				})
			}
			for _, sealed := range []bool{false, true} {
				t.Run(fmt.Sprintf("recovery/sealed=%t", sealed), func(t *testing.T) {
					path := t.TempDir()
					data := append(recoveryRecord(typeBoolTrue, []byte("previous"), nil, 0),
						recoveryRecord(tc.ty, []byte("bad"), tc.value, tc.expiration)...)
					name := writeRecoverySegment(t, path, 0, data)
					if sealed {
						writeRecoverySegment(t, path, 1, nil)
					}
					log, err := newLog(path, newInternalIndex(t))
					if log != nil {
						log.headFile.Close()
					}
					if log != nil || !errors.Is(err, ErrInvalidValue) {
						t.Fatalf("recovery accepted invalid record: %v", err)
					}
					if got, err := os.ReadFile(name); err != nil || !bytes.Equal(got, data) {
						t.Fatalf("failed recovery changed records: %v", err)
					}
					if count := openFilesUnder(t, path); count != 0 {
						t.Fatalf("failed recovery leaked %d descriptors", count)
					}
				})
			}
		})
	}
}

func TestLogTypedRecordTags(t *testing.T) {
	// Pin the public disk format independently of the recordType constants.
	for _, tc := range []struct {
		ty    recordType
		code  byte
		value []byte
	}{
		{typeDelete, 0x01, nil},
		{typeBytes, 0x02, nil},
		{typeInt64, 0x03, make([]byte, 8)},
		{typeUint64, 0x04, make([]byte, 8)},
		{typeFloat64, 0x05, make([]byte, 8)},
		{typeBoolFalse, 0x06, nil},
		{typeBoolTrue, 0x07, nil},
		{typeString, 0x08, nil},
		{typeInt8, 0x09, make([]byte, 1)},
		{typeInt16, 0x0a, make([]byte, 2)},
		{typeInt32, 0x0b, make([]byte, 4)},
		{typeUint8, 0x0c, make([]byte, 1)},
		{typeUint16, 0x0d, make([]byte, 2)},
		{typeUint32, 0x0e, make([]byte, 4)},
		{typeFloat32, 0x0f, make([]byte, 4)},
		{typeInt, 0x10, make([]byte, 8)},
		{typeUint, 0x11, make([]byte, 8)},
		{typeDuration, 0x12, make([]byte, 8)},
	} {
		log, _ := openRecoveryLog(t, t.TempDir())
		if _, _, err := log.write(tc.ty, nil, tc.value, 0); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(log.headFile.Name())
		if err != nil || uint64(len(data)) != recordSize(0, len(tc.value)) || data[4] != tc.code {
			t.Fatalf("record tag %s: data=%x error=%v", tc.ty, data, err)
		}
	}
}

func TestLogRecoveryIncompleteTypedHead(t *testing.T) {
	for _, tc := range typedCases() {
		t.Run(tc.name, func(t *testing.T) {
			key := []byte("partial")
			complete := recoveryRecord(tc.ty, key, make([]byte, tc.payload), 0)
			for _, length := range []int{logHeaderSize - 1, len(complete) - 1} {
				path := t.TempDir()
				prefix := recoveryRecord(typeBoolFalse, []byte("keep"), nil, 0)
				name := writeRecoverySegment(t, path, 0, append(bytes.Clone(prefix), complete[:length]...))
				log, _ := openRecoveryLog(t, path)
				got, err := os.ReadFile(name)
				if err != nil || !bytes.Equal(got, prefix) || log.size != uint64(len(prefix)) {
					t.Fatalf("incomplete typed record not repaired: size=%d error=%v", log.size, err)
				}
			}
		})
	}
}

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
	if addr := idx.slots[pos] & addressMask; addr != logAddress(0, size) {
		t.Fatalf("retry address=%d, want %d", addr, size)
	}
	if got, _, _, _, err := log.readPayload(0, 0, nil); err != nil || !bytes.Equal(got, key) {
		t.Fatalf("original record damaged: key=%q error=%v", got, err)
	}
}
