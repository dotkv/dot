package dot

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestLogRotationWraps(t *testing.T) {
	path := t.TempDir()
	writeRecoverySegment(t, path, maxSegment, nil)
	log, _ := openRecoveryLog(t, path)
	padLog(t, log, logSegmentSize)
	seg, off, err := log.write('s', []byte("key"), []byte("new"), 0)
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
		ty         byte
		key, value []byte
		expiration int64
		offset     uint64
	}{
		{ty: 's'},
		{ty: 's', key: key, value: bytes.Repeat([]byte{'a'}, logWriteBufferSize-logHeaderSize-len(key)-1), expiration: 123456789},
		{ty: 's', key: key, value: bytes.Repeat([]byte{'b'}, logWriteBufferSize-logHeaderSize-len(key))},
		{ty: 's', key: key, value: bytes.Repeat([]byte{'c'}, logWriteBufferSize-logHeaderSize-len(key)+1), expiration: -42},
		{ty: 's', key: bytes.Repeat([]byte{'k'}, 2*logWriteBufferSize), value: []byte("large key")},
		{ty: 's', key: key, value: bytes.Repeat([]byte{'v'}, 16*logWriteBufferSize)},
		{ty: 's', key: key, value: []byte("small again"), expiration: 987654321},
		{ty: 'd', key: key},
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
		size += uint64(logHeaderSize + len(r.key) + len(r.value))
	}
	data, err := os.ReadFile(log.headFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	if log.size != size || uint64(len(data)) != size {
		t.Fatalf("log size: counter %d, file %d, want %d", log.size, len(data), size)
	}
	for i, r := range records {
		k, v, exp, err := log.readPayload(0, r.offset, nil)
		if err != nil || !bytes.Equal(k, r.key) || !bytes.Equal(v, r.value) || exp != r.expiration || data[r.offset+4] != r.ty {
			t.Fatalf("record %d: key/value/expiration/type mismatch or read error: %v", i, err)
		}
	}
}

func TestLogReadRejectsCorruption(t *testing.T) {
	log, _ := openRecoveryLog(t, t.TempDir())
	key, value := []byte("key"), []byte("value")
	if _, _, err := log.write('s', key, value, 42); err != nil {
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
				if _, _, _, err := log.readPayload(0, 0, buf); err == nil || err.Error() != "crc mismatch" {
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
			seg, off, err := log.write('s', key, value, 12345)
			if err != nil {
				t.Fatal(err)
			}
			for _, buf := range [][]byte{nil, log.writeBuffer[:0]} {
				gotKey, gotValue, gotExp, err := log.readPayload(seg, off, buf)
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
	for _, extra := range []uint64{0, 1} {
		t.Run(fmt.Sprintf("extra-%d", extra), func(t *testing.T) {
			log, _ := openRecoveryLog(t, t.TempDir())
			key, value := []byte("boundary"), []byte("value spanning the boundary")
			offset := logSegmentSize - uint64(logHeaderSize+len(key)+len(value)) + extra
			padLog(t, log, offset)
			segment, off, err := log.write('s', key, value, 0)
			if err != nil {
				t.Fatal(err)
			}
			if segment != 0 || off != offset {
				t.Fatalf("boundary record address: got (%d, %d), want (0, %d)", segment, off, offset)
			}
			old := log.headFile
			newKey, newValue := []byte("new"), []byte("v")
			newSegment, newOff, err := log.write('s', newKey, newValue, 0)
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
				k, v, _, err := log.readPayload(record.segment, record.offset, nil)
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
	if _, _, err := log.write('s', []byte("old"), []byte("value"), 0); err != nil {
		t.Fatal(err)
	}
	sealedPath := log.headFile.Name()
	padLog(t, log, logSegmentSize)
	if _, _, err := log.write('s', []byte("new"), []byte("v"), 0); err != nil {
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
		if _, _, _, err := log.readPayload(0, 0, nil); err != nil {
			t.Fatal(err)
		}
		if got := countOpen(); got != 0 {
			t.Fatalf("successful read left %d sealed descriptors open", got)
		}
		if _, _, _, err := log.readPayload(0, logSegmentSize, nil); err == nil {
			t.Fatal("reading past sealed EOF unexpectedly succeeded")
		}
		if got := countOpen(); got != 0 {
			t.Fatalf("failed read left %d sealed descriptors open", got)
		}
	}
}
