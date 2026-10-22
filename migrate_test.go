package dot

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/cespare/xxhash/v2"
)

func TestMetaBinaryFormat(t *testing.T) {
	// Pin signature, offsets, byte order, reserved bytes, and CRC independently
	// of the decoder. This fixture describes format 1 with 64 shards.
	want, err := hex.DecodeString("444f544d45544144010000004000141a0400000000000000000000000000000000000000000000000000000000000000000000000000000000000000858d6229")
	if err != nil {
		t.Fatal(err)
	}
	got := encodeMeta(currentMeta(64))
	if !bytes.Equal(got[:], want) {
		t.Fatalf("binary META = %x, want %x", got, want)
	}
	meta, err := decodeMeta(want)
	if err != nil || meta != currentMeta(64) {
		t.Fatalf("decoded META = %+v, error = %v", meta, err)
	}
}

func TestWithShardCount(t *testing.T) {
	for _, count := range []int{1, 4, 128} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			db := newInternalDB(t, WithShardCount(count))
			if len(db.shards) != count || cap(db.compactionQueue) != count {
				t.Fatalf("shards=%d queue=%d, want %d", len(db.shards), cap(db.compactionQueue), count)
			}
			var keys [][]byte
			for i := range db.shards {
				key := keysForShard(db, &db.shards[i], "custom-shards", 1)[0]
				if s, hash := db.shard(key); hash != xxhash.Sum64(key) || s != &db.shards[i] {
					t.Fatalf("incorrect shard routing for %q", key)
				}
				if err := db.Set(key, uint64(i)); err != nil {
					t.Fatal(err)
				}
				if err := db.Update(key, func(v uint64, exists bool) (uint64, error) {
					if !exists {
						t.Fatal("Update did not find key")
					}
					return v + 1, nil
				}); err != nil {
					t.Fatal(err)
				}
				keys = append(keys, key)
			}
			if err := db.compact(context.Background()); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(db.path, metaName))
			if err != nil {
				t.Fatal(err)
			}
			meta, err := decodeMeta(data)
			if err != nil {
				t.Fatal(err)
			}
			if meta != (metaData{DiskFormatVersion: 1, ShardCount: count, SegmentIDBits: 20, SegmentSizeBits: 26, RecordAlignmentBits: 4}) {
				t.Fatalf("unexpected persisted layout: %+v", meta)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			for _, opts := range [][]Option{nil, {WithShardCount(count)}} {
				reopened, err := Open(db.path, opts...)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := reopened.Close(); err != nil {
						t.Error(err)
					}
				})
				if len(reopened.shards) != count {
					t.Fatalf("reopen loaded %d shards, want %d", len(reopened.shards), count)
				}
				for i, key := range keys {
					if v, err := reopened.Get[uint64](key); err != nil || v != uint64(i+1) {
						t.Fatalf("recovered key %d: value=%d error=%v", i, v, err)
					}
				}
				scanned := 0
				for range reopened.Scan() {
					scanned++
				}
				if scanned != count {
					t.Fatalf("Scan returned %d keys, want %d", scanned, count)
				}
				if err := reopened.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := rejectedOpen(t, db.path, WithShardCount(2)); !strings.Contains(err.Error(), fmt.Sprintf("migration failed: changing shard count from %d to 2", count)) {
				t.Fatalf("conflicting count: %v", err)
			}
		})
	}
}

func TestWithShardCountValidation(t *testing.T) {
	for _, count := range []int{-1, 0, 3, 63, 1025, 1 << 20} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			if db, err := Open(path, WithShardCount(count)); db != nil || err == nil {
				if db != nil {
					db.Close()
				}
				t.Fatalf("count %d: db=%v error=%v", count, db, err)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid option touched database directory: %v", err)
			}
		})
	}
	// Functional options follow the existing last-value-wins convention.
	db := newInternalDB(t, WithShardCount(0), WithShardCount(4))
	if err := db.Set("key", "value", WithShardCount(0)); err != nil {
		t.Fatal(err)
	}
	if len(db.shards) != 4 {
		t.Fatal("key operation changed the layout")
	}
}

// rejectedOpen verifies that a rejected migration neither repairs a partial
// log tail nor creates/removes any database data, and that it releases LOCK.
func rejectedOpen(t *testing.T, path string, opts ...Option) error {
	t.Helper()
	before := databaseContents(t, path)
	db, err := Open(path, opts...)
	if db != nil || err == nil {
		if db != nil {
			db.Close()
		}
		t.Fatalf("expected Open rejection: db=%v error=%v", db, err)
	}
	if after := databaseContents(t, path); !reflect.DeepEqual(before, after) {
		t.Fatal("rejected Open modified database contents")
	}
	lock, lockErr := acquireLock(path)
	if lockErr != nil {
		t.Fatalf("rejected Open retained database lock: %v", lockErr)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	return err
}

func databaseContents(t *testing.T, path string) map[string]string {
	t.Helper()
	contents := make(map[string]string)
	err := filepath.WalkDir(path, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(path, name)
		if err != nil || relative == lockName {
			return err
		}
		if entry.IsDir() {
			contents[relative] = "directory"
			return nil
		}
		data, err := os.ReadFile(name)
		contents[relative] = "file:" + string(data)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return contents
}

func TestMigrationRejectsUnsupportedLayouts(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reason string
		change func(*metaData)
	}{
		{"version", "unsupported disk format version", func(m *metaData) { m.DiskFormatVersion++ }},
		{"segment-id-bits", "unsupported segment layout", func(m *metaData) { m.SegmentIDBits-- }},
		{"segment-size-bits", "unsupported segment layout", func(m *metaData) { m.SegmentSizeBits++ }},
		{"alignment-bits", "unsupported segment layout", func(m *metaData) { m.RecordAlignmentBits-- }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newInternalDB(t, WithShardCount(1))
			if err := db.Set("key", "value"); err != nil {
				t.Fatal(err)
			}
			logName := db.shards[0].log.headFile.Name()
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			f, err := os.OpenFile(logName, os.O_WRONLY|os.O_APPEND, 0600)
			if err != nil {
				t.Fatal(err)
			}
			_, writeErr := f.Write([]byte("partial"))
			if err := errors.Join(writeErr, f.Close()); err != nil {
				t.Fatal(err)
			}
			meta := currentMeta(1)
			tc.change(&meta)
			data := encodeMeta(meta)
			if err := os.WriteFile(filepath.Join(db.path, metaName), data[:], 0600); err != nil {
				t.Fatal(err)
			}
			if err := rejectedOpen(t, db.path); !strings.Contains(err.Error(), "migration failed: "+tc.reason) {
				t.Fatalf("unsupported layout: %v", err)
			}
		})
	}
}

func TestMigrationRejectsCorruptMetadataBeforeRecovery(t *testing.T) {
	db := newInternalDB(t, WithShardCount(1))
	if err := db.Set("key", "value"); err != nil {
		t.Fatal(err)
	}
	logName := db.shards[0].log.headFile.Name()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(logName, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := f.Write([]byte("partial"))
	if err := errors.Join(writeErr, f.Close()); err != nil {
		t.Fatal(err)
	}
	valid := encodeMeta(currentMeta(1))
	for offset := range valid {
		t.Run(fmt.Sprintf("byte=%d", offset), func(t *testing.T) {
			corrupt := valid
			corrupt[offset] ^= 1
			if err := os.WriteFile(filepath.Join(db.path, metaName), corrupt[:], 0600); err != nil {
				t.Fatal(err)
			}
			if err := rejectedOpen(t, db.path); !strings.Contains(err.Error(), "checksum mismatch") {
				t.Fatalf("corrupt META byte %d: %v", offset, err)
			}
		})
	}
}

func TestMigrationRejectsInvalidMetadata(t *testing.T) {
	valid := encodeMeta(currentMeta(1))
	changed := func(offset int, value byte) []byte {
		data := valid
		data[offset] = value
		binary.LittleEndian.PutUint32(data[60:], crc32.ChecksumIEEE(data[:60]))
		return data[:]
	}
	zeroShards := currentMeta(0)
	invalidShards := currentMeta(3)
	tooManyShards := currentMeta(2048)
	missingFields := metaData{DiskFormatVersion: 1, ShardCount: 1}
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"zeros", make([]byte, 64)},
		{"json", []byte(`{"version":1,"shardCount":1,"segmentIDBits":20,"segmentSizeBits":26,"recordAlignmentBits":4}`)},
		{"zero-shards", metaBytes(zeroShards)},
		{"invalid-shards", metaBytes(invalidShards)},
		{"too-many-shards", metaBytes(tooManyShards)},
		{"missing-fields", metaBytes(missingFields)},
		{"signature", changed(0, 'X')},
		{"reserved", changed(17, 1)},
		{"truncated", valid[:63]},
		{"trailing-data", append(bytes.Clone(valid[:]), 0)},
		{"oversized", append(bytes.Clone(valid[:]), make([]byte, 4096)...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := t.TempDir()
			if err := os.WriteFile(filepath.Join(path, metaName), tc.data, 0600); err != nil {
				t.Fatal(err)
			}
			rejectedOpen(t, path)
		})
	}
}

func metaBytes(meta metaData) []byte {
	data := encodeMeta(meta)
	return data[:]
}

func TestMigrationRejectsMissingMetadata(t *testing.T) {
	db := newInternalDB(t, WithShardCount(4))
	if err := db.Set("key", "value"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(db.path, metaName)); err != nil {
		t.Fatal(err)
	}
	for _, opts := range [][]Option{nil, {WithShardCount(4)}} {
		if err := rejectedOpen(t, db.path, opts...); !strings.Contains(err.Error(), "migration failed: META is missing") {
			t.Fatalf("missing metadata: %v", err)
		}
	}
}

func TestMigrationRejectsUnexpectedShardDirectory(t *testing.T) {
	db := newInternalDB(t, WithShardCount(1))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(db.path, "01"), 0700); err != nil {
		t.Fatal(err)
	}
	rejectedOpen(t, db.path)
}

func TestMigrationResumesMetadataInitialization(t *testing.T) {
	path := t.TempDir()
	if err := os.WriteFile(filepath.Join(path, metaTempName), []byte("DOTMETAD"), 0600); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path, WithShardCount(1))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := os.Stat(filepath.Join(path, metaTempName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unpublished metadata remains: %v", err)
	}
	if info, err := os.Stat(filepath.Join(path, metaName)); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("metadata permissions: info=%v error=%v", info, err)
	}
	if err := db.Set("key", "value"); err != nil {
		t.Fatal(err)
	}
}
