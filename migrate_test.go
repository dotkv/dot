package dot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/cespare/xxhash/v2"
)

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
			var meta metaData
			if err := json.Unmarshal(data, &meta); err != nil {
				t.Fatal(err)
			}
			if meta != (metaData{Version: 1, ShardCount: count, SegmentIDBits: 20, SegmentSizeBits: 26, RecordAlignmentBits: 4}) {
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
		{"version", "unsupported meta version", func(m *metaData) { m.Version++ }},
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
			data, err := json.Marshal(meta)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(db.path, metaName), data, 0600); err != nil {
				t.Fatal(err)
			}
			if err := rejectedOpen(t, db.path); !strings.Contains(err.Error(), "migration failed: "+tc.reason) {
				t.Fatalf("unsupported layout: %v", err)
			}
		})
	}
}

func TestMigrationRejectsInvalidMetadata(t *testing.T) {
	valid := `{"version":1,"shardCount":1,"segmentIDBits":20,"segmentSizeBits":26,"recordAlignmentBits":4}`
	for _, tc := range []struct{ name, data string }{
		{"empty", ""},
		{"null", "null"},
		{"missing-fields", `{"version":1,"shardCount":1}`},
		{"zero-shards", `{"version":1,"shardCount":0}`},
		{"invalid-shards", `{"version":1,"shardCount":3}`},
		{"too-many-shards", `{"version":1,"shardCount":2048}`},
		{"truncated", valid[:len(valid)-1]},
		{"trailing-object", valid + "{}"},
		{"trailing-junk", valid + "junk"},
		{"unknown-field", valid[:len(valid)-1] + `,"unknown":1}`},
		{"oversized", valid + string(bytes.Repeat([]byte(" "), 4096))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := t.TempDir()
			if err := os.WriteFile(filepath.Join(path, metaName), []byte(tc.data), 0600); err != nil {
				t.Fatal(err)
			}
			rejectedOpen(t, path)
		})
	}
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
	if err := os.WriteFile(filepath.Join(path, metaTempName), []byte(`{"version":`), 0600); err != nil {
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
