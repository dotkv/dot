package dot

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newInternalDB(t testing.TB) *DB {
	t.Helper()
	db, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Destroy(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func keysForShard(db *DB, s *shard, prefix string, count int) [][]byte {
	keys := make([][]byte, 0, count)
	for i := 0; len(keys) < count; i++ {
		key := fmt.Appendf(nil, "%s-%d", prefix, i)
		if candidate, _ := db.shard(key); candidate == s {
			keys = append(keys, key)
		}
	}
	return keys
}

func TestDBShards(t *testing.T) {
	db := newInternalDB(t)
	dirs, err := os.ReadDir(db.path)
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) != shardCount {
		t.Fatalf("got %d shard directories, want %d", len(dirs), shardCount)
	}
	for i, dir := range dirs {
		if !dir.IsDir() || dir.Name() != fmt.Sprintf("%02d", i) {
			t.Fatalf("unexpected shard directory %q", dir.Name())
		}
		s := &db.shards[i]
		keys := keysForShard(db, s, "key", 2)
		for j, key := range keys {
			value := fmt.Appendf(nil, "shard-%d-value-%d", i, j)
			off := s.log.size
			if err := db.Set(key, value); err != nil {
				t.Fatal(err)
			}
			// Every shard has its own segment zero, even at identical offsets.
			k, v, err := s.log.readPayload(0, off, nil)
			if err != nil || !bytes.Equal(k, key) || !bytes.Equal(v, value) {
				t.Fatalf("shard %d log: key=%q value=%q error=%v", i, k, v, err)
			}
		}
		if s.index.population != len(keys) {
			t.Fatalf("shard %d: got population %d, want %d", i, s.index.population, len(keys))
		}
		if got := s.log.headFile.Name(); got != filepath.Join(db.path, dir.Name(), "0000000.log") {
			t.Fatalf("shard %d: unexpected log path %q", i, got)
		}
		if err := db.Set(keys[0], []byte("updated")); err != nil {
			t.Fatal(err)
		}
		if err := db.Delete(keys[1]); err != nil {
			t.Fatal(err)
		}
		for j, key := range keys {
			var want []byte
			wantErr := ErrNotFound
			if j == 0 {
				want = []byte("updated")
				wantErr = nil
			}
			if got, err := db.Get[[]byte](key); err != wantErr || !bytes.Equal(got, want) {
				t.Fatalf("shard %d key %q: got %q, want %q, error %v", i, key, got, want, err)
			}
		}
	}
}

func TestDBShardRouting(t *testing.T) {
	db := newInternalDB(t)
	other := newInternalDB(t)
	for _, db := range []*DB{db, other} {
		// Pin the full hash and shard number to protect the persistent layout
		// across DB instances, processes, architectures, and dependency updates.
		for _, tc := range []struct {
			key   []byte
			hash  uint64
			shard int
		}{
			{nil, 0xef46db3751d8e999, 59},
			{[]byte{}, 0xef46db3751d8e999, 59},
			{[]byte("a"), 0xd24ec4f1a98c6e5b, 52},
			{[]byte("as"), 0x1c330fb2d66be179, 7},
			{[]byte("asd"), 0x631c37ce72a97393, 24},
			{[]byte("asdf"), 0x415872f599cea71e, 16},
			{[]byte("Call me Ishmael. Some years ago--never mind how long precisely-"), 0x02a2e85470d6fd96, 0},
		} {
			s, hash := db.shard(tc.key)
			if hash != tc.hash || s != &db.shards[tc.shard] {
				t.Fatalf("key %q: hash=%#x, want hash=%#x and shard=%d", tc.key, hash, tc.hash, tc.shard)
			}
			if again, againHash := db.shard(bytes.Clone(tc.key)); again != s || againHash != hash {
				t.Fatalf("key %q: unstable shard routing", tc.key)
			}
			if err := db.Set(tc.key, []byte("value")); err != nil {
				t.Fatal(err)
			}
			if got, err := db.Get[[]byte](bytes.Clone(tc.key)); err != nil || string(got) != "value" {
				t.Fatalf("key %q: got %q, error %v", tc.key, got, err)
			}
		}
	}
}

func TestDBGetValueOwnership(t *testing.T) {
	for _, size := range []int{0, 128, logWriteBufferSize, 64 << 10} {
		t.Run(fmt.Sprintf("%dB", size), func(t *testing.T) {
			db := newInternalDB(t)
			key := []byte("key")
			value := bytes.Repeat([]byte{'v'}, size)
			if err := db.Set(key, value); err != nil {
				t.Fatal(err)
			}
			first, err := db.Get[[]byte](key)
			if err != nil || first == nil || !bytes.Equal(first, value) {
				t.Fatalf("first get: value=%q error=%v", first, err)
			}
			retained, err := db.Get[[]byte](key)
			if err != nil || !bytes.Equal(retained, value) {
				t.Fatalf("second get: value=%q error=%v", retained, err)
			}
			if len(first) > 0 {
				first[0] ^= 1
				if !bytes.Equal(retained, value) {
					t.Fatal("Get results share storage")
				}
			}
			updated := bytes.Repeat([]byte{'u'}, 128)
			if err := db.Set(key, updated); err != nil {
				t.Fatal(err)
			}
			if got, err := db.Get[[]byte](key); err != nil || !bytes.Equal(got, updated) {
				t.Fatalf("updated get: value=%q error=%v", got, err)
			}
			if err := db.Delete(key); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(retained, value) {
				t.Fatal("later operations changed a retained Get result")
			}
			if got, err := db.Get[[]byte](key); err != ErrNotFound || got != nil {
				t.Fatalf("deleted key: value=%q error=%v", got, err)
			}
		})
	}
}

func TestDBShardLogRotation(t *testing.T) {
	db := newInternalDB(t)
	var keys [shardCount][]byte
	for i := range keys {
		keys[i] = keysForShard(db, &db.shards[i], "rotation", 1)[0]
		if err := db.Set(keys[i], []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	padLog(t, db.shards[0].log, logSegmentSize)
	if err := db.Set(keys[0], []byte("updated")); err != nil {
		t.Fatal(err)
	}
	for i, key := range keys {
		var wantSegment uint64
		want := "value"
		if i == 0 {
			wantSegment, want = 1, "updated"
		}
		if got := db.shards[i].log.headSegment; got != wantSegment {
			t.Fatalf("shard %d: head segment %d, want %d", i, got, wantSegment)
		}
		if got, err := db.Get[[]byte](key); err != nil || string(got) != want {
			t.Fatalf("shard %d: got %q, want %q, error %v", i, got, want, err)
		}
	}
}

func TestDBShardsOperateIndependently(t *testing.T) {
	db := newInternalDB(t)
	locked := &db.shards[0]
	key := keysForShard(db, &db.shards[1], "other-shard", 1)[0]
	locked.lock.Lock()
	defer locked.lock.Unlock()
	done := make(chan error, 1)
	go func() {
		if err := db.Set(key, []byte("value")); err != nil {
			done <- err
			return
		}
		if got, err := db.Get[[]byte](key); err != nil || string(got) != "value" {
			done <- fmt.Errorf("get: value=%q error=%v", got, err)
			return
		}
		done <- db.Delete(key)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("operations on another shard blocked behind the locked shard")
	}
}

func openFilesUnder(t *testing.T, path string) int {
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
		if err == nil && strings.HasPrefix(name, path+string(os.PathSeparator)) {
			count++
		}
	}
	return count
}

func TestDBShardFileBudget(t *testing.T) {
	db := newInternalDB(t)
	if got := openFilesUnder(t, db.path); got != shardCount {
		t.Fatalf("empty database: got %d open log files, want %d", got, shardCount)
	}
	for i := range db.shards {
		s := &db.shards[i]
		keys := keysForShard(db, s, "fd-budget", 2)
		if err := db.Set(keys[0], []byte("sealed")); err != nil {
			t.Fatal(err)
		}
		padLog(t, s.log, logSegmentSize)
		if err := db.Set(keys[1], []byte("head")); err != nil {
			t.Fatal(err)
		}
		if got, err := db.Get[[]byte](keys[0]); err != nil || string(got) != "sealed" {
			t.Fatalf("shard %d sealed read: got %q, error %v", i, got, err)
		}
	}
	if got := openFilesUnder(t, db.path); got != shardCount {
		t.Fatalf("after rotation and sealed reads: got %d open log files, want %d", got, shardCount)
	}
	if err := db.Destroy(); err != nil {
		t.Fatal(err)
	}
	if got := openFilesUnder(t, db.path); got != 0 {
		t.Fatalf("destroyed database: %d log files still open", got)
	}
}

func TestDBShardOpenFailureClosesFiles(t *testing.T) {
	path := t.TempDir()
	blocker := filepath.Join(path, fmt.Sprintf("%02d", shardCount/2))
	if err := os.WriteFile(blocker, []byte("preserve me"), 0600); err != nil {
		t.Fatal(err)
	}
	if db, err := New(path); err == nil || db != nil {
		if db != nil {
			db.Destroy()
		}
		t.Fatalf("opening blocked shard: db=%v error=%v", db, err)
	}
	if got := openFilesUnder(t, path); got != 0 {
		t.Fatalf("failed open leaked %d file descriptors", got)
	}
	if got, err := os.ReadFile(blocker); err != nil || string(got) != "preserve me" {
		t.Fatalf("failed open changed existing file: data=%q error=%v", got, err)
	}
}

func TestDBDestroyShards(t *testing.T) {
	db := newInternalDB(t)
	for i := range db.shards {
		key := keysForShard(db, &db.shards[i], "key", 1)[0]
		if err := db.Set(key, []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Destroy(); err != nil {
		t.Fatal(err)
	}
	for i := range db.shards {
		if idx := db.shards[i].index; idx.slots != nil || idx.memory.current != nil || idx.memory.previous != nil {
			t.Fatalf("shard %d: index storage still owned after destroy", i)
		}
		if _, err := db.shards[i].log.headFile.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("shard %d: log still open after destroy: %v", i, err)
		}
		key := keysForShard(db, &db.shards[i], "key", 1)[0]
		_, getErr := db.Get[[]byte](key)
		for _, err := range []error{getErr, db.Set(key, []byte("new")), db.Delete(key)} {
			if !errors.Is(err, os.ErrClosed) {
				t.Fatalf("shard %d: operation after destroy returned %v", i, err)
			}
		}
	}
	if _, err := os.Stat(db.path); !os.IsNotExist(err) {
		t.Fatalf("database directory still exists: %v", err)
	}
}

func TestDBClosePreservesFiles(t *testing.T) {
	db := newInternalDB(t)
	key, value := []byte("key"), []byte("value")
	if err := db.Set(key, value); err != nil {
		t.Fatal(err)
	}
	s, _ := db.shard(key)
	name := s.log.headFile.Name()
	before, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	after, err := os.ReadFile(name)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("Close changed the log: error=%v", err)
	}
	if got := openFilesUnder(t, db.path); got != 0 {
		t.Fatalf("closed database: %d log files still open", got)
	}
	for i := range db.shards {
		idx := db.shards[i].index
		if idx.slots != nil || idx.memory.current != nil || idx.memory.previous != nil {
			t.Fatalf("shard %d: index storage still owned after close", i)
		}
	}
	_, getErr := db.Get[[]byte](key)
	for _, err := range []error{getErr, db.Set(key, value), db.Delete(key)} {
		if !errors.Is(err, os.ErrClosed) {
			t.Fatalf("operation after close: got %v, want os.ErrClosed", err)
		}
	}
	if err := db.Destroy(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(db.path); !os.IsNotExist(err) {
		t.Fatalf("destroy after close left the database directory: %v", err)
	}
}
