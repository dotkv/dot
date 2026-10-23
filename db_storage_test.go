package dot

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newInternalDB(t testing.TB, opts ...Option) *DB {
	t.Helper()
	// Storage fixtures inspect exact file state and inject synchronous write
	// failures. Buffered behavior and the public default have dedicated tests.
	opts = append([]Option{WithDurability(Write)}, opts...)
	db, err := Open(t.TempDir(), opts...)
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

func keysForShard(db *DB, s *Shard, prefix string, count int) [][]byte {
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
	// The LOCK file guards single-process access; only directories are shards.
	var shardDirs []os.DirEntry
	for _, dir := range dirs {
		if dir.IsDir() {
			shardDirs = append(shardDirs, dir)
		}
	}
	if len(shardDirs) != defaultShardCount {
		t.Fatalf("got %d shard directories, want %d", len(shardDirs), defaultShardCount)
	}
	for i, dir := range shardDirs {
		if dir.Name() != fmt.Sprintf("%02d", i) {
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
			k, v, _, _, err := s.log.readPayload(0, off, nil)
			if err != nil || !bytes.Equal(k, key) || !bytes.Equal(v, value) {
				t.Fatalf("shard %d log: key=%q value=%q error=%v", i, k, v, err)
			}
		}
		if s.index.entryCount != len(keys) {
			t.Fatalf("shard %d: got entryCount %d, want %d", i, s.index.entryCount, len(keys))
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

func TestDBScanErrors(t *testing.T) {
	for _, mode := range []Durability{Buffer, Write} {
		for _, tc := range []struct {
			name string
			want error
		}{
			{"truncated", io.ErrUnexpectedEOF},
			{"checksum", nil},
			{"invalid-type", ErrInvalidValue},
			{"deleted-record", ErrInvalidValue},
		} {
			t.Run(fmt.Sprintf("durability=%d/%s", mode, tc.name), func(t *testing.T) {
				db := newInternalDB(t, WithShardCount(4), WithDurability(mode))
				stopTestMaintenance(db)
				keys := make([][]byte, len(db.shards))
				for i := range db.shards {
					keys[i] = keysForShard(db, &db.shards[i], "scan-error", 1)[0]
					if err := db.Set(keys[i], []byte("value")); err != nil {
						t.Fatal(err)
					}
				}
				// Fail after a successful shard, with later shards still to scan.
				log := db.shards[1].log
				record := recoveryRecord(typeBytes, keys[1], []byte("value"), 0)
				switch tc.name {
				case "truncated":
					record = nil
				case "checksum":
					record[0] ^= 0xff
				case "invalid-type":
					record[4] = 0xff
				case "deleted-record":
					record = recoveryRecord(typeDelete, keys[1], nil, 0)
				}
				if mode == Buffer {
					log.pending = record
				} else {
					if err := os.WriteFile(log.headFile.Name(), record, 0600); err != nil {
						t.Fatal(err)
					}
				}
				m := db.acquireOperationMetrics(true)
				defer db.releaseOperationMetrics(m, true)
				entries, failures := 0, 0
				for entry, err := range db.Scan() {
					if err != nil {
						failures++
						if entry.Key != nil || entry.Value != nil || (tc.want != nil && !errors.Is(err, tc.want)) {
							t.Fatalf("scan failure: entry=%+v error=%v, want %v", entry, err, tc.want)
						}
						continue // Accept the error yield to verify that Scan still stops.
					}
					if failures != 0 || !bytes.Equal(entry.Key, keys[0]) {
						t.Fatalf("scan continued past failed shard: %+v", entry)
					}
					entries++
				}
				if entries != 1 || failures != 1 {
					t.Fatalf("scan yielded %d entries and %d errors, want one of each", entries, failures)
				}
				metrics, _ := m.snapshot(time.Now(), true)
				if metrics.Scan.Count != 1 || metrics.Scan.LatencySamples != 1 || metrics.Scan.MeanNS == nil || metrics.Scan.P99NS == nil {
					t.Fatalf("failed scan was not recorded: %+v", metrics.Scan)
				}
				// A failed scan must also release its read lock.
				if !db.shards[1].lock.TryLock() {
					t.Fatal("failed scan retained the shard lock")
				}
				db.shards[1].lock.Unlock()
			})
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

func TestDBShardLogRotation(t *testing.T) {
	db := newInternalDB(t)
	var keys [defaultShardCount][]byte
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
		// The LOCK file guards single-process access; only log files count
		// toward the descriptor budget.
		if err == nil && strings.HasPrefix(name, path+string(os.PathSeparator)) &&
			name != filepath.Join(path, lockName) {
			count++
		}
	}
	return count
}

func TestDBShardFileBudget(t *testing.T) {
	db := newInternalDB(t)
	if got := openFilesUnder(t, db.path); got != defaultShardCount {
		t.Fatalf("empty database: got %d open log files, want %d", got, defaultShardCount)
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
	if got := openFilesUnder(t, db.path); got != defaultShardCount {
		t.Fatalf("after rotation and sealed reads: got %d open log files, want %d", got, defaultShardCount)
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
	blocker := filepath.Join(path, fmt.Sprintf("%02d", defaultShardCount/2))
	if err := os.WriteFile(blocker, []byte("preserve me"), 0600); err != nil {
		t.Fatal(err)
	}
	if db, err := Open(path); err == nil || db != nil {
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
