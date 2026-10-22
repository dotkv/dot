package dot

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Recover shards directly for fixtures that inspect exact file and index state.
// Open's background scheduling is exercised by the compaction recovery tests.
func openRecoveryFixture(t *testing.T, path string) *DB {
	t.Helper()
	db := &DB{path: path, backgroundCancel: func() {}}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	for i := range db.shards {
		s := &db.shards[i]
		var err error
		s.index, err = newIndex()
		if err != nil {
			t.Fatal(err)
		}
		s.log, err = newLog(filepath.Join(path, fmt.Sprintf("%02d", i)), s.index)
		if err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func reopenDB(t *testing.T, db *DB) *DB {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Recovery fixtures inspect files and index accounting directly. Dedicated
	// scheduler tests cover automatic cleanup after Open.
	return openRecoveryFixture(t, db.path)
}

func TestDBRecovery(t *testing.T) {
	db := newInternalDB(t)
	retained := make([][]byte, shardCount)
	for i := range db.shards {
		retained[i] = keysForShard(db, &db.shards[i], "retained", 1)[0]
		if err := db.Set(retained[i], fmt.Appendf(nil, "shard-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	s := &db.shards[0]
	keys := keysForShard(db, s, "recovery", 8)
	for _, key := range keys {
		if err := db.Set(key, []byte("original")); err != nil {
			t.Fatal(err)
		}
	}
	// Use short sealed segments to exercise replay across files without padding
	// them with bytes that would not be valid records.
	if err := s.log.openHead(1); err != nil {
		t.Fatal(err)
	}
	past, future := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	if err := db.Set(keys[0], []byte("updated")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(keys[1]); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(keys[2], []byte("expired"), WithExpiration(past)); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(keys[3], []byte("expired"), WithExpiration(past)); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(keys[4], []byte("future"), WithExpiration(future)); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(keys[5], []byte("future"), WithExpiration(future)); err != nil {
		t.Fatal(err)
	}
	if err := s.log.openHead(2); err != nil {
		t.Fatal(err)
	}
	large := bytes.Repeat([]byte{'v'}, 128<<10)
	for _, item := range []struct{ key, value []byte }{
		{keys[3], []byte("revived")},
		{keys[5], []byte("cleared")},
		{keys[6], nil},
		{keys[7], large},
		{nil, []byte("empty key")},
	} {
		if err := db.Set(item.key, item.value); err != nil {
			t.Fatal(err)
		}
	}
	scalarKey := keysForShard(db, s, "scalar-recovery", 1)[0]
	if err := db.Set(scalarKey, uint64(41), WithExpiration(future)); err != nil {
		t.Fatal(err)
	}
	size := s.log.size
	db = reopenDB(t, db)
	s = &db.shards[0]
	if s.log.headSegment != 2 || s.log.size != size {
		t.Fatalf("recovered head: segment=%d size=%d, want 2 and %d", s.log.headSegment, s.log.size, size)
	}
	if got := openFilesUnder(t, db.path); got != shardCount {
		t.Fatalf("recovery left %d descriptors open, want %d", got, shardCount)
	}
	for i, key := range retained {
		if got, err := db.Get[[]byte](key); err != nil || string(got) != fmt.Sprintf("shard-%d", i) {
			t.Fatalf("shard %d: value=%q error=%v", i, got, err)
		}
	}
	wantValues := [][]byte{[]byte("updated"), nil, nil, []byte("revived"), []byte("future"), []byte("cleared"), {}, large}
	for i, key := range keys {
		var wantErr error
		if i == 1 || i == 2 {
			wantErr = ErrNotFound
		}
		_, hash := db.shard(key)
		pos, _, _, expiration, err := s.index.findPayload(s.log, key, hash, nil)
		if err != nil {
			t.Fatal(err)
		}
		if i == 4 && (pos < 0 || expiration != future.UnixNano()) {
			t.Fatalf("future expiration was lost: %d", expiration)
		}
		if got, err := db.Get[[]byte](key); err != wantErr || !bytes.Equal(got, wantValues[i]) {
			t.Fatalf("key %d: value length=%d error=%v, want length=%d error=%v", i, len(got), err, len(wantValues[i]), wantErr)
		}
	}
	if got, err := db.Get[[]byte]([]byte(nil)); err != nil || string(got) != "empty key" {
		t.Fatalf("empty key: value=%q error=%v", got, err)
	}
	if len(s.index.expirations) != 2 {
		t.Fatalf("got %d expiration entries, want 2", len(s.index.expirations))
	}
	if err := db.Set(keys[0], []byte("updated")); err != nil || s.log.size != size {
		t.Fatalf("unchanged Set appended after recovery: size=%d error=%v", s.log.size, err)
	}
	if err := db.Set(keys[0], []byte("appended")); err != nil {
		t.Fatal(err)
	}
	_, hash := db.shard(keys[0])
	pos, _, _, _, err := s.index.findPayload(s.log, keys[0], hash, nil)
	if err != nil || pos < 0 || s.index.slots[pos]&addressMask != 2<<26|size {
		t.Fatalf("append did not use recovered offset %d: position=%d error=%v", size, pos, err)
	}
	if err := db.Delete(retained[0]); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(scalarKey, func(v uint64, exists bool) (uint64, error) { return v + 1, nil}); err != nil {
		t.Fatal(err)
	}
	db = reopenDB(t, db)
	if got, err := db.Get[[]byte](keys[0]); err != nil || string(got) != "appended" {
		t.Fatalf("second recovery: value=%q error=%v", got, err)
	}
	if _, err := db.Get[[]byte](retained[0]); err != ErrNotFound {
		t.Fatalf("delete after recovery was lost: %v", err)
	}
	if got, err := db.Get[uint64](scalarKey); err != nil || got != 42 {
		t.Fatalf("scalar update after recovery: value=%d error=%v", got, err)
	}
}

func TestDBRecoveryFailureClosesFiles(t *testing.T) {
	db := newInternalDB(t)
	s := &db.shards[shardCount/2]
	key := keysForShard(db, s, "corruption", 1)[0]
	if err := db.Set(key, []byte("value")); err != nil {
		t.Fatal(err)
	}
	name := s.log.headFile.Name()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 1
	if err := os.WriteFile(name, data, 0600); err != nil {
		t.Fatal(err)
	}
	opened, err := Open(db.path)
	if opened != nil || err == nil || !strings.Contains(err.Error(), "crc mismatch") {
		if opened != nil {
			opened.Close()
		}
		t.Fatalf("Open: db=%v error=%v, want crc mismatch", opened, err)
	}
	if got := openFilesUnder(t, db.path); got != 0 {
		t.Fatalf("failed recovery leaked %d descriptors", got)
	}
	if got, err := os.ReadFile(name); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("failed recovery changed existing data: %v", err)
	}
}
