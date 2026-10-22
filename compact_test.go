package dot

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"
)

func checkSegmentAccounting(t *testing.T, s *Shard) {
	t.Helper()
	refs := make(map[uint64]uint32)
	entries := 0
	addresses := make(map[uint64]bool)
	for _, slot := range s.index.slots {
		if !empty(slot) {
			refs[slot&segmentMask>>26]++
			entries++
			addresses[slot&addressMask] = true
		}
	}
	if entries != s.index.entryCount {
		t.Fatalf("index entries=%d, entryCount=%d", entries, s.index.entryCount)
	}
	var total, totalBytes, liveBytes uint64
	for seg, count := range s.log.segmentRecordCounts {
		total += uint64(count)
		data, err := os.ReadFile(s.log.segmentPath(seg))
		if err != nil {
			t.Fatal(err)
		}
		if s.log.segmentBytes[seg] != uint64(len(data)) {
			t.Fatalf("segment %d bytes=%d, want %d", seg, s.log.segmentBytes[seg], len(data))
		}
		totalBytes += uint64(len(data))
		var records uint32
		for off := 0; off < len(data); records++ {
			if len(data)-off < logHeaderSize {
				t.Fatalf("segment %d has incomplete header at %d", seg, off)
			}
			size := logHeaderSize + int(binary.BigEndian.Uint32(data[off+5:])) + int(binary.BigEndian.Uint32(data[off+9:]))
			if addresses[seg<<26|uint64(off)] {
				if exp, ok := s.index.expirations[seg<<26|uint64(off)]; ok && exp.recordBytes != uint64(size) {
					t.Fatalf("expiration bytes=%d, want %d", exp.recordBytes, size)
				}
				liveBytes += uint64(size)
				delete(addresses, seg<<26|uint64(off))
			}
			off += size
			if off > len(data) {
				t.Fatalf("segment %d has incomplete payload", seg)
			}
		}
		if records != count || refs[seg] != s.log.segmentRefCounts[seg] {
			t.Fatalf("segment %d: records=%d/%d refs=%d/%d", seg, records, count, refs[seg], s.log.segmentRefCounts[seg])
		}
		delete(refs, seg)
	}
	if s.log.recordCount != total {
		t.Fatalf("total records=%d, want %d", s.log.recordCount, total)
	}
	if s.log.recordBytes != totalBytes || s.index.liveBytes != liveBytes {
		t.Fatalf("byte accounting: total=%d/%d live=%d/%d", s.log.recordBytes, totalBytes, s.index.liveBytes, liveBytes)
	}
	if len(s.log.segmentBytes) != len(s.log.segmentRecordCounts) || len(addresses) != 0 {
		t.Fatal("byte accounting references missing records or segments")
	}
	if len(refs) != 0 {
		t.Fatalf("index references missing segments: %v", refs)
	}
	for seg := range s.log.segmentRefCounts {
		if _, exists := s.log.segmentRecordCounts[seg]; !exists {
			t.Fatalf("reference count for missing segment %d", seg)
		}
	}
}

func stopTestMaintenance(db *DB) {
	db.backgroundCancel()
	db.backgroundWG.Wait()
	for i := range db.shards {
		s := &db.shards[i]
		s.lock.Lock()
		s.compactionQueue = nil
		s.lock.Unlock()
	}
}

func TestDBCompaction(t *testing.T) {
	db := newInternalDB(t)
	stopTestMaintenance(db)
	s := &db.shards[0]
	keys := keysForShard(db, s, "compact", 6)
	for _, key := range keys {
		if err := db.Set(key, []byte("old")); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.log.openHead(1); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(time.Hour)
	past := time.Now().Add(-time.Hour)
	if err := db.Delete(keys[0]); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(keys[1], []byte("expired"), WithExpiration(past)); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(keys[2], []byte("future"), WithExpiration(future)); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(keys[3], uint64(42)); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(keys[4], []byte{}); err != nil {
		t.Fatal(err)
	}
	checkSegmentAccounting(t, s)
	if err := db.compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	checkSegmentAccounting(t, s)
	if s.log.headSegment != 1 || len(s.log.segmentRecordCounts) != 1 || s.log.segmentRecordCounts[1] != 4 {
		t.Fatalf("unexpected compacted segments: head=%d counts=%v", s.log.headSegment, s.log.segmentRecordCounts)
	}
	if got := openFilesUnder(t, db.path); got != shardCount {
		t.Fatalf("compaction left %d descriptors open, want %d", got, shardCount)
	}
	if _, err := os.Stat(s.log.segmentPath(0)); !os.IsNotExist(err) {
		t.Fatalf("oldest source was not removed: %v", err)
	}
	before := s.log.size
	if err := db.compact(context.Background()); err != nil || s.log.size != before {
		t.Fatalf("repeat compaction: size=%d error=%v", s.log.size, err)
	}
	db = reopenDB(t, db)
	stopTestMaintenance(db)
	s = &db.shards[0]
	checkSegmentAccounting(t, s)
	for _, key := range keys[:2] {
		if _, err := db.Get[[]byte](key); err != ErrNotFound {
			t.Fatalf("deleted or expired key returned: %v", err)
		}
	}
	if value, err := db.Get[[]byte](keys[2]); err != nil || string(value) != "future" {
		t.Fatalf("future value=%q error=%v", value, err)
	}
	if value, err := db.Get[uint64](keys[3]); err != nil || value != 42 {
		t.Fatalf("scalar value=%d error=%v", value, err)
	}
	if value, err := db.Get[[]byte](keys[4]); err != nil || len(value) != 0 {
		t.Fatalf("empty value=%q error=%v", value, err)
	}
	if value, err := db.Get[[]byte](keys[5]); err != nil || string(value) != "old" {
		t.Fatalf("retained value=%q error=%v", value, err)
	}
	if len(s.index.expirations) != 1 {
		t.Fatalf("expiration count=%d, want 1", len(s.index.expirations))
	}
	if err := db.Set(keys[2], []byte("future"), WithExpiration(future)); err != nil || s.log.size != before {
		t.Fatalf("unchanged set appended after compaction: %v", err)
	}
	if err := db.Update(keys[3], func(value uint64, exists bool) (uint64, error) { return value + 1, nil}); err != nil {
		t.Fatal(err)
	}
	checkSegmentAccounting(t, s)
}

func TestCompactionConcurrentUpdates(t *testing.T) {
	db := newInternalDB(t)
	stopTestMaintenance(db)
	key := keysForShard(db, &db.shards[0], "concurrent-compact", 1)[0]
	if err := db.Set(key, uint64(0)); err != nil {
		t.Fatal(err)
	}
	requests := make(chan struct{}, 1)
	compacted := make(chan error, 1)
	go func() {
		for range requests {
			if err := db.compact(context.Background()); err != nil {
				compacted <- err
				return
			}
		}
		compacted <- nil
	}()
	const workers, updates = 4, 100
	errors := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for i := range updates {
				if err := db.Update(key, func(value uint64, exists bool) (uint64, error) { return value + 1, nil}); err != nil {
					errors <- err
					return
				}
				if i%10 == 0 {
					select {
					case requests <- struct{}{}:
					default:
					}
				}
			}
		})
	}
	wg.Wait()
	close(requests)
	if err := <-compacted; err != nil {
		t.Fatal(err)
	}
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	db = reopenDB(t, db)
	stopTestMaintenance(db)
	if value, err := db.Get[uint64](key); err != nil || value != workers*updates {
		t.Fatalf("counter=%d error=%v, want %d", value, err, workers*updates)
	}
	checkSegmentAccounting(t, &db.shards[0])
}

func TestCompactionDoesNotReuseDeleteBeforeOlderHistory(t *testing.T) {
	path := t.TempDir()
	key := []byte("deleted")
	writeRecoverySegment(t, path, 0, recoveryRecord(typeBytes, key, []byte("old"), 0))
	writeRecoverySegment(t, path, 1, recoveryRecord(typeDelete, key, nil, 0))
	writeRecoverySegment(t, path, 2, nil)
	log, idx := openRecoveryLog(t, path)
	s := &Shard{log: log, index: idx}
	if log.segmentRefCounts[1] != 0 || log.segmentRecordCounts[1] != 1 {
		t.Fatal("fixture does not contain an unreferenced delete")
	}
	if err := s.compact(); err != nil {
		t.Fatal(err)
	}
	if err := log.headFile.Close(); err != nil {
		t.Fatal(err)
	}
	log, idx = openRecoveryLog(t, path)
	if idx.entryCount != 0 || len(log.segmentRecordCounts) != 1 || log.headSegment != 2 {
		t.Fatalf("recovered deleted key or obsolete files: entries=%d counts=%v", idx.entryCount, log.segmentRecordCounts)
	}
}

func TestCompactionRecoveryPublicationBoundaries(t *testing.T) {
	for _, boundary := range []string{"unpublished", "oldest-cleaned", "packed"} {
		t.Run(boundary, func(t *testing.T) {
			path := t.TempDir()
			keep, deleted := []byte("keep"), []byte("deleted")
			writeRecoverySegment(t, path, maxSegment,
				append(recoveryRecord(typeBytes, keep, []byte("keep"), 0), recoveryRecord(typeBytes, deleted, []byte("old"), 0)...))
			writeRecoverySegment(t, path, 0, recoveryRecord(typeDelete, deleted, nil, 0))
			log, idx := openRecoveryLog(t, path)
			pos := checkRecoveredValue(t, log, idx, keep, []byte("keep"))
			staging := log.segmentPath(maxSegment) + ".compact"
			switch boundary {
			case "unpublished":
				if err := os.WriteFile(staging, []byte("incomplete replacement"), 0600); err != nil {
					t.Fatal(err)
				}
			case "oldest-cleaned", "packed":
				if err := log.replaceSegment(idx, maxSegment, []int{pos}, nil); err != nil {
					t.Fatal(err)
				}
				if boundary == "packed" {
					if err := log.replaceSegment(idx, 0, nil, nil); err != nil {
						t.Fatal(err)
					}
					copied := false
					if err := log.replaceSegment(idx, 0, nil, func() (int, bool) {
						if copied {
							return 0, false
						}
						copied = true
						return pos, true
					}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := log.headFile.Close(); err != nil {
				t.Fatal(err)
			}
			log, idx = openRecoveryLog(t, path)
			checkRecoveredValue(t, log, idx, keep, []byte("keep"))
			if idx.entryCount != 1 || log.headSegment != 0 {
				t.Fatalf("recovery: entries=%d head=%d", idx.entryCount, log.headSegment)
			}
			if _, err := os.Stat(staging); !os.IsNotExist(err) {
				t.Fatalf("unpublished replacement was not removed: %v", err)
			}
			s := &Shard{log: log, index: idx}
			checkSegmentAccounting(t, s)
			if err := s.compact(); err != nil {
				t.Fatal(err)
			}
			checkSegmentAccounting(t, s)
		})
	}
}

func TestCompactionFailurePreservesIndexAndLog(t *testing.T) {
	db := newInternalDB(t)
	stopTestMaintenance(db)
	s := &db.shards[0]
	key := keysForShard(db, s, "failure", 1)[0]
	for _, value := range []string{"old", "new"} {
		if err := db.Set(key, []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	before := slices.Clone(s.index.slots)
	data, err := os.ReadFile(s.log.segmentPath(0))
	if err != nil {
		t.Fatal(err)
	}
	blocker := s.log.segmentPath(0) + ".compact"
	if err := os.Mkdir(blocker, 0700); err != nil {
		t.Fatal(err)
	}
	if err := db.compact(context.Background()); err == nil {
		t.Fatal("compaction unexpectedly succeeded")
	}
	current, err := os.ReadFile(s.log.segmentPath(0))
	if err != nil || !bytes.Equal(data, current) || !slices.Equal(before, s.index.slots) {
		t.Fatalf("failed compaction changed data/index: %v", err)
	}
	checkSegmentAccounting(t, s)
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	if err := db.compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	if value, err := db.Get[[]byte](key); err != nil || string(value) != "new" {
		t.Fatalf("retry: value=%q error=%v", value, err)
	}
}

func TestCompactionRandomizedWrapAndReopen(t *testing.T) {
	path := t.TempDir()
	shardPath := filepath.Join(path, "00")
	if err := os.Mkdir(shardPath, 0700); err != nil {
		t.Fatal(err)
	}
	writeRecoverySegment(t, shardPath, maxSegment-2, nil)
	db := openRecoveryFixture(t, path)
	var err error
	keys := keysForShard(db, &db.shards[0], "random", 24)
	want := make(map[int]string)
	random := rand.New(rand.NewPCG(1, 2))
	check := func() {
		t.Helper()
		for i, key := range keys {
			value, err := db.Get[[]byte](key)
			expected, exists := want[i]
			if exists && (err != nil || string(value) != expected) || !exists && err != ErrNotFound {
				t.Fatalf("key %d: value=%q error=%v, want %q exists=%v", i, value, err, expected, exists)
			}
		}
		checkSegmentAccounting(t, &db.shards[0])
	}
	for step := range 360 {
		i := random.IntN(len(keys))
		switch random.IntN(4) {
		case 0:
			err = db.Delete(keys[i])
			delete(want, i)
		case 1:
			err = db.Set(keys[i], []byte("expired"), WithExpiration(time.Now().Add(-time.Hour)))
			delete(want, i)
		default:
			value := fmt.Sprintf("value-%d", step)
			err = db.Set(keys[i], []byte(value))
			want[i] = value
		}
		if err != nil {
			t.Fatal(err)
		}
		if step%20 == 19 {
			log := db.shards[0].log
			if err := log.headFile.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := log.openHead(incrementSegment(log.headSegment)); err != nil {
				t.Fatal(err)
			}
		}
		if step%60 == 59 {
			check()
			if err := db.compact(context.Background()); err != nil {
				t.Fatal(err)
			}
			check()
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = openRecoveryFixture(t, path)
			check()
		}
	}
	if db.shards[0].log.headSegment >= maxSegment-2 {
		t.Fatal("fixture did not wrap")
	}
}

func TestCompactionFillsAndSealsOutputs(t *testing.T) {
	db := newInternalDB(t, WithCompactionThreshold(0, 0))
	stopTestMaintenance(db)
	s := &db.shards[0]
	keys := keysForShard(db, s, "large", 6)
	for _, value := range []string{"old", "new"} {
		if err := db.Set(keys[0], []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Delete(keys[0]); err != nil {
		t.Fatal(err)
	}
	large := bytes.Repeat([]byte{'x'}, logSegmentSize/2)
	for _, key := range keys[1:5] {
		if err := db.Set(key, large); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Set(keys[5], []byte("small")); err != nil {
		t.Fatal(err)
	}
	if err := db.compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(s.log.segmentRefCounts, map[uint64]uint32{1: 2, 2: 3}) {
		t.Fatalf("packed references=%v", s.log.segmentRefCounts)
	}
	if s.log.size < logSegmentSize {
		t.Fatal("head did not reach the sealing boundary")
	}
	db = reopenDB(t, db)
	stopTestMaintenance(db)
	for _, key := range keys[1:5] {
		value, err := db.Get[[]byte](key)
		if err != nil || !bytes.Equal(value, large) {
			t.Fatalf("large value: length=%d error=%v", len(value), err)
		}
	}
	if err := db.Set(keys[5], []byte("rotated")); err != nil {
		t.Fatal(err)
	}
	if db.shards[0].log.headSegment != 3 {
		t.Fatal("append did not rotate the compacted full head")
	}
	checkSegmentAccounting(t, &db.shards[0])
}
