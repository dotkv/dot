package dot

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
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
		if !isEmptySlot(slot) {
			refs[addressSegment(slot)]++
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
		if seg == s.log.headSegment && len(s.log.pending) != 0 {
			if uint64(len(data)) != s.log.bufferStart {
				t.Fatalf("buffer starts at %d, file ends at %d", s.log.bufferStart, len(data))
			}
			data = append(data, s.log.pending...)
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
			size := int(recordSize(int(binary.BigEndian.Uint32(data[off+5:])), int(binary.BigEndian.Uint32(data[off+9:]))))
			if addresses[logAddress(seg, uint64(off))] {
				if exp, ok := s.index.expirations[logAddress(seg, uint64(off))]; ok && exp.recordBytes != uint64(size) {
					t.Fatalf("expiration bytes=%d, want %d", exp.recordBytes, size)
				}
				liveBytes += uint64(size)
				delete(addresses, logAddress(seg, uint64(off)))
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
	if got := openFilesUnder(t, db.path); got != defaultShardCount {
		t.Fatalf("compaction left %d descriptors open, want %d", got, defaultShardCount)
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
	if err := db.Update(keys[3], func(value uint64, exists bool) (uint64, error) { return value + 1, nil }); err != nil {
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
				if err := db.Update(key, func(value uint64, exists bool) (uint64, error) { return value + 1, nil }); err != nil {
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

func TestCompactionResumesAfterRecovery(t *testing.T) {
	for _, state := range []string{"cleaned", "drained", "empty", "unreferenced"} {
		t.Run(state, func(t *testing.T) {
			path := t.TempDir()
			key := []byte("keep")
			var source, head []byte
			switch state {
			case "cleaned":
				source = recoveryRecord(typeBytes, []byte("other"), []byte("value"), 0)
				head = recoveryRecord(typeBytes, key, []byte("new"), 0)
			case "drained":
				head = recoveryRecord(typeBytes, key, []byte("new"), 0)
			case "unreferenced":
				source = recoveryRecord(typeBytes, key, []byte("old"), 0)
				head = recoveryRecord(typeBytes, key, []byte("new"), 0)
			}
			writeRecoverySegment(t, path, maxSegment, source)
			writeRecoverySegment(t, path, 0, head)
			log, idx := openRecoveryLog(t, path)
			if state == "unreferenced" {
				// Removing an unused oldest segment must not need a replacement.
				if err := os.Mkdir(log.segmentPath(maxSegment)+".compact", 0700); err != nil {
					t.Fatal(err)
				}
			}
			shard := &Shard{log: log, index: idx}
			if err := shard.compact(); err != nil {
				t.Fatal(err)
			}
			if len(log.segmentRecordCounts) != 1 || log.headSegment != 0 || log.compactPending {
				t.Fatalf("unfinished pass: head=%d counts=%v pending=%v", log.headSegment, log.segmentRecordCounts, log.compactPending)
			}
			checkSegmentAccounting(t, shard)
			before, err := log.headFile.Stat()
			if err != nil {
				t.Fatal(err)
			}
			if err := shard.compact(); err != nil {
				t.Fatal(err)
			}
			after, err := log.headFile.Stat()
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("completed pass was rewritten: %v", err)
			}
			if err := log.headFile.Close(); err != nil {
				t.Fatal(err)
			}
			log, idx = openRecoveryLog(t, path)
			if state != "empty" {
				checkRecoveredValue(t, log, idx, key, []byte("new"))
			}
			if state == "cleaned" {
				checkRecoveredValue(t, log, idx, []byte("other"), []byte("value"))
			}
		})
	}
}

func TestCompactionRetriesBelowThreshold(t *testing.T) {
	path := t.TempDir()
	var source []byte
	for i := range 10 {
		source = append(source, recoveryRecord(typeBytes, []byte("keep"), fmt.Appendf(nil, "version-%d", i), 0)...)
	}
	for i := range 5 {
		source = append(source, recoveryRecord(typeBytes, fmt.Appendf(nil, "other-%d", i), []byte("value"), 0)...)
	}
	writeRecoverySegment(t, path, 0, source)
	head := append(recoveryRecord(typeBytes, []byte("head"), []byte("old"), 0), recoveryRecord(typeBytes, []byte("head"), []byte("new"), 0)...)
	writeRecoverySegment(t, path, 1, head)
	log, idx := openRecoveryLog(t, path)
	blocker := log.segmentPath(1) + ".compact"
	if err := os.Mkdir(blocker, 0700); err != nil {
		t.Fatal(err)
	}
	shard := &Shard{log: log, index: idx}
	if err := shard.compact(); err == nil {
		t.Fatal("packing unexpectedly succeeded")
	}
	// Cleaning the source left just one dead record out of eight, below 20%.
	if log.segmentRecordCounts[0] != 6 || idx.entryCount != 7 || !log.compactPending {
		t.Fatalf("partial pass: counts=%v entries=%d pending=%v", log.segmentRecordCounts, idx.entryCount, log.compactPending)
	}
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	if err := shard.compact(); err != nil {
		t.Fatal(err)
	}
	if len(log.segmentRecordCounts) != 1 || log.compactPending {
		t.Fatalf("retry did not finish: counts=%v pending=%v", log.segmentRecordCounts, log.compactPending)
	}
	checkRecoveredValue(t, log, idx, []byte("keep"), []byte("version-9"))
	checkRecoveredValue(t, log, idx, []byte("head"), []byte("new"))
	checkSegmentAccounting(t, shard)
}

func TestCompactionResumesPackedFullSource(t *testing.T) {
	path := t.TempDir()
	large := bytes.Repeat([]byte{'x'}, logSegmentSize/2)
	source := append(recoveryRecord(typeBytes, []byte("a"), large, 0), recoveryRecord(typeBytes, []byte("b"), large, 0)...)
	writeRecoverySegment(t, path, 0, source)
	// The head has a copy of a, but its full source has not been retired. Only
	// one of seven records is obsolete, so the ordinary 20% trigger would skip it.
	head := recoveryRecord(typeBytes, []byte("a"), large, 0)
	for i := range 4 {
		head = append(head, recoveryRecord(typeBytes, fmt.Appendf(nil, "small-%d", i), []byte("value"), 0)...)
	}
	writeRecoverySegment(t, path, 1, head)
	log, idx := openRecoveryLog(t, path)
	if err := (&Shard{log: log, index: idx}).compact(); err != nil {
		t.Fatal(err)
	}
	if len(log.segmentRecordCounts) != 1 || log.compactPending {
		t.Fatalf("source not retired: counts=%v pending=%v", log.segmentRecordCounts, log.compactPending)
	}
	for _, key := range []string{"a", "b"} {
		checkRecoveredValue(t, log, idx, []byte(key), large)
	}
	checkSegmentAccounting(t, &Shard{log: log, index: idx})
}

func TestCompactionStaleStaging(t *testing.T) {
	for _, kind := range []string{"regular", "hardlink", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			path := t.TempDir()
			key := []byte("key")
			data := append(recoveryRecord(typeBytes, key, []byte("old"), 0), recoveryRecord(typeBytes, key, []byte("new"), 0)...)
			original := writeRecoverySegment(t, path, 0, data)
			log, idx := openRecoveryLog(t, path)
			staging := original + ".compact"
			var err error
			switch kind {
			case "regular":
				err = os.WriteFile(staging, []byte("unfinished replacement"), 0600)
			case "hardlink":
				err = os.Link(original, staging)
			case "symlink":
				err = os.Symlink(original, staging)
			}
			if err != nil {
				t.Fatal(err)
			}
			err = (&Shard{log: log, index: idx}).compact()
			if kind == "symlink" {
				if !errors.Is(err, os.ErrExist) {
					t.Fatalf("staging symlink was not rejected: %v", err)
				}
				if current, err := os.ReadFile(original); err != nil || !bytes.Equal(current, data) {
					t.Fatalf("staging symlink damaged original: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			checkRecoveredValue(t, log, idx, key, []byte("new"))
			checkSegmentAccounting(t, &Shard{log: log, index: idx})
		})
	}
}

func TestCompactionExpirationAddressReuse(t *testing.T) {
	path := t.TempDir()
	x, a, b, dead := []byte("xxxx"), []byte("aaaa"), []byte("bbbb"), []byte("dead")
	future := time.Now().Add(time.Hour).UnixNano()
	writeRecoverySegment(t, path, 0, recoveryRecord(typeBytes, x, x, future+3))
	var data []byte
	data = append(data, recoveryRecord(typeBytes, dead, []byte("gone"), 0)...)
	data = append(data, recoveryRecord(typeBytes, a, a, future+1)...)
	data = append(data, recoveryRecord(typeBytes, b, b, future+2)...)
	data = append(data, recoveryRecord(typeDelete, dead, nil, 0)...)
	writeRecoverySegment(t, path, 1, data)
	log, idx := openRecoveryLog(t, path)
	pa := checkRecoveredValue(t, log, idx, a, a)
	pb := checkRecoveredValue(t, log, idx, b, b)
	px := checkRecoveredValue(t, log, idx, x, x)
	oldA, oldB := idx.slots[pa]&addressMask, idx.slots[pb]&addressMask
	copied := false
	if err := log.replaceSegment(idx, 1, []int{pa, pb}, func() (int, bool) {
		if copied {
			return 0, false
		}
		copied = true
		return px, true
	}); err != nil {
		t.Fatal(err)
	}
	if idx.slots[pb]&addressMask != oldA || idx.slots[px]&addressMask != oldB {
		t.Fatal("fixture did not reuse earlier expiration addresses")
	}
	for i, pos := range []int{pa, pb, px} {
		if exp := idx.expirations[idx.slots[pos]&addressMask]; exp.at != future+int64(i+1) {
			t.Fatalf("position %d: wrong expiration %v", pos, exp)
		}
	}
	checkSegmentAccounting(t, &Shard{log: log, index: idx})
	if err := log.headFile.Close(); err != nil {
		t.Fatal(err)
	}
	log, idx = openRecoveryLog(t, path)
	for _, key := range [][]byte{a, b, x} {
		checkRecoveredValue(t, log, idx, key, key)
	}
}

func TestCompactionCancellationBetweenShards(t *testing.T) {
	db := newInternalDB(t)
	stopTestMaintenance(db)
	for i := range 2 {
		key := keysForShard(db, &db.shards[i], "cancel", 1)[0]
		for _, value := range []string{"old", "new"} {
			if err := db.Set(key, []byte(value)); err != nil {
				t.Fatal(err)
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &db.shards[0]
	s.lock.RLock()
	locked := true
	defer func() {
		if locked {
			s.lock.RUnlock()
		}
	}()
	done := make(chan error, 1)
	go func() { done <- db.compact(ctx) }()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	// A waiting writer blocks new readers. Hold the first reader until the
	// compactor has passed its cancellation check and is waiting for shard 0.
	for s.lock.TryRLock() {
		s.lock.RUnlock()
		select {
		case <-deadline.C:
			t.Fatal("compaction did not reach shard 0")
		default:
			runtime.Gosched()
		}
	}
	cancel()
	s.lock.RUnlock()
	locked = false
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-deadline.C:
		t.Fatal("canceled compaction did not finish")
	}
	if s.log.segmentRecordCounts[0] != 1 || db.shards[1].log.segmentRecordCounts[0] != 2 {
		t.Fatal("cancellation did not finish shard 0 and leave shard 1 untouched")
	}
}

func TestCompactionPublicationFailures(t *testing.T) {
	for _, failure := range []string{"rename", "directory-sync", "head-reopen"} {
		t.Run(failure, func(t *testing.T) {
			path := t.TempDir()
			key := []byte("key")
			data := append(recoveryRecord(typeBytes, key, []byte("old"), 0), recoveryRecord(typeBytes, key, []byte("new"), 0)...)
			seg := uint64(0)
			writeRecoverySegment(t, path, seg, data)
			if failure == "directory-sync" {
				writeRecoverySegment(t, path, 1, nil)
			}
			log, idx := openRecoveryLog(t, path)
			pos := checkRecoveredValue(t, log, idx, key, []byte("new"))
			oldAddress := idx.slots[pos] & addressMask
			originalPath := log.path
			staging := log.segmentPath(seg) + ".compact"
			// The fill callback runs after own records are copied but before
			// publication, allowing real filesystem failures without test hooks.
			err := log.replaceSegment(idx, seg, []int{pos}, func() (int, bool) {
				if failure == "rename" {
					if err := os.Rename(staging, staging+".held"); err != nil {
						t.Fatal(err)
					}
					if err := os.Mkdir(staging, 0700); err != nil {
						t.Fatal(err)
					}
				} else {
					// Captured publication paths stay valid; reopening the head
					// and/or opening the directory for Sync will now fail.
					log.path = path + "/missing/"
				}
				return 0, false
			})
			log.path = originalPath
			if err == nil {
				t.Fatal("publication unexpectedly succeeded")
			}
			if failure == "rename" {
				if idx.slots[pos]&addressMask != oldAddress || log.writeErr != nil {
					t.Fatal("unpublished failure changed the index or poisoned the log")
				}
				if current, err := os.ReadFile(log.segmentPath(seg)); err != nil || !bytes.Equal(current, data) {
					t.Fatalf("rename failure changed the original: %v", err)
				}
				if err := log.replaceSegment(idx, seg, []int{pos}, nil); err != nil {
					t.Fatalf("retry: %v", err)
				}
			} else {
				if log.writeErr == nil || idx.slots[pos]&addressMask == oldAddress {
					t.Fatal("published replacement did not update the index and block writes")
				}
				if _, _, err := log.write(typeBytes, key, []byte("blocked"), 0); err != log.writeErr {
					t.Fatalf("subsequent write was not blocked: %v", err)
				}
			}
			checkRecoveredValue(t, log, idx, key, []byte("new"))
			checkSegmentAccounting(t, &Shard{log: log, index: idx})
			if err := log.headFile.Close(); err != nil {
				t.Fatal(err)
			}
			log, idx = openRecoveryLog(t, path)
			checkRecoveredValue(t, log, idx, key, []byte("new"))
		})
	}
}

func TestCompactionHeadSyncBeforeRemovingHistory(t *testing.T) {
	for _, multiple := range []bool{false, true} {
		t.Run(fmt.Sprintf("multiple-segments=%v", multiple), func(t *testing.T) {
			path := t.TempDir()
			key := []byte("deleted")
			if multiple {
				writeRecoverySegment(t, path, maxSegment, recoveryRecord(typeBytes, key, []byte("old"), 0))
			}
			writeRecoverySegment(t, path, 0, recoveryRecord(typeDelete, key, nil, 0))
			log, idx := openRecoveryLog(t, path)
			log.compactPending = true
			// With no live records to copy, substitute a descriptor that cannot
			// be synced to observe whether history removal requires a head sync.
			reader, writer, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { reader.Close(); writer.Close() })
			if err := writer.Sync(); err == nil {
				t.Fatal("pipe unexpectedly supports Sync")
			}
			if err := log.headFile.Close(); err != nil {
				t.Fatal(err)
			}
			log.headFile = writer
			err = (&Shard{log: log, index: idx}).compact()
			if multiple {
				if err == nil {
					t.Fatal("removed older history without syncing the head")
				}
				for _, seg := range []uint64{maxSegment, 0} {
					if _, err := os.Stat(log.segmentPath(seg)); err != nil {
						t.Fatalf("changed history after failed head sync: %v", err)
					}
				}
			} else {
				if err != nil {
					t.Fatalf("single-segment replacement required a preliminary sync: %v", err)
				}
				if log.size != 0 || log.compactPending {
					t.Fatal("single-segment cleanup did not finish")
				}
			}
			if err := log.headFile.Close(); err != nil {
				t.Fatal(err)
			}
			log, idx = openRecoveryLog(t, path)
			if idx.entryCount != 0 {
				t.Fatal("recovery resurrected the deleted key")
			}
			checkSegmentAccounting(t, &Shard{log: log, index: idx})
		})
	}
}

func TestCompactionRemovalFailure(t *testing.T) {
	path := t.TempDir()
	key := []byte("key")
	for i, seg := range []uint64{maxSegment - 1, maxSegment, 0} {
		writeRecoverySegment(t, path, seg, recoveryRecord(typeBytes, key, fmt.Appendf(nil, "version-%d", i), 0))
	}
	log, idx := openRecoveryLog(t, path)
	if err := log.headFile.Sync(); err != nil {
		t.Fatal(err)
	}
	blocked := log.segmentPath(maxSegment)
	if err := os.Rename(blocked, blocked+".held"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(blocked, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blocked+"/blocker", nil, 0600); err != nil {
		t.Fatal(err)
	}
	remaining, err := log.removeUnusedPrefix([]uint64{maxSegment - 1, maxSegment, 0})
	if err == nil || len(remaining) != 2 || remaining[0] != maxSegment || log.writeErr != nil {
		t.Fatalf("partial removal: remaining=%v blocked=%v error=%v", remaining, log.writeErr, err)
	}
	if err := os.Remove(blocked + "/blocker"); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(blocked); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(blocked+".held", blocked); err != nil {
		t.Fatal(err)
	}
	// The first removal was already durable; recovery must accept the suffix.
	if err := log.headFile.Close(); err != nil {
		t.Fatal(err)
	}
	log, idx = openRecoveryLog(t, path)
	if err := (&Shard{log: log, index: idx}).compact(); err != nil {
		t.Fatal(err)
	}
	if len(log.segmentRecordCounts) != 1 {
		t.Fatalf("retry left unused segments: %v", log.segmentRecordCounts)
	}
	checkRecoveredValue(t, log, idx, key, []byte("version-2"))
	checkSegmentAccounting(t, &Shard{log: log, index: idx})
}
