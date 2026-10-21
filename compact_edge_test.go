package dot

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"
)

func TestCompactionResumesAfterRecovery(t *testing.T) {
	for _, state := range []string{"cleaned", "drained", "empty", "unreferenced"} {
		t.Run(state, func(t *testing.T) {
			path := t.TempDir()
			key := []byte("keep")
			var source, head []byte
			switch state {
			case "cleaned":
				source = recoveryRecord('s', []byte("other"), []byte("value"), 0)
				head = recoveryRecord('s', key, []byte("new"), 0)
			case "drained":
				head = recoveryRecord('s', key, []byte("new"), 0)
			case "unreferenced":
				source = recoveryRecord('s', key, []byte("old"), 0)
				head = recoveryRecord('s', key, []byte("new"), 0)
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
		source = append(source, recoveryRecord('s', []byte("keep"), fmt.Appendf(nil, "version-%d", i), 0)...)
	}
	for i := range 5 {
		source = append(source, recoveryRecord('s', fmt.Appendf(nil, "other-%d", i), []byte("value"), 0)...)
	}
	writeRecoverySegment(t, path, 0, source)
	head := append(recoveryRecord('s', []byte("head"), []byte("old"), 0), recoveryRecord('s', []byte("head"), []byte("new"), 0)...)
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
	source := append(recoveryRecord('s', []byte("a"), large, 0), recoveryRecord('s', []byte("b"), large, 0)...)
	writeRecoverySegment(t, path, 0, source)
	// The head has a copy of a, but its full source has not been retired. Only
	// one of seven records is obsolete, so the ordinary 20% trigger would skip it.
	head := recoveryRecord('s', []byte("a"), large, 0)
	for i := range 4 {
		head = append(head, recoveryRecord('s', fmt.Appendf(nil, "small-%d", i), []byte("value"), 0)...)
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
			data := append(recoveryRecord('s', key, []byte("old"), 0), recoveryRecord('s', key, []byte("new"), 0)...)
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
	writeRecoverySegment(t, path, 0, recoveryRecord('s', x, x, future+3))
	var data []byte
	data = append(data, recoveryRecord('s', dead, []byte("gone"), 0)...)
	data = append(data, recoveryRecord('s', a, a, future+1)...)
	data = append(data, recoveryRecord('s', b, b, future+2)...)
	data = append(data, recoveryRecord('d', dead, nil, 0)...)
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
			data := append(recoveryRecord('s', key, []byte("old"), 0), recoveryRecord('s', key, []byte("new"), 0)...)
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
				if _, _, err := log.write('s', key, []byte("blocked"), 0); err != log.writeErr {
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

func TestCompactionRemovalFailure(t *testing.T) {
	path := t.TempDir()
	key := []byte("key")
	for i, seg := range []uint64{maxSegment - 1, maxSegment, 0} {
		writeRecoverySegment(t, path, seg, recoveryRecord('s', key, fmt.Appendf(nil, "version-%d", i), 0))
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
