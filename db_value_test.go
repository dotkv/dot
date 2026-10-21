package dot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"testing"
	"time"
)

func checkScalarGet[T interface {
	Value
	comparable
}](t *testing.T, db *DB, key []byte, want T, wantErr error) {
	t.Helper()
	got, err := db.Get[T](key)
	if wantErr == ErrNotFound && err != ErrNotFound {
		t.Fatalf("Get[%T]: got error %v, want the direct ErrNotFound sentinel", want, err)
	}
	if !errors.Is(err, wantErr) || got != want {
		t.Fatalf("Get[%T]: got %v, error %v; want %v, error %v", want, got, err, want, wantErr)
	}
}

func TestDBGetScalarEncodings(t *testing.T) {
	db := newInternalDB(t)
	key := []byte("scalar")
	for _, value := range []int64{0, 42, -42, math.MinInt64, math.MaxInt64} {
		if err := db.Set(key, value); err != nil {
			t.Fatal(err)
		}
		checkScalarGet(t, db, key, value, nil)
	}
	for _, value := range []uint64{0, 42, math.MaxInt64 + 1, math.MaxUint64} {
		if err := db.Set(key, value); err != nil {
			t.Fatal(err)
		}
		checkScalarGet(t, db, key, value, nil)
	}
	for _, bits := range []uint64{0, 1 << 63, 0x3ff8000000000000, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042} {
		if err := db.Set(key, math.Float64frombits(bits)); err != nil {
			t.Fatal(err)
		}
		value, err := db.Get[float64](key)
		if err != nil || math.Float64bits(value) != bits {
			t.Fatalf("float64 bits: got %x, want %x, error %v", math.Float64bits(value), bits, err)
		}
	}
	for _, value := range []bool{false, true} {
		if err := db.Set(key, value); err != nil {
			t.Fatal(err)
		}
		checkScalarGet(t, db, key, value, nil)
	}
}

func checkScalarSet[T Value](t *testing.T, db *DB, key []byte, value T, encoded []byte) {
	t.Helper()
	if err := db.Set(key, value); err != nil {
		t.Fatal(err)
	}
	s, hash := db.shard(key)
	pos, got, ty, _, err := s.index.findPayload(s.log, key, hash, nil)
	if err != nil || pos < 0 || checkType[T](ty) != nil || !bytes.Equal(got, encoded) {
		t.Fatalf("Set[%T]: bytes %x, want %x, error %v", value, got, encoded, err)
	}
	segment, size := s.log.headSegment, s.log.size
	if err := db.Set(key, value); err != nil {
		t.Fatal(err)
	}
	if s.log.headSegment != segment || s.log.size != size {
		t.Fatalf("Set[%T] appended an unchanged value", value)
	}
}

func TestDBSetScalarEncodings(t *testing.T) {
	db := newInternalDB(t)
	key := []byte("scalar")
	for _, value := range []int64{0, 42, -42, math.MinInt64, math.MaxInt64} {
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], uint64(value))
		checkScalarSet(t, db, key, value, encoded[:])
		checkScalarGet(t, db, key, value, nil)
	}
	for _, value := range []uint64{0, 42, math.MaxInt64 + 1, math.MaxUint64} {
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], value)
		checkScalarSet(t, db, key, value, encoded[:])
		checkScalarGet(t, db, key, value, nil)
	}
	for _, bits := range []uint64{0, 1 << 63, 0x3ff8000000000000, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042} {
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], bits)
		checkScalarSet(t, db, key, math.Float64frombits(bits), encoded[:])
		got, err := db.Get[float64](key)
		if err != nil || math.Float64bits(got) != bits {
			t.Fatalf("float64 bits: got %x, want %x, error %v", math.Float64bits(got), bits, err)
		}
	}
	checkScalarSet(t, db, key, false, nil)
	checkScalarGet(t, db, key, false, nil)
	checkScalarSet(t, db, key, true, nil)
	checkScalarGet(t, db, key, true, nil)
	// Explicit type arguments also allow untyped constants and nil.
	if err := db.Set[int64](key, 42); err != nil {
		t.Fatal(err)
	}
	checkScalarGet(t, db, key, int64(42), nil)
	if err := db.Set[[]byte](key, nil); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get[[]byte](key); err != nil || got == nil || len(got) != 0 {
		t.Fatalf("stored empty bytes: got %v, error %v", got, err)
	}
	value := []byte("original")
	if err := db.Set(key, value); err != nil {
		t.Fatal(err)
	}
	value[0] = 'x'
	if got, err := db.Get[[]byte](key); err != nil || string(got) != "original" {
		t.Fatalf("Set retained caller's buffer: got %q, error %v", got, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, err := range []error{
		db.Set(key, int64(42)), db.Set(key, uint64(42)),
		db.Set(key, 1.5), db.Set(key, true),
	} {
		if !errors.Is(err, os.ErrClosed) {
			t.Fatalf("Set after Close: got %v, want os.ErrClosed", err)
		}
	}
}

func checkGetMissing(t *testing.T, db *DB, key []byte) {
	t.Helper()
	checkScalarGet(t, db, key, int64(0), ErrNotFound)
	checkScalarGet(t, db, key, uint64(0), ErrNotFound)
	checkScalarGet(t, db, key, float64(0), ErrNotFound)
	checkScalarGet(t, db, key, false, ErrNotFound)
	if got, err := db.Get[[]byte](key); err != ErrNotFound || got != nil {
		t.Fatalf("Get[[]byte]: got %v, error %v; want nil and the direct ErrNotFound sentinel", got, err)
	}
}

func TestDBGetMissingDeletedAndClosed(t *testing.T) {
	db := newInternalDB(t)
	key := []byte("absent")
	for _, deleted := range []bool{false, true} {
		if deleted {
			if err := db.Set(key, []byte{1}); err != nil {
				t.Fatal(err)
			}
			if err := db.Delete(key); err != nil {
				t.Fatal(err)
			}
		}
		checkGetMissing(t, db, key)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	checkScalarGet(t, db, key, int64(0), os.ErrClosed)
	checkScalarGet(t, db, key, uint64(0), os.ErrClosed)
	checkScalarGet(t, db, key, float64(0), os.ErrClosed)
	checkScalarGet(t, db, key, false, os.ErrClosed)
}

func TestDBGetMissingOccupiedBucket(t *testing.T) {
	for _, fingerprintMatch := range []bool{false, true} {
		t.Run(fmt.Sprintf("fingerprint-match=%t", fingerprintMatch), func(t *testing.T) {
			db := newInternalDB(t)
			// The synthetic entry uses the missing key's hash, not the decoy's.
			db.backgroundCancel()
			db.backgroundWG.Wait()
			key := []byte("missing")
			s, hash := db.shard(key)
			segment, off, err := s.log.write(typeBytes, []byte("decoy"), []byte{1}, 0)
			if err != nil {
				t.Fatal(err)
			}
			tag := hashtag(hash)
			if !fingerprintMatch {
				tag ^= 1 << 46
			}
			home := int(hash & uint64(len(s.index.slots)-1))
			s.index.insert(home, tag|segment<<26|off)
			checkGetMissing(t, db, key)
		})
	}
}

func TestDBGetBytesRejectScalarReads(t *testing.T) {
	db := newInternalDB(t)
	key := []byte("invalid")
	for _, size := range []int{0, 1, 7, 9, 2048} {
		if err := db.Set(key, make([]byte, size)); err != nil {
			t.Fatal(err)
		}
		checkScalarGet(t, db, key, int64(0), ErrTypeMismatch)
		checkScalarGet(t, db, key, uint64(0), ErrTypeMismatch)
		checkScalarGet(t, db, key, float64(0), ErrTypeMismatch)
	}
	for _, encoded := range [][]byte{nil, {0, 0}, {2}, {255}} {
		if err := db.Set(key, encoded); err != nil {
			t.Fatal(err)
		}
		checkScalarGet(t, db, key, false, ErrTypeMismatch)
	}
	// An expired record reports ErrNotFound before scalar validation.
	if err := db.Set(key, []byte("invalid-scalar"), WithExpiration(time.Now().Add(-time.Second))); err != nil {
		t.Fatal(err)
	}
	checkGetMissing(t, db, key)
}

func TestDBGetScalarRecordSizes(t *testing.T) {
	db := newInternalDB(t)
	for _, size := range []int{0, 1, 995, 996, 4096} {
		t.Run(fmt.Sprintf("%d-byte-key", size), func(t *testing.T) {
			key := bytes.Repeat([]byte{'k'}, size)
			// A 995-byte key puts the end of this record exactly at 1 KiB.
			if err := db.Set(key, int64(42)); err != nil {
				t.Fatal(err)
			}
			checkScalarGet(t, db, key, int64(42), nil)
		})
	}
}

func TestIndexFindScalarCollisions(t *testing.T) {
	log, idx := openRecoveryLog(t, t.TempDir())
	const hash = uint64(0x123456789abcdef)
	home := int(hash & uint64(len(idx.slots)-1))
	for _, entry := range []struct{ key, value []byte }{
		{[]byte("decoy"), nil},
		{bytes.Repeat([]byte{'k'}, 144), []byte{1}},
		{[]byte("large-value"), make([]byte, 2048)},
		{bytes.Repeat([]byte{'k'}, 1100), []byte{2}},
		{[]byte("target"), []byte{42, 0, 0, 0, 0, 0, 0, 0}},
	} {
		segment, off, err := log.write(typeBytes, entry.key, entry.value, 0)
		if err != nil {
			t.Fatal(err)
		}
		idx.insert(home, hashtag(hash)|segment<<26|off)
	}
	for _, tc := range []struct {
		key   string
		want  scalarValue
		found bool
	}{{"target", scalarValue{bits: 42, size: 8, ty: typeBytes}, true}, {"missing", scalarValue{}, false}} {
		pos, got, _, err := idx.findScalar(log, []byte(tc.key), hash)
		if err != nil || (pos >= 0) != tc.found || got != tc.want {
			t.Fatalf("key %q: position %d, got %v, want %v, error %v", tc.key, pos, got, tc.want, err)
		}
	}
}

func TestDBGetScalarRejectsCorruption(t *testing.T) {
	for _, size := range []int{3, 1100} {
		t.Run(fmt.Sprintf("%d-byte-key", size), func(t *testing.T) {
			db := newInternalDB(t)
			key := bytes.Repeat([]byte{'k'}, size)
			if err := db.Set(key, int64(42)); err != nil {
				t.Fatal(err)
			}
			s, _ := db.shard(key)
			path := s.log.headFile.Name()
			record, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, off := range []int{0, 4, 13, logHeaderSize, logHeaderSize + len(key)} {
				corrupt := bytes.Clone(record)
				corrupt[off] ^= 1
				if err := os.WriteFile(path, corrupt, 0600); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Get[int64](key); err == nil {
					t.Fatalf("accepted corrupt byte at %d", off)
				}
			}
			for _, size := range []int{logHeaderSize - 1, len(record) - 1} {
				if err := os.WriteFile(path, record[:size], 0600); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Get[int64](key); err == nil {
					t.Fatalf("accepted truncated record of %d bytes", size)
				}
			}
		})
	}
}

func TestDBGetScalarSealedSegment(t *testing.T) {
	for _, key := range [][]byte{[]byte("small"), bytes.Repeat([]byte{'k'}, 1100)} {
		t.Run(fmt.Sprintf("%d-byte-key", len(key)), func(t *testing.T) {
			db := newInternalDB(t)
			if err := db.Set(key, int64(42)); err != nil {
				t.Fatal(err)
			}
			s, _ := db.shard(key)
			padLog(t, s.log, logSegmentSize)
			next := keysForShard(db, s, "next", 1)[0]
			if err := db.Set(next, int64(43)); err != nil {
				t.Fatal(err)
			}
			files := openFilesUnder(t, db.path)
			checkScalarGet(t, db, key, int64(42), nil)
			checkScalarGet(t, db, next, int64(43), nil)
			if got := openFilesUnder(t, db.path); got != files {
				t.Fatalf("open files: got %d, want %d", got, files)
			}
		})
	}
}
