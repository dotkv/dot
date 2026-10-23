package dot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"strconv"
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

func checkIntegerEncodings[T interface {
	int | int8 | int16 | int32 | uint | uint8 | uint16 | uint32
}](t *testing.T, db *DB, values []T, size int) {
	t.Helper()
	key := []byte("integer")
	for _, value := range values {
		encoded := make([]byte, size)
		for i := range encoded {
			encoded[i] = byte(uint64(value) >> (8 * i))
		}
		checkScalarSet(t, db, key, value, encoded)
		checkScalarGet(t, db, key, value, nil)
	}
}

func TestDBSmallScalarEncodings(t *testing.T) {
	for _, durability := range []Durability{Write, Buffer} {
		t.Run(fmt.Sprint(durability), func(t *testing.T) {
			db := newInternalDB(t, WithDurability(durability))
			stopTestMaintenance(db)
			checkIntegerEncodings(t, db, []int{0, 42, -42, math.MinInt, math.MaxInt}, 8)
			checkIntegerEncodings(t, db, []int8{0, 42, -42, math.MinInt8, math.MaxInt8}, 1)
			checkIntegerEncodings(t, db, []int16{0, 0x1234, -42, math.MinInt16, math.MaxInt16}, 2)
			checkIntegerEncodings(t, db, []int32{0, 0x12345678, -42, math.MinInt32, math.MaxInt32}, 4)
			checkIntegerEncodings(t, db, []uint{0, 42, math.MaxInt + 1, math.MaxUint}, 8)
			checkIntegerEncodings(t, db, []uint8{0, 42, math.MaxInt8 + 1, math.MaxUint8}, 1)
			checkIntegerEncodings(t, db, []uint16{0, 0x1234, math.MaxInt16 + 1, math.MaxUint16}, 2)
			checkIntegerEncodings(t, db, []uint32{0, 0x12345678, math.MaxInt32 + 1, math.MaxUint32}, 4)
			key := []byte("float32")
			for _, bits := range []uint32{0, 1, 1 << 31, 0x3fc00000, 0x7f800000, 0xff800000, 0x7fc00042, 0x7f800001} {
				var encoded [4]byte
				binary.LittleEndian.PutUint32(encoded[:], bits)
				checkScalarSet(t, db, key, math.Float32frombits(bits), encoded[:])
				got, err := db.Get[float32](key)
				if err != nil || math.Float32bits(got) != bits {
					t.Fatalf("float32 bits: got %x, want %x, error %v", math.Float32bits(got), bits, err)
				}
			}
			if err := db.Set("literal", 42); err != nil {
				t.Fatal(err)
			}
			checkScalarGet(t, db, []byte("literal"), 42, nil)
		})
	}
}

func checkNativeIntegerRange[T interface{ int | uint }](t *testing.T, db *DB, ty recordType, bits uint64, want T, wantErr error) {
	t.Helper()
	key := fmt.Appendf(nil, "%s-%x", ty, bits)
	s, hash := db.shard(key)
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], bits)
	change, err := s.index.prepareSet(s.log, ty, key, encoded[:], hash, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.appendSet(ty, key, encoded[:], change, Write); err != nil {
		t.Fatal(err)
	}
	checkScalarGet(t, db, key, want, wantErr)
	size := s.log.size
	called := false
	err = db.Update(key, func(v T, exists bool) (T, error) {
		called = true
		if !exists || v != want {
			t.Fatalf("native integer update: got %v, exists=%t, want %v", v, exists, want)
		}
		return v, nil
	})
	if !errors.Is(err, wantErr) || called != (wantErr == nil) || s.log.size != size {
		t.Fatalf("native integer update: callback=%t error=%v, want %v; log size=%d, want %d", called, err, wantErr, s.log.size, size)
	}
}

func TestDBDurationEncodings(t *testing.T) {
	for _, durability := range []Durability{Write, Buffer} {
		for _, keySize := range []int{3, 1100} {
			t.Run(fmt.Sprintf("durability=%d/key=%d", durability, keySize), func(t *testing.T) {
				db := newInternalDB(t, WithDurability(durability))
				stopTestMaintenance(db)
				key := bytes.Repeat([]byte{'k'}, keySize)
				for _, value := range []time.Duration{0, time.Nanosecond, -time.Nanosecond, time.Hour + time.Nanosecond, math.MinInt64, math.MaxInt64} {
					var encoded [8]byte
					binary.LittleEndian.PutUint64(encoded[:], uint64(value))
					checkScalarSet(t, db, key, value, encoded[:])
					checkScalarGet(t, db, key, value, nil)
				}
			})
		}
	}
}

func TestDBNativeIntegerRange(t *testing.T) {
	db := newInternalDB(t)
	stopTestMaintenance(db)
	for _, v := range []int64{math.MinInt64, math.MinInt32 - 1, math.MinInt32, math.MaxInt32, math.MaxInt32 + 1, math.MaxInt64} {
		want, wantErr := int(v), error(nil)
		if strconv.IntSize == 32 && (v < math.MinInt32 || v > math.MaxInt32) {
			want, wantErr = 0, ErrValueOverflow
		}
		checkNativeIntegerRange(t, db, typeInt, uint64(v), want, wantErr)
	}
	for _, v := range []uint64{0, math.MaxUint32, math.MaxUint32 + 1, math.MaxUint64} {
		want, wantErr := uint(v), error(nil)
		if strconv.IntSize == 32 && v > math.MaxUint32 {
			want, wantErr = 0, ErrValueOverflow
		}
		checkNativeIntegerRange(t, db, typeUint, v, want, wantErr)
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
		db.Set(key, time.Second),
	} {
		if !errors.Is(err, os.ErrClosed) {
			t.Fatalf("Set after Close: got %v, want os.ErrClosed", err)
		}
	}
}

func checkGetMissing(t *testing.T, db *DB, key []byte) {
	t.Helper()
	checkScalarGet(t, db, key, int(0), ErrNotFound)
	checkScalarGet(t, db, key, int8(0), ErrNotFound)
	checkScalarGet(t, db, key, int16(0), ErrNotFound)
	checkScalarGet(t, db, key, int32(0), ErrNotFound)
	checkScalarGet(t, db, key, int64(0), ErrNotFound)
	checkScalarGet(t, db, key, uint(0), ErrNotFound)
	checkScalarGet(t, db, key, uint8(0), ErrNotFound)
	checkScalarGet(t, db, key, uint16(0), ErrNotFound)
	checkScalarGet(t, db, key, uint32(0), ErrNotFound)
	checkScalarGet(t, db, key, uint64(0), ErrNotFound)
	checkScalarGet(t, db, key, float32(0), ErrNotFound)
	checkScalarGet(t, db, key, float64(0), ErrNotFound)
	checkScalarGet(t, db, key, time.Duration(0), ErrNotFound)
	checkScalarGet(t, db, key, false, ErrNotFound)
	if got, err := db.Get[[]byte](key); err != ErrNotFound || got != nil {
		t.Fatalf("Get[[]byte]: got %v, error %v; want nil and the direct ErrNotFound sentinel", got, err)
	}
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
				tag ^= 1 << addressBits
			}
			home := int(hash & uint64(len(s.index.slots)-1))
			s.index.insert(home, tag|logAddress(segment, off))
			checkGetMissing(t, db, key)
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
		idx.insert(home, hashtag(hash)|logAddress(segment, off))
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
			for _, size := range []int{logHeaderSize - 1, logHeaderSize + len(key) + 8 - 1} {
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

type typedRecordCase struct {
	name    string
	logical string
	ty      recordType
	payload int
	set     func(*DB, []byte, ...Option) error
	noop    func(*DB, []byte) error
	check   func(*testing.T, *DB, []byte)
}

func typedCase[T Value](name, logical string, ty recordType, payload int, value T) typedRecordCase {
	return typedRecordCase{
		name: name, logical: logical, ty: ty, payload: payload,
		set: func(db *DB, key []byte, opts ...Option) error { return db.Set(key, value, opts...) },
		noop: func(db *DB, key []byte) error {
			return db.Update(key, func(v T, _ bool) (T, error) { return v, nil })
		},
		check: func(t *testing.T, db *DB, key []byte) {
			t.Helper()
			got, err := db.Get[T](key)
			equal := reflect.DeepEqual(got, value)
			if want, ok := any(value).(float64); ok {
				equal = math.Float64bits(any(got).(float64)) == math.Float64bits(want)
			}
			if want, ok := any(value).(float32); ok {
				equal = math.Float32bits(any(got).(float32)) == math.Float32bits(want)
			}
			if err != nil || !equal {
				t.Fatalf("Get[%s]: got %v, want %v, error %v", logical, got, value, err)
			}
		},
	}
}

func typedCases() []typedRecordCase {
	return []typedRecordCase{
		typedCase("bytes", "[]byte", typeBytes, 8, make([]byte, 8)),
		typedCase("int64", "int64", typeInt64, 8, int64(0)),
		typedCase("uint64", "uint64", typeUint64, 8, uint64(0)),
		typedCase("float64", "float64", typeFloat64, 8, float64(0)),
		typedCase("empty-bytes", "[]byte", typeBytes, 0, []byte{}),
		typedCase("false", "bool", typeBoolFalse, 0, false),
		typedCase("true", "bool", typeBoolTrue, 0, true),
		typedCase("string", "string", typeString, 0, ""),
		typedCase("int", "int", typeInt, 8, int(0)),
		typedCase("duration", "time.Duration", typeDuration, 8, time.Duration(0)),
		typedCase("int8", "int8", typeInt8, 1, int8(0)),
		typedCase("int16", "int16", typeInt16, 2, int16(0)),
		typedCase("int32", "int32", typeInt32, 4, int32(0)),
		typedCase("uint", "uint", typeUint, 8, uint(0)),
		typedCase("uint8", "uint8", typeUint8, 1, uint8(0)),
		typedCase("uint16", "uint16", typeUint16, 2, uint16(0)),
		typedCase("uint32", "uint32", typeUint32, 4, uint32(0)),
		typedCase("float32", "float32", typeFloat32, 4, float32(0)),
	}
}

func TestDBSetChangesTypeWithIdenticalPayload(t *testing.T) {
	db := newInternalDB(t)
	stopTestMaintenance(db)
	key := []byte("replacement")
	s, hash := db.shard(key)
	cases := typedCases()
	cases = append(cases, cases[5], cases[4]) // true -> false -> empty bytes
	for _, tc := range cases {
		before := s.log.size
		if err := tc.set(db, key); err != nil {
			t.Fatal(err)
		}
		if want := before + recordSize(len(key), tc.payload); s.log.size != want {
			t.Fatalf("Set %s: log size %d, want %d", tc.name, s.log.size, want)
		}
		_, payload, ty, _, err := s.index.findPayload(s.log, key, hash, nil)
		if err != nil || ty != tc.ty || len(payload) != tc.payload {
			t.Fatalf("Set %s: tag=%d payload=%x error=%v", tc.name, ty, payload, err)
		}
		tc.check(t, db, key)
		before = s.log.size
		if err := tc.set(db, key); err != nil || s.log.size != before {
			t.Fatalf("unchanged Set %s appended: %v", tc.name, err)
		}
	}
}

// Public value tests check identity updates semantically. This test protects
// the storage optimization, including NaN payloads and signed floating zero.
func TestDBUpdateNoOpDoesNotAppend(t *testing.T) {
	db := newInternalDB(t, WithShardCount(1))
	stopTestMaintenance(db)
	cases := append(typedCases(),
		typedCase("nan", "float64", typeFloat64, 8, math.Float64frombits(0x7ff8000000000042)),
		typedCase("negative-zero", "float64", typeFloat64, 8, math.Float64frombits(1<<63)),
		typedCase("nan32", "float32", typeFloat32, 4, math.Float32frombits(0x7f800001)),
		typedCase("negative-zero32", "float32", typeFloat32, 4, math.Float32frombits(1<<31)),
	)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key := []byte("identity")
			if err := tc.set(db, key); err != nil {
				t.Fatal(err)
			}
			s := &db.shards[0]
			size, count := s.log.size, s.log.recordCount
			if err := tc.noop(db, key); err != nil || s.log.size != size || s.log.recordCount != count {
				t.Fatalf("identity Update appended a record: %v", err)
			}
			tc.check(t, db, key)
		})
	}
}

func TestDBBoolUpdates(t *testing.T) {
	db := newInternalDB(t)
	stopTestMaintenance(db)
	key := []byte("bool")
	if err := db.Set(key, false); err != nil {
		t.Fatal(err)
	}
	s, _ := db.shard(key)
	for _, want := range []bool{true, false, true, false} {
		before := s.log.size
		if err := db.Update(key, func(v bool, found bool) (bool, error) { return !v, nil }); err != nil {
			t.Fatal(err)
		}
		checkScalarGet(t, db, key, want, nil)
		if s.log.size != before+recordSize(len(key), 0) {
			t.Fatal("boolean update did not append exactly one payload-free record")
		}
		before = s.log.size
		if err := db.Update(key, func(v bool, found bool) (bool, error) { return v, nil }); err != nil || s.log.size != before {
			t.Fatalf("unchanged boolean update appended: %v", err)
		}
	}
}

func TestDBTypedCompactionAndRecovery(t *testing.T) {
	db := newInternalDB(t)
	stopTestMaintenance(db)
	s := &db.shards[0]
	cases := append(typedCases(),
		typedCase("negative", "int64", typeInt64, 8, int64(math.MinInt64)),
		typedCase("max-uint", "uint64", typeUint64, 8, uint64(math.MaxUint64)),
		typedCase("nan", "float64", typeFloat64, 8, math.Float64frombits(0x7ff8000000000042)),
		typedCase("negative-zero", "float64", typeFloat64, 8, math.Float64frombits(1<<63)),
		typedCase("min-int", "int", typeInt, 8, int(math.MinInt)),
		typedCase("min-int8", "int8", typeInt8, 1, int8(math.MinInt8)),
		typedCase("min-int16", "int16", typeInt16, 2, int16(math.MinInt16)),
		typedCase("min-int32", "int32", typeInt32, 4, int32(math.MinInt32)),
		typedCase("max-native-uint", "uint", typeUint, 8, uint(math.MaxUint)),
		typedCase("max-uint8", "uint8", typeUint8, 1, uint8(math.MaxUint8)),
		typedCase("max-uint16", "uint16", typeUint16, 2, uint16(math.MaxUint16)),
		typedCase("max-uint32", "uint32", typeUint32, 4, uint32(math.MaxUint32)),
		typedCase("nan32", "float32", typeFloat32, 4, math.Float32frombits(0x7fc00042)),
		typedCase("negative-zero32", "float32", typeFloat32, 4, math.Float32frombits(1<<31)),
		typedCase("positive-duration", "time.Duration", typeDuration, 8, time.Hour+time.Nanosecond),
		typedCase("negative-duration", "time.Duration", typeDuration, 8, -time.Hour-time.Nanosecond),
		typedCase("min-duration", "time.Duration", typeDuration, 8, time.Duration(math.MinInt64)),
		typedCase("max-duration", "time.Duration", typeDuration, 8, time.Duration(math.MaxInt64)),
	)
	keys := keysForShard(db, s, "typed", len(cases))
	expiration := time.Now().Add(time.Hour)
	for _, key := range keys {
		if err := db.Set(key, []byte("old")); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.log.openHead(1); err != nil {
		t.Fatal(err)
	}
	for i, tc := range cases {
		if err := tc.set(db, keys[i], WithExpiration(expiration)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.log.openHead(2); err != nil {
		t.Fatal(err)
	}
	if err := s.compact(); err != nil {
		t.Fatal(err)
	}
	if s.log.recordCount != uint64(len(cases)) || len(s.log.segmentRecordCounts) != 1 {
		t.Fatal("compaction did not pack the typed records")
	}
	check := func() {
		t.Helper()
		s := &db.shards[0]
		for i, tc := range cases {
			tc.check(t, db, keys[i])
			_, hash := db.shard(keys[i])
			_, payload, ty, exp, err := s.index.findPayload(s.log, keys[i], hash, nil)
			if err != nil || ty != tc.ty || len(payload) != tc.payload || exp != expiration.UnixNano() {
				t.Fatalf("%s: tag=%d bytes=%d expiration=%d error=%v", tc.name, ty, len(payload), exp, err)
			}
		}
		checkSegmentAccounting(t, s)
	}
	check()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	db, err = Open(db.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	stopTestMaintenance(db)
	check()
	for _, key := range keys {
		if err := db.Delete(key); err != nil {
			t.Fatal(err)
		}
		checkGetMissing(t, db, key)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(db.path)
	if err != nil {
		t.Fatal(err)
	}
	stopTestMaintenance(db)
	for _, key := range keys {
		checkGetMissing(t, db, key)
	}
}

func TestDBTypeChecksAfterKeyMatch(t *testing.T) {
	db := newInternalDB(t)
	stopTestMaintenance(db)
	key := []byte("target")
	s, hash := db.shard(key)
	for _, candidate := range []struct {
		ty         recordType
		key, value []byte
	}{
		{typeBoolTrue, []byte("decoy"), nil},
		{typeBytes, bytes.Repeat([]byte{'k'}, 1100), make([]byte, 2048)},
		{typeInt64, key, []byte{42, 0, 0, 0, 0, 0, 0, 0}},
	} {
		segment, off, err := s.log.write(candidate.ty, candidate.key, candidate.value, 0)
		if err != nil {
			t.Fatal(err)
		}
		s.index.insert(int(hash&uint64(len(s.index.slots)-1)), hashtag(hash)|logAddress(segment, off))
	}
	checkScalarGet(t, db, key, int64(42), nil)
	if err := db.Update(key, func(v int64, _ bool) (int64, error) { return v + 1, nil }); err != nil {
		t.Fatal(err)
	}
	checkScalarGet(t, db, key, int64(43), nil)
}

func TestDBUpdatePreservesExpiration(t *testing.T) {
	db := newInternalDB(t, WithExpiration(time.Now().Add(-time.Hour)))
	key := []byte("key")
	for _, expiration := range []time.Time{{}, time.Now().Add(time.Hour)} {
		if err := db.Set(key, uint64(1), WithExpiration(expiration)); err != nil {
			t.Fatal(err)
		}
		if err := db.Update(key, func(v uint64, exists bool) (uint64, error) { return v + 1, nil }); err != nil {
			t.Fatal(err)
		}
		s, hash := db.shard(key)
		_, _, gotExp, err := s.index.findScalar(s.log, key, hash)
		wantExp := int64(0)
		if !expiration.IsZero() {
			wantExp = expiration.UnixNano()
		}
		if err != nil || gotExp != wantExp {
			t.Fatalf("expiration: got %d, want %d, error %v", gotExp, wantExp, err)
		}
	}
}

func TestDBUpdateWriteFailure(t *testing.T) {
	db := newInternalDB(t)
	key := []byte("key")
	if err := db.Set(key, []byte("original")); err != nil {
		t.Fatal(err)
	}
	s, _ := db.shard(key)
	if err := s.log.headFile.Truncate(logSegmentSize); err != nil {
		t.Fatal(err)
	}
	s.log.size = logSegmentSize
	blocker := s.log.segmentPath(s.log.headSegment + 1)
	if err := os.Mkdir(blocker, 0700); err != nil {
		t.Fatal(err)
	}
	calls := 0
	fn := func(v []byte, exists bool) ([]byte, error) {
		calls++
		v[0] = 'O'
		return v, nil
	}
	if err := db.Update(key, fn); err == nil {
		t.Fatal("expected log rotation failure")
	}
	if got, err := db.Get[[]byte](key); err != nil || string(got) != "original" {
		t.Fatalf("failed append changed value: %q, %v", got, err)
	}
	if calls != 1 {
		t.Fatalf("failed write invoked callback %d times", calls)
	}
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(key, fn); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get[[]byte](key); err != nil || string(got) != "Original" {
		t.Fatalf("update after rotation: %q, %v", got, err)
	}
}
