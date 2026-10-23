package dot

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

type typedRecordCase struct {
	name    string
	logical string
	ty      recordType
	payload int
	set     func(*DB, []byte, ...Option) error
	check   func(*testing.T, *DB, []byte)
}

func typedCase[T Value](name, logical string, ty recordType, payload int, value T) typedRecordCase {
	return typedRecordCase{
		name: name, logical: logical, ty: ty, payload: payload,
		set: func(db *DB, key []byte, opts ...Option) error { return db.Set(key, value, opts...) },
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

func checkTypeAccess[T Value](t *testing.T, db *DB, key []byte, stored, requested string) {
	t.Helper()
	var wantErr error
	if stored != requested {
		wantErr = ErrTypeMismatch
	}
	got, err := db.Get[T](key)
	if !errors.Is(err, wantErr) {
		t.Fatalf("Get[%s] of %s: %v, want %v", requested, stored, err, wantErr)
	}
	if wantErr != nil {
		var zero T
		if !reflect.DeepEqual(got, zero) || !strings.Contains(err.Error(), "stored "+stored+", requested "+requested) {
			t.Fatalf("Get[%s] of %s: got %v, error %v", requested, stored, got, err)
		}
	}
	called := false
	err = db.Update(key, func(v T, _ bool) (T, error) { called = true; return v, nil })
	if !errors.Is(err, wantErr) || called != (wantErr == nil) {
		t.Fatalf("Update[%s] of %s: callback=%t error=%v, want %v", requested, stored, called, err, wantErr)
	}
}

func TestDBTypeChecks(t *testing.T) {
	db := newInternalDB(t)
	stopTestMaintenance(db)
	for _, tc := range typedCases() {
		t.Run(tc.name, func(t *testing.T) {
			key := []byte(tc.name)
			if err := tc.set(db, key); err != nil {
				t.Fatal(err)
			}
			s, hash := db.shard(key)
			pos, _, _, _, err := s.index.findPayload(s.log, key, hash, nil)
			if err != nil || pos < 0 {
				t.Fatalf("lookup: pos=%d, error=%v", pos, err)
			}
			slot, size, count := s.index.slots[pos], s.log.size, s.log.recordCount
			checkTypeAccess[[]byte](t, db, key, tc.logical, "[]byte")
			checkTypeAccess[string](t, db, key, tc.logical, "string")
			checkTypeAccess[int](t, db, key, tc.logical, "int")
			checkTypeAccess[int8](t, db, key, tc.logical, "int8")
			checkTypeAccess[int16](t, db, key, tc.logical, "int16")
			checkTypeAccess[int32](t, db, key, tc.logical, "int32")
			checkTypeAccess[int64](t, db, key, tc.logical, "int64")
			checkTypeAccess[uint](t, db, key, tc.logical, "uint")
			checkTypeAccess[uint8](t, db, key, tc.logical, "uint8")
			checkTypeAccess[uint16](t, db, key, tc.logical, "uint16")
			checkTypeAccess[uint32](t, db, key, tc.logical, "uint32")
			checkTypeAccess[uint64](t, db, key, tc.logical, "uint64")
			checkTypeAccess[float32](t, db, key, tc.logical, "float32")
			checkTypeAccess[float64](t, db, key, tc.logical, "float64")
			checkTypeAccess[time.Duration](t, db, key, tc.logical, "time.Duration")
			checkTypeAccess[bool](t, db, key, tc.logical, "bool")
			if s.log.size != size || s.log.recordCount != count || s.index.slots[pos] != slot {
				t.Fatal("type checks or identity updates changed the stored record")
			}
			tc.check(t, db, key)
		})
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

func TestDBExpiredTypesBeforeMismatch(t *testing.T) {
	db := newInternalDB(t)
	stopTestMaintenance(db)
	key := []byte("expired")
	for _, tc := range typedCases() {
		if err := tc.set(db, key, WithExpiration(time.Now().Add(-time.Hour))); err != nil {
			t.Fatal(err)
		}
		checkGetMissing(t, db, key)
		called := false
		if err := db.Update(key, func(v []byte, exists bool) ([]byte, error) {
			called = true
			if exists || v != nil {
				t.Fatalf("expired %s: value=%v exists=%t, want nil and false", tc.name, v, exists)
			}
			return []byte("replacement"), nil
		}); err != nil || !called {
			t.Fatalf("expired %s byte update: callback=%t error=%v", tc.name, called, err)
		}
		if got, err := db.Get[[]byte](key); err != nil || !bytes.Equal(got, []byte("replacement")) {
			t.Fatalf("expired %s byte replacement: value=%q error=%v", tc.name, got, err)
		}
		if err := tc.set(db, key, WithExpiration(time.Now().Add(-time.Hour))); err != nil {
			t.Fatal(err)
		}
		called = false
		if err := db.Update(key, func(v bool, exists bool) (bool, error) {
			called = true
			if exists || v {
				t.Fatalf("expired %s: value=%t exists=%t, want false and false", tc.name, v, exists)
			}
			return true, nil
		}); err != nil || !called {
			t.Fatalf("expired %s boolean update: callback=%t error=%v", tc.name, called, err)
		}
		checkScalarGet(t, db, key, true, nil)
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
