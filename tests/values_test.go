package dot_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/dotkv/dot"
)

// Each case carries an independent oracle for the public typed and Scan APIs.
// The same cases drive the contract tests and mixed-operation workloads.
type valueCase struct {
	name, kind string
	value      any
	raw        []byte
	set        func(*dot.DB, string, ...dot.Option) error
	check      func(*testing.T, *dot.DB, string)
	missing    func(*testing.T, *dot.DB, string)
	access     func(*testing.T, *dot.DB, string, string)
	update     func(*testing.T, *dot.DB, string, *valueCase, ...dot.Option)
	abort      func(*testing.T, *dot.DB, string)
}

func sameValue(got, want any) bool {
	switch v := want.(type) {
	case float64:
		return math.Float64bits(got.(float64)) == math.Float64bits(v)
	case float32:
		return math.Float32bits(got.(float32)) == math.Float32bits(v)
	case []byte:
		return bytes.Equal(got.([]byte), v)
	default:
		return reflect.DeepEqual(got, want)
	}
}

func scanBytes(v any) []byte {
	switch x := v.(type) {
	case []byte:
		return bytes.Clone(x)
	case string:
		return []byte(x)
	case bool:
		return nil // Boolean values live entirely in the type tag.
	case int:
		v = int64(x)
	case uint:
		v = uint64(x)
	case time.Duration:
		v = int64(x)
	}
	var out bytes.Buffer
	if err := binary.Write(&out, binary.LittleEndian, v); err != nil {
		panic(err)
	}
	return out.Bytes()
}

func typedValue[T dot.Value](name string, want T) valueCase {
	kind := reflect.TypeOf(want).String()
	return valueCase{
		name: name, kind: kind, value: want, raw: scanBytes(want),
		set: func(db *dot.DB, key string, opts ...dot.Option) error {
			return db.Set(key, want, opts...)
		},
		check: func(t *testing.T, db *dot.DB, key string) {
			t.Helper()
			got, err := db.Get[T](key)
			if err != nil || !sameValue(got, want) {
				t.Fatalf("Get[%s](%q) = %v, %v; want %v", kind, key, got, err, want)
			}
			if b, ok := any(got).([]byte); ok && b == nil {
				t.Fatal("stored empty bytes must be distinct from a missing value")
			}
		},
		missing: func(t *testing.T, db *dot.DB, key string) {
			t.Helper()
			var zero T
			if got, err := db.Get[T](key); err != dot.ErrNotFound || !reflect.DeepEqual(got, zero) {
				t.Fatalf("missing Get[%s](%q) = %v, %v; want zero and ErrNotFound", kind, key, got, err)
			}
		},
		access: func(t *testing.T, db *dot.DB, key, stored string) {
			t.Helper()
			var wantErr error
			if stored != kind {
				wantErr = dot.ErrTypeMismatch
			}
			got, err := db.Get[T](key)
			var zero T
			if !errors.Is(err, wantErr) || (wantErr != nil && !reflect.DeepEqual(got, zero)) {
				t.Fatalf("Get[%s] of %s = %v, %v; want %v", kind, stored, got, err, wantErr)
			}
			calls := 0
			err = db.Update(key, func(v T, exists bool) (T, error) {
				calls++
				if !exists {
					t.Fatal("existing value reported as absent")
				}
				return v, nil
			})
			if !errors.Is(err, wantErr) || calls != boolInt(wantErr == nil) {
				t.Fatalf("Update[%s] of %s: calls=%d error=%v; want %v", kind, stored, calls, err, wantErr)
			}
		},
		update: func(t *testing.T, db *dot.DB, key string, previous *valueCase, opts ...dot.Option) {
			t.Helper()
			calls := 0
			err := db.Update(key, func(v T, exists bool) (T, error) {
				calls++
				var zero T
				expected := any(zero)
				if previous != nil {
					expected = previous.value
				}
				if exists != (previous != nil) || !sameValue(v, expected) {
					t.Fatalf("Update[%s](%q): value=%v exists=%t; want %v exists=%t", kind, key, v, exists, expected, previous != nil)
				}
				return want, nil
			}, opts...)
			if err != nil || calls != 1 {
				t.Fatalf("Update[%s](%q): calls=%d error=%v", kind, key, calls, err)
			}
		},
		abort: func(t *testing.T, db *dot.DB, key string) {
			t.Helper()
			sentinel := errors.New("abort")
			calls := 0
			err := db.Update(key, func(v T, exists bool) (T, error) {
				calls++
				if !exists || !sameValue(v, want) {
					t.Fatalf("aborted Update[%s]: value=%v exists=%t", kind, v, exists)
				}
				if b, ok := any(v).([]byte); ok {
					clear(b)
				}
				return want, sentinel
			})
			if err != sentinel || calls != 1 {
				t.Fatalf("aborted Update[%s]: calls=%d error=%v", kind, calls, err)
			}
		},
	}
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func valueCases() []valueCase {
	return []valueCase{
		typedValue("bytes", []byte("original")), typedValue("empty-bytes", []byte{}),
		typedValue("nil-bytes", []byte(nil)), typedValue("large-bytes", bytes.Repeat([]byte{0, 255, 'x'}, 3000)),
		typedValue("string", "hello\x00world"), typedValue("empty-string", ""),
		typedValue("false", false), typedValue("true", true),
		typedValue("int-min", int(math.MinInt)), typedValue("int-max", int(math.MaxInt)),
		typedValue("int8-min", int8(math.MinInt8)), typedValue("int8-max", int8(math.MaxInt8)),
		typedValue("int16-min", int16(math.MinInt16)), typedValue("int16-max", int16(math.MaxInt16)),
		typedValue("int32-min", int32(math.MinInt32)), typedValue("int32-max", int32(math.MaxInt32)),
		typedValue("int64-min", int64(math.MinInt64)), typedValue("int64-max", int64(math.MaxInt64)),
		typedValue("uint-zero", uint(0)), typedValue("uint-max", uint(math.MaxUint)),
		typedValue("uint8-zero", uint8(0)), typedValue("uint8-max", uint8(math.MaxUint8)),
		typedValue("uint16-zero", uint16(0)), typedValue("uint16-max", uint16(math.MaxUint16)),
		typedValue("uint32-zero", uint32(0)), typedValue("uint32-max", uint32(math.MaxUint32)),
		typedValue("uint64-zero", uint64(0)), typedValue("uint64-max", uint64(math.MaxUint64)),
		typedValue("float32", float32(1.5)), typedValue("float32-nan", math.Float32frombits(0x7fc00042)),
		typedValue("float32-signaling-nan", math.Float32frombits(0x7f800001)),
		typedValue("float32-negative-zero", math.Float32frombits(1<<31)),
		typedValue("float32-inf", float32(math.Inf(1))),
		typedValue("float64", float64(1.5)), typedValue("float64-nan", math.Float64frombits(0x7ff8000000000042)),
		typedValue("float64-negative-zero", math.Float64frombits(1<<63)), typedValue("float64-inf", math.Inf(-1)),
		typedValue("duration-min", time.Duration(math.MinInt64)), typedValue("duration-max", time.Duration(math.MaxInt64)),
		typedValue("duration", time.Hour+time.Nanosecond),
	}
}

func TestValueContracts(t *testing.T) {
	values := valueCases()
	readers := make(map[string]valueCase)
	for _, v := range values {
		readers[v.kind] = v
	}
	for _, mode := range []dot.Durability{dot.Buffer, dot.Write, dot.Sync} {
		t.Run(fmt.Sprintf("durability=%d", mode), func(t *testing.T) {
			path := t.TempDir()
			db, err := dot.Open(path, dot.WithShardCount(1), dot.WithDurability(mode))
			check(t, err)
			t.Cleanup(func() { check(t, db.Close()) })
			for _, size := range []int{0, 3, 995, 1100, 4096} {
				key := string(bytes.Repeat([]byte{'k'}, size))
				for _, v := range values {
					t.Run(fmt.Sprintf("key=%d/%s", size, v.name), func(t *testing.T) {
						check(t, db.Delete(key))
						v.missing(t, db, key)
						v.update(t, db, key, nil)
						v.check(t, db, key)
						for _, reader := range readers {
							reader.access(t, db, key, v.kind)
						}
						previous := readers[v.kind]
						check(t, previous.set(db, key))
						v.update(t, db, key, &previous)
						v.check(t, db, key)
						v.abort(t, db, key)
						v.check(t, db, key)
						found := false
						for k, raw := range db.Scan() {
							if string(k) == key {
								if found || !bytes.Equal(raw, v.raw) {
									t.Fatalf("Scan: duplicate key or value %x; want %x", raw, v.raw)
								}
								found = true
							}
						}
						if !found {
							t.Fatal("Scan omitted the live key")
						}
					})
				}
			}
			check(t, db.Close())
			db, err = dot.Open(path, dot.WithDurability(mode))
			check(t, err)
			for _, size := range []int{0, 3, 995, 1100, 4096} {
				values[len(values)-1].check(t, db, string(bytes.Repeat([]byte{'k'}, size)))
			}
		})
	}
}
