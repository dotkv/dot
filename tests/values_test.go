package dot_test

import (
	"bytes"
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
	set        func(*dot.DB, string, ...dot.Option) error
	check      func(*testing.T, *dot.DB, string)
	missing    func(*testing.T, *dot.DB, string)
	access     func(*testing.T, *dot.DB, string, string)
	update     func(*testing.T, *dot.DB, string, *valueCase, ...dot.Option)
	abort      func(*testing.T, *dot.DB, string)
	condition  func(func(any, bool) bool) dot.Option
}

func sameValue(got, want any) bool {
	if reflect.TypeOf(got) != reflect.TypeOf(want) {
		return false
	}
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

func typedValue[T dot.Value](name string, want T) valueCase {
	kind := reflect.TypeOf(want).String()
	return valueCase{
		name: name, kind: kind, value: want,
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
		condition: func(fn func(any, bool) bool) dot.Option {
			return dot.WithCondition(func(v T, exists bool) bool { return fn(v, exists) })
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
						for entry, err := range db.Scan() {
							check(t, err)
							if string(entry.Key) == key {
								if found || !sameValue(entry.Value, v.value) {
									t.Fatalf("Scan: duplicate key or value %T(%v); want %T(%v)", entry.Value, entry.Value, v.value, v.value)
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

func TestConditionValues(t *testing.T) {
	db := newDB(t, dot.WithShardCount(1))
	for _, v := range valueCases() {
		t.Run(v.name, func(t *testing.T) {
			check(t, v.set(db, v.name))
			calls := 0
			err := db.Delete(v.name, v.condition(func(got any, exists bool) bool {
				calls++
				if !exists || !sameValue(got, v.value) {
					t.Fatalf("predicate decoded %v, exists=%t; want %v", got, exists, v.value)
				}
				return false
			}))
			if err != dot.ErrConditionFailed || calls != 1 {
				t.Fatalf("typed condition: %v, calls=%d", err, calls)
			}
			v.check(t, db, v.name)
			check(t, db.Delete(v.name, v.condition(func(got any, exists bool) bool {
				return exists && sameValue(got, v.value)
			})))
			for _, expired := range []bool{false, true} {
				if expired {
					check(t, v.set(db, v.name, dot.WithTTL(-time.Hour)))
				}
				calls := 0
				check(t, db.Delete(v.name, v.condition(func(got any, exists bool) bool {
					calls++
					zero := reflect.Zero(reflect.TypeOf(v.value)).Interface()
					if exists || !reflect.DeepEqual(got, zero) {
						t.Fatalf("absent predicate decoded %v, exists=%t; want %v", got, exists, zero)
					}
					return true
				})))
				if calls != 1 {
					t.Fatalf("absent predicate ran %d times", calls)
				}
			}
		})
	}
}

func TestArithmeticIntegers(t *testing.T) {
	for _, mode := range []dot.Durability{dot.Buffer, dot.Write, dot.Sync} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			db := newDB(t, dot.WithShardCount(1), dot.WithDurability(mode))
			checkIntegerArithmetic(t, db, int(math.MinInt), int(math.MaxInt))
			checkIntegerArithmetic(t, db, int8(math.MinInt8), int8(math.MaxInt8))
			checkIntegerArithmetic(t, db, int16(math.MinInt16), int16(math.MaxInt16))
			checkIntegerArithmetic(t, db, int32(math.MinInt32), int32(math.MaxInt32))
			checkIntegerArithmetic(t, db, int64(math.MinInt64), int64(math.MaxInt64))
			checkIntegerArithmetic(t, db, uint(0), uint(math.MaxUint))
			checkIntegerArithmetic(t, db, uint8(0), uint8(math.MaxUint8))
			checkIntegerArithmetic(t, db, uint16(0), uint16(math.MaxUint16))
			checkIntegerArithmetic(t, db, uint32(0), uint32(math.MaxUint32))
			checkIntegerArithmetic(t, db, uint64(0), uint64(math.MaxUint64))
			checkIntegerArithmetic(t, db, time.Duration(math.MinInt64), time.Duration(math.MaxInt64))
		})
	}
}

func checkIntegerArithmetic[T dot.Number](t *testing.T, db *dot.DB, minimum, maximum T) {
	t.Helper()
	t.Run(fmt.Sprintf("%T", minimum), func(t *testing.T) {
		check(t, db.Delete("counter"))
		if got, err := db.Add("counter", T(2)); err != nil || got != 2 {
			t.Fatalf("Add missing: %v, %v; want 2", got, err)
		}
		if got, err := db.Sub([]byte("counter"), T(1)); err != nil || got != 1 {
			t.Fatalf("Sub existing: %v, %v; want 1", got, err)
		}
		cases := []struct {
			initial, delta, want T
			subtract, overflow   bool
		}{
			{maximum, 0, maximum, false, false},
			{minimum, 0, minimum, true, false},
			{maximum, 1, 0, false, true},
			{minimum, 1, 0, true, true},
			{minimum, 1, minimum + 1, false, false},
			{maximum, 1, maximum - 1, true, false},
			{maximum, maximum, 0, true, false},
			{minimum, minimum, 0, true, false},
		}
		if minimum < 0 {
			negativeOne := minimum - minimum - 1
			cases = append(cases, []struct {
				initial, delta, want T
				subtract, overflow   bool
			}{
				{minimum, negativeOne, 0, false, true},
				{maximum, negativeOne, 0, true, true},
				{0, minimum, 0, true, true},
				{minimum, maximum, negativeOne, false, false},
				{maximum, minimum, negativeOne, false, false},
				{minimum, negativeOne, minimum + 1, true, false},
			}...)
		}
		for _, tc := range cases {
			check(t, db.Set("counter", tc.initial))
			var got T
			var err error
			if tc.subtract {
				got, err = db.Sub("counter", tc.delta)
			} else {
				got, err = db.Add("counter", tc.delta)
			}
			var wantErr error
			stored := tc.want
			if tc.overflow {
				wantErr, stored = dot.ErrValueOverflow, tc.initial
			}
			if !errors.Is(err, wantErr) || got != tc.want {
				t.Fatalf("initial=%v delta=%v subtract=%v: %v, %v; want %v, %v", tc.initial, tc.delta, tc.subtract, got, err, tc.want, wantErr)
			}
			if got, err := db.Get[T]("counter"); err != nil || got != stored {
				t.Fatalf("stored value=%v, %v; want %v", got, err, stored)
			}
		}
		check(t, db.Delete("counter"))
		got, err := db.Sub("counter", T(1))
		if minimum < 0 {
			if err != nil || got != minimum-minimum-1 {
				t.Fatalf("Sub missing signed: %v, %v; want -1", got, err)
			}
		} else {
			if !errors.Is(err, dot.ErrValueOverflow) || got != 0 {
				t.Fatalf("Sub missing unsigned: %v, %v; want zero and ErrValueOverflow", got, err)
			}
			if _, err := db.Get[T]("counter"); err != dot.ErrNotFound {
				t.Fatalf("underflow inserted a missing key: %v", err)
			}
		}
	})
}

func TestArithmeticFloats(t *testing.T) {
	db := newDB(t, dot.WithShardCount(1))
	checkFloatArithmetic(t, db, float32(math.MaxFloat32))
	checkFloatArithmetic(t, db, float64(math.MaxFloat64))
}

func checkFloatArithmetic[T interface{ float32 | float64 }](t *testing.T, db *dot.DB, maximum T) {
	t.Helper()
	t.Run(fmt.Sprintf("%T", maximum), func(t *testing.T) {
		infinity, nan, negativeZero := T(math.Inf(1)), T(math.NaN()), T(math.Copysign(0, -1))
		for _, tc := range []struct{ initial, delta, add, sub T }{
			{1.5, 0.25, 1.75, 1.25},
			{maximum, maximum, infinity, 0},
			{infinity, infinity, infinity, nan},
			{nan, 1, nan, nan},
			{negativeZero, negativeZero, negativeZero, 0},
		} {
			for _, subtract := range []bool{false, true} {
				check(t, db.Set("float", tc.initial))
				var got, want T
				var err error
				if subtract {
					got, err = db.Sub("float", tc.delta)
					want = tc.sub
				} else {
					got, err = db.Add("float", tc.delta)
					want = tc.add
				}
				check(t, err)
				stored, err := db.Get[T]("float")
				check(t, err)
				for _, value := range []T{got, stored} {
					if math.IsNaN(float64(want)) {
						if !math.IsNaN(float64(value)) {
							t.Fatalf("got %v, want NaN", value)
						}
					} else if !sameValue(value, want) {
						t.Fatalf("initial=%v delta=%v subtract=%v: got %v, want %v", tc.initial, tc.delta, subtract, value, want)
					}
				}
			}
		}
	})
}
