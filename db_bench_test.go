package dot

import (
	"bytes"
	"encoding/binary"
	"testing"
)

/*
goos: linux
goarch: amd64
pkg: dot
cpu: AMD Ryzen 7 7840HS w/ Radeon 780M Graphics
BenchmarkSet-16             	 1400335	       841.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkGetHit-16          	 1514816	       794.5 ns/op	     144 B/op	       1 allocs/op
BenchmarkReplace-16         	  810716	      1526 ns/op	       0 B/op	       0 allocs/op
BenchmarkSetUnchanged-16    	 1593102	       750.8 ns/op	       0 B/op	       0 allocs/op
BenchmarkGetMiss-16         	89616434	        12.73 ns/op	       0 B/op	       0 allocs/op
BenchmarkGetScalar/bytes/hit-16         	 1453092	       827.1 ns/op	      24 B/op	       1 allocs/op
BenchmarkGetScalar/int64/hit-16         	 1484664	       919.3 ns/op	       0 B/op	       0 allocs/op
BenchmarkGetScalar/uint64/hit-16        	 1000000	      1072 ns/op	       0 B/op	       0 allocs/op
BenchmarkGetScalar/float64/hit-16       	 1390905	       836.3 ns/op	       0 B/op	       0 allocs/op
BenchmarkGetScalar/bool/hit-16          	 1476885	       811.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkGetScalar/bytes/miss-16        	91962274	        12.64 ns/op	       0 B/op	       0 allocs/op
BenchmarkGetScalar/int64/miss-16        	100000000	        11.13 ns/op	       0 B/op	       0 allocs/op
BenchmarkGetScalar/uint64/miss-16       	100000000	        11.13 ns/op	       0 B/op	       0 allocs/op
BenchmarkGetScalar/float64/miss-16      	97737777	        11.80 ns/op	       0 B/op	       0 allocs/op
BenchmarkGetScalar/bool/miss-16         	96750692	        11.35 ns/op	       0 B/op	       0 allocs/op
BenchmarkDeleteHit-16                   	  847647	      1459 ns/op	       0 B/op	       0 allocs/op
BenchmarkDeleteMiss-16                  	80069713	        14.72 ns/op	       0 B/op	       0 allocs/op
PASS
ok  	dot	25.871s
*/
const benchmarkKeyCount = 4096

// Use 16-byte keys and 128-byte values. The second half of keys is never
// inserted, so misses exercise a populated database.
type primitiveFixture struct {
	db    *DB
	keys  [2 * benchmarkKeyCount][16]byte
	value [128]byte
}

func newPrimitiveFixture(b *testing.B) *primitiveFixture {
	b.Helper()
	// Callers must start the timer after setup, even when using b.Loop.
	b.StopTimer()
	db := newInternalDB(b)
	f := &primitiveFixture{db: db}
	for i := range f.keys {
		binary.LittleEndian.PutUint64(f.keys[i][:], uint64(i))
	}
	for i := range f.value {
		f.value[i] = 'a'
	}
	b.ReportAllocs()
	b.ResetTimer()
	return f
}

// Reset outside the timed loop to bound log growth below the 26-bit offset
// limit and ensure every batch starts with the same number of live keys.
func (f *primitiveFixture) reset(b *testing.B, populated bool) {
	f.resetValue(b, populated, f.value[:])
}

func (f *primitiveFixture) resetValue[T Value](b *testing.B, populated bool, value T) {
	b.Helper()
	for i := range f.db.shards {
		s := &f.db.shards[i]
		if err := s.log.headFile.Truncate(0); err != nil {
			b.Fatal(err)
		}
		s.log.size = 0
		clear(s.index.slots)
		s.index.population = 0
	}
	if populated {
		for i := 0; i < benchmarkKeyCount; i++ {
			if err := f.db.Set(f.keys[i][:], value); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkSet(b *testing.B) {
	f := newPrimitiveFixture(b)
	for done := 0; done < b.N; {
		f.reset(b, false)
		n := min(benchmarkKeyCount, b.N-done)
		b.StartTimer()
		for i := 0; i < n; i++ {
			if err := f.db.Set(f.keys[i][:], f.value[:]); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		done += n
	}
}

func BenchmarkGetHit(b *testing.B) {
	benchmarkGet(b, true, nil)
}

func BenchmarkReplace(b *testing.B) {
	f := newPrimitiveFixture(b)
	replacement := f.value
	replacement[0] = 'b'
	for done := 0; done < b.N; {
		f.reset(b, true)
		n := min(benchmarkKeyCount, b.N-done)
		b.StartTimer()
		for i := range n {
			if err := f.db.Set(f.keys[i][:], replacement[:]); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		done += n
	}
}

func BenchmarkSetUnchanged(b *testing.B) {
	f := newPrimitiveFixture(b)
	f.reset(b, true)
	b.StartTimer()
	for i := 0; b.Loop(); i++ {
		if err := f.db.Set(f.keys[i%benchmarkKeyCount][:], f.value[:]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func BenchmarkGetMiss(b *testing.B) {
	benchmarkGet(b, false, nil)
}

func benchmarkGet(b *testing.B, hit bool, value []byte) {
	b.Helper()
	f := newPrimitiveFixture(b)
	if value == nil {
		value = f.value[:]
	}
	f.resetValue(b, true, value)
	offset, want := 0, value
	var wantErr error
	if !hit {
		offset, want = benchmarkKeyCount, nil
		wantErr = ErrNotFound
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		got, err := f.db.Get[[]byte](f.keys[offset+i%benchmarkKeyCount][:])
		if err != wantErr {
			b.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			b.Fatalf("get: got %q, want %q", got, want)
		}
	}
	b.StopTimer()
}

// Compare owned bytes and scalar decoding with the same small records. Setup,
// including binary encoding, stays outside the timed loops.
func BenchmarkGetScalar(b *testing.B) {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], 42)
	for _, hit := range []bool{true, false} {
		name := "hit"
		if !hit {
			name = "miss"
		}
		b.Run("bytes/"+name, func(b *testing.B) {
			benchmarkGet(b, hit, encoded[:])
		})
		b.Run("int64/"+name, func(b *testing.B) {
			benchmarkScalarGet(b, hit, int64(42))
		})
		b.Run("uint64/"+name, func(b *testing.B) {
			benchmarkScalarGet(b, hit, uint64(42))
		})
		b.Run("float64/"+name, func(b *testing.B) {
			benchmarkScalarGet(b, hit, 1.5)
		})
		b.Run("bool/"+name, func(b *testing.B) {
			benchmarkScalarGet(b, hit, true)
		})
	}
}

func benchmarkScalarGet[T interface {
	Value
	comparable
}](b *testing.B, hit bool, want T) {
	b.Helper()
	f := newPrimitiveFixture(b)
	f.resetValue(b, true, want)
	offset := 0
	var wantErr error
	if !hit {
		offset = benchmarkKeyCount
		var zero T
		want = zero
		wantErr = ErrNotFound
	}
	b.StartTimer()
	for i := 0; b.Loop(); i++ {
		got, err := f.db.Get[T](f.keys[offset+i%benchmarkKeyCount][:])
		if err != wantErr {
			b.Fatal(err)
		}
		if got != want {
			b.Fatalf("get: got %v, want %v", got, want)
		}
	}
	b.StopTimer()
}

func BenchmarkSetScalar(b *testing.B) {
	var initial, replacement [8]byte
	binary.LittleEndian.PutUint64(initial[:], 42)
	binary.LittleEndian.PutUint64(replacement[:], 43)
	for _, operation := range []string{"insert", "replace", "unchanged"} {
		b.Run("bytes/"+operation, func(b *testing.B) {
			benchmarkScalarSet(b, operation, initial[:], replacement[:])
		})
		b.Run("int64/"+operation, func(b *testing.B) {
			benchmarkScalarSet(b, operation, int64(42), int64(43))
		})
		b.Run("uint64/"+operation, func(b *testing.B) {
			benchmarkScalarSet(b, operation, uint64(42), uint64(43))
		})
		b.Run("float64/"+operation, func(b *testing.B) {
			benchmarkScalarSet(b, operation, 1.5, 2.5)
		})
		b.Run("bool/"+operation, func(b *testing.B) {
			benchmarkScalarSet(b, operation, false, true)
		})
	}
}

func benchmarkScalarSet[T Value](b *testing.B, operation string, initial, replacement T) {
	b.Helper()
	f := newPrimitiveFixture(b)
	if operation == "unchanged" {
		f.resetValue(b, true, initial)
		b.StartTimer()
		for i := 0; b.Loop(); i++ {
			if err := f.db.Set(f.keys[i%benchmarkKeyCount][:], initial); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		return
	}
	value := initial
	if operation == "replace" {
		value = replacement
	}
	for done := 0; done < b.N; {
		f.resetValue(b, operation == "replace", initial)
		n := min(benchmarkKeyCount, b.N-done)
		b.StartTimer()
		for i := range n {
			if err := f.db.Set(f.keys[i][:], value); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		done += n
	}
}

func BenchmarkDeleteHit(b *testing.B) {
	f := newPrimitiveFixture(b)
	for done := 0; done < b.N; {
		f.reset(b, true)
		n := min(benchmarkKeyCount, b.N-done)
		b.StartTimer()
		for i := 0; i < n; i++ {
			if err := f.db.Delete(f.keys[i][:]); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		done += n
	}
}

func BenchmarkDeleteMiss(b *testing.B) {
	f := newPrimitiveFixture(b)
	f.reset(b, true)
	b.StartTimer()
	for i := 0; b.Loop(); i++ {
		if err := f.db.Delete(f.keys[benchmarkKeyCount+i%benchmarkKeyCount][:]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}
