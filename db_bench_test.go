package dot

import (
	"bytes"
	"encoding/binary"
	"testing"
)

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
			if err := f.db.Set(f.keys[i][:], f.value[:]); err != nil {
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
	benchmarkGet(b, true)
}

func BenchmarkReplace(b *testing.B) {
	f := newPrimitiveFixture(b)
	replacement := f.value
	replacement[0] = 'b'
	for done := 0; done < b.N; {
		f.reset(b, true)
		n := min(benchmarkKeyCount, b.N-done)
		b.StartTimer()
		for i := 0; i < n; i++ {
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
	for i := 0; i < b.N; i++ {
		if err := f.db.Set(f.keys[i%benchmarkKeyCount][:], f.value[:]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func BenchmarkGetMiss(b *testing.B) {
	benchmarkGet(b, false)
}

func benchmarkGet(b *testing.B, hit bool) {
	b.Helper()
	f := newPrimitiveFixture(b)
	f.reset(b, true)
	offset, want := 0, f.value[:]
	if !hit {
		offset, want = benchmarkKeyCount, nil
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		got, err := f.db.Get(f.keys[offset+i%benchmarkKeyCount][:])
		if err != nil {
			b.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			b.Fatalf("get: got %q, want %q", got, want)
		}
	}
	b.StopTimer()
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
