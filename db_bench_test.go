package dot

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"
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
	// Callers must start the timer after setup, even when using b.Loop.
	b.StopTimer()
	// Primitive benchmarks reset files and index state directly. Keep background
	// maintenance out of these fixtures and their foreground measurements.
	db := newInternalDB(b)
	stopTestMaintenance(db)
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
		s.log.recordCount = 0
		s.log.recordBytes = 0
		clear(s.log.segmentBytes)
		s.log.segmentBytes[s.log.headSegment] = 0
		clear(s.log.segmentRecordCounts)
		s.log.segmentRecordCounts[s.log.headSegment] = 0
		clear(s.log.segmentRefCounts)
		s.log.segmentRefCounts[s.log.headSegment] = 0
		clear(s.index.slots)
		s.index.entryCount = 0
		s.index.liveBytes = 0
		clear(s.index.expirations)
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

func BenchmarkGetBytesHit(b *testing.B) {
	benchmarkGetBytes(b, true)
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

func BenchmarkGetBytesMiss(b *testing.B) {
	benchmarkGetBytes(b, false)
}

func benchmarkGetBytes(b *testing.B, hit bool) {
	b.Helper()
	f := newPrimitiveFixture(b)
	f.reset(b, true)
	offset, want := 0, f.value[:]
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

func BenchmarkGetUint64Hit(b *testing.B) {
	f := newPrimitiveFixture(b)
	want := uint64(42)
	f.resetValue(b, true, want)
	b.StartTimer()
	for i := 0; b.Loop(); i++ {
		got, err := f.db.Get[uint64](f.keys[i%benchmarkKeyCount][:])
		if err != nil {
			b.Fatal(err)
		}
		if got != want {
			b.Fatalf("get: got %d, want %d", got, want)
		}
	}
	b.StopTimer()
}

func BenchmarkGetUint64Miss(b *testing.B) {
	f := newPrimitiveFixture(b)
	f.resetValue(b, true, uint64(42))
	b.StartTimer()
	for i := 0; b.Loop(); i++ {
		got, err := f.db.Get[uint64](f.keys[benchmarkKeyCount+i%benchmarkKeyCount][:])
		if err != ErrNotFound {
			b.Fatal(err)
		}
		if got != 0 {
			b.Fatalf("get: got %d, want 0", got)
		}
	}
	b.StopTimer()
}

func BenchmarkSetUint64(b *testing.B) {
	f := newPrimitiveFixture(b)
	for done := 0; done < b.N; {
		f.reset(b, false)
		n := min(benchmarkKeyCount, b.N-done)
		b.StartTimer()
		for i := range n {
			if err := f.db.Set(f.keys[i][:], uint64(42)); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		done += n
	}
}

func BenchmarkUpdateUint64(b *testing.B) {
	f := newPrimitiveFixture(b)
	for done := 0; done < b.N; {
		f.resetValue(b, true, uint64(0))
		n := min(benchmarkKeyCount, b.N-done)
		b.StartTimer()
		for i := range n {
			if err := f.db.Update(f.keys[i][:], func(v uint64, exists bool) (uint64, error) { return v + 1, nil}); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		done += n
	}
}

func BenchmarkUpdateUint64Unchanged(b *testing.B) {
	f := newPrimitiveFixture(b)
	f.resetValue(b, true, uint64(42))
	b.StartTimer()
	for i := 0; b.Loop(); i++ {
		if err := f.db.Update(f.keys[i%benchmarkKeyCount][:], func(v uint64, exists bool) (uint64, error) { return v, nil}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpdateUint64Parallel(b *testing.B) {
	for _, hotKey := range []bool{false, true} {
		name := "distributed"
		if hotKey {
			name = "hot-key"
		}
		b.Run(name, func(b *testing.B) {
			f := newPrimitiveFixture(b)
			f.resetValue(b, true, uint64(0))
			var workerID atomic.Uint64
			b.StartTimer()
			b.RunParallel(func(pb *testing.PB) {
				// Spread worker starting points across the populated keys.
				for i := int(workerID.Add(1)) * 257; pb.Next(); i++ {
					key := i % benchmarkKeyCount
					if hotKey {
						key = 0
					}
					if err := f.db.Update(f.keys[key][:], func(v uint64, exists bool) (uint64, error) { return v + 1, nil}); err != nil {
						b.Error(err)
						return
					}
				}
			})
			b.StopTimer()
			var total uint64
			for i := range benchmarkKeyCount {
				value, err := f.db.Get[uint64](f.keys[i][:])
				if err != nil {
					b.Fatal(err)
				}
				total += value
			}
			if total != uint64(b.N) {
				b.Fatalf("got %d increments, want %d", total, b.N)
			}
		})
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
