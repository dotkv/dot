//go:build !race

package dot

import (
	"bytes"
	"fmt"
	"testing"
)

// Race instrumentation adds allocations, so measure only ordinary builds.
func TestDBMutationAllocations(t *testing.T) {
	db := newInternalDB(t)
	key, value, replacement := []byte("key"), []byte("value"), []byte("updated")
	if err := db.Set(key, value); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{"unchanged", func() error { return db.Set(key, value) }},
		{"replace", func() error {
			if err := db.Set(key, replacement); err != nil {
				return err
			}
			return db.Set(key, value)
		}},
		{"delete-and-reinsert", func() error {
			if err := db.Delete(key); err != nil {
				return err
			}
			return db.Set(key, value)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allocations := testing.AllocsPerRun(100, func() {
				if err := tc.run(); err != nil {
					t.Fatal(err)
				}
			})
			if allocations != 0 {
				t.Fatalf("got %g allocations, want 0", allocations)
			}
		})
	}
}

func TestIndexFindPayloadCollisionAllocations(t *testing.T) {
	for _, size := range []int{128, 2 * logWriteBufferSize} {
		for _, hit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%dB/hit=%t", size, hit), func(t *testing.T) {
				idx := newInternalIndex(t)
				log, err := newLog(t.TempDir(), idx)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := log.headFile.Close(); err != nil {
						t.Error(err)
					}
				})
				key, value := []byte("target"), bytes.Repeat([]byte{'v'}, size)
				const hash = uint64(0x123456789abcdef)
				home := int(hash & uint64(len(idx.slots)-1))
				for i := range 3 {
					candidate := []byte("decoy!")
					candidateValue := bytes.Repeat([]byte{byte('a' + i)}, size)
					if hit && i == 2 {
						candidate, candidateValue = key, value
					}
					segment, off, err := log.write(typeBytes, candidate, candidateValue, 0)
					if err != nil {
						t.Fatal(err)
					}
					// Force fingerprint matches so every candidate needs a log read.
					idx.insert(home, hashtag(hash)|segment<<26|off)
				}
				for _, buf := range [][]byte{nil, log.writeBuffer[:0]} {
					wantAllocs := 1.0
					if buf != nil && cap(buf) >= len(key)+len(value) {
						wantAllocs = 0
					}
					allocations := testing.AllocsPerRun(100, func() {
						pos, got, _, _, err := idx.findPayload(log, key, hash, buf)
						if err != nil {
							t.Fatal(err)
						}
						if hit {
							if pos < 0 || !bytes.Equal(got, value) {
								t.Fatal("collision lookup did not return the matching value")
							}
						} else if pos >= 0 || got != nil {
							t.Fatal("collision lookup returned a nonmatching key")
						}
					})
					if allocations != wantAllocs {
						t.Fatalf("buffer capacity %d: got %g allocations, want %g", cap(buf), allocations, wantAllocs)
					}
				}
			})
		}
	}
}

func checkScalarAllocations[T interface {
	Value
	comparable
}](t *testing.T, db *DB, key []byte, want T) {
	t.Helper()
	allocations := testing.AllocsPerRun(100, func() {
		got, err := db.Get[T](key)
		if err != nil || got != want {
			t.Fatalf("got %v, want %v, error %v", got, want, err)
		}
	})
	if allocations != 0 {
		t.Fatalf("Get[%T]: got %g allocations, want 0", want, allocations)
	}
}

func TestDBGetScalarAllocations(t *testing.T) {
	db := newInternalDB(t)
	intKey, uintKey, floatKey, boolKey := []byte("int"), []byte("uint"), []byte("float"), []byte("bool")
	if err := db.Set(intKey, int64(42)); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(uintKey, uint64(42)); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(floatKey, float64(0)); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(boolKey, true); err != nil {
		t.Fatal(err)
	}
	checkScalarAllocations(t, db, intKey, int64(42))
	checkScalarAllocations(t, db, uintKey, uint64(42))
	checkScalarAllocations(t, db, floatKey, float64(0))
	checkScalarAllocations(t, db, boolKey, true)
}

func TestIndexFindScalarCollisionAllocations(t *testing.T) {
	log, idx := openRecoveryLog(t, t.TempDir())
	const hash = uint64(0x123456789abcdef)
	home := int(hash & uint64(len(idx.slots)-1))
	for _, entry := range []struct{ key, value []byte }{
		// This candidate exceeds the initial read, but still fits on the stack.
		{bytes.Repeat([]byte{'k'}, 144), []byte{1}},
		{[]byte("target"), []byte{42, 0, 0, 0, 0, 0, 0, 0}},
	} {
		segment, off, err := log.write(typeBytes, entry.key, entry.value, 0)
		if err != nil {
			t.Fatal(err)
		}
		idx.insert(home, hashtag(hash)|segment<<26|off)
	}
	for _, hit := range []bool{false, true} {
		key := []byte("missing")
		var want scalarValue
		if hit {
			key, want = []byte("target"), scalarValue{bits: 42, size: 8, ty: typeBytes}
		}
		allocations := testing.AllocsPerRun(100, func() {
			pos, got, _, err := idx.findScalar(log, key, hash)
			if err != nil || (pos >= 0) != hit || got != want {
				t.Fatalf("position %d, got %v, want %v, error %v", pos, got, want, err)
			}
		})
		if allocations != 0 {
			t.Fatalf("hit=%t: got %g allocations, want 0", hit, allocations)
		}
	}
}

func checkGetMissingAllocations[T Value](t *testing.T, db *DB, key []byte) {
	t.Helper()
	allocations := testing.AllocsPerRun(100, func() {
		if _, err := db.Get[T](key); err != ErrNotFound {
			t.Fatalf("got %v, want the direct ErrNotFound sentinel", err)
		}
	})
	if allocations != 0 {
		t.Fatalf("Get miss: got %g allocations, want 0", allocations)
	}
}

func TestDBGetMissingAllocations(t *testing.T) {
	db := newInternalDB(t)
	key := []byte("missing")
	checkGetMissingAllocations[[]byte](t, db, key)
	checkGetMissingAllocations[int64](t, db, key)
	checkGetMissingAllocations[uint64](t, db, key)
	checkGetMissingAllocations[float64](t, db, key)
	checkGetMissingAllocations[bool](t, db, key)
}

func checkScalarSetAllocations[T Value](t *testing.T, db *DB, key []byte, initial, replacement T) {
	t.Helper()
	if err := db.Set(key, initial); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{"unchanged", func() error { return db.Set(key, initial) }},
		{"replace", func() error {
			if err := db.Set(key, replacement); err != nil {
				return err
			}
			return db.Set(key, initial)
		}},
		{"delete-and-reinsert", func() error {
			if err := db.Delete(key); err != nil {
				return err
			}
			return db.Set(key, initial)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allocations := testing.AllocsPerRun(100, func() {
				if err := tc.run(); err != nil {
					t.Fatal(err)
				}
			})
			if allocations != 0 {
				t.Fatalf("Set[%T]: got %g allocations, want 0", initial, allocations)
			}
		})
	}
}

func TestDBSetScalarAllocations(t *testing.T) {
	db := newInternalDB(t)
	key := []byte("scalar")
	t.Run("int64", func(t *testing.T) {
		checkScalarSetAllocations(t, db, key, int64(-42), int64(42))
	})
	t.Run("uint64", func(t *testing.T) {
		checkScalarSetAllocations(t, db, key, uint64(0), ^uint64(0))
	})
	t.Run("float64", func(t *testing.T) {
		checkScalarSetAllocations(t, db, key, -1.5, 1.5)
	})
	t.Run("bool", func(t *testing.T) {
		checkScalarSetAllocations(t, db, key, false, true)
	})
}

func TestDBUpdateUint64Allocations(t *testing.T) {
	db := newInternalDB(t)
	key := []byte("counter")
	if err := db.Set(key, uint64(0)); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		fn   func(uint64, bool) (error, uint64)
	}{
		{"increment", func(v uint64, exists bool) (error, uint64) { return nil, v + 1 }},
		{"unchanged", func(v uint64, exists bool) (error, uint64) { return nil, v }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allocations := testing.AllocsPerRun(100, func() {
				if err := db.Update(key, tc.fn); err != nil {
					t.Fatal(err)
				}
			})
			if allocations != 0 {
				t.Fatalf("got %g allocations, want 0", allocations)
			}
		})
	}
}
