//go:build !race

package dot

import (
	"bytes"
	"fmt"
	"testing"
	"time"
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
					idx.insert(home, fingerprint(hash)|logAddress(segment, off))
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
	if err := db.Set(key, want); err != nil {
		t.Fatal(err)
	}
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
	for _, durability := range []Durability{Write, Buffer} {
		t.Run(fmt.Sprint(durability), func(t *testing.T) {
			db := newInternalDB(t, WithDurability(durability))
			stopTestMaintenance(db)
			key := []byte("scalar")
			checkScalarAllocations(t, db, key, int(-42))
			checkScalarAllocations(t, db, key, int8(-42))
			checkScalarAllocations(t, db, key, int16(-42))
			checkScalarAllocations(t, db, key, int32(-42))
			checkScalarAllocations(t, db, key, int64(-42))
			checkScalarAllocations(t, db, key, uint(42))
			checkScalarAllocations(t, db, key, uint8(42))
			checkScalarAllocations(t, db, key, uint16(42))
			checkScalarAllocations(t, db, key, uint32(42))
			checkScalarAllocations(t, db, key, uint64(42))
			checkScalarAllocations(t, db, key, float32(1.5))
			checkScalarAllocations(t, db, key, float64(1.5))
			checkScalarAllocations(t, db, key, time.Second)
			checkScalarAllocations(t, db, key, true)
		})
	}
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
		idx.insert(home, fingerprint(hash)|logAddress(segment, off))
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
	checkGetMissingAllocations[int](t, db, key)
	checkGetMissingAllocations[int8](t, db, key)
	checkGetMissingAllocations[int16](t, db, key)
	checkGetMissingAllocations[int32](t, db, key)
	checkGetMissingAllocations[int64](t, db, key)
	checkGetMissingAllocations[uint](t, db, key)
	checkGetMissingAllocations[uint8](t, db, key)
	checkGetMissingAllocations[uint16](t, db, key)
	checkGetMissingAllocations[uint32](t, db, key)
	checkGetMissingAllocations[uint64](t, db, key)
	checkGetMissingAllocations[float32](t, db, key)
	checkGetMissingAllocations[float64](t, db, key)
	checkGetMissingAllocations[time.Duration](t, db, key)
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
	t.Run("int", func(t *testing.T) {
		checkScalarSetAllocations(t, db, key, int(-42), int(42))
	})
	t.Run("int8", func(t *testing.T) {
		checkScalarSetAllocations(t, db, key, int8(-42), int8(42))
	})
	t.Run("int16", func(t *testing.T) {
		checkScalarSetAllocations(t, db, key, int16(-42), int16(42))
	})
	t.Run("int32", func(t *testing.T) {
		checkScalarSetAllocations(t, db, key, int32(-42), int32(42))
	})
	t.Run("int64", func(t *testing.T) {
		checkScalarSetAllocations(t, db, key, int64(-42), int64(42))
	})
	t.Run("uint", func(t *testing.T) {
		checkScalarSetAllocations(t, db, key, uint(0), ^uint(0))
	})
	t.Run("uint8", func(t *testing.T) {
		checkScalarSetAllocations(t, db, key, uint8(0), ^uint8(0))
	})
	t.Run("uint16", func(t *testing.T) {
		checkScalarSetAllocations(t, db, key, uint16(0), ^uint16(0))
	})
	t.Run("uint32", func(t *testing.T) {
		checkScalarSetAllocations(t, db, key, uint32(0), ^uint32(0))
	})
	t.Run("uint64", func(t *testing.T) {
		checkScalarSetAllocations(t, db, key, uint64(0), ^uint64(0))
	})
	t.Run("float32", func(t *testing.T) {
		checkScalarSetAllocations(t, db, key, float32(-1.5), float32(1.5))
	})
	t.Run("float64", func(t *testing.T) {
		checkScalarSetAllocations(t, db, key, -1.5, 1.5)
	})
	t.Run("duration", func(t *testing.T) {
		checkScalarSetAllocations(t, db, key, -time.Second, time.Second)
	})
	t.Run("bool", func(t *testing.T) {
		checkScalarSetAllocations(t, db, key, false, true)
	})
}

func checkScalarUpdateAllocations[T interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64 | float32 | float64 | time.Duration
}](t *testing.T, db *DB, initial T) {
	t.Helper()
	key := []byte("counter")
	if err := db.Set(key, initial); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		fn   func(T, bool) (T, error)
	}{
		{"increment", func(v T, exists bool) (T, error) { return v + 1, nil }},
		{"unchanged", func(v T, exists bool) (T, error) { return v, nil }},
	} {
		t.Run(fmt.Sprintf("%T/%s", initial, tc.name), func(t *testing.T) {
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

func TestDBUpdateScalarAllocations(t *testing.T) {
	db := newInternalDB(t)
	checkScalarUpdateAllocations(t, db, int(-42))
	checkScalarUpdateAllocations(t, db, int8(-42))
	checkScalarUpdateAllocations(t, db, int16(-42))
	checkScalarUpdateAllocations(t, db, int32(-42))
	checkScalarUpdateAllocations(t, db, int64(-42))
	checkScalarUpdateAllocations(t, db, uint(0))
	checkScalarUpdateAllocations(t, db, uint8(0))
	checkScalarUpdateAllocations(t, db, uint16(0))
	checkScalarUpdateAllocations(t, db, uint32(0))
	checkScalarUpdateAllocations(t, db, uint64(0))
	checkScalarUpdateAllocations(t, db, float32(1.5))
	checkScalarUpdateAllocations(t, db, float64(1.5))
	checkScalarUpdateAllocations(t, db, time.Second)
}

func TestDBArithmeticAllocations(t *testing.T) {
	db := newInternalDB(t, WithShardCount(1))
	stopTestMaintenance(db)
	key := []byte("counter")
	if err := db.Set(key, int64(0)); err != nil {
		t.Fatal(err)
	}
	allocations := testing.AllocsPerRun(100, func() {
		if _, err := db.Add(key, int64(1)); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Sub(key, int64(1)); err != nil {
			t.Fatal(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("arithmetic allocated %g times, want 0", allocations)
	}
}

func TestOperationMetricsAllocations(t *testing.T) {
	for _, latency := range []bool{false, true} {
		t.Run(fmt.Sprintf("latency=%t", latency), func(t *testing.T) {
			db := newInternalDB(t, WithShardCount(1))
			if err := db.Set("a", uint64(42)); err != nil {
				t.Fatal(err)
			}
			h := db.MetricsHandler(WithMetricsHistory(0), WithMetricsLatency(latency)).(*metricsHandler)
			_, stop := h.subscribe()
			defer stop()
			for name, run := range map[string]func() error{
				"get":    func() error { _, err := db.Get[uint64]("a"); return err },
				"set":    func() error { return db.Set("a", uint64(42)) },
				"delete": func() error { return db.Delete("absent") },
				"update": func() error {
					return db.Update("a", func(v uint64, _ bool) (uint64, error) { return v, nil })
				},
			} {
				t.Run(name, func(t *testing.T) {
					allocations := testing.AllocsPerRun(100, func() {
						if err := run(); err != nil {
							t.Fatal(err)
						}
					})
					if allocations != 0 {
						t.Fatalf("instrumented operation allocated %f times", allocations)
					}
				})
			}
		})
	}
}
