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

func TestIndexFindCollisionAllocations(t *testing.T) {
	for _, size := range []int{128, 2 * logWriteBufferSize} {
		for _, hit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%dB/hit=%t", size, hit), func(t *testing.T) {
				log, err := newLog(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := log.headFile.Close(); err != nil {
						t.Error(err)
					}
				})
				idx := newInternalIndex(t)
				key, value := []byte("target"), bytes.Repeat([]byte{'v'}, size)
				const hash = uint64(0x123456789abcdef)
				home := int(hash & uint64(len(idx.slots)-1))
				for i := range 3 {
					candidate := []byte("decoy!")
					candidateValue := bytes.Repeat([]byte{byte('a' + i)}, size)
					if hit && i == 2 {
						candidate, candidateValue = key, value
					}
					segment, off, err := log.write('s', candidate, candidateValue)
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
						pos, got, err := idx.find(log, key, hash, buf)
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
