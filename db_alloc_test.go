//go:build !race

package dot

import "testing"

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
