//go:build !race

package dot

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
)

// Exercise actual mmap failures in a subprocess so the address-space limit
// cannot interfere with other tests or the race detector's own mappings.
func TestDBMappingFailure(t *testing.T) {
	const helperEnv = "DOT_TEST_MMAP_FAILURE"
	mode := os.Getenv(helperEnv)
	if mode == "" {
		for _, mode := range []string{"new", "grow", "shrink"} {
			t.Run(mode, func(t *testing.T) {
				cmd := exec.Command(os.Args[0], "-test.run=^TestDBMappingFailure$")
				cmd.Env = append(os.Environ(), helperEnv+"="+mode)
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("%v\n%s", err, output)
				}
			})
		}
		return
	}

	// Warm constructor/cleanup paths before forbidding new OS mappings.
	db := newInternalDB(t)
	// The worker can allocate lazily; stop it before restricting address space.
	stopTestMaintenance(db)
	if mode == "new" {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		path := db.path
		marker := filepath.Join(path, "preserve")
		if err := os.WriteFile(marker, []byte("existing data"), 0600); err != nil {
			t.Fatal(err)
		}
		var opened *DB
		var err error
		withoutNewMappings(t, func() { opened, err = Open(path) })
		if opened != nil || !errors.Is(err, syscall.ENOMEM) {
			if opened != nil {
				opened.Close()
			}
			t.Fatalf("Open: got database=%v error=%v, want ENOMEM", opened, err)
		}
		if got := openFilesUnder(t, path); got != 0 {
			t.Fatalf("failed mapping leaked %d file descriptors", got)
		}
		if got, err := os.ReadFile(marker); err != nil || string(got) != "existing data" {
			t.Fatalf("failed mapping changed existing data: %q, %v", got, err)
		}
		return
	}
	if mode != "grow" && mode != "shrink" {
		t.Fatalf("unknown helper mode %q", mode)
	}
	s := &db.shards[0]
	n := len(s.index.slots)
	keys := keysForShard(db, s, "mapping-failure", n*3/4+1)
	value := []byte("value")
	for _, key := range keys[:len(keys)-1] {
		if err := db.Set(key, value); err != nil {
			t.Fatal(err)
		}
	}
	if mode == "shrink" {
		if err := db.Set(keys[len(keys)-1], value); err != nil {
			t.Fatal(err)
		}
		for _, key := range keys[16:] {
			if err := db.Delete(key); err != nil {
				t.Fatal(err)
			}
		}
		before := slices.Clone(s.index.slots)
		size, records := s.log.size, s.log.recordCount
		var shrinkErr error
		withoutNewMappings(t, func() { shrinkErr = s.compactExpirations() })
		if !errors.Is(shrinkErr, syscall.ENOMEM) {
			t.Fatalf("shrink: got %v, want ENOMEM", shrinkErr)
		}
		if !slices.Equal(before, s.index.slots) || s.index.entryCount != 16 || s.log.size != size || s.log.recordCount != records || s.compactionRetry {
			t.Fatal("failed shrink changed the index, log, or compaction scheduling")
		}
		if got, err := db.Get[[]byte](keys[0]); err != nil || !bytes.Equal(got, value) {
			t.Fatalf("existing value after failed shrink: %q, %v", got, err)
		}
		if err := db.Set(keys[0], []byte("updated")); err != nil {
			t.Fatalf("mutation after failed shrink: %v", err)
		}
		if err := s.compactExpirations(); err != nil {
			t.Fatalf("retry shrink: %v", err)
		}
		if len(s.index.slots) != n {
			t.Fatal("retry did not shrink to the minimum capacity")
		}
		checkSegmentAccounting(t, s)
		return
	}
	before := slices.Clone(s.index.slots)
	size, entryCount := s.log.size, s.index.entryCount
	key := keys[len(keys)-1]
	var setErr error
	withoutNewMappings(t, func() { setErr = db.Set(key, value) })
	if !errors.Is(setErr, syscall.ENOMEM) {
		t.Fatalf("Set: got %v, want ENOMEM", setErr)
	}
	if !slices.Equal(before, s.index.slots) || s.index.entryCount != entryCount || s.log.size != size {
		t.Fatal("failed mapping changed the index or appended a log record")
	}
	if got, err := db.Get[[]byte](keys[0]); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("existing value after failure: %q, %v", got, err)
	}
	if err := db.Set(key, value); err != nil {
		t.Fatalf("retry after mapping failure: %v", err)
	}
	if len(s.index.slots) != 2*n {
		t.Fatal("retry did not grow the index")
	}
	if got, err := db.Get[[]byte](key); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("retried value: %q, %v", got, err)
	}
}

func withoutNewMappings(t *testing.T, run func()) {
	t.Helper()
	var original syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_AS, &original); err != nil {
		t.Fatal(err)
	}
	limited := original
	limited.Cur = 1
	if err := syscall.Setrlimit(syscall.RLIMIT_AS, &limited); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := syscall.Setrlimit(syscall.RLIMIT_AS, &original); err != nil {
			t.Fatal(err)
		}
	}()
	run()
}
