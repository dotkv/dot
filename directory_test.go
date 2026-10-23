package dot

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestSyncDirectoryTree(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	path := filepath.Join("one", "two", "db")
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	want := []string{filepath.Join(root, path), filepath.Join(root, "one", "two"), filepath.Join(root, "one"), root}
	for parent := filepath.Dir(root); parent != want[len(want)-1]; parent = filepath.Dir(parent) {
		want = append(want, parent)
	}
	for _, name := range []string{path, filepath.Join(root, path), ".", string(os.PathSeparator)} {
		t.Run(name, func(t *testing.T) {
			expected := want
			if name == "." {
				expected = want[3:]
			} else if name == string(os.PathSeparator) {
				expected = want[len(want)-1:]
			}
			var synced []string
			if err := syncDirectoryTree(name, func(dir string) error {
				synced = append(synced, dir)
				return syncDirectory(dir)
			}); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(synced, expected) {
				t.Fatalf("synced %q, want %q", synced, expected)
			}
		})
	}

	injected := errors.New("injected directory sync failure")
	for failed := range want {
		var synced []string
		err := syncDirectoryTree(path, func(dir string) error {
			synced = append(synced, dir)
			if len(synced)-1 == failed {
				return injected
			}
			return nil
		})
		if !errors.Is(err, injected) || !strings.Contains(err.Error(), want[failed]) || !slices.Equal(synced, want[:failed+1]) {
			t.Fatalf("failure at %q: synced=%q error=%v", want[failed], synced, err)
		}
		// A previous failure leaves directories present. Retrying must still
		// persist the whole chain, rather than stop at an existing ancestor.
		synced = nil
		if err := syncDirectoryTree(path, func(dir string) error {
			synced = append(synced, dir)
			return nil
		}); err != nil || !slices.Equal(synced, want) {
			t.Fatalf("retry: synced=%q error=%v", synced, err)
		}
	}
}

func TestOpenDirectorySyncFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("directory permissions do not restrict root")
	}
	parent := filepath.Join(t.TempDir(), "parent")
	if err := os.Mkdir(parent, 0300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(parent, 0700) })
	path := filepath.Join(parent, "nested", "db")
	// Write and execute permission allow initialization, but opening this
	// ancestor for its final fsync must fail. All shards have been opened.
	db, err := Open(path, WithShardCount(2))
	if db != nil {
		db.Close()
		t.Fatal("Open returned a database after a directory sync failure")
	}
	if !errors.Is(err, os.ErrPermission) || !strings.Contains(err.Error(), "sync directory") {
		t.Fatalf("Open error=%v, want directory permission failure", err)
	}
	if err := os.Chmod(parent, 0700); err != nil {
		t.Fatal(err)
	}
	for _, shard := range []string{"00", "01"} {
		if _, err := os.Stat(filepath.Join(path, shard, "0000000.log")); err != nil {
			t.Fatalf("directory sync did not run after shard initialization: %v", err)
		}
	}
	if got := openFilesUnder(t, path); got != 0 {
		t.Fatalf("failed Open leaked %d log descriptors", got)
	}
	// Successful retry also verifies that failure released the database lock.
	db, err = Open(path)
	if err != nil {
		t.Fatalf("retry Open: %v", err)
	}
	defer db.Close()
	if err := db.Set("key", "value", WithDurability(Sync)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if value, err := db.Get[string]("key"); err != nil || value != "value" {
		t.Fatalf("reopened value=%q error=%v", value, err)
	}
}
