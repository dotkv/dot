package dot

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

// syncDirectoryTree persists the database's shard entries and its own name,
// including any ancestors created by MkdirAll. Visit existing ancestors too:
// they may have been created by an earlier Open whose sync failed. syncDir is
// passed explicitly so initialization ordering and failures can be tested.
func syncDirectoryTree(path string, syncDir func(string) error) error {
	path, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	for {
		if err := syncDir(path); err != nil {
			return fmt.Errorf("sync directory %q: %w", path, err)
		}
		parent := filepath.Dir(path)
		if parent == path {
			return nil
		}
		path = parent
	}
}
