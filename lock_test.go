package dot_test

import (
	"bufio"
	"dot"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestLockFileDoubleOpen(t *testing.T) {
	dir := t.TempDir()
	db1, err := dot.Open(dir)
	check(t, err)
	if _, err := os.Stat(filepath.Join(dir, "LOCK")); err != nil {
		t.Fatalf("stat LOCK: %s", err)
	}
	// A second Open in this process also fails: separate file descriptions
	// conflict under flock, which is what we want — two handles would
	// corrupt each other just like two processes.
	_, err = dot.Open(dir)
	if !errors.Is(err, dot.ErrDatabaseLocked) {
		t.Fatalf("second Open err = %v, want ErrDatabaseLocked", err)
	}
	check(t, db1.Close())
	// Closing releases the lock; reopening works.
	db2, err := dot.Open(dir)
	check(t, err)
	check(t, db2.Close())
}

// TestLockFileSubprocess holds the lock in a child process: the parent's
// Open must fail while it is held, and killing the child must release it —
// the kernel drops flock on process death, so the lock can never go stale.
func TestLockFileSubprocess(t *testing.T) {
	if os.Getenv("DOTKV_TEST_LOCK_HOLDER") == "1" {
		db, err := dot.Open(os.Getenv("DOTKV_TEST_LOCK_DIR"))
		if err != nil {
			os.Exit(2)
		}
		defer db.Close()
		os.Stdout.WriteString("ready\n")
		select {}
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=TestLockFileSubprocess")
	cmd.Env = append(os.Environ(),
		"DOTKV_TEST_LOCK_HOLDER=1",
		"DOTKV_TEST_LOCK_DIR="+dir,
	)
	stdout, err := cmd.StdoutPipe()
	check(t, err)
	check(t, cmd.Start())
	line, err := bufio.NewReader(stdout).ReadString('\n')
	check(t, err)
	if line != "ready\n" {
		t.Fatalf("child ready = %q", line)
	}
	_, err = dot.Open(dir)
	if !errors.Is(err, dot.ErrDatabaseLocked) {
		t.Fatalf("Open during holder err = %v, want ErrDatabaseLocked", err)
	}
	check(t, cmd.Process.Kill())
	cmd.Wait()
	db, err := dot.Open(dir)
	check(t, err)
	check(t, db.Close())
}
