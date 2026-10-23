package dot_test

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/dotkv/dot"
)

func newDB(t testing.TB, opts ...dot.Option) *dot.DB {
	t.Helper()
	db, err := dot.Open(t.TempDir(), opts...)
	check(t, err)
	t.Cleanup(func() {
		check(t, db.Destroy())
	})
	return db
}

func check(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected err %s", err)
	}
}

func compare(t *testing.T, a []byte, b []byte) {
	t.Helper()
	if !bytes.Equal(a, b) {
		t.Fatalf("expected '%s' got '%s'", b, a)
	}
}

func equal(t *testing.T, db *dot.DB, key []byte, val []byte) {
	t.Helper()
	v, err := db.Get[[]byte](key)
	check(t, err)
	compare(t, v, val)
}

func expectMissing(t *testing.T, db *dot.DB, key []byte) {
	t.Helper()
	value, err := db.Get[[]byte](key)
	if err != dot.ErrNotFound || value != nil {
		t.Fatalf("missing key: got %q, error %v; want nil and ErrNotFound", value, err)
	}
}

func TestDBBasic(t *testing.T) {
	db := newDB(t)
	key, val := []byte("foo"), []byte("bar")
	check(t, db.Set(key, val))
	equal(t, db, key, val)
	val = []byte("baz")
	check(t, db.Set(key, val))
	equal(t, db, key, val)
	val = []byte("bar")
	check(t, db.Set(key, val))
	equal(t, db, key, val)
	expectMissing(t, db, []byte("bar"))
	check(t, db.Delete(key))
	expectMissing(t, db, key)
	expectMissing(t, db, []byte("missing"))
}

func TestDBSetSameValueAfterDelete(t *testing.T) {
	db := newDB(t)
	key, val := []byte("foo"), []byte("bar")
	check(t, db.Set(key, val))
	check(t, db.Delete(key))
	expectMissing(t, db, key)
	check(t, db.Set(key, val))
	equal(t, db, key, val)
}

func TestDBMultipleKeys(t *testing.T) {
	db := newDB(t)
	const count = 4096
	keys, vals := make([][]byte, count), make([][]byte, count)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "key-%d", i)
		vals[i] = fmt.Appendf(nil, "value-%d", i)
		check(t, db.Set(keys[i], vals[i]))
	}
	for i := range keys {
		equal(t, db, keys[i], vals[i])
	}

	// Deleting some keys must leave all the remaining keys reachable.
	for i := 0; i < count; i += 2 {
		check(t, db.Delete(keys[i]))
	}
	for i := range keys {
		if i%2 == 0 {
			expectMissing(t, db, keys[i])
		} else {
			equal(t, db, keys[i], vals[i])
		}
	}

	// Update the remaining keys while deleted positions still exist.
	for i := 1; i < count; i += 2 {
		vals[i] = fmt.Appendf(nil, "updated-%d", i)
		check(t, db.Set(keys[i], vals[i]))
		equal(t, db, keys[i], vals[i])
	}
	for i := 1; i < count; i += 2 {
		check(t, db.Delete(keys[i]))
	}
	// No old value should become visible after deleting an updated key.
	for i := range keys {
		expectMissing(t, db, keys[i])
	}

	for i := range keys {
		check(t, db.Set(keys[i], vals[i]))
	}
	for i := range keys {
		equal(t, db, keys[i], vals[i])
	}
}

func TestDBExpiration(t *testing.T) {
	db := newDB(t)
	key, val := []byte("foo"), []byte("bar")

	// Future expiration remains readable until the deadline passes.
	check(t, db.Set(key, val, dot.WithExpiration(time.Now().Add(time.Hour))))
	equal(t, db, key, val)

	// Zero time.Time clears expiration.
	check(t, db.Set(key, val, dot.WithExpiration(time.Now().Add(-time.Hour)), dot.WithExpiration(time.Time{})))
	equal(t, db, key, val)

	// Past timestamps (including Unix epoch) are immediately expired.
	for _, past := range []time.Time{time.Now().Add(-time.Second), time.Unix(0, 0), time.Unix(-10, 0)} {
		check(t, db.Set(key, val, dot.WithExpiration(past)))
		expectMissing(t, db, key)
	}

	// Re-setting an expired key with the same value makes it live again.
	check(t, db.Set(key, val))
	equal(t, db, key, val)

	// Short future expiration transitions from live to missing.
	check(t, db.Set(key, val, dot.WithExpiration(time.Now().Add(25*time.Millisecond))))
	equal(t, db, key, val)
	time.Sleep(35 * time.Millisecond)
	expectMissing(t, db, key)

	// Open with a default expiration applies to Set unless overridden.
	dbWithDefault := newDB(t, dot.WithExpiration(time.Now().Add(-time.Second)))
	check(t, dbWithDefault.Set(key, val))
	expectMissing(t, dbWithDefault, key)
	check(t, dbWithDefault.Set(key, val, dot.WithExpiration(time.Now().Add(time.Hour))))
	equal(t, dbWithDefault, key, val)
	check(t, dbWithDefault.Set(key, val, dot.WithExpiration(time.Time{})))
	equal(t, dbWithDefault, key, val)
}

func TestLockFileDoubleOpen(t *testing.T) {
	dir := t.TempDir()
	db1, err := dot.Open(dir)
	check(t, err)
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

func TestStringValues(t *testing.T) {
	db := newDB(t)
	key := []byte("strkey")
	if err := db.Set(key, "hello"); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get[string](key); err != nil || got != "hello" {
		t.Fatalf("Get[string]=%q error=%v", got, err)
	}
	if err := db.Set(key, ""); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get[string](key); err != nil || got != "" {
		t.Fatalf("empty string: got %q error=%v", got, err)
	}
	// String and []byte are distinct value types.
	if _, err := db.Get[[]byte](key); !errors.Is(err, dot.ErrTypeMismatch) {
		t.Fatalf("Get[[]byte] on string: error=%v, want ErrTypeMismatch", err)
	}
	if err := db.Set([]byte("byteskey"), []byte("raw")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get[string]([]byte("byteskey")); !errors.Is(err, dot.ErrTypeMismatch) {
		t.Fatalf("Get[string] on []byte: error=%v, want ErrTypeMismatch", err)
	}
	if err := db.Update(key, func(cur string, exists bool) (string, error) { return cur + " world", nil }); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get[string](key); err != nil || got != " world" {
		t.Fatalf("after Update: got %q error=%v", got, err)
	}
}

func TestStringKeys(t *testing.T) {
	db := newDB(t)
	if err := db.Set("skey", uint64(42)); err != nil {
		t.Fatal(err)
	}
	// Key identity ignores the key's Go type.
	if got, err := db.Get[uint64]([]byte("skey")); err != nil || got != 42 {
		t.Fatalf("Get with []byte key: got %d error=%v", got, err)
	}
	if err := db.Update("skey", func(cur uint64, exists bool) (uint64, error) { return cur + 1, nil }); err != nil {
		t.Fatal(err)
	}
	if err := db.Update("skey", func(cur uint64, exists bool) (uint64, error) {
		if !exists || cur != 43 {
			t.Errorf("update saw cur=%d exists=%v", cur, exists)
		}
		return cur + 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get[uint64]("skey"); err != nil || got != 44 {
		t.Fatalf("final: got %d error=%v", got, err)
	}
	if err := db.Delete("skey"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get[uint64]([]byte("skey")); err != dot.ErrNotFound {
		t.Fatalf("after Delete: error=%v, want ErrNotFound", err)
	}
}

func TestScan(t *testing.T) {
	db := newDB(t)
	// Empty database yields nothing.
	for range db.Scan() {
		t.Fatal("empty scan yielded a pair")
	}
	const n = 200
	want := make(map[string]string, n)
	for i := range n {
		key := fmt.Sprintf("scan-%04d", i)
		value := fmt.Sprintf("value-%04d", i)
		if err := db.Set(key, value); err != nil {
			t.Fatal(err)
		}
		want[key] = value
	}
	// Scalars and expirations exercise the skip paths.
	if err := db.Set("scan-scalar", uint64(7)); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("scan-expired", "gone", dot.WithExpiration(time.Now().Add(-time.Hour))); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete("scan-0000"); err != nil {
		t.Fatal(err)
	}
	delete(want, "scan-0000")

	got := make(map[string]string)
	for k, v := range db.Scan() {
		got[string(k)] = string(v)
	}
	// The scalar is stored as 8 little-endian bytes; check presence only.
	if len(got) != len(want)+1 {
		t.Fatalf("scan yielded %d pairs, want %d", len(got), len(want)+1)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("key %q: got %q want %q", k, got[k], v)
		}
	}
	if _, ok := got["scan-expired"]; ok {
		t.Fatal("scan yielded an expired key")
	}

	// Early termination stops the scan.
	count := 0
	for range db.Scan() {
		count++
		if count == 3 {
			break
		}
	}
	if count != 3 {
		t.Fatalf("early break scanned %d pairs, want 3", count)
	}

	// A second scan sees the same content: no state leaks between scans.
	again := make(map[string]string)
	for k, v := range db.Scan() {
		again[string(k)] = string(v)
	}
	if len(again) != len(got) {
		t.Fatalf("second scan: got %d pairs, want %d", len(again), len(got))
	}
}

func TestDBUpdateMissingInvalidAndClosed(t *testing.T) {
	db := newDB(t)
	key := []byte("key")
	// A missing key calls fn with the zero value and exists=false; returning
	// a value inserts it.
	calls := 0
	if err := db.Update(key, func(v uint64, exists bool) (uint64, error) {
		calls++
		if exists || v != 0 {
			t.Fatalf("missing key: v=%d exists=%v", v, exists)
		}
		return 7, nil
	}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("callback called %d times, want 1", calls)
	}
	if got, err := db.Get[uint64](key); err != nil || got != 7 {
		t.Fatalf("insert: got %d error=%v", got, err)
	}
	// A callback error aborts without writing.
	sentinel := errors.New("abort")
	if err := db.Update(key, func(v uint64, exists bool) (uint64, error) {
		return 0, sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatalf("callback error: %v", err)
	}
	if got, err := db.Get[uint64](key); err != nil || got != 7 {
		t.Fatalf("aborted update changed value: got %d error=%v", got, err)
	}
	// Update-only callers return ErrNotFound themselves for missing keys.
	if err := db.Delete(key); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(key, func(v uint64, exists bool) (uint64, error) {
		if !exists {
			return 0, dot.ErrNotFound
		}
		return v + 1, nil
	}); err != dot.ErrNotFound {
		t.Fatalf("missing key with guard: %v", err)
	}
	// Type mismatch fails without calling fn.
	unexpected := func(v uint64, exists bool) (uint64, error) {
		t.Fatal("callback must not run")
		return v, nil
	}
	if err := db.Set(key, []byte("invalid")); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(key, unexpected); !errors.Is(err, dot.ErrTypeMismatch) {
		t.Fatalf("invalid scalar: %v", err)
	}
	// Expired keys look absent: fn sees exists=false and replaces the value.
	if err := db.Set(key, []byte("invalid"), dot.WithExpiration(time.Now().Add(-time.Second))); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(key, func(v uint64, exists bool) (uint64, error) {
		if exists || v != 0 {
			t.Fatalf("expired key: v=%d exists=%v", v, exists)
		}
		return 42, nil
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get[uint64](key); err != nil || got != 42 {
		t.Fatalf("replaced expired: got %d error=%v", got, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(key, unexpected); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed database: %v", err)
	}
}

func TestDBGetValueOwnership(t *testing.T) {
	for _, size := range []int{0, 128, (4 << 10), 64 << 10} {
		t.Run(fmt.Sprintf("%dB", size), func(t *testing.T) {
			db := newDB(t)
			key := []byte("key")
			value := bytes.Repeat([]byte{'v'}, size)
			inputKey, inputValue := bytes.Clone(key), bytes.Clone(value)
			if err := db.Set(inputKey, inputValue); err != nil {
				t.Fatal(err)
			}
			clear(inputKey)
			clear(inputValue)
			first, err := db.Get[[]byte](key)
			if err != nil || first == nil || !bytes.Equal(first, value) {
				t.Fatalf("first get: value=%q error=%v", first, err)
			}
			retained, err := db.Get[[]byte](key)
			if err != nil || !bytes.Equal(retained, value) {
				t.Fatalf("second get: value=%q error=%v", retained, err)
			}
			if len(first) > 0 {
				first[0] ^= 1
				if !bytes.Equal(retained, value) {
					t.Fatal("Get results share storage")
				}
			}
			updated := bytes.Repeat([]byte{'u'}, 128)
			if err := db.Set(key, updated); err != nil {
				t.Fatal(err)
			}
			if got, err := db.Get[[]byte](key); err != nil || !bytes.Equal(got, updated) {
				t.Fatalf("updated get: value=%q error=%v", got, err)
			}
			if err := db.Delete(key); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(retained, value) {
				t.Fatal("later operations changed a retained Get result")
			}
			if got, err := db.Get[[]byte](key); err != dot.ErrNotFound || got != nil {
				t.Fatalf("deleted key: value=%q error=%v", got, err)
			}
		})
	}
}

func TestDBUpdatePanic(t *testing.T) {
	db := newDB(t)
	check(t, db.Set("panic", []byte("original")))
	func() {
		defer func() {
			if got := recover(); got != "callback panic" {
				t.Fatalf("unexpected panic: %v", got)
			}
		}()
		_ = db.Update("panic", func(v []byte, _ bool) ([]byte, error) {
			clear(v)
			panic("callback panic")
		})
	}()
	if got, err := db.Get[[]byte]("panic"); err != nil || string(got) != "original" {
		t.Fatalf("panic changed value or retained lock: %q, %v", got, err)
	}
	check(t, db.Set("panic", "usable"))
}

func TestDBUpdateByteOwnership(t *testing.T) {
	for _, size := range []int{0, 128, 8192} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			db := newDB(t)
			initial := bytes.Repeat([]byte{'a'}, size)
			check(t, db.Set("bytes", initial))
			var retained []byte
			check(t, db.Update("bytes", func(v []byte, exists bool) ([]byte, error) {
				if !exists || !bytes.Equal(v, initial) {
					t.Fatalf("callback value=%q exists=%t", v, exists)
				}
				retained = append(v, 'b')
				return retained, nil
			}))
			clear(retained)
			want := append(bytes.Clone(initial), 'b')
			if got, err := db.Get[[]byte]("bytes"); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("callback result aliases stored value: %q, %v", got, err)
			}
			check(t, db.Update("bytes", func(_ []byte, _ bool) ([]byte, error) { return nil, nil }))
			if got, err := db.Get[[]byte]("bytes"); err != nil || got == nil || len(got) != 0 {
				t.Fatalf("nil callback result must store empty bytes: %v, %v", got, err)
			}
		})
	}
}

func TestDBUpdateExpiration(t *testing.T) {
	db := newDB(t, dot.WithExpiration(time.Unix(-1, 0)))
	deadline := time.Now().Add(200 * time.Millisecond)
	for _, key := range []string{"preserved", "cleared", "overridden"} {
		check(t, db.Set(key, uint64(1), dot.WithExpiration(deadline)))
	}
	increment := func(v uint64, _ bool) (uint64, error) { return v + 1, nil }
	check(t, db.Update("preserved", increment))
	check(t, db.Update("cleared", increment, dot.WithExpiration(time.Time{})))
	check(t, db.Update("overridden", increment, dot.WithExpiration(time.Unix(-1, 0))))
	check(t, db.Update("inserted", increment, dot.WithExpiration(deadline)))
	check(t, db.Update("default", increment))
	for _, key := range []string{"preserved", "inserted", "cleared"} {
		if _, err := db.Get[uint64](key); err != nil {
			t.Fatalf("%s expired before its deadline: %v", key, err)
		}
	}
	for _, key := range []string{"default", "overridden"} {
		if _, err := db.Get[uint64](key); err != dot.ErrNotFound {
			t.Fatalf("%s: error=%v; want ErrNotFound", key, err)
		}
	}
	time.Sleep(max(0, time.Until(deadline)) + time.Millisecond)
	for _, key := range []string{"preserved", "inserted"} {
		if _, err := db.Get[uint64](key); err != dot.ErrNotFound {
			t.Fatalf("%s lost its expiration: %v", key, err)
		}
	}
	if got, err := db.Get[uint64]("cleared"); err != nil || got != 2 {
		t.Fatalf("cleared expiration: value=%d error=%v", got, err)
	}
}

func TestDurabilityInvalid(t *testing.T) {
	for _, d := range []dot.Durability{dot.Sync | dot.Buffer, 255} {
		path := filepath.Join(t.TempDir(), "invalid")
		if db, err := dot.Open(path, dot.WithDurability(d)); err == nil || db != nil {
			t.Fatalf("dot.Open accepted durability %d", d)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("invalid option created database: %v", err)
		}
		db := newDB(t)
		if err := db.Set("key", uint64(1), dot.WithDurability(d)); err == nil {
			t.Fatal("Set accepted invalid durability")
		}
		if err := db.Update("key", func(v uint64, exists bool) (uint64, error) {
			t.Fatal("invalid Update invoked callback")
			return v, nil
		}, dot.WithDurability(d)); err == nil {
			t.Fatal("Update accepted invalid durability")
		}
		if err := db.Delete("missing", dot.WithDurability(d)); err == nil {
			t.Fatal("Delete accepted invalid durability")
		}
	}
}
