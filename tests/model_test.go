package dot_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/dotkv/dot"
)

// The model stores logical values, not Dot's indexes, record offsets, or format
// tags. A four-byte instruction selects operation, key, value, and durability.
// Keeping this runner deterministic lets fuzzing save and minimize failures.
func runDBModel(t *testing.T, trace []byte, opts ...dot.Option) {
	t.Helper()
	defer func() {
		if t.Failed() {
			t.Logf("operation trace (hex): %x", trace)
		}
	}()
	path := t.TempDir()
	db, err := dot.Open(path, opts...)
	check(t, err)
	t.Cleanup(func() { check(t, db.Close()) })
	values := valueCases()
	keys := []string{"", "\x00", "key", string(bytes.Repeat([]byte{'k'}, 1100))}
	for i := len(keys); i < 32; i++ {
		keys = append(keys, fmt.Sprintf("key-%02d", i))
	}
	want := make(map[string]valueCase)
	verify := func() {
		t.Helper()
		seen := make(map[string]bool)
		for k, raw := range db.Scan() {
			key := string(k)
			v, exists := want[key]
			if !exists || seen[key] || !bytes.Equal(raw, v.raw) {
				t.Fatalf("Scan(%q) = %x; expected live=%t duplicate=%t value=%x", key, raw, exists, seen[key], v.raw)
			}
			seen[key] = true
		}
		if len(seen) != len(want) {
			t.Fatalf("Scan returned %d keys; want %d", len(seen), len(want))
		}
		for _, key := range keys {
			if v, exists := want[key]; exists {
				v.check(t, db, key)
			} else {
				for _, reader := range values {
					reader.missing(t, db, key)
				}
			}
		}
	}
	reopen := func() {
		t.Helper()
		check(t, db.Close())
		next, err := dot.Open(path, opts...)
		check(t, err)
		db = next
		verify()
	}
	for step := 0; step+4 <= len(trace); step += 4 {
		instruction := trace[step : step+4]
		key := keys[int(instruction[1])%len(keys)]
		v := values[int(instruction[2])%len(values)]
		previous, exists := want[key]
		var writeOpts []dot.Option
		if instruction[3]&1 != 0 {
			modes := [...]dot.Durability{dot.Buffer, dot.Write, dot.Sync}
			writeOpts = append(writeOpts, dot.WithDurability(modes[int(instruction[3]>>1)%len(modes)]))
		}
		switch instruction[0] % 12 {
		case 0, 1: // Set can change a key's stored type.
			check(t, v.set(db, key, writeOpts...))
			want[key] = v
		case 2:
			if exists {
				previous.check(t, db, key)
			} else {
				v.missing(t, db, key)
			}
		case 3:
			check(t, db.Delete([]byte(key), writeOpts...))
			delete(want, key)
		case 4:
			check(t, v.set(db, key, append(writeOpts, dot.WithExpiration(time.Unix(-1, 0)))...))
			delete(want, key)
		case 5:
			if exists && previous.kind != v.kind {
				v.access(t, db, key, previous.kind)
			} else {
				var current *valueCase
				if exists {
					current = &previous
				}
				v.update(t, db, key, current, writeOpts...)
				want[key] = v
			}
		case 6:
			if exists {
				previous.abort(t, db, key)
			}
		case 7:
			if exists {
				v.access(t, db, key, previous.kind)
			}
		case 8:
			reopen()
		case 9:
			verify()
		case 10:
			if exists {
				check(t, previous.set(db, key, writeOpts...))
			}
		case 11: // Expiration wins over the previous type on Update.
			check(t, v.set(db, key, append(writeOpts, dot.WithExpiration(time.Unix(-1, 0)))...))
			replacement := values[(int(instruction[2])+1)%len(values)]
			replacement.update(t, db, key, nil, writeOpts...)
			want[key] = replacement
		}
		if current, exists := want[key]; exists {
			current.check(t, db, key)
		} else {
			v.missing(t, db, key)
		}
	}
	verify()
	reopen() // Every trace must verify persistence, even without opcode 8.
}

func TestDBModel(t *testing.T) {
	for _, config := range []struct {
		name string
		opts []dot.Option
	}{
		{"defaults", nil},
		{"buffer/single-shard", []dot.Option{dot.WithShardCount(1)}},
		{"write/four-shards", []dot.Option{dot.WithShardCount(4), dot.WithDurability(dot.Write)}},
		{"sync/single-shard", []dot.Option{dot.WithShardCount(1), dot.WithDurability(dot.Sync)}},
	} {
		for _, seed := range []uint64{1, 23} {
			t.Run(fmt.Sprintf("%s/seed=%d", config.name, seed), func(t *testing.T) {
				random := rand.New(rand.NewPCG(seed, seed+1))
				trace := make([]byte, 4*256)
				for i := range trace {
					trace[i] = byte(random.Uint32())
				}
				runDBModel(t, trace, config.opts...)
			})
		}
	}
}

func FuzzDBOperations(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 5, 0, 1, 1, 8, 0, 0, 0, 3, 0, 0, 0})
	f.Add([]byte{0, 2, 8, 0, 7, 2, 9, 0, 11, 2, 4, 0, 8, 0, 0, 0})
	f.Fuzz(func(t *testing.T, trace []byte) {
		// Bound disk usage and reopen work independently of fuzz input size.
		runDBModel(t, trace[:min(len(trace), 1024)], dot.WithShardCount(1))
	})
}

// Repeatedly grow a live key set, replace its values, delete most of it, and
// reopen. Background maintenance runs normally throughout this public workload.
// Exact growth/shrink and compaction publication boundaries have focused tests.
func TestDBChurn(t *testing.T) {
	for _, mode := range []dot.Durability{dot.Buffer, dot.Write} {
		t.Run(fmt.Sprintf("durability=%d", mode), func(t *testing.T) {
			path := t.TempDir()
			opts := []dot.Option{dot.WithShardCount(1), dot.WithDurability(mode), dot.WithCompactionThreshold(0.1, 0)}
			db, err := dot.Open(path, opts...)
			check(t, err)
			t.Cleanup(func() { check(t, db.Close()) })
			const count = 16000
			for round := range 3 {
				for i := range count {
					check(t, db.Set(fmt.Sprint(i), uint64(round*count+i)))
				}
				for i := range count {
					key := fmt.Sprint(i)
					if i%4 == 0 {
						check(t, db.Update(key, func(v uint64, exists bool) (uint64, error) {
							if !exists || v != uint64(round*count+i) {
								t.Fatalf("Update(%q) = %d, exists=%t", key, v, exists)
							}
							return v + 1, nil
						}))
					} else {
						check(t, db.Delete(key))
					}
				}
				check(t, db.Close())
				next, err := dot.Open(path, opts...)
				check(t, err)
				db = next
				for i := range count {
					v, err := db.Get[uint64](fmt.Sprint(i))
					if i%4 == 0 {
						if err != nil || v != uint64(round*count+i+1) {
							t.Fatalf("round %d key %d: value=%d error=%v", round, i, v, err)
						}
					} else if err != dot.ErrNotFound || v != 0 {
						t.Fatalf("round %d deleted key %d reappeared: value=%d error=%v", round, i, v, err)
					}
				}
				scanned := 0
				for range db.Scan() {
					scanned++
				}
				if scanned != count/4 {
					t.Fatalf("round %d: Scan returned %d keys; want %d", round, scanned, count/4)
				}
			}
		})
	}
}

// Kill a writer after acknowledged Sync mutations, without calling Close.
// This exercises process-crash recovery; it does not simulate a power failure.
func TestDBCrashRecovery(t *testing.T) {
	const directoryEnv = "DOT_TEST_CRASH_RECOVERY_DIR"
	const count = 64
	if path := os.Getenv(directoryEnv); path != "" {
		db, err := dot.Open(path, dot.WithShardCount(1), dot.WithDurability(dot.Sync), dot.WithCompactionThreshold(0.1, 0))
		check(t, err)
		for i := range count {
			key := fmt.Sprint(i)
			check(t, db.Set(key, uint64(i)))
			check(t, db.Update(key, func(v uint64, _ bool) (uint64, error) { return v + 1, nil }))
			if i%3 == 0 {
				check(t, db.Delete(key))
			}
		}
		fmt.Println("acknowledged")
		select {}
	}
	path := t.TempDir()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDBCrashRecovery$")
	cmd.Env = append(os.Environ(), directoryEnv+"="+path)
	stdout, err := cmd.StdoutPipe()
	check(t, err)
	check(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || line != "acknowledged\n" {
		t.Fatalf("writer acknowledgement = %q, %v", line, err)
	}
	check(t, cmd.Process.Kill())
	_ = cmd.Wait()
	db, err := dot.Open(path)
	check(t, err)
	t.Cleanup(func() { check(t, db.Close()) })
	for i := range count {
		v, err := db.Get[uint64](fmt.Sprint(i))
		if i%3 == 0 {
			if err != dot.ErrNotFound || v != 0 {
				t.Fatalf("deleted key %d survived the crash: value=%d error=%v", i, v, err)
			}
		} else if err != nil || v != uint64(i+1) {
			t.Fatalf("acknowledged key %d lost after crash: value=%d error=%v", i, v, err)
		}
	}
}
