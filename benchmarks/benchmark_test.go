package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/dotkv/dot"
)

func TestEngineContracts(t *testing.T) {
	for _, name := range []string{"dot", "pebble"} {
		t.Run(name, func(t *testing.T) {
			db, err := engines[name](caseConfig{Path: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			key := []byte("key")
			if _, found, err := db.Get(key); found || err != nil {
				t.Fatalf("missing bytes: found=%v err=%v", found, err)
			}
			if _, found, err := db.GetUint64(key); found || err != nil {
				t.Fatalf("missing scalar: found=%v err=%v", found, err)
			}
			for _, size := range []int{8, 128, 4096, 65536} {
				value := make([]byte, size)
				for i := range value {
					value[i] = byte(mix64(uint64(i)))
				}
				want := bytes.Clone(value)
				if err := db.Set(key, value); err != nil {
					t.Fatal(err)
				}
				clear(value)
				got, found, err := db.Get(key)
				if err != nil || !found || !bytes.Equal(got, want) {
					t.Fatalf("read %d bytes: found=%v err=%v", size, found, err)
				}
				clear(got)
				got, found, err = db.Get(key)
				if err != nil || !found || !bytes.Equal(got, want) {
					t.Fatalf("result ownership for %d bytes: found=%v err=%v", size, found, err)
				}
			}
			const scalar = uint64(0xfedcba9876543210)
			if err := db.SetUint64(key, scalar); err != nil {
				t.Fatal(err)
			}
			if got, found, err := db.GetUint64(key); err != nil || !found || got != scalar {
				t.Fatalf("scalar: got=%x found=%v err=%v", got, found, err)
			}
			got, found, err := db.Get(key)
			if name == "dot" {
				if !errors.Is(err, dot.ErrTypeMismatch) || found || got != nil {
					t.Fatalf("scalar read as bytes: %x, found=%v, err=%v", got, found, err)
				}
			} else if err != nil || !found || len(got) != 8 || binary.LittleEndian.Uint64(got) != scalar {
				t.Fatalf("scalar encoding: %x, found=%v, err=%v", got, found, err)
			}
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if err := db.Delete(key); err != nil {
					t.Fatal(err)
				}
			}
			if _, found, err := db.Get(key); found || err != nil {
				t.Fatalf("deleted bytes: found=%v err=%v", found, err)
			}
			if _, found, err := db.GetUint64(key); found || err != nil {
				t.Fatalf("deleted scalar: found=%v err=%v", found, err)
			}
		})
	}
}

func TestWorkloadSemantics(t *testing.T) {
	for _, workers := range []int{1, 16, 128} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			c := caseConfig{Workload: "80/20", ValueBytes: 8, Scalar: true, Workers: workers, Keys: 2*workers + 1, Seed: 42}
			// Keep all historical keys to catch collisions between workers and
			// accidental reuse of an inserted key after deleting it.
			seen := make(map[[16]byte]bool)
			for id := range workers {
				w := newWorker(c, id)
				live := make(map[[16]byte]uint64)
				for _, entry := range w.live {
					value := w.prepare(operation{kind: insert, entry: entry})
					if seen[w.key] {
						t.Fatal("preloaded keys collide")
					}
					seen[w.key], live[w.key] = true, value
				}
				var counts [kindCount]int
				for range 2000 {
					op := w.next()
					value := w.prepare(op)
					old, exists := live[w.key]
					counts[op.kind]++
					switch op.kind {
					case readHit:
						if !exists || old != value {
							t.Fatal("hit targets absent key or stale value")
						}
					case readMiss:
						if exists || seen[w.key] {
							t.Fatal("miss targets a previously inserted key")
						}
					case insert:
						if exists || seen[w.key] {
							t.Fatal("insert reuses a key")
						}
						seen[w.key], live[w.key] = true, value
					case update:
						if !exists || old == value {
							t.Fatal("update is absent or unchanged")
						}
						live[w.key] = value
					case remove:
						if !exists {
							t.Fatal("delete targets absent key")
						}
						delete(live, w.key)
					}
					if len(live) < share(c.Keys, workers, id) || len(live) > share(c.Keys, workers, id)+1 {
						t.Fatal("live key count drifted")
					}
				}
				if max(counts[insert], counts[update], counts[remove])-min(counts[insert], counts[update], counts[remove]) > 1 {
					t.Fatalf("unbalanced mutations: %v", counts)
				}
				if counts[readMiss] == 0 || counts[readHit] <= counts[readMiss] {
					t.Fatalf("unexpected read distribution: %v", counts)
				}
			}
		})
	}
}

func TestRunCase(t *testing.T) {
	for _, mode := range []struct {
		workers, size int
		scalar        bool
	}{{1, 8, true}, {16, 8, false}, {128, 128, false}, {1, 65536, false}} {
		var previous *result
		for _, name := range []string{"dot", "pebble"} {
			t.Run(fmt.Sprintf("%s/%d/%d/scalar=%v", name, mode.workers, mode.size, mode.scalar), func(t *testing.T) {
				c := caseConfig{
					Engine: name, Workload: "80/20", ValueBytes: mode.size, Scalar: mode.scalar,
					Workers: mode.workers, Keys: 256, Operations: 3840, Warmup: 600, Seed: 7, Path: t.TempDir(),
				}
				out, err := runCase(context.Background(), c)
				if err != nil {
					t.Fatal(err)
				}
				if out.Latency.Count != uint64(c.Operations) || out.OpsPerSecond <= 0 || out.Latency.MeanNS <= 0 || out.Latency.P99NS <= 0 {
					t.Fatalf("invalid operation totals: %+v", out)
				}
				var total uint64
				var weightedMean float64
				for _, name := range operationNames {
					l := out.ByOperation[name]
					total += l.Count
					weightedMean += float64(l.Count) * l.MeanNS
					if l.Count == 0 || l.MeanNS <= 0 || l.P99NS <= 0 {
						t.Fatalf("invalid %s latency: %+v", name, l)
					}
					if previous != nil && l.Count != previous.ByOperation[name].Count {
						t.Fatalf("engine operation counts differ for %s", name)
					}
				}
				if total != out.Latency.Count || math.Abs(weightedMean/float64(total)-out.Latency.MeanNS) > 0.001 {
					t.Fatal("aggregate is not the weighted operation mean")
				}
				if out.RSSHarnessBytes == 0 || out.RSSPreparedBytes == 0 || out.RSSFinalBytes == 0 || out.RSSPeakBytes < out.RSSFinalBytes {
					t.Fatal("invalid RSS measurements")
				}
				if out.LiveKeys < c.Keys || out.LiveKeys > c.Keys+c.Workers {
					t.Fatal("invalid disk/live data measurements")
				}
				if out.DiskFinal.AllocatedBytes == 0 || out.SpaceAmplification <= 0 {
					t.Fatal("persistent engine reported no disk usage")
				}
				previous = &out
			})
		}
	}
}

func TestAllocatedDiskUsage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sparse")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(1 << 20); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := diskUsage(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := uint64(info.Sys().(*syscall.Stat_t).Blocks) * 512
	if got.FileBytes != 1<<20 || got.AllocatedBytes != want {
		t.Fatalf("disk usage %+v, want file size 1 MiB and allocation %d", got, want)
	}
}
