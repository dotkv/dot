package main

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestProfileMatrices(t *testing.T) {
	base := caseConfig{Keys: 256, Warmup: 64, Scalar: true, Seed: 1}
	for _, name := range []string{"fast", "slow", "all"} {
		t.Run(name, func(t *testing.T) {
			cases, err := buildCases(name, base, []string{"dot", "pebble"}, nil, nil, []int{16}, 2)
			if err != nil {
				t.Fatal(err)
			}
			want := map[string]int{"fast": 40, "slow": 4, "all": 44}[name]
			if len(cases) != want {
				t.Fatalf("got %d cases, want %d", len(cases), want)
			}
			seen := make(map[scenario]int)
			for _, c := range cases {
				if c.KeyBytes != 16 || c.Scalar != (c.ValueBytes == 8) || c.CalibrationDuration != 0 {
					t.Fatalf("invalid measured case: %+v", c)
				}
				if c.Profile == "fast" && (!c.DisableCompaction || c.DotTarget != 3*time.Second || (c.ValueBytes != 8 && c.ValueBytes != 1024)) {
					t.Fatalf("invalid fast configuration: %+v", c)
				}
				if c.Profile == "slow" && (c.DisableCompaction || c.DotTarget != 45*time.Second || c.ValueBytes != 128 || c.Workload != "50/50") {
					t.Fatalf("invalid slow configuration: %+v", c)
				}
				seen[scenarioFor(c)]++
			}
			for key, count := range seen {
				if count != 4 {
					t.Fatalf("%+v has %d cases, want both engines and repetitions", key, count)
				}
			}
			for start := 0; start < len(cases)/2; start += 2 {
				if cases[start].Engine == cases[start+len(cases)/2].Engine {
					t.Fatal("engine order did not rotate across repetitions")
				}
			}
		})
	}
	cases, err := buildCases("all", caseConfig{Keys: 16, Operations: 96}, []string{"dot"}, []string{"100/0"}, []int{1024}, []int{1}, 1)
	if err != nil || len(cases) != 2 {
		t.Fatalf("explicit overrides: %d cases, %v", len(cases), err)
	}
	for _, c := range cases {
		if c.Operations != 96 || c.DotTarget != 0 || c.Workload != "100/0" || c.ValueBytes != 1024 {
			t.Fatalf("explicit overrides ignored: %+v", c)
		}
	}
}

func TestDotCalibrationBudget(t *testing.T) {
	c := caseConfig{
		Engine: "dot", Workload: "50/50", ValueBytes: 8, Scalar: true,
		Workers: 16, Keys: 256, Warmup: 300, DisableCompaction: true,
		CalibrationDuration: 50 * time.Millisecond, Path: t.TempDir(),
	}
	pilot, err := runCase(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if pilot.ElapsedSeconds < c.CalibrationDuration.Seconds() || pilot.Latency.Count == 0 {
		t.Fatalf("invalid calibration: %+v", pilot)
	}
	budget, err := operationBudget(pilot, time.Second, c.Workers)
	if err != nil {
		t.Fatal(err)
	}
	want := float64(pilot.Latency.Count) / pilot.ElapsedSeconds
	if math.Abs(float64(budget)-want) >= float64(c.Workers*3) {
		t.Fatalf("budget %d does not follow Dot's measured rate %f", budget, want)
	}
	for _, name := range []string{"dot", "pebble"} {
		measured := c
		measured.Engine, measured.CalibrationDuration, measured.Operations = name, 0, 1920
		measured.Path = t.TempDir()
		out, err := runCase(context.Background(), measured)
		if err != nil {
			t.Fatal(err)
		}
		if out.Latency.Count != 1920 || out.Config.CalibrationDuration != 0 {
			t.Fatalf("%s did not execute the fixed budget", name)
		}
	}
	for _, invalid := range []result{{}, {Latency: latencySummary{Count: 1}, ElapsedSeconds: math.Inf(1)}} {
		if _, err := operationBudget(invalid, time.Second, 16); err == nil {
			t.Fatal("accepted invalid calibration")
		}
	}
}

func TestPureWorkloads(t *testing.T) {
	for _, name := range []string{"dot", "pebble"} {
		for _, workload := range []string{"0/100", "100/0"} {
			c := caseConfig{
				Engine: name, Workload: workload, ValueBytes: 8, Scalar: true,
				Workers: 16, Keys: 256, Operations: 1920, Warmup: 96,
				DisableCompaction: true, Path: t.TempDir(),
			}
			out, err := runCase(context.Background(), c)
			if err != nil {
				t.Fatal(err)
			}
			reads := out.ByOperation["read_hit"].Count + out.ByOperation["read_miss"].Count
			if (workload == "0/100" && reads != 0) || (workload == "100/0" && reads != uint64(c.Operations)) {
				t.Fatalf("%s %s: unexpected reads %d", name, workload, reads)
			}
		}
	}
}

func TestPebbleCompactionDisabled(t *testing.T) {
	c := caseConfig{Path: t.TempDir(), DisableCompaction: true}
	db, err := engines["pebble"](c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	// Cross the normal L0 write-stop threshold with overlapping tables. The
	// disabled profile must still make progress, with flushes but no compaction.
	for i := range 16 {
		if err := db.SetUint64([]byte("key"), uint64(i)); err != nil {
			t.Fatal(err)
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	m := db.(*pebbleEngine).db.Metrics()
	if m.Compact.Count != 0 || m.Levels[0].Sublevels < 12 {
		t.Fatalf("compactions=%d, L0 sublevels=%d", m.Compact.Count, m.Levels[0].Sublevels)
	}
	if pebbleOptions(caseConfig{}).DisableAutomaticCompactions || pebbleOptions(caseConfig{}).L0StopWritesThreshold == fastL0StopWritesThreshold {
		t.Fatal("normal profile lost Pebble defaults")
	}
}
