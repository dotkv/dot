package main

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestBenchmarkMatrix(t *testing.T) {
	cases := buildCases()
	if len(cases) != 8 {
		t.Fatalf("got %d cases, want 8", len(cases))
	}
	seen := make(map[scenario]map[string]bool)
	for _, c := range cases {
		if c.KeyBytes != 16 || c.Workers != 8 || c.Scalar != (c.ValueBytes == 8) ||
			c.Operations != 0 || c.CalibrationDuration != 0 || c.DotTarget != 15*time.Second ||
			c.Keys != 16384 || c.Warmup != 30000 || c.Seed != 1 ||
			(c.ValueBytes != 8 && c.ValueBytes != 1024) ||
			(c.Workload != "80/20" && c.Workload != "20/80") {
			t.Fatalf("invalid measured case: %+v", c)
		}
		key := scenarioFor(c)
		if seen[key] == nil {
			seen[key] = make(map[string]bool)
		}
		if seen[key][c.Engine] {
			t.Fatalf("duplicate engine %s in %+v", c.Engine, key)
		}
		seen[key][c.Engine] = true
	}
	if len(seen) != 4 {
		t.Fatalf("got %d configurations, want 4", len(seen))
	}
	for key, engines := range seen {
		if !engines["dot"] || !engines["pebble"] {
			t.Fatalf("%+v must compare Dot and Pebble", key)
		}
	}
	for start := 2; start < len(cases); start += 2 {
		if cases[start].Engine == cases[start-2].Engine {
			t.Fatal("engine order did not rotate across configurations")
		}
	}
}

func TestDotCalibrationBudget(t *testing.T) {
	c := caseConfig{
		Engine: "dot", Workload: "80/20", ValueBytes: 8, Scalar: true,
		Workers: 8, Keys: 256, Warmup: 300,
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
	for name := range engines {
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
		if _, err := operationBudget(invalid, time.Second, 8); err == nil {
			t.Fatal("accepted invalid calibration")
		}
	}
}

func TestWorkloadRatios(t *testing.T) {
	for name := range engines {
		for workload, readPercent := range workloads {
			t.Run(name+"/"+workload, func(t *testing.T) {
				c := caseConfig{
					Engine: name, Workload: workload, ValueBytes: 8, Scalar: true,
					Workers: 8, Keys: 256, Operations: 6000, Warmup: 96,
					Seed: 1, Path: t.TempDir(),
				}
				out, err := runCase(context.Background(), c)
				if err != nil {
					t.Fatal(err)
				}
				reads := out.ByOperation["read_hit"].Count + out.ByOperation["read_miss"].Count
				if ratio := float64(reads) / float64(c.Operations); math.Abs(ratio-float64(readPercent)/100) > 0.05 {
					t.Fatalf("unexpected read ratio %f", ratio)
				}
			})
		}
	}
}

func TestOnlyEightWorkersRun(t *testing.T) {
	for _, workers := range []int{0, 1, 4, 16, 128} {
		c := caseConfig{Engine: "dot", Workload: "80/20", ValueBytes: 8, Workers: workers, Keys: 256, Operations: 3840}
		if _, err := runCase(context.Background(), c); err == nil {
			t.Fatalf("isolated case accepted %d workers", workers)
		}
		c.Operations, c.CalibrationDuration = 0, time.Millisecond
		if _, err := runCase(context.Background(), c); err == nil {
			t.Fatalf("calibration accepted %d workers", workers)
		}
	}
}
