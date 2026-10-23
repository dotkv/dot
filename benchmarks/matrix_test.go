package main

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestBenchmarkMatrix(t *testing.T) {
	base := caseConfig{Keys: 256, Warmup: 64, Scalar: true, Seed: 1}
	cases, err := buildCases(base, []string{"dot", "pebble", "pogreb", "bitcask"}, nil, nil, []int{8}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(cases) != 32 {
		t.Fatalf("got %d cases, want 32", len(cases))
	}
	seen := make(map[scenario]int)
	for _, c := range cases {
		if c.KeyBytes != 16 || c.Workers != 8 || c.Scalar != (c.ValueBytes == 8) || c.CalibrationDuration != 0 ||
			c.DotTarget != 15*time.Second || (c.ValueBytes != 8 && c.ValueBytes != 1024) ||
			(c.Workload != "80/20" && c.Workload != "20/80") {
			t.Fatalf("invalid measured case: %+v", c)
		}
		seen[scenarioFor(c)]++
	}
	for key, count := range seen {
		if count != 8 {
			t.Fatalf("%+v has %d cases, want four engines and two repetitions", key, count)
		}
	}
	for start := 0; start < len(cases)/2; start += 4 {
		if cases[start].Engine == cases[start+len(cases)/2].Engine {
			t.Fatal("engine order did not rotate across repetitions")
		}
	}
	cases, err = buildCases(caseConfig{Keys: 16, Operations: 96}, []string{"dot"}, []string{"20/80"}, []int{1024}, []int{8}, 1)
	if err != nil || len(cases) != 1 {
		t.Fatalf("explicit overrides: %d cases, %v", len(cases), err)
	}
	c := cases[0]
	if c.Operations != 96 || c.DotTarget != 0 || c.Workload != "20/80" || c.ValueBytes != 1024 {
		t.Fatalf("explicit overrides ignored: %+v", c)
	}
	for _, workload := range []string{"0/100", "10/90", "50/50", "90/10", "100/0", "mixed", "read-heavy", "write-heavy"} {
		if _, err := buildCases(base, []string{"dot"}, []string{workload}, nil, []int{8}, 1); err == nil {
			t.Fatalf("accepted removed workload %q", workload)
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
		if _, err := buildCases(c, []string{"dot"}, nil, nil, []int{workers}, 1); err == nil {
			t.Fatalf("matrix accepted %d workers", workers)
		}
		if _, err := buildCases(c, []string{"dot"}, nil, nil, []int{8, workers}, 1); err == nil {
			t.Fatalf("matrix accepted mixed worker counts including %d", workers)
		}
		// The internal -case path calls runCase directly, bypassing buildCases.
		if _, err := runCase(context.Background(), c); err == nil {
			t.Fatalf("isolated case accepted %d workers", workers)
		}
		c.Operations, c.CalibrationDuration = 0, time.Millisecond
		if _, err := runCase(context.Background(), c); err == nil {
			t.Fatalf("calibration accepted %d workers", workers)
		}
	}
}
