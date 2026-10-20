package main

import (
	"context"
	"testing"
)

func TestOpenMultipleFullSegments(t *testing.T) {
	var previous *result
	for _, name := range []string{"dot", "pebble"} {
		t.Run(name, func(t *testing.T) {
			c := caseConfig{
				Engine: name, Workload: "mixed", ValueBytes: 4096,
				Workers: 1, Keys: 128, Operations: 60, Warmup: 6,
				Seed: 7, Path: t.TempDir(), OpenSegments: 2,
			}
			out, err := runCase(context.Background(), c)
			if err != nil {
				t.Fatal(err)
			}
			if out.OpenSeconds <= 0 || out.OpenPreparationSeconds <= 0 || out.OpenHistoryBytes < 2*dotSegmentBytes {
				t.Fatalf("missing large-history open measurement: %+v", out)
			}
			if name == "dot" && out.OpenFullSegments < 2 {
				t.Fatalf("only %d full Dot segments", out.OpenFullSegments)
			}
			if out.Latency.Count != uint64(c.Operations) || out.LiveKeys < c.Keys || out.LiveKeys > c.Keys+c.Workers {
				t.Fatal("open preparation changed operation counts or live population")
			}
			if previous != nil && (out.OpenHistoryBytes != previous.OpenHistoryBytes || out.OpenHistoryRecords != previous.OpenHistoryRecords) {
				t.Fatal("engines received different update histories")
			}
			previous = &out
		})
	}
}

func TestCountFullDotSegmentsRejectsShortFiles(t *testing.T) {
	// The ordinary small fixture must not satisfy the full-segment requirement.
	c := caseConfig{
		Engine: "dot", Workload: "mixed", ValueBytes: 4096,
		Workers: 1, Keys: 128, Operations: 60, Seed: 7, Path: t.TempDir(),
	}
	if _, err := runCase(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if full, err := countFullDotSegments(c.Path, 0); err != nil || full != 0 {
		t.Fatalf("short fixture: full segments=%d error=%v", full, err)
	}
}
