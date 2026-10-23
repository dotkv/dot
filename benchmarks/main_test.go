package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if os.Getenv(childProcessEnv) == "1" {
		main()
		return
	}
	os.Exit(m.Run())
}

func TestCommandRejectsArguments(t *testing.T) {
	original := os.Args
	t.Cleanup(func() { os.Args = original })
	for _, arg := range []string{"-help", "-json", "-case", "extra"} {
		os.Args = []string{original[0], arg}
		if err := command(context.Background()); err == nil || !strings.Contains(err.Error(), "accepts no arguments") {
			t.Fatalf("argument %q: %v", arg, err)
		}
	}
}

func TestIsolatedCase(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"dot", "pebble"} {
		t.Run(name, func(t *testing.T) {
			parent := t.TempDir()
			c := caseConfig{
				Engine: name, Workload: "80/20", ValueBytes: 8, Scalar: true,
				Workers: 8, Keys: 256, Operations: 1920, Warmup: 96, Seed: 1,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			out, err := isolatedCase(ctx, executable, parent, c)
			if err != nil {
				t.Fatal(err)
			}
			if out.Config.Engine != name || out.Latency.Count != uint64(c.Operations) || out.OpsPerSecond <= 0 {
				t.Fatalf("incorrect child result: %+v", out)
			}
			entries, err := os.ReadDir(parent)
			if err != nil || len(entries) != 0 {
				t.Fatalf("temporary database was not removed: %v, %v", entries, err)
			}
		})
	}
}
