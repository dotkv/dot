// Command benchmarks compares embedded engines in isolated Linux processes.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := command(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func command(ctx context.Context) error {
	engineList := flag.String("engines", "dot,pebble", "comma-separated engines: dot, pebble")
	workloadList := flag.String("workloads", "80/20,20/80", "comma-separated read/write ratios: 80/20, 20/80")
	sizeList := flag.String("sizes", "8,1024", "comma-separated value sizes in bytes")
	workerList := flag.String("workers", "16", "comma-separated worker counts (1, 16, 128)")
	keys := flag.Int("keys", 16384, "total preloaded live keys per case")
	ops := flag.Int("ops", 0, "fixed measured operations per case; overrides Dot calibration")
	target := flag.Duration("target", 0, "Dot time target for shared operation budgets (default 15s)")
	timeout := flag.Duration("timeout", 10*time.Minute, "timeout for each process, including preparation and drain")
	warmup := flag.Int("warmup", 30000, "total untimed warmup operations per case")
	seed := flag.Uint64("seed", 1, "deterministic workload seed")
	scalar := flag.Bool("scalar", true, "use uint64 operations for 8-byte values; false uses byte slices")
	repeat := flag.Int("repeat", 1, "repetitions of each case")
	dir := flag.String("dir", ".", "parent directory for temporary databases; choose the disk to measure")
	jsonOutput := flag.Bool("json", false, "emit one JSON result per line, including operation breakdowns")
	fromJSON := flag.String("from-json", "", "generate README tables from saved JSON results instead of running benchmarks")
	child := flag.String("case", "", "internal: run one isolated case encoded as JSON")
	flag.Parse()
	if flag.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flag.Args())
	}
	if *child != "" {
		var c caseConfig
		if err := json.Unmarshal([]byte(*child), &c); err != nil {
			return err
		}
		out, err := runCase(ctx, c)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(out)
	}
	if *fromJSON != "" {
		results, err := loadResults(*fromJSON)
		if err != nil {
			return err
		}
		if err := writeReadme("README.md", results); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "Updated result tables in README.md from %s\n", *fromJSON)
		return nil
	}
	if *repeat < 1 || *ops < 0 || *target < 0 || *timeout <= 0 {
		return fmt.Errorf("repeat and timeout must be positive; ops and target must be nonnegative")
	}
	if *ops > 0 && *target > 0 {
		return fmt.Errorf("-ops and -target are mutually exclusive")
	}
	if err := checkReadme("README.md"); err != nil {
		return err
	}
	names, err := csv(*engineList)
	if err != nil {
		return err
	}
	workloadNames, err := csv(*workloadList)
	if err != nil {
		return err
	}
	sizes, err := integers(*sizeList)
	if err != nil {
		return err
	}
	concurrency, err := integers(*workerList)
	if err != nil {
		return err
	}
	cases, err := buildCases(caseConfig{
		Keys: *keys, Operations: *ops, DotTarget: *target, Warmup: *warmup, Seed: *seed, Scalar: *scalar,
	}, names, workloadNames, sizes, concurrency, *repeat)
	if err != nil {
		return err
	}
	parentDir, err := filepath.Abs(*dir)
	if err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	if !*jsonOutput {
		fmt.Fprintf(os.Stdout, "%-8s %-11s %7s %7s %8s %10s %10s %12s %9s %9s %9s\n",
			"ENGINE", "READ/WRITE", "VALUE", "WORKERS", "SECONDS", "MEAN us", "P99 us", "OPS/s", "PEAK MiB", "DISK MiB", "DRAIN s")
	}
	var results []result
	budgets := make(map[scenario]int)
	run := func(c caseConfig) (result, error) {
		caseCtx, cancel := context.WithTimeout(ctx, *timeout)
		defer cancel()
		return isolatedCase(caseCtx, executable, parentDir, c)
	}
	for i, c := range cases {
		if *ops == 0 {
			key := scenarioFor(c)
			if budgets[key] == 0 {
				fmt.Fprintf(os.Stderr, "Calibrating Dot: %s %dB workers=%d target=%s\n", c.Workload, c.ValueBytes, c.Workers, c.DotTarget)
				probe := c
				probe.Engine, probe.Operations, probe.CalibrationDuration = "dot", 0, calibrationDuration
				pilot, err := run(probe)
				if err != nil {
					return fmt.Errorf("Dot calibration: %w", err)
				}
				budgets[key], err = operationBudget(pilot, c.DotTarget, c.Workers)
				if err != nil {
					return err
				}
			}
			c.Operations = budgets[key]
		}
		fmt.Fprintf(os.Stderr, "[%d/%d] %s %s %dB scalar=%v workers=%d repeat=%d ops=%d\n", i+1, len(cases), c.Engine, c.Workload, c.ValueBytes, c.Scalar, c.Workers, c.Repetition, c.Operations)
		out, err := run(c)
		if err != nil {
			return err
		}
		results = append(results, out)
		if *jsonOutput {
			if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
				return err
			}
		} else {
			value := strconv.Itoa(c.ValueBytes) + "B"
			if c.Scalar {
				value = "uint64"
			}
			_, err := fmt.Fprintf(os.Stdout, "%-8s %-11s %7s %7d %8.3f %10.3f %10.3f %12.0f %9.1f %9.2f %9.3f\n",
				c.Engine, c.Workload, value, c.Workers, out.ElapsedSeconds, out.Latency.MeanNS/1000, float64(out.Latency.P99NS)/1000,
				out.OpsPerSecond, float64(out.RSSPeakBytes)/(1<<20),
				float64(out.DiskFinal.AllocatedBytes)/(1<<20), out.DrainSeconds)
			if err != nil {
				return err
			}
		}
	}
	if err := writeReadme("README.md", results); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "Updated result tables in README.md")
	return nil
}

func isolatedCase(ctx context.Context, executable, parent string, c caseConfig) (out result, err error) {
	c.Path, err = os.MkdirTemp(parent, ".dot-bench-")
	if err != nil {
		return out, err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(c.Path)) }()
	payload, err := json.Marshal(c)
	if err != nil {
		return out, err
	}
	cmd := exec.CommandContext(ctx, executable, "-case", string(payload))
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return out, fmt.Errorf("%s case: %w", c.Engine, errors.Join(err, ctx.Err()))
	}
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		return out, fmt.Errorf("decode %s result: %w", c.Engine, err)
	}
	return out, nil
}

func csv(value string) ([]string, error) {
	parts := strings.Split(value, ",")
	seen := make(map[string]bool)
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
		if parts[i] == "" || seen[parts[i]] {
			return nil, fmt.Errorf("list %q contains an empty or duplicate entry", value)
		}
		seen[parts[i]] = true
	}
	return parts, nil
}

func integers(value string) ([]int, error) {
	parts, err := csv(value)
	if err != nil {
		return nil, err
	}
	values := make([]int, len(parts))
	seen := make(map[int]bool)
	for i, part := range parts {
		values[i], err = strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid integer %q: %w", part, err)
		}
		if seen[values[i]] {
			return nil, fmt.Errorf("duplicate integer %q", part)
		}
		seen[values[i]] = true
	}
	return values, nil
}
