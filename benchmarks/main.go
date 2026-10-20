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
	engineList := flag.String("engines", "dot,pebble", "comma-separated engines")
	workloadList := flag.String("workloads", "read-heavy,mixed,write-heavy", "comma-separated workloads")
	sizeList := flag.String("sizes", "8,1024", "comma-separated value sizes in bytes (>= 8)")
	workerList := flag.String("workers", "16", "comma-separated worker counts (1, 16, 128)")
	keys := flag.Int("keys", 16384, "total preloaded live keys per case")
	ops := flag.Int("ops", 300000, "total measured operations per case")
	warmup := flag.Int("warmup", 30000, "total untimed warmup operations per case")
	seed := flag.Uint64("seed", 1, "deterministic workload seed")
	scalar := flag.Bool("scalar", true, "use uint64 operations for 8-byte values; false uses byte slices")
	repeat := flag.Int("repeat", 1, "repetitions of each case")
	dir := flag.String("dir", ".", "parent directory for temporary databases; choose the disk to measure")
	jsonOutput := flag.Bool("json", false, "emit one JSON result per line, including operation breakdowns")
	readme := flag.String("readme", "", "update result tables in this README after all cases succeed")
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
		if *readme == "" {
			return fmt.Errorf("-from-json requires -readme")
		}
		results, err := loadResults(*fromJSON)
		if err != nil {
			return err
		}
		if err := writeReadme(*readme, results); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "Updated result tables in %s from %s\n", *readme, *fromJSON)
		return nil
	}
	if *repeat < 1 {
		return fmt.Errorf("repeat must be >= 1")
	}
	if *readme != "" {
		if err := checkReadme(*readme); err != nil {
			return err
		}
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
	var cases []caseConfig
	for repetition := range *repeat {
		for _, workload := range workloadNames {
			for _, size := range sizes {
				for _, workers := range concurrency {
					// Rotate engine order to distribute effects of run order.
					offset := (len(cases)/len(names) + repetition) % len(names)
					for i := range names {
						c := caseConfig{
							Engine: names[(i+offset)%len(names)], Workload: workload,
							ValueBytes: size, Scalar: *scalar && size == 8, Workers: workers,
							Keys: *keys, Operations: *ops, Warmup: *warmup, Seed: *seed,
							Repetition: repetition + 1,
						}
						if err := c.validate(); err != nil {
							return err
						}
						cases = append(cases, c)
					}
				}
			}
		}
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
		fmt.Fprintf(os.Stdout, "%-8s %-11s %7s %7s %10s %10s %12s %9s %9s %9s\n",
			"ENGINE", "WORKLOAD", "VALUE", "WORKERS", "MEAN us", "P99 us", "OPS/s", "PEAK MiB", "DISK MiB", "DRAIN s")
	}
	var results []result
	for i, c := range cases {
		fmt.Fprintf(os.Stderr, "[%d/%d] %s %s %dB scalar=%v workers=%d repeat=%d\n", i+1, len(cases), c.Engine, c.Workload, c.ValueBytes, c.Scalar, c.Workers, c.Repetition)
		out, err := isolatedCase(ctx, executable, parentDir, c)
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
			_, err := fmt.Fprintf(os.Stdout, "%-8s %-11s %7s %7d %10.3f %10.3f %12.0f %9.1f %9.2f %9.3f\n",
				c.Engine, c.Workload, value, c.Workers, out.Latency.MeanNS/1000, float64(out.Latency.P99NS)/1000,
				out.OpsPerSecond, float64(out.RSSPeakBytes)/(1<<20),
				float64(out.DiskFinal.AllocatedBytes)/(1<<20), out.DrainSeconds)
			if err != nil {
				return err
			}
		}
	}
	if *readme != "" {
		if err := writeReadme(*readme, results); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "Updated result tables in %s\n", *readme)
	}
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
		return out, fmt.Errorf("%s case: %w", c.Engine, err)
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
