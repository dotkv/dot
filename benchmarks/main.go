// Command benchmarks compares embedded engines in isolated Linux processes.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

const childProcessEnv = "DOT_BENCH_CHILD"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := command(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func command(ctx context.Context) error {
	if len(os.Args) != 1 {
		return fmt.Errorf("benchmark runner accepts no arguments")
	}
	if os.Getenv(childProcessEnv) == "1" {
		var c caseConfig
		if err := json.NewDecoder(os.Stdin).Decode(&c); err != nil {
			return err
		}
		out, err := runCase(ctx, c)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(out)
	}
	if err := checkReadme("README.md"); err != nil {
		return err
	}
	parentDir, err := os.Getwd()
	if err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "%-8s %-11s %7s %7s %8s %10s %10s %12s %9s %9s %9s\n",
		"ENGINE", "READ/WRITE", "VALUE", "WORKERS", "SECONDS", "MEAN us", "P99 us", "OPS/s", "PEAK MiB", "DISK MiB", "DRAIN s")
	var results []result
	budgets := make(map[scenario]int)
	run := func(c caseConfig) (result, error) {
		caseCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
		defer cancel()
		return isolatedCase(caseCtx, executable, parentDir, c)
	}
	for _, c := range buildCases() {
		key := scenarioFor(c)
		if budgets[key] == 0 {
			probe := c
			probe.Engine, probe.CalibrationDuration = "dot", calibrationDuration
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
		out, err := run(c)
		if err != nil {
			return err
		}
		results = append(results, out)
		value := strconv.Itoa(c.ValueBytes) + "B"
		if c.Scalar {
			value = "uint64"
		}
		_, err = fmt.Fprintf(os.Stdout, "%-8s %-11s %7s %7d %8.3f %10.3f %10.3f %12.0f %9.1f %9.2f %9.3f\n",
			c.Engine, c.Workload, value, c.Workers, out.ElapsedSeconds, out.Latency.MeanNS/1000, float64(out.Latency.P99NS)/1000,
			out.OpsPerSecond, float64(out.RSSPeakBytes)/(1<<20),
			float64(out.DiskFinal.AllocatedBytes)/(1<<20), out.DrainSeconds)
		if err != nil {
			return err
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
	cmd := exec.CommandContext(ctx, executable)
	cmd.Env = append(os.Environ(), childProcessEnv+"=1")
	cmd.Stdin = bytes.NewReader(payload)
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
