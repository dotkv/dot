package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

type caseConfig struct {
	Engine     string        `json:"engine"`
	Workload   string        `json:"workload"`
	KeyBytes   int           `json:"key_bytes"`
	ValueBytes int           `json:"value_bytes"`
	Scalar     bool          `json:"scalar"`
	Workers    int           `json:"workers"`
	Keys       int           `json:"initial_keys"`
	Operations int           `json:"operations"`
	DotTarget  time.Duration `json:"dot_target_ns,omitempty"`
	// Only internal Dot calibration probes stop on time. Reported cases always
	// execute Operations, identically across engines and repetitions.
	CalibrationDuration time.Duration `json:"calibration_duration_ns,omitempty"`
	Warmup              int           `json:"warmup_operations"`
	Seed                uint64        `json:"seed"`
	Repetition          int           `json:"repetition"`
	Path                string        `json:"data_path"`
}

func (c caseConfig) validate() error {
	if _, ok := engines[c.Engine]; !ok {
		return fmt.Errorf("unknown engine %q", c.Engine)
	}
	if _, ok := workloads[c.Workload]; !ok {
		return fmt.Errorf("unknown workload %q", c.Workload)
	}
	if c.Workers != 1 && c.Workers != 16 && c.Workers != 128 {
		return fmt.Errorf("workers must be 1, 16, or 128")
	}
	if c.ValueBytes < 8 {
		return fmt.Errorf("value size must be at least 8 bytes")
	}
	// Zero key_bytes is accepted for results saved before the field existed.
	if c.KeyBytes != 0 && c.KeyBytes != keyBytes {
		return fmt.Errorf("keys must be %d bytes", keyBytes)
	}
	if c.Scalar && c.ValueBytes != 8 {
		return fmt.Errorf("scalar operations require 8-byte values")
	}
	if c.Keys < c.Workers || c.Warmup < 0 {
		return fmt.Errorf("keys must be >= workers; warmup must be >= 0")
	}
	if c.DotTarget < 0 || c.CalibrationDuration < 0 {
		return fmt.Errorf("Dot target and calibration duration must be nonnegative")
	}
	if c.CalibrationDuration > 0 {
		if c.Operations != 0 || c.Engine != "dot" {
			return fmt.Errorf("calibration requires Dot with no fixed operation budget")
		}
	} else if c.Operations < c.Workers {
		return fmt.Errorf("operations must be >= workers")
	}
	return nil
}

type latencySummary struct {
	Count  uint64  `json:"count"`
	MeanNS float64 `json:"mean_ns"`
	P99NS  int64   `json:"p99_ns"`
}

func summarize(l latency) latencySummary {
	s := latencySummary{Count: l.count}
	if l.count > 0 {
		s.MeanNS = float64(l.total) / float64(l.count)
		s.P99NS = l.hist.ValueAtQuantile(99)
	}
	return s
}

type environment struct {
	GoVersion     string            `json:"go_version"`
	OS            string            `json:"os"`
	Arch          string            `json:"arch"`
	CPUs          int               `json:"cpus"`
	CPUModel      string            `json:"cpu_model,omitempty"`
	GOMAXPROCS    int               `json:"gomaxprocs"`
	ModuleVersion map[string]string `json:"module_versions"`
	VCS           map[string]string `json:"vcs"`
}

type result struct {
	Config             caseConfig                `json:"config"`
	Environment        environment               `json:"environment"`
	ElapsedSeconds     float64                   `json:"elapsed_seconds"`
	OpsPerSecond       float64                   `json:"ops_per_second"`
	Latency            latencySummary            `json:"latency"`
	ByOperation        map[string]latencySummary `json:"by_operation"`
	RSSHarnessBytes    uint64                    `json:"rss_harness_bytes"`
	RSSPreparedBytes   uint64                    `json:"rss_prepared_bytes"`
	RSSFinalBytes      uint64                    `json:"rss_final_bytes"`
	RSSPeakBytes       uint64                    `json:"rss_peak_bytes"`
	DiskPrepared       diskSize                  `json:"disk_prepared"`
	DiskFinal          diskSize                  `json:"disk_final"`
	DrainSeconds       float64                   `json:"drain_seconds"`
	LiveKeys           int                       `json:"live_keys"`
	LogicalBytes       uint64                    `json:"logical_bytes"`
	SpaceAmplification float64                   `json:"space_amplification"`
}

func currentEnvironment() environment {
	e := environment{
		GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH,
		CPUs: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0),
		ModuleVersion: make(map[string]string), VCS: make(map[string]string),
	}
	if data, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			name, value, ok := strings.Cut(line, ":")
			if ok && strings.TrimSpace(name) == "model name" {
				e.CPUModel = strings.TrimSpace(value)
				break
			}
		}
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			e.ModuleVersion[dep.Path] = dep.Version
		}
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" || setting.Key == "vcs.modified" || setting.Key == "vcs.time" {
				e.VCS[setting.Key] = setting.Value
			}
		}
	}
	return e
}

func runCase(ctx context.Context, c caseConfig) (out result, err error) {
	if err := c.validate(); err != nil {
		return out, err
	}
	out.Config = c
	out.Config.KeyBytes = keyBytes
	out.Environment = currentEnvironment()
	workers := make([]*worker, c.Workers)
	for i := range workers {
		workers[i] = newWorker(c, i)
	}
	baseline, err := readRSS()
	if err != nil {
		return out, err
	}
	out.RSSHarnessBytes = baseline.Current
	db, err := engines[c.Engine](c)
	if err != nil {
		return out, err
	}
	defer func() {
		if db != nil {
			err = errors.Join(err, db.Close())
		}
	}()
	// Populate using the same ownership and encoding as the timed operations.
	if _, err := parallel(ctx, workers, func(ctx context.Context, i int, w *worker, _ time.Time) error {
		for _, entry := range w.live {
			if err := ctx.Err(); err != nil {
				return err
			}
			op := operation{kind: insert, entry: entry}
			w.prepare(op)
			if _, _, _, err := w.execute(db, op); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return out, fmt.Errorf("preload: %w", err)
	}
	// Flush preloaded data so a Pebble read test is not just a memtable test.
	if err := db.Flush(); err != nil {
		return out, err
	}
	phase := func(total int, duration time.Duration, measured bool) (time.Duration, error) {
		return parallel(ctx, workers, func(ctx context.Context, id int, w *worker, begin time.Time) error {
			deadline := begin.Add(duration)
			budget := share(total, c.Workers, id)
			for j := 0; duration > 0 || j < budget; j++ {
				if err := ctx.Err(); err != nil {
					return err
				}
				if duration > 0 && !time.Now().Before(deadline) {
					break
				}
				if err := w.step(db, measured); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if _, err := phase(c.Warmup, 0, false); err != nil {
		return out, fmt.Errorf("warmup: %w", err)
	}
	if err := db.Flush(); err != nil {
		return out, err
	}
	prepared, err := readRSS()
	if err != nil {
		return out, err
	}
	out.RSSPreparedBytes = prepared.Current
	out.DiskPrepared, err = diskUsage(c.Path)
	if err != nil {
		return out, err
	}
	elapsed, err := phase(c.Operations, c.CalibrationDuration, true)
	if err != nil {
		return out, fmt.Errorf("measurement: %w", err)
	}
	out.ElapsedSeconds = elapsed.Seconds()
	finalRSS, err := readRSS()
	if err != nil {
		return out, err
	}
	out.RSSFinalBytes = finalRSS.Current
	out.RSSPeakBytes = finalRSS.Peak
	// Include unflushed writes in the final disk figure. Report this untimed
	// drain separately; it is not silently included in operation throughput.
	drainStart := time.Now()
	flushErr := db.Flush()
	closeErr := db.Close()
	db = nil
	out.DrainSeconds = time.Since(drainStart).Seconds()
	if err := errors.Join(flushErr, closeErr); err != nil {
		return out, err
	}
	out.DiskFinal, err = diskUsage(c.Path)
	if err != nil {
		return out, err
	}
	aggregate := newLatency()
	out.ByOperation = make(map[string]latencySummary)
	for kind, name := range operationNames {
		merged := newLatency()
		for _, w := range workers {
			l := w.stats[kind]
			merged.count += l.count
			merged.total += l.total
			if dropped := merged.hist.Merge(l.hist); dropped != 0 {
				return out, fmt.Errorf("merging latency dropped %d samples", dropped)
			}
		}
		out.ByOperation[name] = summarize(merged)
		aggregate.count += merged.count
		aggregate.total += merged.total
		if dropped := aggregate.hist.Merge(merged.hist); dropped != 0 {
			return out, fmt.Errorf("merging total latency dropped %d samples", dropped)
		}
	}
	if c.CalibrationDuration == 0 && aggregate.count != uint64(c.Operations) {
		return out, fmt.Errorf("recorded %d operations, want %d", aggregate.count, c.Operations)
	}
	if aggregate.count == 0 {
		return out, fmt.Errorf("measurement completed no operations")
	}
	out.Latency = summarize(aggregate)
	out.OpsPerSecond = float64(aggregate.count) / elapsed.Seconds()
	for _, w := range workers {
		out.LiveKeys += len(w.live)
	}
	out.LogicalBytes = uint64(out.LiveKeys) * uint64(keyBytes+c.ValueBytes)
	out.SpaceAmplification = float64(out.DiskFinal.AllocatedBytes) / float64(out.LogicalBytes)
	return out, nil
}

// Workers own disjoint key sets, so expected hits, updates and deletes are
// deterministic despite scheduling. They still share one engine and its locks.
func parallel(ctx context.Context, workers []*worker, fn func(context.Context, int, *worker, time.Time) error) (time.Duration, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	start := make(chan struct{})
	errs := make(chan error, 1)
	var ready, done sync.WaitGroup
	var begin time.Time // Published to every worker by closing start.
	ready.Add(len(workers))
	for i, w := range workers {
		done.Go(func() {
			ready.Done()
			<-start
			if err := fn(ctx, i, w, begin); err != nil {
				select {
				case errs <- err:
				default:
				}
				cancel()
			}
		})
	}
	ready.Wait()
	begin = time.Now()
	close(start)
	done.Wait()
	elapsed := time.Since(begin)
	select {
	case err := <-errs:
		return elapsed, err
	default:
		return elapsed, ctx.Err()
	}
}
