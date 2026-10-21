package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const resultsStart = "<!-- benchmark-results:start -->"
const resultsEnd = "<!-- benchmark-results:end -->"

func loadResults(path string) ([]result, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	var results []result
	for {
		var r result
		if err := decoder.Decode(&r); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("decode result %d: %w", len(results)+1, err)
		}
		if err := r.Config.validate(); err != nil {
			return nil, fmt.Errorf("result %d: %w", len(results)+1, err)
		}
		if r.Config.CalibrationDuration != 0 {
			return nil, fmt.Errorf("result %d is a calibration probe, not a fixed-work case", len(results)+1)
		}
		r.Config.KeyBytes = keyBytes
		if r.Latency.Count != uint64(r.Config.Operations) {
			return nil, fmt.Errorf("result %d has %d completed operations, want %d", len(results)+1, r.Latency.Count, r.Config.Operations)
		}
		results = append(results, r)
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("results file is empty")
	}
	return results, nil
}

func checkReadme(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read README: %w", err)
	}
	_, err = replaceResults(data, nil)
	return err
}

// Replace only the generated section, preserving handwritten documentation.
// Incomplete or duplicate markers are an error rather than a reason to append
// another section or replace unrelated text.
func replaceResults(data, report []byte) ([]byte, error) {
	start, end := bytes.Index(data, []byte(resultsStart)), bytes.Index(data, []byte(resultsEnd))
	block := []byte(resultsStart + "\n\n" + string(report) + "\n" + resultsEnd)
	if start == -1 && end == -1 {
		out := append(bytes.Clone(bytes.TrimRight(data, "\n")), []byte("\n\n## Results\n\n")...)
		out = append(out, block...)
		return append(out, '\n'), nil
	}
	if start == -1 || end < start || bytes.Count(data, []byte(resultsStart)) != 1 || bytes.Count(data, []byte(resultsEnd)) != 1 {
		return nil, fmt.Errorf("README must contain exactly one ordered pair of benchmark result markers, or neither marker")
	}
	out := append(bytes.Clone(data[:start]), block...)
	return append(out, data[end+len(resultsEnd):]...), nil
}

func writeReadme(path string, results []result) error {
	report, err := renderResults(results)
	if err != nil {
		return err
	}
	// Read at completion so documentation edits made during the benchmark are
	// retained. A failed run never reaches this replacement.
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	updated, err := replaceResults(data, report)
	if err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".benchmark-readme-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(temp.Name()) }()
	if err := temp.Chmod(info.Mode().Perm()); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(updated); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(temp.Name(), path)
}

type scenario struct {
	profile  string
	workload string
	bytes    int
	scalar   bool
	workers  int
}

func scenarioFor(c caseConfig) scenario {
	return scenario{c.Profile, c.Workload, c.ValueBytes, c.Scalar, c.Workers}
}

func renderResults(results []result) ([]byte, error) {
	if len(results) == 0 {
		return nil, fmt.Errorf("cannot report an empty benchmark run")
	}
	first := results[0]
	var order []scenario
	groups := make(map[scenario]map[string][]result)
	type profileSettings struct {
		target            time.Duration
		disableCompaction bool
	}
	settings := make(map[string]profileSettings)
	var profileOrder []string
	budgets := make(map[scenario]int)
	for _, r := range results {
		c := r.Config
		if err := c.validate(); err != nil {
			return nil, err
		}
		if c.Keys != first.Config.Keys || c.Warmup != first.Config.Warmup || c.Seed != first.Config.Seed {
			return nil, fmt.Errorf("cannot combine results with different dataset, warmup, or seed settings")
		}
		if c.CalibrationDuration != 0 {
			return nil, fmt.Errorf("cannot report calibration probes")
		}
		setting := profileSettings{c.DotTarget, c.DisableCompaction}
		if previous, ok := settings[c.Profile]; ok && previous != setting {
			return nil, fmt.Errorf("cannot combine results with different target or compaction settings for profile %q", c.Profile)
		} else if !ok {
			profileOrder = append(profileOrder, c.Profile)
			settings[c.Profile] = setting
		}
		key := scenarioFor(c)
		if budget, ok := budgets[key]; ok && budget != c.Operations {
			return nil, fmt.Errorf("cannot combine different operation budgets for %s %s %d B %d workers", c.Profile, c.Workload, c.ValueBytes, c.Workers)
		}
		budgets[key] = c.Operations
		if groups[key] == nil {
			order = append(order, key)
			groups[key] = make(map[string][]result)
		}
		groups[key][c.Engine] = append(groups[key][c.Engine], r)
	}
	var out strings.Builder
	fmt.Fprintf(&out, "Completed cases: **%d**, covering **%d configurations**.\n\n", len(results), len(order))
	if missing := missingScenarios(order); len(missing) > 0 {
		out.WriteString("**Partial matrix.** Unmeasured configurations: ")
		for i, key := range missing {
			if i > 0 {
				out.WriteString("; ")
			}
			if key.profile != "" {
				fmt.Fprintf(&out, "%s / ", key.profile)
			}
			fmt.Fprintf(&out, "%s / %s B / %d workers", key.workload, integerWithCommas(key.bytes), key.workers)
		}
		out.WriteString(".\n\n")
	}
	fmt.Fprintf(&out, "Configuration: **%s initial keys**, **%d-byte keys**, **%s warmup operations**, seed **%d**. Operation budgets are fixed per configuration and shared across engines and repetitions.\n\n",
		integerWithCommas(first.Config.Keys), keyBytes, integerWithCommas(first.Config.Warmup), first.Config.Seed)
	for _, name := range profileOrder {
		setting := settings[name]
		label := name
		if label == "" {
			label = "legacy"
		}
		compaction := "enabled"
		if setting.disableCompaction {
			compaction = "disabled"
		}
		fmt.Fprintf(&out, "Profile **%s**: automatic compaction **%s**", label, compaction)
		if setting.target > 0 {
			fmt.Fprintf(&out, "; budgets estimated from a short Dot probe targeting **%s for Dot**", setting.target)
		}
		if setting.disableCompaction {
			fmt.Fprintf(&out, "; Pebble L0 write-stop threshold **%s**", integerWithCommas(fastL0StopWritesThreshold))
		}
		out.WriteString(".\n\n")
	}
	e := first.Environment
	fmt.Fprintf(&out, "Environment: %s, %s/%s, %d logical CPUs, GOMAXPROCS=%d.\n\n", e.GoVersion, e.OS, e.Arch, e.CPUs, e.GOMAXPROCS)
	if e.CPUModel != "" {
		fmt.Fprintf(&out, "CPU: %s.\n\n", e.CPUModel)
	}
	if version := e.ModuleVersion["github.com/cockroachdb/pebble/v2"]; version != "" {
		fmt.Fprintf(&out, "Pebble: `%s`. ", version)
	}
	out.WriteString("Eight-byte scalar cases use `uint64` in Dot and equivalent encoding/decoding in Pebble.\n\n")
	out.WriteString("Columns summarize all operation types. Each cell is the median across the indicated runs; p99 is the median of run p99s, not a pooled percentile. Peak RSS includes preparation. Disk usage is measured after the final drain. These are closed-loop saturation results.\n\n")
	for _, key := range order {
		value := fmt.Sprintf("%s B values", integerWithCommas(key.bytes))
		if key.scalar {
			value = "8 B uint64 values"
		}
		workerLabel := "workers"
		if key.workers == 1 {
			workerLabel = "worker"
		}
		title := strings.ReplaceAll(key.workload, "-", " ")
		if key.profile != "" {
			title = key.profile + " · " + title
		}
		fmt.Fprintf(&out, "### %s · %s · %d %s\n\n", title, value, key.workers, workerLabel)
		fmt.Fprintf(&out, "Measured operations per engine: **%s**. Ratios are reads/writes.\n\n", integerWithCommas(budgets[key]))
		out.WriteString("| Implementation | Runs | Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |\n")
		out.WriteString("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
		var engines []string
		for name := range groups[key] {
			engines = append(engines, name)
		}
		sort.Strings(engines)
		for _, engine := range engines {
			runs := groups[key][engine]
			name := engine
			switch engine {
			case "dot":
				name = "Dot"
			case "pebble":
				name = "Pebble"
			}
			fmt.Fprintf(&out, "| %s | %d | %.3f | %.3f | %.3f | %s | %.1f | %.2f |\n",
				name, len(runs),
				median(runs, func(r result) float64 { return r.ElapsedSeconds }),
				median(runs, func(r result) float64 { return r.Latency.MeanNS / 1000 }),
				median(runs, func(r result) float64 { return float64(r.Latency.P99NS) / 1000 }),
				integerWithCommas(int(median(runs, func(r result) float64 { return r.OpsPerSecond })+0.5)),
				median(runs, func(r result) float64 { return float64(r.RSSPeakBytes) / (1 << 20) }),
				median(runs, func(r result) float64 { return float64(r.DiskFinal.AllocatedBytes) / (1 << 20) }))
		}
		out.WriteByte('\n')
	}
	return []byte(out.String()), nil
}

// Identify gaps within the workload/value/worker axes present in a saved run.
// This keeps a snapshot from an interrupted run from looking like a full sweep.
func missingScenarios(order []scenario) []scenario {
	byProfile := make(map[string][]scenario)
	for _, key := range order {
		byProfile[key.profile] = append(byProfile[key.profile], key)
	}
	var missing []scenario
	for profile, scenarios := range byProfile {
		workloads := make(map[string]bool)
		values := make(map[scenario]bool)
		workers := make(map[int]bool)
		present := make(map[scenario]bool)
		for _, key := range scenarios {
			workloads[key.workload] = true
			values[scenario{bytes: key.bytes, scalar: key.scalar}] = true
			workers[key.workers] = true
			present[key] = true
		}
		for workload := range workloads {
			for value := range values {
				for worker := range workers {
					key := scenario{profile, workload, value.bytes, value.scalar, worker}
					if !present[key] {
						missing = append(missing, key)
					}
				}
			}
		}
	}
	sort.Slice(missing, func(i, j int) bool {
		a, b := missing[i], missing[j]
		if a.profile != b.profile {
			return a.profile < b.profile
		}
		if a.workload != b.workload {
			return a.workload < b.workload
		}
		if a.bytes != b.bytes {
			return a.bytes < b.bytes
		}
		if a.scalar != b.scalar {
			return !a.scalar
		}
		return a.workers < b.workers
	})
	return missing
}

func median(runs []result, metric func(result) float64) float64 {
	values := make([]float64, len(runs))
	for i, r := range runs {
		values[i] = metric(r)
	}
	sort.Float64s(values)
	i := len(values) / 2
	if len(values)%2 != 0 {
		return values[i]
	}
	return (values[i-1] + values[i]) / 2
}

func integerWithCommas(n int) string {
	value := fmt.Sprint(n)
	for i := len(value) - 3; i > 0; i -= 3 {
		value = value[:i] + "," + value[i:]
	}
	return value
}
