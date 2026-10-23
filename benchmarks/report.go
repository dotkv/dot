package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"
)

const resultsStart = "<!-- benchmark-results:start -->"
const resultsEnd = "<!-- benchmark-results:end -->"

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
	workload string
	bytes    int
	scalar   bool
	workers  int
}

func scenarioFor(c caseConfig) scenario {
	return scenario{c.Workload, c.ValueBytes, c.Scalar, c.Workers}
}

func renderResults(results []result) ([]byte, error) {
	if len(results) == 0 {
		return nil, fmt.Errorf("cannot report an empty benchmark run")
	}
	first := results[0]
	var order []scenario
	groups := make(map[scenario]map[string][]result)
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
		if c.DotTarget != first.Config.DotTarget {
			return nil, fmt.Errorf("cannot combine results with different Dot targets")
		}
		key := scenarioFor(c)
		if budget, ok := budgets[key]; ok && budget != c.Operations {
			return nil, fmt.Errorf("cannot combine different operation budgets for %s %d B %d workers", c.Workload, c.ValueBytes, c.Workers)
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
			fmt.Fprintf(&out, "%s / %s B / %d workers", key.workload, integerWithCommas(key.bytes), key.workers)
		}
		out.WriteString(".\n\n")
	}
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
		fmt.Fprintf(&out, "### %s · %s · %d %s\n\n", title, value, key.workers, workerLabel)
		fmt.Fprintf(&out, "Measured operations per engine: **%s**. Ratios are reads/writes.\n\n", integerWithCommas(budgets[key]))
		var engines []string
		for name := range groups[key] {
			engines = append(engines, name)
		}
		sort.Strings(engines)
		var rows [][8]string
		for _, engine := range engines {
			runs := groups[key][engine]
			name := engine
			switch engine {
			case "dot":
				name = "Dot"
			case "pebble":
				name = "Pebble"
			}
			rows = append(rows, [8]string{
				name, fmt.Sprint(len(runs)),
				fmt.Sprintf("%.3f", median(runs, func(r result) float64 { return r.ElapsedSeconds })),
				fmt.Sprintf("%.3f", median(runs, func(r result) float64 { return r.Latency.MeanNS / 1000 })),
				fmt.Sprintf("%.3f", median(runs, func(r result) float64 { return float64(r.Latency.P99NS) / 1000 })),
				integerWithCommas(int(median(runs, func(r result) float64 { return r.OpsPerSecond }) + 0.5)),
				fmt.Sprintf("%.1f", median(runs, func(r result) float64 { return float64(r.RSSPeakBytes) / (1 << 20) })),
				fmt.Sprintf("%.2f", median(runs, func(r result) float64 { return float64(r.DiskFinal.AllocatedBytes) / (1 << 20) })),
			})
		}
		writeResultTable(&out, rows)
		out.WriteByte('\n')
	}
	fmt.Fprintf(&out, "Setup: **%s initial keys**, **%d-byte keys**, **%s warmup operations**, seed **%d**.\n\n",
		integerWithCommas(first.Config.Keys), keyBytes, integerWithCommas(first.Config.Warmup), first.Config.Seed)
	if first.Config.DotTarget > 0 {
		fmt.Fprintf(&out, "Shared operation budgets target approximately **%s for Dot**.\n\n", first.Config.DotTarget)
	}
	e := first.Environment
	fmt.Fprintf(&out, "Environment: %s, %s/%s, %d logical CPUs, GOMAXPROCS=%d.\n\n", e.GoVersion, e.OS, e.Arch, e.CPUs, e.GOMAXPROCS)
	if e.CPUModel != "" {
		fmt.Fprintf(&out, "CPU: %s.\n\n", e.CPUModel)
	}
	if version := e.ModuleVersion["github.com/cockroachdb/pebble/v2"]; version != "" {
		fmt.Fprintf(&out, "Pebble: `%s`.\n", version)
	}
	return []byte(out.String()), nil
}

func writeResultTable(out *strings.Builder, rows [][8]string) {
	headers := [8]string{"Implementation", "Runs", "Seconds", "Mean (µs)", "p99 (µs)", "Throughput (ops/s)", "Peak RSS (MiB)", "Disk (MiB)"}
	var widths [8]int
	for i, header := range headers {
		widths[i] = utf8.RuneCountInString(header)
	}
	for _, row := range rows {
		for i, cell := range row {
			widths[i] = max(widths[i], utf8.RuneCountInString(cell))
		}
	}
	writeRow := func(row [8]string) {
		out.WriteByte('|')
		for i, cell := range row {
			if i == 0 {
				fmt.Fprintf(out, " %-*s |", widths[i], cell)
			} else {
				fmt.Fprintf(out, " %*s |", widths[i], cell)
			}
		}
		out.WriteByte('\n')
	}
	writeRow(headers)
	out.WriteByte('|')
	for i, width := range widths {
		separator := strings.Repeat("-", width)
		if i > 0 {
			separator = separator[:width-1] + ":"
		}
		fmt.Fprintf(out, " %s |", separator)
	}
	out.WriteByte('\n')
	for _, row := range rows {
		writeRow(row)
	}
}

// Identify gaps within the workload/value/worker axes present in a result set.
func missingScenarios(order []scenario) []scenario {
	workloads := make(map[string]bool)
	values := make(map[scenario]bool)
	workers := make(map[int]bool)
	present := make(map[scenario]bool)
	for _, key := range order {
		workloads[key.workload] = true
		values[scenario{bytes: key.bytes, scalar: key.scalar}] = true
		workers[key.workers] = true
		present[key] = true
	}
	var missing []scenario
	for workload := range workloads {
		for value := range values {
			for worker := range workers {
				key := scenario{workload, value.bytes, value.scalar, worker}
				if !present[key] {
					missing = append(missing, key)
				}
			}
		}
	}
	sort.Slice(missing, func(i, j int) bool {
		a, b := missing[i], missing[j]
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
