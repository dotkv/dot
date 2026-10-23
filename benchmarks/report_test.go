package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func reportFixture(engine string, mean float64, p99 int64) result {
	return result{
		Config: caseConfig{
			Engine: engine, Workload: "80/20", ValueBytes: 8, Scalar: true,
			Workers: 8, Keys: 1024, Operations: 12000, Warmup: 1200, Seed: 1,
		},
		Latency: latencySummary{Count: 12000, MeanNS: mean, P99NS: p99}, OpsPerSecond: 1000000,
		RSSFinalBytes: 12 << 20, RSSPeakBytes: 20 << 20,
		DiskFinal: diskSize{AllocatedBytes: 4 << 20}, SpaceAmplification: 2,
	}
}

func TestReportScenarioBudgets(t *testing.T) {
	a := reportFixture("dot", 1000, 2000)
	a.Config.DotTarget = 3 * time.Second
	b := a
	b.Config.ValueBytes, b.Config.Scalar, b.Config.Operations = 1024, false, 24000
	b.Latency.Count = 24000
	data, err := renderResults([]result{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("Partial matrix")) || !bytes.Contains(data, []byte("**16-byte keys**")) || !bytes.Contains(data, []byte("**3s for Dot**")) || !bytes.Contains(data, []byte("**24,000**")) {
		t.Fatalf("incorrect report:\n%s", data)
	}
	incompatible := a
	incompatible.Config.DotTarget = time.Second
	if _, err := renderResults([]result{a, incompatible}); err == nil {
		t.Fatal("combined different Dot targets")
	}
	incompatible = a
	incompatible.Config.Engine = "pebble"
	incompatible.Config.Operations++
	if _, err := renderResults([]result{a, incompatible}); err == nil {
		t.Fatal("compared different operation counts across engines")
	}
}

func TestReportMarksMissingConfigurations(t *testing.T) {
	a := reportFixture("dot", 1000, 2000)
	b := a
	b.Config.ValueBytes = 1024
	b.Config.Scalar = false
	b.Config.Workload = "20/80"
	data, err := renderResults([]result{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte("**Partial matrix.**")) || !bytes.Contains(data, []byte("80/20 / 1,024 B / 8 workers")) {
		t.Fatalf("missing configurations were not identified:\n%s", data)
	}
}

func TestReportGroupingAndMedians(t *testing.T) {
	other := reportFixture("dot", 2000, 4000)
	other.Config.Scalar = false
	other.ElapsedSeconds = 12345.678
	data, err := renderResults([]result{
		reportFixture("pebble", 1000, 2000),
		reportFixture("dot", 500, 1000),
		reportFixture("dot", 1500, 9000),
		other,
	})
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Join(strings.Fields(string(data)), " ")
	if strings.Count(text, "| Implementation |") != 2 || !strings.Contains(text, "8 B uint64 values") || !strings.Contains(text, "8 B values") {
		t.Fatal("scalar and byte-slice configurations were combined")
	}
	// Medians are per run: mean=(500+1500)/2 ns, p99=(1000+9000)/2 ns.
	row := "| Dot | 2 | 0.000 | 1.000 | 5.000 | 1,000,000 | 20.0 | 4.00 |"
	if !strings.Contains(text, row) {
		t.Fatalf("missing median/unit-converted row %q:\n%s", row, text)
	}
	if !strings.Contains(text, "| Pebble |") {
		t.Fatal("missing Pebble label")
	}
	if strings.Index(text, "| Dot |") > strings.Index(text, "| Pebble |") {
		t.Fatal("implementation ordering depends on execution order")
	}
	// Columns account for Unicode headers and values wider than their headers.
	table := `| Implementation | Runs |   Seconds | Mean (µs) | p99 (µs) | Throughput (ops/s) | Peak RSS (MiB) | Disk (MiB) |
| -------------- | ---: | --------: | --------: | -------: | -----------------: | -------------: | ---------: |
| Dot            |    1 | 12345.678 |     2.000 |    4.000 |          1,000,000 |           20.0 |       4.00 |
`
	if !bytes.Contains(data, []byte(table)) {
		t.Fatalf("missing aligned table:\n%s", data)
	}
	other.Config.Scalar = true
	other.Config.Operations++
	if _, err := renderResults([]result{reportFixture("dot", 1, 1), other}); err == nil {
		t.Fatal("combined different operation budgets")
	}
}

func TestWriteReadmePreservesDocumentation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "README.md")
	prefix, suffix := "# Handwritten introduction\n\n", "\n\n## Handwritten methodology\nKeep this text.\n"
	initial := prefix + resultsStart + "\nOld results\n" + resultsEnd + suffix
	if err := os.WriteFile(path, []byte(initial), 0640); err != nil {
		t.Fatal(err)
	}
	if err := checkReadme(path); err != nil {
		t.Fatal(err)
	}
	runs := []result{reportFixture("dot", 1000, 2000), reportFixture("pebble", 2000, 3000)}
	for range 2 {
		if err := writeReadme(path, runs); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.HasPrefix(data, []byte(prefix)) || !bytes.HasSuffix(data, []byte(suffix)) || bytes.Contains(data, []byte("Old results")) {
			t.Fatal("handwritten documentation changed or old results remained")
		}
		if bytes.Count(data, []byte(resultsStart)) != 1 || bytes.Count(data, []byte("| Dot ")) != 1 {
			t.Fatal("updating results appended duplicate content")
		}
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0640 {
			t.Fatal("README file permissions changed")
		}
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeReadme(path, nil); err == nil {
		t.Fatal("empty result set should not replace existing results")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("failed report generation changed the README")
	}
}

func TestResultMarkers(t *testing.T) {
	for _, text := range []string{
		resultsStart,
		resultsEnd,
		resultsEnd + resultsStart,
		resultsStart + resultsStart + resultsEnd,
		resultsStart + resultsEnd + resultsEnd,
	} {
		if _, err := replaceResults([]byte(text), []byte("new")); err == nil {
			t.Fatalf("accepted malformed result markers: %q", text)
		}
	}
	initial := []byte("# Existing README\n\nPreserve this paragraph.\n")
	updated, err := replaceResults(initial, []byte("new results\n"))
	if err != nil || !bytes.HasPrefix(updated, initial) || !bytes.Contains(updated, []byte("## Results")) {
		t.Fatal("adding a results section changed the existing README")
	}
}
