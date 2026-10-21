package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func reportFixture(engine string, mean float64, p99 int64) result {
	return result{
		Config: caseConfig{
			Engine: engine, Workload: "mixed", ValueBytes: 8, Scalar: true,
			Workers: 16, Keys: 1024, Operations: 12000, Warmup: 1200, Seed: 1,
		},
		Latency: latencySummary{Count: 12000, MeanNS: mean, P99NS: p99}, OpsPerSecond: 1000000,
		RSSFinalBytes: 12 << 20, RSSPeakBytes: 20 << 20,
		DiskFinal: diskSize{AllocatedBytes: 4 << 20}, SpaceAmplification: 2,
	}
}

func TestSavedResultsValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.jsonl")
	var data bytes.Buffer
	for _, name := range []string{"dot", "pebble"} {
		if err := json.NewEncoder(&data).Encode(reportFixture(name, 1000, 2000)); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(path, data.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	results, err := loadResults(path)
	if err != nil || len(results) != 2 {
		t.Fatalf("load results: %d rows, error %v", len(results), err)
	}
	for _, broken := range [][]byte{
		nil,
		append(bytes.Clone(data.Bytes()), []byte(`{"config":`)...),
		bytes.ReplaceAll(data.Bytes(), []byte(`"count":12000`), []byte(`"count":11999`)),
	} {
		if err := os.WriteFile(path, broken, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadResults(path); err == nil {
			t.Fatal("accepted an empty, truncated, or incomplete results file")
		}
	}
}

func TestReportMarksMissingConfigurations(t *testing.T) {
	a := reportFixture("dot", 1000, 2000)
	b := a
	b.Config.ValueBytes = 1024
	b.Config.Scalar = false
	b.Config.Workers = 1
	data, err := renderResults([]result{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte("**Partial matrix.**")) || !bytes.Contains(data, []byte("mixed / 1,024 B / 16 workers")) {
		t.Fatalf("missing configurations were not identified:\n%s", data)
	}
}

func TestReportGroupingAndMedians(t *testing.T) {
	other := reportFixture("dot", 2000, 4000)
	other.Config.Scalar = false
	data, err := renderResults([]result{
		reportFixture("pebble", 1000, 2000),
		reportFixture("dot", 500, 1000),
		reportFixture("dot", 1500, 9000),
		other,
	})
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if strings.Count(text, "| Implementation |") != 2 || !strings.Contains(text, "8 B uint64 values") || !strings.Contains(text, "8 B values") {
		t.Fatal("scalar and byte-slice configurations were combined")
	}
	// Medians are per run: mean=(500+1500)/2 ns, p99=(1000+9000)/2 ns.
	row := "| Dot | 2 | 1.000 | 5.000 | 1,000,000 | 20.0 | 4.00 |"
	if !strings.Contains(text, row) {
		t.Fatalf("missing median/unit-converted row %q:\n%s", row, text)
	}
	if strings.Index(text, "| Dot |") > strings.Index(text, "| Pebble |") {
		t.Fatal("implementation ordering depends on execution order")
	}
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
		if bytes.Count(data, []byte(resultsStart)) != 1 || bytes.Count(data, []byte("| Dot |")) != 1 {
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
