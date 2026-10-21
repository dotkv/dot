package main

import (
	"fmt"
	"math"
	"time"
)

const calibrationDuration = 500 * time.Millisecond

type benchmarkProfile struct {
	name       string
	workloads  []string
	sizes      []int
	target     time.Duration
	compaction bool
}

func buildCases(profile string, base caseConfig, engines, workloads []string, sizes, workers []int, repeat int) ([]caseConfig, error) {
	fast := benchmarkProfile{"fast", []string{"0/100", "10/90", "50/50", "90/10", "100/0"}, []int{8, 1024}, 3 * time.Second, false}
	slow := benchmarkProfile{"slow", []string{"50/50"}, []int{128}, 45 * time.Second, true}
	var profiles []benchmarkProfile
	switch profile {
	case "fast":
		profiles = []benchmarkProfile{fast}
	case "slow":
		profiles = []benchmarkProfile{slow}
	case "all":
		profiles = []benchmarkProfile{fast, slow}
	default:
		return nil, fmt.Errorf("unknown profile %q; choose fast, slow, or all", profile)
	}
	if len(engines) == 0 || len(workers) == 0 || repeat < 1 {
		return nil, fmt.Errorf("engines, workers, and repetitions must be nonempty")
	}
	var cases []caseConfig
	for repetition := range repeat {
		configuration := 0
		for _, p := range profiles {
			workloadNames, valueSizes := workloads, sizes
			if len(workloadNames) == 0 {
				workloadNames = p.workloads
			}
			if len(valueSizes) == 0 {
				valueSizes = p.sizes
			}
			for _, workload := range workloadNames {
				for _, size := range valueSizes {
					for _, concurrency := range workers {
						// Rotate engine order to distribute effects of run order.
						offset := (configuration + repetition) % len(engines)
						for i := range engines {
							c := base
							c.Profile, c.Engine, c.Workload = p.name, engines[(i+offset)%len(engines)], workload
							c.KeyBytes, c.ValueBytes, c.Workers = keyBytes, size, concurrency
							c.Scalar, c.DisableCompaction, c.Repetition = base.Scalar && size == 8, !p.compaction, repetition+1
							if c.Operations == 0 {
								// Placeholder validated here; calibrated once per scenario
								// before any engine runs its measured case.
								c.Operations = c.Workers
								if c.DotTarget == 0 {
									c.DotTarget = p.target
								}
							}
							if err := c.validate(); err != nil {
								return nil, err
							}
							cases = append(cases, c)
						}
						configuration++
					}
				}
			}
		}
	}
	return cases, nil
}

func operationBudget(pilot result, target time.Duration, workers int) (int, error) {
	if pilot.Latency.Count == 0 || pilot.ElapsedSeconds <= 0 || math.IsNaN(pilot.ElapsedSeconds) || math.IsInf(pilot.ElapsedSeconds, 0) || target <= 0 || workers <= 0 {
		return 0, fmt.Errorf("invalid Dot calibration measurement")
	}
	// Give every worker the same budget, with complete mutation cycles for the
	// all-write workload. The same count is reused across engines and repeats.
	quantum := workers * 3
	units := math.Ceil(float64(pilot.Latency.Count) / pilot.ElapsedSeconds * target.Seconds() / float64(quantum))
	if units >= float64(math.MaxInt/quantum) {
		return 0, fmt.Errorf("calibrated operation budget overflows int")
	}
	return max(1, int(units)) * quantum, nil
}
