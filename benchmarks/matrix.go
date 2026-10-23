package main

import (
	"fmt"
	"math"
	"time"
)

const calibrationDuration = 500 * time.Millisecond

func buildCases(base caseConfig, engines, workloads []string, sizes, workers []int, repeat int) ([]caseConfig, error) {
	if len(engines) == 0 || len(workers) == 0 || repeat < 1 {
		return nil, fmt.Errorf("engines, workers, and repetitions must be nonempty")
	}
	if len(workloads) == 0 {
		workloads = []string{"80/20", "20/80"}
	}
	if len(sizes) == 0 {
		sizes = []int{8, 1024}
	}
	var cases []caseConfig
	for repetition := range repeat {
		configuration := 0
		for _, workload := range workloads {
			for _, size := range sizes {
				for _, concurrency := range workers {
					// Rotate engine order to distribute effects of run order.
					offset := (configuration + repetition) % len(engines)
					for i := range engines {
						c := base
						c.Engine, c.Workload = engines[(i+offset)%len(engines)], workload
						c.KeyBytes, c.ValueBytes, c.Workers = keyBytes, size, concurrency
						c.Scalar, c.Repetition = base.Scalar && size == 8, repetition+1
						if c.Operations == 0 {
							// Placeholder validated here; calibrated once per scenario
							// before any engine runs its measured case.
							c.Operations = c.Workers
							if c.DotTarget == 0 {
								c.DotTarget = 15 * time.Second
							}
						}
						if err := c.validateRun(); err != nil {
							return nil, err
						}
						cases = append(cases, c)
					}
					configuration++
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
	// Give every worker the same budget. The same count is reused across
	// engines and repetitions.
	quantum := workers * 3
	units := math.Ceil(float64(pilot.Latency.Count) / pilot.ElapsedSeconds * target.Seconds() / float64(quantum))
	if units >= float64(math.MaxInt/quantum) {
		return 0, fmt.Errorf("calibrated operation budget overflows int")
	}
	return max(1, int(units)) * quantum, nil
}
