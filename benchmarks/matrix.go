package main

import (
	"fmt"
	"math"
	"time"
)

const calibrationDuration = 500 * time.Millisecond

func buildCases() []caseConfig {
	engines := []string{"dot", "pebble"}
	var cases []caseConfig
	configuration := 0
	for _, workload := range []string{"80/20", "20/80"} {
		for _, size := range []int{8, 1024} {
			// Rotate engine order to distribute effects of run order.
			for i := range engines {
				cases = append(cases, caseConfig{
					Engine: engines[(i+configuration)%len(engines)], Workload: workload,
					KeyBytes: keyBytes, ValueBytes: size, Scalar: size == 8,
					Workers: 8, Keys: 16384, Warmup: 30000, Seed: 1,
					DotTarget: 15 * time.Second,
				})
			}
			configuration++
		}
	}
	return cases
}

func operationBudget(pilot result, target time.Duration, workers int) (int, error) {
	if pilot.Latency.Count == 0 || pilot.ElapsedSeconds <= 0 || math.IsNaN(pilot.ElapsedSeconds) || math.IsInf(pilot.ElapsedSeconds, 0) || target <= 0 || workers <= 0 {
		return 0, fmt.Errorf("invalid Dot calibration measurement")
	}
	// Give every worker the same budget. The same count is reused across
	// engines.
	quantum := workers * 3
	units := math.Ceil(float64(pilot.Latency.Count) / pilot.ElapsedSeconds * target.Seconds() / float64(quantum))
	if units >= float64(math.MaxInt/quantum) {
		return 0, fmt.Errorf("calibrated operation budget overflows int")
	}
	return max(1, int(units)) * quantum, nil
}
