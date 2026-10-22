package dot

import "fmt"

// shrink halves a sparse index during maintenance, after expiration pruning.
// It requires the shard's exclusive lock. A failed allocation leaves the index
// unchanged; a failed unmap retains both mappings for a later cleanup attempt.
func (idx *Index) shrink() error {
	// Retry cleanup even at the minimum capacity or above the shrink threshold.
	if err := idx.memory.releasePrevious(); err != nil {
		return fmt.Errorf("shrink index: %w", err)
	}
	n := len(idx.slots)
	if n <= 1<<indexBits || idx.entryCount >= n/4 {
		return nil
	}
	m := n / 2
	slots, err := idx.memory.allocate(m)
	if err != nil {
		return fmt.Errorf("shrink index: %w", err)
	}
	if idx.entryCount == 0 {
		idx.slots = slots
		return idx.memory.releasePrevious()
	}
	old := idx.slots
	mask := m - 1

	// Count entries in each destination bucket using the replacement mapping as
	// scratch space. Walking runs recovers their home buckets without log reads.
	start := 0
	for !empty(old[start]) {
		start++
	}
	home := start
	for i := 1; i <= n; i++ {
		slot := old[(start+i)&(n-1)]
		if empty(slot) {
			continue
		}
		if slot&continuationMask == 0 {
			for {
				home = (home + 1) & (n - 1)
				if old[home]&occupiedMask != 0 {
					break
				}
			}
		}
		slots[home&mask]++
	}
	// Choose a circular starting point after an empty destination bucket. This
	// keeps merged clusters from wrapping over entries already written. Clear
	// only populated counts, leaving untouched mmap pages demand-zeroed.
	var pending uint64
	start = 0
	for bucket, count := range slots {
		pending += count
		if pending == 0 {
			start = (bucket + 1) & mask
		} else {
			pending--
		}
		if count != 0 {
			slots[bucket] = 0
		}
	}

	// Merge the two source halves in destination-bucket order. Each cursor
	// advances through whole runs, avoiding repeated probes in long clusters.
	var positions [2]int
	seek := func(first int) {
		for half := range 2 {
			for bucket := first + half*m; bucket < (half+1)*m; bucket++ {
				if old[bucket]&occupiedMask != 0 {
					positions[half] = idx.run(bucket)
					break
				}
			}
		}
	}
	seek(start)
	next := 0
	for i := range m {
		bucket := (start + i) & mask
		if i > 0 && bucket == 0 {
			seek(0)
		}
		first := true
		for half := range 2 {
			if old[bucket+half*m]&occupiedMask == 0 {
				continue
			}
			source := positions[half]
			for empty(old[source]) {
				source = (source + 1) & (n - 1)
			}
			for {
				slot := old[source] &^ metadataMask
				pos := max(next, i)
				next = pos + 1
				pos = (start + pos) & mask
				if !first {
					slot |= continuationMask
				}
				if pos != bucket {
					slot |= shiftedMask
				}
				slots[bucket] |= occupiedMask
				slots[pos] = slot | slots[pos]&occupiedMask
				first = false
				source = (source + 1) & (n - 1)
				if old[source]&continuationMask == 0 {
					break
				}
			}
			positions[half] = source
		}
	}
	idx.slots = slots
	return idx.memory.releasePrevious()
}
