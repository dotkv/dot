package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
)

// These describe Dot's persistent format. Check the resulting files below so
// a layout change cannot silently turn this back into a small-log benchmark.
const (
	dotSegmentBytes = 1 << 26
	dotHeaderBytes  = 21
	dotShardBits    = 6
)

type openMeasurement struct {
	elapsed, preparation  time.Duration
	records, historyBytes uint64
	fullSegments          int
}

type openKey struct {
	worker *worker
	index  int
}

// Add the same update history to both engines, concentrating it in one Dot
// shard so it actually rotates rather than spreading partial files over 64 shards.
func prepareOpenHistory(ctx context.Context, db engine, workers []*worker, c caseConfig) (records, historyBytes uint64, shard int, err error) {
	first := workers[0]
	first.prepare(operation{kind: readHit, entry: first.live[0]})
	shard = int(xxhash.Sum64(first.key[:]) >> (64 - dotShardBits))
	var keys []openKey
	for _, w := range workers {
		for i, entry := range w.live {
			w.prepare(operation{kind: readHit, entry: entry})
			if int(xxhash.Sum64(w.key[:])>>(64-dotShardBits)) == shard {
				keys = append(keys, openKey{w, i})
			}
		}
	}
	recordBytes := uint64(dotHeaderBytes+16) + uint64(c.ValueBytes)
	recordsPerSegment := (dotSegmentBytes + recordBytes - 1) / recordBytes
	if uint64(c.OpenSegments) > (^uint64(0)-1)/recordsPerSegment {
		return 0, 0, shard, fmt.Errorf("open history record count overflows")
	}
	// One extra record ensures there is a nonempty head after the full segments.
	records = uint64(c.OpenSegments)*recordsPerSegment + 1
	if records > ^uint64(0)/recordBytes {
		return 0, 0, shard, fmt.Errorf("open history byte count overflows")
	}
	historyBytes = records * recordBytes
	for i := uint64(0); i < records; i++ {
		if err := ctx.Err(); err != nil {
			return 0, 0, shard, err
		}
		key := keys[i%uint64(len(keys))]
		entry := &key.worker.live[key.index]
		entry.version++
		op := operation{kind: update, entry: *entry}
		key.worker.prepare(op)
		if _, _, _, err := key.worker.execute(db, op); err != nil {
			return 0, 0, shard, err
		}
	}
	return records, historyBytes, shard, nil
}

func countFullDotSegments(path string, shard int) (int, error) {
	path = filepath.Join(path, fmt.Sprintf("%02d", shard))
	entries, err := os.ReadDir(path)
	if err != nil {
		return 0, err
	}
	var logs []os.DirEntry
	for _, entry := range entries {
		if entry.Type().IsRegular() && strings.HasSuffix(entry.Name(), ".log") {
			logs = append(logs, entry)
		}
	}
	var full int
	for i, entry := range logs {
		info, err := entry.Info()
		if err != nil {
			return 0, err
		}
		// Canonical segment filenames sort numerically. The last is the head.
		if i < len(logs)-1 && info.Size() >= dotSegmentBytes {
			full++
		}
		if i == len(logs)-1 && info.Size() == 0 {
			return 0, fmt.Errorf("open fixture has an empty head")
		}
	}
	return full, nil
}

func measureOpen(ctx context.Context, c caseConfig, workers []*worker) (out openMeasurement, err error) {
	var db engine
	defer func() {
		if db != nil {
			err = errors.Join(err, db.Close())
		}
	}()
	if c.OpenSegments > 0 {
		prepareStart := time.Now()
		db, err = engines[c.Engine](c.Path)
		if err != nil {
			return out, err
		}
		var shard int
		out.records, out.historyBytes, shard, err = prepareOpenHistory(ctx, db, workers, c)
		if err != nil {
			return out, fmt.Errorf("prepare open history: %w", err)
		}
		flushErr := db.Flush()
		closeErr := db.Close()
		db = nil
		if err := errors.Join(flushErr, closeErr); err != nil {
			return out, err
		}
		if c.Engine == "dot" {
			out.fullSegments, err = countFullDotSegments(c.Path, shard)
			if err != nil {
				return out, err
			}
			if out.fullSegments < c.OpenSegments {
				return out, fmt.Errorf("open fixture has %d full sealed Dot segments, want at least %d", out.fullSegments, c.OpenSegments)
			}
		}
		out.preparation = time.Since(prepareStart)
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	openStart := time.Now()
	db, err = engines[c.Engine](c.Path)
	out.elapsed = time.Since(openStart)
	if err != nil {
		return out, fmt.Errorf("reopen: %w", err)
	}
	for _, w := range workers {
		for _, entry := range w.live {
			if err := ctx.Err(); err != nil {
				return out, err
			}
			op := operation{kind: readHit, entry: entry}
			want := w.prepare(op)
			got, found, size, err := w.execute(db, op)
			if err != nil {
				return out, fmt.Errorf("validate reopen: %w", err)
			}
			if !found || got != want || size != c.ValueBytes {
				return out, fmt.Errorf("validate reopen: found=%v, value=%d, bytes=%d; want %d, %d bytes", found, got, size, want, c.ValueBytes)
			}
		}
	}
	return out, nil
}
