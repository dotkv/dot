package dot

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

// ErrMigrationRequired means Open cannot use the requested or stored layout
// without a data migration. Automatic data migrations are not supported.
var ErrMigrationRequired = errors.New("database migration required")

const (
	// Version 1 pins the record encoding, 16-byte alignment, and XXH64 shard
	// routing. Layout fields below must also match before logs can be opened.
	metaVersion  = 1
	metaName     = "META"
	metaTempName = "META.tmp"
)

type metaData struct {
	Version             uint16 `json:"version"`
	ShardCount          int    `json:"shardCount"`
	SegmentIDBits       byte   `json:"segmentIDBits"`
	SegmentSizeBits     byte   `json:"segmentSizeBits"`
	RecordAlignmentBits byte   `json:"recordAlignmentBits"`
}

func currentMeta(count int) metaData {
	return metaData{
		Version: metaVersion, ShardCount: count,
		SegmentIDBits: segmentIDBits, SegmentSizeBits: segmentSizeBits,
		RecordAlignmentBits: logAlignmentBits,
	}
}

// migrate runs under the database lock, before recovery can modify any log.
// For now the only supported transition is initializing an empty directory.
// Missing metadata in a nonempty directory cannot safely identify its format.
func (db *DB) migrate(options Options) (int, error) {
	metaPath := filepath.Join(db.path, metaName)
	fi, err := os.Lstat(metaPath)
	if errors.Is(err, os.ErrNotExist) {
		entries, err := os.ReadDir(db.path)
		if err != nil {
			return 0, fmt.Errorf("read database directory: %w", err)
		}
		for _, entry := range entries {
			if entry.Name() == lockName || (entry.Name() == metaTempName && entry.Type().IsRegular()) {
				continue
			}
			return 0, fmt.Errorf("%w: META is missing from a nonempty database directory; cannot determine its layout", ErrMigrationRequired)
		}
		if err := db.migrateWriteMeta(currentMeta(options.shardCount)); err != nil {
			return 0, err
		}
		return options.shardCount, nil
	}
	if err != nil {
		return 0, fmt.Errorf("stat meta file: %w", err)
	}
	if !fi.Mode().IsRegular() || fi.Size() == 0 || fi.Size() > 4096 {
		return 0, fmt.Errorf("invalid meta file: expected a nonempty regular file of at most 4096 bytes")
	}
	f, err := os.Open(metaPath)
	if err != nil {
		return 0, fmt.Errorf("open meta file: %w", err)
	}
	defer f.Close()
	var meta metaData
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&meta); err != nil {
		return 0, fmt.Errorf("decode meta file: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return 0, fmt.Errorf("invalid meta file: trailing data")
	}
	if meta.Version != metaVersion {
		return 0, fmt.Errorf("%w: unsupported meta version %d (want %d)", ErrMigrationRequired, meta.Version, metaVersion)
	}
	if !validShardCount(meta.ShardCount) {
		return 0, fmt.Errorf("invalid meta shard count %d: must be a power of two between 1 and %d", meta.ShardCount, maxShardCount)
	}
	if meta.SegmentIDBits != segmentIDBits || meta.SegmentSizeBits != segmentSizeBits || meta.RecordAlignmentBits != logAlignmentBits {
		return 0, fmt.Errorf("%w: unsupported segment layout (ID bits %d, size bits %d, alignment bits %d)",
			ErrMigrationRequired, meta.SegmentIDBits, meta.SegmentSizeBits, meta.RecordAlignmentBits)
	}
	if options.shardCountSet && options.shardCount != meta.ShardCount {
		return 0, fmt.Errorf("%w: changing shard count from %d to %d requires rewriting database data", ErrMigrationRequired, meta.ShardCount, options.shardCount)
	}
	// Do not silently ignore shard directories outside the declared layout.
	entries, err := os.ReadDir(db.path)
	if err != nil {
		return 0, fmt.Errorf("read database directory: %w", err)
	}
	for _, entry := range entries {
		id, err := strconv.Atoi(entry.Name())
		if (entry.IsDir() || err == nil) && (err != nil || id < 0 || id >= meta.ShardCount || entry.Name() != fmt.Sprintf("%02d", id) || !entry.IsDir()) {
			return 0, fmt.Errorf("invalid shard directory %q for stored count %d", entry.Name(), meta.ShardCount)
		}
	}
	return meta.ShardCount, nil
}

func (db *DB) migrateWriteMeta(meta metaData) error {
	staging := filepath.Join(db.path, metaTempName)
	// A crash before publication may leave this unpublished file. Its contents
	// never establish the database layout; only the renamed META does.
	if info, err := os.Lstat(staging); err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("invalid meta staging file")
		}
		if err := os.Remove(staging); err != nil {
			return fmt.Errorf("remove unfinished meta file: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat meta staging file: %w", err)
	}
	f, err := os.OpenFile(staging, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("create meta file: %w", err)
	}
	defer os.Remove(staging)
	writeErr := json.NewEncoder(f).Encode(meta)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	if err := errors.Join(writeErr, f.Close()); err != nil {
		return fmt.Errorf("write meta file: %w", err)
	}
	if err := os.Rename(staging, filepath.Join(db.path, metaName)); err != nil {
		return fmt.Errorf("publish meta file: %w", err)
	}
	dir, err := os.Open(db.path)
	if err != nil {
		return fmt.Errorf("open database directory: %w", err)
	}
	if err := errors.Join(dir.Sync(), dir.Close()); err != nil {
		return fmt.Errorf("sync meta directory: %w", err)
	}
	return nil
}
