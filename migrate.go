package dot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

const (
	// Version 1 pins this metadata layout, log record encoding, alignment, and
	// XXH64 shard routing. It is independent of the metrics history format.
	diskFormatVersion = 1
	metaMagic         = "DOTMETAD"
	metaSize          = 64
	metaName          = "META"
	metaTempName      = "META.tmp"
)

type metaData struct {
	DiskFormatVersion   uint32
	ShardCount          int
	SegmentIDBits       byte
	SegmentSizeBits     byte
	RecordAlignmentBits byte
}

func currentMeta(count int) metaData {
	return metaData{
		DiskFormatVersion: diskFormatVersion, ShardCount: count,
		SegmentIDBits: segmentIDBits, SegmentSizeBits: segmentSizeBits,
		RecordAlignmentBits: logAlignmentBits,
	}
}

// META is exactly 64 bytes, with explicit little-endian fields:
// [0:8] signature, [8:12] disk format version, [12:14] shards,
// [14:17] segment/alignment bits, [17:60] reserved zeros, [60:64] CRC32-IEEE.
func encodeMeta(meta metaData) [metaSize]byte {
	var data [metaSize]byte
	copy(data[:8], metaMagic)
	binary.LittleEndian.PutUint32(data[8:12], meta.DiskFormatVersion)
	binary.LittleEndian.PutUint16(data[12:14], uint16(meta.ShardCount))
	data[14], data[15], data[16] = meta.SegmentIDBits, meta.SegmentSizeBits, meta.RecordAlignmentBits
	binary.LittleEndian.PutUint32(data[60:], crc32.ChecksumIEEE(data[:60]))
	return data
}

func decodeMeta(data []byte) (metaData, error) {
	var meta metaData
	if len(data) != metaSize {
		return meta, fmt.Errorf("invalid meta file: expected exactly %d bytes", metaSize)
	}
	if binary.LittleEndian.Uint32(data[60:]) != crc32.ChecksumIEEE(data[:60]) {
		return meta, errors.New("invalid meta file: checksum mismatch")
	}
	if string(data[:8]) != metaMagic {
		return meta, errors.New("invalid meta file: unknown signature")
	}
	meta = metaData{
		DiskFormatVersion: binary.LittleEndian.Uint32(data[8:12]),
		ShardCount:        int(binary.LittleEndian.Uint16(data[12:14])),
		SegmentIDBits:     data[14], SegmentSizeBits: data[15], RecordAlignmentBits: data[16],
	}
	if meta.DiskFormatVersion != diskFormatVersion {
		return meta, fmt.Errorf("unsupported disk format version %d (want %d)", meta.DiskFormatVersion, diskFormatVersion)
	}
	for _, value := range data[17:60] {
		if value != 0 {
			return meta, errors.New("invalid meta file: nonzero reserved bytes")
		}
	}
	if !validShardCount(meta.ShardCount) {
		return meta, fmt.Errorf("invalid meta shard count %d: must be a power of two between 1 and %d", meta.ShardCount, maxShardCount)
	}
	if meta.SegmentIDBits != segmentIDBits || meta.SegmentSizeBits != segmentSizeBits || meta.RecordAlignmentBits != logAlignmentBits {
		return meta, fmt.Errorf("unsupported segment layout (ID bits %d, size bits %d, alignment bits %d)",
			meta.SegmentIDBits, meta.SegmentSizeBits, meta.RecordAlignmentBits)
	}
	return meta, nil
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
			return 0, errors.New("META is missing from a nonempty database directory; cannot determine its layout")
		}
		if err := db.migrateWriteMeta(currentMeta(options.shardCount)); err != nil {
			return 0, err
		}
		return options.shardCount, nil
	}
	if err != nil {
		return 0, fmt.Errorf("stat meta file: %w", err)
	}
	if !fi.Mode().IsRegular() || fi.Size() != metaSize {
		return 0, fmt.Errorf("invalid meta file: expected a regular file of exactly %d bytes", metaSize)
	}
	f, err := os.Open(metaPath)
	if err != nil {
		return 0, fmt.Errorf("open meta file: %w", err)
	}
	defer f.Close()
	var data [metaSize]byte
	if _, err := io.ReadFull(f, data[:]); err != nil {
		return 0, fmt.Errorf("read meta file: %w", err)
	}
	meta, err := decodeMeta(data[:])
	if err != nil {
		return 0, err
	}
	if options.shardCountSet && options.shardCount != meta.ShardCount {
		return 0, fmt.Errorf("changing shard count from %d to %d requires rewriting database data", meta.ShardCount, options.shardCount)
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
	data := encodeMeta(meta)
	_, writeErr := f.Write(data[:])
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
