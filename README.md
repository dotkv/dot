# dot

A small log-backed key-value store in Go.

`New(path)` creates 64 shards in directories `path/00` through `path/63`.
Keys route to a directory using `CRC32-IEEE(key) & 63`. Each shard owns its
index, read/write lock, and log, so operations on different shards can run
concurrently. Logs rotate independently at the existing 64 MiB boundary.

The `New`, `Set`, `Get`, `Delete`, and `Destroy` API is unchanged. `Destroy`
waits for active operations, closes every shard's log, and removes the database
directory. Subsequent operations return `os.ErrClosed`.

An empty database keeps 64 log files open and allocates 32 MiB for its indexes.
Indexes grow independently as their shards fill. Reading a sealed log opens
one additional file for the duration of that read; rotation also opens the new
head before closing the old one. These temporary files are not cached.

Each shard's log also owns a fixed 4 KiB write buffer (256 KiB in total).
Writes assemble the header, key, and value into one record and append it with
one file write. Records larger than the buffer use a temporary heap allocation.
Reads keep their own payload allocations.

`New` currently starts with empty in-memory indexes; reopening and replaying an
existing database is not implemented. Existing flat log directories are not
migrated to the sharded layout. Changing the shard count also changes key
routing and requires redistribution of existing records.

Run checks with `go test ./...`, `go test -race ./...`, and `go vet ./...`.
