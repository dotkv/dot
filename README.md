# dot

A small log-backed key-value store in Go.

`New(path)` creates 64 shards in directories `path/00` through `path/63`.
Keys route to a directory using `CRC32-IEEE(key) & 63`. Each shard owns its
index, read/write lock, and log, so operations on different shards can run
concurrently. Logs rotate independently at the existing 64 MiB boundary.

`Close` waits for active operations, closes every shard's log, and unmaps its
index without removing files. `Destroy` releases the same resources and removes
the database directory, including after `Close`. Both are safe to call repeatedly
or concurrently. Subsequent operations return `os.ErrClosed`. Call `Close` or
`Destroy` when finished: garbage collection does not release the mappings.

An empty database keeps 64 log files open and maps 32 MiB for its indexes using
private, anonymous `syscall.Mmap` memory. Pages become resident as they are used.
Each index starts with 65,536 eight-byte slots and doubles at 75% occupancy,
independently of the other shards. Its 15 fingerprint bits support growth up to
2^31 slots (16 GiB) per shard. Reading a sealed log opens one additional file for
the duration of that read; rotation also opens the new head before closing the
old one. These temporary files are not cached. Index growth builds a new mapping
under the shard's exclusive lock before unmapping the old one. A failed mapping
returns an error without changing the index or appending the pending log record.
Failed initialization releases already opened files and mappings without
removing existing data. Unmap failures are returned and retained for a later
`Close` or `Destroy` to retry. A failed unmap after growth retains one previous
mapping, whose release must succeed before another replacement is allocated.
The `indexMemory` owner in `index_mmap.go` contains all mapping and unmapping;
the index algorithm operates on its slot slices under the shard's lock.

Each shard's log also owns a fixed 4 KiB write buffer (256 KiB in total).
Writes assemble the header, key, and value into one record and append it with
one file write. Records larger than the buffer use a temporary heap allocation.
Index lookups for `Set` and `Delete` reuse the same buffer for payloads up to
4 KiB while holding the shard's exclusive lock. Larger payloads still use
temporary allocations. `Get` allocates its own payload so reads within a shard
can run concurrently and returned values remain independent of later calls.

Index mappings contribute to RSS but are outside Go's live heap, heap profiles,
and `GOMEMLIMIT` accounting. Applications must budget for this memory separately.
Moving the slots off the heap removes their contribution to GC headroom; it does
not reduce the storage required for the slots themselves. The mappings contain
only integers, never Go pointers, and are not persisted to disk.

Process RSS also includes Go's garbage collection headroom and unused heap
pages awaiting release. Successful `Get` calls still allocate a key/value payload
each time; for a 16-byte key and a 1 KiB value, Go rounds this allocation up to
1,152 bytes. The library leaves process-wide GC settings to the caller.

`New` currently starts with empty in-memory indexes; reopening and replaying an
existing database is not implemented. Existing flat log directories are not
migrated to the sharded layout. Changing the shard count also changes key
routing and requires redistribution of existing records.

Run checks with `go test ./...`, `go test -race ./...`, and `go vet ./...`.
