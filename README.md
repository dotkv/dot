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
Each index starts with 65,536 eight-byte slots and doubles at 75% occupancy,
independently of the other shards. Its 15 fingerprint bits support growth up to
2^31 slots (16 GiB) per shard. Reading a sealed log opens one additional file for
the duration of that read; rotation also opens the new head before closing the
old one. These temporary files are not cached.

Each shard's log also owns a fixed 4 KiB write buffer (256 KiB in total).
Writes assemble the header, key, and value into one record and append it with
one file write. Records larger than the buffer use a temporary heap allocation.
Index lookups for `Set` and `Delete` reuse the same buffer for payloads up to
4 KiB while holding the shard's exclusive lock. Larger payloads still use
temporary allocations. `Get` allocates its own payload so reads within a shard
can run concurrently and returned values remain independent of later calls.

Process RSS also includes Go's garbage collection headroom and unused heap
pages awaiting release. The indexes count toward the live Go heap, increasing
the amount of temporary allocation allowed between collections. Successful
`Get` calls allocate a key/value payload each time; for a 16-byte key and a
1 KiB value, Go rounds this allocation up to 1,152 bytes. Native DotKV releases
its read payloads explicitly, and its C indexes do not count toward Go's heap
budget. Thus matching index sizes does not guarantee matching process RSS.
Applications can tune `GOGC` or `GOMEMLIMIT` to trade collection work for lower
memory use; this library leaves those process-wide settings to the caller.

`New` currently starts with empty in-memory indexes; reopening and replaying an
existing database is not implemented. Existing flat log directories are not
migrated to the sharded layout. Changing the shard count also changes key
routing and requires redistribution of existing records.

Run checks with `go test ./...`, `go test -race ./...`, and `go vet ./...`.
