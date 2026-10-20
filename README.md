# dot

A small log-backed key-value store in Go.

`Get[T Value](key)` returns `(T, error)`, supporting `[]byte`, `int64`, `uint64`,
`float64`, and `bool`. For example, use `db.Get[[]byte](key)` for owned bytes or
`db.Get[int64](key)` for an integer. Missing keys return the zero value of `T`
and the shared `ErrNotFound` sentinel directly, without wrapping it. Stored scalar
zeros and empty byte values return no error. Deleting a missing key succeeds.
`Set[T Value](key, value)` accepts the same types and can infer `T` from the
value: `db.Set(key, int64(42))` pairs with `db.Get[int64](key)`. Explicit type
arguments also work, such as `db.Set[int64](key, 42)` or `db.Set[[]byte](key, nil)`.
Byte slices use their raw contents; integers use eight little-endian bytes,
`float64` uses its IEEE 754 bits in the same byte order, and booleans use one
byte (`0` or `1`). Scalar encoding uses a local eight-byte buffer.
Invalid lengths and boolean encodings return `ErrInvalidValue`. Encodings have
no type tags; requesting another type with the same valid representation
interprets those bytes as that type. Generic methods require Go 1.27 or newer.

`DB.Get` selects the payload or scalar path, decodes the requested type, and
returns `ErrNotFound` for a missing position. Both index paths return a matching
slot or `-1`; the log paths read and verify records:

```text
Get[[]byte] -> Index.findPayload -> Log.readPayload
Get[scalar] -> Index.findScalar  -> Log.readScalar
Set/Delete  -> Index.findPayload -> Log.readPayload (reuses the write buffer)
```

`readFromPrefix` is the shared helper that completes an already-started
record read. The scalar path uses it only for records larger than its local
buffer, without reopening the file or rereading the prefix.

`New(path)` creates 64 shards in directories `path/00` through `path/63`.
Each operation hashes its key once with XXH64 (seed zero), implemented by
`github.com/cespare/xxhash/v2`.
The high six bits select the shard; the low bits select index buckets and
fingerprints, including during growth. Each shard owns its index, read/write
lock, and log, so operations on different shards can run concurrently. Logs
rotate independently at the existing 64 MiB boundary.

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
temporary allocations. `Get[[]byte]` allocates its own payload so returned values
remain independent of later calls. Scalar reads initially request only the
header, expected key, and up to eight value bytes, capped at a local 1 KiB buffer.
Candidates that need more bytes are completed in that buffer without allocating
a payload when they fit. Larger records fall back to an owned payload, and
reading a sealed segment still allocates while opening its file. Both read paths
allow concurrent reads within a shard.

Index mappings contribute to RSS but are outside Go's live heap, heap profiles,
and `GOMEMLIMIT` accounting. Applications must budget for this memory separately.
Moving the slots off the heap removes their contribution to GC headroom; it does
not reduce the storage required for the slots themselves. The mappings contain
only integers, never Go pointers, and are not persisted to disk.

Process RSS also includes Go's garbage collection headroom and unused heap
pages awaiting release. Successful `Get[[]byte]` calls allocate a key/value payload
each time; for a 16-byte key and a 1 KiB value, Go rounds this allocation up to
1,152 bytes. The library leaves process-wide GC settings to the caller.

`New` currently starts with empty in-memory indexes; reopening and replaying an
existing database is not implemented. Shard routing is stable across DB
instances and processes, allowing future replay to rebuild each shard's index
from its existing logs. XXH64 with seed zero and the high-six-bit shard selection
are part of the directory layout; changing the hash, seed, or shard count requires
migration of existing records. Existing flat log directories are not migrated to
the sharded layout.

Run checks with `go test ./...`, `go test -race ./...`, and `go vet ./...`.
