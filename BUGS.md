# Bugs and review findings

Reviewed on 2026-10-23 against the working tree. These findings come from code
inspection, not successful reproductions. No tests or benchmarks were run to
validate them. Concurrent implementation changes may resolve or alter a finding;
recheck the named functions before working on it.

Severity reflects the potential impact when the described condition occurs.
The power-loss finding is a durability concern requiring targeted validation,
not a reproduced loss of acknowledged data.

| ID | Severity | Finding | Evidence |
| --- | --- | --- | --- |
| DOT-001 | High | Newly created directory entries are not explicitly persisted in their parents | Persistence ordering inspected; power loss not reproduced |
| DOT-002 | High | Corrupt record lengths can trigger excessive allocation before validation | Allocation and validation order inspected; OOM not reproduced |
| DOT-003 | Medium | Scan silently terminates on storage errors | Error-discarding control flow inspected |

## DOT-001: Persist newly created directory entries before promising durability

**Locations:** `acquireLock` and `Open` in [db.go](db.go), `newLog`, `openHead`,
and `syncDirectory` in [log.go](log.go), `migrateWriteMeta` in
[migrate.go](migrate.go).

**Observed ordering:**

1. `acquireLock` creates the database directory with `os.MkdirAll` if needed.
2. `migrateWriteMeta` publishes `META` and syncs the database directory.
3. `Open` subsequently calls `newLog` for each shard. `newLog` creates its shard
   directory with `os.MkdirAll`.
4. `openHead` creates and syncs the initial log file and syncs the shard directory.
5. A successful `Sync` mutation syncs the log file.

There is no explicit sync of the database directory after the shard directories
are created. The parent of a newly created database directory is also not
explicitly synced. Syncing a child directory does not establish a portable
guarantee that its own name has been persisted in its parent. See the
[Linux fsync documentation](https://man7.org/linux/man-pages/man2/fsync.2.html).

**Potential impact:** A power failure during initialization or before those
parent entries happen to reach storage could make a newly created database or
shard directory disappear despite a successful `Sync` write. This undermines the
documented power-loss guarantee for that case. This concern applies to new
directory entries; it does not establish that ordinary rotation inside an
already persisted shard directory has the same defect.

**Suggested resolution:** Explicitly persist every newly created directory entry
in its parent, including any ancestors created by `MkdirAll`, before allowing
the database to acknowledge durable writes. Propagate failures through `Open`.
Preserve the existing file and shard-directory syncs.

**Validation needed:** Check directory-creation/sync ordering and inject failures
at the relevant boundaries. Use a filesystem crash model or power-loss harness
to validate recovery after acknowledged `Sync` writes on a newly created
hierarchy. Killing a process alone does not test persistence of directory
entries after power loss.

## DOT-002: Validate record lengths before allocating payload buffers

**Locations:** `readFromPrefix` and its callers `readPayload` and `readScalar`
in [log.go](log.go).

`readFromPrefix` reads the two uint32 length fields and immediately computes:

```go
size := int(keylen) + int(vallen)
buf = make([]byte, size)
```

The allocation occurs before payload reads, checksum verification, or record-type
validation. A corrupted length can therefore request gigabytes even when the
actual record is small. On a 32-bit host, conversion or addition can also
overflow and lead to invalid allocation or slice bounds.

**Trigger:** A record's length fields become corrupted while its address remains
in the live index, then a normal read or maintenance operation reads that record.
Changing only a length field is sufficient to reach the allocation; the checksum
does not need to match. Startup replay has separate checks, so this finding is
specifically about the normal payload-reading path.

**Potential impact:** Excessive memory allocation, process termination under
memory pressure, or a panic instead of a controlled corruption error. The exact
outcome depends on architecture, allocator behavior, and memory limits.

**Suggested resolution:** Validate type/length combinations early, perform
checked size and offset arithmetic, and reject lengths exceeding supported
limits or the available file/buffer extent before allocating. Keep checksum
verification. Apply any explicit size policy consistently to writes and reads,
with compatibility considered for existing files. Do not assume that the last
record must end within the nominal 64 MiB segment boundary; the current format
allows it to cross that boundary.

**Validation needed:** Feed malformed length fields to the read paths and assert
a controlled error before a large allocation. Cover truncated records,
fixed-width types with invalid lengths, maximum uint32 lengths, integer
overflow on supported architectures, and valid large records. Any attempt to
reproduce exhaustion should run in a resource-limited subprocess.

## DOT-003: Expose scan failures to the caller

**Locations:** `DB.Scan` and `scanShard` in [db.go](db.go).

When `s.log.readPayload` returns an error, `scanShard` returns `false`. `Scan`
then ends the iterator without exposing the error. The same completion signal
also represents an intentional early stop or a closed shard.

**Trigger:** A record fails checksum verification, a read returns an I/O error,
or opening a sealed segment fails during iteration. The current comment that
read errors cannot occur without storage corruption is too strong; operational
failures can also cause them.

**Impact:** A caller cannot distinguish a complete scan from a partial or empty
result. Export, auditing, counting, and application-level reconciliation can
silently operate on incomplete data. This does not itself corrupt stored data.

**Suggested resolution:** Provide an error-reporting iterator or scan API with
an explicit terminal error. Distinguish successful completion and caller-requested
early termination from storage failure and database closure. Update the API
documentation accordingly.

**Validation needed:** Force a checksum failure and a non-corruption read/open
failure during scanning; assert that each reaches the caller. Also cover clean
completion, deliberate early termination, and scanning a closed database.

## Related API limitation: Scan does not preserve value types

**Locations:** `scanShard` in [db.go](db.go), boolean encodings in
[value.go](value.go), and the scan documentation in [docs/usage.html](docs/usage.html).

The scan discards the record type and yields only the payload. Both boolean
values have empty payloads because their logical value is encoded in the type
tag. Strings and byte slices also lose their distinction, as do numeric types
whose encodings overlap.

This behavior is documented and is **not classified here as an implementation
bug or stored-data corruption**. It does mean that `Scan` cannot serve as a
lossless export/restore interface for a heterogeneous database. If such an
interface is added, return an entry containing type information alongside its
key and value, and ensure that every supported value type can round-trip.

Compaction pauses, cold-overwrite read costs, and missing batch/snapshot APIs
are separate performance or feature considerations, not correctness bugs in
this report.
