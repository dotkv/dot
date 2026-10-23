# Testing Dot

This directory tests the supported API as an importing application. All Go tests
here use `package dot_test` and import `github.com/dotkv/dot`. Helpers must not
reach into the implementation or use Dot's encoder as their expected result.

Run all tests and check for races from the repository root:

```sh
go test ./...
go test -race ./...
```

Use `go test .` for just the internal storage tests, or `go test ./tests` for
just the public API suite.

## Local setup

If VS Code's **run package tests** runs `go test dot` and reports "package dot
is not in std", check `go list .` from the repository root. It should print
`github.com/dotkv/dot`. Run **Developer: Reload Window** to clear the Go
extension's cached package paths, then retry. The package declaration `dot`
is not the import path; there is no need to change `go.mod`.

On Linux, `/tmp` may be a memory-backed filesystem with a per-user quota.
Storage boundary tests write 64 MiB log segments, so they can hit "disk quota
exceeded" even when the public suite passes and `df` still shows free space.
Use an existing directory on disk for temporary build and test files:

```sh
mkdir -p "$HOME/.cache/dot/tmp"
TMPDIR="$HOME/.cache/dot/tmp" go test ./...
TMPDIR="$HOME/.cache/dot/tmp" go test -race ./...
```

`TMPDIR` applies to both Go's build files and `t.TempDir()`. `GOTMPDIR` alone
only moves build files. Tests clean up their temporary directories normally.

For VS Code, set `go.testEnvVars` in local workspace settings. Disk-backed
tests can also need longer than the extension's default 30-second timeout:

```json
{
  "go.testEnvVars": { "TMPDIR": "~/.cache/dot/tmp" },
  "go.testTimeout": "5m"
}
```

If Go runs through mise shims, a local `.mise.local.toml` can instead set
`TMPDIR` under `[env]` to the directory's absolute path. After `mise trust
.mise.local.toml`, this applies to terminal and editor Go commands in the
checkout. Keep that machine-specific file in `.git/info/exclude`.

## Suite layout

The public suite has five files:

- `api_test.go`: keys, scans, errors, ownership, expiration, relative TTLs,
  conditions, callbacks, and locks.
- `values_test.go`: every supported type, edge values, type mismatches, updates,
  arithmetic boundaries, and decoded scan values checked for exact types and bits.
- `concurrency_test.go`: concurrent reads, mutations, atomic updates and
  arithmetic, conditional limits, shutdown, and recovery of concurrent writes.
- `metrics_test.go`: HTTP assets, prefixed dashboards, WebSocket updates, shared
  viewers, and disconnect on database close, decoded through the JSON protocol.
- `model_test.go`: deterministic mixed-operation traces against an independent
  reference map; repeated growth, overwrite, delete, and reopen workloads; and
  subprocess recovery after acknowledged Sync writes without a graceful Close.

The model covers all value types, empty and long keys, expired values, aborted
updates, per-operation durability overrides, and actual Close/Open cycles. A
failure prints the operation trace. `FuzzDBOperations` runs the same bounded
runner, so Go can minimize and save failing traces under `testdata/fuzz/`.

For a longer local run:

```sh
go test ./tests -run 'TestDB(Model|Churn|Concurrent|UpdateConcurrent|Crash)' -count=20
go test ./tests -run '^$' -fuzz '^FuzzDBOperations$' -fuzztime=60s -parallel=2
```

Ordinary `go test ./...` runs the deterministic workloads and fuzz seeds. Active
fuzzing and repeated stress runs are opt-in; CI also runs the full race suite.
The crash test checks process crashes, not power-loss durability. The public
churn workload lets background maintenance run normally without asserting that
a particular internal maintenance step ran on a particular schedule.

Tests beside the implementation protect storage-format compatibility, index
invariants, exact maintenance boundaries, allocation budgets, resource cleanup,
and injected I/O/mmap failures. Keep these focused: ordinary API behavior belongs
here. Add cases to an existing table or runner before adding another test file.

Measure coverage of the library across both suites, including calls from this
separate test package:

```sh
go test -coverpkg=github.com/dotkv/dot -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
```

Use coverage gaps to find missing behavior. Do not preserve an internal assertion
solely to maintain a percentage, or remove a fault test solely because its lines
are also executed by a successful public API workload.
