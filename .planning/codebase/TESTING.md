# Testing Patterns

**Analysis Date:** 2026-04-22

Mimir uses the standard Go testing tooling plus `testify` for assertions, `client_golang/testutil` for metric comparison, and the Grafana `e2e` framework for Docker-based integration tests. PromQL engine correctness is validated against a shared test-file format.

## Test Framework

**Runner:** Go's built-in `testing` package. No special runner.

**Assertion library:** `github.com/stretchr/testify` v1.11.1 (declared in `go.mod:38`).
- `github.com/stretchr/testify/require` — for conditions where continuing the test is pointless (e.g. setup failed, `require.NoError`).
- `github.com/stretchr/testify/assert` — for independent assertions where the test can keep reporting additional failures.
- Both are used in the same file routinely. See `pkg/distributor/distributor_test.go:298-305`.

**Metric comparison:** `github.com/prometheus/client_golang/prometheus/testutil` — `testutil.GatherAndCompare(reg, expected, names...)`. Example: `pkg/distributor/distributor_test.go:318, 468, 524`.

**Integration framework:** [grafana/e2e](https://github.com/grafana/e2e) (Docker-based), with Mimir-specific helpers in `integration/e2emimir/` (`service.go`, `client.go`, `storage.go`, etc.).

## Run Commands

```bash
make test                        # Unit tests (go test -timeout 30m ./... excluding ./integration)
make test-with-race              # Same, -race -tags netgo,stringlabels
make cover                       # Same with -coverprofile=$(COVERFILE)

# Integration tests (require Docker + locally-built grafana/mimir:latest)
make ./cmd/mimir/.uptodate       # Build the image first
go test -v ./integration/...     # Run all
go test -v ./integration -run "^TestChunksStorageAllIndexBackends$"   # Filter
go test -v -timeout=20m ./integration/...                             # Longer budget
```

See `Makefile:475-487` and `docs/internal/contributing/how-integration-tests-work.md`.

**Integration env vars** (`docs/internal/contributing/how-integration-tests-work.md:33-42`):
- `MIMIR_IMAGE` (default `grafana/mimir:latest`)
- `MIMIRTOOL_IMAGE` (default `grafana/mimirtool:latest`)
- `MIMIR_CHECKOUT_DIR` (default `$GOPATH/src/github.com/grafana/mimir`)
- `E2E_TEMP_DIR`, `E2E_NETWORK_NAME`

**Build tags:**
- Integration tests live under `integration/` and default to the `integration` package without a tag. The exception is `integration/tenant_with_metadata_limits_test.go` which is gated by `//go:build requires_docker`.
- Race builds use `-tags netgo,stringlabels` (`Makefile:482`).

## Test File Organization

**Location:** Co-located with source. A file `foo.go` has its tests in `foo_test.go` in the same package. Integration and cross-package tests sometimes use `package foo_test` (blackbox).

**Naming:**
- Unit tests: `<subject>_test.go` → `TestXxx`.
- Mocks: `<thing>_mock.go` (non-test, exported so other packages can use them) or `mock_<thing>_test.go` (test-only). Examples:
  - `pkg/util/validation/limits_mock.go` — exported test doubles (`MockOverrides`, `NewMockTenantLimits`, `MockDefaultLimits`).
  - `pkg/storage/bucket/client_mock.go`
  - `pkg/storage/tsdb/block/block_mock.go`
  - `pkg/ingester/lookupplan/mock_statistics_test.go` — test-only mockery output.
- Benchmarks: `<subject>_bench_test.go` or inline in `<subject>_test.go`, functions named `BenchmarkXxx`.
- Fuzz tests: `<subject>_fuzz_test.go` with `FuzzXxx`. See `pkg/streamingpromql/fuzz/`.

**Layout example:**
```
pkg/distributor/
├── push.go
├── push_test.go
├── distributor.go
├── distributor_test.go
├── ha_dedupe_per_sample.go
├── ha_dedupe_per_sample_test.go
├── validate.go
└── validate_test.go
```

## Test Structure

**Preferred: map-based table-driven tests.** The map key becomes the subtest name. This is the dominant style across the codebase.

```go
// pkg/distributor/distributor_test.go:136
for name, tc := range map[string]struct {
    metricNames           []string
    numIngesters          int
    happyIngesters        int
    samples               samplesIn
    metadata              int
    expectedErrorContains []string
    expectedGRPCError     *status.Status
    expectedErrorDetails  *mimirpb.ErrorDetails
    expectedMetrics       string
    timeOut               bool
    configure             func(*Config)
    customRequest         *mimirpb.WriteRequest
    expectDedupTraceEvent bool
}{
    "A push of no samples shouldn't block or return error, even if ingesters are sad": {
        numIngesters:   3,
        happyIngesters: 0,
    },
    "A push to 3 happy ingesters should succeed": {
        numIngesters:   3,
        happyIngesters: 3,
        samples:        samplesIn{num: 5, startTimestampMs: 123456789000},
        // ...
    },
}
```

Slice-based tables (`for _, tc := range []struct{...}{...}`) are also common when order matters or names are derived from fields. See `pkg/distributor/distributor_test.go:1127, 1237`.

**Subtests:** always wrap loop bodies with `t.Run(name, func(t *testing.T) { ... })` so `go test -run` can target individual cases.

**Parallelism:** use `t.Parallel()` in leaf subtests when state is isolated. Capture loop variables (`tc := tc`) before calling `t.Parallel()` on Go < 1.22; Mimir targets recent Go so the per-iteration copy is the default, but legacy patterns remain.

**Setup / teardown:**
- `t.Cleanup(fn)` for per-test resets. Example `pkg/distributor/distributor_test.go:130` resets monotonic time: `t.Cleanup(mtime.NowReset)`.
- Prefer `t.TempDir()` to manual `os.MkdirTemp`.

## Mocking

**Strategy:** Mimir mixes three approaches. There is no global mockery config in-repo — mocks are typically hand-written or generated ad hoc.

1. **Hand-written mocks in `*_mock.go` files**, exported for cross-package use.
   - `pkg/util/validation/limits_mock.go`:
     - `NewMockTenantLimits(map[string]*Limits) TenantLimits`
     - `MockOverrides(customize func(defaults *Limits, tenantLimits map[string]*Limits)) *Overrides`
     - `MockDefaultLimits()`, `MockDefaultOverrides()`
   - `pkg/storage/bucket/client_mock.go` — an in-memory bucket client.
   - `pkg/storage/tsdb/block/block_mock.go`.

2. **Test-only mocks** named `mock_*_test.go`, e.g. `pkg/ingester/lookupplan/mock_statistics_test.go` (mockery-generated, kept inside the package).

3. **Interface stubs inline in `_test.go`** — ad hoc implementations of a small interface for a single test.

**What to mock:**
- External dependencies: object storage (`bucket.Client`), Kafka (`writer_client`), ingester ring endpoints, tenant overrides.
- Anything that would otherwise require network/disk.

**What not to mock:**
- The Prometheus TSDB — tests use real, on-disk TSDB instances via `t.TempDir()`.
- Proto marshalling — exercise the real `mimirpb.PreallocWriteRequest` code path so `yoloString` hazards are visible.
- The dskit `services.Service` state machine.

## Fixtures and Factories

- Limits fixtures: `validation.MockDefaultLimits()` / `MockDefaultOverrides()` in `pkg/util/validation/limits_mock.go`.
- Write request builders: helpers in `pkg/distributor/push_test.go` (`verifyWritePushFunc`, `readBodyPushFunc`, `dummyPushFunc` at `:512, :527, :1317`).
- Integration-test helpers: `integration/e2emimir/` exposes `Mimir()`, `Alertmanager()`, `Compactor()`, etc.; plus `integration/configs.go` and `integration/util.go`.
- There is no `testdata/` convention across the repo; fixture data lives next to the tests that need it.

## Coverage

No hard threshold enforced by CI. Coverage is generated via `make cover` which writes `coverage.out` (`Makefile:487`). There is no badge or gate.

```bash
make cover
go tool cover -html=coverage.out
```

## Test Types

**Unit tests:**
- Live under `pkg/...`, `cmd/...`, `tools/...`.
- Scope: a single package, dependencies mocked.
- Run with `make test`.

**Integration tests (Docker):**
- Live in `integration/`.
- Spin up Mimir + dependencies (Consul, memberlist, object storage, Memcached, etc.) in Docker via `grafana/e2e`.
- Each test creates its own Docker network and tears it down — isolation is per-test (`docs/internal/contributing/how-integration-tests-work.md:44-46`).
- Require a locally-built `grafana/mimir:latest` image (`make ./cmd/mimir/.uptodate`).
- Use the `integration` package. Start points: `integration/distributor_test.go`, `integration/ingester_test.go`, `integration/backfill_test.go`, `integration/otlp_ingestion_test.go`.

**Benchmarks:**
- Naming: `BenchmarkXxx(b *testing.B)`.
- Locations: often in dedicated `*_bench_test.go` files (e.g. `pkg/mimirpb/unmarshal_bench_test.go:18`) or alongside unit tests (`pkg/mimirpb/compat_test.go:202, 213, 269, 307`).
- Run:
  ```bash
  go test -run=XXX -bench=BenchmarkUnMarshal -benchtime=5s ./pkg/mimirpb/...
  ```
- The dedicated query-engine benchmark tool lives at `tools/benchmark-query-engine/`.

**Fuzz tests:**
- PromQL fuzzing: `pkg/streamingpromql/fuzz/` with corpora in `pkg/streamingpromql/testdata/fuzz/`.

## PromQL Test Files

Mimir's streaming PromQL engine (MQE) is validated against Prometheus-format `.test` files.

**Locations:**
- `pkg/streamingpromql/testdata/upstream/` — copied from Prometheus `promql/promqltest/testdata`, with tests MQE does not yet support disabled. **Do not add ours-specific cases here.**
- `pkg/streamingpromql/testdata/ours/` — Mimir-only tests (features or edge cases beyond upstream).
- `pkg/streamingpromql/testdata/ours-only/` — Mimir-only tests that cannot run against upstream.
- `pkg/streamingpromql/testdata/fuzz/` — fuzzing corpora.

**Format** (Prometheus `promqltest`): `load`, `clear`, and `eval` directives. Example `pkg/streamingpromql/testdata/ours/limit.test`:

```
load 5m
  http_requests{job="api-server", instance="0", group="production"}	0+10x10
  ...

eval instant at 50m count(limitk by (group) (1, http_requests) and http_requests)
  {} 2
```

**Formatter:** `make format-promql-tests` runs `tools/format-promql-test.sh`, which:
- Indents non-directive lines to two spaces.
- Strips leading whitespace from `load`/`eval`/`clear`.
- Collapses trailing whitespace and repeated blank lines.
- Migrates `eval_info`/`eval_warn` to the new `expect info`/`expect warn` syntax introduced in Prometheus PR #15995.

CI runs `check-promql-tests` (`Makefile:508-509`), which fails on uncommitted formatter output.

**Supporting tooling:** `tools/check-for-disabled-but-supported-mqe-test-cases/`, `tools/promql-generator/`.

## Jsonnet / Mixin Tests

**Mixin tests** live under `operations/mimir-mixin-tests/`:
- `run.sh` — entry point.
- `test-custom-labels.libsonnet` — configuration under test.
- `test-custom-labels-asserts.sh` — assertions.

They render the mixin with alternate configurations and assert on the output.

**Mixin checks** invoked from CI (`Makefile:711-714`): `check-mixin`, `check-mixin-jb`, `check-mixin-mixtool`, `check-mixin-runbooks`, `check-mixin-mimirtool-rules`. Run `make build-mixin format-mixin` before committing mixin changes.

**Jsonnet manifest checks:** `make check-jsonnet-manifests` (`Makefile:746-748`) verifies `operations/mimir`, `operations/mimir-tests`, and `development/` are formatted via `make format-jsonnet-manifests`.

## Config Documentation Tests

- `tools/doc-generator/` renders flag and YAML reference docs from the code. `make doc` rebuilds `docs/sources/mimir/references/configuration-parameters/*.md`.
- `make reference-help` rebuilds CLI help. Output is committed.
- `make check-doc` and `make check-reference-help` (`Makefile:13`) are CI guards — they re-run the generators and `git diff --exit-code`. Any config/flag change that doesn't regenerate docs will fail CI.

## Common Patterns

**Metric assertions:**
```go
// pkg/distributor/distributor_test.go:468
require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
    # HELP ...
    # TYPE ...
    cortex_distributor_xxx{user="user"} 1
`), "cortex_distributor_xxx"))
```

**Error-contains assertions:**
```go
assert.ErrorContains(t, err, msg)   // pkg/distributor/distributor_test.go:305
```

**Per-test fresh registry and overrides:**
```go
reg := prometheus.NewPedanticRegistry()
limits := validation.MockOverrides(func(d *validation.Limits, tenantLimits map[string]*validation.Limits) {
    d.IngestionRate = 1e6
})
```

**Dskit service lifecycle:**
```go
require.NoError(t, services.StartAndAwaitRunning(context.Background(), svc))
t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), svc) })
```

**Async assertions** use `test.Poll(t, timeout, expected, func() interface{}{...})` from dskit or `assert.Eventually`.

**Test-time overrides for package-level behaviour** are gated behind helpers like `mtime.NowReset` (`pkg/distributor/distributor_test.go:130`) so tests never leak state into their neighbours.

---

*Testing analysis: 2026-04-22*
