---
phase: 01-backfill-pre-verification
plan: 01
subsystem: testing
tags: [mimirtool, backfill, verification, framework, go-kit-log, errgroup]

# Dependency graph
requires: []
provides:
  - verify.Verifier orchestrator with functional-options composition
  - verify.BlockVerifier and verify.BatchVerifier interfaces (extension seam for v2 batch checks)
  - verify.BlockRef input type for batch checks (already-parsed meta, no re-read)
  - verify.Mode enum (Deep / Medium) with Verifier.Mode() accessor
  - verify.Report aggregator with per-block per-check failure entries and summary-only Err()
  - WithBlockCheck, WithBatchCheck, WithMode, WithFailFast, WithConcurrency options
  - Fail-fast semantics with explicit "skipping batch checks" info log line
  - Bounded parallelism via errgroup.WithContext + SetLimit (auto = min(GOMAXPROCS, 4))
affects: [01-02, 01-03, 01-04, 01-05]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Functional options for Verifier composition (no global registry, no init())"
    - "errgroup.WithContext + SetLimit for bounded per-block parallelism"
    - "Mutex-guarded Report with map-based unique-block counting"
    - "Summary-only aggregated error (detail in log lines, not in error string)"

key-files:
  created:
    - pkg/mimirtool/backfill/verify/verify.go
    - pkg/mimirtool/backfill/verify/options.go
    - pkg/mimirtool/backfill/verify/report.go
    - pkg/mimirtool/backfill/verify/verify_test.go
  modified: []

key-decisions:
  - "Exported aggregator type is Report (package-qualified verify.Report) — VerificationReport would be redundant"
  - "Report.Err() returns summary-only text; per-failure detail is emitted as log lines to avoid unreadable error strings for 500+ block batches"
  - "Batch stage is skipped when fail-fast trips, but emits an explicit info log line so users who registered a BatchVerifier know why it did not run"
  - "WithConcurrency(n) with n<0 is clamped to 0 (auto) instead of panicking"
  - "errFailFast is an internal lowercase sentinel — the errgroup signal never leaks to callers; Run returns only *Report"

patterns-established:
  - "Pattern: three-file package layout (interfaces+orchestrator / options / report) is the template that 01-02 and 01-03 plug into without touching the framework files"
  - "Pattern: SPDX-only header on net-new Mimir code; Provenance-includes-* only when copying from upstream (cortex-tools, Prometheus, Thanos)"
  - "Pattern: test doubles for framework-seam proof live inline in _test.go (no _test helpers package), matching Mimir's existing test style"

requirements-completed: [FRAMEWORK-01, FRAMEWORK-02, FRAMEWORK-04]

# Metrics
duration: 5min
completed: 2026-04-22
---

# Phase 1 Plan 01: Verify Package Skeleton Summary

**Pluggable verifier framework under `pkg/mimirtool/backfill/verify/` with BlockVerifier/BatchVerifier interfaces, functional-options composition, errgroup-bounded parallelism, and summary-only Report error — seam proven by inline test doubles.**

## Performance

- **Duration:** 5 min
- **Started:** 2026-04-22T18:41:11Z
- **Completed:** 2026-04-22T18:46:02Z
- **Tasks:** 2
- **Files created:** 4

## Accomplishments

- Created `pkg/mimirtool/backfill/verify/` package with production types: `Verifier`, `BlockVerifier`, `BatchVerifier`, `BlockRef`, `Mode` (Deep/Medium), `Report`, `Failure`, and `Option` functional-options.
- `Verifier.Run` parses `meta.json` once per block, runs per-block checks in bounded-parallel via `errgroup.WithContext` + `SetLimit`, then runs batch checks serially after per-block settles. Concurrency auto-picks `min(GOMAXPROCS, 4)` when unset.
- Fail-fast (default) skips the batch stage on any per-block failure, but emits an explicit `level=info msg="skipping batch checks due to per-block fail-fast"` log line with `batch_check_count`.
- Full-report mode (`WithFailFast(false)`) drives every block through every check before the batch stage runs.
- `Report.Err()` returns `verification failed: N failure(s) across M block(s)` — summary only, no per-failure enumeration, so 500-block batches do not blow up the error string.
- Framework extension seam proven by six tests including an inline `rejectMultiBatchVerifier` test double; zero real `BatchVerifier` implementations ship in v1 (deferred to v2).
- All checks pass: `go build`, `go vet`, `go test -count=1`, `make format` (no diff), and the `goimports -l` check returns empty.

## Exported API Surface (stable contract for 01-02, 01-03, 01-04, 01-05)

Package `github.com/grafana/mimir/pkg/mimirtool/backfill/verify`:

| Symbol | Kind | Notes |
|---|---|---|
| `Mode` | type (int) | `Deep` (default), `Medium` |
| `Deep`, `Medium` | const Mode | iota'd |
| `BlockVerifier` | interface | `Name() string`, `Verify(ctx, blockDir string, meta block.Meta) error` |
| `BatchVerifier` | interface | `Name() string`, `Verify(ctx, blocks []BlockRef) error` |
| `BlockRef` | struct | `Dir string`, `Meta block.Meta` |
| `Verifier` | struct | Orchestrator; has `Mode()` accessor and `Run(ctx, blockDirs)` |
| `NewVerifier(logger, opts ...Option) *Verifier` | constructor | Defaults: `Deep`, `failFast=true`, `concurrency=0` (auto) |
| `Option` | type `func(*options)` | |
| `WithBlockCheck(BlockVerifier) Option` | option | Appendable; ordered |
| `WithBatchCheck(BatchVerifier) Option` | option | Appendable; runs after per-block |
| `WithMode(Mode) Option` | option | |
| `WithFailFast(bool) Option` | option | Default `true` |
| `WithConcurrency(int) Option` | option | `n<0` clamped to 0 (auto) |
| `Failure` | struct | `BlockULID`, `BlockDir`, `Check`, `Err` |
| `Report` | struct | Thread-safe aggregator |
| `(*Report).Add(blockULID, checkName, blockDir string, err error)` | method | 4-arg signature |
| `(*Report).HasFailures() bool` | method | |
| `(*Report).Failures() []Failure` | method | Returns a copy |
| `(*Report).Summary() (total, failedBlocks, totalFailures int)` | method | |
| `(*Report).Err() error` | method | Summary-only text, `nil` when no failures |

## Task Commits

1. **Task 1: Create verify package with Verifier, interfaces, options, and Report types** — `53bed28ab7` (feat)
2. **Task 2: Write framework-seam tests with dummy BlockVerifier + dummy BatchVerifier** — `2bb61e8fb2` (test)

_Note: This plan is a TDD plan per frontmatter, but the plan's action text directed production code first (Task 1) then tests (Task 2). Tests pass immediately against the implementation; the RED gate is implicit in the plan author's design. This is captured under "Deviations" below._

## Files Created/Modified

- `pkg/mimirtool/backfill/verify/verify.go` — Verifier orchestrator, BlockVerifier/BatchVerifier interfaces, BlockRef, Mode enum, NewVerifier constructor, Run method with errgroup parallelism + fail-fast skip log line.
- `pkg/mimirtool/backfill/verify/options.go` — Option type plus five functional-options helpers.
- `pkg/mimirtool/backfill/verify/report.go` — Failure struct, thread-safe Report aggregator, summary-only Err().
- `pkg/mimirtool/backfill/verify/verify_test.go` — Six framework-seam tests using inline test doubles.

## Fail-Fast Semantics (for reviewers of plans 01-02 / 01-04)

- On first per-block failure in `failFast=true` mode, the worker returns the internal sentinel `errFailFast` to the errgroup, which cancels the errgroup context. Peer workers observe `ctx.Done()` **between blocks and between checks** and return without scheduling new work.
- **Important:** already-running `block.VerifyBlock` calls are NOT aborted mid-walk. The Prometheus TSDB postings walk does not check `ctx` between iterations, so a deep verify that has already started on another worker may complete and record a stray failure. This is documented in the `Verifier.Run` doc-comment and accepted per D-19.
- When fail-fast trips and at least one `BatchVerifier` is registered, Run emits exactly one info log line with shape:
  `level=info msg="skipping batch checks due to per-block fail-fast" batch_check_count=<N>`.
  Plans 01-04 and 01-05 may assert against this shape if they want to test batch-skip behavior.

## Decisions Made

- **Report (not VerificationReport):** The type is `verify.Report`. Package-qualified, `VerificationReport` would be redundant. Called out up-front so 01-02 / 01-04 import the right name.
- **Summary-only Err():** Explicitly locked by `TestReport_ErrFormat` — the aggregated error is of the form `^verification failed: \d+ failure\(s\) across \d+ block\(s\)$`. Any future refactor that wants to add detail to `Err()` must update this test and justify the trade-off for large batches.
- **Inline test doubles:** `rejectMultiBatchVerifier` lives in `verify_test.go` and proves the extension seam without any test-helpers package or fixture directory. Future v2 batch checks replace this double with a real implementation.

## Deviations from Plan

### Process Deviation (TDD ordering)

**1. [Rule 3 — Blocking] TDD phase ordering: production code first, tests second**

- **Found during:** Task 1 kickoff.
- **Issue:** Both tasks in the plan frontmatter are marked `tdd="true"`, which normally implies RED → GREEN → REFACTOR. The plan's `<action>` text for Task 1 provides the full production implementation directly (verify.go, options.go, report.go) and the Task 2 `<action>` text provides the test file. The plan author's action text is authoritative; following it produces a test file that immediately passes rather than a classic RED commit.
- **Fix:** Followed the plan's action text as written: Task 1 = production code (`feat` commit), Task 2 = tests (`test` commit). This matches the plan's commit type hints and avoids a stub-then-rewrite cycle that would not add coverage.
- **Files modified:** None extra.
- **Verification:** `go test ./pkg/mimirtool/backfill/verify/... -count=1` exits 0 on Task 2; all 6 tests green.
- **Committed in:** `53bed28ab7`, `2bb61e8fb2`.

### Code Deviations

None. All three production files and the test file match the plan's action text verbatim except for `make format` preserving the plan's import ordering as-is.

---

**Total deviations:** 1 process deviation (TDD ordering clarification). 0 code deviations.
**Impact on plan:** None. Plan author's intent preserved; tests verify the implementation; six-test success matches the plan's `<verify>` block exactly.

## TDD Gate Compliance

The plan frontmatter marks both tasks `tdd="true"`. Git log for this plan shows:

- `test` commit: `2bb61e8fb2` (Task 2)
- `feat` commit: `53bed28ab7` (Task 1)

Because the plan's action text for Task 1 is the full production implementation and Task 2 is the test file, the commit order is `feat → test` rather than the classic `test → feat`. Tests pass on first run. This is the plan's designed order; the test commit still locks the contract so that future changes to `verify.go` / `options.go` / `report.go` that break the invariants (4-arg `Add`, summary-only `Err`, extension seam, fail-fast halts processing) will show up as test failures. Gate functionally satisfied; sequence differs from idealised RED-first only because the plan's action text combined both phases into the production task.

## Issues Encountered

None. `go build`, `go vet`, `go test -count=1`, and `make format` all pass on first try.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `WellFormedVerifier` (plan 01-02) plugs in via `WithBlockCheck(wellFormedVerifier)` — no framework change needed.
- `SingleUTCDayVerifier` (plan 01-03) plugs in the same way; may call `Verifier.Mode()` if it needs to branch on Deep vs Medium.
- `MimirClient.BackfillWithOptions` (plan 01-04) constructs a `Verifier` via `verify.NewVerifier(logger, ...)`, calls `Run(ctx, blockDirs)`, inspects `report.HasFailures()`, and propagates `report.Err()` as the returned error. Zero HTTP requests issued on failure since the upload loop is gated on `!report.HasFailures()`.
- CLI flag wiring (plan 01-05) threads `--full-report`, `--skip-chunk-verification`, and `--verify-concurrency` into the corresponding `With*` options. The fail-fast skip log line may be asserted against in any integration test; its exact shape is documented above.

## Self-Check

- [x] `pkg/mimirtool/backfill/verify/verify.go` exists.
- [x] `pkg/mimirtool/backfill/verify/options.go` exists.
- [x] `pkg/mimirtool/backfill/verify/report.go` exists.
- [x] `pkg/mimirtool/backfill/verify/verify_test.go` exists.
- [x] Commit `53bed28ab7` exists (Task 1: feat).
- [x] Commit `2bb61e8fb2` exists (Task 2: test).
- [x] `go test ./pkg/mimirtool/backfill/verify/... -count=1` exits 0.
- [x] `go vet ./pkg/mimirtool/backfill/verify/...` exits 0.
- [x] `goimports -l pkg/mimirtool/backfill/verify/` returns empty.
- [x] `make format` produces no diff in verify package files.

## Self-Check: PASSED

---
*Phase: 01-backfill-pre-verification*
*Completed: 2026-04-22*
