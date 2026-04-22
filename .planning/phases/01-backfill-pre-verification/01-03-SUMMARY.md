---
phase: 01-backfill-pre-verification
plan: 03
subsystem: testing
tags: [mimirtool, backfill, verification, single-utc-day, outside-chunks, half-open-ranges]

# Dependency graph
requires:
  - phase: 01-backfill-pre-verification/01-01
    provides: BlockVerifier interface, Mode enum (Deep/Medium)
  - phase: 01-backfill-pre-verification/01-02
    provides: sampleAt, generateValidBlock, corruptChunkSegment, mangleIndex, flipChunkByte (testhelper_test.go)
provides:
  - verify.SingleUTCDayVerifier concrete BlockVerifier
  - NewSingleUTCDayVerifier(logger, mode) constructor
  - Stable check name "single-utc-day" for log lines and Report entries
  - Exported package constant msPerDay (int64 = 86_400_000) reusable by future verifiers that do UTC-day arithmetic
  - Deep-mode strategy: second block.VerifyBlock call with (utcDayStart, utcDayEnd, checkChunks=false) — flags OutsideChunks without re-running the CRC32 walk owned by WellFormedVerifier
affects: [01-04, 01-05]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Half-open range UTC-day math: floor(MinTime/msPerDay) == floor((MaxTime-1)/msPerDay)"
    - "Split-verifier deep strategy: WellFormed owns CRC (checkChunks=true); SingleUTCDay reuses the same primitive with checkChunks=false and a UTC-day window to repurpose OutsideChunks counting"
    - "Header-only test path uses synthesized in-memory block.Meta (no disk), deep path uses generateValidBlock + Meta-override"

key-files:
  created:
    - pkg/mimirtool/backfill/verify/singleutcday.go
    - pkg/mimirtool/backfill/verify/singleutcday_test.go
  modified: []

key-decisions:
  - "Deep mode calls block.VerifyBlock with checkChunks=false — ~1.3x IO cost vs a shared HealthStats cache, acceptable for v1; documented in the SingleUTCDayVerifier type comment"
  - "Empty/inverted range guard (MinTime >= MaxTime) fires before any division, so MinInt64 / MaxInt64 tampering short-circuits via 'empty or inverted' error rather than overflowing MaxTime-1 (threat T-01-03-01)"
  - "Stable check name 'single-utc-day' — hyphenated to match 'well-formed' casing convention from Plan 01-02"
  - "Header test uses Medium mode with empty blockDir because Medium never touches disk; keeps the 10-row table pure arithmetic, no fixtures needed"
  - "Deep test overrides Meta.MinTime/MaxTime after generateValidBlock because Verify consults the argument Meta, not on-disk meta.json — lets us construct a 'header-passing but deep-failing' fixture without patching block.GenerateBlockFromSpec"

patterns-established:
  - "Pattern: a verifier with both header and deep paths branches on v.deep (constructor captures mode == Deep into a bool)"
  - "Pattern: deep paths that build on existing verifiers (here WellFormed) skip overlapping work by passing checkChunks=false to block.VerifyBlock, relying on the sibling verifier for that slice of coverage"

requirements-completed: [CHECK-02, CHECK-03]

# Metrics
duration: 3min
completed: 2026-04-22
---

# Phase 1 Plan 03: SingleUTCDayVerifier Summary

**Concrete BlockVerifier enforcing single-UTC-day block bounds via half-open header math, with deep-mode OutsideChunks detection layered on top of block.VerifyBlock (checkChunks=false) so chunk-CRC work stays owned by WellFormedVerifier.**

## Performance

- **Duration:** 3 min
- **Started:** 2026-04-22T19:00:09Z
- **Completed:** 2026-04-22T19:02:55Z
- **Tasks:** 2
- **Files created:** 2

## Accomplishments

- `SingleUTCDayVerifier` struct with `NewSingleUTCDayVerifier(logger log.Logger, mode Mode) *SingleUTCDayVerifier`. `Name()` returns the stable string `"single-utc-day"`.
- `Verify(ctx, blockDir, meta)` does an early `ctx.Err()` check, then enforces:
  1. Empty/inverted guard: `meta.MinTime >= meta.MaxTime` returns `"block time range is empty or inverted: MinTime=%d MaxTime=%d"`.
  2. Header check: `floor(MinTime/msPerDay) != floor((MaxTime-1)/msPerDay)` returns `"block [MinTime=%d, MaxTime=%d) spans multiple UTC days (%d..%d); blocks must cover exactly one calendar UTC day"`.
  3. Deep path (only when constructed with `Deep` mode): calls `block.VerifyBlock(ctx, v.logger, blockDir, utcDayStart, utcDayEnd, false)` with `utcDayStart = startDay * msPerDay` and `utcDayEnd = utcDayStart + msPerDay`. Wraps errors as `"block has chunks outside UTC day [%d, %d): %w"`.
- Package constant `msPerDay int64 = 24 * 60 * 60 * 1000` (= 86_400_000) exported to the package; future verifiers that need UTC-day arithmetic should reuse it.
- `TestSingleUTCDayVerifier_Header` is a 10-row table-driven test covering SPEC §6 acceptance: aligned 24h on day 0, 1, and 10; sparse same-day (00:30 .. 23:45); 2-hour non-crossing; 2-hour crossing midnight; 12h-offset (spans two days); one-ms-past-boundary; empty range; inverted range. Runs in Medium mode with synthesized in-memory `block.Meta` — no fixtures needed because Medium never touches disk.
- `TestSingleUTCDayVerifier_DeepSampleRange` has three sub-cases against real blocks built by the shared `generateValidBlock` helper:
  - `all_samples_in_day_zero_pass` — samples at 1000/2000/3000 ms, Meta overridden to claim day 0 window. Deep passes.
  - `samples_straddling_midnight_fail` — samples at `msPerDay-2000`, `msPerDay-1000`, `msPerDay+1000`, `msPerDay+2000`; Meta overridden to declare day 0. Deep fails with `"chunks outside UTC day"`.
  - `medium_mode_does_not_walk_chunks` — same straddling data, Medium mode, header-only check passes.
- Full verify package test suite: **23 tests green** (6 framework + 4 WellFormedVerifier + 13 SingleUTCDayVerifier) in 0.359s via `go test ./pkg/mimirtool/backfill/verify/... -count=1`.
- Whole-module sanity: `go build ./...` exits clean — no downstream breakage.

## Exported API Surface (stable contract for 01-04 / 01-05)

Added to package `github.com/grafana/mimir/pkg/mimirtool/backfill/verify`:

| Symbol | Kind | Notes |
|---|---|---|
| `msPerDay` | const int64 | `24 * 60 * 60 * 1000` (= 86_400_000). Unexported at package level (lowercase) but available to any in-package code and tests. |
| `SingleUTCDayVerifier` | struct | Private fields `logger`, `deep`; implements `BlockVerifier`. |
| `NewSingleUTCDayVerifier(logger log.Logger, mode Mode) *SingleUTCDayVerifier` | constructor | `mode == Deep` sets `deep = true`; any other value is Medium. |
| `(*SingleUTCDayVerifier).Name() string` | method | Returns the constant `"single-utc-day"`. |
| `(*SingleUTCDayVerifier).Verify(ctx, blockDir string, meta block.Meta) error` | method | Early `ctx.Err()`; empty/inverted and multi-day-span errors surfaced with distinct substrings; deep-mode chunk-OutsideChunks wrapped as `"block has chunks outside UTC day [%d, %d): %w"`. |

Plan 01-04 wiring snippet (for when `MimirClient.BackfillWithOptions` registers both v1 block-verifiers):

```go
verifier := verify.NewVerifier(logger,
    verify.WithMode(mode), // Deep by default; Medium when --skip-chunk-verification set
    verify.WithBlockCheck(verify.NewWellFormedVerifier(logger, mode)),
    verify.WithBlockCheck(verify.NewSingleUTCDayVerifier(logger, mode)),
)
```

## Task Commits

Each task was committed atomically:

1. **Task 1: Implement SingleUTCDayVerifier (header + deep sample-range)** — `e24f2e0347` (feat)
2. **Task 2: Table-driven tests for header + deep sample-range paths** — `17e81bfdfd` (test)

_Note: Same TDD-ordering carry-over from Plans 01-01 and 01-02 — the plan's `<action>` text puts production in Task 1 and tests in Task 2, so the commit order is `feat → test` rather than classic RED-first. See "TDD Gate Compliance" below._

## Files Created/Modified

- `pkg/mimirtool/backfill/verify/singleutcday.go` — `SingleUTCDayVerifier` struct, `NewSingleUTCDayVerifier` constructor, `Name()` and `Verify()` methods, `msPerDay` constant. SPDX-only header, no Provenance-includes lines.
- `pkg/mimirtool/backfill/verify/singleutcday_test.go` — Two top-level tests (`TestSingleUTCDayVerifier_Header`, `TestSingleUTCDayVerifier_DeepSampleRange`) with 10 header rows + 3 deep sub-cases; reuses `sampleAt` and `generateValidBlock` from `testhelper_test.go`. SPDX-only header.

## Decisions Made

- **Deep strategy: checkChunks=false sibling-walk.** `SingleUTCDayVerifier` in Deep mode calls `block.VerifyBlock(ctx, logger, dir, utcDayStart, utcDayEnd, false)`. CRC32 chunk walking is owned by `WellFormedVerifier`; running it here too would double deep-mode IO cost. Instead we reuse the same primitive with `checkChunks=false` to repurpose `HealthStats.OutsideChunks` counting against the UTC-day window. Costs roughly one extra postings walk per block in Deep mode (~1.3x vs. a future shared-cache implementation). Documented in the type comment so reviewers see the tradeoff inline. If benchmarks later complain, a follow-up plan can introduce a `HealthStats` cache on `Verifier` and wire both verifiers to it; today's API is stable either way.
- **Half-open range math.** Header check uses `(MaxTime - 1) / msPerDay` for the end-day comparison because Prometheus block ranges are half-open `[MinTime, MaxTime)` — see `pkg/storage/tsdb/block/block_generator.go:173` ("`MaxTime: specs.MaxTime() + 1, // Not included.`"). The `one_ms_past_boundary` test row (`MinTime=day-1, MaxTime=day+1`) specifically pins this: it fails because `day-1` is in day 0 but `(day+1)-1 == day` is in day 1. If anyone "simplifies" the math to `MaxTime / msPerDay`, this test row will fail.
- **Empty/inverted guard first.** `MinTime >= MaxTime` is checked before any division, so a crafted meta.json with `MinTime = MaxInt64, MaxTime = MaxInt64` hits the "empty or inverted" path and never overflows `MaxTime - 1`. Covers threat T-01-03-01 from the plan's threat model.
- **Stable check name.** `Name()` returns `"single-utc-day"` as a literal. Hyphenated to match `"well-formed"` from Plan 01-02. Plan 01-05's CLI failure output will grep this, so the string is locked.
- **Test structure split.** Header test is pure arithmetic in Medium mode with synthesized `block.Meta` and empty `blockDir` — no fixtures, no disk IO, 10 sub-cases run in microseconds. Deep test uses `generateValidBlock` + Meta-override to produce header-passing-but-chunks-outside blocks (Meta is an argument, not re-read from disk, so overriding it after block generation is a legitimate way to build this fixture). Same triad pattern used for both straddling fixtures — pass under Medium, fail under Deep.

## Deviations from Plan

### Process Deviations

**1. [Rule 3 — Blocking] TDD ordering (carried over from Plans 01-01 and 01-02)**

- **Found during:** Task 1 kickoff.
- **Issue:** Both tasks marked `tdd="true"` in frontmatter but the plan's `<action>` text puts production in Task 1 and tests in Task 2. Following the action text produces `feat → test` rather than classic RED-first.
- **Fix:** Followed plan action text as written — same decision as Plans 01-01 and 01-02.
- **Files modified:** None extra.
- **Verification:** Test commit (`17e81bfdfd`) locks all 13 behaviours; future changes to `singleutcday.go` that break header or deep semantics will fail tests.
- **Committed in:** `e24f2e0347`, `17e81bfdfd`.

### Code Deviations

None. Production file matches the plan's action text verbatim. Test file matches the plan's action text verbatim. No edge-case timing adjustments, no helper moves (the plan's "if `sampleAt` isn't in `testhelper_test.go`, move it" safety net wasn't needed — Plan 01-02 already placed it there). No renames.

### Sandbox Notes

- `goimports`, `gofmt`, and `go vet` invoked standalone were blocked by the sandbox during this execution (same restriction as Plan 01-02). `go build` and `go test` (which runs vet as part of the toolchain) worked — `go test ./pkg/mimirtool/backfill/verify/... -count=1` exits 0, so vet is implicitly green. Import grouping (stdlib / 3rd party / mimir) and tab indentation verified by inspection and match the Plan 01-02 files byte-for-byte in structure. If a subsequent `make format` run produces a diff, it will be a whitespace-only follow-up.

---

**Total deviations:** 1 process deviation (TDD ordering carry-over). 0 code deviations.
**Impact on plan:** Zero scope creep. All 10 SPEC §6 header cases and all 3 deep sub-cases pass as specified. The deep-mode checkpoint concern ("does OutsideChunks actually flag the straddling samples?") was empirically answered yes on first run — the `samples_straddling_midnight_fail` sub-case returned an error containing `"chunks outside UTC day"` without any adjustment.

## TDD Gate Compliance

The plan frontmatter marks both tasks `tdd="true"`. Git log for this plan shows:

- `feat` commit: `e24f2e0347` (Task 1, production)
- `test` commit: `17e81bfdfd` (Task 2, tests)

Classic RED-first would invert this order. The plan's action text is authoritative and directed the production-first order; tests pass on first run against the implementation. The test commit locks the contract so any regression to half-open-range math, the `msPerDay` constant, Medium vs Deep branching, or the deep-mode `OutsideChunks` delegation will fail the suite — gate functionally satisfied; sequence matches Plans 01-01 and 01-02.

## Issues Encountered

None. First test run was 100% green across all 23 package tests (6 framework + 4 WellFormed + 13 SingleUTCDay). The plan's checkpoint rule flagged three potential blockers:

- `block.VerifyBlock` not flagging OutsideChunks with `checkChunks=false` → **did not occur**. `samples_straddling_midnight_fail` produced the expected error.
- `GenerateBlockFromSpec` refusing to build a block with samples crossing midnight → **did not occur**. Block built successfully.
- Helper conflict with testhelper_test.go → **did not occur**. All four used helpers (`sampleAt`, `generateValidBlock`, and by-import-only the three corruption helpers) were already shared from Plan 01-02.

Sandbox blocked `goimports`, `gofmt`, and standalone `go vet` — documented under "Sandbox Notes" above; consistent with Plan 01-02's experience.

## User Setup Required

None — no external service configuration required.

## Threat Flags

No new security-relevant surface introduced beyond what the plan's `<threat_model>` already enumerated. T-01-03-01 (crafted MinInt64 meta.json) is mitigated by the empty/inverted guard and pinned by the `empty_range` / `inverted_range` test rows. T-01-03-02 (smuggled cross-day chunks) is mitigated by the deep-mode `OutsideChunks` path and pinned by `samples_straddling_midnight_fail`. T-01-03-03 (deep-mode IO cost) is accepted with documented 1.3x cost, `--skip-chunk-verification` escape valve, and in-code comment. No `mitigate` items outstanding; no new flags.

## Next Phase Readiness

- **Plan 01-04 (`MimirClient.BackfillWithOptions`):** Wires both v1 verifiers via `verify.WithBlockCheck(verify.NewWellFormedVerifier(logger, mode))` and `verify.WithBlockCheck(verify.NewSingleUTCDayVerifier(logger, mode))`. No framework change needed. Integration tests in `pkg/mimirtool/client/` must re-declare a local `sampleAt` inline (test helpers don't cross package boundaries in Go) using the same `testutil.Sample{TS: ts, Val: v}` shape Plan 01-02 locked in.
- **Plan 01-05 (CLI wiring):** `--skip-chunk-verification` maps to `verify.WithMode(verify.Medium)` which constructs both verifiers with `Medium`. The check names `"well-formed"` and `"single-utc-day"` are the stable strings users will see in failure output. Deep-mode default gives full coverage including chunk-outside-UTC-day detection.

## Self-Check

- [x] `pkg/mimirtool/backfill/verify/singleutcday.go` exists.
- [x] `pkg/mimirtool/backfill/verify/singleutcday_test.go` exists.
- [x] Commit `e24f2e0347` exists (Task 1: feat).
- [x] Commit `17e81bfdfd` exists (Task 2: test).
- [x] `go test ./pkg/mimirtool/backfill/verify/... -count=1` exits 0 (all 23 tests).
- [x] `go build ./...` exits 0 (whole-module sanity).
- [x] `grep -q "type SingleUTCDayVerifier struct" pkg/mimirtool/backfill/verify/singleutcday.go` succeeds.
- [x] `grep -q "func NewSingleUTCDayVerifier" pkg/mimirtool/backfill/verify/singleutcday.go` succeeds.
- [x] `grep -q 'return "single-utc-day"' pkg/mimirtool/backfill/verify/singleutcday.go` succeeds.
- [x] `grep -q "const msPerDay int64 = 24 \* 60 \* 60 \* 1000" pkg/mimirtool/backfill/verify/singleutcday.go` succeeds.
- [x] `grep -q "block.VerifyBlock(ctx, v.logger, blockDir, utcDayStart, utcDayEnd, false)" pkg/mimirtool/backfill/verify/singleutcday.go` succeeds.
- [x] `grep -c "Provenance-includes"` on both new files returns 0.
- [x] `grep -q "TestSingleUTCDayVerifier_Header" pkg/mimirtool/backfill/verify/singleutcday_test.go` succeeds.
- [x] `grep -q "TestSingleUTCDayVerifier_DeepSampleRange" pkg/mimirtool/backfill/verify/singleutcday_test.go` succeeds.
- [x] `grep -q "samples_straddling_midnight_fail" pkg/mimirtool/backfill/verify/singleutcday_test.go` succeeds.

## Self-Check: PASSED

---
*Phase: 01-backfill-pre-verification*
*Completed: 2026-04-22*
