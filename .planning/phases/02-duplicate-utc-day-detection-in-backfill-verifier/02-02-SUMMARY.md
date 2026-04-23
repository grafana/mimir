---
phase: 02-duplicate-utc-day-detection-in-backfill-verifier
plan: 02
subsystem: mimirtool
tags: [mimirtool, backfill, verification, cli, changelog]

requires:
  - phase: 01-backfill-pre-verification
    provides: verify.NewVerifier functional-options assembly, httptest helpers in client/backfill_test.go
  - phase: 02-duplicate-utc-day-detection-in-backfill-verifier/01
    provides: verify.NewDuplicateDayVerifier constructor
provides:
  - CLI wiring for DuplicateDayVerifier (always on, no flag)
  - End-to-end zero-upload proof test for duplicate-day rejection
  - CHANGELOG entry for CHECK-04 (#15133)
affects: []

tech-stack:
  added: []
  patterns:
    - "BatchVerifier wiring style: append a single verify.WithBatchCheck(...) line after the block-check options"

key-files:
  created: []
  modified:
    - pkg/mimirtool/commands/backfill.go
    - pkg/mimirtool/client/backfill_test.go
    - CHANGELOG.md

key-decisions:
  - "No new CLI flag. The check is always on, matching Phase 1's no-escape-hatch principle."
  - "Test uses two well-formed, single-day blocks on UTC day 0 so fail-fast never trips before the batch stage — exercises the real default CLI path."
  - "Do not alter existing Phase 1 CHANGELOG entry (#15115). Phase 2 is a follow-up PR."
  - "In default fail-fast mode, per-block failures short-circuit before the batch stage, so duplicate-day findings surface alongside per-block failures only under -full-report. Noted in the CHANGELOG entry itself."

patterns-established: []

requirements-completed: [CHECK-04]

duration: 6min
completed: 2026-04-23
---

# Phase 2 Plan 02: CLI wiring + end-to-end test + CHANGELOG Summary

**Duplicate-day rejection is now live on the shipping mimirtool backfill CLI, with an httptest-backed zero-upload proof and a CHANGELOG entry pinned to PR #15133.**

## Performance

- **Duration:** ~6 min
- **Completed:** 2026-04-23
- **Tasks:** 3
- **Files modified:** 3 (no new files)

## Accomplishments

- `pkg/mimirtool/commands/backfill.go`: one-line addition inside the `verify.NewVerifier(...)` functional-options call. `git diff` is +1/-0.
- `pkg/mimirtool/client/backfill_test.go`: new `TestBackfillWithOptions_DuplicateDayRejected` exercises the real production path — two well-formed, single-day blocks on UTC day 0, full default verifier assembly with fail-fast, assertion on `verification failed` error, assertion of 0 upload requests to the httptest server.
- `CHANGELOG.md`: new `[FEATURE]` entry at line 40 (above the Phase 1 entry at line 41, preserving mimirtool entry grouping). Documents the `-full-report` interaction explicitly so users know how to surface duplicate-day findings alongside per-block failures.
- `make format` produces no further changes.
- `golangci-lint run ./pkg/mimirtool/backfill/verify/... ./pkg/mimirtool/client/... ./pkg/mimirtool/commands/...` reports 0 issues.
- `go test ./pkg/mimirtool/... -count=1` passes across the entire mimirtool tree.

## Task Commits

1. **Task 1 — CLI wiring:** `5949b3f981` (feat, +1/-0)
2. **Task 2 — E2E zero-upload test:** `4e66e81218` (test, +36 lines)
3. **Task 3 — CHANGELOG entry:** `d976fb3185` (docs, +1 line)

## Files Modified

- `pkg/mimirtool/commands/backfill.go` — adds `verify.WithBatchCheck(verify.NewDuplicateDayVerifier(logger))`.
- `pkg/mimirtool/client/backfill_test.go` — adds `TestBackfillWithOptions_DuplicateDayRejected`.
- `CHANGELOG.md` — adds the `[FEATURE]` line for #15133.

## Decisions Made

- **`-full-report` interaction documented in the CHANGELOG.** Users who want to see duplicate-day findings alongside a bad-block failure need `-full-report`; otherwise fail-fast short-circuits. Documenting this preempts support questions.
- **Test uses `verify.Medium` mode** to skip the CRC walk (tested in Phase 1) and keep this test fast; the batch-stage behaviour is the only thing under examination.

## Deviations from Plan

None. Plan executed exactly as specified:

- CLI diff is +1/-0 as the plan demanded.
- Test imports: no new imports needed — `verify`, `log`, `assert`, `require`, `chunks`, `time`, `context`, `testing` were already in the file.
- PR number pulled live from `./tools/github-next-pr-number.sh` — landed `#15133`.

## Issues Encountered

None.

## Next Phase Readiness

Phase 2 complete. CHECK-04 can be marked done in REQUIREMENTS.md traceability. No further phases queued.

---
*Phase: 02-duplicate-utc-day-detection-in-backfill-verifier*
*Completed: 2026-04-23*
