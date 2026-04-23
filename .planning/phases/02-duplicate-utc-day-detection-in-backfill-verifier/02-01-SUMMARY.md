---
phase: 02-duplicate-utc-day-detection-in-backfill-verifier
plan: 01
subsystem: mimirtool
tags: [mimirtool, backfill, verification, batch-verifier, duplicate-day]

requires:
  - phase: 01-backfill-pre-verification
    provides: BatchVerifier interface, BlockRef struct, msPerDay constant, Verifier.Run batch stage
provides:
  - DuplicateDayVerifier (first production BatchVerifier)
  - Deterministic, lex-sorted error reporting for day-collision groups
affects: [02-02-PLAN]

tech-stack:
  added: []
  patterns:
    - "BatchVerifier pattern: pure map-based bucketing over meta.MinTime with deterministic sort"

key-files:
  created:
    - pkg/mimirtool/backfill/verify/duplicateday.go
    - pkg/mimirtool/backfill/verify/duplicateday_test.go
  modified: []

key-decisions:
  - "Reuse msPerDay from singleutcday.go (no re-declaration in duplicateday.go)."
  - "Use slices.Sort (Go 1.21+) instead of sort.Slice/sort.Strings to pre-empt slicessort linter."
  - "Single formatted error enumerating every colliding day, days sorted ascending, ULIDs lex-sorted within each day. One error, not many."
  - "Document but do not defend against blocks that skip SingleUTCDayVerifier: a block spanning two days is keyed only on its MinTime day. Splitting it would mask real same-day collisions."

patterns-established:
  - "BatchVerifier implementation style: struct with logger field, constructor, Name() returning a kebab-case string, Verify(ctx, []BlockRef) error with deterministic sort + single aggregate error."

requirements-completed: [CHECK-04]

duration: 5min
completed: 2026-04-23
---

# Phase 2 Plan 01: DuplicateDayVerifier Summary

**First production BatchVerifier — rejects a batch with two or more blocks on the same UTC day, with deterministic, lex-sorted error output.**

## Performance

- **Duration:** ~5 min
- **Completed:** 2026-04-23
- **Tasks:** 2
- **Files created:** 2

## Accomplishments

- `DuplicateDayVerifier` lands on the `BatchVerifier` seam Phase 1 shipped but had no production consumers for.
- Day key = `meta.MinTime / msPerDay` (reused constant — no redeclaration).
- Error output is deterministic regardless of input order: colliding days ascending, ULIDs lex-sorted within each day.
- Single and empty batches trivially return nil; no edge cases.
- Seven table-driven subtests: empty, single, two distinct, two colliding, three colliding (lex ULID order), multi-group (day ascending + loner not reported), and determinism under input shuffling.

## Task Commits

1. **Task 1 — DuplicateDayVerifier impl:** `392744ccd9` (feat)
2. **Task 2 — Table-driven unit tests:** `5ea5ef73b8` (test)

## Files Created

- `pkg/mimirtool/backfill/verify/duplicateday.go` — verifier implementation
- `pkg/mimirtool/backfill/verify/duplicateday_test.go` — 7 table-driven subtests

## Decisions Made

- **slices.Sort over sort.Slice/sort.Strings** — the LSP `slicessort` lint suggestion fired immediately; switched to pre-empt a later golangci-lint failure. Behavior identical.
- **Logger field retained on the struct** even though `Verify` does not log (matches the Phase 1 pattern on `WellFormedVerifier` and `SingleUTCDayVerifier`; the orchestrator logs batch-check errors in `Verifier.Run`).

## Deviations from Plan

- Plan body prescribed `sort.Slice(...)` and `sort.Strings(...)`. Switched to `slices.Sort(...)` after the LSP `slicessort` diagnostic surfaced on the first build. Behavior-preserving; no functional divergence.

## Issues Encountered

- None beyond the lint swap noted above.

## Next Phase Readiness

Plan 02-02 can consume `verify.NewDuplicateDayVerifier(logger)` with a single line inside the existing functional-options call in `pkg/mimirtool/commands/backfill.go`.

---
*Phase: 02-duplicate-utc-day-detection-in-backfill-verifier*
*Completed: 2026-04-23*
