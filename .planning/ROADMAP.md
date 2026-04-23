# Roadmap: Mimirtool Backfill Pre-Verification

## Overview

Client-side verification module for `mimirtool backfill` that rejects malformed or non-single-UTC-day blocks before upload. Phase 1 shipped the framework plus well-formed and single-UTC-day (header) checks. Phase 2 adds the batch-level duplicate-UTC-day check, landing on the `BatchVerifier` seam Phase 1 was designed to support.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

- [x] **Phase 1: Backfill Pre-Verification** - Pluggable client-side verifier module plus well-formed + 24hr + UTC-alignment checks wired into the backfill upload path.
- [x] **Phase 2: Duplicate UTC-day detection** - BatchVerifier that rejects batches containing two or more blocks covering the same UTC day.

## Phase Details

### Phase 1: Backfill Pre-Verification
**Goal**: Mimirtool's backfill command rejects malformed blocks and non-24hr-UTC-aligned blocks before attempting any upload, via a pluggable verification module that can accept future checks.
**Depends on**: Nothing (first phase)
**Requirements**: FRAMEWORK-01, FRAMEWORK-02, FRAMEWORK-03, FRAMEWORK-04, CHECK-01, CHECK-02, CHECK-03, INT-01, INT-02
**Success Criteria** (what must be TRUE):
  1. Running `mimirtool backfill` against a directory containing a 2-hour Prometheus block exits non-zero with a clear message naming the block and the failing check, and uploads nothing.
  2. Running `mimirtool backfill` against a directory containing a valid 24-hour UTC-aligned block uploads successfully (same observable behavior as today).
  3. Running `mimirtool backfill` against a directory containing a corrupted block (missing chunk file, truncated index, or invalid `meta.json`) exits non-zero with a well-formed-check failure and uploads nothing.
  4. The verifier interface accepts new per-block and batch-level check implementations without modifying existing check code (verified by adding a dummy no-op check in tests).
  5. New/changed code passes `make format`, `make lint`, `make test`, and has unit-test coverage for each check.
**Plans**: 5 plans

Plans:
- [x] 01-01-PLAN.md — Create verify/ package skeleton (BlockVerifier, BatchVerifier, Verifier, Report, options) + framework seam tests
- [x] 01-02-PLAN.md — WellFormedVerifier wrapping block.VerifyBlock + unit tests (valid / truncated / mangled / checksum-mismatch)
- [x] 01-03-PLAN.md — SingleUTCDayVerifier (header math + deep OutsideChunks via VerifyBlock) + table-driven tests
- [x] 01-04-PLAN.md — MimirClient.BackfillWithOptions integration + httptest-based zero-upload assertions (verify-fail, dry-run, valid-batch, legacy-wrapper)
- [x] 01-05-PLAN.md — CLI flag wiring (--dry-run, --skip-chunk-verification, --full-report, --verify-concurrency), cmd.Validate mutual-exclusion, CHANGELOG, make format/lint/test/reference-help, human-verify checkpoint

### Phase 2: Duplicate UTC-day detection
**Goal**: Mimirtool's backfill rejects a batch when two or more blocks in the batch cover the same UTC day, preventing accidental duplicate uploads that would create compaction conflicts.
**Depends on**: Phase 1 (uses the `BatchVerifier` seam already shipped)
**Requirements**: CHECK-04
**Success Criteria** (what must be TRUE):
  1. Running `mimirtool backfill` against a batch containing two blocks whose `[MinTime, MaxTime)` ranges land on the same UTC day exits non-zero with a clear error naming the two offending block ULIDs and the shared date.
  2. Running `mimirtool backfill` against a batch where every block covers a distinct UTC day verifies and uploads successfully (no regression on the existing happy path).
  3. The new check is implemented as a `verify.BatchVerifier` registered via `WithBatchCheck` — no changes to `Verifier.Run` or the `BlockVerifier` interface.
  4. Unit tests cover: distinct days pass; two blocks same day fail; N>2 blocks same day fail and all colliding ULIDs are named; a single-block batch trivially passes.
  5. Changed code passes `make format`, `make lint`, `make test`.
**Plans**: 2 plans

Plans:
- [x] 02-01-PLAN.md — Implement DuplicateDayVerifier (BatchVerifier) + unit tests covering empty / single / distinct / collisions / multi-group / determinism
- [x] 02-02-PLAN.md — Wire into CLI (commands/backfill.go), end-to-end zero-upload test, CHANGELOG entry, make format/lint/test gate

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Backfill Pre-Verification | 5/5 | Complete | 2026-04-23 |
| 2. Duplicate UTC-day detection | 2/2 | Complete | 2026-04-23 |
