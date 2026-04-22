# Roadmap: Mimirtool Backfill Pre-Verification

## Overview

Single-phase scope: add a pluggable client-side verification module to `mimirtool backfill` that validates blocks are well-formed and exactly-one-UTC-day-aligned 24-hour blocks before any upload happens. Framework is designed to accept additional checks (duplicate-day, overlap, etc.) in follow-up work without structural change.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

- [ ] **Phase 1: Backfill Pre-Verification** - Pluggable client-side verifier module plus well-formed + 24hr + UTC-alignment checks wired into the backfill upload path.

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
- [ ] 01-04-PLAN.md — MimirClient.BackfillWithOptions integration + httptest-based zero-upload assertions (verify-fail, dry-run, valid-batch, legacy-wrapper)
- [ ] 01-05-PLAN.md — CLI flag wiring (--dry-run, --skip-chunk-verification, --full-report, --verify-concurrency), cmd.Validate mutual-exclusion, CHANGELOG, make format/lint/test/reference-help, human-verify checkpoint

## Progress

**Execution Order:**
Phases execute in numeric order: 1

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Backfill Pre-Verification | 3/5 | In progress | - |
