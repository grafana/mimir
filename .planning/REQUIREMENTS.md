# Requirements: Mimirtool Backfill Pre-Verification

**Defined:** 2026-04-22
**Core Value:** Mimirtool users don't upload blocks that will be rejected by the server or that create avoidable load on downstream compactors.

## v1 Requirements

### Framework

- [x] **FRAMEWORK-01**: A verification module exists in `pkg/mimirtool/backfill/` (package location TBD in spec phase) with a pluggable interface so new checks can be added without modifying existing ones.
- [x] **FRAMEWORK-02**: The interface supports both per-block checks and batch-level checks (multiple blocks in one invocation) so future duplicate-day and overlap checks fit cleanly.
- [x] **FRAMEWORK-03**: Verification runs before the upload loop in `MimirClient.Backfill()` at `pkg/mimirtool/client/backfill.go:25-53`.
- [x] **FRAMEWORK-04**: Failure reporting identifies which block failed which check and why, aggregated across all blocks so users see every issue in one run.

### Checks (v1)

- [x] **CHECK-01**: Well-formed block — `meta.json` valid, `index` file readable, chunk segments present on disk and internally consistent. Reuse Prometheus `tsdb.OpenBlock` or Thanos `block.VerifyIndex` rather than reimplementing.
- [x] **CHECK-02**: 24-hour block duration — `MaxTime - MinTime == 24h` exactly, measured from block meta. (Per SPEC §6 this requirement was superseded by single-UTC-day semantics: a block may have any `MaxTime - MinTime <= 24h` so long as `[MinTime, MaxTime)` lies inside one calendar UTC day. Implemented by `SingleUTCDayVerifier` in Plan 01-03.)
- [x] **CHECK-03**: UTC-day alignment — `MinTime` is a whole-hour UTC-midnight boundary (`MinTime % (24h in ms) == 0`). Paired with CHECK-02 so the block covers exactly one calendar UTC day. (Per SPEC §6, enforcement is via the half-open day-floor equality `floor(MinTime/msPerDay) == floor((MaxTime-1)/msPerDay)` — covers the "exactly one calendar UTC day" intent without requiring strict start-of-day alignment. Implemented by `SingleUTCDayVerifier` in Plan 01-03.)

### Integration

- [x] **INT-01**: When verification fails, `mimirtool backfill` does not attempt any uploads and exits with a non-zero code and a clear error summary.
- [x] **INT-02**: Verification output follows existing mimirtool logging conventions (go-kit/log at the level used elsewhere in `client/backfill.go`).

### Batch Checks (Phase 2)

- [ ] **CHECK-04**: Batch-level duplicate-day detection — reject a batch if two or more blocks cover the same UTC day. Implemented as a `verify.BatchVerifier` registered via `WithBatchCheck`.

## v2 Requirements

### Future Checks

- **CHECK-05**: Batch-level overlap detection — reject if any two blocks have overlapping time ranges.
- **CHECK-06**: Tenant-scoped cardinality / series-limit pre-flight — requires server-side metadata; out of scope for v1.

## Out of Scope

| Feature | Reason |
|---------|--------|
| Server-side validation changes | Server already validates on upload; this project is a client-side fail-fast layer. |
| Block repair / rewriting | Verification only — no mutation of user data. |
| Replacing the block *creation* path (`pkg/mimirtool/backfill/backfill.go`) | This project targets the upload path only. |
| Removing `GetBlockMeta`'s existing checks | Those checks stay; verification module augments, doesn't replace. |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| FRAMEWORK-01 | Phase 1 | Complete |
| FRAMEWORK-02 | Phase 1 | Complete |
| FRAMEWORK-03 | Phase 1 | Complete |
| FRAMEWORK-04 | Phase 1 | Complete |
| CHECK-01 | Phase 1 | Complete |
| CHECK-02 | Phase 1 | Complete |
| CHECK-03 | Phase 1 | Complete |
| INT-01 | Phase 1 | Complete |
| INT-02 | Phase 1 | Complete |
| CHECK-04 | Phase 2 | Pending |

**Coverage:**
- Requirements: 10 total (9 v1 delivered + 1 Phase 2)
- Mapped to phases: 10
- Unmapped: 0 ✓

---
*Requirements defined: 2026-04-22*
*Last updated: 2026-04-23 — Phase 1 complete; Phase 2 (CHECK-04 duplicate-day detection) added to milestone.*
