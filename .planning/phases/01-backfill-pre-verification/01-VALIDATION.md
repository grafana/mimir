---
phase: 1
slug: backfill-pre-verification
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-04-22
---

# Phase 1 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Go stdlib `testing` + `github.com/stretchr/testify/require` and `assert` (Mimir standard) |
| **Config file** | `go.mod` (no framework install needed) |
| **Quick run command** | `go test ./pkg/mimirtool/backfill/verify/... -count=1` |
| **Full suite command** | `go test ./pkg/mimirtool/... -count=1` |
| **Estimated runtime** | Quick: ~15s; Full mimirtool: ~60s |

---

## Sampling Rate

- **After every task commit:** Run `go test ./pkg/mimirtool/backfill/verify/... -count=1`
- **After every plan wave:** Run `go test ./pkg/mimirtool/... -count=1`
- **Before `/gsd-verify-work`:** Full suite plus `make lint` must be green
- **Max feedback latency:** 60 seconds

---

## Per-Task Verification Map

The concrete task IDs are filled in after planning. This file will be refined by the planner; for now, it documents the *strategy* mapped to each of the 9 REQ-IDs.

| REQ-ID | Category | Test Type | Automated Command | Fixture Strategy |
|--------|----------|-----------|-------------------|------------------|
| FRAMEWORK-01 | Framework structure | unit | `go test ./pkg/mimirtool/backfill/verify/ -run TestNewVerifier` | Table-driven, constructor assertions |
| FRAMEWORK-02 | BlockVerifier + BatchVerifier interfaces | unit | `go test ./pkg/mimirtool/backfill/verify/ -run TestVerifierInterfaces` | Mock impls of both interfaces, confirm both are invoked |
| FRAMEWORK-03 | Integration into `MimirClient.Backfill()` | integration | `go test ./pkg/mimirtool/client/ -run TestBackfillVerifiesBeforeUpload` | `httptest.Server` intercepts `/api/v1/upload/block/...`; test asserts zero requests on failure |
| FRAMEWORK-04 | Failure aggregation + log reporting | unit + integration | `go test ./pkg/mimirtool/backfill/verify/ -run TestReport` and client integration | `VerificationReport` struct assertions; log capture via go-kit testutil or buffer |
| CHECK-01 | Well-formed verifier | unit + integration | `go test ./pkg/mimirtool/backfill/verify/ -run TestWellFormedVerifier` | `block.GenerateBlockFromSpec` for valid; `os.Truncate` on chunk file for corrupted |
| CHECK-02 | Single-UTC-day semantics (header path) | unit, table-driven | `go test ./pkg/mimirtool/backfill/verify/ -run TestSingleUTCDayVerifier` | `block.Meta` structs synthesized with chosen MinTime/MaxTime values; no real block needed for header-only cases |
| CHECK-03 | Single-UTC-day sample-range (deep mode) | unit | `go test ./pkg/mimirtool/backfill/verify/ -run TestSingleUTCDayDeep` | `block.GenerateBlockFromSpec` with samples that straddle midnight |
| INT-01 | Pre-upload gate: zero HTTP requests on failure | integration | `go test ./pkg/mimirtool/client/ -run TestBackfillAbortsOnVerificationFailure` | `httptest.Server` counting `/api/v1/upload/block/...` requests; expect 0 |
| INT-02 | Log format follows mimirtool conventions | unit | `go test ./pkg/mimirtool/backfill/verify/ -run TestLogFormat` | go-kit/log with capturing logger; assert expected field keys |

Plan artifacts will contain the full Task ID → test mapping after planning completes.

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

No new framework install required — Go + testify already in `go.mod`.

Wave 0 test stubs to create before implementation:

- [ ] `pkg/mimirtool/backfill/verify/verify_test.go` — framework + report tests (FRAMEWORK-01, FRAMEWORK-02, FRAMEWORK-04)
- [ ] `pkg/mimirtool/backfill/verify/well_formed_test.go` — well-formed tests (CHECK-01)
- [ ] `pkg/mimirtool/backfill/verify/single_utc_day_test.go` — UTC-day tests (CHECK-02, CHECK-03)
- [ ] `pkg/mimirtool/client/backfill_test.go` — add or extend for integration tests (FRAMEWORK-03, INT-01, INT-02)
- [ ] Shared test helpers for generating corrupted blocks (truncating chunk files, mangling meta.json, etc.) — reuse `block.GenerateBlockFromSpec` from `pkg/storage/tsdb/block/block_generator.go`

If `pkg/mimirtool/client/backfill_test.go` already exists, extend it; otherwise create fresh.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| End-to-end `mimirtool backfill --dry-run <dir>` against a real local Mimir | INT-01 implicit | Requires running Mimir instance — CI-unfriendly | Start `development/mimir-read-write-mode/compose-up.sh`, run `mimirtool backfill --address http://localhost:9009 --id demo --dry-run <block-dir>`, confirm exit 0 and zero upload requests in Mimir logs |
| `--help` output lists new flags and their descriptions | CLI surface | Text output; automated check can grep but human readability is qualitative | Run `mimirtool backfill --help`, verify `--dry-run`, `--skip-chunk-verification`, `--full-report`, `--verify-concurrency` (or final name) are listed with clear descriptions |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (Wave 0 list above)
- [ ] No watch-mode flags (Go test suite runs to completion)
- [ ] Feedback latency < 60s
- [ ] `nyquist_compliant: true` set in frontmatter after plan is approved

**Approval:** pending
