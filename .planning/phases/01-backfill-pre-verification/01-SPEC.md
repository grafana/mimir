# Phase 1: Backfill Pre-Verification — Specification

**Created:** 2026-04-22
**Ambiguity score:** 0.147 (gate: ≤ 0.20)
**Requirements:** 9 locked

## Goal

`mimirtool backfill` verifies every block locally and rejects the whole batch before any upload if any block is malformed or not confined to a single calendar UTC day.

## Background

Today `MimirClient.Backfill()` at `pkg/mimirtool/client/backfill.go:25-53` iterates blocks and calls `backfillBlock()` for each. The only client-side validation is in `GetBlockMeta()` at `pkg/mimirtool/client/backfill.go:182-242` — it parses `meta.json`, enforces `Version == 1`, and stats index + chunk files. It does not check that the block is structurally sound, nor that it falls on a UTC-day boundary. The Mimir server validates blocks on upload (`client/backfill.go:129`), but uploading first and finding out later is slow, burns bandwidth, and for 2-hour Prometheus-default blocks it silently multiplies compactor load even when accepted. This phase adds a pluggable client-side verifier that fails fast before any upload begins.

## Requirements

1. **Verifier framework**: A pluggable verification module ships in `pkg/mimirtool/` with an interface that accepts both per-block and batch-level checks.
   - Current: No verification abstraction exists anywhere in `pkg/mimirtool/`.
   - Target: A package with a `Verifier` contract (per-block + batch-level) that `MimirClient.Backfill()` calls over the full block list before starting the upload loop. Adding a new check is a matter of implementing the interface and registering it.
   - Acceptance: A test registers a dummy no-op verifier and confirms it's invoked during `Backfill()` without modifying any existing check code.

2. **Pre-upload gate**: Verification runs against every block before any block is uploaded.
   - Current: `MimirClient.Backfill()` starts uploading the first block immediately after parsing its meta.
   - Target: `MimirClient.Backfill()` verifies all blocks first; if any block fails, no upload is attempted.
   - Acceptance: Given a batch of N blocks where block k (1 ≤ k ≤ N) is malformed, running `mimirtool backfill` performs zero HTTP requests to `/api/v1/upload/block/...` and exits non-zero.

3. **Aggregated failure reporting**: Every failing block and every failing check is reported in one run.
   - Current: No failure reporting (no verification).
   - Target: Verification surfaces per-block, per-check failures via go-kit/log at the same level conventions used in `client/backfill.go`, and exits non-zero with a summary line stating total blocks, total failures.
   - Acceptance: Given a batch where 3 of 5 blocks fail (two for different reasons), the log output identifies each failing block by ULID and lists each failing check by name; exit code is non-zero.

4. **Well-formed check (deep by default)**: The well-formed check validates block integrity at two depth levels.
   - Current: Only `meta.json` version check and file-stat in `GetBlockMeta`.
   - Target: A "well-formed" check with two modes — **deep** (default: open block, walk the full index, verify chunk checksums, roughly matching what the compactor/store-gateway do at load time — e.g. Thanos `block.VerifyIndex`) and **medium** (opt-in: open block via `tsdb.OpenBlock` to validate index header, chunk segment headers, series count vs meta, but skip full index walk and chunk checksums). No shallow-only mode exists.
   - Acceptance: A corrupted block (truncated chunk segment, mangled index, or missing file) fails the deep check and fails the medium check; a structurally-valid block passes both; deep-mode verification of a block with a silent checksum mismatch fails, medium-mode passes.

5. **Medium-depth opt-in**: Users who trust their block producer can reduce verification cost.
   - Current: No depth control exists.
   - Target: A CLI flag on `mimirtool backfill` selects medium depth; default is deep.
   - Acceptance: Running `mimirtool backfill` with the medium-depth flag on a block with a chunk checksum mismatch succeeds at verification; the same command without the flag fails verification.

6. **Single-UTC-day check**: Every block must be confined to one calendar UTC day.
   - Current: Block-duration and UTC alignment are unchecked.
   - Target: A check that passes iff both `meta.MinTime` and `meta.MaxTime-1` fall in the same UTC day — i.e. `floor(MinTime / 86_400_000) == floor((MaxTime - 1) / 86_400_000)`. In deep mode, actual min/max sample timestamps from the block index must also satisfy the same condition; in medium mode, the header check alone is required. Block duration is hardcoded to the 24h canonical range; there is no flag to change it.
   - Acceptance: A block with `MinTime = 2026-04-01T00:00:00Z`, `MaxTime = 2026-04-02T00:00:00Z` passes. A 2-hour Prometheus-default block (`MaxTime - MinTime == 2h`) fails. A block with `MinTime = 2026-04-01T12:00:00Z`, `MaxTime = 2026-04-02T12:00:00Z` (crosses midnight) fails. A sparse block with `MinTime = 2026-04-01T00:30:00Z`, `MaxTime = 2026-04-01T23:45:00Z` (same UTC day, span < 24h) passes. In deep mode, a block whose header fits one UTC day but whose actual sample data extends outside that day fails.

7. **No escape hatch**: Verification cannot be bypassed.
   - Current: No verification, so no bypass exists or is needed.
   - Target: No `--skip-verification`, `--no-verify`, `--force`, per-check opt-out flags, or equivalent mechanism. Users who need to upload a non-compliant block must rebuild it to spec.
   - Acceptance: `mimirtool backfill --help` lists no bypass-verification flag; grepping the command implementation finds no code path that skips verification.

8. **Dry-run mode**: A flag runs verification without uploading.
   - Current: No such mode.
   - Target: A `--dry-run` (or equivalent name; final name to be chosen in discuss-phase) flag runs verification across all blocks and exits 0 if all pass or non-zero otherwise, without issuing any HTTP request to the upload API.
   - Acceptance: With a valid batch, `mimirtool backfill --dry-run <dir>` exits 0 and performs zero HTTP requests to `/api/v1/upload/block/...`. With a batch containing a 2-hour block, the same command exits non-zero and still performs zero upload requests.

9. **Framework extension seam**: Batch-level future checks must not require refactor of the framework.
   - Current: No framework exists, so no seam.
   - Target: The batch-level interface accepts a slice of blocks and returns per-block per-check failures. Known future checks — duplicate-day detection, overlap detection — must be implementable as new `BatchVerifier` registrations without modifying existing verifier code, the per-block interface, or `MimirClient.Backfill()`.
   - Acceptance: A test implements a dummy `BatchVerifier` that rejects batches of more than one block, registers it, runs `Backfill()` with two blocks, and observes the batch failing without any modification to the existing per-block verifiers or call sites.

## Boundaries

**In scope:**
- Pluggable verifier framework in `pkg/mimirtool/` (exact package path to be chosen in discuss-phase) with per-block and batch-level interfaces.
- Well-formed block verifier with deep (default) and medium modes, reusing upstream primitives (Prometheus `tsdb.OpenBlock`, Thanos `block.VerifyIndex`, or Mimir internal equivalents from `pkg/storage/tsdb/block/`).
- Single-UTC-day verifier (header-based in medium mode; header + sample-range in deep mode).
- Integration into `MimirClient.Backfill()` so verification runs over the full batch before any upload begins.
- CLI flag for medium depth (final name chosen in discuss-phase).
- CLI `--dry-run` flag (final name chosen in discuss-phase).
- Aggregated per-block, per-check failure reporting via go-kit/log.
- Unit tests for each verifier and for `MimirClient.Backfill()` rejection behavior.
- `[FEATURE]` CHANGELOG entry with a PR number from `./tools/github-next-pr-number.sh`.

**Out of scope:**
- Server-side validation changes — Mimir's server-side uploader already validates blocks and is not modified here.
- Block repair, rewriting, or merging — verification reports failure; users fix their producer.
- Changes to `pkg/mimirtool/backfill/backfill.go` (the block *creation* path) — only the upload path is touched.
- Removing `GetBlockMeta`'s existing checks — they stay and augment the new framework.
- Escape hatches of any kind (global `--skip-verification`, per-check opt-outs, `--force`) — explicitly excluded per locked decision.
- Configurable block duration — 24h is hardcoded; users with custom compactor block-ranges are a deferred concern.
- Batch-level duplicate-day and overlap checks — deferred to v2 via the extension seam; not implemented in this phase.
- Tenant-scoped cardinality / series-limit preflight — deferred to v2, requires server metadata this phase does not add.

## Constraints

- **Build/lint**: Passes `make format`, `make lint`, `make test`. Uses `goimports -local github.com/grafana/mimir` with the three-group import layout described in `pkg/CLAUDE.md`.
- **Dependencies**: Prefer existing Prometheus, Thanos, and Mimir internal block primitives over new code. Do not add new direct dependencies unless no suitable primitive exists.
- **Performance budget**: Deep verification is expected to be O(seconds) per block; this is acceptable since it is a pre-flight check, not hot-path. The medium-depth flag is the escape valve for users who find deep too expensive.
- **Yolo-string safety**: Verification code must not retain strings unmarshaled from shared pooled buffers past the verification call window (see root `CLAUDE.md` § "Unsafe memory tricks"). Follow the Mimir convention of deep-copying via `strings.Clone` when strings outlive their source buffer.
- **Provenance**: Files derived from cortex-tools or upstream Prometheus must carry the corresponding `Provenance-includes-*` headers already used in `pkg/mimirtool/backfill/*.go`.

## Acceptance Criteria

- [ ] A pluggable verifier framework exists with separate per-block and batch-level interfaces.
- [ ] `MimirClient.Backfill()` runs verification on every block before any upload begins.
- [ ] A batch with any failing block produces zero HTTP requests to `/api/v1/upload/block/...` and a non-zero exit.
- [ ] Failure output identifies every failing block (by ULID) and every failing check (by name) in a single run.
- [ ] Well-formed check in deep mode detects truncated chunk segments, mangled indexes, and chunk checksum mismatches.
- [ ] Well-formed check in medium mode detects header-level structural problems (invalid index header, chunk segment header, series-count mismatch vs meta) but may pass silent checksum mismatches.
- [ ] Single-UTC-day check passes blocks whose `[MinTime, MaxTime)` is confined to one UTC day, including sparse blocks with span < 24h.
- [ ] Single-UTC-day check fails 2-hour Prometheus-default blocks and any block whose window crosses a UTC-midnight boundary.
- [ ] Single-UTC-day check in deep mode also validates actual sample timestamps against the one-UTC-day bound.
- [ ] A CLI flag selects medium depth; the default is deep.
- [ ] `--dry-run` (or final equivalent) runs verification without any HTTP request to the upload API and exits 0 on all-pass, non-zero otherwise.
- [ ] No CLI flag or internal code path bypasses verification.
- [ ] A test registers an additional `BatchVerifier` implementation and exercises the framework end-to-end without modifying existing verifier code.
- [ ] `make format`, `make lint`, `make test` all pass on the changed packages.
- [ ] `CHANGELOG.md` has a `[FEATURE]` entry with a PR number from `./tools/github-next-pr-number.sh`.

## Ambiguity Report

| Dimension          | Score | Min  | Status | Notes                                                                        |
|--------------------|-------|------|--------|------------------------------------------------------------------------------|
| Goal Clarity       | 0.90  | 0.75 | ✓      | Single-UTC-day semantics locked; escape hatch excluded.                      |
| Boundary Clarity   | 0.85  | 0.70 | ✓      | Explicit in/out lists; batch-level checks deferred to v2 via extension seam. |
| Constraint Clarity | 0.78  | 0.65 | ✓      | Depth budget defined; flag-tunable; yolo-string rule cited.                  |
| Acceptance Criteria| 0.85  | 0.70 | ✓      | 14 pass/fail checkboxes; each requirement has an explicit acceptance clause. |
| **Ambiguity**      | 0.147 | ≤0.20| ✓      | Gate passed after 2 rounds.                                                  |

## Interview Log

| Round | Perspective                | Question summary                                  | Decision locked                                                                                                      |
|-------|----------------------------|---------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| 0     | Researcher (pre-interview) | Scouted current code + existing primitives        | Upload path at `pkg/mimirtool/client/backfill.go`; `GetBlockMeta` only stats files; Thanos/Prom deep-verify available. |
| 1     | Researcher                 | Failure mode on multi-block failure?              | Abort whole batch; verify all first, upload nothing if any fail.                                                      |
| 1     | Researcher                 | Escape hatch?                                     | None. Verification is mandatory. Users must rebuild bad blocks.                                                       |
| 1     | Researcher                 | Dry-run / check-only mode?                        | Yes, dedicated flag; verification without upload; exit 0 on all-pass.                                                 |
| 2     | Simplifier                 | Depth of well-formed check?                       | Deep default, medium opt-in via flag; no shallow-only mode.                                                           |
| 2     | Boundary Keeper            | 24h hardcoded or configurable?                    | Hardcoded 24h. No flag.                                                                                               |
| 2     | Boundary Keeper            | 24hr block semantics — header, sample range, tolerance? | "One calendar UTC day", not "MaxTime−MinTime exactly 24h". Sparse blocks may span < 24h. Header check in medium; header + sample range in deep. |

---

*Phase: 01-backfill-pre-verification*
*Spec created: 2026-04-22*
*Next step: /gsd-discuss-phase 1 — implementation decisions (package path, flag names, verifier registration mechanism, failure-report shape)*
