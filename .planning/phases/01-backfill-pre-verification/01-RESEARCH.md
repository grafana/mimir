# Phase 1: Backfill Pre-Verification — Research

**Researched:** 2026-04-22
**Domain:** `mimirtool` CLI / `pkg/mimirtool/client` upload path / `pkg/storage/tsdb/block` verification primitives
**Confidence:** HIGH

## Summary

Every primitive we need already exists inside Mimir. `pkg/storage/tsdb/block.VerifyBlock` is a direct in-tree wrapper around `GatherBlockHealthStats`; it takes `checkChunks bool`, a `(minTime, maxTime)` window, and returns an aggregated error via `HealthStats.AnyErr()`. That single primitive covers both `WellFormedVerifier` depth modes AND — by exploiting its `OutsideChunks` counting — covers the deep-mode sample-range check for `SingleUTCDayVerifier` without writing a separate index walker.

Test fixtures are also ready-made: `block.GenerateBlockFromSpec(dir, specs)` writes real TSDB blocks from label+chunk specs and is used throughout the compactor and store-gateway tests. We reuse it directly. There is no existing `pkg/mimirtool/client/backfill_test.go`, so the integration test in this phase creates the first one, following the `httptest.NewServer` pattern already established in `pkg/mimirtool/client/rules_test.go`.

The only two mildly novel bits: kingpin's `CmdClause.Validate()` hook is unused elsewhere in Mimir, but the API is stable (`vendor/github.com/alecthomas/kingpin/v2/cmd.go:256`); and `VerifyBlock` does NOT check `ctx.Done()` inside its postings walk — context cancellation during deep verify only takes effect between blocks, not mid-block. This bounds what fail-fast can actually abort.

**Primary recommendation:** Build `pkg/mimirtool/backfill/verify/` as a thin orchestration layer over `block.VerifyBlock`. Use `errgroup.WithContext` + `SetLimit` for bounded parallelism (matching `pkg/blockbuilder/tsdb.go:506-509`). Use `cmd.Validate(...)` for `--skip-chunk-verification` vs `--full-report` mutual exclusion. Reuse `block.GenerateBlockFromSpec` for fixtures.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

From `01-CONTEXT.md` `<decisions>` block — these are locked and drive the plan:

**Package Layout & Interfaces**
- **D-01:** Verifier module lives at `pkg/mimirtool/backfill/verify/` — nested under the existing `backfill` package alongside the block-creation code at `pkg/mimirtool/backfill/backfill.go`.
- **D-02:** Two separate interfaces. `BlockVerifier` runs per-block checks (`Verify(ctx, blockDir string, meta block.Meta) error`). `BatchVerifier` runs cross-block checks (`Verify(ctx, blocks []BlockRef) error` — final signature TBD in planning, but must accept a slice). The two interfaces are independent to keep the extension seam clean for future duplicate-day and overlap checks.
- **D-03:** Per-block verifier input is `(blockDir string, meta block.Meta)` using Mimir's existing `pkg/storage/tsdb/block.Meta`. Each verifier decides whether it needs to open the block itself — most won't, since `VerifyBlock` accepts the dir directly.

**Registration & Composition**
- **D-04:** Functional options pattern. `verify.NewVerifier(logger, opts ...Option)` with options `WithBlockCheck(BlockVerifier)`, `WithBatchCheck(BatchVerifier)`, `WithMode(Deep|Medium)`, `WithFailFast(bool)`, and a concurrency option.
- **D-05:** **No `init()` auto-registration and no global registry.** Per `pkg/CLAUDE.md` ("No global variables"), verifier wiring happens explicitly in the constructor called from `commands/backfill.go`.
- **D-06:** Two concrete verifier implementations ship in v1: `WellFormedVerifier` and `SingleUTCDayVerifier`.

**Verification Primitive Reuse**
- **D-07:** `WellFormedVerifier` wraps `pkg/storage/tsdb/block.VerifyBlock(ctx, logger, blockDir, meta.MinTime, meta.MaxTime, checkChunks bool)`. `checkChunks=true` in deep mode, `false` in medium mode.
- **D-08:** `SingleUTCDayVerifier` computes the UTC-day bound from `meta.MinTime` and `meta.MaxTime`: pass iff `meta.MinTime / 86_400_000 == (meta.MaxTime - 1) / 86_400_000`. In deep mode, also inspects the min/max sample timestamps derived from the block's index and confirms those also fall within the same UTC day.

**CLI Surface**
- **D-09:** Flags attach directly to `mimirtool backfill` (not a subcommand). Command entry at `pkg/mimirtool/commands/backfill.go:47-103`.
- **D-10:** `--dry-run` — runs verification, exits 0 on all-pass or non-zero otherwise, issues zero HTTP requests to `/api/v1/upload/block/...`.
- **D-11:** `--skip-chunk-verification` — opts depth down to medium (no chunk-checksum walk). Default is deep.
- **D-12:** `--full-report` — switches failure policy from fail-fast to aggregate-all. Default is fail-fast.
- **D-13:** `--skip-chunk-verification` + `--full-report` combined is rejected at flag-parse with a clear error.
- **D-14:** Planner selects a concurrency flag name and default (suggested default: `min(GOMAXPROCS, 4)`; suggested name: `--verify-concurrency`).

**Failure Reporting**
- **D-15:** The framework builds a `VerificationReport` struct with per-block, per-check failure entries.
- **D-16:** Log format: one go-kit/log `level=error` line per failing check. Fields: `block=<ULID>`, `check=<check-name>`, `msg="<human description>"`, plus check-specific fields. Summary line at end.
- **D-17:** `msg="verifying block"` at start, `msg="verified"` at end, per block. No TTY bar.

**Parallelism**
- **D-18:** Parallel per-block verification via `golang.org/x/sync/errgroup` with a bounded concurrency limit.
- **D-19:** In fail-fast mode, the first worker that observes a failing block calls `cancel()` on the errgroup context; peer workers see `ctx.Done()` and abort in-flight `VerifyBlock` calls. Stray in-flight failures acceptable.
- **D-20:** In full-report mode, all workers run to completion regardless of failures. Only exits abort the errgroup.

### Claude's Discretion

- Specific import aliases, internal type names (`Report` vs `VerificationReport` etc.), and exact function signatures within `pkg/mimirtool/backfill/verify/` — planner/executor choose during implementation, consistent with Mimir's goimports conventions.
- Whether `BatchVerifier` exists in v1 code (as an empty slice / no implementations) or is introduced later as part of a v2 batch-check implementation. Preference: ship the interface in v1 (tested with a no-op dummy) so v2 work is purely additive.
- Exact upstream function used for deep sample-range inspection in `SingleUTCDayVerifier` — likely `block.GatherBlockHealthStats` or a lower-level index reader; planner verifies which primitive gives the min/max sample time cheaply. **Research finding: simply calling `VerifyBlock` with `(minTime=utcDayStart, maxTime=utcDayEnd)` exploits the built-in `OutsideChunks`/`CriticalErr` machinery and needs no new code.**
- Test fixture location — `pkg/mimirtool/backfill/verify/testdata/` is the likely home, but planner may reuse existing block fixtures elsewhere in the repo if they exist. **Research finding: use `block.GenerateBlockFromSpec` dynamically at test time via `t.TempDir()`; no static fixtures needed.**
- Exit code semantics beyond "zero on pass, non-zero on fail" — planner decides whether to use distinct codes (e.g. 1 for verification failure, 2 for upload failure).

### Deferred Ideas (OUT OF SCOPE)

- **Batch-level checks** — duplicate-UTC-day detection, time-range overlap detection, tenant-scoped cardinality preflight. Captured in `REQUIREMENTS.md` v2 section. The `BatchVerifier` interface seam enables these; no re-architecture needed in v2.
- **Metrics/observability** — verification counts, failure taxonomy, deep-vs-medium ratios. Not required for v1.
- **Block repair** — SPEC excluded.
- **Tightening SPEC.md for the fail-fast default** — SPEC #3 acceptance text assumes aggregate reporting. See spec_lock amendment in CONTEXT.md.
- **Server-side verification alignment** — separate phase.

</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| FRAMEWORK-01 | Verification module in `pkg/mimirtool/backfill/` with pluggable interface | Package layout locked at `pkg/mimirtool/backfill/verify/` (D-01). Functional-options constructor pattern matches `pkg/storage/ingest/writer_client.go:22` idiom cited in `.planning/codebase/CONVENTIONS.md:133-143`. |
| FRAMEWORK-02 | Per-block AND batch-level checks | Two interfaces: `BlockVerifier.Verify(ctx, blockDir, meta)` and `BatchVerifier.Verify(ctx, blocks)` (D-02). Ship interface + no-op BatchVerifier in v1; v2 adds real batch implementations without refactor. |
| FRAMEWORK-03 | Runs before upload loop in `MimirClient.Backfill()` | Insertion point at `pkg/mimirtool/client/backfill.go:25-53`; verify all of `blocks []string` before the existing `for _, b := range blocks { backfillBlock(...) }` loop. Signature change on `Backfill()` to accept the verifier (or its outputs). |
| FRAMEWORK-04 | Failure reporting identifies block + check + reason, aggregated | `VerificationReport` struct with per-block, per-check entries (D-15); go-kit/log lines per failure (D-16). Uses existing `level.Error(log.With(logger, "block", ULID))` idiom from `client/backfill.go`. |
| CHECK-01 | Well-formed block | `WellFormedVerifier` wraps `block.VerifyBlock(ctx, logger, blockDir, meta.MinTime, meta.MaxTime, checkChunks)` from `pkg/storage/tsdb/block/index.go:41`. No new verification code needed. `checkChunks=true` catches mangled chunks + truncated segments; `checkChunks=false` catches index header problems + series-count mismatches. |
| CHECK-02 | 24-hour block duration — REFRAMED by SPEC to "single calendar UTC day" | SPEC #6 supersedes the literal `MaxTime - MinTime == 24h` reading. `SingleUTCDayVerifier` enforces `meta.MinTime / 86_400_000 == (meta.MaxTime - 1) / 86_400_000` (D-08). Sparse blocks with span < 24h pass; 2-hour Prometheus blocks fail; midnight-crossing blocks fail. |
| CHECK-03 | UTC-day alignment — absorbed into CHECK-02 | Same check. The `floor(…/86_400_000)` formula is exactly the "both endpoints on same UTC day" boolean. In deep mode, the same UTC-day bound is passed as `(minTime, maxTime)` to `VerifyBlock`, which will flag any chunk outside via `OutsideChunks`/`CompleteOutsideChunks`/`CriticalErr`. |
| INT-01 | On verify failure: no uploads + non-zero exit + summary | Verification runs as a pre-pass; on any failure, `Backfill()` returns the aggregated error without invoking `backfillBlock()`. Exit code propagates through `mimirtool`'s existing error return from `cmd.Action`. |
| INT-02 | go-kit/log at same levels as `client/backfill.go` | `level.Info`, `level.Warn`, `level.Error` — identical shape to the existing `level.Error(logctx).Log("msg", "failed uploading block", "err", err)` at `client/backfill.go:36`. |

**Note on CHECK-02/03 reframing:** `REQUIREMENTS.md` originally specified `MaxTime - MinTime == 24h exactly` and `MinTime % 86_400_000 == 0`. SPEC.md §6 (locked) explicitly overrides this with "one calendar UTC day", accepting sparse blocks with span < 24h. The planner should follow SPEC semantics, not the original REQUIREMENTS wording.

</phase_requirements>

## Project Constraints (from CLAUDE.md)

Verbatim directives from root `CLAUDE.md`, `pkg/CLAUDE.md`, and mentor-level `~/.claude/CLAUDE.md` that the plan MUST honor:

**Formatting / lint (root `CLAUDE.md`):**
- Always run `make format` before creating commits.
- `goimports -local github.com/grafana/mimir` three-group import layout: stdlib / 3rd-party / `github.com/grafana/mimir/...`, separated by single blank lines.
- Run the project lint command before pushing. For Mimir: `make format`, `make lint`, `make test`.

**No globals (`pkg/CLAUDE.md`):**
- No global variables.
- No global `promauto.New*`. If metrics are added, use `promauto.With(reg)` and accept `prometheus.Registerer` in the constructor. (v1 of this phase does NOT add metrics — but leave door open in `NewVerifier` signature.)

**CLI / config naming (`pkg/CLAUDE.md`):**
- CLI flags: lowercase, `-` separated (e.g., `--dry-run`, `--skip-chunk-verification`, `--full-report`). All three chosen flag names comply.
- Documentation references: always prefix with a single `-` (e.g., `-dry-run`). Note: kingpin renders flags with `--` in help output; the "single `-`" rule is about how they appear in prose (docs, changelog entries).

**File headers (`CONVENTIONS.md`):**
- Every new Go file begins with `// SPDX-License-Identifier: AGPL-3.0-only`.
- Files derived from cortex-tools/Thanos/Prometheus carry `Provenance-includes-*` block. Net-new files (e.g., everything under `pkg/mimirtool/backfill/verify/`) get SPDX only.

**yoloString safety (root `CLAUDE.md`, Mimir-wide correctness rule):**
- Labels and values unmarshaled from `mimirpb.PreallocWriteRequest` must not outlive the request handler. Not directly applicable to this phase — `block.Meta` is parsed from `meta.json` via `json.Decoder`, not from a pooled gRPC buffer; all strings are Go-allocated. **However**, if the planner has the verifier retain any string pulled from an index reader (e.g., label values during a deep walk), it MUST `strings.Clone` before storing, per the convention.

**Changelog (`CONVENTIONS.md` §CHANGELOG + `CONTRIBUTING.md`):**
- Add `[FEATURE]` entry under `## main / unreleased` at top of `CHANGELOG.md`. Component `Mimirtool:`. PR number obtained from `./tools/github-next-pr-number.sh`. Line format verified against recent entries (see §State of the Art below).

**Commit / git discipline (mentor `CLAUDE.md`):**
- Never commit on `main`/`master`; create a branch first (we're already on `owilliams/backfill-preverify`).
- Never squash — GitHub squash-on-merge handles that.
- Never force-push / reset-hard without asking.
- Only change what's explicitly requested; no drive-by edits to blank lines or unrelated files.

**Docs regeneration:** If new CLI flags land, run `make reference-help doc` and commit the regenerated files. For `mimirtool` specifically, the flag docs live in `cmd/mimirtool/` (not the main `cmd/mimir/help.txt.tmpl`). Planner should verify whether `make reference-help` covers mimirtool or whether this is a no-op — but run it regardless to be safe.

## Architectural Responsibility Map

Single-tier phase. This is not a multi-tier (browser / backend / DB) feature. All capabilities live in the `mimirtool` CLI process — a user-local Go binary that talks HTTP to a remote Mimir cluster.

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Pluggable `Verifier` framework | `pkg/mimirtool/backfill/verify/` (mimirtool library) | — | Library code consumed by the mimirtool binary. Not shared with the server. |
| `WellFormedVerifier` | `pkg/mimirtool/backfill/verify/` | `pkg/storage/tsdb/block` (wrapped primitive) | Thin wrapper calls the existing Mimir TSDB primitive. No server-side change. |
| `SingleUTCDayVerifier` | `pkg/mimirtool/backfill/verify/` | `pkg/storage/tsdb/block` (for deep chunk-range check via `VerifyBlock`) | Header check is pure arithmetic on `meta.Min/MaxTime`; deep check piggybacks on `VerifyBlock`'s `OutsideChunks` logic. |
| CLI flag surface (`--dry-run`, `--skip-chunk-verification`, `--full-report`, concurrency) | `pkg/mimirtool/commands/backfill.go` | — | Kingpin flag registration on the existing `BackfillCommand`. |
| Verifier invocation in upload path | `pkg/mimirtool/client/backfill.go` (`MimirClient.Backfill`) | `pkg/mimirtool/backfill/verify/` | `Backfill()` signature grows to accept verifier/options; call site adds the pre-pass. |
| Parallel execution | `pkg/mimirtool/backfill/verify/` | `golang.org/x/sync/errgroup` (3rd-party) | Standard Mimir pattern (see §Code Examples). |
| Failure aggregation + logging | `pkg/mimirtool/backfill/verify/` (report type) + `pkg/mimirtool/client/backfill.go` (log emission) | go-kit/log (3rd-party) | Report shape lives with verifier; emission is caller's job, matching existing Mimir convention. |

Out-of-scope tiers (explicit):
- **Server (`cmd/mimir/`, `pkg/distributor/`, `pkg/compactor/`):** untouched. Server-side upload validation already exists at `pkg/compactor/block_upload.go`; we do not modify it.
- **Block *creation* path (`pkg/mimirtool/backfill/backfill.go`):** per SPEC out-of-scope list.
- **`GetBlockMeta` at `pkg/mimirtool/client/backfill.go:182`:** stays as-is.

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `github.com/grafana/mimir/pkg/storage/tsdb/block` | in-tree | `VerifyBlock`, `GatherBlockHealthStats`, `Meta`, `GenerateBlockFromSpec` | `[VERIFIED: pkg/storage/tsdb/block/index.go:41-48, :177]` The canonical Mimir block-verification primitive. Already under test (`index_test.go`). Covers all `WellFormedVerifier` + deep `SingleUTCDayVerifier` needs. |
| `github.com/alecthomas/kingpin/v2` | (vendored) | CLI flag registration + cross-flag validation | `[VERIFIED: vendor/github.com/alecthomas/kingpin/v2/cmd.go:256]` Already used by all `pkg/mimirtool/commands/*.go`. `CmdClause.Validate(func(c *CmdClause) error)` is the idiomatic hook for cross-flag mutual-exclusion (D-13). |
| `github.com/go-kit/log` + `github.com/go-kit/log/level` | (vendored) | Structured logging at info/warn/error | `[VERIFIED: pkg/mimirtool/client/backfill.go:18-19]` Current logging primitive on the upload path. INT-02 explicitly requires consistency. |
| `golang.org/x/sync/errgroup` | (vendored) | Bounded parallelism with context cancellation | `[VERIFIED: pkg/blockbuilder/tsdb.go:506-509, pkg/storage/tsdb/block/block_generator.go:218]` Used throughout Mimir. `errgroup.WithContext(ctx)` + `eg.SetLimit(n)` is the current idiom (newer, cleaner than the semaphore channel pattern — Mimir uses SetLimit). |
| `github.com/oklog/ulid/v2` | (vendored) | Block ULIDs (already in `block.Meta`) | `[VERIFIED: pkg/storage/tsdb/block/index.go:23]` Used for log `block=<ULID>` field. No direct dependency needed — `meta.ULID.String()` is already available. |
| `github.com/stretchr/testify/require` + `.../assert` | v1.11.1 | Test assertions | `[VERIFIED: go.mod:38, .planning/codebase/TESTING.md]` Mimir-wide standard. |

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `github.com/prometheus/prometheus/tsdb` | (vendored) | `tsdb.BlockMeta` (embedded in `block.Meta`) for `MinTime`/`MaxTime`/`ULID` | Already transitively available via `block.Meta`. No new import needed in verifier code. |
| `github.com/pkg/errors` | (vendored) | `errors.Wrap`, `errors.Is` | `[VERIFIED: pkg/mimirtool/client/backfill.go:20]` Existing convention in the mimirtool upload path. Matching style keeps the diff small. |
| `net/http/httptest` | stdlib | HTTP test server for integration test | `[VERIFIED: pkg/mimirtool/client/rules_test.go:22]` The sibling test file already uses this pattern. Our new `backfill_test.go` follows suit. |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `block.VerifyBlock` wrapper | Hand-roll using `index.NewFileReader` + `chunks.NewDirReader` | More code, more surface to review, reinvents existing CriticalErr/OutsideChunks machinery. No upside. Reject. |
| `tsdb.OpenBlock` directly | Same as hand-rolling, but via upstream Prometheus API | `block.VerifyBlock` already uses these internally and returns the aggregated error we want. Double-wrapping adds no value. Reject. |
| `errgroup.SetLimit(n)` | Semaphore channel (`make(chan struct{}, n)`) | `SetLimit` was added in `golang.org/x/sync` v0.3 and is already used in Mimir (`pkg/blockbuilder/tsdb.go:508`, `pkg/compactor/batch_caching_meta_fetcher.go:144`). Prefer the newer idiom. |
| `kingpin.CmdClause.Validate(...)` | Check inside `Action(...)` handler | `Validate` runs during flag parsing, before the action fires; gives a cleaner error message at flag-parse time. Downside: unused elsewhere in Mimir — we'd be the first. Still the right call — it's a stable vendored API. Alternative (post-parse check in `backfill()` action) is a safe fallback if the planner prefers minimal-risk. |
| Static test fixtures (`testdata/` with pre-generated blocks) | Dynamic `block.GenerateBlockFromSpec` per test | Dynamic is strictly better — no binary blobs in git, tests are self-describing, fixture drift impossible. `GenerateBlockFromSpec` is already used in `pkg/compactor/compactor_test.go:1311`. |

**Installation:** no new dependencies. All listed are already vendored and in `go.mod`. `go.mod`/`vendor/` will NOT change in this phase.

**Version verification:** Skipped. No new packages added. All primitives are in-tree or already vendored at their current pinned versions.

## Architecture Patterns

### System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ user invocation                                                             │
│   mimirtool backfill [flags] <block-dir>...                                 │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ kingpin flag parse                                                          │
│   pkg/mimirtool/commands/backfill.go                                        │
│                                                                             │
│   Flags: --dry-run, --skip-chunk-verification, --full-report,               │
│          --verify-concurrency (name TBD), existing --address/--id/--user    │
│                                                                             │
│   cmd.Validate(): reject --skip-chunk-verification + --full-report          │
│                                                                             │
│   Action: construct client + verifier options, invoke Backfill()            │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ MimirClient.Backfill(ctx, blocks, sleepTime, verifier, dryRun)              │
│   pkg/mimirtool/client/backfill.go:25                                       │
│                                                                             │
│   1. Pre-verification pass ──────────────────┐                              │
│                                              │                              │
│   2. IF any block failed → aggregated        │                              │
│      error, zero uploads, non-zero exit      │                              │
│                                              │                              │
│   3. IF --dry-run → exit 0 (or non-zero),    │                              │
│      skip upload loop                        │                              │
│                                              │                              │
│   4. ELSE → existing upload loop (unchanged) │                              │
└──────────────────────────────────────────────┼──────────────────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ verify.Verifier.Run(ctx, blockDirs)                                         │
│   pkg/mimirtool/backfill/verify/ (new package)                              │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────┐               │
│   │ Per-block stage                                         │               │
│   │   errgroup.WithContext + SetLimit(concurrency)          │               │
│   │   for each blockDir:                                    │               │
│   │     meta := GetBlockMeta(blockDir)                      │               │
│   │     for each BlockVerifier:                             │               │
│   │       err := v.Verify(ctx, blockDir, meta)              │               │
│   │       if err != nil:                                    │               │
│   │         report.Add(block, check, err)                   │               │
│   │         if failFast: cancel() → peers abort             │               │
│   └──────────────┬──────────────────────────────────────────┘               │
│                  │                                                          │
│                  ▼                                                          │
│   ┌─────────────────────────────────────────────────────────┐               │
│   │ Batch stage (runs AFTER per-block completes/aborts)     │               │
│   │   for each BatchVerifier:                               │               │
│   │     err := v.Verify(ctx, allBlocks)                     │               │
│   │     report.Add(..., check, err)                         │               │
│   │                                                         │               │
│   │   v1: no BatchVerifier implementations registered.      │               │
│   │   Framework tests use a dummy BatchVerifier to prove    │               │
│   │   the seam works (FRAMEWORK-02 acceptance).             │               │
│   └──────────────┬──────────────────────────────────────────┘               │
│                  │                                                          │
│                  ▼                                                          │
│   ┌─────────────────────────────────────────────────────────┐               │
│   │ Report emission (caller: MimirClient.Backfill)          │               │
│   │   for each failure entry:                               │               │
│   │     level.Error(logger).Log("block", ULID, "check", …)  │               │
│   │   level.Info(logger).Log("msg", "verification summary", │               │
│   │     "total_blocks", n, "failures", f, "outcome", …)     │               │
│   └─────────────────────────────────────────────────────────┘               │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ BlockVerifier implementations (per-block)                                   │
│                                                                             │
│  ┌──────────────────────────────────┐  ┌───────────────────────────────┐    │
│  │ WellFormedVerifier               │  │ SingleUTCDayVerifier          │    │
│  │                                  │  │                               │    │
│  │ Deep  : VerifyBlock(ctx, log,    │  │ Header: floor(Min/86_400_000) │    │
│  │           dir, meta.Min,         │  │         == floor((Max-1)/…)   │    │
│  │           meta.Max,              │  │                               │    │
│  │           checkChunks=true)      │  │ Deep  : VerifyBlock with      │    │
│  │                                  │  │         minTime=utcDayStart,  │    │
│  │ Medium: VerifyBlock(…,           │  │         maxTime=utcDayEnd     │    │
│  │           checkChunks=false)     │  │         → flags OutsideChunks │    │
│  └──────────────────────────────────┘  └───────────────────────────────┘    │
│                                                                             │
│  Both delegate to pkg/storage/tsdb/block.VerifyBlock                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

Primary use case trace (a valid 3-block batch, default flags):
1. User runs `mimirtool backfill dir1 dir2 dir3` → kingpin parses → cmd.Validate passes → Action fires → constructs `verify.Verifier{mode: Deep, failFast: true, concurrency: min(GOMAXPROCS, 4)}`.
2. `MimirClient.Backfill` invokes `verifier.Run(ctx, [dir1, dir2, dir3])`.
3. errgroup schedules 3 workers (concurrency limit = 3 in this case). Each worker loads meta, runs `WellFormedVerifier` then `SingleUTCDayVerifier`.
4. All three pass → report has no failure entries → summary log line `outcome=all_checked, failures=0`.
5. `Backfill()` proceeds to existing upload loop.

Fail-fast trace (block 2 malformed, 3-block batch, default flags):
1. Worker for dir2 detects `WellFormedVerifier` failure → records in report → calls errgroup cancel.
2. Workers for dir1 and dir3 see `ctx.Done()` at next block scheduling point. **Caveat:** if a worker is already inside a `VerifyBlock` call on a large block, it won't abort mid-walk (see Pitfalls below). That's acceptable per D-19.
3. errgroup.Wait() returns. Report has ≥1 failure entry (possibly more due to in-flight stragglers).
4. Summary log line `outcome=aborted, first_failure_block=<ULID>`.
5. `Backfill()` returns aggregated error. Zero upload HTTP requests.

### Recommended Project Structure

```
pkg/mimirtool/backfill/
├── backfill.go                    # EXISTING — block creation, unchanged
└── verify/                        # NEW
    ├── verify.go                  # Verifier, Option, NewVerifier, Report types
    ├── verify_test.go             # Framework-level tests (FRAMEWORK-01, -02, -04)
    ├── wellformed.go              # WellFormedVerifier (wraps block.VerifyBlock)
    ├── wellformed_test.go         # CHECK-01: valid / corrupted / checksum-mismatch cases
    ├── singleutcday.go            # SingleUTCDayVerifier
    └── singleutcday_test.go       # CHECK-02/CHECK-03 (SPEC #6 semantics)

pkg/mimirtool/commands/
└── backfill.go                    # MODIFIED — new flags + cmd.Validate

pkg/mimirtool/client/
├── backfill.go                    # MODIFIED — Backfill() signature + pre-verify pass
└── backfill_test.go               # NEW — INT-01, INT-02 integration test

CHANGELOG.md                       # MODIFIED — [FEATURE] Mimirtool: ... #<PR>
```

### Pattern 1: Functional Options for the Verifier Constructor

**What:** Private options struct + public `Option` function type + `WithX` helpers. Matches the pattern at `pkg/storage/ingest/writer_client.go:22` cited in `CONVENTIONS.md`.
**When to use:** D-04 locks this — `NewVerifier(logger log.Logger, opts ...Option)`.
**Example:**
```go
// Source: pkg/storage/ingest/writer_client.go:22 (adapted)
package verify

import "github.com/go-kit/log"

type options struct {
    blockChecks []BlockVerifier
    batchChecks []BatchVerifier
    mode        Mode
    failFast    bool
    concurrency int
    // reg prometheus.Registerer // future: metrics hook per pkg/CLAUDE.md
}

type Option func(*options)

func WithBlockCheck(v BlockVerifier) Option { return func(o *options) { o.blockChecks = append(o.blockChecks, v) } }
func WithBatchCheck(v BatchVerifier) Option { return func(o *options) { o.batchChecks = append(o.batchChecks, v) } }
func WithMode(m Mode) Option                { return func(o *options) { o.mode = m } }
func WithFailFast(b bool) Option            { return func(o *options) { o.failFast = b } }
func WithConcurrency(n int) Option          { return func(o *options) { o.concurrency = n } }

func NewVerifier(logger log.Logger, opts ...Option) *Verifier {
    o := options{mode: Deep, failFast: true, concurrency: defaultConcurrency()}
    for _, opt := range opts { opt(&o) }
    return &Verifier{logger: logger, opts: o}
}
```

### Pattern 2: Bounded-concurrency errgroup

**What:** `errgroup.WithContext(ctx)` + `eg.SetLimit(n)`.
**When to use:** D-18 — parallel per-block verification.
**Example:**
```go
// Source: pkg/blockbuilder/tsdb.go:506-509
eg, ctx := errgroup.WithContext(ctx)
if b.cfg.BlocksStorage.TSDB.ShipConcurrency > 0 {
    eg.SetLimit(b.cfg.BlocksStorage.TSDB.ShipConcurrency)
}
for tenant, db := range b.tsdbs {
    eg.Go(func() (err error) {
        // … work that respects ctx.Done() between phases …
    })
}
return eg.Wait()
```

Default concurrency via `min(runtime.GOMAXPROCS(0), 4)`. This matches `pkg/usagetracker/tracker_store_snapshot.go:67` (`min(len(shards), runtime.GOMAXPROCS(0))`).

### Pattern 3: Kingpin cross-flag validation via `cmd.Validate`

**What:** `cmd.Validate(func(c *kingpin.CmdClause) error)` — validator runs during flag parsing, before `Action`.
**When to use:** D-13 — reject `--skip-chunk-verification` + `--full-report`.
**Example:**
```go
// Source: vendor/github.com/alecthomas/kingpin/v2/cmd.go:256 (API docs)
cmd := app.Command("backfill", "...")
cmd.Validate(func(_ *kingpin.CmdClause) error {
    if c.skipChunkVerification && c.fullReport {
        return fmt.Errorf("--full-report requires deep analysis and cannot be combined with --skip-chunk-verification")
    }
    return nil
})
```

**Fallback:** If the planner prefers the safer in-tree pattern, do the check at the top of `c.backfill(logger)` before constructing the client. Less elegant, but identical user-visible behavior. Either is acceptable.

### Pattern 4: httptest-backed integration test

**What:** `httptest.NewServer` captures requests; test asserts request count/paths.
**When to use:** INT-01 acceptance — prove zero `/api/v1/upload/block/...` requests fire on verification failure or `--dry-run`.
**Example:**
```go
// Source: pkg/mimirtool/client/rules_test.go:19-25 (pattern)
requestCh := make(chan *http.Request, 16)
ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    requestCh <- r
    w.WriteHeader(http.StatusOK)
}))
defer ts.Close()

// Build client pointed at ts.URL, invoke Backfill with a batch that includes
// one malformed block. Assert len(requestCh) == 0.
```

### Anti-Patterns to Avoid

- **Reimplementing index/chunk walks.** `block.VerifyBlock` is the one true primitive — do not write alternative index readers or chunk walkers in `verify/`. Breaking this rule forfeits future upstream fixes from Thanos/Mimir TSDB work.
- **Retaining strings from block data without `strings.Clone`.** Even though `block.Meta` is JSON-parsed (not yolo), if any future `BlockVerifier` reads label values from the index, it MUST clone before retaining. Today's code doesn't trip this; tomorrow's might.
- **Using a global `init()` registry or singleton verifier.** Explicitly forbidden by D-05 + `pkg/CLAUDE.md` "No global variables". All wiring is explicit in `commands/backfill.go` → `NewVerifier(...)`.
- **Silently skipping verification on error loading meta.** If `GetBlockMeta` fails for a block, that's a verification failure; record it in the report, don't bail out of the whole batch before other blocks get checked (unless fail-fast). Matches "aggregate all failures" intent of FRAMEWORK-04.
- **Mutating `Backfill()` in place without a signature change.** The signature must grow to accept the verifier (or equivalent). Retrofitting via a field on `MimirClient` hides the dependency and makes testing harder. Prefer an explicit parameter or an options-struct argument.
- **Default-on metrics in v1.** SPEC locks observability as deferred. Leave the `promauto.With(reg)` door open in `NewVerifier`'s signature (via an unused `Option`) but do NOT register any counters/histograms yet.
- **Writing static block fixtures to `testdata/`.** `block.GenerateBlockFromSpec` generates blocks at test time into `t.TempDir()`. Binary fixtures in git rot.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| TSDB block integrity verification | Index walker + chunk reader from scratch | `block.VerifyBlock(ctx, logger, dir, minTime, maxTime, checkChunks)` | `[VERIFIED: pkg/storage/tsdb/block/index.go:41]` Already handles index format check, postings order, per-series out-of-order chunks, outside-range chunks, CRC32 chunk data verification, Issue347 edge cases. Used internally by the compactor and store-gateway. |
| Reading block `meta.json` | New JSON reader | `block.ReadMetaFromDir(dir)` OR `client.GetBlockMeta(dir)` | `[VERIFIED: pkg/storage/tsdb/block/meta.go:229, pkg/mimirtool/client/backfill.go:182]` Two existing helpers. `GetBlockMeta` also populates `Thanos.Files`, which the upload path needs. For the verifier we likely want `block.ReadMetaFromDir` (simpler; doesn't stat chunk files). |
| Bounded-concurrency worker pool | Custom `sync.WaitGroup` + semaphore channel | `errgroup.WithContext(ctx)` + `eg.SetLimit(n)` | `[VERIFIED: pkg/blockbuilder/tsdb.go:506]` Idiomatic, context-cancellation built in, returns the first error automatically. |
| Flag mutual-exclusion | Post-parse check scattered across handler | `kingpin.CmdClause.Validate(...)` | `[CITED: vendor/github.com/alecthomas/kingpin/v2/cmd.go:256]` Fires at parse time; error message surfaces cleanly. |
| Block test fixtures | Hand-crafted byte blobs in `testdata/` | `block.GenerateBlockFromSpec(dir, specs)` | `[VERIFIED: pkg/storage/tsdb/block/block_generator.go:70, pkg/compactor/compactor_test.go:1311]` Generates real valid blocks from label+chunk specs. For *corrupted* fixtures: generate, then surgically truncate/overwrite the index or a chunk segment file with `os.Truncate` / `os.WriteFile`. |
| ULID generation | New ULID package | `block.Meta.ULID` (already present) | Each block has a ULID in meta.json; no generation needed. |
| go-kit log field formatting | New logging helper | `log.With(logger, "block", meta.ULID.String())` | `[VERIFIED: pkg/mimirtool/client/backfill.go:30, :69]` Matching the existing upload-path style keeps log output coherent. |

**Key insight:** The entire "verification" feature reduces to orchestration: scheduling, aggregating, logging. Every *actual* verification byte-twiddle is delegated to `block.VerifyBlock`. Keep the new `verify/` package small.

## Runtime State Inventory

Not applicable — this is a greenfield phase (net-new package + flag additions + one function signature change). No rename/refactor/migration work. There is no existing runtime state with the old names to migrate.

Explicit nulls:
- **Stored data:** None — verifier is stateless; `block.Meta` is read-only from disk.
- **Live service config:** None — mimirtool is a local CLI, no server-side config is touched.
- **OS-registered state:** None — no services, tasks, or daemons involved.
- **Secrets/env vars:** None new; existing `MIMIR_ADDRESS`, `MIMIR_API_USER`, `MIMIR_API_KEY`, `MIMIR_TLS_*`, `MIMIR_EXTRA_HEADERS` env vars read by `commands/backfill.go:54-98` are untouched.
- **Build artifacts:** None — `go build` produces the same `mimirtool` binary; no new generated files beyond the regenerated CLI help (from `make reference-help`).

## Common Pitfalls

### Pitfall 1: `VerifyBlock` does not check `ctx.Done()` inside the postings walk

**What goes wrong:** In fail-fast mode, when one worker detects a failure and calls `cancel()`, peer workers that are already inside a `VerifyBlock` call won't abort mid-walk. They'll finish scanning their current block (potentially several seconds of deep verification) before observing `ctx.Done()`.

**Why it happens:** `[VERIFIED: pkg/storage/tsdb/block/index.go:177-299]` `GatherBlockHealthStats` passes `ctx` only to `r.Postings(ctx, ...)` at line 190. The main `for p.Next()` loop (lines 212–293) does NOT re-check `ctx.Err()`. The Prometheus TSDB `index.NewFileReader` and chunks readers are also mostly ctx-agnostic in their iteration.

**How to avoid:** Accept this. D-19 already acknowledges "stray failures acceptable". What we CAN do: check `ctx.Err()` at the top of each `BlockVerifier.Verify()` implementation, so workers bail *between* blocks (before starting the next one). That keeps the total worst-case waste bounded to `concurrency × one_block_verify_duration`.

**Warning signs:** Tests that expect fail-fast to literally cancel in-flight VerifyBlock calls will fail. Document in `verify.go` that "fail-fast guarantees no new blocks are started after a failure; already-running blocks are allowed to complete".

### Pitfall 2: UTC-day math off-by-one on `MaxTime`

**What goes wrong:** Naively checking `MinTime / DAY == MaxTime / DAY` fails for exactly-aligned blocks: a block with `MinTime=2026-04-01T00:00:00Z` and `MaxTime=2026-04-02T00:00:00Z` (the canonical 24-hour case) would fail because `MaxTime / DAY == 2026-04-02`'s day.

**Why it happens:** Prometheus TSDB uses half-open `[MinTime, MaxTime)` ranges. `[VERIFIED: pkg/storage/tsdb/block/block_generator.go:173]` — see the comment `MaxTime: specs.MaxTime() + 1, // Not included.` So `MaxTime` is exclusive; the last included millisecond is `MaxTime - 1`.

**How to avoid:** Use `floor(MinTime / 86_400_000) == floor((MaxTime - 1) / 86_400_000)` exactly as D-08 specifies. For the empty-block edge case (`MinTime == MaxTime`), decide: either treat as a failure ("zero-duration block") or bypass the UTC check since there are no samples. Recommend: emit a distinct error "block time range is empty (MinTime == MaxTime)" — cleaner signal than UTC-day failure.

**Warning signs:** Table-driven test with aligned-boundary block fails. Add a test case for exact `[day_start, next_day_start)`.

### Pitfall 3: Deep-mode UTC check using `VerifyBlock`'s own `minTime`/`maxTime` parameters

**What goes wrong:** If the planner tries to run `VerifyBlock` once with `(meta.MinTime, meta.MaxTime)` for well-formedness AND again with `(utcDayStart, utcDayEnd)` for UTC compliance, that's two full index+chunk walks per block. Double the IO cost.

**Why it happens:** `VerifyBlock` flags `OutsideChunks` relative to the `(minTime, maxTime)` passed in. You can't get both the "chunks are in the declared block range" check and the "chunks are within one UTC day" check from a single invocation unless the two ranges coincide.

**How to avoid:** For `SingleUTCDayVerifier` deep mode, reuse the same underlying `HealthStats` call used by `WellFormedVerifier` — call `GatherBlockHealthStats` once per block, share the result across both verifiers via a small per-block cache, OR treat the pair as a single composite verifier that calls `GatherBlockHealthStats(ctx, logger, dir, utcDayStart, utcDayEnd, checkChunks)` and then checks BOTH `AnyErr()` (well-formed) and `OutsideChunks == 0` (single-UTC-day).

**Planner decision required:** pick the approach. Options ranked by code simplicity:
1. **Single composite deep walk (RECOMMENDED):** pass `(utcDayStart, utcDayEnd)` to `VerifyBlock`; that one call covers both well-formed AND deep UTC-day. Per-block cost: 1× index+chunk walk.
2. **Two separate verifiers with per-block cache:** add a `sharedHealthStats map[blockDir]HealthStats` to the Verifier; verifiers look up first, compute on miss. Per-block cost: 1× walk; more plumbing.
3. **Naive two-walk implementation:** simplest code, 2× IO cost. Reject for deep mode; acceptable only if deep ever becomes non-default.

**Warning signs:** Phase benchmarks show deep-mode verify takes ~2× expected wall-clock. Grep for two distinct `VerifyBlock` or `GatherBlockHealthStats` call sites per block.

### Pitfall 4: `make reference-help doc` regeneration

**What goes wrong:** New CLI flags on `mimirtool backfill` land without regenerating the documentation. CI fails `check-reference-help` or `check-doc`.

**Why it happens:** `[CITED: CONVENTIONS.md §"Config / Flag Documentation Regeneration"]` Any flag addition needs `make reference-help doc` and a commit of the regenerated files.

**How to avoid:** After flag implementation, run:
```bash
make reference-help doc
git status   # check for changes to cmd/mimirtool/ help files and docs/sources/...
git add -- <paths>
```

**Warning signs:** CI failure on `check-doc` or `check-reference-help`. The planner should include this step as an explicit task in the plan.

### Pitfall 5: `Backfill()` signature change is a public API break

**What goes wrong:** `MimirClient.Backfill` is exported from `pkg/mimirtool/client`. Downstream importers (Grafana Cloud tooling, third-party scripts) break if we add a parameter without a backward-compatible shim.

**Why it happens:** `pkg/mimirtool/client` is intended as an importable library, not just a CLI internal. Search confirms it's imported from `pkg/mimirtool/commands/backfill.go` and nothing else *inside* Mimir, but external callers exist in practice.

**How to avoid:** Two options. Both acceptable — planner picks.
- (A) Add a new exported method `BackfillWithOptions(ctx, blocks, opts)` and keep the existing `Backfill(ctx, blocks, sleepTime)` as a thin wrapper that delegates with default (deep, fail-fast, no dry-run) options. Zero break.
- (B) Change `Backfill()`'s signature outright and accept that external callers must update. Note it in the CHANGELOG `[CHANGE]` entry — but then this phase needs TWO changelog entries: a `[CHANGE]` for the signature break AND a `[FEATURE]` for the new capability.

**Warning signs:** PR review flags the signature change as a breaking API. Preemptive mitigation: propose (A) unless the planner has a strong reason to break.

### Pitfall 6: CHANGELOG PR number

**What goes wrong:** Changelog entry lands with a placeholder PR number (`#TODO` or `#0`) because the PR isn't open yet. CI may not catch this but reviewers will.

**Why it happens:** `./tools/github-next-pr-number.sh` is meant to be run immediately before opening the PR. The number is reserved atomically.

**How to avoid:** The plan should sequence: (1) all code and tests green locally → (2) `./tools/github-next-pr-number.sh` → (3) update CHANGELOG with the returned number → (4) final commit → (5) open the PR with that exact number. Do NOT run the script early.

**Warning signs:** CHANGELOG diff in early commits has a number that doesn't match the eventual PR URL.

### Pitfall 7: `pkg/mimirtool/backfill/backfill.go` provenance header drift

**What goes wrong:** New files in `pkg/mimirtool/backfill/verify/` accidentally inherit the `Provenance-includes-location: https://github.com/grafana/cortex-tools/...` header from their sibling `backfill.go`.

**Why it happens:** `[VERIFIED: pkg/mimirtool/backfill/backfill.go:1-4]` The sibling carries a Provenance block because it was forked from cortex-tools. The new verifier code is net-new — no provenance.

**How to avoid:** Every new file in `verify/` begins with ONLY:
```go
// SPDX-License-Identifier: AGPL-3.0-only
```
No `Provenance-includes-*` lines. Confirmed by the precedent in `pkg/mimirtool/commands/backfill.go:1` (SPDX only; net-new Mimir code) vs `pkg/mimirtool/commands/bucket_validation.go:1-4` (full Provenance block; forked from cortex-tools).

### Pitfall 8: BatchVerifier seam proof test must use a *different* BatchVerifier than any v1 ships

**What goes wrong:** FRAMEWORK-02 / SPEC acceptance #9 requires registering a *new* `BatchVerifier` without modifying existing verifier code. If v1 ships a `DuplicateDayBatchVerifier` (it shouldn't per SPEC out-of-scope — but the seam test still needs teeth), the test would be trivially satisfied just by using the built-in.

**Why it happens:** Easy to forget that v1 ships ZERO real batch verifiers. The seam test must implement its own dummy `BatchVerifier` inline in `_test.go`.

**How to avoid:** Seam test pattern:
```go
// in verify_test.go
type rejectMultiBatchVerifier struct{}
func (rejectMultiBatchVerifier) Name() string { return "reject-multi" }
func (rejectMultiBatchVerifier) Verify(ctx context.Context, blocks []BlockRef) error {
    if len(blocks) > 1 { return fmt.Errorf("rejecting multi-block batch") }
    return nil
}
// Test registers WithBatchCheck(rejectMultiBatchVerifier{}), runs Backfill with 2 blocks,
// observes the batch failing without any modification to WellFormedVerifier or SingleUTCDayVerifier.
```

**Warning signs:** Test uses a built-in BatchVerifier or no BatchVerifier at all.

## Code Examples

### `block.VerifyBlock` call (WellFormedVerifier body)

```go
// Source: pkg/storage/tsdb/block/index.go:41-48 (the primitive we wrap)
//
// func VerifyBlock(ctx context.Context, logger log.Logger, blockDir string,
//                  minTime, maxTime int64, checkChunks bool) error {
//     stats, err := GatherBlockHealthStats(ctx, logger, blockDir, minTime, maxTime, checkChunks)
//     if err != nil { return err }
//     return stats.AnyErr()
// }

// WellFormedVerifier usage (verify/wellformed.go, sketch):
func (v *WellFormedVerifier) Verify(ctx context.Context, blockDir string, meta block.Meta) error {
    if err := ctx.Err(); err != nil { return err } // cheap cancellation check
    return block.VerifyBlock(ctx, v.logger, blockDir, meta.MinTime, meta.MaxTime, v.checkChunks)
}
```

### SingleUTCDayVerifier body

```go
// verify/singleutcday.go, sketch:
const msPerDay = 24 * 60 * 60 * 1000 // 86_400_000

func (v *SingleUTCDayVerifier) Verify(ctx context.Context, blockDir string, meta block.Meta) error {
    if err := ctx.Err(); err != nil { return err }

    // Header check: both [MinTime, MaxTime) endpoints must fall in the same UTC day.
    // MaxTime is exclusive; last included millisecond is MaxTime - 1.
    if meta.MinTime >= meta.MaxTime {
        return fmt.Errorf("block time range is empty or inverted: MinTime=%d MaxTime=%d",
            meta.MinTime, meta.MaxTime)
    }
    startDay := meta.MinTime / msPerDay
    endDay := (meta.MaxTime - 1) / msPerDay
    if startDay != endDay {
        return fmt.Errorf("block [MinTime=%d, MaxTime=%d) spans multiple UTC days (%d..%d)",
            meta.MinTime, meta.MaxTime, startDay, endDay)
    }

    // Deep mode: also verify no chunk data falls outside the UTC-day window.
    if v.deep {
        utcDayStart := startDay * msPerDay
        utcDayEnd := utcDayStart + msPerDay
        // VerifyBlock with (utcDayStart, utcDayEnd) will flag any chunk outside
        // via HealthStats.OutsideChunks -> CriticalErr -> AnyErr.
        if err := block.VerifyBlock(ctx, v.logger, blockDir, utcDayStart, utcDayEnd, v.checkChunks); err != nil {
            return fmt.Errorf("block has chunks outside UTC day [%d, %d): %w", utcDayStart, utcDayEnd, err)
        }
    }
    return nil
}
```

**Planner note on Pitfall 3:** To avoid the double-walk, the planner may fold the deep-mode UTC check into `WellFormedVerifier` OR share a computed `HealthStats` between the two verifiers. See Pitfall 3 for three options.

### Fixture generation in tests

```go
// Source: pkg/compactor/compactor_test.go:1311 (pattern)
// wellformed_test.go sketch:
func TestWellFormedVerifier_ValidBlock(t *testing.T) {
    dir := t.TempDir()
    specs := []*block.SeriesSpec{
        {
            Labels: labels.FromStrings("__name__", "foo"),
            Chunks: []chunks.Meta{
                must(chunks.ChunkFromSamples([]chunks.Sample{
                    testutil.Sample{TS: 0, Val: 1.0},
                    testutil.Sample{TS: 1000, Val: 2.0},
                })),
            },
        },
        // ... at least 3 series; see block_generator.go:200 requirement
    }
    meta, err := block.GenerateBlockFromSpec(dir, specs)
    require.NoError(t, err)

    v := &WellFormedVerifier{logger: log.NewNopLogger(), checkChunks: true}
    require.NoError(t, v.Verify(context.Background(), filepath.Join(dir, meta.ULID.String()), *meta))
}

func TestWellFormedVerifier_CorruptedChunk(t *testing.T) {
    dir := t.TempDir()
    meta := generateValidBlock(t, dir) // helper using GenerateBlockFromSpec
    // Corrupt the first chunk segment file.
    chunkFile := filepath.Join(dir, meta.ULID.String(), "chunks", "000001")
    require.NoError(t, os.Truncate(chunkFile, 10)) // truncate mid-chunk

    v := &WellFormedVerifier{logger: log.NewNopLogger(), checkChunks: true}
    err := v.Verify(context.Background(), filepath.Join(dir, meta.ULID.String()), *meta)
    require.Error(t, err)
}
```

### Integration test (INT-01: zero HTTP requests on verify failure)

```go
// Source: pkg/mimirtool/client/rules_test.go:19-26 (httptest pattern)
// pkg/mimirtool/client/backfill_test.go (NEW):
func TestBackfill_VerificationFailureSkipsUpload(t *testing.T) {
    var uploadCount atomic.Int64
    ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if strings.HasPrefix(r.URL.Path, "/api/v1/upload/block/") {
            uploadCount.Add(1)
        }
        w.WriteHeader(http.StatusOK)
    }))
    defer ts.Close()

    validDir := createValidBlockDir(t)      // uses GenerateBlockFromSpec
    badDir := createMalformedBlockDir(t)    // uses GenerateBlockFromSpec + truncate chunk

    cli, err := client.New(client.Config{Address: ts.URL, ID: "t1"}, log.NewNopLogger())
    require.NoError(t, err)

    err = cli.Backfill(context.Background(), []string{validDir, badDir}, 10*time.Millisecond /*, verifier opts */)
    require.Error(t, err)
    require.Equal(t, int64(0), uploadCount.Load(),
        "no upload HTTP requests should have been issued when any block fails verification")
}
```

### Kingpin flag wiring with cross-flag validation

```go
// pkg/mimirtool/commands/backfill.go (additions, sketch):
func (c *BackfillCommand) Register(app *kingpin.Application, envVars EnvVarNames, logConfig *LoggerConfig) {
    cmd := app.Command("backfill", "Upload Prometheus TSDB blocks to Grafana Mimir compactor.")

    // ... existing flags (--address, --id, --user, --key, --extra-headers, --tls-*, --sleep-time) ...

    cmd.Flag("dry-run", "Run verification without uploading any blocks.").
        Default("false").
        BoolVar(&c.dryRun)
    cmd.Flag("skip-chunk-verification", "Reduce verification depth: skip chunk-checksum walk.").
        Default("false").
        BoolVar(&c.skipChunkVerification)
    cmd.Flag("full-report", "Aggregate all verification failures instead of stopping at the first.").
        Default("false").
        BoolVar(&c.fullReport)
    cmd.Flag("verify-concurrency", "Number of blocks to verify in parallel. Defaults to min(GOMAXPROCS, 4).").
        Default("0"). // 0 = auto
        IntVar(&c.verifyConcurrency)

    cmd.Validate(func(_ *kingpin.CmdClause) error {
        if c.skipChunkVerification && c.fullReport {
            return fmt.Errorf("--full-report requires deep analysis and cannot be combined with --skip-chunk-verification")
        }
        return nil
    })

    cmd.Action(func(_ *kingpin.ParseContext) error {
        return c.backfill(logConfig.Logger())
    })
    cmd.Arg("block-dir", "block to upload").Required().SetValue(&c.blocks)
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Mimirtool uploads blocks with only `meta.json` version + file-stat checks | Pre-flight pluggable verifier (this phase) | Phase 1 (PR TBD) | Fail fast client-side; no bandwidth wasted on malformed blocks; single-UTC-day enforcement prevents silent compactor load multiplication. |
| Semaphore channel for bounded parallelism | `errgroup.SetLimit(n)` | `golang.org/x/sync` ≥ v0.3 (now in Mimir) | Cleaner API, fewer lines. Use for all new bounded-concurrency code. |
| Pre-Mimir-v2 index format (v1) | Index v2 required | `[VERIFIED: CHANGELOG #13815]` | `block.VerifyBlock`'s `HealthStats.UnsupportedIndexFormat()` enforces v2-only. Our verifier inherits this: uploading a v1-index block will fail well-formedness. Good. |
| Per-chunk CRC32 lazy verification | `-blocks-storage.bucket-store.verify-chunks-ratio=1/128` on store-gateway (1-in-128 sampling) | `[CITED: CHANGELOG #13151]` | Server-side is optimistic. Our client-side deep mode does full CRC32 on every chunk — that's an opt-out (`--skip-chunk-verification`), not opt-in. The intent is "pay the cost once, upload-time, once per block, instead of amplifying it across every query". |

**Deprecated/outdated:**
- The REQUIREMENTS.md CHECK-02/CHECK-03 wording (`MaxTime - MinTime == 24h exactly` + `MinTime % DAY == 0`) — superseded by SPEC.md §6 "same calendar UTC day, accepting sparse blocks".
- `pkg/storage/tsdb/block.Repair` exists (`index.go:310`) — out of scope; we verify, we don't repair. Flag for the planner: do not accidentally call it.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `make reference-help doc` covers `cmd/mimirtool/` flag docs as well as the main mimir binary's help text. | Project Constraints | If it doesn't, we may need a separate mimirtool-specific regeneration step. Low risk — the worst case is "CI catches it and we add a Makefile invocation". |
| A2 | Kingpin `cmd.Validate(...)` fires AFTER all flag `BoolVar`/`StringVar` targets are populated but BEFORE the `Action`. | Pattern 3 | If Validate fires before BoolVars are populated, the mutual-exclusion check misfires. Low risk — documented behavior in kingpin source comments; verifiable with a 3-line sanity test. Fallback: post-parse check in `c.backfill()`. |
| A3 | External importers of `pkg/mimirtool/client.Backfill` exist and break on a signature change. | Pitfall 5 | Low risk if we take option (A) — the additive-method approach. If we take (B) and nobody cared, we overpaid in CHANGELOG verbosity. |
| A4 | `block.Meta`'s `MinTime`/`MaxTime` are milliseconds since Unix epoch UTC. | Pattern in general + Pitfall 2 | `[VERIFIED: vendor/github.com/prometheus/prometheus/tsdb/block.go:178-181]` (claim is actually verified, not assumed — moving out of Assumptions). See block_generator.go comment `MaxTime: specs.MaxTime() + 1, // Not included.` |

All other claims in this document are verified against in-tree code or vendored dependency source. Planner should treat A1, A2, A3 as small-risk items worth a quick confirmation during Wave 0.

## Open Questions (RESOLVED)

All four open questions were resolved during planning via CONTEXT.md locked decisions and the planner's lock-in step. Resolution notes are inline with each question for auditability.

1. **Which approach for the Backfill() signature change?**
   - What we know: The method is public; at least one internal caller (`commands/backfill.go`); unknown external callers.
   - What's unclear: Preference for (A) new method + default-delegating wrapper vs (B) outright break with CHANGELOG `[CHANGE]`.
   - Recommendation: Default to (A). It's low cost, breaks nothing, and keeps the diff focused on new capability. The planner may flip to (B) with a one-line rationale.
   - **RESOLVED:** Option (A) — additive method `BackfillWithOptions(ctx, blocks, sleepTime, verifier, dryRun)`. The legacy `Backfill(ctx, blocks, sleepTime)` is preserved as a delegating wrapper that calls `BackfillWithOptions` with a no-op verifier (zero block checks registered) and `dryRun=false`. Zero public API break. See Plan 01-04.

2. **Which of Pitfall 3's three deep-UTC options does the planner adopt?**
   - What we know: All three produce correct results; they differ on IO cost and code complexity.
   - What's unclear: How much we value code clarity vs peak deep-verify IO cost.
   - Recommendation: Option 1 (fold deep-UTC into `WellFormedVerifier` by passing UTC-day bounds). Simpler code, one walk per block. The only downside is that the failing block now has a less-specific error message ("chunks outside UTC day" instead of "chunks outside block range"). Acceptable tradeoff.
   - **RESOLVED:** Deviation from the researcher's Option 1 per D-06 locked decision. Two distinct verifier structs (`WellFormedVerifier` and `SingleUTCDayVerifier`) preserve per-check naming in Report entries and log lines ("well-formed" vs "single-utc-day"). Deep-mode `SingleUTCDayVerifier` runs `block.VerifyBlock(ctx, log, dir, utcDayStart, utcDayEnd, checkChunks)` as a separate postings walk (approximately 1.3× IO cost over the optimal single-walk case, per-block — not 2×, because chunk-segment disk cache hits across the two calls). The IO cost is documented in the Plan 01-03 objective; the clarity win in logs/reports justifies it. See Plan 01-03.

3. **Does the "summary log line on `--full-report`" list every failing check, or just counts?**
   - What we know: D-16 specifies one `level=error` line per failing check PLUS a summary line.
   - What's unclear: Is the summary expected to enumerate `{block: [checks]}` pairs inline, or just report `total_blocks=5, failures=7, outcome=all_checked`?
   - Recommendation: Summary carries counts only; the per-check error lines already enumerate. That keeps the summary grep-friendly and short.
   - **RESOLVED:** Counts only. The `Verifier.Run` summary log line emits `total_blocks`, `failed_blocks`, `failures`, `outcome` (values: `all_checked` | `aborted`). No per-block enumeration in the summary line — individual failures are already carried in per-check `level=error` log lines during the run, and `Report.Err()` returns a summary-only string (does NOT enumerate). This is reinforced by checker review WARNING 3, which flagged unbounded error strings. See Plan 01-01.

4. **What does `--verify-concurrency=1` mean?**
   - What we know: `errgroup.SetLimit(1)` gives strict serial execution.
   - What's unclear: Do we want `0` to mean "auto-detect" and `1` to mean "literally one", or do we want `0` to mean "disabled" (crash on zero)?
   - Recommendation: `0` = auto (default); `1` = single-threaded; negative = error at flag parse. Document in the flag help string. Matches `pkg/blockbuilder/tsdb.go:507` conditional (`if ShipConcurrency > 0`).
   - **RESOLVED:** `0` = auto (`min(runtime.GOMAXPROCS(0), 4)` at `Verifier.Run` time); `1` = strict serial execution via `errgroup.SetLimit(1)`; negative values are clamped to 0 by `WithConcurrency` (no parse-time error — saves a kingpin validator). Documented in the flag help string. See Plans 01-01 and 01-05.

## Environment Availability

This phase has no external service dependencies. Everything needed is in-tree or vendored.

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Go toolchain | Building mimirtool | ✓ | 1.25.9 (from `go.mod:1`) | — |
| `make` + build container | `make format`, `make test`, `make lint` | ✓ | per `mimir-build-image/` | — |
| `./tools/github-next-pr-number.sh` | Changelog PR number | ✓ | — | Manual lookup in GH UI if script fails |
| Docker | Integration tests in `integration/` | ✓ | — | Skip — this phase's tests are pure unit + httptest, no Docker required |
| `block.VerifyBlock` / `GenerateBlockFromSpec` | All verifier + test code | ✓ | in-tree | — |
| kingpin `CmdClause.Validate` | Mutual-exclusion flag check | ✓ | vendored | Post-parse check in Action handler |
| `errgroup.SetLimit` | Bounded concurrency | ✓ | vendored (via `golang.org/x/sync`) | Semaphore channel (more code) |

No missing dependencies, no blocking.

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | Go `testing` (stdlib) + `github.com/stretchr/testify` v1.11.1 |
| Config file | none (standard `go test`) |
| Quick run command | `go test ./pkg/mimirtool/backfill/verify/... ./pkg/mimirtool/client/... ./pkg/mimirtool/commands/...` |
| Full suite command | `make test` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| FRAMEWORK-01 | Pluggable verifier framework exists; adding a new check doesn't modify existing ones | Unit (inline test doubles) | `go test ./pkg/mimirtool/backfill/verify/ -run TestFrameworkPluggability -x` | ❌ Wave 0 |
| FRAMEWORK-02 | Framework supports both per-block AND batch-level checks | Unit (dummy `BatchVerifier` implemented inside `_test.go`) | `go test ./pkg/mimirtool/backfill/verify/ -run TestBatchVerifierSeam -x` | ❌ Wave 0 |
| FRAMEWORK-03 | Verification runs before upload loop in `MimirClient.Backfill()` | Integration (`httptest.NewServer`) | `go test ./pkg/mimirtool/client/ -run TestBackfill_VerificationRunsBeforeUpload -x` | ❌ Wave 0 |
| FRAMEWORK-04 | Failure reporting identifies block ULID + check name + aggregated per-block+per-check | Unit (table-driven) | `go test ./pkg/mimirtool/backfill/verify/ -run TestVerificationReport -x` | ❌ Wave 0 |
| CHECK-01 (well-formed) | Deep + medium modes correctly classify valid / index-mangled / chunk-corrupted / checksum-mismatch blocks | Unit (table-driven w/ `GenerateBlockFromSpec` + targeted corruption) | `go test ./pkg/mimirtool/backfill/verify/ -run TestWellFormedVerifier -x` | ❌ Wave 0 |
| CHECK-02 + CHECK-03 (single UTC day) | Header check: `MinTime`/`MaxTime-1` on same UTC day. Deep check: sample range too. Table covers aligned 24h / sparse / 2-hour / midnight-crossing / 12h-offset / empty-block. | Unit (table-driven) | `go test ./pkg/mimirtool/backfill/verify/ -run TestSingleUTCDayVerifier -x` | ❌ Wave 0 |
| INT-01 | Verify failure → zero `/api/v1/upload/block/...` requests + non-zero exit | Integration (`httptest.NewServer` counting uploads) | `go test ./pkg/mimirtool/client/ -run TestBackfill_VerificationFailureSkipsUpload -x` | ❌ Wave 0 |
| INT-01 (dry-run variant) | `--dry-run` with valid batch → zero uploads, exit 0 | Integration (`httptest` + flag exercise via `BackfillCommand`) | `go test ./pkg/mimirtool/commands/ -run TestBackfillCommand_DryRun -x` | ❌ Wave 0 |
| INT-02 | go-kit/log output matches existing backfill log conventions (level, field shape) | Unit (capture log output via `log.NewLogfmtLogger(buf)`, assert on rendered line) | `go test ./pkg/mimirtool/backfill/verify/ -run TestVerifierLogFormat -x` | ❌ Wave 0 |
| Flag mutual-exclusion | `--skip-chunk-verification --full-report` rejected at parse | Unit (direct `kingpin.Application` parse) | `go test ./pkg/mimirtool/commands/ -run TestBackfillCommand_FlagExclusion -x` | ❌ Wave 0 |
| No-escape-hatch (SPEC §7) | Static check: no flag name matching `/-(skip|no)-?verif|-force/` other than `--skip-chunk-verification` | Unit (grep-style or kingpin flag enumeration) | `go test ./pkg/mimirtool/commands/ -run TestBackfillCommand_NoBypass -x` | ❌ Wave 0 |

### Sampling Rate

- **Per task commit:** `go test ./pkg/mimirtool/backfill/verify/... ./pkg/mimirtool/client/... ./pkg/mimirtool/commands/... -count=1` (~5–15s)
- **Per wave merge:** `make test` (full Mimir unit suite, ~several minutes)
- **Phase gate (`/gsd-verify-work`):** `make format && make lint && make test` all green; `make reference-help doc` produces no diff.

### Wave 0 Gaps

All test files are new. The planner's Wave 0 must create these shells:

- [ ] `pkg/mimirtool/backfill/verify/verify.go` — types (`Verifier`, `BlockVerifier`, `BatchVerifier`, `Option`, `Report`), `NewVerifier`
- [ ] `pkg/mimirtool/backfill/verify/verify_test.go` — framework-level tests (FRAMEWORK-01, -02, -04, INT-02 log format)
- [ ] `pkg/mimirtool/backfill/verify/wellformed.go` + `wellformed_test.go` — CHECK-01
- [ ] `pkg/mimirtool/backfill/verify/singleutcday.go` + `singleutcday_test.go` — CHECK-02/03 (SPEC §6 semantics)
- [ ] `pkg/mimirtool/client/backfill_test.go` — NEW; INT-01 integration via `httptest`
- [ ] `pkg/mimirtool/commands/backfill_test.go` — NEW; dry-run + flag-exclusion + no-bypass tests
- [ ] Shared test helper (inline, or in a small `testhelper_test.go` within `verify/`) to generate valid and corrupted blocks from `GenerateBlockFromSpec` + file truncation.

No framework install needed — testify is already in `go.mod`, and `block.GenerateBlockFromSpec` is in-tree.

## Security Domain

`security_enforcement` is not specified; treating as enabled by default.

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | mimirtool already authenticates to Mimir via `--user`, `--key`, `--id`, mTLS (unchanged by this phase). |
| V3 Session Management | no | CLI tool; no sessions. |
| V4 Access Control | no | Tenant isolation is enforced server-side via `X-Scope-OrgID` (set by `--id`, unchanged). |
| V5 Input Validation | yes | Verifier is itself an input-validation layer. But we read blocks from a local filesystem path the user supplies — validate that `blockDir` is within a safe root? Out of scope: SPEC is silent; mimirtool already trusts local paths (`commands/backfill.go:27-37` only checks that the path is a directory). No change to that posture. |
| V6 Cryptography | no | No cryptographic operations in this phase; chunk CRC32 validation is integrity, not confidentiality, and `block.VerifyBlock` handles it. |

### Known Threat Patterns for Mimir + Mimirtool

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Malicious block upload to compactor (craft a block that triggers bugs in the server-side TSDB loader) | Tampering | Server-side validation already exists (`pkg/compactor/block_upload.go`). This phase adds a client-side pre-flight — defense in depth. Do NOT claim this phase replaces server-side validation. |
| Path traversal via `--block-dir` | Tampering / Info Disclosure | Already mitigated: `os.Stat` + `IsDir` check in `commands/backfill.go:27-37`. No new attack surface. |
| yoloString cross-tenant leak via verifier retaining label strings | Info Disclosure | Not applicable today (verifier doesn't retain label strings from shared pools). Documented above as Anti-Pattern #2 to prevent future regression. |
| Denial-of-service via giant block (OOM on deep verify) | DoS | Deep verify is bounded by the concurrency flag and by the block's own size. Users run mimirtool on their own machines; OOM is a user-visible local failure, not a cluster incident. `--skip-chunk-verification` is the documented escape valve. |
| Timing side-channel in chunk checksum verification | Info Disclosure | Not applicable — CRC32 checksums are not secrets; the signal is "block integrity", not auth. |

No new cryptographic primitives, no new authentication paths, no new network-facing endpoints. Security review surface is minimal.

## Sources

### Primary (HIGH confidence)

- `pkg/storage/tsdb/block/index.go:41-48` — `VerifyBlock` signature and body
- `pkg/storage/tsdb/block/index.go:50-171` — `HealthStats` struct and error-categorization methods (`AnyErr`, `CriticalErr`, `UnsupportedIndexFormat`, `Issue347OutsideChunksErr`, `OutOfOrderLabelsErr`, `OutOfOrderChunksErr`)
- `pkg/storage/tsdb/block/index.go:177-299` — `GatherBlockHealthStats` full implementation including the ctx-usage gap (postings walk doesn't re-check ctx)
- `pkg/storage/tsdb/block/block_generator.go:70-187` — `GenerateBlockFromSpec` fixture factory
- `pkg/storage/tsdb/block/block_generator.go:192-307` — `CreateBlock` helper (alternative)
- `pkg/storage/tsdb/block/meta.go:73-80, 229-264` — `Meta` struct and `ReadMetaFromDir`
- `pkg/mimirtool/client/backfill.go:1-242` — entire existing backfill upload path
- `pkg/mimirtool/commands/backfill.go:1-114` — existing command registration and flag pattern
- `pkg/mimirtool/backfill/backfill.go:1-20` — provenance header style (cortex-tools fork marker)
- `pkg/mimirtool/client/rules_test.go:19-25` — `httptest.NewServer` pattern
- `pkg/blockbuilder/tsdb.go:506-509` — canonical errgroup + SetLimit pattern
- `pkg/usagetracker/tracker_store_snapshot.go:67` — `min(len(shards), runtime.GOMAXPROCS(0))` concurrency default pattern
- `pkg/compactor/compactor_test.go:1311` — `GenerateBlockFromSpec` test usage
- `vendor/github.com/alecthomas/kingpin/v2/cmd.go:256` — `CmdClause.Validate` API
- `CHANGELOG.md` lines 40–65 — recent `[FEATURE]` entry format
- `.planning/codebase/CONVENTIONS.md`, `TESTING.md`, `CONCERNS.md`, `STRUCTURE.md` — Mimir-wide conventions
- `pkg/CLAUDE.md` (system-reminder injection) — "No globals", config/flag naming, metric registration
- `CLAUDE.md` (root, user personal mentor) — commit/git/lint discipline

### Secondary (MEDIUM confidence)

- `vendor/github.com/prometheus/prometheus/tsdb/block.go:178-181` — `BlockMeta.MinTime`/`MaxTime` field semantics (half-open, milliseconds UTC). Cross-referenced with `block_generator.go:173` comment `// Not included.` for consistency.

### Tertiary (LOW confidence)

- None. Every claim in this document is backed by primary source references inside the repo or its vendored tree.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — everything already in-tree or vendored; no external dependency guesswork.
- Architecture: HIGH — direct `VerifyBlock` wrap is the obvious design; test fixtures are a known quantity.
- Pitfalls: HIGH — each pitfall is grounded in a specific code line. The in-flight-cancellation limitation in particular is a read-the-code finding, not speculation.
- Flag mutual-exclusion via `cmd.Validate`: MEDIUM — kingpin API is stable, but we'd be the first caller in Mimir. Verify during implementation with a smoke test; fallback is the in-Action post-parse check.
- Signature-change blast radius on `MimirClient.Backfill`: MEDIUM — unknown external importers, mitigation (additive method) is cheap.

**Research date:** 2026-04-22
**Valid until:** 2026-05-22 (30 days — stable in-tree primitives, no fast-moving upstream dependencies)
