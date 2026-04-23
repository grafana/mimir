# Phase 1: Backfill Pre-Verification — Context

**Gathered:** 2026-04-22
**Status:** Ready for planning

<domain>
## Phase Boundary

Add a pluggable client-side verifier to `mimirtool backfill` that runs over every block before any upload begins, fails the whole batch when any block is malformed or not confined to a single calendar UTC day, and provides a `--dry-run` mode and a `--full-report` mode. Verifier lives in a new `pkg/mimirtool/backfill/verify/` package and is invoked from `MimirClient.Backfill()` before its upload loop at `pkg/mimirtool/client/backfill.go:25-53`.

Net change in user-visible behavior: three new flags on `mimirtool backfill` (`--dry-run`, `--skip-chunk-verification`, `--full-report`), one new optional concurrency flag (name TBD in planning), and pre-flight verification that rejects 2-hour blocks, blocks crossing UTC midnight, and structurally broken blocks before any bytes hit the network.

</domain>

<spec_lock>
## Requirements (locked via SPEC.md)

**9 requirements are locked.** See `01-SPEC.md` for full requirements, boundaries, and acceptance criteria.

Downstream agents MUST read `01-SPEC.md` before planning or implementing. Requirements are not duplicated here.

**In scope (from SPEC.md):**
- Pluggable verifier framework in `pkg/mimirtool/` with per-block and batch-level interfaces.
- Well-formed block verifier with deep (default) and medium modes, reusing upstream primitives.
- Single-UTC-day verifier (header-based in medium; header + sample-range in deep).
- Integration into `MimirClient.Backfill()` so verification runs over the full batch before any upload begins.
- CLI flag for medium depth.
- CLI `--dry-run` flag.
- Aggregated per-block, per-check failure reporting via go-kit/log.
- Unit tests for each verifier and for `MimirClient.Backfill()` rejection behavior.
- `[FEATURE]` CHANGELOG entry with a PR number from `./tools/github-next-pr-number.sh`.

**Out of scope (from SPEC.md):**
- Server-side validation changes.
- Block repair, rewriting, or merging.
- Changes to `pkg/mimirtool/backfill/backfill.go` (block creation path).
- Removing `GetBlockMeta`'s existing checks.
- Escape hatches of any kind.
- Configurable block duration.
- Batch-level duplicate-day and overlap checks (v2 via extension seam).
- Tenant-scoped cardinality / series-limit preflight.

**Amendment flag for planner/verifier:** SPEC.md requirement #3 ("Aggregated failure reporting") carries an acceptance criterion ("3 of 5 failing blocks each report multiple check failures") that only holds under the `--full-report` flag. Under the new fail-fast default, only the first failing block is reported. Treat SPEC.md acceptance #3 as specifying a capability that `--full-report` exposes, not the default behavior. If this reading is wrong, re-run `/gsd-spec-phase 1` before planning.

</spec_lock>

<decisions>
## Implementation Decisions

### Package Layout & Interfaces

- **D-01:** The verifier module lives at `pkg/mimirtool/backfill/verify/` — nested under the existing `backfill` package alongside the block-creation code at `pkg/mimirtool/backfill/backfill.go`.
- **D-02:** Two separate interfaces. `BlockVerifier` runs per-block checks (`Verify(ctx, blockDir string, meta block.Meta) error`). `BatchVerifier` runs cross-block checks (`Verify(ctx, blocks []BlockRef) error` — final signature TBD in planning, but must accept a slice). The two interfaces are independent to keep the extension seam clean for future duplicate-day and overlap checks.
- **D-03:** Per-block verifier input is `(blockDir string, meta block.Meta)` using Mimir's existing `pkg/storage/tsdb/block.Meta`. Each verifier decides whether it needs to open the block itself — most won't, since `VerifyBlock` accepts the dir directly.

### Registration & Composition

- **D-04:** Functional options pattern for assembling the verifier. `verify.NewVerifier(logger, opts ...Option)` where options include `WithBlockCheck(BlockVerifier)`, `WithBatchCheck(BatchVerifier)`, `WithMode(Deep|Medium)`, `WithFailFast(bool)`, and a concurrency option (name TBD). This matches the style of service constructors elsewhere in Mimir and composes cleanly.
- **D-05:** **No `init()` auto-registration and no global registry.** Per `pkg/CLAUDE.md` ("No global variables"), verifier wiring happens explicitly in the constructor called from `commands/backfill.go`. Adding a new check means adding a `With*` helper and a constructor call — trivial, testable, no hidden deps.
- **D-06:** Two concrete verifier implementations ship in v1: `WellFormedVerifier` and `SingleUTCDayVerifier`. Each is a distinct struct so users see each check by name in logs and tests can stub them independently.

### Verification Primitive Reuse

- **D-07:** `WellFormedVerifier` wraps `pkg/storage/tsdb/block.VerifyBlock(ctx, logger, blockDir, meta.MinTime, meta.MaxTime, checkChunks bool)`. `checkChunks=true` in deep mode, `false` in medium mode. This is Mimir's internal primitive — already tested, covers index integrity and chunk checksums, and matches our depth split one-for-one. No new verification code needs to be written for well-formedness; we're a thin wrapper.
- **D-08:** `SingleUTCDayVerifier` computes the UTC-day bound from `meta.MinTime` and `meta.MaxTime`: pass iff `meta.MinTime / 86_400_000 == (meta.MaxTime - 1) / 86_400_000`. **Revised 2026-04-23 during execution review:** Originally specified a deep path that also called `block.VerifyBlock` against the UTC-day window to catch chunks outside the boundary. That deep path was removed. Reason: `WellFormedVerifier` already calls `block.VerifyBlock` against meta's own `[MinTime, MaxTime)` range, and any chunk that lies outside the UTC-day window either (a) lies outside meta's range too — in which case WellFormedVerifier flags it via `OutsideChunks` — or (b) lies inside meta's range but outside the UTC-day window, which is only possible when meta itself already crosses midnight, and the header arithmetic already rejects that. SingleUTCDayVerifier is now pure header math; the Mode parameter was dropped from its constructor. Benefit: no duplicate `block.VerifyBlock` work, simpler verifier, clearer separation of responsibilities.

### CLI Surface

- **D-09:** Flags attach directly to `mimirtool backfill` (not a subcommand). Command entry at `pkg/mimirtool/commands/backfill.go:47-103`.
- **D-10:** `--dry-run` — runs verification, exits 0 on all-pass or non-zero otherwise, issues zero HTTP requests to `/api/v1/upload/block/...`.
- **D-11:** `--skip-chunk-verification` — opts depth down to medium (no chunk-checksum walk). Default is deep.
- **D-12:** `--full-report` — switches failure policy from fail-fast to aggregate-all. Default is fail-fast (abort verification at first failing block).
- **D-13:** `--skip-chunk-verification` + `--full-report` combined is rejected at flag-parse with a clear error: "--full-report requires deep analysis and cannot be combined with --skip-chunk-verification". Rationale: a full report with no chunk checksums misses the failures users asked for a full report to surface.
- **D-14:** Planner selects a concurrency flag name and default (suggested default: `min(GOMAXPROCS, 4)`; suggested name: `--verify-concurrency`). This is one of the few details explicitly left for planning to land alongside implementation.

### Failure Reporting

- **D-15:** The framework builds a `VerificationReport` struct with per-block, per-check failure entries. `MimirClient.Backfill()` consumes the report, emits log lines, and returns the aggregated error.
- **D-16:** Log format: one go-kit/log `level=error` line per failing check. Fields: `block=<ULID>`, `check=<check-name>`, `msg="<human description>"`, plus check-specific fields (e.g. `min_time`, `max_time`, `chunk_file`). At end of run, a single `level=info` summary line with total blocks, total failures, and outcome (`aborted` for fail-fast, `all_checked` for full-report).
- **D-17:** Log format for success: one `level=info block=<ULID> msg="verifying block"` line when verification begins for a block, one `msg="verified"` line at end. No bar, no TTY animation. Quiet enough for logs, noisy enough that users know the process hasn't hung during a long deep-verify.

### Parallelism

- **D-18:** Parallel per-block verification via `golang.org/x/sync/errgroup` with a bounded concurrency limit (flag from D-14). `errgroup.Group` + semaphore or `errgroup.SetLimit()` (planner picks). Cross-block batch checks run after all per-block checks settle.
- **D-19:** Parallelism interaction with fail-fast: in fail-fast mode, the first worker that observes a failing block calls `cancel()` on the errgroup context; peer workers see `ctx.Done()` and abort in-flight `VerifyBlock` calls. The framework may still emit a few stray failures that were already in flight — acceptable, but the summary log reflects the first-abort block as the trigger.
- **D-20:** In full-report mode, all workers run to completion regardless of failures. Only exits abort the errgroup.

### Claude's Discretion

- Specific import aliases, internal type names (`Report` vs `VerificationReport` etc.), and exact function signatures within `pkg/mimirtool/backfill/verify/` — planner/executor choose during implementation, consistent with Mimir's goimports conventions.
- Whether `BatchVerifier` exists in v1 code (as an empty slice / no implementations) or is introduced later as part of a v2 batch-check implementation. Preference: ship the interface in v1 (tested with a no-op dummy) so v2 work is purely additive.
- Exact upstream function used for deep sample-range inspection in `SingleUTCDayVerifier` — likely `block.GatherBlockHealthStats` or a lower-level index reader; planner verifies which primitive gives the min/max sample time cheaply.
- Test fixture location — `pkg/mimirtool/backfill/verify/testdata/` is the likely home, but planner may reuse existing block fixtures elsewhere in the repo if they exist.
- Exit code semantics beyond "zero on pass, non-zero on fail" — planner decides whether to use distinct codes (e.g. 1 for verification failure, 2 for upload failure).

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase artifacts (locked requirements and discussion decisions)

- `.planning/phases/01-backfill-pre-verification/01-SPEC.md` — Locked requirements, boundaries, acceptance criteria. MUST read before planning.
- `.planning/PROJECT.md` — Project scope, core value, constraints, key decisions.
- `.planning/REQUIREMENTS.md` — Checkable REQ-IDs mapped to Phase 1.
- `.planning/STATE.md` — Project state and current position.

### Code to read before implementing

- `pkg/mimirtool/client/backfill.go` — Upload path and integration point. `MimirClient.Backfill()` at lines 25–53 is where verification inserts; `GetBlockMeta()` at lines 182–242 is the existing meta-parse helper (stays unchanged but informs input signature).
- `pkg/mimirtool/backfill/backfill.go` — Sibling package for block creation; verify/ lives alongside it. Read for provenance-header style and package-docstring conventions.
- `pkg/mimirtool/commands/backfill.go` — CLI wiring at lines 47–103. New flags attach here.
- `pkg/storage/tsdb/block/index.go` — Contains `VerifyBlock(ctx, logger, blockDir, minTime, maxTime, checkChunks bool) error` (the primitive we wrap) and `GatherBlockHealthStats` (richer return). Read the function docs and error categorization (`HealthStats.CriticalErr`, etc.).
- `pkg/storage/tsdb/block/meta.go` — `block.Meta` type used in verifier input signature.

### Project-wide conventions

- `CLAUDE.md` (root) — General contribution rules, yolo-string warnings, changelog scopes, and `./tools/github-next-pr-number.sh`.
- `pkg/CLAUDE.md` — Go coding style, import grouping, "No global variables" (drives D-05), Prometheus metrics registration pattern, config-file/CLI-flag naming conventions.
- `.planning/codebase/CONVENTIONS.md` — Mapped conventions for Mimir.
- `.planning/codebase/TESTING.md` — Testing patterns to follow for unit tests.
- `.planning/codebase/CONCERNS.md` — Includes yolo-string warning; applies to any string extracted from block metadata passed through the verifier.
- `CHANGELOG.md` — Add a `[FEATURE]` entry with a `./tools/github-next-pr-number.sh` PR number.

### No external specs

No external ADRs, PRDs, or design docs exist beyond the artifacts above. Requirements are fully captured in SPEC.md and this CONTEXT.md.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

- **`pkg/storage/tsdb/block.VerifyBlock`** — Mimir's existing block verifier. Signature: `func VerifyBlock(ctx context.Context, logger log.Logger, blockDir string, minTime, maxTime int64, checkChunks bool) error`. `checkChunks` toggles the deep-walk. This eliminates any need to write well-formed checking from scratch.
- **`pkg/storage/tsdb/block.GatherBlockHealthStats`** — Returns a `HealthStats` struct with rich error categorization (`CriticalErr`, `UnsupportedIndexFormat`, `OutOfOrderLabels`, `Issue347OutsideChunks`, `OutOfOrderChunks`, `AnyErr`). Useful for sample-range extraction in the deep `SingleUTCDayVerifier`.
- **`pkg/storage/tsdb/block.Meta`** — Canonical block meta type. Use in the verifier input signature.
- **`GetBlockMeta()` at `pkg/mimirtool/client/backfill.go:182`** — Existing meta-parse helper; stays as-is per SPEC. Verification is layered alongside, not inside, `GetBlockMeta`.
- **`golang.org/x/sync/errgroup`** — Standard Mimir/Go idiom for bounded parallelism. Already a transitive dep.

### Established Patterns

- **Functional options** — Common in Mimir for service constructors (e.g. ingester, distributor). `NewVerifier(logger, opts ...Option)` matches shape.
- **`promauto.With(reg)` metrics registration** — If the verifier ever records metrics (latency, failures), follow `pkg/CLAUDE.md`'s pattern: accept a `prometheus.Registerer` in the constructor, use `promauto.With(reg)`, no globals. Not required for v1 but leave door open.
- **goimports 3-group layout** — stdlib / 3rd-party / `github.com/grafana/mimir/...`. Enforced via `make format`.
- **Provenance headers** — `pkg/mimirtool/backfill/*.go` carries `Provenance-includes-*` headers from cortex-tools. New verifier files in the same package may not need them if the code is net-new, but mimic the SPDX `AGPL-3.0-only` header at minimum.
- **go-kit/log levels** — `level.Info`, `level.Warn`, `level.Error` used throughout `client/backfill.go`. Verifier follows the same pattern.
- **Kingpin flag registration** — `cmd.Flag("name", "description").Default(...).BoolVar(&c.field)` pattern in `commands/backfill.go:54-102`. New flags register the same way.

### Integration Points

- **Call site**: `MimirClient.Backfill()` at `pkg/mimirtool/client/backfill.go:25` — insert verification as a new pass over `blocks []string` before the existing upload loop at line 29. On verification failure, log and return without invoking `backfillBlock()`.
- **CLI flags**: `BackfillCommand.Register()` at `pkg/mimirtool/commands/backfill.go:47` — add `--dry-run`, `--skip-chunk-verification`, `--full-report`, and the concurrency flag. Thread the values down through `MimirClient.Backfill()` (signature change) into `verify.NewVerifier()`.
- **Tests**: Existing `pkg/mimirtool/client/backfill_test.go` (if present — planner to check) is the test integration point. Verifier package gets its own `_test.go` files alongside each verifier.

</code_context>

<specifics>
## Specific Ideas

- **Reuse `block.VerifyBlock` over writing a custom verifier.** Owen's Area 1 feedback plus the discovery that this primitive already takes `checkChunks bool` made this unanimous. Do not reimplement index walks or chunk checksum logic.
- **Fail-fast default + `--full-report` opt-in is deliberate.** Users doing frequent backfills want quick feedback on the first bad block. The tension with SPEC #3 is documented in `<spec_lock>` — planner should treat that acceptance as "capability exists via `--full-report`", not "default behavior".
- **Concurrency default should be conservative.** Deep verification does a lot of IO. A default of `min(GOMAXPROCS, 4)` avoids thrashing storage on machines with 16+ cores while still giving speedup on typical laptops. Planner may revisit.
- **Log a `block=<ULID>` field on every verification log line.** Grep-ability matters — backfills produce long logs, and per-block filtering is essential.

</specifics>

<deferred>
## Deferred Ideas

- **Batch-level checks** — duplicate-UTC-day detection, time-range overlap detection, tenant-scoped cardinality preflight. Already captured in `REQUIREMENTS.md` v2 section and `SPEC.md` out-of-scope. The `BatchVerifier` interface seam enables these; no re-architecture needed in v2.
- **Metrics/observability** — verification counts, failure taxonomy, deep-vs-medium ratios. Not required for v1; if added, follow `pkg/CLAUDE.md` `promauto.With(reg)` pattern.
- **Block repair** — SPEC excluded. If future work wants to auto-repair a specific class of failure (e.g. "24hr block split across midnight" could be refactored via `block.Rewrite`), that's a new phase.
- **Tightening SPEC.md for the fail-fast default** — SPEC #3 acceptance text assumes aggregate reporting. Update SPEC via `/gsd-spec-phase 1` if the reading in `<spec_lock>` amendment flag isn't right.
- **Server-side verification alignment** — server already validates; one day we might want to ensure client and server check the same things so client passes imply server accepts. Separate phase, needs server-code coordination.

</deferred>

---

*Phase: 01-backfill-pre-verification*
*Context gathered: 2026-04-22*
