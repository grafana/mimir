# Phase 1: Backfill Pre-Verification — Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-22
**Phase:** 01-backfill-pre-verification
**Areas discussed:** Package & interface shape, Check registration, CLI flag surface, Failure reporting & parallelism

---

## Package & Interface Shape

### Q1: Where should the verifier code live?

| Option | Description | Selected |
|--------|-------------|----------|
| pkg/mimirtool/backfill/verify/ (Recommended) | Nested under existing backfill package; related code co-located. | ✓ |
| pkg/mimirtool/verify/ | Sibling package. Cleaner scope but fragments mimirtool layout. | |
| pkg/mimirtool/client/verify/ | Co-locate with caller. Awkward — client/ is HTTP, not block analysis. | |

**User's choice:** pkg/mimirtool/backfill/verify/ (Recommended)

### Q2: How should the verifier contract be split?

| Option | Description | Selected |
|--------|-------------|----------|
| Two interfaces: BlockVerifier + BatchVerifier (Recommended) | Separate contracts; matches SPEC's extension seam. | ✓ |
| One combined interface with both methods | Less boilerplate but asymmetric. | |
| Slice-only: every verifier takes []Block | Simplest signature; loses per-block/batch semantic. | |

**User's choice:** Two interfaces: BlockVerifier + BatchVerifier (Recommended)

### Q3: What does each verifier receive?

| Option | Description | Selected |
|--------|-------------|----------|
| Path + Meta struct (Recommended) | `(blockDir string, meta block.Meta)`. | ✓ |
| Pre-opened tsdb.Block handle | Avoids redundant opens but most checks don't need the handle. | |
| Just blockDir string | Simplest but duplicates meta parsing. | |

**User's choice:** Path + Meta struct (Recommended)

---

## Check Registration

### Q1: How do verifiers get wired in?

| Option | Description | Selected |
|--------|-------------|----------|
| Static slice, explicit constructor (Recommended) | Go-idiomatic, zero magic. | |
| Functional options pattern | Matches Mimir service constructors; highly composable. | ✓ |
| init() auto-registration | Fights `pkg/CLAUDE.md` no-globals rule. | |

**User's choice:** Functional options pattern
**Notes:** User preferred composability over the stricter static-slice approach.

### Q2: Per-block failure policy?

| Option | Description | Selected |
|--------|-------------|----------|
| Run all checks on all blocks, collect everything (Recommended) | Maximizes signal per run. | |
| Fail-fast per block | Stop on first failure per block, move to next block. | |
| Fail-fast whole run | Abort entirely at first failure. | |

**User's choice:** Custom — "Let's make fast-fail optional, with two levels: fail-fast the whole run or full checks on all blocks with a full report"
**Notes:** User wanted both behaviors available. Resolved with the Q3-area default profile question to avoid conflicting with SPEC.md depth default (see CLI Flag Surface, Q-defaults below).

### Q3: Concrete v1 verifier types?

| Option | Description | Selected |
|--------|-------------|----------|
| Two: WellFormedVerifier + SingleUTCDayVerifier (Recommended) | Clean separation, distinct failure categories. | ✓ |
| One: BlockSanityVerifier that does both | Fewer files but harder per-check diagnostics. | |

**User's choice:** Two: WellFormedVerifier + SingleUTCDayVerifier (Recommended)

---

## CLI Flag Surface

### Q1: Name for verify-without-upload flag?

| Option | Description | Selected |
|--------|-------------|----------|
| --dry-run (Recommended) | Widely recognized Unix convention. | ✓ |
| --check-only | Explicit but less idiomatic. | |
| --verify-only | Clear but non-standard. | |

**User's choice:** --dry-run (Recommended)

### Q2: Name for medium-depth opt-in?

| Option | Description | Selected |
|--------|-------------|----------|
| --skip-chunk-verification (Recommended) | Self-documenting. | |
| --medium-verify | Internal jargon. | |
| --fast-verify | Vague. | |
| --no-deep-verify | Confusing with triple-negatives. | |

**User's choice:** Custom — "The default should be the fast fail, so the optional flag is the full check: --verify-all"
**Notes:** User surfaced a confusion between the depth axis and the failure-policy axis; resolved in the follow-up defaults question.

### Q3: Name for abort-on-first-failing-block flag?

**User's choice:** Custom — "Oops I think maybe this is what I thought I was answering with the previous question? We might want to talk about the depth flag vs fail-fast flag again"
**Notes:** User caught own confusion between depth and failure-policy axes. Follow-up disambiguation question presented.

### Q4: Flag location (command vs subcommand)?

| Option | Description | Selected |
|--------|-------------|----------|
| Directly on `mimirtool backfill` (Recommended) | Matches existing CLI shape. | ✓ |
| Subcommand: `mimirtool backfill verify` | Splits muscle memory unnecessarily. | |

**User's choice:** Directly on `mimirtool backfill` (Recommended)

### Q5 (disambiguation): Which default profile?

| Option | Description | Selected |
|--------|-------------|----------|
| Thorough default (SPEC as written) | Deep + full-report. | |
| Fast default, --verify-all for thorough | Contradicts SPEC deep-default. | |
| Hybrid: deep default + fail-fast default | Two independent flags, two independent defaults. | ✓ |
| Full-report + deep default, --quick convenience flag | SPEC-compliant, one-flag shortcut. | |

**User's choice:** Hybrid — "By default we dive deep into each chunk to make sure it's good, and as soon as we find a failure we abort. So the depth flag indeed can be --skip-chunk-verification. And then --full-report for full failure reports. I think --full-report necessarily implies a deep analysis, so we can't let users try to both skip chunk verification AND select a full report"
**Notes:** Locked the final flag model. `--skip-chunk-verification` + `--full-report` = rejected at flag-parse. This means SPEC #3 acceptance criterion only holds under `--full-report` — flagged for the planner in CONTEXT.md.

---

## Failure Reporting & Parallelism

### Q1: Error aggregation shape?

| Option | Description | Selected |
|--------|-------------|----------|
| Structured VerificationReport type (Recommended) | Testable, inspectable, rich messages. | ✓ |
| errors.Join of per-block errors | Go stdlib idiom; loses per-check granularity. | |
| Hybrid: report struct + sentinel error | Best ergonomics, slightly more surface. | |

**User's choice:** Structured VerificationReport type (Recommended)

### Q2: Log format for failures?

| Option | Description | Selected |
|--------|-------------|----------|
| One go-kit/log line per failure + aggregated summary (Recommended) | Grep-friendly; matches client/backfill.go. | ✓ |
| One line per block, checks concatenated | Denser but harder to parse. | |
| Structured JSON payload | Outlier in mimirtool logging style. | |

**User's choice:** One go-kit/log line per failure + aggregated summary (Recommended)

### Q3: Sequential or parallel?

| Option | Description | Selected |
|--------|-------------|----------|
| Sequential (Recommended) | Simple, predictable, ordered logs. | |
| Parallel with errgroup + concurrency limit | Speeds large batches; bounded workers. | ✓ |
| Parallel, unbounded | Dangerous on large batches. | |

**User's choice:** Parallel with errgroup + concurrency limit
**Notes:** User prioritized throughput over log-ordering simplicity. Planner to pick concurrency flag name and default (suggested: `--verify-concurrency`, default `min(GOMAXPROCS, 4)`).

### Q4: Progress UI?

| Option | Description | Selected |
|--------|-------------|----------|
| Silent + per-block info logs (Recommended) | Matches existing mimirtool output. | ✓ |
| Progress bar (mpb or similar) | Adds dep; mimirtool doesn't use elsewhere. | |
| Silent unless failure | Leaves users wondering "is it hung?". | |

**User's choice:** Silent + per-block info logs (Recommended)

---

## Claude's Discretion

- Specific Go type names (`Report` vs `VerificationReport`, option type naming).
- Exact function signatures inside the new `verify` package.
- Test fixture location (suggested `pkg/mimirtool/backfill/verify/testdata/`).
- Exit code semantics beyond zero/non-zero.
- Whether `BatchVerifier` ships with any concrete implementation in v1 (recommendation: empty for v1, door open for v2).

## Deferred Ideas

None raised during discussion beyond what SPEC.md already covers. SPEC.md's v2 entries (duplicate-day, overlap, cardinality preflight) remain the backlog for this project.

## Open Flag Noted

The fail-fast default plus SPEC.md requirement #3 ("aggregated failure reporting") creates a small tension. CONTEXT.md documents the resolution: SPEC #3 acceptance criterion reads as "capability exists via `--full-report`". If the planner disagrees, re-run `/gsd-spec-phase 1` to tighten SPEC before implementation.
