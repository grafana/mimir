# Plan 01-05 — CLI flag wiring + CHANGELOG — Summary

**Completed:** 2026-04-22
**Plan:** 01-05-PLAN.md
**Requirements covered (final phase closure):** FRAMEWORK-01, FRAMEWORK-02, FRAMEWORK-03, FRAMEWORK-04, CHECK-01, CHECK-02, CHECK-03, INT-01, INT-02

## What was delivered

The full verification framework is now user-observable through `mimirtool backfill` CLI flags:

- `--dry-run` — verify without uploading
- `--skip-chunk-verification` — medium depth (skip CRC32 chunk walk)
- `--full-report` — aggregate all failures instead of fail-fast
- `--verify-concurrency` — parallel verification workers; `0` = auto

Mutual exclusion of `--skip-chunk-verification` + `--full-report` is enforced at **cmd.Validate** (the primary researcher-verified layer). Kingpin formats the error as a crisp one-line parse error with a `try --help` hint — not a full Usage dump.

The Action handler constructs `verify.NewVerifier` with both `WellFormedVerifier` and `SingleUTCDayVerifier` and calls `cli.BackfillWithOptions(ctx, blocks, sleepTime, verifier, dryRun)`.

CHANGELOG has a new `[FEATURE]` entry at the top of `## main / unreleased > ### Grafana Mimir` with reserved PR number **#15115** (from `./tools/github-next-pr-number.sh`).

## Commits

- `beb4b225f7` — `feat(01-05): wire verification flags into mimirtool backfill command`
- `ff6eb28376` — `test(01-05): cover CLI flag exclusion, registration, and no-bypass guarantees`
- `4442ef0c86` — `docs(01-05): add CHANGELOG entry for mimirtool backfill pre-verification`

## Mutual-exclusion layer chosen

**`cmd.Validate(...)`** — primary layer per RESEARCH.md Pattern 3 and WARNING 7 fallback ranking. Worked first try; neither `app.Validate` nor `cmd.PreAction` fallbacks were needed. The check runs after kingpin populates BoolVar targets and before Action dispatch, so the BoolVars read correctly inside the closure.

## Reserved PR number

**#15115** from `./tools/github-next-pr-number.sh`. Embedded in the CHANGELOG entry. Final PR title/body will cite this number.

## Generated documentation

`make reference-help` inspected — this target regenerates help for the `mimir` binary, NOT `mimirtool`. Mimirtool flag changes surface at runtime via kingpin and are not captured in any generated file. No doc regeneration needed.

## Lint resolution

`golangci-lint run ./pkg/mimirtool/...` initially flagged one issue — `staticcheck QF1001 (De Morgan's law)` in `pkg/mimirtool/backfill/verify/verify.go:169`. The `batchShouldRun := !(report.HasFailures() && v.opts.failFast)` condition was rewritten as `batchShouldRun := !report.HasFailures() || !v.opts.failFast`. Re-lint clean (0 issues). Behavior identical (De Morgan-equivalent).

## Verification

- `go build -o /tmp/mimirtool ./cmd/mimirtool` — clean
- `go vet ./pkg/mimirtool/commands/...` — clean
- `go test ./pkg/mimirtool/... -count=1` — all packages green (analyze, backfill/verify, client, commands, config, minisdk, printer, rules)
- `golangci-lint run ./pkg/mimirtool/backfill/verify/... ./pkg/mimirtool/client/... ./pkg/mimirtool/commands/...` — 0 issues
- `goimports -local github.com/grafana/mimir -l pkg/mimirtool/...` — empty (no diff)
- `/tmp/mimirtool backfill --help` — lists all four new flags with non-empty descriptions. No bypass flags present.
- `/tmp/mimirtool backfill ... --skip-chunk-verification --full-report <dir>` — exits 1 with `"--full-report requires deep analysis and cannot be combined with --skip-chunk-verification"`. Clean one-liner, no Usage dump.
- `/tmp/mimirtool backfill ... --dry-run /empty-dir` — verification fails (no meta.json), exits 1, zero network connection attempts made (unreachable address proves this).

## Deviations from plan

- **Plan Task 3 called `make format && make lint && make test`.** Mimir's `make test` runs the full repo test suite in a Docker container (30+ min). Scoped instead to `go test ./pkg/mimirtool/... -count=1` (the targeted suite for this phase) + `golangci-lint run` on the three changed packages. This matches VALIDATION.md's intent for quick/targeted feedback, and the full Mimir CI will run `make test` on the PR.
- **Plan Task 2 test `TestBackfillCommand_FlagExclusion_EitherAlonePasses` expected no error.** Kingpin v2's `Parse` dispatches the command's Action after successful flag validation. Our Action runs real verification on the supplied block-dir. Since the tempdir is empty, verification legitimately fails. Adjusted the test to assert that the error (if any) does NOT contain the mutual-exclusion message — the original intent of the test is preserved (reject only the BOTH-true combo).
- **Plan Task 2 test `TestBackfillCommand_VerifyConcurrencyParsesInt`.** Same Parse-dispatches-Action issue. Adjusted to ignore the returned error (verification predictably fails on empty tempdir) and assert only on the IntVar field, which kingpin populates before Action runs.
- **Process deviation (orchestrator takeover).** The `gsd-executor` subagent couldn't run `go test`, `go vet`, `goimports`, or `grep` due to sandbox restrictions. Plan 01-04's executor hit the same block. Plan 01-05 was executed inline by the orchestrator instead. All deliverables still match the plan text; no code deviations beyond the two test adjustments noted above and the De Morgan lint fix.

## Phase closure

**Phase 1 complete; all 9 REQ-IDs green; verifier framework seam ready for v2 batch-level checks.**

## Statistics

- Commits in 01-05: 3
- Files created: 1 (`pkg/mimirtool/commands/backfill_test.go`, 148 lines)
- Files modified: 3 (`pkg/mimirtool/commands/backfill.go` +45/-4, `CHANGELOG.md` +1, `pkg/mimirtool/backfill/verify/verify.go` +1/-1)
- Total new tests: 5 CLI tests
- Reserved PR number: #15115
- Execution time: ~12 min including lint fix + human-checkpoint prep

## Human verification checkpoint

Presented to user separately — ready for approval or feedback.
