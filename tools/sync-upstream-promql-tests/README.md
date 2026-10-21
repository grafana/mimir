This tool re-syncs `pkg/streamingpromql/testdata/upstream` with the upstream PromQL test cases vendored under `vendor/github.com/prometheus/prometheus/promql/promqltest/testdata`.

It applies upstream's changes to our copies with a 3-way merge, preserving the eval commands we have disabled (`# Unsupported by streaming engine.`). If upstream changed a case we had disabled, or the lines next to it, that block is taken from upstream (so it comes back enabled) while the rest of the file keeps its disabling, and the file is listed in the report. The disabling tool then disables the case again if it still fails.

It does not decide which cases to disable. That is done afterwards by [`disable-failing-upstream-promql-tests`](../disable-failing-upstream-promql-tests), which runs the cases against Mimir's engine and comments out the ones that fail.

Run this tool with `go run .` in this directory, or via `make sync-upstream-promql-tests` from the repository root. It prints its report, and also writes it to `MIMIR_SYNC_REPORT` when that is set. That is only read when running the tool directly (as CI does), not through `make` in the build container.

## Fixing a failing mimir-prometheus vendoring PR

For `[main] Update mimir-prometheus to ...` PRs this is done automatically. When the vendoring bot opens the PR, the "Sync upstream PromQL test cases" workflow runs both tools, then "Commit synced upstream PromQL test cases" pushes the result to the PR as up to two commits from `mimir-vendoring[bot]` (re-sync, then disable) and comments the list of disabled cases on the PR, mentioning the MQE team if it disabled any cases that weren't already disabled. Review that list before approving, in particular any previously-passing cases that now fail, since those may be real regressions. It only commits while every commit on the PR is from the bot and the branch hasn't changed since the sync ran. While `DRY_RUN` is set in the commit workflow, it only comments what it would commit.

If that didn't happen or failed, or for other branches, fix the PR locally:

1. Check out the PR branch, run `make sync-upstream-promql-tests` and commit the result on its own, so reviewers can see upstream's changes separately from what gets disabled.
2. Run `make disable-failing-upstream-promql-tests MIMIR_SYNC_BASELINE_REF=HEAD~1` and commit the result. It prints the cases it disabled, grouped by cause and by whether they are new upstream cases or existing ones. Review that list before pushing: a divergent result may be a real bug in Mimir's engine rather than something to disable.
3. Push both commits. The PR then needs approval from someone other than you.

See [these docs](../../pkg/streamingpromql/testdata/upstream/README.md) for more information, and the inverse tool [`check-for-disabled-but-supported-mqe-test-cases`](../check-for-disabled-but-supported-mqe-test-cases).
