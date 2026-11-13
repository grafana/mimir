This tool re-syncs `pkg/streamingpromql/testdata/upstream` with the upstream PromQL test cases vendored under `vendor/github.com/prometheus/prometheus/promql/promqltest/testdata`.

It applies upstream's changes to our copies with a 3-way merge, preserving the eval commands we have disabled (`# Unsupported by streaming engine.`). If upstream changed a case we had disabled, or the lines next to it, that block is taken from upstream (so it comes back enabled) while the rest of the file keeps its disabling, and the file is listed in the report. The disabling tool then disables the case again if it still fails.

It does not decide which cases to disable. That is done afterwards by [`disable-failing-upstream-promql-tests`](../disable-failing-upstream-promql-tests), which runs the cases against Mimir's engine and comments out the ones that fail.

Run this tool with `go run .` in this directory, or via `make sync-upstream-promql-tests` from the repository root. It prints its report, and also writes it to `MIMIR_SYNC_REPORT` when that is set. That is only read when running the tool directly (as CI does), not through `make` in the build container.

## Fixing a failing mimir-prometheus vendoring PR

The re-sync is done automatically. The "Update mimir-prometheus dependency" workflow runs this tool after updating the vendored dependencies, and includes the result in the PR as a separate commit, with a summary in the PR description. Because the workflow holds the vendoring bot's token, it builds the tool from the workflow's own commit, not from the PR's target branch or its newly vendored dependencies. So a change to this tool takes effect in that workflow once it is merged to `main`, for all target branches.

This means that for a PR to a weekly or release branch, `main`'s tool writes the test files, but that branch's own `TestOurUpstreamTestCasesAreInSyncWithUpstream` checks them. If you change the format of the files on `main`, for example the license header or the `# Unsupported by streaming engine.` marker in `pkg/streamingpromql/upstreamtestdata`, backport the change to the weekly and release branches that still get vendoring PRs. If you don't, their vendoring PRs fail that test.

No test cases are disabled automatically. If `TestUpstreamTestCases` fails after the re-sync, someone on the MQE team decides whether to fix the issue or disable the failing cases. To disable them, fix the PR locally:

1. Check out the PR branch. If the PR description says the re-sync didn't happen or failed, run `make sync-upstream-promql-tests` and commit the result on its own, so reviewers can see upstream's changes separately from what gets disabled.
2. Run `make disable-failing-upstream-promql-tests MIMIR_SYNC_BASELINE_REF=$(git merge-base HEAD origin/main)` and commit the result. For a PR to a weekly or release branch, replace `main` with that branch, which the PR title shows. The baseline must be a commit from before the re-sync, and the commit the PR branch started from is one, even when other commits were pushed after the re-sync. The tool prints the cases it disabled, grouped by cause and by whether they are new upstream cases or existing ones. Review that list before pushing: a divergent result may be a real bug in Mimir's engine rather than something to disable.
3. Push. The PR then needs approval from someone other than you.

See [these docs](../../pkg/streamingpromql/testdata/upstream/README.md) for more information, and the inverse tool [`check-for-disabled-but-supported-mqe-test-cases`](../check-for-disabled-but-supported-mqe-test-cases).
