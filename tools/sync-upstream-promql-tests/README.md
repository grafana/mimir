This tool re-syncs `pkg/streamingpromql/testdata/upstream` with the upstream PromQL test cases vendored under `vendor/github.com/prometheus/prometheus/promql/promqltest/testdata`.

It applies upstream's changes to our copies with a 3-way merge, preserving the eval commands we have disabled (`# Unsupported by streaming engine.`). If upstream changed a case we had disabled, that block is taken from upstream (so it comes back enabled) while the rest of the file keeps its disabling, and the file is listed in the report.

It does not decide which cases to disable. That is done afterwards by [`disable-failing-upstream-promql-tests`](../disable-failing-upstream-promql-tests), which runs the cases against Mimir's engine and comments out the ones that fail.

Run this tool with `go run .` in this directory, or via `make sync-upstream-promql-tests` from the repository root.

## Fixing a failing mimir-prometheus vendoring PR

When a `[main] Update mimir-prometheus to ...` PR fails `TestOurUpstreamTestCasesAreInSyncWithUpstream` or `TestUpstreamTestCases`:

1. Check out the PR branch, run `make sync-upstream-promql-tests` and commit the result on its own, so reviewers can see upstream's changes separately from what gets disabled.
2. Run `make disable-failing-upstream-promql-tests` and commit the result. It prints the cases it disabled, grouped by cause. Review that list before pushing: a divergent result may be a real bug in Mimir's engine rather than something to disable.
3. Push both commits. The PR then needs approval from someone other than you.

See [these docs](../../pkg/streamingpromql/testdata/upstream/README.md) for more information, and the inverse tool [`check-for-disabled-but-supported-mqe-test-cases`](../check-for-disabled-but-supported-mqe-test-cases).
