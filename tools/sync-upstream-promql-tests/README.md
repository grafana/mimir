This tool re-syncs `pkg/streamingpromql/testdata/upstream` with the upstream PromQL test cases vendored under `vendor/github.com/prometheus/prometheus/promql/promqltest/testdata`.

It applies upstream's changes to our copies with a 3-way merge, preserving the eval commands we have disabled (`# Unsupported by streaming engine.`). If upstream changed a case we had disabled, that block is taken from upstream (so it comes back enabled) while the rest of the file keeps its disabling, and the file is listed in the report.

It does not decide which cases to disable. That is done afterwards by [`disable-failing-upstream-promql-tests`](../disable-failing-upstream-promql-tests), which runs the cases against Mimir's engine and comments out the ones that fail.

Run this tool with `go run .` in this directory, or via `make sync-upstream-promql-tests` from the repository root.

## Fixing a failing mimir-prometheus vendoring PR

When a `[main] Update mimir-prometheus to ...` PR fails `TestOurUpstreamTestCasesAreInSyncWithUpstream` or `TestUpstreamTestCases`:

1. Before pushing anything to the PR branch, run the "Sync upstream PromQL test cases" workflow from `main` (Actions tab, "Run workflow") with the PR's branch name as input. Only maintainers and admins can run it. It pushes up to two commits as `mimir-vendoring[bot]` (re-sync, then disable) and comments a report on the PR listing what it disabled and why. Review that list, in particular any previously-passing cases that now fail, since those may be real regressions.
2. If you then need to push your own fixes, do so after the workflow has run. The PR then needs approval from someone other than you.
3. The workflow refuses to run if the branch already has any commit not made by the bot, including a merge commit from "Update branch". This is deliberate: its commits would make the bot the last pusher, letting you approve your own earlier commits. In that case, run `make sync-upstream-promql-tests disable-failing-upstream-promql-tests` locally and push the result, or re-run the vendoring workflow to get a fresh branch if you need changes from `main`.

See [these docs](../../pkg/streamingpromql/testdata/upstream/README.md) for more information, and the inverse tool [`check-for-disabled-but-supported-mqe-test-cases`](../check-for-disabled-but-supported-mqe-test-cases).
