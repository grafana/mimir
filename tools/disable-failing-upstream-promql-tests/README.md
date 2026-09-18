This tool comments out every enabled upstream PromQL test case that Mimir's engine does not run
successfully - whether because the feature is unsupported or the result diverges from upstream - so
that `TestUpstreamTestCases` stays green after a mimir-prometheus bump. It is the second half of the
automated vendoring sync, run after [`sync-upstream-promql-tests`](../sync-upstream-promql-tests).

It runs `TestUpstreamTestCases` once with `go test -json`, reads the failing per-case subtests to
learn exactly which eval commands failed, and comments those blocks out. A single run reports every
failing case, so there is no per-case isolation or iteration.

Disabled cases are split by cause (unsupported vs divergent result) and, when `MIMIR_SYNC_BASELINE_DIR`
points at the pre-sync copies, by origin (new upstream case vs previously-passing case). The list is
written to `MIMIR_SYNC_REPORT` and `MIMIR_SYNC_DISABLED` when those are set.

Run this tool with `go run .` in this directory, or via `make disable-failing-upstream-promql-tests`
from the repository root.

See [these docs](../../pkg/streamingpromql/testdata/upstream/README.md) for more information, and the
inverse tool [`check-for-disabled-but-supported-mqe-test-cases`](../check-for-disabled-but-supported-mqe-test-cases).
