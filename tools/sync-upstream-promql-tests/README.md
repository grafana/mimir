This tool re-syncs `pkg/streamingpromql/testdata/upstream` with the upstream PromQL test cases
vendored under `vendor/github.com/prometheus/prometheus/promql/promqltest/testdata`.

It applies upstream's changes to our copies while preserving the eval commands we have disabled
(`# Unsupported by streaming engine.`), using a 3-way merge. When upstream changes a region we had
disabled, the merge conflicts and the file is re-synced fully enabled instead - the conflict is
reported so it appears in the vendoring PR description.

It does not decide which new cases to disable. That is done afterwards by
`TestSyncDisableFailingUpstreamCases`, which runs the cases against Mimir's engine and comments out
the ones that fail.

Run this tool with `go run .` in this directory, or via `make sync-upstream-promql-tests` from the
repository root.

See [these docs](../../pkg/streamingpromql/testdata/upstream/README.md) for more information, and the
inverse tool [`check-for-disabled-but-supported-mqe-test-cases`](../check-for-disabled-but-supported-mqe-test-cases).
