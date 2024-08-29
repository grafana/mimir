This directory contains sets of test cases used to test Mimir's query engine (MQE).

There are three subdirectories, each with a different purpose:

- `ours`: test cases we have created. These are run against MQE by `TestOurTestCases`, and also run against Prometheus' engine to confirm they are valid test cases.
- `ours-only`: same as above, but the comparison against Prometheus' engine is skipped.
  This is used for test cases that fail on Prometheus' engine due to a bug in Prometheus' engine.
  These test cases will generally only remain in this directory temporarily, and move to `ours` once the bug in Prometheus' engine is resolved.
- `upstream`: test cases from https://github.com/prometheus/prometheus/tree/main/promql/promqltest/testdata, modified to disable test cases MQE does not yet support.
  These are run against MQE by `TestUpstreamTestCases`, and kept in sync with the source test cases by `TestOurUpstreamTestCasesAreInSyncWithUpstream`.
