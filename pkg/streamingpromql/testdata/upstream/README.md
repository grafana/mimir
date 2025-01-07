This directory duplicates the test cases from https://github.com/prometheus/prometheus/tree/main/promql/testdata, used by `TestUpstreamTestCases` to ensure the streaming engine
produces the same results as Prometheus' engine.

Test cases that are not supported by the streaming engine are commented out with `# Unsupported by streaming engine.`.
If the entire file is not supported, appending `.disabled` to the file name disables it entirely.

The `TestOurUpstreamTestCasesAreInSyncWithUpstream` test ensures that these files remain in sync with upstream, even if we disable some tests in a file.

Once the streaming engine supports all PromQL features exercised by Prometheus' test cases, we can remove these files and instead call `promql.RunBuiltinTests` from our tests.
