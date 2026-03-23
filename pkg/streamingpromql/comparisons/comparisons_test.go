// SPDX-License-Identifier: AGPL-3.0-only

package comparisons

import (
	"io"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
)

const (
	prometheusEngineName = "Prometheus' engine"
	mimirEngineName      = "Mimir's engine"
)

// This test runs the test cases defined upstream in https://github.com/prometheus/prometheus/tree/main/promql/testdata and copied to testdata/upstream.
// Test cases that are not supported by the streaming engine are commented out (or, if the entire file is not supported, .disabled is appended to the file name).
// Once the streaming engine supports all PromQL features exercised by Prometheus' test cases, we can remove these files and instead call promql.RunBuiltinTests here instead.
func TestUpstreamTestCases(t *testing.T) {
	opts := streamingpromql.NewTestEngineOpts()
	limits := streamingpromql.NewStaticQueryLimitsProvider()
	limits.EnableDelayedNameRemoval = true
	opts.Limits = limits
	planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engine, err := streamingpromql.NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	testdataFS := os.DirFS("../testdata")
	testFiles, err := fs.Glob(testdataFS, "upstream/*.test")
	require.NoError(t, err)
	require.NotEmpty(t, testFiles)

	for _, testFile := range testFiles {
		t.Run(testFile, func(t *testing.T) {
			f, err := testdataFS.Open(testFile)
			require.NoError(t, err)
			defer f.Close()

			b, err := io.ReadAll(f)
			require.NoError(t, err)

			testScript := string(b)
			promqltest.RunTest(t, testScript, engine)
		})
	}
}

func TestOurTestCases(t *testing.T) {
	makeEngines := func(t *testing.T, opts streamingpromql.EngineOpts, enableDelayedNameRemoval bool) (promql.QueryEngine, promql.QueryEngine) {
		limits := streamingpromql.NewStaticQueryLimitsProvider()
		limits.EnableDelayedNameRemoval = enableDelayedNameRemoval
		opts.Limits = limits
		if enableDelayedNameRemoval {
			// This line only affects Prometheus engine, as MQE's is in opts.Limits
			opts.CommonOpts.EnableDelayedNameRemoval = true
		}
		planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
		require.NoError(t, err)
		mimirEngine, err := streamingpromql.NewEngine(opts, stats.NewQueryMetrics(nil), planner)
		require.NoError(t, err)

		prometheusEngine := promql.NewEngine(opts.CommonOpts)

		return mimirEngine, prometheusEngine
	}
	opts := streamingpromql.NewTestEngineOpts()
	mimirEngine, prometheusEngine := makeEngines(t, opts, false)

	optsWithDelayedNameRemoval := streamingpromql.NewTestEngineOpts()
	mimirEngineWithDelayedNameRemoval, prometheusEngineWithDelayedNameRemoval := makeEngines(t, optsWithDelayedNameRemoval, true)

	testdataFS := os.DirFS("../testdata")
	testFiles, err := fs.Glob(testdataFS, "ours*/*.test")
	require.NoError(t, err)
	require.NotEmpty(t, testFiles)

	for _, testFile := range testFiles {
		t.Run(testFile, func(t *testing.T) {
			f, err := testdataFS.Open(testFile)
			require.NoError(t, err)
			defer f.Close()

			b, err := io.ReadAll(f)
			require.NoError(t, err)

			testScript := string(b)

			mimirEngineToTest := mimirEngine
			prometheusEngineToTest := prometheusEngine

			// switch to the alternate engines if we need delayed name removal
			if strings.Contains(testFile, "name_label_dropping") || strings.Contains(testFile, "delayed_name_removal_enabled") {
				mimirEngineToTest = mimirEngineWithDelayedNameRemoval
				prometheusEngineToTest = prometheusEngineWithDelayedNameRemoval
			}

			t.Run(mimirEngineName, func(t *testing.T) {
				promqltest.RunTest(t, testScript, mimirEngineToTest)
			})

			// Run the tests against Prometheus' engine to ensure our test cases are valid.
			t.Run(prometheusEngineName, func(t *testing.T) {
				if strings.HasPrefix(testFile, "ours-only") {
					t.Skip("disabled for Prometheus' engine due to bug in Prometheus' engine")
				}

				promqltest.RunTest(t, testScript, prometheusEngineToTest)
			})
		})
	}
}
