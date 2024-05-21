// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"context"
	"io"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
)

func TestUnsupportedPromQLFeatures(t *testing.T) {
	opts := NewTestEngineOpts()
	engine, err := NewEngine(opts)
	require.NoError(t, err)
	ctx := context.Background()

	// The goal of this is not to list every conceivable expression that is unsupported, but to cover all the
	// different cases and make sure we produce a reasonable error message when these cases are encountered.
	unsupportedExpressions := map[string]string{
		"1 + 2":                      "scalar value as top-level expression",
		"1 + metric{}":               "binary expression with scalars",
		"metric{} + 1":               "binary expression with scalars",
		"metric{} < other_metric{}":  "binary expression with '<'",
		"metric{} or other_metric{}": "binary expression with many-to-many matching",
		"metric{} + on() group_left() other_metric{}":  "binary expression with many-to-one matching",
		"metric{} + on() group_right() other_metric{}": "binary expression with one-to-many matching",
		"1":                            "scalar value as top-level expression",
		"metric{} offset 2h":           "instant vector selector with 'offset'",
		"avg(metric{})":                "'avg' aggregation",
		"sum without(l) (metric{})":    "grouping with 'without'",
		"rate(metric{}[5m] offset 2h)": "range vector selector with 'offset'",
		"rate(metric{}[5m:1m])":        "PromQL expression type *parser.SubqueryExpr",
		"avg_over_time(metric{}[5m])":  "'avg_over_time' function",
		"-sum(metric{})":               "PromQL expression type *parser.UnaryExpr",
	}

	for expression, expectedError := range unsupportedExpressions {
		t.Run(expression, func(t *testing.T) {
			qry, err := engine.NewRangeQuery(ctx, nil, nil, expression, time.Now().Add(-time.Hour), time.Now(), time.Minute)
			require.Error(t, err)
			require.ErrorIs(t, err, compat.NotSupportedError{})
			require.EqualError(t, err, "not supported by streaming engine: "+expectedError)
			require.Nil(t, qry)

			qry, err = engine.NewInstantQuery(ctx, nil, nil, expression, time.Now())
			require.Error(t, err)
			require.ErrorIs(t, err, compat.NotSupportedError{})
			require.EqualError(t, err, "not supported by streaming engine: "+expectedError)
			require.Nil(t, qry)
		})
	}

	// These expressions are also unsupported, but are only valid as instant queries.
	unsupportedInstantQueryExpressions := map[string]string{
		"'a'":                    "string value as top-level expression",
		"metric{}[5m] offset 2h": "range vector selector with 'offset'",
		"metric{}[5m:1m]":        "PromQL expression type *parser.SubqueryExpr",
	}

	for expression, expectedError := range unsupportedInstantQueryExpressions {
		t.Run(expression, func(t *testing.T) {
			qry, err := engine.NewInstantQuery(ctx, nil, nil, expression, time.Now())
			require.Error(t, err)
			require.ErrorIs(t, err, compat.NotSupportedError{})
			require.EqualError(t, err, "not supported by streaming engine: "+expectedError)
			require.Nil(t, qry)
		})
	}
}

func TestNewRangeQuery_InvalidQueryTime(t *testing.T) {
	opts := NewTestEngineOpts()
	engine, err := NewEngine(opts)
	require.NoError(t, err)
	ctx := context.Background()

	_, err = engine.NewRangeQuery(ctx, nil, nil, "vector(0)", time.Now(), time.Now(), 0)
	require.EqualError(t, err, "0s is not a valid interval for a range query, must be greater than 0")

	start := time.Date(2024, 3, 22, 3, 0, 0, 0, time.UTC)
	_, err = engine.NewRangeQuery(ctx, nil, nil, "vector(0)", start, start.Add(-time.Hour), time.Second)
	require.EqualError(t, err, "range query time range is invalid: end time 2024-03-22T02:00:00Z is before start time 2024-03-22T03:00:00Z")
}

func TestNewRangeQuery_InvalidExpressionTypes(t *testing.T) {
	opts := NewTestEngineOpts()
	engine, err := NewEngine(opts)
	require.NoError(t, err)
	ctx := context.Background()

	_, err = engine.NewRangeQuery(ctx, nil, nil, "metric[3m]", time.Now(), time.Now(), time.Second)
	require.EqualError(t, err, "query expression produces a range vector, but expression for range queries must produce an instant vector or scalar")

	_, err = engine.NewRangeQuery(ctx, nil, nil, `"thing"`, time.Now(), time.Now(), time.Second)
	require.EqualError(t, err, "query expression produces a string, but expression for range queries must produce an instant vector or scalar")
}

// This test runs the test cases defined upstream in https://github.com/prometheus/prometheus/tree/main/promql/testdata and copied to testdata/upstream.
// Test cases that are not supported by the streaming engine are commented out (or, if the entire file is not supported, .disabled is appended to the file name).
// Once the streaming engine supports all PromQL features exercised by Prometheus' test cases, we can remove these files and instead call promql.RunBuiltinTests here instead.
func TestUpstreamTestCases(t *testing.T) {
	opts := NewTestEngineOpts()
	engine, err := NewEngine(opts)
	require.NoError(t, err)

	testdataFS := os.DirFS("./testdata")
	testFiles, err := fs.Glob(testdataFS, "upstream/*.test")
	require.NoError(t, err)

	for _, testFile := range testFiles {
		t.Run(testFile, func(t *testing.T) {
			f, err := testdataFS.Open(testFile)
			require.NoError(t, err)
			defer f.Close()

			testScript, err := io.ReadAll(f)
			require.NoError(t, err)

			promqltest.RunTest(t, string(testScript), engine)
		})
	}
}

func TestOurTestCases(t *testing.T) {
	opts := NewTestEngineOpts()
	streamingEngine, err := NewEngine(opts)
	require.NoError(t, err)

	prometheusEngine := promql.NewEngine(opts)

	testdataFS := os.DirFS("./testdata")
	testFiles, err := fs.Glob(testdataFS, "ours/*.test")
	require.NoError(t, err)

	for _, testFile := range testFiles {
		t.Run(testFile, func(t *testing.T) {
			f, err := testdataFS.Open(testFile)
			require.NoError(t, err)
			defer f.Close()

			b, err := io.ReadAll(f)
			require.NoError(t, err)

			testScript := string(b)

			t.Run("streaming engine", func(t *testing.T) {
				promqltest.RunTest(t, testScript, streamingEngine)
			})

			// Run the tests against Prometheus' engine to ensure our test cases are valid.
			t.Run("Prometheus' engine", func(t *testing.T) {
				promqltest.RunTest(t, testScript, prometheusEngine)
			})
		})
	}
}

// Testing instant queries that return a range vector is not supported by Prometheus' PromQL testing framework,
// and adding support for this would be quite involved.
//
// So instead, we test these few cases here instead.
func TestRangeVectorSelectors(t *testing.T) {
	opts := NewTestEngineOpts()
	streamingEngine, err := NewEngine(opts)
	require.NoError(t, err)

	prometheusEngine := promql.NewEngine(opts)

	baseT := timestamp.Time(0)
	storage := promqltest.LoadedStorage(t, `
		load 1m
			some_metric{env="1"} 0+1x4
			some_metric{env="2"} 0+2x4
			some_metric_with_gaps 0 1 _ 3
			some_metric_with_stale_marker 0 1 stale 3
	`)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	testCases := map[string]struct {
		expr     string
		expected *promql.Result
		ts       time.Time
	}{
		"matches series with points in range": {
			expr: "some_metric[1m]",
			ts:   baseT.Add(2 * time.Minute),
			expected: &promql.Result{
				Value: promql.Matrix{
					{
						Metric: labels.FromStrings("__name__", "some_metric", "env", "1"),
						Floats: []promql.FPoint{
							{T: timestamp.FromTime(baseT.Add(time.Minute)), F: 1},
							{T: timestamp.FromTime(baseT.Add(2 * time.Minute)), F: 2},
						},
					},
					{
						Metric: labels.FromStrings("__name__", "some_metric", "env", "2"),
						Floats: []promql.FPoint{
							{T: timestamp.FromTime(baseT.Add(time.Minute)), F: 2},
							{T: timestamp.FromTime(baseT.Add(2 * time.Minute)), F: 4},
						},
					},
				},
			},
		},
		"matches no series": {
			expr: "some_nonexistent_metric[1m]",
			ts:   baseT,
			expected: &promql.Result{
				Value: promql.Matrix{},
			},
		},
		"no samples in range": {
			expr: "some_metric[1m]",
			ts:   baseT.Add(20 * time.Minute),
			expected: &promql.Result{
				Value: promql.Matrix{},
			},
		},
		"does not return points outside range if last selected point does not align to end of range": {
			expr: "some_metric_with_gaps[1m]",
			ts:   baseT.Add(2 * time.Minute),
			expected: &promql.Result{
				Value: promql.Matrix{
					{
						Metric: labels.FromStrings("__name__", "some_metric_with_gaps"),
						Floats: []promql.FPoint{
							{T: timestamp.FromTime(baseT.Add(time.Minute)), F: 1},
						},
					},
				},
			},
		},
		"metric with stale marker": {
			expr: "some_metric_with_stale_marker[3m]",
			ts:   baseT.Add(3 * time.Minute),
			expected: &promql.Result{
				Value: promql.Matrix{
					{
						Metric: labels.FromStrings("__name__", "some_metric_with_stale_marker"),
						Floats: []promql.FPoint{
							{T: timestamp.FromTime(baseT), F: 0},
							{T: timestamp.FromTime(baseT.Add(time.Minute)), F: 1},
							{T: timestamp.FromTime(baseT.Add(3 * time.Minute)), F: 3},
						},
					},
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			runTest := func(t *testing.T, eng promql.QueryEngine, expr string, ts time.Time, expected *promql.Result) {
				q, err := eng.NewInstantQuery(context.Background(), storage, nil, expr, ts)
				require.NoError(t, err)
				defer q.Close()

				res := q.Exec(context.Background())
				require.Equal(t, expected, res)
			}

			t.Run("streaming engine", func(t *testing.T) {
				runTest(t, streamingEngine, testCase.expr, testCase.ts, testCase.expected)
			})

			// Run the tests against Prometheus' engine to ensure our test cases are valid.
			t.Run("Prometheus' engine", func(t *testing.T) {
				runTest(t, prometheusEngine, testCase.expr, testCase.ts, testCase.expected)
			})
		})
	}
}
