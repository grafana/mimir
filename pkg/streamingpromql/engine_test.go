// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"context"
	"io"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

func TestUnsupportedPromQLFeatures(t *testing.T) {
	opts := NewTestEngineOpts()
	engine, err := NewEngine(opts)
	require.NoError(t, err)
	ctx := context.Background()

	// The goal of this is not to list every conceivable expression that is unsupported, but to cover all the
	// different cases and make sure we produce a reasonable error message when these cases are encountered.
	unsupportedExpressions := map[string]string{
		"1 + 2":                      "binary expression with scalars",
		"1 + metric{}":               "binary expression with scalars",
		"metric{} + 1":               "binary expression with scalars",
		"metric{} < other_metric{}":  "binary expression with '<'",
		"metric{} or other_metric{}": "binary expression with many-to-many matching",
		"metric{} + on() group_left() other_metric{}":  "binary expression with many-to-one matching",
		"metric{} + on() group_right() other_metric{}": "binary expression with one-to-many matching",
		"1":                            "PromQL expression type *parser.NumberLiteral",
		"metric{} offset 2h":           "instant vector selector with 'offset'",
		"avg(metric{})":                "'avg' aggregation",
		"sum without(l) (metric{})":    "grouping with 'without'",
		"rate(metric{}[5m] offset 2h)": "range vector selector with 'offset'",
		"avg_over_time(metric{}[5m])":  "'avg_over_time' function",
		"-sum(metric{})":               "PromQL expression type *parser.UnaryExpr",
	}

	for expression, expectedError := range unsupportedExpressions {
		t.Run(expression, func(t *testing.T) {
			qry, err := engine.NewRangeQuery(ctx, nil, nil, expression, time.Now().Add(-time.Hour), time.Now(), time.Minute)
			require.Error(t, err)
			require.ErrorIs(t, err, NotSupportedError{})
			require.EqualError(t, err, "not supported by streaming engine: "+expectedError)
			require.Nil(t, qry)

			qry, err = engine.NewInstantQuery(ctx, nil, nil, expression, time.Now())
			require.Error(t, err)
			require.ErrorIs(t, err, NotSupportedError{})
			require.EqualError(t, err, "not supported by streaming engine: "+expectedError)
			require.Nil(t, qry)
		})
	}

	// These expressions are also unsupported, but are only valid as instant queries.
	unsupportedInstantQueryExpressions := map[string]string{
		"'a'":                    "PromQL expression type *parser.StringLiteral",
		"metric{}[5m]":           "PromQL expression type *parser.MatrixSelector",
		"metric{}[5m] offset 2h": "PromQL expression type *parser.MatrixSelector",
		"metric{}[5m] @ 123":     "PromQL expression type *parser.MatrixSelector",
		"metric{}[5m] @ start()": "PromQL expression type *parser.MatrixSelector",
		"metric{}[5m] @ end()":   "PromQL expression type *parser.MatrixSelector",
		"metric{}[5m:1m]":        "PromQL expression type *parser.SubqueryExpr",
	}

	for expression, expectedError := range unsupportedInstantQueryExpressions {
		t.Run(expression, func(t *testing.T) {
			qry, err := engine.NewInstantQuery(ctx, nil, nil, expression, time.Now())
			require.Error(t, err)
			require.ErrorIs(t, err, NotSupportedError{})
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

			promql.RunTest(t, string(testScript), engine)
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
				promql.RunTest(t, testScript, streamingEngine)
			})

			// Run the tests against Prometheus' engine to ensure our test cases are valid.
			t.Run("Prometheus' engine", func(t *testing.T) {
				promql.RunTest(t, testScript, prometheusEngine)
			})
		})
	}
}
