// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/tree/main/promql/engine_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streamingpromql

import (
	"context"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/assert"

	"github.com/go-kit/log"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"
)

func init() {
	types.EnableManglingReturnedSlices = true
	parser.ExperimentalDurationExpr = true
	parser.EnableExperimentalFunctions = true
}

type TestQueryContext struct {
	prometheusEngine  *promql.Engine
	mimirEngine       *Engine
	testStorage       *teststorage.TestStorage
	ignoreAnnotations map[string]bool
}

func DoComparisonQuery(t *testing.T, asInstantQuery bool, query string, ctx TestQueryContext) {

	var prom, mimir *promql.Result
	var promErr, mimiErr error
	var promQry, mimiQry promql.Query

	if asInstantQuery {

		ts := time.Unix(0, 0)

		promQry, promErr = ctx.prometheusEngine.NewInstantQuery(context.Background(), ctx.testStorage, nil, query, ts)
		mimiQry, mimiErr = ctx.mimirEngine.NewInstantQuery(context.Background(), ctx.testStorage, nil, query, ts)

	} else {

		start := time.Unix(0, 0)
		end := start.Add(time.Minute)
		step := time.Minute

		promQry, promErr = ctx.prometheusEngine.NewRangeQuery(context.Background(), ctx.testStorage, nil, query, start, end, step)
		mimiQry, mimiErr = ctx.mimirEngine.NewRangeQuery(context.Background(), ctx.testStorage, nil, query, start, end, step)
	}

	if promErr == nil {
		defer promQry.Close()
		prom = promQry.Exec(context.Background())
	}

	if mimiErr == nil {
		defer mimiQry.Close()
		mimir = mimiQry.Exec(context.Background())
	}

	if promErr == nil {

		// Replace series Metric which are nil with an empty Labels{}
		/// Prometheus may have a metric label = nil, and mimir could have an empty slice of metric labels
		FixUpEmptyLabels(prom)

		skipAnnotationComparison := ctx.ignoreAnnotations[query]

		if mimir != nil && prom != nil && prom.Err != nil && prom.Err.Error() == "expanding series: unexpected all postings" {
			assert.Equal(t, "unexpected all postings", mimir.Err.Error())
		} else {
			testutils.RequireEqualResults(t, query, prom, mimir, skipAnnotationComparison)
		}
	} else {
		require.NotNil(t, mimiErr)
	}
}

// This is a utility test which can be useful testing individual query strings
func TestSingleQuery(t *testing.T) {
	opts := NewTestEngineOpts()

	data, err := os.ReadFile("testdata/fuzz/data/seed-data.test")
	require.NoError(t, err)

	prometheusEngine := promql.NewEngine(opts.CommonOpts)
	mimirEngine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), NewQueryPlanner(opts), log.NewNopLogger())
	require.NoError(t, err)

	testStorage := promqltest.LoadedStorage(t, string(data))
	t.Cleanup(func() { require.NoError(t, testStorage.Close()) })

	//expr := `{""!="",""}`
	expr := `false / quantile_over_time(1.95, metric[5m])`
	ignoreAnnotations := map[string]bool{
		expr: true,
	}

	testCtx := TestQueryContext{
		prometheusEngine:  prometheusEngine,
		mimirEngine:       mimirEngine,
		testStorage:       testStorage,
		ignoreAnnotations: ignoreAnnotations,
	}

	DoComparisonQuery(t, true, expr, testCtx)
	DoComparisonQuery(t, false, expr, testCtx)
}

// This test function will run like a normal unit test with go test
// It can be used for Fuzz testing with go test -fuzz=FuzzQuery
// These tests can be extended in 2 ways;
// a. Add more sample data to the seed-data.test file
// b. Add more sample queries to the seed-queries.test file
// The Go Fuzzer will create new queries based off the entries in seed-queries.test
func FuzzQuery(f *testing.F) {

	// seed some test data - see prometheus/promql/promqltest/README.md
	data, err := os.ReadFile("testdata/fuzz/data/seed-data.test")
	require.NoError(f, err)

	opts := NewTestEngineOpts()
	mimirEngine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), NewQueryPlanner(opts), log.NewNopLogger())
	require.NoError(f, err)

	prometheusEngine := promql.NewEngine(opts.CommonOpts)

	testStorage := promqltest.LoadedStorage(f, string(data))
	f.Cleanup(func() { require.NoError(f, testStorage.Close()) })

	// Seed some example queries/expressions
	queries, err := os.ReadFile("testdata/fuzz/data/seed-queries.test")
	require.NoError(f, err)

	ignoreAnnotations := map[string]bool{}

	for _, query := range strings.Split(string(queries), "\n") {
		if len(query) == 0 || strings.HasPrefix(query, "#") {
			continue
		}

		query, found := strings.CutPrefix(query, "[ignore_annotations] ")
		ignoreAnnotations[query] = found

		f.Add(query)
	}

	testCtx := TestQueryContext{
		prometheusEngine:  prometheusEngine,
		mimirEngine:       mimirEngine,
		testStorage:       testStorage,
		ignoreAnnotations: ignoreAnnotations,
	}

	f.Fuzz(func(t *testing.T, query string) {
		DoComparisonQuery(t, true, query, testCtx)
		DoComparisonQuery(t, false, query, testCtx)
	})
}

// Test different combinations of start, end times in a range query
// Note that we do not compare to the Prometheus engine, as it validates range inputs in its http api handler
func FuzzRangeQueryTimes(f *testing.F) {

	const expression = "vector(0)"
	const duration = time.Duration(60*5) * time.Second

	opts := NewTestEngineOpts()
	mimirEngine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), NewQueryPlanner(opts), log.NewNopLogger())
	require.NoError(f, err)

	now := time.Now()

	testStart := []time.Time{now.Add(-time.Hour), now.Add(-time.Second), now.Add(-time.Millisecond), now.Add(-time.Hour * 24 * 365), now, now.Add(time.Second)}
	for _, start := range testStart {
		f.Add(start.UnixMilli(), now.UnixMilli())
	}

	f.Fuzz(func(t *testing.T, startEpoch int64, endEpoch int64) {

		start := time.UnixMilli(startEpoch)
		end := time.UnixMilli(endEpoch)

		_, mimErr := mimirEngine.NewRangeQuery(context.Background(), nil, nil, expression, start, end, duration)

		if startEpoch <= endEpoch {
			require.Nil(t, mimErr, "Expected success for start=%s, end=%s", start, end)
		} else {
			require.NotNil(t, mimErr, "Expected failure for start=%s, end=%s", start, end)
		}
	})
}

// Seed the fuzz with sample durations in a range query
// These tests we only check the Mimir engine, as the step size is validated in the Prometheus API handler (api.go)
// Note also that the max resolution size (end-start/step) is validated in codec.go so we don't test that here
func FuzzRangeTestStep(f *testing.F) {

	const expression = "vector(0)"
	var startTime = time.Now().Add(-time.Hour)
	var endTime = time.Now()

	opts := NewTestEngineOpts()
	mimirEngine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), NewQueryPlanner(opts), log.NewNopLogger())
	require.NoError(f, err)

	testStepSecs := []int64{-1, 0, 60, math.MaxInt64, int64(math.MaxInt64 / time.Second), int64(math.MaxInt64/time.Second) - 1}
	for _, stepSec := range testStepSecs {
		f.Add(stepSec)
	}

	f.Fuzz(func(t *testing.T, stepSecs int64) {

		step := time.Duration(stepSecs) * time.Second

		_, mimErr := mimirEngine.NewRangeQuery(context.Background(), nil, nil, expression, startTime, endTime, step)

		if stepSecs <= 0 || stepSecs > int64(math.MaxInt64/time.Second) {
			require.NotNil(t, mimErr, "Expected error for StepSecs=%d", stepSecs)
		} else {
			require.Nil(t, mimErr, "Expected success for StepSecs=%d", stepSecs)
		}
	})
}
