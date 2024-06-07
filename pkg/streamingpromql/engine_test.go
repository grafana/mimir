// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/pooling"
	"github.com/grafana/mimir/pkg/util/globalerror"
)

func TestUnsupportedPromQLFeatures(t *testing.T) {
	opts := NewTestEngineOpts()
	engine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
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
	engine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
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
	engine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
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
	engine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
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
	mimirEngine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
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

			t.Run("Mimir's engine", func(t *testing.T) {
				promqltest.RunTest(t, testScript, mimirEngine)
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
	mimirEngine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
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

			t.Run("Mimir's engine", func(t *testing.T) {
				runTest(t, mimirEngine, testCase.expr, testCase.ts, testCase.expected)
			})

			// Run the tests against Prometheus' engine to ensure our test cases are valid.
			t.Run("Prometheus' engine", func(t *testing.T) {
				runTest(t, prometheusEngine, testCase.expr, testCase.ts, testCase.expected)
			})
		})
	}
}

func TestQueryCancellation(t *testing.T) {
	opts := NewTestEngineOpts()
	engine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
	require.NoError(t, err)

	// Simulate the query being cancelled by another goroutine by waiting for the Select() call to be made,
	// then cancel the query and wait for the query context to be cancelled.
	//
	// In both this test and production, we rely on the underlying storage responding to the context cancellation -
	// we don't explicitly check for context cancellation in the query engine.
	var q promql.Query
	queryable := cancellationQueryable{func() {
		q.Cancel()
	}}

	q, err = engine.NewInstantQuery(context.Background(), queryable, nil, "some_metric", timestamp.Time(0))
	require.NoError(t, err)
	defer q.Close()

	res := q.Exec(context.Background())

	require.Error(t, res.Err)
	require.ErrorIs(t, res.Err, context.Canceled)
	require.EqualError(t, res.Err, "context canceled: query execution cancelled")
	require.Nil(t, res.Value)
}

func TestQueryTimeout(t *testing.T) {
	opts := NewTestEngineOpts()
	opts.Timeout = 20 * time.Millisecond
	engine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
	require.NoError(t, err)

	// Simulate the query doing some work and check that the query context has been cancelled.
	//
	// In both this test and production, we rely on the underlying storage responding to the context cancellation -
	// we don't explicitly check for context cancellation in the query engine.
	var q promql.Query
	queryable := cancellationQueryable{func() {
		time.Sleep(opts.Timeout * 10)
	}}

	q, err = engine.NewInstantQuery(context.Background(), queryable, nil, "some_metric", timestamp.Time(0))
	require.NoError(t, err)
	defer q.Close()

	res := q.Exec(context.Background())

	require.Error(t, res.Err)
	require.ErrorIs(t, res.Err, context.DeadlineExceeded)
	require.EqualError(t, res.Err, "context deadline exceeded: query timed out")
	require.Nil(t, res.Value)
}

type cancellationQueryable struct {
	onQueried func()
}

func (w cancellationQueryable) Querier(_, _ int64) (storage.Querier, error) {
	// nolint:gosimple
	return cancellationQuerier{onQueried: w.onQueried}, nil
}

type cancellationQuerier struct {
	onQueried func()
}

func (w cancellationQuerier) LabelValues(ctx context.Context, _ string, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, w.waitForCancellation(ctx)
}

func (w cancellationQuerier) LabelNames(ctx context.Context, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, w.waitForCancellation(ctx)
}

func (w cancellationQuerier) Select(ctx context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
	return storage.ErrSeriesSet(w.waitForCancellation(ctx))
}

func (w cancellationQuerier) Close() error {
	return nil
}

func (w cancellationQuerier) waitForCancellation(ctx context.Context) error {
	w.onQueried()

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-time.After(time.Second):
		return errors.New("expected query context to be cancelled after 1 second, but it was not")
	}
}

func TestQueryContextCancelledOnceQueryFinished(t *testing.T) {
	opts := NewTestEngineOpts()
	engine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
	require.NoError(t, err)

	storage := promqltest.LoadedStorage(t, `
		load 1m
			some_metric 0+1x4
	`)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	queryable := &contextCapturingQueryable{inner: storage}

	q, err := engine.NewInstantQuery(context.Background(), queryable, nil, "some_metric", timestamp.Time(0))
	require.NoError(t, err)
	defer q.Close()

	res := q.Exec(context.Background())
	require.NoError(t, res.Err)
	require.NotNil(t, res.Value)

	contextErr := queryable.capturedContext.Err()
	require.Equal(t, context.Canceled, contextErr)

	contextCause := context.Cause(queryable.capturedContext)
	require.ErrorIs(t, contextCause, context.Canceled)
	require.EqualError(t, contextCause, "context canceled: query execution finished")
}

type contextCapturingQueryable struct {
	capturedContext context.Context
	inner           storage.Queryable
}

func (q *contextCapturingQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	innerQuerier, err := q.inner.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}

	return &contextCapturingQuerier{
		queryable: q,
		inner:     innerQuerier,
	}, nil
}

type contextCapturingQuerier struct {
	queryable *contextCapturingQueryable
	inner     storage.Querier
}

func (q *contextCapturingQuerier) LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	q.queryable.capturedContext = ctx
	return q.inner.LabelValues(ctx, name, matchers...)
}

func (q *contextCapturingQuerier) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	q.queryable.capturedContext = ctx
	return q.inner.LabelNames(ctx, matchers...)
}

func (q *contextCapturingQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	q.queryable.capturedContext = ctx
	return q.inner.Select(ctx, sortSeries, hints, matchers...)
}

func (q *contextCapturingQuerier) Close() error {
	return q.inner.Close()
}

func TestMemoryConsumptionLimit_SingleQueries(t *testing.T) {
	storage := promqltest.LoadedStorage(t, `
		load 1m
			some_metric{idx="1"} 0+1x5
			some_metric{idx="2"} 0+1x5
			some_metric{idx="3"} 0+1x5
			some_metric{idx="4"} 0+1x5
			some_metric{idx="5"} 0+1x5
	`)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	testCases := map[string]struct {
		expr                     string
		rangeQueryExpectedPeak   uint64
		rangeQueryLimit          uint64
		instantQueryExpectedPeak uint64
		instantQueryLimit        uint64
		shouldSucceed            bool
	}{
		"limit disabled": {
			expr:          "some_metric",
			shouldSucceed: true,

			// Each series has five samples, which will be rounded up to 8 (the nearest power of two) by the bucketed pool, and we have five series.
			rangeQueryExpectedPeak: 5 * 8 * pooling.FPointSize,
			rangeQueryLimit:        0,

			// At peak, we'll hold all the output samples plus one series, which has one sample.
			// The output contains five samples, which will be rounded up to 8 (the nearest power of two).
			instantQueryExpectedPeak: pooling.FPointSize + 8*pooling.VectorSampleSize,
			instantQueryLimit:        0,
		},
		"limit enabled, but query does not exceed limit": {
			expr:          "some_metric",
			shouldSucceed: true,

			// Each series has five samples, which will be rounded up to 8 (the nearest power of two) by the bucketed pool, and we have five series.
			rangeQueryExpectedPeak: 5 * 8 * pooling.FPointSize,
			rangeQueryLimit:        1000,

			// At peak, we'll hold all the output samples plus one series, which has one sample.
			// The output contains five samples, which will be rounded up to 8 (the nearest power of two).
			instantQueryExpectedPeak: pooling.FPointSize + 8*pooling.VectorSampleSize,
			instantQueryLimit:        1000,
		},
		"limit enabled, and query exceeds limit": {
			expr:          "some_metric",
			shouldSucceed: false,

			// Allow only a single sample.
			rangeQueryLimit:   pooling.FPointSize,
			instantQueryLimit: pooling.FPointSize,

			// The query never successfully allocates anything.
			rangeQueryExpectedPeak:   0,
			instantQueryExpectedPeak: 0,
		},
		"limit enabled, query selects more samples than limit but should not load all of them into memory at once, and peak consumption is under limit": {
			expr:          "sum(some_metric)",
			shouldSucceed: true,

			// Each series has five samples, which will be rounded up to 8 (the nearest power of two) by the bucketed pool.
			// At peak we'll hold in memory: the running total for the sum() (a float and a bool at each step, with the number of steps rounded to the nearest power of 2), and the next series from the selector.
			rangeQueryExpectedPeak: 8*(pooling.Float64Size+pooling.BoolSize) + 8*pooling.FPointSize,
			rangeQueryLimit:        8*(pooling.Float64Size+pooling.BoolSize) + 8*pooling.FPointSize,

			// Each series has one sample, which is already a power of two.
			// At peak we'll hold in memory: the running total for the sum() (a float and a bool), the next series from the selector, and the output sample.
			instantQueryExpectedPeak: pooling.Float64Size + pooling.BoolSize + pooling.FPointSize + pooling.VectorSampleSize,
			instantQueryLimit:        pooling.Float64Size + pooling.BoolSize + pooling.FPointSize + pooling.VectorSampleSize,
		},
		"limit enabled, query selects more samples than limit but should not load all of them into memory at once, and peak consumption is over limit": {
			expr:          "sum(some_metric)",
			shouldSucceed: false,

			// Each series has five samples, which will be rounded up to 8 (the nearest power of two) by the bucketed pool.
			// At peak we'll hold in memory: the running total for the sum() (a float and a bool at each step, with the number of steps rounded to the nearest power of 2), and the next series from the selector.
			// The last thing to be allocated is the bool slice for the running total, so that won't contribute to the peak before the query is aborted.
			rangeQueryExpectedPeak: 8*pooling.Float64Size + 8*pooling.FPointSize,
			rangeQueryLimit:        8*(pooling.Float64Size+pooling.BoolSize) + 8*pooling.FPointSize - 1,

			// Each series has one sample, which is already a power of two.
			// At peak we'll hold in memory: the running total for the sum() (a float and a bool), the next series from the selector, and the output sample.
			// The last thing to be allocated is the bool slice for the running total, so that won't contribute to the peak before the query is aborted.
			instantQueryExpectedPeak: pooling.Float64Size + pooling.FPointSize + pooling.VectorSampleSize,
			instantQueryLimit:        pooling.Float64Size + pooling.BoolSize + pooling.FPointSize + pooling.VectorSampleSize - 1,
		},
	}

	createEngine := func(t *testing.T, limit uint64) (promql.QueryEngine, *prometheus.Registry, opentracing.Span, context.Context) {
		reg := prometheus.NewPedanticRegistry()
		opts := NewTestEngineOpts()
		opts.Reg = reg

		engine, err := NewEngine(opts, NewStaticQueryLimitsProvider(limit), stats.NewQueryMetrics(reg), log.NewNopLogger())
		require.NoError(t, err)

		tracer, closer := jaeger.NewTracer("test", jaeger.NewConstSampler(true), jaeger.NewNullReporter())
		t.Cleanup(func() { _ = closer.Close() })
		span, ctx := opentracing.StartSpanFromContextWithTracer(context.Background(), tracer, "query")

		return engine, reg, span, ctx
	}

	assertEstimatedPeakMemoryConsumption := func(t *testing.T, reg *prometheus.Registry, span opentracing.Span, expectedMemoryConsumptionEstimate uint64) {
		peakMemoryConsumptionHistogram := getHistogram(t, reg, "cortex_mimir_query_engine_estimated_query_peak_memory_consumption")
		require.Equal(t, float64(expectedMemoryConsumptionEstimate), peakMemoryConsumptionHistogram.GetSampleSum())

		jaegerSpan, ok := span.(*jaeger.Span)
		require.True(t, ok)
		require.Len(t, jaegerSpan.Logs(), 1)
		traceLog := jaegerSpan.Logs()[0]
		expectedFields := []otlog.Field{
			otlog.String("level", "info"),
			otlog.String("msg", "query stats"),
			otlog.Uint64("estimatedPeakMemoryConsumption", expectedMemoryConsumptionEstimate),
		}
		require.Equal(t, expectedFields, traceLog.Fields)
	}

	start := timestamp.Time(0)

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			queryTypes := map[string]func(t *testing.T) (promql.Query, *prometheus.Registry, opentracing.Span, context.Context, uint64){
				"range query": func(t *testing.T) (promql.Query, *prometheus.Registry, opentracing.Span, context.Context, uint64) {
					engine, reg, span, ctx := createEngine(t, testCase.rangeQueryLimit)
					q, err := engine.NewRangeQuery(ctx, storage, nil, testCase.expr, start, start.Add(4*time.Minute), time.Minute)
					require.NoError(t, err)
					return q, reg, span, ctx, testCase.rangeQueryExpectedPeak
				},
				"instant query": func(t *testing.T) (promql.Query, *prometheus.Registry, opentracing.Span, context.Context, uint64) {
					engine, reg, span, ctx := createEngine(t, testCase.instantQueryLimit)
					q, err := engine.NewInstantQuery(ctx, storage, nil, testCase.expr, start)
					require.NoError(t, err)
					return q, reg, span, ctx, testCase.instantQueryExpectedPeak
				},
			}

			for queryType, createQuery := range queryTypes {
				t.Run(queryType, func(t *testing.T) {
					q, reg, span, ctx, expectedPeakMemoryConsumption := createQuery(t)
					t.Cleanup(q.Close)

					res := q.Exec(ctx)

					if testCase.shouldSucceed {
						require.NoError(t, res.Err)
						require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(rejectedMetrics(0)), "cortex_querier_queries_rejected_total"))
					} else {
						require.ErrorContains(t, res.Err, globalerror.MaxEstimatedMemoryConsumptionPerQuery.Error())
						require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(rejectedMetrics(1)), "cortex_querier_queries_rejected_total"))
					}

					assertEstimatedPeakMemoryConsumption(t, reg, span, expectedPeakMemoryConsumption)
				})
			}
		})
	}
}

func TestMemoryConsumptionLimit_MultipleQueries(t *testing.T) {
	storage := promqltest.LoadedStorage(t, `
		load 1m
			some_metric{idx="1"} 0+1x5
			some_metric{idx="2"} 0+1x5
			some_metric{idx="3"} 0+1x5
			some_metric{idx="4"} 0+1x5
			some_metric{idx="5"} 0+1x5
	`)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	reg := prometheus.NewPedanticRegistry()
	opts := NewTestEngineOpts()
	opts.Reg = reg

	limit := 3 * 8 * pooling.FPointSize // Allow up to three series with five points (which will be rounded up to 8, the nearest power of 2)
	engine, err := NewEngine(opts, NewStaticQueryLimitsProvider(limit), stats.NewQueryMetrics(reg), log.NewNopLogger())
	require.NoError(t, err)

	runQuery := func(expr string, shouldSucceed bool) {
		q, err := engine.NewRangeQuery(context.Background(), storage, nil, expr, timestamp.Time(0), timestamp.Time(0).Add(4*time.Minute), time.Minute)
		require.NoError(t, err)
		defer q.Close()

		res := q.Exec(context.Background())

		if shouldSucceed {
			require.NoError(t, res.Err)
		} else {
			require.ErrorContains(t, res.Err, globalerror.MaxEstimatedMemoryConsumptionPerQuery.Error())
		}
	}

	runQuery(`some_metric{idx=~"1"}`, true)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(rejectedMetrics(0)), "cortex_querier_queries_rejected_total"))

	runQuery(`some_metric{idx=~"1|2|3"}`, true)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(rejectedMetrics(0)), "cortex_querier_queries_rejected_total"))

	runQuery(`some_metric{idx=~"1|2|3|4"}`, false)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(rejectedMetrics(1)), "cortex_querier_queries_rejected_total"))

	runQuery(`some_metric{idx=~"1|2|3|4"}`, false)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(rejectedMetrics(2)), "cortex_querier_queries_rejected_total"))

	runQuery(`some_metric{idx=~"1|2|3"}`, true)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(rejectedMetrics(2)), "cortex_querier_queries_rejected_total"))
}

func rejectedMetrics(rejectedDueToMemoryConsumption int) string {
	return fmt.Sprintf(`
		# HELP cortex_querier_queries_rejected_total Number of queries that were rejected, for example because they exceeded a limit.
		# TYPE cortex_querier_queries_rejected_total counter
		cortex_querier_queries_rejected_total{reason="max-estimated-fetched-chunks-per-query"} 0
		cortex_querier_queries_rejected_total{reason="max-estimated-memory-consumption-per-query"} %v
		cortex_querier_queries_rejected_total{reason="max-fetched-chunk-bytes-per-query"} 0
		cortex_querier_queries_rejected_total{reason="max-fetched-chunks-per-query"} 0
		cortex_querier_queries_rejected_total{reason="max-fetched-series-per-query"} 0
	`, rejectedDueToMemoryConsumption)
}

func getHistogram(t *testing.T, reg *prometheus.Registry, name string) *dto.Histogram {
	metrics, err := reg.Gather()
	require.NoError(t, err)

	for _, m := range metrics {
		if m.GetName() == name {
			require.Len(t, m.Metric, 1)

			return m.Metric[0].Histogram
		}
	}

	require.Fail(t, "expected to find a metric with name "+name)
	return nil
}

func TestActiveQueryTracker(t *testing.T) {
	for _, shouldSucceed := range []bool{true, false} {
		t.Run(fmt.Sprintf("successful query = %v", shouldSucceed), func(t *testing.T) {
			opts := NewTestEngineOpts()
			tracker := &testQueryTracker{}
			opts.ActiveQueryTracker = tracker
			engine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
			require.NoError(t, err)

			innerStorage := promqltest.LoadedStorage(t, "")
			t.Cleanup(func() { require.NoError(t, innerStorage.Close()) })

			// Use a fake queryable as a way to check that the query is recorded as active while the query is in progress.
			queryTrackingTestingQueryable := &activeQueryTrackerQueryable{
				innerStorage: innerStorage,
				tracker:      tracker,
			}

			if !shouldSucceed {
				queryTrackingTestingQueryable.err = errors.New("something went wrong inside the query")
			}

			queryTypes := map[string]func(expr string) (promql.Query, error){
				"range": func(expr string) (promql.Query, error) {
					return engine.NewRangeQuery(context.Background(), queryTrackingTestingQueryable, nil, expr, timestamp.Time(0), timestamp.Time(0).Add(time.Hour), time.Minute)
				},
				"instant": func(expr string) (promql.Query, error) {
					return engine.NewInstantQuery(context.Background(), queryTrackingTestingQueryable, nil, expr, timestamp.Time(0))
				},
			}

			for queryType, createQuery := range queryTypes {
				t.Run(queryType+" query", func(t *testing.T) {
					expr := "test_" + queryType + "_query"
					queryTrackingTestingQueryable.activeQueryAtQueryTime = trackedQuery{}

					q, err := createQuery(expr)
					require.NoError(t, err)
					defer q.Close()

					res := q.Exec(context.Background())

					if shouldSucceed {
						require.NoError(t, res.Err)
					} else {
						require.EqualError(t, res.Err, "something went wrong inside the query")
					}

					// Check that the query was active in the query tracker while the query was executing.
					require.Equal(t, expr, queryTrackingTestingQueryable.activeQueryAtQueryTime.expr)
					require.False(t, queryTrackingTestingQueryable.activeQueryAtQueryTime.deleted)

					// Check that the query has now been marked as deleted in the query tracker.
					require.NotEmpty(t, tracker.queries)
					trackedQuery := tracker.queries[len(tracker.queries)-1]
					require.Equal(t, expr, trackedQuery.expr)
					require.Equal(t, true, trackedQuery.deleted)
				})
			}
		})
	}
}

type testQueryTracker struct {
	queries []trackedQuery
}

type trackedQuery struct {
	expr    string
	deleted bool
}

func (qt *testQueryTracker) GetMaxConcurrent() int {
	return 0
}

func (qt *testQueryTracker) Insert(_ context.Context, query string) (int, error) {
	qt.queries = append(qt.queries, trackedQuery{
		expr:    query,
		deleted: false,
	})

	return len(qt.queries) - 1, nil
}

func (qt *testQueryTracker) Delete(insertIndex int) {
	qt.queries[insertIndex].deleted = true
}

type activeQueryTrackerQueryable struct {
	tracker *testQueryTracker

	activeQueryAtQueryTime trackedQuery

	innerStorage storage.Queryable
	err          error
}

func (a *activeQueryTrackerQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	a.activeQueryAtQueryTime = a.tracker.queries[len(a.tracker.queries)-1]

	if a.err != nil {
		return nil, a.err
	}

	return a.innerStorage.Querier(mint, maxt)
}

func TestActiveQueryTracker_WaitingForTrackerIncludesQueryTimeout(t *testing.T) {
	tracker := &timeoutTestingQueryTracker{}
	opts := NewTestEngineOpts()
	opts.Timeout = 10 * time.Millisecond
	opts.ActiveQueryTracker = tracker
	engine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
	require.NoError(t, err)

	queryTypes := map[string]func() (promql.Query, error){
		"range": func() (promql.Query, error) {
			return engine.NewRangeQuery(context.Background(), nil, nil, "some_test_query", timestamp.Time(0), timestamp.Time(0).Add(time.Hour), time.Minute)
		},
		"instant": func() (promql.Query, error) {
			return engine.NewInstantQuery(context.Background(), nil, nil, "some_test_query", timestamp.Time(0))
		},
	}

	for queryType, createQuery := range queryTypes {
		t.Run(queryType+" query", func(t *testing.T) {
			tracker.sawTimeout = false

			q, err := createQuery()
			require.NoError(t, err)
			defer q.Close()

			res := q.Exec(context.Background())

			require.True(t, tracker.sawTimeout, "query tracker was not called with a context that timed out")

			require.Error(t, res.Err)
			require.ErrorIs(t, res.Err, context.DeadlineExceeded)
			require.EqualError(t, res.Err, "context deadline exceeded: query timed out")
			require.Nil(t, res.Value)
		})
	}
}

type timeoutTestingQueryTracker struct {
	sawTimeout bool
}

func (t *timeoutTestingQueryTracker) GetMaxConcurrent() int {
	return 0
}

func (t *timeoutTestingQueryTracker) Insert(ctx context.Context, _ string) (int, error) {
	select {
	case <-ctx.Done():
		t.sawTimeout = true
		return 0, context.Cause(ctx)
	case <-time.After(time.Second):
		return 0, errors.New("gave up waiting for query to time out")
	}
}

func (t *timeoutTestingQueryTracker) Delete(_ int) {
	panic("should not be called")
}
