// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/tree/main/promql/engine_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streamingpromql

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	promstats "github.com/prometheus/prometheus/util/stats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/lazyquery"
	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	mqetest "github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/promqlext"
	syncutil "github.com/grafana/mimir/pkg/util/sync"
)

var (
	// We use an in-memory span exporter since we test that things are instrumented
	// correctly in unit tests, but it only retains a fixed number of spans to avoid
	// using an excessive amount of memory.
	spanExporter = mqetest.NewFixedInMemoryExporter(1024)
)

const (
	prometheusEngineName = "Prometheus' engine"
	mimirEngineName      = "Mimir's engine"
)

func init() {
	types.EnableManglingReturnedSlices = true

	// Set a tracer provider with in memory span exporter so we can check the spans later.
	otel.SetTracerProvider(
		tracesdk.NewTracerProvider(
			tracesdk.WithSpanProcessor(tracesdk.NewSimpleSpanProcessor(spanExporter)),
		),
	)
}

func TestUnsupportedPromQLFeatures(t *testing.T) {
	// The goal of this is not to list every conceivable expression that is unsupported, but to cover all the
	// different cases and make sure we produce a reasonable error message when these cases are encountered.
	unsupportedExpressions := map[string]string{
		"left_vector + fill_right(0) right_vector": "'fill' modifier",
		"left_vector + fill(0) right_vector":       "'fill' modifier",
		"left_vector + fill_left(0) right_vector":  "'fill' modifier",
	}

	for expression, expectedError := range unsupportedExpressions {
		t.Run(expression, func(t *testing.T) {
			requireQueryIsUnsupported(t, expression, expectedError)
		})
	}
}

func requireQueryIsUnsupported(t *testing.T, expression string, expectedError string) {
	t.Run("range query", func(t *testing.T) {
		requireRangeQueryIsUnsupported(t, expression, expectedError)
	})

	t.Run("instant query", func(t *testing.T) {
		requireInstantQueryIsUnsupported(t, expression, expectedError)
	})
}

func requireRangeQueryIsUnsupported(t *testing.T, expression string, expectedError string) {
	parserOpts := promqlext.NewPromQLParserOptions()
	parserOpts.EnableBinopFillModifiers = true

	opts := NewTestEngineOpts()
	opts.CommonOpts.Parser = parser.NewParser(parserOpts)

	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	qry, err := engine.NewRangeQuery(context.Background(), nil, nil, expression, time.Now().Add(-time.Hour), time.Now(), time.Minute)
	require.EqualError(t, err, "not supported by streaming engine: "+expectedError)
	require.ErrorIs(t, err, compat.NotSupportedError{})
	require.Nil(t, qry)
}

func requireInstantQueryIsUnsupported(t *testing.T, expression string, expectedError string) {
	parserOpts := promqlext.NewPromQLParserOptions()
	parserOpts.EnableBinopFillModifiers = true

	opts := NewTestEngineOpts()
	opts.CommonOpts.Parser = parser.NewParser(parserOpts)

	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	qry, err := engine.NewInstantQuery(context.Background(), nil, nil, expression, time.Now())
	require.Error(t, err)
	require.ErrorIs(t, err, compat.NotSupportedError{})
	require.EqualError(t, err, "not supported by streaming engine: "+expectedError)
	require.Nil(t, qry)
}

func TestNewRangeQuery_InvalidQueryTime(t *testing.T) {
	opts := NewTestEngineOpts()
	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = engine.NewRangeQuery(ctx, nil, nil, "vector(0)", time.Now(), time.Now(), 0)
	require.Equal(t, apierror.New(apierror.TypeBadData, "0s is not a valid interval for a range query, must be greater than 0"), err)

	start := time.Date(2024, 3, 22, 3, 0, 0, 0, time.UTC)
	_, err = engine.NewRangeQuery(ctx, nil, nil, "vector(0)", start, start.Add(-time.Hour), time.Second)
	require.Equal(t, apierror.New(apierror.TypeBadData, "range query time range is invalid: end time 2024-03-22T02:00:00Z is before start time 2024-03-22T03:00:00Z"), err)
}

func TestNewRangeQuery_InvalidExpressionTypes(t *testing.T) {
	opts := NewTestEngineOpts()
	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = engine.NewRangeQuery(ctx, nil, nil, "metric[3m]", time.Now(), time.Now(), time.Second)
	require.Equal(t, apierror.New(apierror.TypeBadData, "query expression produces a range vector, but expression for range queries must produce an instant vector or scalar"), err)

	_, err = engine.NewRangeQuery(ctx, nil, nil, `"thing"`, time.Now(), time.Now(), time.Second)
	require.Equal(t, apierror.New(apierror.TypeBadData, "query expression produces a string, but expression for range queries must produce an instant vector or scalar"), err)
}

func TestNewInstantQuery_Strings(t *testing.T) {
	ctx := context.Background()
	opts := NewTestEngineOpts()
	prometheusEngine := promql.NewEngine(opts.CommonOpts)

	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	mimirEngine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	storage := promqltest.LoadedStorage(t, ``)

	expr := `"thing"`
	q, err := mimirEngine.NewInstantQuery(ctx, storage, nil, expr, time.Now())
	require.NoError(t, err)
	mimir := q.Exec(context.Background())
	defer q.Close()

	q, err = prometheusEngine.NewInstantQuery(ctx, storage, nil, expr, time.Now())
	require.NoError(t, err)
	prometheus := q.Exec(context.Background())
	defer q.Close()

	mqetest.RequireEqualResults(t, expr, prometheus, mimir, false)
}

// This test runs the test cases defined upstream in https://github.com/prometheus/prometheus/tree/main/promql/testdata and copied to testdata/upstream.
// Test cases that are not supported by the streaming engine are commented out (or, if the entire file is not supported, .disabled is appended to the file name).
// Once the streaming engine supports all PromQL features exercised by Prometheus' test cases, we can remove these files and instead call promql.RunBuiltinTests here instead.
func TestUpstreamTestCases(t *testing.T) {
	opts := NewTestEngineOpts()
	limits := NewStaticQueryLimitsProvider()
	limits.EnableDelayedNameRemoval = true
	opts.Limits = limits
	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	testdataFS := os.DirFS("./testdata")
	testFiles, err := fs.Glob(testdataFS, "upstream/*.test")
	require.NoError(t, err)

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
	makeEngines := func(t *testing.T, opts EngineOpts, enableDelayedNameRemoval bool) (promql.QueryEngine, promql.QueryEngine) {
		limits := NewStaticQueryLimitsProvider()
		limits.EnableDelayedNameRemoval = enableDelayedNameRemoval
		opts.Limits = limits
		if enableDelayedNameRemoval {
			// This line only affects Prometheus engine, as MQE's is in opts.Limits
			opts.CommonOpts.EnableDelayedNameRemoval = true
		}
		planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
		require.NoError(t, err)
		mimirEngine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
		require.NoError(t, err)

		prometheusEngine := promql.NewEngine(opts.CommonOpts)

		return mimirEngine, prometheusEngine
	}
	opts := NewTestEngineOpts()
	mimirEngine, prometheusEngine := makeEngines(t, opts, false)

	optsWithDelayedNameRemoval := NewTestEngineOpts()
	mimirEngineWithDelayedNameRemoval, prometheusEngineWithDelayedNameRemoval := makeEngines(t, optsWithDelayedNameRemoval, true)

	testdataFS := os.DirFS("./testdata")
	testFiles, err := fs.Glob(testdataFS, "ours*/*.test")
	require.NoError(t, err)

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

// Testing instant queries that return a range vector is not supported by Prometheus' PromQL testing framework,
// and adding support for this would be quite involved.
//
// So instead, we test these few cases here instead.
func TestRangeVectorSelectors(t *testing.T) {
	opts := NewTestEngineOpts()
	prometheusEngine := promql.NewEngine(opts.CommonOpts)
	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	mimirEngine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	baseT := timestamp.Time(0)
	storage := promqltest.LoadedStorage(t, `
		load 1m
			some_metric{env="1"} 0+1x4
			some_metric{env="2"} 0+2x4
			some_metric_with_gaps 0 1 _ 3
			some_metric_with_stale_marker 0 1 stale 3
			incr_histogram{env="1"}	{{schema:0 sum:4 count:4 buckets:[1 2 1]}}+{{sum:2 count:1 buckets:[1] offset:1}}x4
			incr_histogram{env="2"}	{{schema:0 sum:4 count:4 buckets:[1 2 1]}}+{{sum:4 count:2 buckets:[1 2] offset:1}}x4
			histogram_with_gaps	{{sum:1 count:1 buckets:[1]}} {{sum:2 count:2 buckets:[1 1]}} _ {{sum:3 count:3 buckets:[1 1 1]}}
			histogram_with_stale_marker	{{sum:1 count:1 buckets:[1]}} {{sum:2 count:2 buckets:[1 1]}} stale {{sum:4 count:4 buckets:[1 1 1 1]}}
			mixed_metric {{schema:0 sum:4 count:4 buckets:[1 2 1]}} 1 2 {{schema:0 sum:3 count:3 buckets:[1 2 1]}}
			mixed_metric_histogram_first {{schema:0 sum:4 count:4 buckets:[1 2 1]}} 1
			mixed_metric_float_first 1 {{schema:0 sum:4 count:4 buckets:[1 2 1]}}
	`)

	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	testCases := map[string]struct {
		expr     string
		expected *promql.Result
		ts       time.Time
	}{
		"floats: matches series with points in range": {
			expr: "some_metric[1m1s]",
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
		"floats: matches no series": {
			expr: "some_nonexistent_metric[1m]",
			ts:   baseT,
			expected: &promql.Result{
				Value: promql.Matrix{},
			},
		},
		"floats: no samples in range": {
			expr: "some_metric[1m]",
			ts:   baseT.Add(20 * time.Minute),
			expected: &promql.Result{
				Value: promql.Matrix{},
			},
		},
		"floats: does not return points outside range if last selected point does not align to end of range": {
			expr: "some_metric_with_gaps[1m1s]",
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
		"floats: metric with stale marker": {
			expr: "some_metric_with_stale_marker[3m1s]",
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
		"histograms: matches series with points in range": {
			expr: "incr_histogram[1m1s]",
			ts:   baseT.Add(2 * time.Minute),
			expected: &promql.Result{
				Value: promql.Matrix{
					{
						Metric: labels.FromStrings("__name__", "incr_histogram", "env", "1"),
						Histograms: []promql.HPoint{
							{
								T: timestamp.FromTime(baseT.Add(time.Minute)),
								H: &histogram.FloatHistogram{
									Sum:   6,
									Count: 5,
									PositiveSpans: []histogram.Span{
										{
											Offset: 0,
											Length: 3,
										},
									},
									PositiveBuckets: []float64{
										1, 3, 1,
									},
									CounterResetHint: histogram.NotCounterReset,
								},
							},
							{
								T: timestamp.FromTime(baseT.Add(2 * time.Minute)),
								H: &histogram.FloatHistogram{
									Sum:   8,
									Count: 6,
									PositiveSpans: []histogram.Span{
										{
											Offset: 0,
											Length: 3,
										},
									},
									PositiveBuckets: []float64{
										1, 4, 1,
									},
									CounterResetHint: histogram.NotCounterReset,
								},
							},
						},
					},
					{
						Metric: labels.FromStrings("__name__", "incr_histogram", "env", "2"),
						Histograms: []promql.HPoint{
							{
								T: timestamp.FromTime(baseT.Add(time.Minute)),
								H: &histogram.FloatHistogram{
									Sum:   8,
									Count: 6,
									PositiveSpans: []histogram.Span{
										{
											Offset: 0,
											Length: 3,
										},
									},
									PositiveBuckets: []float64{
										1, 3, 3,
									},
									CounterResetHint: histogram.NotCounterReset,
								},
							},
							{
								T: timestamp.FromTime(baseT.Add(2 * time.Minute)),
								H: &histogram.FloatHistogram{
									Sum:   12,
									Count: 8,
									PositiveSpans: []histogram.Span{
										{
											Offset: 0,
											Length: 3,
										},
									},
									PositiveBuckets: []float64{
										1, 4, 5,
									},
									CounterResetHint: histogram.NotCounterReset,
								},
							},
						},
					},
				},
			},
		},
		"histograms: no samples in range": {
			expr: "incr_histogram[1m]",
			ts:   baseT.Add(20 * time.Minute),
			expected: &promql.Result{
				Value: promql.Matrix{},
			},
		},
		"histograms: does not return points outside range if last selected point does not align to end of range": {
			expr: "histogram_with_gaps[1m1s]",
			ts:   baseT.Add(2 * time.Minute),
			expected: &promql.Result{
				Value: promql.Matrix{
					{
						Metric: labels.FromStrings("__name__", "histogram_with_gaps"),
						Histograms: []promql.HPoint{
							{
								T: timestamp.FromTime(baseT.Add(time.Minute)),
								H: &histogram.FloatHistogram{
									Sum:   2,
									Count: 2,
									PositiveSpans: []histogram.Span{
										{
											Offset: 0,
											Length: 3,
										},
									},
									PositiveBuckets: []float64{
										1, 1, 0,
									},
									CounterResetHint: histogram.NotCounterReset,
								},
							},
						},
					},
				},
			},
		},
		"histograms: metric with stale marker": {
			expr: "histogram_with_stale_marker[3m1s]",
			ts:   baseT.Add(3 * time.Minute),
			expected: &promql.Result{
				Value: promql.Matrix{
					{
						Metric: labels.FromStrings("__name__", "histogram_with_stale_marker"),
						Histograms: []promql.HPoint{
							{
								T: timestamp.FromTime(baseT),
								H: &histogram.FloatHistogram{
									Sum:   1,
									Count: 1,
									PositiveSpans: []histogram.Span{
										{
											Offset: 0,
											Length: 2,
										},
									},
									PositiveBuckets: []float64{
										1, 0,
									},
									CounterResetHint: histogram.UnknownCounterReset,
								},
							},
							{
								T: timestamp.FromTime(baseT.Add(time.Minute)),
								H: &histogram.FloatHistogram{
									Sum:   2,
									Count: 2,
									PositiveSpans: []histogram.Span{
										{
											Offset: 0,
											Length: 2,
										},
									},
									PositiveBuckets: []float64{
										1, 1,
									},
									CounterResetHint: histogram.NotCounterReset,
								},
							},
							{
								T: timestamp.FromTime(baseT.Add(3 * time.Minute)),
								H: &histogram.FloatHistogram{
									Sum:   4,
									Count: 4,
									PositiveSpans: []histogram.Span{
										{
											Offset: 0,
											Length: 4,
										},
									},
									PositiveBuckets: []float64{
										1, 1, 1, 1,
									},
									CounterResetHint: histogram.UnknownCounterReset,
								},
							},
						},
					},
				},
			},
		},
		"mixed series with histograms and floats": {
			expr: "mixed_metric[4m]",
			ts:   baseT.Add(4 * time.Minute),
			expected: &promql.Result{
				Value: promql.Matrix{
					{
						Metric: labels.FromStrings("__name__", "mixed_metric"),
						Floats: []promql.FPoint{
							{
								T: timestamp.FromTime(baseT.Add(time.Minute)),
								F: 1,
							},
							{
								T: timestamp.FromTime(baseT.Add(2 * time.Minute)),
								F: 2,
							},
						},
						Histograms: []promql.HPoint{
							{
								T: timestamp.FromTime(baseT.Add(3 * time.Minute)),
								H: &histogram.FloatHistogram{
									Sum:   3,
									Count: 3,
									PositiveSpans: []histogram.Span{
										{
											Offset: 0,
											Length: 3,
										},
									},
									PositiveBuckets: []float64{
										1, 2, 1,
									},
								},
							},
						},
					},
				},
			},
		},
		"mixed series with a histogram then a float": {
			// This is unexpected, but consistent behavior between the engines
			// See: https://github.com/prometheus/prometheus/issues/14172
			expr: "mixed_metric_histogram_first[2m]",
			ts:   baseT.Add(2 * time.Minute),
			expected: &promql.Result{
				Value: promql.Matrix{
					{
						Metric: labels.FromStrings("__name__", "mixed_metric_histogram_first"),
						Floats: []promql.FPoint{
							{
								T: timestamp.FromTime(baseT.Add(time.Minute)),
								F: 1,
							},
						},
					},
				},
			},
		},
		"mixed series with a float then a histogram": {
			// No incorrect lookback
			expr: "mixed_metric_float_first[2m1s]",
			ts:   baseT.Add(2 * time.Minute),
			expected: &promql.Result{
				Value: promql.Matrix{
					{
						Metric: labels.FromStrings("__name__", "mixed_metric_float_first"),
						Floats: []promql.FPoint{
							{
								T: timestamp.FromTime(baseT),
								F: 1,
							},
						},
						Histograms: []promql.HPoint{
							{
								T: timestamp.FromTime(baseT.Add(time.Minute)),
								H: &histogram.FloatHistogram{
									Sum:   4,
									Count: 4,
									PositiveSpans: []histogram.Span{
										{
											Offset: 0,
											Length: 3,
										},
									},
									PositiveBuckets: []float64{
										1, 2, 1,
									},
								},
							},
						},
					},
				},
			},
		},
		"selector with positive offset (looking backwards)": {
			expr: "some_metric[1m1s] offset 1m",
			ts:   baseT.Add(3 * time.Minute),
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
		"selector with negative offset (looking forwards)": {
			expr: "some_metric[1m1s] offset -1m",
			ts:   baseT.Add(1 * time.Minute),
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
		"selector with offset to before beginning of available data": {
			expr: "some_metric[1m] offset 10m",
			ts:   baseT.Add(2 * time.Minute),
			expected: &promql.Result{
				Value: promql.Matrix{},
			},
		},
		"selector with offset to after end of available data": {
			expr: "some_metric[1m] offset -20m",
			ts:   baseT.Add(2 * time.Minute),
			expected: &promql.Result{
				Value: promql.Matrix{},
			},
		},
		"selector with @ modifier": {
			expr: "some_metric[1m1s] @ 2m",
			ts:   baseT.Add(20 * time.Minute),
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
		"selector with @ modifier and subquery 1m resolution": {
			expr: "some_metric[1m1s:1m] @ 2m",
			ts:   baseT.Add(20 * time.Minute),
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
		"selector with @ modifier and subquery 30s resolution": {
			expr: "some_metric[1m1s:30s] @ 2m",
			ts:   baseT.Add(20 * time.Minute),
			expected: &promql.Result{
				Value: promql.Matrix{
					{
						Metric: labels.FromStrings("__name__", "some_metric", "env", "1"),
						Floats: []promql.FPoint{
							{T: timestamp.FromTime(baseT.Add(time.Second * 60)), F: 1},
							{T: timestamp.FromTime(baseT.Add(time.Second * 90)), F: 1},
							{T: timestamp.FromTime(baseT.Add(time.Second * 120)), F: 2},
						},
					},
					{
						Metric: labels.FromStrings("__name__", "some_metric", "env", "2"),
						Floats: []promql.FPoint{
							{T: timestamp.FromTime(baseT.Add(time.Second * 60)), F: 2},
							{T: timestamp.FromTime(baseT.Add(time.Second * 90)), F: 2},
							{T: timestamp.FromTime(baseT.Add(time.Second * 120)), F: 4},
						},
					},
				},
			},
		},
		"selector with @ modifier and offset": {
			expr: "some_metric[1m1s] @ 3m offset 1m",
			ts:   baseT.Add(20 * time.Minute),
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
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			runTest := func(t *testing.T, eng promql.QueryEngine, expr string, ts time.Time, expected *promql.Result) {
				q, err := eng.NewInstantQuery(context.Background(), storage, nil, expr, ts)
				require.NoError(t, err)
				defer q.Close()

				res := q.Exec(context.Background())

				// Because Histograms are pointers, it is hard to use Equal for the whole result
				// Instead, compare each point individually.
				expectedMatrix := expected.Value.(promql.Matrix)
				actualMatrix := res.Value.(promql.Matrix)
				require.Equal(t, expectedMatrix.Len(), actualMatrix.Len(), "Result has incorrect number of series")
				for seriesIdx, expectedSeries := range expectedMatrix {
					actualSeries := actualMatrix[seriesIdx]

					if expectedSeries.Histograms == nil {
						require.Equalf(t, expectedSeries, actualSeries, "Result for series does not match expected value")
					} else {
						require.Equal(t, expectedSeries.Metric, actualSeries.Metric, "Metric does not match expected value")
						require.Equal(t, expectedSeries.Floats, actualSeries.Floats, "Float samples do not match expected samples")
						require.Lenf(t, actualSeries.Histograms, len(expectedSeries.Histograms), "Number of histogram samples does not match expected result (%v)", expectedSeries.Histograms)

						for sampleIdx := range expectedSeries.Histograms {
							require.EqualValuesf(
								t,
								expectedSeries.Histograms[sampleIdx].H,
								actualSeries.Histograms[sampleIdx].H,
								"Histogram samples for %v do not match expected result. First difference is at sample index %v. Expected: %v, actual: %v",
								expectedSeries.Metric,
								sampleIdx,
								expectedSeries.Histograms,
								actualSeries.Histograms,
							)
						}
					}
				}
			}

			t.Run(mimirEngineName, func(t *testing.T) {
				runTest(t, mimirEngine, testCase.expr, testCase.ts, testCase.expected)
			})

			// Run the tests against Prometheus' engine to ensure our test cases are valid.
			t.Run(prometheusEngineName, func(t *testing.T) {
				runTest(t, prometheusEngine, testCase.expr, testCase.ts, testCase.expected)
			})
		})
	}
}

func TestSubqueries(t *testing.T) {
	// This test is based on Prometheus' TestSubquerySelector.
	data := `load 10s
	           metric{type="floats"} 1 2
	           metric{type="histograms"} {{count:1}} {{count:2}}
	           http_requests{job="api-server", instance="0", group="production"} 0+10x1000 100+30x1000
	           http_requests{job="api-server", instance="1", group="production"} 0+20x1000 200+30x1000
	           http_requests{job="api-server", instance="0", group="canary"}     0+30x1000 300+80x1000
	           http_requests{job="api-server", instance="1", group="canary"}     0+40x2000
	           other_metric{type="floats"} 0 4 3 6 -1 10
	           other_metric{type="histograms"} {{count:0}} {{count:4}} {{count:3}} {{count:6}} {{count:1}} {{count:10}}
	           other_metric{type="mixed"} 0 4 3 6 {{count:1}} {{count:10}}
	`

	opts := NewTestEngineOpts()
	prometheusEngine := promql.NewEngine(opts.CommonOpts)
	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	mimirEngine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	storage := promqltest.LoadedStorage(t, data)
	t.Cleanup(func() { storage.Close() })

	testCases := []struct {
		Query  string
		Result promql.Result
		Start  time.Time
	}{
		{
			Query: "metric[20s:10s]",
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 1, T: 0}, {F: 2, T: 10000}},
						Metric: labels.FromStrings("__name__", "metric", "type", "floats"),
					},
					promql.Series{
						Histograms: []promql.HPoint{{H: &histogram.FloatHistogram{Count: 1}, T: 0}, {H: &histogram.FloatHistogram{Count: 2}, T: 10000}},
						Metric:     labels.FromStrings("__name__", "metric", "type", "histograms"),
					},
				},
			},
			Start: time.Unix(10, 0),
		},
		{
			// A query where SeriesMetadata returns some series but evaluates to no samples should not return anything.
			Query: `(metric{type="floats"} > Inf)[20s:10s]`,
			Start: time.Unix(30, 0),
			Result: promql.Result{
				Value: promql.Matrix{},
			},
		},
		{
			// A nested subquery with the same properties as above.
			Query: `last_over_time((metric{type="floats"} > Inf)[20s:10s])[30s:5s]`,
			Start: time.Unix(30, 0),
			Result: promql.Result{
				Value: promql.Matrix{},
			},
		},
		{
			Query: "metric[20s:5s]",
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 1, T: 0}, {F: 1, T: 5000}, {F: 2, T: 10000}},
						Metric: labels.FromStrings("__name__", "metric", "type", "floats"),
					},
					promql.Series{
						Histograms: []promql.HPoint{{H: &histogram.FloatHistogram{Count: 1}, T: 0}, {H: &histogram.FloatHistogram{Count: 1}, T: 5000}, {H: &histogram.FloatHistogram{Count: 2}, T: 10000}},
						Metric:     labels.FromStrings("__name__", "metric", "type", "histograms"),
					},
				},
			},
			Start: time.Unix(10, 0),
		},
		{
			Query: "metric[20s:5s] @ end()",
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 1, T: 0}, {F: 1, T: 5000}, {F: 2, T: 10000}},
						Metric: labels.FromStrings("__name__", "metric", "type", "floats"),
					},
					promql.Series{
						Histograms: []promql.HPoint{{H: &histogram.FloatHistogram{Count: 1}, T: 0}, {H: &histogram.FloatHistogram{Count: 1}, T: 5000}, {H: &histogram.FloatHistogram{Count: 2}, T: 10000}},
						Metric:     labels.FromStrings("__name__", "metric", "type", "histograms"),
					},
				},
			},
			Start: time.Unix(10, 0),
		},
		{
			// subquery is evaluated from 0 - so only a single sample is found
			Query: "metric[20s:5s] @ 0",
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 1, T: 0}},
						Metric: labels.FromStrings("__name__", "metric", "type", "floats"),
					},
					promql.Series{
						Histograms: []promql.HPoint{{H: &histogram.FloatHistogram{Count: 1}, T: 0}},
						Metric:     labels.FromStrings("__name__", "metric", "type", "histograms"),
					},
				},
			},
			Start: time.Unix(10, 0),
		},
		{
			// subquery is evaluated from 60s - the 2nd (and last) sample falls within the lookback window so this value is used
			Query: "metric[20s:5s] @ 60s",
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 2, T: 45000}, {F: 2, T: 50000}, {F: 2, T: 55000}, {F: 2, T: 60000}},
						Metric: labels.FromStrings("__name__", "metric", "type", "floats"),
					},
					promql.Series{
						Histograms: []promql.HPoint{{H: &histogram.FloatHistogram{Count: 2}, T: 45000}, {H: &histogram.FloatHistogram{Count: 2}, T: 50000}, {H: &histogram.FloatHistogram{Count: 2}, T: 55000}, {H: &histogram.FloatHistogram{Count: 2}, T: 60000}},
						Metric:     labels.FromStrings("__name__", "metric", "type", "histograms"),
					},
				},
			},
			Start: time.Unix(10, 0),
		},
		{
			// subquery is evaluated from 60m - no samples are recorded 20s back from 60m and this is outside the lookback window so no results are found
			Query: "metric[20s:5s] @ 60m",
			Result: promql.Result{
				Value: promql.Matrix{},
			},
			Start: time.Unix(10, 0),
		},
		{
			Query: "metric[20s:5s] offset 2s",
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 1, T: 0}, {F: 1, T: 5000}, {F: 2, T: 10000}},
						Metric: labels.FromStrings("__name__", "metric", "type", "floats"),
					},
					promql.Series{
						Histograms: []promql.HPoint{{H: &histogram.FloatHistogram{Count: 1}, T: 0}, {H: &histogram.FloatHistogram{Count: 1}, T: 5000}, {H: &histogram.FloatHistogram{Count: 2}, T: 10000}},
						Metric:     labels.FromStrings("__name__", "metric", "type", "histograms"),
					},
				},
			},
			Start: time.Unix(12, 0),
		},
		{
			Query: "metric[20s:5s] offset 6s",
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 1, T: 0}, {F: 1, T: 5000}, {F: 2, T: 10000}},
						Metric: labels.FromStrings("__name__", "metric", "type", "floats"),
					},
					promql.Series{
						Histograms: []promql.HPoint{{H: &histogram.FloatHistogram{Count: 1}, T: 0}, {H: &histogram.FloatHistogram{Count: 1}, T: 5000}, {H: &histogram.FloatHistogram{Count: 2}, T: 10000}},
						Metric:     labels.FromStrings("__name__", "metric", "type", "histograms"),
					},
				},
			},
			Start: time.Unix(20, 0),
		},
		{
			Query: "metric[20s:5s] offset 4s",
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 2, T: 15000}, {F: 2, T: 20000}, {F: 2, T: 25000}, {F: 2, T: 30000}},
						Metric: labels.FromStrings("__name__", "metric", "type", "floats"),
					},
					promql.Series{
						Histograms: []promql.HPoint{{H: &histogram.FloatHistogram{Count: 2}, T: 15000}, {H: &histogram.FloatHistogram{Count: 2}, T: 20000}, {H: &histogram.FloatHistogram{Count: 2}, T: 25000}, {H: &histogram.FloatHistogram{Count: 2}, T: 30000}},
						Metric:     labels.FromStrings("__name__", "metric", "type", "histograms"),
					},
				},
			},
			Start: time.Unix(35, 0),
		},
		{
			Query: "metric[20s:5s]",
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 2, T: 15000}, {F: 2, T: 20000}, {F: 2, T: 25000}, {F: 2, T: 30000}},
						Metric: labels.FromStrings("__name__", "metric", "type", "floats"),
					},
					promql.Series{
						Histograms: []promql.HPoint{{H: &histogram.FloatHistogram{Count: 2}, T: 15000}, {H: &histogram.FloatHistogram{Count: 2}, T: 20000}, {H: &histogram.FloatHistogram{Count: 2}, T: 25000}, {H: &histogram.FloatHistogram{Count: 2}, T: 30000}},
						Metric:     labels.FromStrings("__name__", "metric", "type", "histograms"),
					},
				},
			},
			Start: time.Unix(30, 0),
		},
		{
			Query: "metric[20s:5s] offset 5s",
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 2, T: 15000}, {F: 2, T: 20000}, {F: 2, T: 25000}, {F: 2, T: 30000}},
						Metric: labels.FromStrings("__name__", "metric", "type", "floats"),
					},
					promql.Series{
						Histograms: []promql.HPoint{{H: &histogram.FloatHistogram{Count: 2}, T: 15000}, {H: &histogram.FloatHistogram{Count: 2}, T: 20000}, {H: &histogram.FloatHistogram{Count: 2}, T: 25000}, {H: &histogram.FloatHistogram{Count: 2}, T: 30000}},
						Metric:     labels.FromStrings("__name__", "metric", "type", "histograms"),
					},
				},
			},
			Start: time.Unix(35, 0),
		},
		{
			Query: "metric[20s:5s] offset 6s",
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 2, T: 10000}, {F: 2, T: 15000}, {F: 2, T: 20000}, {F: 2, T: 25000}},
						Metric: labels.FromStrings("__name__", "metric", "type", "floats"),
					},
					promql.Series{
						Histograms: []promql.HPoint{{H: &histogram.FloatHistogram{Count: 2}, T: 10000}, {H: &histogram.FloatHistogram{Count: 2}, T: 15000}, {H: &histogram.FloatHistogram{Count: 2}, T: 20000}, {H: &histogram.FloatHistogram{Count: 2}, T: 25000}},
						Metric:     labels.FromStrings("__name__", "metric", "type", "histograms"),
					},
				},
			},
			Start: time.Unix(35, 0),
		},
		{
			Query: "metric[20s:5s] offset 7s",
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 2, T: 10000}, {F: 2, T: 15000}, {F: 2, T: 20000}, {F: 2, T: 25000}},
						Metric: labels.FromStrings("__name__", "metric", "type", "floats"),
					},
					promql.Series{
						Histograms: []promql.HPoint{{H: &histogram.FloatHistogram{Count: 2}, T: 10000}, {H: &histogram.FloatHistogram{Count: 2}, T: 15000}, {H: &histogram.FloatHistogram{Count: 2}, T: 20000}, {H: &histogram.FloatHistogram{Count: 2}, T: 25000}},
						Metric:     labels.FromStrings("__name__", "metric", "type", "histograms"),
					},
				},
			},
			Start: time.Unix(35, 0),
		},
		{ // Normal selector.
			Query: `http_requests{group=~"pro.*",instance="0"}[30s:10s]`,
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 10000, T: 10000000}, {F: 100, T: 10010000}, {F: 130, T: 10020000}},
						Metric: labels.FromStrings("__name__", "http_requests", "job", "api-server", "instance", "0", "group", "production"),
					},
				},
			},
			Start: time.Unix(10020, 0),
		},
		{ // Normal selector. Add 1ms to the range to see the legacy behavior of the previous test.
			Query: `http_requests{group=~"pro.*",instance="0"}[30s1ms:10s]`,
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 9990, T: 9990000}, {F: 10000, T: 10000000}, {F: 100, T: 10010000}, {F: 130, T: 10020000}},
						Metric: labels.FromStrings("__name__", "http_requests", "job", "api-server", "instance", "0", "group", "production"),
					},
				},
			},
			Start: time.Unix(10020, 0),
		},
		{ // Default step.
			Query: `http_requests{group=~"pro.*",instance="0"}[5m:]`,
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 9840, T: 9840000}, {F: 9900, T: 9900000}, {F: 9960, T: 9960000}, {F: 130, T: 10020000}, {F: 310, T: 10080000}},
						Metric: labels.FromStrings("__name__", "http_requests", "job", "api-server", "instance", "0", "group", "production"),
					},
				},
			},
			Start: time.Unix(10100, 0),
		},
		{ // Checking if high offset (>LookbackDelta) is being taken care of.
			Query: `http_requests{group=~"pro.*",instance="0"}[5m:] offset 20m`,
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 8640, T: 8640000}, {F: 8700, T: 8700000}, {F: 8760, T: 8760000}, {F: 8820, T: 8820000}, {F: 8880, T: 8880000}},
						Metric: labels.FromStrings("__name__", "http_requests", "job", "api-server", "instance", "0", "group", "production"),
					},
				},
			},
			Start: time.Unix(10100, 0),
		},
		{
			Query: `rate(http_requests[1m])[15s:5s]`,
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats:   []promql.FPoint{{F: 3, T: 7990000}, {F: 3, T: 7995000}, {F: 3, T: 8000000}},
						Metric:   labels.FromStrings("job", "api-server", "instance", "0", "group", "canary"),
						DropName: true,
					},
					promql.Series{
						Floats:   []promql.FPoint{{F: 4, T: 7990000}, {F: 4, T: 7995000}, {F: 4, T: 8000000}},
						Metric:   labels.FromStrings("job", "api-server", "instance", "1", "group", "canary"),
						DropName: true,
					},
					promql.Series{
						Floats:   []promql.FPoint{{F: 1, T: 7990000}, {F: 1, T: 7995000}, {F: 1, T: 8000000}},
						Metric:   labels.FromStrings("job", "api-server", "instance", "0", "group", "production"),
						DropName: true,
					},
					promql.Series{
						Floats:   []promql.FPoint{{F: 2, T: 7990000}, {F: 2, T: 7995000}, {F: 2, T: 8000000}},
						Metric:   labels.FromStrings("job", "api-server", "instance", "1", "group", "production"),
						DropName: true,
					},
				},
				Warnings: annotations.New().Add(annotations.NewPossibleNonCounterInfo("http_requests", posrange.PositionRange{Start: 5})),
			},
			Start: time.Unix(8000, 0),
		},
		{
			Query: `rate(http_requests[1m])[15s1ms:5s]`, // Add 1ms to the range to see the legacy behavior of the previous test.
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats:   []promql.FPoint{{F: 3, T: 7985000}, {F: 3, T: 7990000}, {F: 3, T: 7995000}, {F: 3, T: 8000000}},
						Metric:   labels.FromStrings("job", "api-server", "instance", "0", "group", "canary"),
						DropName: true,
					},
					promql.Series{
						Floats:   []promql.FPoint{{F: 4, T: 7985000}, {F: 4, T: 7990000}, {F: 4, T: 7995000}, {F: 4, T: 8000000}},
						Metric:   labels.FromStrings("job", "api-server", "instance", "1", "group", "canary"),
						DropName: true,
					},
					promql.Series{
						Floats:   []promql.FPoint{{F: 1, T: 7985000}, {F: 1, T: 7990000}, {F: 1, T: 7995000}, {F: 1, T: 8000000}},
						Metric:   labels.FromStrings("job", "api-server", "instance", "0", "group", "production"),
						DropName: true,
					},
					promql.Series{
						Floats:   []promql.FPoint{{F: 2, T: 7985000}, {F: 2, T: 7990000}, {F: 2, T: 7995000}, {F: 2, T: 8000000}},
						Metric:   labels.FromStrings("job", "api-server", "instance", "1", "group", "production"),
						DropName: true,
					},
				},
				Warnings: annotations.New().Add(annotations.NewPossibleNonCounterInfo("http_requests", posrange.PositionRange{Start: 5})),
			},
			Start: time.Unix(8000, 0),
		},
		{
			Query: `sum(http_requests{group=~"pro.*"})[30s:10s]`,
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 300, T: 100000}, {F: 330, T: 110000}, {F: 360, T: 120000}},
						Metric: labels.EmptyLabels(),
					},
				},
			},
			Start: time.Unix(120, 0),
		},
		{
			Query: `sum(http_requests{group=~"pro.*"})[30s:10s]`,
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 300, T: 100000}, {F: 330, T: 110000}, {F: 360, T: 120000}},
						Metric: labels.EmptyLabels(),
					},
				},
			},
			Start: time.Unix(121, 0), // 1s later doesn't change the result compared to above.
		},
		{
			Query: `sum(http_requests)[40s:10s]`,
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 900, T: 90000}, {F: 1000, T: 100000}, {F: 1100, T: 110000}, {F: 1200, T: 120000}},
						Metric: labels.EmptyLabels(),
					},
				},
			},
			Start: time.Unix(120, 0),
		},
		{
			Query: `sum(http_requests)[40s1ms:10s]`, // Add 1ms to the range to see the legacy behavior of the previous test.
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 800, T: 80000}, {F: 900, T: 90000}, {F: 1000, T: 100000}, {F: 1100, T: 110000}, {F: 1200, T: 120000}},
						Metric: labels.EmptyLabels(),
					},
				},
			},
			Start: time.Unix(120, 0),
		},
		{
			Query: `(sum(http_requests{group=~"p.*"})+sum(http_requests{group=~"c.*"}))[20s:5s]`,
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 1000, T: 105000}, {F: 1100, T: 110000}, {F: 1100, T: 115000}, {F: 1200, T: 120000}},
						Metric: labels.EmptyLabels(),
					},
				},
			},
			Start: time.Unix(120, 0),
		},
		{
			Query: `(sum(http_requests{group=~"p.*"})+sum(http_requests{group=~"c.*"}))[20s1ms:5s]`, // Add 1ms to the range to see the legacy behavior of the previous test.
			Result: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Floats: []promql.FPoint{{F: 1000, T: 100000}, {F: 1000, T: 105000}, {F: 1100, T: 110000}, {F: 1100, T: 115000}, {F: 1200, T: 120000}},
						Metric: labels.EmptyLabels(),
					},
				},
			},
			Start: time.Unix(120, 0),
		},
		// These tests exercise @ start() and @ end(), and use the same data as testdata/ours/subqueries.test, to
		// mirror the range query tests there.
		{
			Query: `last_over_time(other_metric[20s:10s] @ start())`,
			Result: promql.Result{
				Value: promql.Vector{
					{
						F:      -1,
						T:      40000,
						Metric: labels.FromStrings(model.MetricNameLabel, "other_metric", "type", "floats"),
					},
					{
						H:      &histogram.FloatHistogram{Count: 1, CounterResetHint: histogram.UnknownCounterReset},
						T:      40000,
						Metric: labels.FromStrings(model.MetricNameLabel, "other_metric", "type", "histograms"),
					},
					{
						H:      &histogram.FloatHistogram{Count: 1, CounterResetHint: histogram.UnknownCounterReset},
						T:      40000,
						Metric: labels.FromStrings(model.MetricNameLabel, "other_metric", "type", "mixed"),
					},
				},
			},
			Start: time.Unix(40, 0),
		},
		{
			Query: `last_over_time(other_metric[20s:10s] @ end())`,
			Result: promql.Result{
				Value: promql.Vector{
					{
						F:      6,
						T:      30000,
						Metric: labels.FromStrings(model.MetricNameLabel, "other_metric", "type", "floats"),
					},
					{
						H:      &histogram.FloatHistogram{Count: 6},
						T:      30000,
						Metric: labels.FromStrings(model.MetricNameLabel, "other_metric", "type", "histograms"),
					},
					{
						F:      6,
						T:      30000,
						Metric: labels.FromStrings(model.MetricNameLabel, "other_metric", "type", "mixed"),
					},
				},
			},
			Start: time.Unix(30, 0),
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%v evaluated at %v", testCase.Query, testCase.Start.Unix()), func(t *testing.T) {
			runTest := func(t *testing.T, engine promql.QueryEngine) {
				qry, err := engine.NewInstantQuery(context.Background(), storage, nil, testCase.Query, testCase.Start)
				require.NoError(t, err)

				res := qry.Exec(context.Background())
				mqetest.RequireEqualResults(t, testCase.Query, &testCase.Result, res, false)
				qry.Close()
			}

			t.Run(mimirEngineName, func(t *testing.T) {
				runTest(t, mimirEngine)
			})

			// Ensure our test cases are correct by running them against Prometheus' engine too.
			t.Run(prometheusEngineName, func(t *testing.T) {
				runTest(t, prometheusEngine)
			})
		})
	}
}

func TestQueryCancellation(t *testing.T) {
	opts := NewTestEngineOpts()
	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
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
	opts.CommonOpts.Timeout = 20 * time.Millisecond
	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	// Simulate the query doing some work and check that the query context has been cancelled.
	//
	// In both this test and production, we rely on the underlying storage responding to the context cancellation -
	// we don't explicitly check for context cancellation in the query engine.
	var q promql.Query
	queryable := cancellationQueryable{func() {
		time.Sleep(opts.CommonOpts.Timeout * 10)
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
	return cancellationQuerier(w), nil
}

type cancellationQuerier struct {
	onQueried func()
}

func (w cancellationQuerier) LabelValues(ctx context.Context, _ string, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, w.waitForCancellation(ctx)
}

func (w cancellationQuerier) LabelNames(ctx context.Context, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
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
	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
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

func (q *contextCapturingQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	q.queryable.capturedContext = ctx
	return q.inner.LabelValues(ctx, name, hints, matchers...)
}

func (q *contextCapturingQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	q.queryable.capturedContext = ctx
	return q.inner.LabelNames(ctx, hints, matchers...)
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
			some_histogram{idx="1"} {{schema:1 sum:10 count:9 buckets:[3 3 3]}}x5
			some_histogram{idx="2"} {{schema:1 sum:10 count:9 buckets:[3 3 3]}}x5
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

			// Each series has five samples, which will be rounded up to eight (the nearest power of two) by the bucketed pool,
			// and we have five series and each of the series has labels of the same size.
			rangeQueryExpectedPeak: 5*8*types.FPointSize + 8*types.SeriesMetadataSize + 5*uint64(labels.FromStrings(model.MetricNameLabel, "some_metric", "idx", "i").ByteSize()),
			rangeQueryLimit:        0,

			// At peak, we'll hold all the output samples plus one series, which has one sample.
			// The output contains five samples with SeriesMetadata, which will be rounded up to eight (the nearest power of two).
			// Five out of SeriesMetadata has labels.Labels with each of them having the same ByteSize.
			instantQueryExpectedPeak: types.FPointSize + 8*(types.VectorSampleSize+types.SeriesMetadataSize) + 5*uint64(labels.FromStrings(model.MetricNameLabel, "some_metric", "idx", "i").ByteSize()),
			instantQueryLimit:        0,
		},
		"limit enabled, but query does not exceed limit": {
			expr:          "some_metric",
			shouldSucceed: true,

			// Each series has five samples with SeriesMetadata, which will be rounded up to 8 (the nearest power of two) by the bucketed pool, and we have five series.
			// Five out of SeriesMetadata has labels.Labels with each of them having the same ByteSize.
			rangeQueryExpectedPeak: 5*8*types.FPointSize + 8*types.SeriesMetadataSize + 5*uint64(labels.FromStrings(model.MetricNameLabel, "some_metric", "idx", "i").ByteSize()),
			rangeQueryLimit:        5*8*types.FPointSize + 8*types.SeriesMetadataSize + 5*uint64(labels.FromStrings(model.MetricNameLabel, "some_metric", "idx", "i").ByteSize()),

			// At peak, we'll hold all the output samples plus one series, which has one sample.
			// The output contains five samples with SeriesMetadata, which will be rounded up to 8 (the nearest power of two).
			// Five out of SeriesMetadata has labels.Labels with each of them having the same ByteSize.
			instantQueryExpectedPeak: types.FPointSize + 8*(types.VectorSampleSize+types.SeriesMetadataSize) + 5*uint64(labels.FromStrings(model.MetricNameLabel, "some_metric", "idx", "i").ByteSize()),
			instantQueryLimit:        types.FPointSize + 8*(types.VectorSampleSize+types.SeriesMetadataSize) + 5*uint64(labels.FromStrings(model.MetricNameLabel, "some_metric", "idx", "i").ByteSize()),
		},
		"limit enabled, and query exceeds limit": {
			expr:          "some_metric",
			shouldSucceed: false,

			// Allow only a single sample.
			rangeQueryLimit:   types.FPointSize,
			instantQueryLimit: types.FPointSize,

			// The query never successfully allocates anything.
			rangeQueryExpectedPeak:   0,
			instantQueryExpectedPeak: 0,
		},
		"limit enabled, query selects more samples than limit but should not load all of them into memory at once, and peak consumption is under limit": {
			expr:          "sum(some_metric)",
			shouldSucceed: true,

			// There are two stages to processing the query. They take different memory depending on whether we're running with stringlabels or not.
			// At peak we'll hold in memory either A) or B)
			rangeQueryExpectedPeak: max(
				// A)
				//   - 5 input series labels (8 series metadata because of bucketed pool rounding to a power of 2)
				//   - 1 output series metadata (no labels)
				8*types.SeriesMetadataSize+5*uint64(labels.FromStrings(model.MetricNameLabel, "some_metric", "idx", "i").ByteSize())+types.SeriesMetadataSize,
				// B)
				//   - the running total for the sum() (two floats (due to kahan) and a bool at each step, with the number of steps rounded to the nearest power of 2),
				//   - the next series from the selector
				//   - the series metadata for the output series (no labels)
				8*(2*types.Float64Size+types.BoolSize)+8*types.FPointSize+types.SeriesMetadataSize,
			),
			rangeQueryLimit: max(
				// A)
				8*types.SeriesMetadataSize+5*uint64(labels.FromStrings(model.MetricNameLabel, "some_metric", "idx", "i").ByteSize())+types.SeriesMetadataSize,
				// B)
				8*(2*types.Float64Size+types.BoolSize)+8*types.FPointSize+types.SeriesMetadataSize,
			),

			// Each series has one sample, which is already a power of two.
			// At peak we'll hold in memory 9 SeriesMetadata.
			instantQueryExpectedPeak: 9*types.SeriesMetadataSize + 5*uint64(labels.FromStrings(model.MetricNameLabel, "some_metric", "idx", "i").ByteSize()),
			instantQueryLimit:        9*types.SeriesMetadataSize + 5*uint64(labels.FromStrings(model.MetricNameLabel, "some_metric", "idx", "i").ByteSize()),
		},
		"limit enabled, query selects more samples than limit but should not load all of them into memory at once, and peak consumption is over limit": {
			expr:          "sum(some_metric)",
			shouldSucceed: false,

			// At peak we'll hold in memory
			//   - 5 input series labels (8 series metadata because of bucketed pool rounding to a power of 2)
			//   - 1 output series metadata (no labels). This will tip over the limit and we won't allocate it, so the peak calculations don't include it.
			rangeQueryExpectedPeak: 8*types.SeriesMetadataSize + 5*uint64(labels.FromStrings(model.MetricNameLabel, "some_metric", "idx", "i").ByteSize()),
			rangeQueryLimit:        9*types.SeriesMetadataSize + 5*uint64(labels.FromStrings(model.MetricNameLabel, "some_metric", "idx", "i").ByteSize()) - 1,

			instantQueryExpectedPeak: 8*types.SeriesMetadataSize + 5*uint64(labels.FromStrings(model.MetricNameLabel, "some_metric", "idx", "i").ByteSize()),
			instantQueryLimit:        9*types.SeriesMetadataSize + 5*uint64(labels.FromStrings(model.MetricNameLabel, "some_metric", "idx", "i").ByteSize()) - 1,
		},
		"histogram: limit enabled, but query does not exceed limit": {
			expr:          "sum(some_histogram)",
			shouldSucceed: true,

			rangeQueryExpectedPeak: 2*8*types.HistogramPointerSize + 8*types.HPointSize + types.SeriesMetadataSize + 8*types.CounterResetHintSize,
			rangeQueryLimit:        2*8*types.HistogramPointerSize + 8*types.HPointSize + types.SeriesMetadataSize + 8*types.CounterResetHintSize,

			instantQueryExpectedPeak: types.HPointSize + types.VectorSampleSize + types.SeriesMetadataSize,
			instantQueryLimit:        types.HPointSize + types.VectorSampleSize + types.SeriesMetadataSize,
		},
		"histogram: limit enabled, and query exceeds limit": {
			expr:          "sum(some_histogram)",
			shouldSucceed: false,

			// Each series has five samples, which will be rounded up to 8 (the nearest power of two) by the bucketed pool.
			// At peak we'll hold in memory:
			//  - the running total for the sum() (a histogram pointer at each step, with the number of steps rounded to the nearest power of 2),
			//  - and the next series from the selector.
			// The last thing to be allocated is the HistogramPointerSize slice for the running total, so that won't contribute to the peak before the query is aborted.
			rangeQueryExpectedPeak: 8*types.HPointSize + types.SeriesMetadataSize,
			rangeQueryLimit:        8*types.HPointSize + types.SeriesMetadataSize + 8*types.HistogramPointerSize - 1,
			// Each series has one sample, which is already a power of two.
			// At peak we'll hold in memory:
			//  - the running total for the sum() + compensating value for kahan summation
			//    (2 histogram pointers),
			//  - the next series from the selector,
			//  - and the output sample.
			// The last thing to be allocated is the vector slice for the final result (after the sum()'s running total has been returned), so those won't contribute to the peak before the query is aborted.
			instantQueryExpectedPeak: types.HPointSize + types.SeriesMetadataSize + 2*types.HistogramPointerSize + types.CounterResetHintSize,
			instantQueryLimit:        types.HPointSize + types.SeriesMetadataSize + types.VectorSampleSize - 1,
		},
	}

	createEngine := func(t *testing.T, limit uint64) (promql.QueryEngine, *prometheus.Registry) {
		reg := prometheus.NewPedanticRegistry()
		opts := NewTestEngineOpts()
		opts.CommonOpts.Reg = reg
		limits := NewStaticQueryLimitsProvider()
		limits.MaxEstimatedMemoryConsumptionPerQuery = limit
		opts.Limits = limits

		planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
		require.NoError(t, err)
		engine, err := NewEngine(opts, stats.NewQueryMetrics(reg), planner)
		require.NoError(t, err)

		spanExporter.Reset()
		return engine, reg
	}

	start := timestamp.Time(0)
	end := start.Add(4 * time.Minute)
	step := time.Minute
	ctx := context.Background()

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			queryTypes := map[string]func(t *testing.T) (promql.Query, *prometheus.Registry, uint64, uint64){
				"range": func(t *testing.T) (promql.Query, *prometheus.Registry, uint64, uint64) {
					engine, reg := createEngine(t, testCase.rangeQueryLimit)
					q, err := engine.NewRangeQuery(ctx, storage, nil, testCase.expr, start, end, step)
					require.NoError(t, err)
					return q, reg, testCase.rangeQueryLimit, testCase.rangeQueryExpectedPeak
				},
				"instant": func(t *testing.T) (promql.Query, *prometheus.Registry, uint64, uint64) {
					engine, reg := createEngine(t, testCase.instantQueryLimit)
					q, err := engine.NewInstantQuery(ctx, storage, nil, testCase.expr, start)
					require.NoError(t, err)
					return q, reg, testCase.instantQueryLimit, testCase.instantQueryExpectedPeak
				},
			}

			for queryType, createQuery := range queryTypes {
				t.Run(queryType, func(t *testing.T) {
					q, reg, memoryConsumptionLimit, expectedPeakMemoryConsumption := createQuery(t)
					t.Cleanup(q.Close)

					res := q.Exec(ctx)

					if testCase.shouldSucceed {
						assert.NoError(t, res.Err)
						assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(rejectedMetrics(0)), "cortex_querier_queries_rejected_total"))
					} else {
						assert.ErrorContains(t, res.Err, globalerror.MaxEstimatedMemoryConsumptionPerQuery.Error())
						assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(rejectedMetrics(1)), "cortex_querier_queries_rejected_total"))
					}

					spanStubs := filter(spanExporter.GetSpans(), func(stub tracetest.SpanStub) bool {
						return stub.Name == "Evaluator.Evaluate"
					})
					require.Len(t, spanStubs, 1)
					assertEstimatedPeakMemoryConsumption(
						t,
						reg,
						spanStubs[0],
						memoryConsumptionLimit,
						expectedPeakMemoryConsumption,
						queryType,
						testCase.expr,
						start,
						end,
						step,
						testCase.shouldSucceed,
					)
				})
			}
		})
	}
}

func filter[T any](slice []T, fn func(T) bool) []T {
	var result []T
	for _, item := range slice {
		if fn(item) {
			result = append(result, item)
		}
	}
	return result
}

func assertEstimatedPeakMemoryConsumption(
	t *testing.T,
	reg *prometheus.Registry,
	span tracetest.SpanStub,
	memoryConsumptionLimit, expectedMemoryConsumptionEstimate uint64,
	queryType string,
	expr string,
	start time.Time,
	end time.Time,
	step time.Duration,
	shouldSucceed bool,
) {
	peakMemoryConsumptionHistogram := getHistogram(t, reg, "cortex_mimir_query_engine_estimated_query_peak_memory_consumption")
	require.Equal(t, float64(expectedMemoryConsumptionEstimate), peakMemoryConsumptionHistogram.GetSampleSum())

	require.NotEmpty(t, span.Events, "There should be events in the span.")

	logEvents := filter(span.Events, func(e tracesdk.Event) bool {
		return e.Name == "log" && slices.Contains(e.Attributes, attribute.String("msg", "evaluation stats"))
	})
	require.Len(t, logEvents, 1, "There should be exactly one log event in the span.")
	logEvent := logEvents[0]
	expectedFields := []attribute.KeyValue{
		attribute.String("level", "info"),
		attribute.String("msg", "evaluation stats"),
		attribute.Int64("estimatedPeakMemoryConsumption", int64(expectedMemoryConsumptionEstimate)),
		attribute.String("originalExpression", expr),
		attribute.String("timeRangeType", queryType),
		attribute.Int("nodeCount", 1),
	}

	switch queryType {
	case "instant":
		expectedFields = append(expectedFields,
			attribute.Int64("time", start.UnixMilli()),
		)
	case "range":
		expectedFields = append(expectedFields,
			attribute.Int64("start", start.UnixMilli()),
			attribute.Int64("end", end.UnixMilli()),
			attribute.Int64("step", step.Milliseconds()),
		)
	default:
		panic(fmt.Sprintf("unknown query type: %s", queryType))
	}

	if shouldSucceed {
		expectedFields = append(expectedFields, attribute.String("status", "success"))
	} else {
		expectedFields = append(expectedFields,
			attribute.String("status", "failed"),
			attribute.String("err", limiter.NewMaxEstimatedMemoryConsumptionPerQueryLimitError(memoryConsumptionLimit).Error()),
		)
	}

	require.ElementsMatch(t, expectedFields, logEvent.Attributes)
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
	opts.CommonOpts.Reg = reg

	limit := 32*types.FPointSize + 4*types.SeriesMetadataSize + 3*uint64(labels.FromStrings(model.MetricNameLabel, "some_metric", "idx", "i").ByteSize())
	limits := NewStaticQueryLimitsProvider()
	limits.MaxEstimatedMemoryConsumptionPerQuery = limit
	opts.Limits = limits
	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engine, err := NewEngine(opts, stats.NewQueryMetrics(reg), planner)
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

func TestEvaluator_ReportsMemoryConsumptionLimit(t *testing.T) {
	spanExporter.Reset()

	storage := promqltest.LoadedStorage(t, `
		load 1m
			some_metric 0+1x5
	`)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	reg := prometheus.NewPedanticRegistry()
	opts := NewTestEngineOpts()
	opts.CommonOpts.Reg = reg

	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engine, err := NewEngine(opts, stats.NewQueryMetrics(reg), planner)
	require.NoError(t, err)

	ctx := context.Background()
	expr := "some_metric"
	plan, err := planner.NewQueryPlan(ctx, expr, types.NewInstantQueryTimeRange(timestamp.Time(0)), false, NoopPlanningObserver{})
	require.NoError(t, err)

	evaluator, err := engine.NewEvaluator(ctx, storage, nil, plan.Parameters, []NodeEvaluationRequest{{Node: plan.Root, TimeRange: plan.Parameters.TimeRange}})
	require.NoError(t, err)

	err = evaluator.Evaluate(ctx, noopEvaluationObserver{})
	require.NoError(t, err)

	spanStubs := filter(spanExporter.GetSpans(), func(stub tracetest.SpanStub) bool {
		return stub.Name == "Evaluator.Evaluate"
	})
	require.Len(t, spanStubs, 1)
	assertEstimatedPeakMemoryConsumption(
		t,
		reg,
		spanStubs[0],
		0,
		61,
		"instant",
		expr,
		timestamp.Time(0),
		timestamp.Time(0),
		0,
		true,
	)
}

type noopEvaluationObserver struct{}

func (n noopEvaluationObserver) SeriesMetadataEvaluated(ctx context.Context, evaluator *Evaluator, node planning.Node, series []types.SeriesMetadata) error {
	return nil
}

func (n noopEvaluationObserver) InstantVectorSeriesDataEvaluated(ctx context.Context, evaluator *Evaluator, node planning.Node, seriesIndex int, seriesCount int, seriesData types.InstantVectorSeriesData) error {
	return nil
}

func (n noopEvaluationObserver) RangeVectorStepSamplesEvaluated(ctx context.Context, evaluator *Evaluator, node planning.Node, seriesIndex int, stepIndex int, stepData *types.RangeVectorStepData) error {
	return nil
}

func (n noopEvaluationObserver) ScalarEvaluated(ctx context.Context, evaluator *Evaluator, node planning.Node, data types.ScalarData) error {
	return nil
}

func (n noopEvaluationObserver) StringEvaluated(ctx context.Context, evaluator *Evaluator, node planning.Node, data string) error {
	return nil
}

func (n noopEvaluationObserver) EvaluationCompleted(ctx context.Context, evaluator *Evaluator, annotations *annotations.Annotations, stats *types.QueryStats) error {
	return nil
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

func getMetrics(t *testing.T, reg *prometheus.Registry, name string) []*dto.Metric {
	metrics, err := reg.Gather()
	require.NoError(t, err)

	for _, m := range metrics {
		if m.GetName() == name {
			return m.Metric
		}
	}

	require.Fail(t, "expected to find a metric with name "+name)
	return nil
}

func getHistogram(t *testing.T, reg *prometheus.Registry, name string) *dto.Histogram {
	m := getMetrics(t, reg, name)
	require.Len(t, m, 1)

	return m[0].Histogram
}

func TestActiveQueryTracker_SuccessfulQuery(t *testing.T) {
	opts := NewTestEngineOpts()
	tracker := &testQueryTracker{}
	opts.ActiveQueryTracker = tracker
	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	testActiveQueryTracker(
		t, engine, tracker,
		trackedQuery{expr: "test_query", stage: "planning", deleted: true},
		trackedQuery{expr: "test_query", stage: "materialization", deleted: true},
	)
}

func testActiveQueryTracker(t *testing.T, engine *Engine, tracker *testQueryTracker, expectedCreationActivities ...trackedQuery) {
	innerStorage := promqltest.LoadedStorage(t, "")
	t.Cleanup(func() { require.NoError(t, innerStorage.Close()) })

	// Use a fake queryable as a way to check that the query is recorded as active while the query is in progress.
	queryTrackingTestingQueryable := &activeQueryTrackerQueryable{
		innerStorage: innerStorage,
		tracker:      tracker,
	}

	queryTypes := map[string]func(expr string) (promql.Query, types.QueryTimeRange, error){
		"range": func(expr string) (promql.Query, types.QueryTimeRange, error) {
			start := timestamp.Time(0)
			end := timestamp.Time(0).Add(time.Hour)
			step := time.Minute
			q, err := engine.NewRangeQuery(context.Background(), queryTrackingTestingQueryable, nil, expr, start, end, step)

			return q, types.NewRangeQueryTimeRange(start, end, step), err
		},
		"instant": func(expr string) (promql.Query, types.QueryTimeRange, error) {
			ts := timestamp.Time(0)
			q, err := engine.NewInstantQuery(context.Background(), queryTrackingTestingQueryable, nil, expr, ts)
			return q, types.NewInstantQueryTimeRange(ts), err
		},
	}

	for queryType, createQuery := range queryTypes {
		t.Run(queryType+" query", func(t *testing.T) {
			expr := "test_query"
			queryTrackingTestingQueryable.activeQueryAtQueryTime = trackedQuery{}
			tracker.Clear()

			q, timeRange, err := createQuery(expr)
			require.NoError(t, err)
			defer q.Close()

			for i := range expectedCreationActivities {
				expectedCreationActivities[i].timeRange = timeRange
			}

			require.Equal(t, expectedCreationActivities, tracker.queries)

			res := q.Exec(context.Background())
			require.NoError(t, res.Err)

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
}

func TestActiveQueryTracker_FailedQuery(t *testing.T) {
	opts := NewTestEngineOpts()
	tracker := &testQueryTracker{}
	opts.ActiveQueryTracker = tracker
	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	innerStorage := promqltest.LoadedStorage(t, "")
	t.Cleanup(func() { require.NoError(t, innerStorage.Close()) })

	// Use a fake queryable as a way to check that the query is recorded as active while the query is in progress,
	// and to inject an error that causes the query to fail.
	queryTrackingTestingQueryable := &activeQueryTrackerQueryable{
		innerStorage: innerStorage,
		tracker:      tracker,
		err:          errors.New("something went wrong inside the query"),
	}

	expr := "test_metric"
	q, err := engine.NewInstantQuery(context.Background(), queryTrackingTestingQueryable, nil, expr, timestamp.Time(0))
	require.NoError(t, err)
	defer q.Close()

	res := q.Exec(context.Background())
	require.EqualError(t, res.Err, "something went wrong inside the query")

	// Check that the query was active in the query tracker while the query was executing.
	require.Equal(t, expr, queryTrackingTestingQueryable.activeQueryAtQueryTime.expr)
	require.False(t, queryTrackingTestingQueryable.activeQueryAtQueryTime.deleted)

	// Check that the query has now been marked as deleted in the query tracker.
	require.NotEmpty(t, tracker.queries)
	trackedQuery := tracker.queries[len(tracker.queries)-1]
	require.Equal(t, expr, trackedQuery.expr)
	require.Equal(t, true, trackedQuery.deleted)
}

type testQueryTracker struct {
	queries []trackedQuery
}

type trackedQuery struct {
	expr      string
	stage     string
	timeRange types.QueryTimeRange
	deleted   bool
}

func (qt *testQueryTracker) GetMaxConcurrent() int {
	return 0
}

func (qt *testQueryTracker) InsertWithDetails(ctx context.Context, query string, stage string, includeTimeRange bool, timeRange types.QueryTimeRange) (int, error) {
	qt.queries = append(qt.queries, trackedQuery{
		expr:      query,
		stage:     stage,
		timeRange: timeRange,
		deleted:   false,
	})

	return len(qt.queries) - 1, nil
}

func (qt *testQueryTracker) Delete(insertIndex int) {
	qt.queries[insertIndex].deleted = true
}

func (qt *testQueryTracker) Close() error {
	return nil
}

func (qt *testQueryTracker) Clear() {
	qt.queries = nil
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
	opts.CommonOpts.Timeout = 10 * time.Millisecond
	opts.ActiveQueryTracker = tracker
	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
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
			tracker.shouldWaitForTimeout = false // Query planning adds activities to the tracker, but we're not interested in testing that these activities are considered in the query timeout here.
			q, err := createQuery()
			require.NoError(t, err)
			tracker.shouldWaitForTimeout = true
			defer q.Close()

			tracker.sawTimeout = false
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
	shouldWaitForTimeout bool
	sawTimeout           bool
}

func (t *timeoutTestingQueryTracker) GetMaxConcurrent() int {
	return 0
}

func (t *timeoutTestingQueryTracker) InsertWithDetails(ctx context.Context, query string, stage string, includeTimeRange bool, timeRange types.QueryTimeRange) (int, error) {
	if !t.shouldWaitForTimeout {
		return 0, nil
	}

	select {
	case <-ctx.Done():
		t.sawTimeout = true
		return 0, context.Cause(ctx)
	case <-time.After(time.Second):
		return 0, errors.New("gave up waiting for query to time out")
	}
}

func (t *timeoutTestingQueryTracker) Delete(_ int) {}

func (t *timeoutTestingQueryTracker) Close() error {
	return nil
}

type annotationTestCase struct {
	data                       string
	expr                       string
	expectedWarningAnnotations []string
	expectedInfoAnnotations    []string

	// an alternate set of annotations for when delayed name removal is enabled.
	// if not set the test will fall back to expectedWarningAnnotations / expectedInfoAnnotations
	expectedWarningAnnotationsDelayedNameRemovalEnabled []string
	expectedInfoAnnotationsDelayedNameRemovalEnabled    []string

	// an alternate set of info annotations for Prometheus where it differs,
	// including both instant and range queries.
	// if not set the test will fall back to expectedInfoAnnotations / expectedInfoAnnotationsDelayedNameRemovalEnabled
	// This is only required temporarily until we fix merging annotations in MQE.
	expectedInfoAnnotationsPrometheus                               []string
	expectedInfoAnnotationsDelayedNameRemovalEnabledPrometheus      []string
	expectedInfoAnnotationsPrometheusRange                          []string
	expectedInfoAnnotationsDelayedNameRemovalEnabledPrometheusRange []string
	skipComparingAnnotationsWithPrometheus                          bool

	skipComparisonWithPrometheusReason string
	instantEvaluationTimestamp         *time.Time
}

func (a annotationTestCase) getExpectedInfoAnnotations(delayedNameRemovalEnabled bool, engineName, queryType string) []string {
	isRangeQuery := queryType == "range"

	// Prometheus engine may have specific annotations that differ from Mimir
	if engineName == prometheusEngineName {
		if delayedNameRemovalEnabled {
			if isRangeQuery && a.expectedInfoAnnotationsDelayedNameRemovalEnabledPrometheusRange != nil {
				return a.expectedInfoAnnotationsDelayedNameRemovalEnabledPrometheusRange
			}
			if !isRangeQuery && a.expectedInfoAnnotationsDelayedNameRemovalEnabledPrometheus != nil {
				return a.expectedInfoAnnotationsDelayedNameRemovalEnabledPrometheus
			}
		}
		if isRangeQuery && a.expectedInfoAnnotationsPrometheusRange != nil {
			return a.expectedInfoAnnotationsPrometheusRange
		}
		if !isRangeQuery && a.expectedInfoAnnotationsPrometheus != nil {
			return a.expectedInfoAnnotationsPrometheus
		}
	}

	if delayedNameRemovalEnabled && a.expectedInfoAnnotationsDelayedNameRemovalEnabled != nil {
		return a.expectedInfoAnnotationsDelayedNameRemovalEnabled
	}
	return a.expectedInfoAnnotations
}

func (a annotationTestCase) getExpectedWarningAnnotations(delayedNameRemovalEnabled bool) []string {
	if delayedNameRemovalEnabled && a.expectedWarningAnnotationsDelayedNameRemovalEnabled != nil {
		return a.expectedWarningAnnotationsDelayedNameRemovalEnabled
	}
	return a.expectedWarningAnnotations
}

func runAnnotationTests(t *testing.T, testCases map[string]annotationTestCase) {
	startT := timestamp.Time(0).Add(time.Minute)
	step := time.Minute
	endT := startT.Add(2 * step)

	// create 2 sets of engines - a Mimir and Prometheus engine each with EnableDelayedNameRemoval=true and other with EnableDelayedNameRemoval=false
	// there are some histogram annotation test cases which will emit a different warning/info annotation string depending on the delayed name removal setting
	engineSets := make([]struct {
		mimirEngine               promql.QueryEngine
		prometheusEngine          promql.QueryEngine
		delayedNameRemovalEnabled bool
	}, 0, 2)

	for _, delayedNameRemovalEnabled := range []bool{true, false} {
		opts := NewTestEngineOpts()
		limits := NewStaticQueryLimitsProvider()
		limits.EnableDelayedNameRemoval = delayedNameRemovalEnabled
		opts.Limits = limits
		opts.CommonOpts.EnableDelayedNameRemoval = delayedNameRemovalEnabled

		planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
		require.NoError(t, err)
		mimirEngine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
		require.NoError(t, err)
		prometheusEngine := promql.NewEngine(opts.CommonOpts)

		engineSets = append(engineSets, struct {
			mimirEngine               promql.QueryEngine
			prometheusEngine          promql.QueryEngine
			delayedNameRemovalEnabled bool
		}{
			mimirEngine:               mimirEngine,
			prometheusEngine:          prometheusEngine,
			delayedNameRemovalEnabled: delayedNameRemovalEnabled,
		})
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			store := promqltest.LoadedStorage(t, "load 1m\n"+strings.TrimSpace(testCase.data))
			t.Cleanup(func() { _ = store.Close() })

			queryTypes := map[string]func(engine promql.QueryEngine) (promql.Query, error){
				"range": func(engine promql.QueryEngine) (promql.Query, error) {
					return engine.NewRangeQuery(context.Background(), store, nil, testCase.expr, startT, endT, step)
				},
				"instant": func(engine promql.QueryEngine) (promql.Query, error) {
					t := startT

					if testCase.instantEvaluationTimestamp != nil {
						t = *testCase.instantEvaluationTimestamp
					}

					return engine.NewInstantQuery(context.Background(), store, nil, testCase.expr, t)
				},
			}

			for queryType, generator := range queryTypes {
				for _, engineSet := range engineSets {
					subTestName := fmt.Sprintf("%s - delayed name removal enabled=%t", queryType, engineSet.delayedNameRemovalEnabled)
					t.Run(subTestName, func(t *testing.T) {
						results := make([]*promql.Result, 0, 2)

						for _, engine := range []promql.QueryEngine{engineSet.mimirEngine, engineSet.prometheusEngine} {
							if engine == engineSet.prometheusEngine && testCase.skipComparisonWithPrometheusReason != "" {
								t.Logf("Skipping comparison with Prometheus' engine: %v", testCase.skipComparisonWithPrometheusReason)
								continue
							}
							engineName := mimirEngineName
							if engine == engineSet.prometheusEngine {
								engineName = prometheusEngineName
							}
							t.Run(engineName, func(t *testing.T) {
								query, err := generator(engine)
								require.NoError(t, err)
								t.Cleanup(query.Close)

								res := query.Exec(context.Background())
								require.NoError(t, res.Err)
								results = append(results, res)

								warnings, infos := res.Warnings.AsStrings(testCase.expr, 0, 0)
								require.ElementsMatch(t, testCase.getExpectedWarningAnnotations(engineSet.delayedNameRemovalEnabled), warnings)
								require.ElementsMatch(t, testCase.getExpectedInfoAnnotations(engineSet.delayedNameRemovalEnabled, engineName, queryType), infos)
							})
						}

						// If both results are available, compare them (sometimes we skip prometheus)
						if len(results) == 2 {
							// We do this extra comparison to ensure that we don't skip a series that may be outputted during a warning
							// or vice-versa where no result may be expected etc.
							mqetest.RequireEqualResults(t, testCase.expr, results[0], results[1], testCase.skipComparingAnnotationsWithPrometheus)
						}
					})
				}
			}
		})
	}
}

func TestAnnotations(t *testing.T) {
	floatData := `
		metric{type="float", series="1"} 0+1x3
		metric{type="float", series="2"} 1+1x3
	`

	mixedFloatHistogramData := `
		metric{type="float", series="1"} 0+1x3
		metric{type="float", series="2"} 1+1x3
		metric{type="histogram", series="1"} {{schema:0 sum:0 count:0}}+{{schema:0 sum:5 count:4 buckets:[1 2 1]}}x3
		metric{type="histogram", series="2"} {{schema:0 sum:1 count:1 buckets:[1]}}+{{schema:0 sum:5 count:4 buckets:[1 2 1]}}x3
	`

	nativeHistogramsWithCustomBucketsData := `
		metric{series="exponential-buckets"} {{schema:0 sum:1 count:1 buckets:[1]}}+{{schema:0 sum:5 count:4 buckets:[1 2 1]}}x3
		metric{series="custom-buckets-1"} {{schema:-53 sum:1 count:1 custom_values:[5 10] buckets:[1]}}+{{schema:-53 sum:5 count:4 custom_values:[5 10] buckets:[1 2 1]}}x3
		metric{series="custom-buckets-2"} {{schema:-53 sum:1 count:1 custom_values:[2 3] buckets:[1]}}+{{schema:-53 sum:5 count:4 custom_values:[2 3] buckets:[1 2 1]}}x3
		metric{series="mixed-exponential-custom-buckets"} {{schema:0 sum:1 count:1 buckets:[1]}} {{schema:-53 sum:1 count:1 custom_values:[5 10] buckets:[1]}} {{schema:0 sum:5 count:4 buckets:[1 2 1]}}
		metric{series="mismatched-custom-buckets"} {{schema:-53 sum:1 count:1 custom_values:[5 10] buckets:[1]}} {{schema:-53 sum:1 count:1 custom_values:[2 3] buckets:[1]}} {{schema:-53 sum:5 count:4 custom_values:[5 10] buckets:[1 2 1]}}
	`

	nativeHistogramsWithResetHintsMix := `
		metric{reset_hint="unknown"} {{schema:0 sum:0 count:0}}+{{schema:0 sum:5 count:4 buckets:[1 2 1]}}x3
		metric{reset_hint="gauge"} {{schema:0 sum:0 count:0 counter_reset_hint:gauge}}+{{schema:0 sum:5 count:4 buckets:[1 2 1] counter_reset_hint:gauge}}x3
		metric{reset_hint="gauge-unknown"} {{schema:0 sum:0 count:0 counter_reset_hint:gauge}} {{schema:0 sum:0 count:0}}+{{schema:0 sum:5 count:4 buckets:[1 2 1]}}x3
		metric{reset_hint="unknown-gauge"} {{schema:0 sum:0 count:0}}+{{schema:0 sum:5 count:4 buckets:[1 2 1] counter_reset_hint:gauge}}x3
	`

	testCases := map[string]annotationTestCase{
		"sum() with float and native histogram at same step": {
			data:                       mixedFloatHistogramData,
			expr:                       "sum by (series) (metric)",
			expectedWarningAnnotations: []string{`PromQL warning: encountered a mix of histograms and floats for aggregation (1:18)`},
		},
		"sum() with floats and native histograms for different output series at the same step": {
			data: mixedFloatHistogramData,
			expr: "sum by (type) (metric)",
		},
		"sum() with only floats": {
			data: mixedFloatHistogramData,
			expr: `sum(metric{type="float"})`,
		},
		"sum() with only native histograms": {
			data: mixedFloatHistogramData,
			expr: `sum(metric{type="histogram"})`,
		},

		"delta() over a native histogram with unknown CounterResetHint": {
			data:                       nativeHistogramsWithResetHintsMix,
			expr:                       `delta(metric{reset_hint="unknown"}[3m])`,
			expectedWarningAnnotations: []string{`PromQL warning: this native histogram metric is not a gauge: "metric" (1:7)`},
		},
		"delta() over a native histogram with gauge CounterResetHint": {
			data: nativeHistogramsWithResetHintsMix,
			expr: `delta(metric{reset_hint="gauge"}[3m])`,
		},
		"delta() with first point having gauge CounterResetHint and last point having unknown CounterResetHint": {
			data:                       nativeHistogramsWithResetHintsMix,
			expr:                       `delta(metric{reset_hint="gauge-unknown"}[3m])`,
			expectedWarningAnnotations: []string{`PromQL warning: this native histogram metric is not a gauge: "metric" (1:7)`},
		},
		"delta() with first point having unknown CounterResetHint and last point having gauge CounterResetHint": {
			data:                       nativeHistogramsWithResetHintsMix,
			expr:                       `delta(metric{reset_hint="unknown-gauge"}[3m])`,
			expectedWarningAnnotations: []string{`PromQL warning: this native histogram metric is not a gauge: "metric" (1:7)`},
		},

		"stdvar() with only floats": {
			data: mixedFloatHistogramData,
			expr: `stdvar(metric{type="float"})`,
		},
		"stdvar() with only native histograms": {
			data:                    mixedFloatHistogramData,
			expr:                    `stdvar(metric{type="histogram"})`,
			expectedInfoAnnotations: []string{"PromQL info: ignored histogram in stdvar aggregation (1:8)"},
		},

		"stddev() with only floats": {
			data: mixedFloatHistogramData,
			expr: `stddev(metric{type="float"})`,
		},
		"stddev() with only native histograms": {
			data:                    mixedFloatHistogramData,
			expr:                    `stddev(metric{type="histogram"})`,
			expectedInfoAnnotations: []string{"PromQL info: ignored histogram in stddev aggregation (1:8)"},
		},

		"min() with only floats": {
			data: mixedFloatHistogramData,
			expr: `min(metric{type="float"})`,
		},
		"min() with only native histograms": {
			data:                    mixedFloatHistogramData,
			expr:                    `min(metric{type="histogram"})`,
			expectedInfoAnnotations: []string{"PromQL info: ignored histogram in min aggregation (1:5)"},
		},

		"max() with only floats": {
			data: mixedFloatHistogramData,
			expr: `max(metric{type="float"})`,
		},
		"max() with only native histograms": {
			data:                    mixedFloatHistogramData,
			expr:                    `max(metric{type="histogram"})`,
			expectedInfoAnnotations: []string{"PromQL info: ignored histogram in max aggregation (1:5)"},
		},

		"avg() with float and native histogram at same step": {
			data:                       mixedFloatHistogramData,
			expr:                       "avg by (series) (metric)",
			expectedWarningAnnotations: []string{`PromQL warning: encountered a mix of histograms and floats for aggregation (1:18)`},
		},
		"avg() with floats and native histograms for different output series at the same step": {
			data: mixedFloatHistogramData,
			expr: "avg by (type) (metric)",
		},
		"avg() with only floats": {
			data: mixedFloatHistogramData,
			expr: `avg(metric{type="float"})`,
		},
		"avg() with only native histograms": {
			data: mixedFloatHistogramData,
			expr: `avg(metric{type="histogram"})`,
		},

		"sum() over native histograms with both exponential and custom buckets": {
			data: nativeHistogramsWithCustomBucketsData,
			expr: `sum(metric{series=~"exponential-buckets|custom-buckets-1"})`,
			expectedWarningAnnotations: []string{
				`PromQL warning: vector contains a mix of histograms with exponential and custom buckets schemas for metric name "metric" (1:5)`,
			},
		},
		"sum() over native histograms with mismatched custom buckets": {
			data: nativeHistogramsWithCustomBucketsData,
			expr: `sum(metric{series=~"custom-buckets-(1|2)"})`,
			expectedInfoAnnotations: []string{
				`PromQL info: mismatched custom buckets were reconciled during aggregation (1:5)`,
			},
		},

		"sum_over_time() over series with both floats and histograms": {
			data:                       `some_metric 10 {{schema:0 sum:1 count:1 buckets:[1]}}`,
			expr:                       `sum_over_time(some_metric[1m1s])`,
			expectedWarningAnnotations: []string{`PromQL warning: encountered a mix of histograms and floats for metric name "some_metric" (1:15)`},
		},
		"sum_over_time() over native histograms with both exponential and custom buckets": {
			data: nativeHistogramsWithCustomBucketsData,
			expr: `sum_over_time(metric{series="mixed-exponential-custom-buckets"}[1m1s])`,
			expectedWarningAnnotations: []string{
				`PromQL warning: vector contains a mix of histograms with exponential and custom buckets schemas for metric name "metric" (1:15)`,
			},
		},
		"sum_over_time() over native histograms with mismatched custom buckets": {
			data: nativeHistogramsWithCustomBucketsData,
			expr: `sum_over_time(metric{series="mismatched-custom-buckets"}[1m1s])`,
			expectedInfoAnnotations: []string{
				`PromQL info: mismatched custom buckets were reconciled during aggregation (1:15)`,
			},
		},

		"avg_over_time() over series with both floats and histograms": {
			data:                       `some_metric 10 {{schema:0 sum:1 count:1 buckets:[1]}}`,
			expr:                       `avg_over_time(some_metric[1m1s])`,
			expectedWarningAnnotations: []string{`PromQL warning: encountered a mix of histograms and floats for metric name "some_metric" (1:15)`},
		},
		"avg_over_time() over native histograms with both exponential and custom buckets": {
			data: nativeHistogramsWithCustomBucketsData,
			expr: `avg_over_time(metric{series="mixed-exponential-custom-buckets"}[1m1s])`,
			expectedWarningAnnotations: []string{
				`PromQL warning: vector contains a mix of histograms with exponential and custom buckets schemas for metric name "metric" (1:15)`,
			},
		},
		"avg_over_time() over native histograms with mismatched custom buckets": {
			data: nativeHistogramsWithCustomBucketsData,
			expr: `avg_over_time(metric{series="mismatched-custom-buckets"}[1m1s])`,
			expectedInfoAnnotations: []string{
				`PromQL info: mismatched custom buckets were reconciled during aggregation (1:15)`,
			},
		},

		"topk() with only floats": {
			data: mixedFloatHistogramData,
			expr: `topk(1, metric{type="float"})`,
		},
		"topk() with only histograms()": {
			data: mixedFloatHistogramData,
			expr: `topk(1, metric{type="histogram"})`,
			expectedInfoAnnotations: []string{
				`PromQL info: ignored histogram in topk aggregation (1:1)`,
			},
		},
		"topk() with both floats and histograms()": {
			data: mixedFloatHistogramData,
			expr: `topk(1, metric)`,
			expectedInfoAnnotations: []string{
				`PromQL info: ignored histogram in topk aggregation (1:1)`,
			},
		},

		"bottomk() with only floats": {
			data: mixedFloatHistogramData,
			expr: `bottomk(1, metric{type="float"})`,
		},
		"bottomk() with only histograms()": {
			data: mixedFloatHistogramData,
			expr: `bottomk(1, metric{type="histogram"})`,
			expectedInfoAnnotations: []string{
				`PromQL info: ignored histogram in bottomk aggregation (1:1)`,
			},
		},
		"bottomk() with both floats and histograms()": {
			data: mixedFloatHistogramData,
			expr: `bottomk(1, metric)`,
			expectedInfoAnnotations: []string{
				`PromQL info: ignored histogram in bottomk aggregation (1:1)`,
			},
		},

		"quantile_over_time() with negative quantile": {
			data: `metric 0 1 2 3`,
			expr: `quantile_over_time(-1, metric[1m1s])`,
			expectedWarningAnnotations: []string{
				`PromQL warning: quantile value should be between 0 and 1, got -1 (1:20)`,
			},
		},
		"quantile_over_time() with 0 quantile": {
			data: `some_metric 0 1 2 3`,
			expr: `quantile_over_time(0, some_metric[1m1s])`,
		},
		"quantile_over_time() with quantile between 0 and 1": {
			data: `some_metric 0 1 2 3`,
			expr: `quantile_over_time(0.5, some_metric[1m1s])`,
		},
		"quantile_over_time() with 1 quantile": {
			data: `some_metric 0 1 2 3`,
			expr: `quantile_over_time(1, some_metric[1m1s])`,
		},
		"quantile_over_time() with quantile greater than 1": {
			data: `some_metric 0 1 2 3`,
			expr: `quantile_over_time(1.2, some_metric[1m1s])`,
			expectedWarningAnnotations: []string{
				`PromQL warning: quantile value should be between 0 and 1, got 1.2 (1:20)`,
			},
		},
		"quantile_over_time() over series with only floats": {
			data: `some_metric 1 2`,
			expr: `quantile_over_time(0.2, some_metric[1m1s])`,
		},
		"quantile_over_time() over series with only histograms": {
			data: `some_metric {{count:1}} {{count:2}}`,
			expr: `quantile_over_time(0.2, some_metric[1m1s])`,
		},
		"quantile_over_time() over series with both floats and histograms": {
			data: `some_metric 1 {{count:2}}`,
			expr: `quantile_over_time(0.2, some_metric[1m1s])`,
			expectedInfoAnnotations: []string{
				`PromQL info: ignored histograms in a range containing both floats and histograms for metric name "some_metric" (1:20)`,
			},
		},

		"multiple annotations from different operators": {
			data: `
				mixed_metric_count       10 {{schema:0 sum:1 count:1 buckets:[1]}}
				other_mixed_metric_count 10 {{schema:0 sum:1 count:1 buckets:[1]}}
				float_metric             10 20
				other_float_metric       10 20
			`,
			expr: "rate(mixed_metric_count[1m1s]) + rate(other_mixed_metric_count[1m1s]) + rate(float_metric[1m1s]) + rate(other_float_metric[1m1s])",
			expectedWarningAnnotations: []string{
				`PromQL warning: encountered a mix of histograms and floats for metric name "mixed_metric_count" (1:6)`,
				`PromQL warning: encountered a mix of histograms and floats for metric name "other_mixed_metric_count" (1:39)`,
			},
			expectedInfoAnnotations: []string{
				`PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "float_metric" (1:78)`,
				`PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "other_float_metric" (1:105)`,
			},
		},

		"quantile with mixed histograms": {
			data: mixedFloatHistogramData,
			expr: "quantile(0.9, metric)",
			expectedInfoAnnotations: []string{
				`PromQL info: ignored histogram in quantile aggregation (1:15)`,
			},
		},
		"quantile with invalid param": {
			data: floatData,
			expr: "quantile(1.5, metric)",
			expectedWarningAnnotations: []string{
				`PromQL warning: quantile value should be between 0 and 1, got 1.5 (1:10)`,
			},
		},
		"double_exponential_smoothing() with float and native histogram at same step": {
			data:                    `some_metric 10 {{schema:0 sum:1 count:1 buckets:[1]}}`,
			expr:                    "double_exponential_smoothing(some_metric[1m1s], 0.5, 0.5)",
			expectedInfoAnnotations: []string{`PromQL info: ignored histograms in a range containing both floats and histograms for metric name "some_metric" (1:30)`},
		},
		"double_exponential_smoothing() with only native histogram at same step will result with no annotations": {
			data:                       `some_histo_metric {{schema:0 sum:1 count:1 buckets:[1]}} {{schema:0 sum:1 count:1 buckets:[1]}}`,
			expr:                       "double_exponential_smoothing(some_histo_metric[1m1s], 0.5, 0.5)",
			expectedInfoAnnotations:    []string{},
			expectedWarningAnnotations: []string{},
		},
	}

	for _, f := range []string{"min_over_time", "max_over_time", "stddev_over_time", "stdvar_over_time"} {
		testCases[fmt.Sprintf("%v() over series with only floats", f)] = annotationTestCase{
			data: `some_metric 1 2`,
			expr: fmt.Sprintf(`%v(some_metric[1m1s])`, f),
		}
		testCases[fmt.Sprintf("%v() over series with only histograms", f)] = annotationTestCase{
			data: `some_metric {{count:1}} {{count:2}}`,
			expr: fmt.Sprintf(`%v(some_metric[1m1s])`, f),
		}
		testCases[fmt.Sprintf("%v() over series with both floats and histograms", f)] = annotationTestCase{
			data: `some_metric 1 {{count:2}}`,
			expr: fmt.Sprintf(`%v(some_metric[1m1s])`, f),
			expectedInfoAnnotations: []string{
				fmt.Sprintf(`PromQL info: ignored histograms in a range containing both floats and histograms for metric name "some_metric" (1:%v)`, len(f)+2),
			},
		}
	}

	runAnnotationTests(t, testCases)
}

func TestRateIncreaseAnnotations(t *testing.T) {
	mixedFloatHistogramData := `
		metric{type="float", series="1"} 0+1x3
		metric{type="float", series="2"} 1+1x3
		metric{type="histogram", series="1"} {{schema:0 sum:0 count:0}}+{{schema:0 sum:5 count:4 buckets:[1 2 1]}}x3
		metric{type="histogram", series="2"} {{schema:0 sum:1 count:1 buckets:[1]}}+{{schema:0 sum:5 count:4 buckets:[1 2 1]}}x3
	`

	nativeHistogramsWithCustomBucketsData := `
		metric{series="exponential-buckets"} {{schema:0 sum:1 count:1 buckets:[1]}}+{{schema:0 sum:5 count:4 buckets:[1 2 1]}}x3
		metric{series="custom-buckets-1"} {{schema:-53 sum:1 count:1 custom_values:[5 10] buckets:[1]}}+{{schema:-53 sum:5 count:4 custom_values:[5 10] buckets:[1 2 1]}}x3
		metric{series="custom-buckets-2"} {{schema:-53 sum:1 count:1 custom_values:[2 3] buckets:[1]}}+{{schema:-53 sum:5 count:4 custom_values:[2 3] buckets:[1 2 1]}}x3
		metric{series="mixed-exponential-custom-buckets"} {{schema:0 sum:1 count:1 buckets:[1]}} {{schema:-53 sum:1 count:1 custom_values:[5 10] buckets:[1]}} {{schema:0 sum:5 count:4 buckets:[1 2 1]}}
		metric{series="mismatched-custom-buckets"} {{schema:-53 sum:1 count:1 custom_values:[5 10] buckets:[1]}} {{schema:-53 sum:1 count:1 custom_values:[2 3] buckets:[1]}} {{schema:-53 sum:5 count:4 custom_values:[2 10] buckets:[1 2 1]}}
		metric{series="mismatched-custom-buckets-with-resets"} {{schema:-53 sum:10 count:1 custom_values:[5 10] buckets:[10]}} {{schema:-53 sum:1 count:1 custom_values:[2 3] buckets:[1]}}
    `

	testCases := map[string]annotationTestCase{}
	// rate and increase use the same annotations
	for _, function := range []string{"rate", "increase"} {
		position := len(fmt.Sprintf("%s(", function)) + 1
		testCases[fmt.Sprintf("%s() over metric without counter suffix containing only floats", function)] = annotationTestCase{
			data:                    mixedFloatHistogramData,
			expr:                    fmt.Sprintf(`%s(metric{type="float"}[1m1s])`, function),
			expectedInfoAnnotations: []string{fmt.Sprintf(`PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "metric" (1:%d)`, position)},
		}

		testCases[fmt.Sprintf("%s() over metric without counter suffix containing only native histograms", function)] = annotationTestCase{
			data: mixedFloatHistogramData,
			expr: fmt.Sprintf(`%s(metric{type="histogram"}[1m1s])`, function),
		}
		testCases[fmt.Sprintf("%s() over metric ending in _total", function)] = annotationTestCase{
			data: `some_metric_total 0+1x3`,
			expr: fmt.Sprintf(`%s(some_metric_total[1m1s])`, function),
		}
		testCases[fmt.Sprintf("%s() over metric ending in _sum", function)] = annotationTestCase{
			data: `some_metric_sum 0+1x3`,
			expr: fmt.Sprintf(`%s(some_metric_sum[1m1s])`, function),
		}
		testCases[fmt.Sprintf("%s() over metric ending in _count", function)] = annotationTestCase{
			data: `some_metric_count 0+1x3`,
			expr: fmt.Sprintf(`%s(some_metric_count[1m1s])`, function),
		}
		testCases[fmt.Sprintf("%s() over metric ending in _bucket", function)] = annotationTestCase{
			data: `some_metric_bucket 0+1x3`,
			expr: fmt.Sprintf(`%s(some_metric_bucket[1m1s])`, function),
		}
		testCases[fmt.Sprintf("%s() over multiple metric names", function)] = annotationTestCase{
			data: `
				not_a_counter{env="prod", series="1"}      0+1x3
				a_total{series="2"}                        1+1x3
				a_sum{series="3"}                          2+1x3
				a_count{series="4"}                        3+1x3
				a_bucket{series="5"}                       4+1x3
				not_a_counter{env="test", series="6"}      5+1x3
				also_not_a_counter{env="test", series="7"} 6+1x3
			`,
			expr: fmt.Sprintf(`%s({__name__!=""}[1m1s])`, function),
			expectedInfoAnnotations: []string{
				fmt.Sprintf(`PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "not_a_counter" (1:%d)`, position),
				fmt.Sprintf(`PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "also_not_a_counter" (1:%d)`, position),
			},
		}
		testCases[fmt.Sprintf("%s() over series with both floats and histograms", function)] = annotationTestCase{
			data:                       `some_metric_count 10 {{schema:0 sum:1 count:1 buckets:[1]}}`,
			expr:                       fmt.Sprintf(`%s(some_metric_count[1m1s])`, function),
			expectedWarningAnnotations: []string{fmt.Sprintf(`PromQL warning: encountered a mix of histograms and floats for metric name "some_metric_count" (1:%d)`, position)},
		}
		testCases[fmt.Sprintf("%s() over series with first histogram that is not a counter", function)] = annotationTestCase{
			data:                       `some_metric {{schema:0 sum:1 count:1 buckets:[1] counter_reset_hint:gauge}} {{schema:0 sum:2 count:2 buckets:[2]}}`,
			expr:                       fmt.Sprintf(`%s(some_metric[1m1s])`, function),
			expectedWarningAnnotations: []string{fmt.Sprintf(`PromQL warning: this native histogram metric is not a counter: "some_metric" (1:%d)`, position)},
		}
		testCases[fmt.Sprintf("%s() over series with last histogram that is not a counter", function)] = annotationTestCase{
			data:                       `some_metric {{schema:0 sum:1 count:1 buckets:[1]}} {{schema:0 sum:2 count:2 buckets:[2] counter_reset_hint:gauge}}`,
			expr:                       fmt.Sprintf(`%s(some_metric[1m1s])`, function),
			expectedWarningAnnotations: []string{fmt.Sprintf(`PromQL warning: this native histogram metric is not a counter: "some_metric" (1:%d)`, position)},
		}
		testCases[fmt.Sprintf("%s() over series with a histogram that is not a counter that is neither the first or last in the range", function)] = annotationTestCase{
			data:                       `some_metric {{schema:0 sum:1 count:1 buckets:[1]}} {{schema:0 sum:2 count:2 buckets:[2] counter_reset_hint:gauge}} {{schema:0 sum:3 count:3 buckets:[3]}}`,
			expr:                       fmt.Sprintf(`%s(some_metric[2m1s] @ 2m)`, function),
			expectedWarningAnnotations: []string{fmt.Sprintf(`PromQL warning: this native histogram metric is not a counter: "some_metric" (1:%d)`, position)},
		}

		// We ignore the first sample if it's incompatible with the second, so we need to run the two test cases below
		// at a time range where we'll get at least three points in the range.
		incompatibleSchemaEvaluationTimestamp := timestamp.Time(0).Add(2 * time.Minute)

		testCases[fmt.Sprintf("%s() over native histograms with both exponential and custom buckets", function)] = annotationTestCase{
			data:                       nativeHistogramsWithCustomBucketsData,
			expr:                       fmt.Sprintf(`%s(metric{series="mixed-exponential-custom-buckets"}[2m1s])`, function),
			instantEvaluationTimestamp: &incompatibleSchemaEvaluationTimestamp,
			expectedWarningAnnotations: []string{
				fmt.Sprintf(`PromQL warning: vector contains a mix of histograms with exponential and custom buckets schemas for metric name "metric" (1:%d)`, position),
			},
		}

		testCases[fmt.Sprintf("%s() over native histograms with mismatched custom buckets", function)] = annotationTestCase{
			data:                       nativeHistogramsWithCustomBucketsData,
			expr:                       fmt.Sprintf(`%s(metric{series="mismatched-custom-buckets"}[2m1s])`, function),
			instantEvaluationTimestamp: &incompatibleSchemaEvaluationTimestamp,
			expectedInfoAnnotations: []string{
				fmt.Sprintf(`PromQL info: mismatched custom buckets were reconciled during subtraction (1:%d)`, position),
			},
			expectedWarningAnnotations: []string{},
		}

		testCases[fmt.Sprintf("%s() over native histograms with mismatched custom buckets with reset", function)] = annotationTestCase{
			// No info annotation - as the mismatched buckets samples are also detected as a reset, no reconciled annotation is returned
			data:                       nativeHistogramsWithCustomBucketsData,
			expr:                       fmt.Sprintf(`%s(metric{series="mismatched-custom-buckets-with-resets"}[2m1s])`, function),
			instantEvaluationTimestamp: &incompatibleSchemaEvaluationTimestamp,
		}

		testCases[fmt.Sprintf("%s() over metric without counter suffix with single float or histogram in range", function)] = annotationTestCase{
			data: `
				series 3 1 {{schema:3 sum:12 count:7 buckets:[2 2 3]}}
			`,
			expr:                       fmt.Sprintf("%s(series[46s])", function),
			expectedWarningAnnotations: []string{},
			expectedInfoAnnotations:    []string{},
		}
		testCases[fmt.Sprintf("%s() over one point in range", function)] = annotationTestCase{
			data: `
				series 1
			`,
			expr:                       fmt.Sprintf("%s(series[1m1s])", function),
			expectedWarningAnnotations: []string{},
			expectedInfoAnnotations:    []string{},
		}
	}
	runAnnotationTests(t, testCases)
}

func TestDeltaAnnotations(t *testing.T) {
	nativeHistogramsWithGaugeResetHints := `
		metric{series="mix-float-nh"} 10 {{schema:-53 sum:1 count:1 custom_values:[5 10] buckets:[1] counter_reset_hint:gauge}} {{schema:-53 sum:1 count:1 custom_values:[5 10] buckets:[1] counter_reset_hint:gauge}} {{schema:-53 sum:5 count:4 custom_values:[5 10] buckets:[1] counter_reset_hint:gauge}}
		metric{series="mixed-exponential-custom-buckets"} {{schema:0 sum:1 count:1 buckets:[1]}} {{schema:-53 sum:1 count:1 custom_values:[5 10] buckets:[1]}} {{schema:0 sum:5 count:4 buckets:[1 2 1]}}
	`

	testCases := map[string]annotationTestCase{
		"delta() over series with mixed floats and native histograms": {
			data: nativeHistogramsWithGaugeResetHints,
			expr: `delta(metric{series="mix-float-nh"}[1m1s])`,
			expectedWarningAnnotations: []string{
				`PromQL warning: encountered a mix of histograms and floats for metric name "metric" (1:7)`,
			},
		},
		"delta() over metric with incompatible schema": {
			data: nativeHistogramsWithGaugeResetHints,
			expr: `delta(metric{series="mixed-exponential-custom-buckets"}[1m1s])`,
			expectedWarningAnnotations: []string{
				`PromQL warning: vector contains a mix of histograms with exponential and custom buckets schemas for metric name "metric" (1:7)`,
			},
		},
	}

	runAnnotationTests(t, testCases)
}

func TestIrateIdeltaAnnotations(t *testing.T) {
	irateData := `
		metric{series="floats"} 1 2
		metric{series="nh"} {{schema:0 sum:1 count:1 buckets:[1]}} {{schema:0 sum:2 count:2 buckets:[2]}}
		metric{series="mixed-float-nh"} 10 {{schema:-53 sum:1 count:1 custom_values:[5 10] buckets:[1]}} {{schema:-53 sum:1 count:1 custom_values:[5 10] buckets:[1]}} {{schema:-53 sum:5 count:4 custom_values:[5 10] buckets:[1]}}
		metric{series="mixed-exponential-custom-buckets"} {{schema:-53 sum:1 count:1 custom_values:[5 10] buckets:[1]}} {{schema:0 sum:1 count:1 buckets:[1]}} {{schema:0 sum:5 count:4 buckets:[1 2 1]}}
		metric{series="mismatched-custom-buckets"} {{schema:-53 sum:1 count:1 custom_values:[5 10] buckets:[1]}} {{schema:-53 sum:1 count:1 custom_values:[2 3] buckets:[1]}} {{schema:-53 sum:5 count:4 custom_values:[2 10] buckets:[1 2 1]}}
		metric{series="mismatched-custom-buckets-with-resets"} {{schema:-53 sum:10 count:1 custom_values:[5 10] buckets:[10]}} {{schema:-53 sum:1 count:1 custom_values:[2 3] buckets:[1]}}
		metric{series="nh-first-gauge"} {{schema:0 sum:1 count:1 buckets:[1] counter_reset_hint:gauge}} {{schema:0 sum:2 count:2 buckets:[2]}}
		metric{series="nh-second-gauge"} {{schema:0 sum:1 count:1 buckets:[1]}} {{schema:0 sum:2 count:2 buckets:[2] counter_reset_hint:gauge}}
	`

	ideltaData := `
		metric{series="floats"} 1 2
		metric{series="nh"} {{schema:0 sum:1 count:1 buckets:[1] counter_reset_hint:gauge}} {{schema:0 sum:2 count:2 buckets:[2] counter_reset_hint:gauge}}
		metric{series="mixed-float-nh"} 10 {{schema:-53 sum:1 count:1 custom_values:[5 10] buckets:[1] counter_reset_hint:gauge}} {{schema:-53 sum:1 count:1 custom_values:[5 10] buckets:[1] counter_reset_hint:gauge}} {{schema:-53 sum:5 count:4 custom_values:[5 10] buckets:[1] counter_reset_hint:gauge}}
		metric{series="mixed-exponential-custom-buckets"} {{schema:0 sum:1 count:1 buckets:[1] counter_reset_hint:gauge}} {{schema:-53 sum:1 count:1 custom_values:[5 10] buckets:[1] counter_reset_hint:gauge}} {{schema:0 sum:5 count:4 buckets:[1 2 1] counter_reset_hint:gauge}}
		metric{series="mismatched-custom-buckets"} {{schema:-53 sum:1 count:1 custom_values:[5 10] buckets:[1] counter_reset_hint:gauge}} {{schema:-53 sum:1 count:1 custom_values:[5 12] buckets:[1] counter_reset_hint:gauge}}
		metric{series="nh-first-not-gauge"} {{schema:0 sum:1 count:1 buckets:[1]}} {{schema:0 sum:2 count:2 buckets:[2] counter_reset_hint:gauge}}
		metric{series="nh-second-not-gauge"} {{schema:0 sum:1 count:1 buckets:[1] counter_reset_hint:gauge}} {{schema:0 sum:2 count:2 buckets:[2]}}
	`

	testCases := map[string]annotationTestCase{
		"irate() over series with only floats": {
			data:                       irateData,
			expr:                       `irate(metric{series="floats"}[1m1s])`,
			expectedWarningAnnotations: []string{},
		},
		"irate() over series with only native histograms": {
			data:                       irateData,
			expr:                       `irate(metric{series="nh"}[1m1s])`,
			expectedWarningAnnotations: []string{},
		},
		"irate() over series with mixed floats and native histograms": {
			data: irateData,
			expr: `irate(metric{series="mixed-float-nh"}[1m1s])`,
			expectedWarningAnnotations: []string{
				`PromQL warning: encountered a mix of histograms and floats for metric name "metric" (1:7)`,
			},
		},
		// In the case where irate() is run over a metric with both exponential and custom buckets,
		// the change in schema counts as a reset and so we'll just return the last point with no annotation.
		"irate() over metric with incompatible schema": {
			data:                       irateData,
			expr:                       `irate(metric{series="mixed-exponential-custom-buckets"}[1m1s])`,
			expectedWarningAnnotations: []string{},
		},
		"irate() over metric with mismatched custom buckets": {
			data: irateData,
			expr: `irate(metric{series="mismatched-custom-buckets"}[1m1s])`,
			expectedInfoAnnotations: []string{
				`PromQL info: mismatched custom buckets were reconciled during subtraction (1:7)`,
			},
		},
		// If a reset is detected for mismatched buckets, no annotations are returned
		"irate() over metric with mismatched custom buckets with reset": {
			data: irateData,
			expr: `irate(metric{series="mismatched-custom-buckets-with-resets"}[1m1s])`,
		},
		"irate() over metric with first point not being a counter native histogram": {
			data: irateData,
			expr: `irate(metric{series="nh-first-gauge"}[1m1s])`,
			expectedWarningAnnotations: []string{
				`PromQL warning: this native histogram metric is not a counter: "metric" (1:7)`,
			},
		},
		"irate() over metric with second point not being a counter native histogram": {
			data: irateData,
			expr: `irate(metric{series="nh-second-gauge"}[1m1s])`,
			expectedWarningAnnotations: []string{
				`PromQL warning: this native histogram metric is not a counter: "metric" (1:7)`,
			},
		},

		"idelta() over series with only floats": {
			data:                       ideltaData,
			expr:                       `idelta(metric{series="floats"}[1m1s])`,
			expectedWarningAnnotations: []string{},
		},
		"idelta() over series with only native histograms": {
			data:                       ideltaData,
			expr:                       `idelta(metric{series="nh"}[1m1s])`,
			expectedWarningAnnotations: []string{},
		},
		"idelta() over series with mixed floats and native histograms": {
			data: ideltaData,
			expr: `idelta(metric{series="mixed-float-nh"}[1m1s])`,
			expectedWarningAnnotations: []string{
				`PromQL warning: encountered a mix of histograms and floats for metric name "metric" (1:8)`,
			},
		},
		"idelta() over metric with incompatible schema": {
			data: ideltaData,
			expr: `idelta(metric{series="mixed-exponential-custom-buckets"}[1m1s])`,
			expectedWarningAnnotations: []string{
				`PromQL warning: vector contains a mix of histograms with exponential and custom buckets schemas for metric name "metric" (1:8)`,
			},
		},
		"idelta() over metric with mismatched custom buckets": {
			data: ideltaData,
			expr: `idelta(metric{series="mismatched-custom-buckets"}[1m1s])`,
			expectedInfoAnnotations: []string{
				`PromQL info: mismatched custom buckets were reconciled during subtraction (1:8)`,
			},
		},
		"idelta() over metric with first point not being a gauge native histogram": {
			data: ideltaData,
			expr: `idelta(metric{series="nh-first-not-gauge"}[1m1s])`,
			expectedWarningAnnotations: []string{
				`PromQL warning: this native histogram metric is not a gauge: "metric" (1:8)`,
			},
		},
		"idelta() over metric with second point not being a gauge native histogram": {
			data: ideltaData,
			expr: `idelta(metric{series="nh-second-not-gauge"}[1m1s])`,
			expectedWarningAnnotations: []string{
				`PromQL warning: this native histogram metric is not a gauge: "metric" (1:8)`,
			},
		},
	}

	runAnnotationTests(t, testCases)
}

func TestDerivPredictLinearAnnotations(t *testing.T) {
	data := `
		only_floats 0 1
		only_histograms {{count:0}} {{count:0}}
		mixed 0 {{count:0}}
    `

	testCases := map[string]annotationTestCase{
		"deriv() over series with only floats": {
			data: data,
			expr: `deriv(only_floats[1m1s])`,
			// Expect no annotations.
		},
		"deriv() over series with only histograms": {
			data: data,
			expr: `deriv(only_histograms[1m1s])`,
			// Expect no annotations.
		},
		"deriv() over series with both floats and histograms": {
			data: data,
			expr: `deriv(mixed[1m1s])`,
			expectedInfoAnnotations: []string{
				`PromQL info: ignored histograms in a range containing both floats and histograms for metric name "mixed" (1:7)`,
			},
		},

		"predict_linear() over series with only floats": {
			data: data,
			expr: `predict_linear(only_floats[1m1s], 5)`,
			// Expect no annotations.
		},
		"predict_linear() over series with only histograms": {
			data: data,
			expr: `predict_linear(only_histograms[1m1s], 5)`,
			// Expect no annotations.
		},
		"predict_linear() over series with both floats and histograms": {
			data: data,
			expr: `predict_linear(mixed[1m1s], 5)`,
			expectedInfoAnnotations: []string{
				`PromQL info: ignored histograms in a range containing both floats and histograms for metric name "mixed" (1:16)`,
			},
		},
	}

	runAnnotationTests(t, testCases)
}

func TestBinaryOperationAnnotations(t *testing.T) {
	mixedFloatHistogramData := `
	metric{type="float", series="1"} 0+1x3
	metric{type="float", series="2"} 1+1x3
	metric{type="histogram", series="1"} {{schema:0 sum:0 count:0}}+{{schema:0 sum:5 count:4 buckets:[1 2 1]}}x3
	metric{type="histogram", series="2"} {{schema:0 sum:1 count:1 buckets:[1]}}+{{schema:0 sum:5 count:4 buckets:[1 2 1]}}x3
	metric{type="histogram-custom-buckets", series="1"} {{schema:-53 sum:0 count:0 custom_values:[5 10]}}+{{schema:-53 sum:5 count:4 buckets:[1 2 1] custom_values:[5 10]}}x3
	metric{type="histogram-custom-buckets", series="2"} {{schema:-53 sum:1 count:1 buckets:[1] custom_values:[5 10]}}+{{schema:-53 sum:5 count:4 buckets:[1 2 1] custom_values:[5 10]}}x3
	metric{type="histogram-custom-buckets-other-layout", series="1"} {{schema:-53 sum:0 count:0 custom_values:[5 12]}}+{{schema:-53 sum:5 count:4 buckets:[1 2 1] custom_values:[5 12]}}x3
	metric{type="histogram-custom-buckets-other-layout", series="2"} {{schema:-53 sum:1 count:1 buckets:[1] custom_values:[5 12]}}+{{schema:-53 sum:5 count:4 buckets:[1 2 1] custom_values:[5 12]}}x3
`

	testCases := map[string]annotationTestCase{}
	binaryOperations := map[string]struct {
		floatHistogramSupported              bool
		histogramFloatSupported              bool
		histogramHistogramSupported          bool
		supportsBool                         bool
		emitsWarningOnIncompatibleHistograms bool
		emitsInfoOnMismatchedCustomBuckets   bool
	}{
		"+": {
			floatHistogramSupported:              false,
			histogramFloatSupported:              false,
			histogramHistogramSupported:          true,
			emitsWarningOnIncompatibleHistograms: true,
			emitsInfoOnMismatchedCustomBuckets:   true,
		},
		"-": {
			floatHistogramSupported:              false,
			histogramFloatSupported:              false,
			histogramHistogramSupported:          true,
			emitsWarningOnIncompatibleHistograms: true,
			emitsInfoOnMismatchedCustomBuckets:   true,
		},
		"*": {
			floatHistogramSupported:     true,
			histogramFloatSupported:     true,
			histogramHistogramSupported: false,
		},
		"/": {
			floatHistogramSupported:     false,
			histogramFloatSupported:     true,
			histogramHistogramSupported: false,
		},
		"^": {
			floatHistogramSupported:     false,
			histogramFloatSupported:     false,
			histogramHistogramSupported: false,
		},
		"%": {
			floatHistogramSupported:     false,
			histogramFloatSupported:     false,
			histogramHistogramSupported: false,
		},
		"atan2": {
			floatHistogramSupported:     false,
			histogramFloatSupported:     false,
			histogramHistogramSupported: false,
		},
		"==": {
			floatHistogramSupported:     false,
			histogramFloatSupported:     false,
			histogramHistogramSupported: true,
			supportsBool:                true,
		},
		"!=": {
			floatHistogramSupported:     false,
			histogramFloatSupported:     false,
			histogramHistogramSupported: true,
			supportsBool:                true,
		},
		">": {
			floatHistogramSupported:     false,
			histogramFloatSupported:     false,
			histogramHistogramSupported: false,
			supportsBool:                true,
		},
		"<": {
			floatHistogramSupported:     false,
			histogramFloatSupported:     false,
			histogramHistogramSupported: false,
			supportsBool:                true,
		},
		">=": {
			floatHistogramSupported:     false,
			histogramFloatSupported:     false,
			histogramHistogramSupported: false,
			supportsBool:                true,
		},
		"<=": {
			floatHistogramSupported:     false,
			histogramFloatSupported:     false,
			histogramHistogramSupported: false,
			supportsBool:                true,
		},
	}

	addIncompatibleTypesTestCase := func(op string, name string, expr string, left string, right string, supported bool) {
		testCase := annotationTestCase{
			data: mixedFloatHistogramData,
			expr: expr,
		}

		if !supported {
			testCase.expectedInfoAnnotations = []string{fmt.Sprintf(`PromQL info: incompatible sample types encountered for binary operator "%v": %v %v %v (1:1)`, op, left, op, right)}
		}

		testCases[name] = testCase
	}

	addIncompatibleLayoutWarningTestCase := func(op string, name string, expr string) {
		testCases[name] = annotationTestCase{
			data:                       mixedFloatHistogramData,
			expr:                       expr,
			expectedWarningAnnotations: []string{fmt.Sprintf(`PromQL warning: incompatible bucket layout encountered for binary operator %v (1:1)`, op)},
		}
	}

	addMismatchedCustomBucketsInfoTestCase := func(op string, name string, expr string) {
		opName := op

		switch op {
		case "+":
			opName = "addition"
		case "-":
			opName = "subtraction"
		}

		testCases[name] = annotationTestCase{
			data:                    mixedFloatHistogramData,
			expr:                    expr,
			expectedInfoAnnotations: []string{fmt.Sprintf(`PromQL info: mismatched custom buckets were reconciled during %v (1:1)`, opName)},
		}
	}

	cardinalities := map[string]string{
		"one-to-one":  "",
		"many-to-one": "group_left",
		"one-to-many": "group_right",
	}

	for op, binop := range binaryOperations {
		expressions := []string{op}

		if binop.supportsBool {
			expressions = append(expressions, op+" bool")
		}

		for _, expr := range expressions {
			addIncompatibleTypesTestCase(op, fmt.Sprintf("binary %v between a scalar on the left side and a histogram on the right", expr), fmt.Sprintf(`2 %v metric{type="histogram"}`, expr), "float", "histogram", binop.floatHistogramSupported)
			addIncompatibleTypesTestCase(op, fmt.Sprintf("binary %v between a histogram on the left side and a scalar on the right", expr), fmt.Sprintf(`metric{type="histogram"} %v 2`, expr), "histogram", "float", binop.histogramFloatSupported)

			for cardinalityName, cardinalityModifier := range cardinalities {
				addIncompatibleTypesTestCase(op, fmt.Sprintf("binary %v between two floats with %v matching", expr, cardinalityName), fmt.Sprintf(`metric{type="float"} %v ignoring(type) %v metric{type="float"}`, expr, cardinalityModifier), "float", "float", true)
				addIncompatibleTypesTestCase(op, fmt.Sprintf("binary %v between a float on the left side and a histogram on the right with %v matching", expr, cardinalityName), fmt.Sprintf(`metric{type="float"} %v ignoring(type) %v metric{type="histogram"}`, expr, cardinalityModifier), "float", "histogram", binop.floatHistogramSupported)
				addIncompatibleTypesTestCase(op, fmt.Sprintf("binary %v between a histogram on the left side and a float on the right with %v matching", expr, cardinalityName), fmt.Sprintf(`metric{type="histogram"} %v ignoring(type) %v metric{type="float"}`, expr, cardinalityModifier), "histogram", "float", binop.histogramFloatSupported)
				addIncompatibleTypesTestCase(op, fmt.Sprintf("binary %v between two histograms with %v matching", expr, cardinalityName), fmt.Sprintf(`metric{type="histogram"} %v ignoring(type) %v metric{type="histogram"}`, expr, cardinalityModifier), "histogram", "histogram", binop.histogramHistogramSupported)

				if binop.histogramHistogramSupported {
					if binop.emitsWarningOnIncompatibleHistograms {
						addIncompatibleLayoutWarningTestCase(op, fmt.Sprintf("binary %v between histograms with exponential and custom buckets with %v matching", expr, cardinalityName), fmt.Sprintf(`metric{type="histogram"} %v ignoring(type) %v metric{type="histogram-custom-buckets"}`, expr, cardinalityModifier))
					}
					if binop.emitsInfoOnMismatchedCustomBuckets {
						addMismatchedCustomBucketsInfoTestCase(op, fmt.Sprintf("binary %v between histograms with incompatible custom bucket schemas with %v matching", expr, cardinalityName), fmt.Sprintf(`metric{type="histogram-custom-buckets"} %v ignoring(type) %v metric{type="histogram-custom-buckets-other-layout"}`, expr, cardinalityModifier))
					}
				}
			}
		}
	}

	runAnnotationTests(t, testCases)
}

func TestHistogramAnnotations(t *testing.T) {
	mixedClassicHistograms := `
		series{host="a", le="0.1"}  2
		series{host="a", le="1"}    1
		series{host="a", le="10"}   5
		series{host="a", le="100"}  4
		series{host="a", le="1000"} 9
		series{host="a", le="+Inf"} 8
		series{host="a"}            {{schema:0 sum:5 count:4 buckets:[9 2 1]}}
		series{host="b"}            1
		series{host="c", le="abc"}  1
		series{host="d", le="0.1"}  2
		series{host="d", le="1"}    1
		series{host="d", le="10"}   5
		series{host="d", le="100"}  4
		series{host="d", le="1000"} 9
		series{host="d", le="+Inf"} 8
	`

	testCases := map[string]annotationTestCase{
		"bad bucket label warning": {
			data:                       mixedClassicHistograms,
			expr:                       `histogram_quantile(0.5, series{host="c"})`,
			expectedWarningAnnotations: []string{`PromQL warning: bucket label "le" is missing or has a malformed value of "abc" (1:25)`},
			expectedWarningAnnotationsDelayedNameRemovalEnabled: []string{`PromQL warning: bucket label "le" is missing or has a malformed value of "abc" for metric name "series" (1:25)`},
		},
		"invalid quantile warning": {
			data:                       mixedClassicHistograms,
			expr:                       `histogram_quantile(2, series{host="d"})`,
			expectedWarningAnnotations: []string{`PromQL warning: quantile value should be between 0 and 1, got 2 (1:20)`},
		},
		"mixed classic and native histogram warning": {
			data:                       mixedClassicHistograms,
			expr:                       `histogram_quantile(0.5, series{host="a"})`,
			expectedWarningAnnotations: []string{`PromQL warning: vector contains a mix of classic and native histograms (1:25)`},
			expectedWarningAnnotationsDelayedNameRemovalEnabled: []string{`PromQL warning: vector contains a mix of classic and native histograms for metric name "series" (1:25)`},
		},
		"forced monotonicity info": {
			data:                    mixedClassicHistograms,
			expr:                    `histogram_quantile(0.5, series{host="d"})`,
			expectedInfoAnnotations: []string{`PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile) (1:25)`},
			expectedInfoAnnotationsDelayedNameRemovalEnabled:                []string{`PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile) for metric name "series" (1:25)`},
			expectedInfoAnnotationsPrometheus:                               []string{`PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile), from buckets 1 to +Inf, with a max diff of 1, over 1 samples from 1970-01-01T00:01:00Z to 1970-01-01T00:01:00Z (1:25)`},
			expectedInfoAnnotationsPrometheusRange:                          []string{`PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile), from buckets 1 to +Inf, with a max diff of 1, over 10 samples from 1970-01-01T00:01:00Z to 1970-01-01T00:03:00Z (1:25)`},
			expectedInfoAnnotationsDelayedNameRemovalEnabledPrometheus:      []string{`PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile) for metric name "series", from buckets 1 to +Inf, with a max diff of 1, over 1 samples from 1970-01-01T00:01:00Z to 1970-01-01T00:01:00Z (1:25)`},
			expectedInfoAnnotationsDelayedNameRemovalEnabledPrometheusRange: []string{`PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile) for metric name "series", from buckets 1 to +Inf, with a max diff of 1, over 10 samples from 1970-01-01T00:01:00Z to 1970-01-01T00:03:00Z (1:25)`},
			skipComparingAnnotationsWithPrometheus:                          true,
		},
		"both mixed classic+native histogram and invalid quantile warnings": {
			data: mixedClassicHistograms,
			expr: `histogram_quantile(9, series{host="a"})`,
			expectedWarningAnnotations: []string{
				`PromQL warning: vector contains a mix of classic and native histograms (1:23)`,
				`PromQL warning: quantile value should be between 0 and 1, got 9 (1:20)`,
			},
			expectedWarningAnnotationsDelayedNameRemovalEnabled: []string{
				`PromQL warning: vector contains a mix of classic and native histograms for metric name "series" (1:23)`,
				`PromQL warning: quantile value should be between 0 and 1, got 9 (1:20)`,
			},
		},
		"forced monotonicity info is not emitted when quantile is invalid": {
			data:                       mixedClassicHistograms,
			expr:                       `histogram_quantile(2, series{host="d"})`,
			expectedWarningAnnotations: []string{`PromQL warning: quantile value should be between 0 and 1, got 2 (1:20)`},
		},
		"no le label on selected series": {
			data: `
				series  2
			`,
			expr:                       `histogram_quantile(0.5, series{})`,
			expectedWarningAnnotations: []string{`PromQL warning: bucket label "le" is missing or has a malformed value of "" (1:25)`},
			expectedWarningAnnotationsDelayedNameRemovalEnabled: []string{`PromQL warning: bucket label "le" is missing or has a malformed value of "" for metric name "series" (1:25)`},
		},
		"extra entry in series without le label": {
			data: `
				series{le="+Inf"} 1
				series  2
			`,
			expr:                       `histogram_quantile(0.5, series{})`,
			expectedWarningAnnotations: []string{`PromQL warning: bucket label "le" is missing or has a malformed value of "" (1:25)`},
			expectedWarningAnnotationsDelayedNameRemovalEnabled: []string{`PromQL warning: bucket label "le" is missing or has a malformed value of "" for metric name "series" (1:25)`},
		},
	}

	runAnnotationTests(t, testCases)
}

func getMixedMetricsForTests(includeClassicHistograms bool) ([]string, int, string) {
	// We're loading series with the following combinations of values. This is difficult to visually see in the actual
	// data loaded, so it is represented in a table here.
	// f = float value, h = native histogram, _ = no value, N = NaN, s = stale, i = infinity
	// {a} f f f f f f f
	// {b} h h h h h h h
	// {c} f h i h N h f
	// {d} f _ i s f f _
	// {e} h h _ s i N h
	// {f} f N _ i f N _
	// {g} N N N N N N N
	// {h} N N i _ N s N
	// {i} f h _ N h s i
	// {j} f i s s s s f
	// {k} 0 0 i N s 0 0
	// {l} h _ i _ s N f
	// {m} s i N _ _ f _
	// {n} _ _ _ _ _ _ _
	// {o} i i i i i i i

	pointsPerSeries := 7
	samples := `
		series{label="a", group="a"} 1 2 3 4 5 -50 100
		series{label="b", group="a"} {{schema:1 sum:15 count:10 buckets:[3 2 5 7 9]}} {{schema:2 sum:20 count:15 buckets:[4]}} {{schema:3 sum:25 count:20 buckets:[5 8]}} {{schema:4 sum:30 count:25 buckets:[6 9 10 11]}} {{schema:5 sum:35 count:30 buckets:[7 10 13]}} {{schema:6 sum:40 count:35 buckets:[8 11 14]}} {{schema:7 sum:45 count:40 buckets:[9 12 15]}}
		series{label="c", group="a"} 1 {{schema:3 sum:5 count:3 buckets:[1 1 1]}} -Inf {{schema:3 sum:10 count:6 buckets:[2 2 2]}} NaN {{schema:3 sum:12 count:7 buckets:[2 2 3]}} 5
		series{label="d", group="a"} 1 _ Inf stale 5 6 _
		series{label="e", group="b"} {{schema:4 sum:12 count:8 buckets:[2 3 3]}} {{schema:4 sum:14 count:9 buckets:[3 3 3]}} _ stale Inf NaN {{schema:4 sum:18 count:11 buckets:[4 4 3]}}
		series{label="f", group="b"} 1 NaN _ Inf 5 NaN _
		series{label="g", group="b"} NaN NaN NaN NaN NaN NaN NaN
		series{label="h", group="b"} NaN NaN Inf _ NaN stale NaN
		series{label="i", group="c"} 1 {{schema:5 sum:15 count:10 buckets:[3 2 5]}} _ NaN {{schema:2 sum:30 count:25 buckets:[6 9 10 9 1]}} stale Inf
		series{label="j", group="c"} 1 Inf stale stale stale stale 2
		series{label="k", group="c"} 0 0 -Inf NaN stale 0 0
		series{label="l", group="d"} {{schema:1 sum:10 count:5 buckets:[1 2]}} _ -Inf _ stale NaN 3
		series{label="m", group="d"} stale Inf NaN _ _ 4 _
		series{label="n", group="d"} _ _ _ _ _ _ _
		series{label="o", group="d"} Inf Inf -Inf Inf Inf -Inf -Inf`

	// Series p and q are special cases with classic histograms
	// q includes extra series without the `le` label, as well as different types in each bucket
	// {p} c c c c c c c
	// {q} (mixed)
	samples += `
		series{label="p", le="0.1", group="a"}  0+2x7
		series{label="p", le="1", group="a"}    0+1x7
		series{label="p", le="10", group="a"}   0+5x7
		series{label="p", le="100", group="a"}  0+4x7
		series{label="p", le="1000", group="a"} 0+9x7
		series{label="p", le="+Inf", group="a"} 0+8x7
		series{label="q", le="0.1", group="a"}  1 _ 2 3 stale NaN _
		series{label="q", le="1", group="a"}    2 _ Inf _ stale 5 _
		series{label="q", le="10", group="a"}   3 _ stale 3 stale 5 _
		series{label="q", le="100", group="a"}  4 _ 2 3 stale 5 _
		series{label="q", le="1000", group="a"} 5 {{schema:1 sum:10 count:5 buckets:[1 2]}} 2 3 stale 5 _
		series{label="q", le="+Inf", group="a"} 9 _ 2 3 NaN 5 _
		series{label="q", group="a"} 1 _ 2 {{schema:1 sum:10 count:5 buckets:[1 2]}} stale 5 _
	`

	labelsToUse := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o"}
	if includeClassicHistograms {
		labelsToUse = append(labelsToUse, []string{"p", "q"}...)
	}

	return labelsToUse, pointsPerSeries, samples
}

func runMixedMetricsTests(t *testing.T, expressions []string, pointsPerSeries int, samples string, skipAnnotationComparison bool) {
	// Although most tests are covered with the promql test files (both ours and upstream),
	// there is a lot of repetition around a few edge cases.
	// This is not intended to be comprehensive, but instead check for some common edge cases
	// ensuring MQE and Prometheus' engines return the same result when querying:
	// - Series with mixed floats and histograms
	// - Aggregations with mixed data types
	// - Points with NaN or infinity
	// - Stale markers
	// - Look backs

	opts := NewTestEngineOpts()
	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	mimirEngine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)
	prometheusEngine := promql.NewEngine(opts.CommonOpts)

	timeRanges := []struct {
		loadStep int
		interval time.Duration
	}{
		{loadStep: 1, interval: 1 * time.Minute},
		{loadStep: 1, interval: 6 * time.Minute},
		{loadStep: 1, interval: 5 * time.Minute},
		{loadStep: 6, interval: 6 * time.Minute},
		{loadStep: 6, interval: 5 * time.Minute},
	}

	for _, tr := range timeRanges {
		start := timestamp.Time(0)
		end := start.Add(time.Duration(pointsPerSeries*tr.loadStep) * time.Minute) // Deliberately queries 1 step past the final loaded point

		storage := promqltest.LoadedStorage(t, fmt.Sprintf("load %dm", tr.loadStep)+samples)
		t.Cleanup(func() { require.NoError(t, storage.Close()) })

		for _, expr := range expressions {
			// We run so many combinations that calling t.Run() for each of them has a noticeable performance impact.
			// So we instead just log the test case before we run it.
			t.Logf("Expr: %s, Start: %d, End: %d, Interval: %s", expr, start.Unix(), end.Unix(), tr.interval)
			prometheusQuery, err := prometheusEngine.NewRangeQuery(context.Background(), storage, nil, expr, start, end, tr.interval)
			require.NoError(t, err)
			prometheusResults := prometheusQuery.Exec(context.Background())

			mimirQuery, err := mimirEngine.NewRangeQuery(context.Background(), storage, nil, expr, start, end, tr.interval)
			require.NoError(t, err)
			mimirResults := mimirQuery.Exec(context.Background())
			if strings.Contains(expr, "histogram_quantile") {
				skipAnnotationComparison = true
			}
			mqetest.RequireEqualResults(t, expr, prometheusResults, mimirResults, skipAnnotationComparison)

			prometheusQuery.Close()
			mimirQuery.Close()
		}
	}
}

func TestCompareVariousMixedMetricsFunctions(t *testing.T) {
	t.Parallel()

	labelsToUse, pointsPerSeries, seriesData := getMixedMetricsForTests(true)

	// Test each label individually to catch edge cases in with single series
	labelCombinations := mqetest.Combinations(labelsToUse, 1)
	// Generate combinations of 2 labels. (e.g., "a,b", "e,f" etc)
	labelCombinations = append(labelCombinations, mqetest.Combinations(labelsToUse, 2)...)

	expressions := []string{}

	for _, labels := range labelCombinations {
		labelRegex := strings.Join(labels, "|")
		expressions = append(expressions, fmt.Sprintf(`histogram_avg(series{label=~"(%s)"})`, labelRegex))
		expressions = append(expressions, fmt.Sprintf(`histogram_count(series{label=~"(%s)"})`, labelRegex))
		expressions = append(expressions, fmt.Sprintf(`histogram_fraction(-5, 5, series{label=~"(%s)"})`, labelRegex))
		expressions = append(expressions, fmt.Sprintf(`histogram_fraction(0, scalar(series{label="i"}), series{label=~"(%s)"})`, labelRegex))
		expressions = append(expressions, fmt.Sprintf(`histogram_fraction(scalar(series{label="i"}), 2, series{label=~"(%s)"})`, labelRegex))
		expressions = append(expressions, fmt.Sprintf(`histogram_fraction(scalar(series{label="i"}), scalar(series{label="i"}), series{label=~"(%s)"})`, labelRegex))
		expressions = append(expressions, fmt.Sprintf(`histogram_quantile(0.8, series{label=~"(%s)"})`, labelRegex))
		expressions = append(expressions, fmt.Sprintf(`histogram_quantile(scalar(series{label="i"}), series{label=~"(%s)"})`, labelRegex))
		expressions = append(expressions, fmt.Sprintf(`histogram_stddev(series{label=~"(%s)"})`, labelRegex))
		expressions = append(expressions, fmt.Sprintf(`histogram_stdvar(series{label=~"(%s)"})`, labelRegex))
		expressions = append(expressions, fmt.Sprintf(`histogram_sum(series{label=~"(%s)"})`, labelRegex))
		expressions = append(expressions, fmt.Sprintf(`timestamp(series{label=~"(%s)"})`, labelRegex))
	}

	runMixedMetricsTests(t, expressions, pointsPerSeries, seriesData, false)
}

func TestCompareVariousMixedMetricsBinaryOperations(t *testing.T) {
	t.Parallel()

	labelsToUse, pointsPerSeries, seriesData := getMixedMetricsForTests(false)

	// Generate combinations of 2 and 3 labels. (e.g., "a,b", "e,f", "c,d,e" etc)
	labelCombinations := mqetest.Combinations(labelsToUse, 2)
	labelCombinations = append(labelCombinations, mqetest.Combinations(labelsToUse, 3)...)

	expressions := []string{}

	for _, labels := range labelCombinations {
		for _, op := range []string{"+", "-", "*", "/", "and", "unless", "or"} {
			expr := fmt.Sprintf(`series{label="%s"}`, labels[0])
			for _, label := range labels[1:] {
				expr += fmt.Sprintf(` %s series{label="%s"}`, op, label)
			}
			expressions = append(expressions, expr)

			// Same thing again, this time with grouping.
			expr = fmt.Sprintf(`series{label="%s"}`, labels[0])
			for i, label := range labels[1:] {
				expr += fmt.Sprintf(` %s ignoring (label, group) `, op)

				if i == 0 && len(labels) > 2 {
					expr += "("
				}

				expr += fmt.Sprintf(`{label="%s"}`, label)
			}
			if len(labels) > 2 {
				expr += ")"
			}
			expressions = append(expressions, expr)
		}

		// Similar thing again, this time with group_left
		expr := fmt.Sprintf(`series{label="%s"}`, labels[0])
		for i, label := range labels[1:] {
			expr += ` * on(group) group_left(label) `

			if i == 0 && len(labels) > 2 {
				expr += "("
			}

			expr += fmt.Sprintf(`{label="%s"}`, label)
		}
		if len(labels) > 2 {
			expr += ")"
		}
		expressions = append(expressions, expr)
	}

	runMixedMetricsTests(t, expressions, pointsPerSeries, seriesData, false)
}

func TestCompareVariousMixedMetricsAggregations(t *testing.T) {
	t.Parallel()

	labelsToUse, pointsPerSeries, seriesData := getMixedMetricsForTests(true)

	// Test each label individually to catch edge cases in with single series
	labelCombinations := mqetest.Combinations(labelsToUse, 1)
	// Generate combinations of 2 and 3 labels. (e.g., "a,b", "e,f", "c,d,e", "a,b,c,d", "c,d,e,f" etc)
	labelCombinations = append(labelCombinations, mqetest.Combinations(labelsToUse, 2)...)
	labelCombinations = append(labelCombinations, mqetest.Combinations(labelsToUse, 3)...)

	expressions := []string{}

	for _, labels := range labelCombinations {
		labelRegex := strings.Join(labels, "|")
		for _, aggFunc := range []string{"avg", "count", "group", "min", "max", "sum", "stddev", "stdvar"} {
			expressions = append(expressions, fmt.Sprintf(`%s(series{label=~"(%s)"})`, aggFunc, labelRegex))
			expressions = append(expressions, fmt.Sprintf(`%s by (group) (series{label=~"(%s)"})`, aggFunc, labelRegex))
			expressions = append(expressions, fmt.Sprintf(`%s without (group) (series{label=~"(%s)"})`, aggFunc, labelRegex))
		}

		expressions = append(expressions, fmt.Sprintf(`quantile (0.9, series{label=~"(%s)"})`, labelRegex))
		expressions = append(expressions, fmt.Sprintf(`quantile by (group) (0.9, series{label=~"(%s)"})`, labelRegex))
		expressions = append(expressions, fmt.Sprintf(`count_values("value", series{label="%s"})`, labelRegex))
	}

	runMixedMetricsTests(t, expressions, pointsPerSeries, seriesData, false)
}

func TestCompareVariousMixedMetricsVectorSelectors(t *testing.T) {
	t.Parallel()

	labelsToUse, pointsPerSeries, seriesData := getMixedMetricsForTests(true)

	expressions := []string{}

	// Test each label individually to catch edge cases in with single series
	labelCombinations := mqetest.Combinations(labelsToUse, 1)

	// We tried to have this test with 2 labels, but it was failing due to the inconsistent ordering of prometheus processing matchers that result in multiples series, e.g series{label=~"(c|e)"}.
	// Prometheus might process series c first or e first which will trigger different validation errors for second and third parameter of double_exponential_smoothing.
	// The different validation errors is occurred due to the range vector of the series being computed against values are skipped for the native histograms until it gets to a value where it has a float.
	// That aligns with a different scalar value for the argument and thus gives a different error.
	for _, labels := range labelCombinations {
		expressions = append(expressions, fmt.Sprintf(`double_exponential_smoothing(series{label=~"(%s)"}[1m], scalar(series{label="f"}),  scalar(series{label="i"}))`, labels))
	}

	// Generate combinations of 2 labels. (e.g., "a,b", "e,f" etc)
	labelCombinations = append(labelCombinations, mqetest.Combinations(labelsToUse, 2)...)

	for _, labels := range labelCombinations {
		labelRegex := strings.Join(labels, "|")
		// FIXME: irate() is temporarily disabled here due to https://github.com/prometheus/prometheus/pull/16199
		for _, function := range []string{"rate", "increase", "changes", "resets", "deriv", "idelta", "delta", "deriv", "stddev_over_time", "stdvar_over_time"} {
			expressions = append(expressions, fmt.Sprintf(`%s(series{label=~"(%s)"}[45s])`, function, labelRegex))
			expressions = append(expressions, fmt.Sprintf(`%s(series{label=~"(%s)"}[1m])`, function, labelRegex))
			expressions = append(expressions, fmt.Sprintf(`sum(%s(series{label=~"(%s)"}[2m15s]))`, function, labelRegex))
		}

		expressions = append(expressions, fmt.Sprintf(`predict_linear(series{label=~"(%s)"}[1m], 30)`, labelRegex))
		expressions = append(expressions, fmt.Sprintf(`quantile_over_time(scalar(series{label="i"}), series{label=~"(%s)"}[1m])`, labelRegex))
		expressions = append(expressions, fmt.Sprintf(`double_exponential_smoothing(series{label=~"(%s)"}[1m], 0.01, 0.1)`, labelRegex))
	}

	runMixedMetricsTests(t, expressions, pointsPerSeries, seriesData, false)
}

func TestCompareVariousMixedMetricsComparisonOps(t *testing.T) {
	t.Parallel()

	labelsToUse, pointsPerSeries, seriesData := getMixedMetricsForTests(true)

	// Test each label individually to catch edge cases in with single series
	labelCombinations := mqetest.Combinations(labelsToUse, 1)
	// Generate combinations of 2 labels. (e.g., "a,b", "e,f", etc)
	labelCombinations = append(labelCombinations, mqetest.Combinations(labelsToUse, 2)...)

	expressions := []string{}

	for _, labels := range labelCombinations {
		allLabelsRegex := strings.Join(labels, "|")
		for _, op := range []string{"==", "!=", ">", "<", ">=", "<="} {
			expressions = append(expressions, fmt.Sprintf(`series{label=~"(%s)"} %s 10`, allLabelsRegex, op))
			expressions = append(expressions, fmt.Sprintf(`1 %s series{label=~"(%s)"}`, op, allLabelsRegex))
			expressions = append(expressions, fmt.Sprintf(`series{label=~"(%s)"} %s Inf`, allLabelsRegex, op))
			expressions = append(expressions, fmt.Sprintf(`-Inf %s series{label=~"(%s)"}`, op, allLabelsRegex))
			expressions = append(expressions, fmt.Sprintf(`series{label=~"(%s)"} %s bool -10`, allLabelsRegex, op))
			expressions = append(expressions, fmt.Sprintf(`-1 %s bool series{label=~"(%s)"}`, op, allLabelsRegex))
			expressions = append(expressions, fmt.Sprintf(`series{label=~"(%s)"} %s bool Inf`, allLabelsRegex, op))
			expressions = append(expressions, fmt.Sprintf(`-Inf %s bool series{label=~"(%s)"}`, op, allLabelsRegex))

			// vector / vector cases
			vectorExpr := fmt.Sprintf(`series{label="%s"}`, labels[0])
			for _, label := range labels[1:] {
				vectorExpr += fmt.Sprintf(` %s series{label="%s"}`, op, label)
			}
			expressions = append(expressions, vectorExpr)
		}
	}

	runMixedMetricsTests(t, expressions, pointsPerSeries, seriesData, false)
}

func TestQueryStats(t *testing.T) {
	opts := NewTestEngineOpts()
	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	mimirEngine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	prometheusEngine := promql.NewEngine(opts.CommonOpts)

	start := timestamp.Time(0)
	end := start.Add(10 * time.Minute)

	storage := promqltest.LoadedStorage(t, `
		load 1m
			dense_series  0 1 2 3 4 5 6 7 8 9 10
			start_series  0 1 _ _ _ _ _ _ _ _ _
			end_series    _ _ _ _ _ 5 6 7 8 9 10
			sparse_series 0 _ _ _ _ _ _ 7 _ _ _
			stale_series  0 1 2 3 4 5 stale 7 8 9 10
			nan_series    NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN
			native_histogram_series {{schema:0 sum:2 count:4 buckets:[1 2 1]}} {{sum:2 count:4 buckets:[1 2 1]}}
			classic_histogram_series{le="0.1"}   0+1x10
			classic_histogram_series{le="1"}     0+5x10
			classic_histogram_series{le="10"}    0+8x10
			classic_histogram_series{le="100"}   0+12x10
			classic_histogram_series{le="1000"}  0+21x10
			classic_histogram_series{le="+Inf"}  0+21x10
	`)

	runQueryAndGetSamplesStats := func(t *testing.T, engine promql.QueryEngine, expr string, isInstantQuery bool) *promstats.QuerySamples {
		var q promql.Query
		var err error
		opts := promql.NewPrometheusQueryOpts(false, 0)
		if isInstantQuery {
			q, err = engine.NewInstantQuery(context.Background(), storage, opts, expr, end)
		} else {
			q, err = engine.NewRangeQuery(context.Background(), storage, opts, expr, start, end, time.Minute)
		}

		require.NoError(t, err)

		defer q.Close()

		res := q.Exec(context.Background())
		require.NoError(t, res.Err)

		return q.Stats().Samples
	}

	testCases := map[string]struct {
		expr                      string
		isInstantQuery            bool
		expectedTotalSamples      int64
		skipCompareWithPrometheus string
		// ...WithMQE expectations are optional and should be set only if a query with MQE reports different stats
		// (eg. due to optimisations like common subexpression elimination, or due to the different way we count samples in subqueries)
		expectedTotalSamplesWithMQE int64
	}{
		"instant vector selector with point at every time step": {
			expr:                 `dense_series{}`,
			expectedTotalSamples: 11,
		},
		"instant vector selector with points only in start of time range": {
			expr:                 `start_series{}`,
			expectedTotalSamples: 2 + 4, // 2 for original points, plus 4 for lookback to last point.
		},
		"instant vector selector with points only at end of time range": {
			expr:                 `end_series{}`,
			expectedTotalSamples: 6,
		},
		"instant vector selector with sparse points": {
			expr:                 `sparse_series{}`,
			expectedTotalSamples: 5 + 4, // 5 for first point at T=0, and 4 for second point at T=7
		},
		"instant vector selector with stale marker": {
			expr:                 `stale_series{}`,
			expectedTotalSamples: 10, // Instant vector selectors ignore stale markers.
		},
		"instant vector selector with @ modifier": {
			expr:                 `dense_series{} @ 0`,
			expectedTotalSamples: 1,
			isInstantQuery:       true,
		},
		"instant vector with offset modifier": {
			expr:                 `dense_series{} offset 2m`,
			expectedTotalSamples: 9,
		},
		"instant vector with offset modifier before start of the series": {
			expr:                 `dense_series{} offset 1w`,
			expectedTotalSamples: 0,
		},

		"raw range vector selector with single point": {
			expr:                 `dense_series[45s]`,
			isInstantQuery:       true,
			expectedTotalSamples: 1,
		},
		"raw range vector selector with multiple points": {
			expr:                 `dense_series[3m45s]`,
			isInstantQuery:       true,
			expectedTotalSamples: 4,
		},
		"range vector selector with point at every time step": {
			expr:                 `sum_over_time(dense_series{}[30s])`,
			expectedTotalSamples: 11,
		},
		"range vector selector with 2 points at every time step": {
			expr:                 `sum_over_time(dense_series{}[1m30s])`,
			expectedTotalSamples: 21,
		},
		"range vector selector with points only in start of time range": {
			expr:                 `sum_over_time(start_series{}[30s])`,
			expectedTotalSamples: 2,
		},
		"range vector selector with points only at end of time range": {
			expr:                 `sum_over_time(end_series{}[30s])`,
			expectedTotalSamples: 6,
		},
		"range vector selector with sparse points": {
			expr:                 `sum_over_time(sparse_series{}[30s])`,
			expectedTotalSamples: 2,
		},
		"range vector selector where range overlaps previous step's range": {
			expr:                 `sum_over_time(dense_series{}[1m30s])`,
			expectedTotalSamples: 21, // Each step except the first selects two points.
		},
		"range vector selector with stale marker": {
			expr:                 `count_over_time(stale_series{}[1m30s])`,
			expectedTotalSamples: 19, // Each step except the first selects two points. Range vector selectors ignore stale markers.
		},
		"expression with multiple selectors": {
			expr:                 `dense_series{} + end_series{}`,
			expectedTotalSamples: 11 + 6,
		},
		"instant vector selector with NaNs": {
			expr:                 `nan_series{}`,
			expectedTotalSamples: 11,
		},
		"range vector selector with NaNs": {
			expr:                 `sum_over_time(nan_series{}[1m])`,
			expectedTotalSamples: 11,
		},
		"instant vector selector with native histograms": {
			expr:                 `native_histogram_series{}`,
			expectedTotalSamples: 78,
		},
		"range vector selector with native histograms": {
			expr:                 `sum_over_time(native_histogram_series{}[1m])`,
			expectedTotalSamples: 26,
		},
		"range vector selector with @ modifier": {
			expr:                        `sum_over_time(dense_series{}[2m] @ 300)`,
			expectedTotalSamples:        22, // each step selects 2 points at T=300 over query range
			expectedTotalSamplesWithMQE: 2,  // the range vector with @ is step invariant so these 2 samples are re-used for each step
		},
		"subquery": {
			expr:                 `dense_series{}[5m:1m]`,
			expectedTotalSamples: 5,
			isInstantQuery:       true,
		},
		"aggregation over subquery": {
			expr:                 `max_over_time(dense_series{}[5m:1m])`,
			expectedTotalSamples: 5,
			isInstantQuery:       true,
		},
		"aggregation over subquery - range query": {
			expr:                        `max_over_time(dense_series[5m:1m])`,
			expectedTotalSamples:        45,
			expectedTotalSamplesWithMQE: 11,
		},
		"subquery range equals subquery interval": {
			expr:                 `dense_series[1m:1m]`,
			expectedTotalSamples: 1,
			isInstantQuery:       true,
		},
		"subquery range equals subquery interval -  range query": {
			expr:                 `max_over_time(dense_series{}[1m:1m])`,
			expectedTotalSamples: 11,
		},
		"subquery resolution greater than subquery interval": {
			expr:                 `dense_series{}[1m:5m]`,
			expectedTotalSamples: 1,
			isInstantQuery:       true,
		},
		"subquery resolution greater than subquery interval - range query": {
			expr:                 `max_over_time(dense_series{}[1m:5m])`,
			expectedTotalSamples: 3,
			isInstantQuery:       false,
		},
		"subquery not aligned with parent query": {
			expr:                 `dense_series{}[5m:44s]`,
			expectedTotalSamples: 7,
			isInstantQuery:       true,
		},
		"subquery not aligned with parent query - range query": {
			expr:                        `max_over_time(dense_series{}[5m:44s])`,
			expectedTotalSamples:        57,
			expectedTotalSamplesWithMQE: 14,
		},
		"classic histogram quantile": {
			expr:                 `histogram_quantile(0.9, rate(classic_histogram_series[5m]))`,
			expectedTotalSamples: 30,
			isInstantQuery:       true,
		},
		"classic histogram quantile – range query": {
			expr:                 `histogram_quantile(0.9, rate(classic_histogram_series[5m]))`,
			expectedTotalSamples: 270,
		},
		"classic histogram fraction": {
			expr:                 `histogram_fraction(10, 100, rate(classic_histogram_series[5m]))`,
			expectedTotalSamples: 30,
			isInstantQuery:       true,
		},
		"classic histogram fraction – range query": {
			expr:                 `histogram_fraction(10, 100, rate(classic_histogram_series[5m]))`,
			expectedTotalSamples: 270,
		},
		"common subexpression elimination": {
			expr:                        `sum(dense_series) + sum(dense_series)`,
			isInstantQuery:              true,
			expectedTotalSamples:        2,
			expectedTotalSamplesWithMQE: 1,
		},
		"common subexpression elimination inside subquery, instant query": {
			expr:                        `sum_over_time((sum(dense_series))[5m:1m]) + sum_over_time((count(dense_series))[5m:1m])`,
			isInstantQuery:              true,
			expectedTotalSamples:        10,
			expectedTotalSamplesWithMQE: 5,
		},
		"common subexpression elimination inside subquery, range query": {
			expr:                        `sum_over_time((sum(dense_series))[5m:1m]) + sum_over_time((count(dense_series))[5m:1m])`,
			expectedTotalSamples:        90,
			expectedTotalSamplesWithMQE: 11,
		},
		// Three tests below cover PQE bug: sample counting is incorrect when subqueries with range vector selectors are wrapped in functions.
		// In MQE it's fixed, so that's why cases have a skipCompareWithPrometheus set.
		// See this for details: https://github.com/prometheus/prometheus/issues/16638
		"subquery with range vector selector": {
			expr:                        `rate(dense_series[1m30s])[5m:1m]`,
			expectedTotalSamples:        10,
			expectedTotalSamplesWithMQE: 10,
			isInstantQuery:              true,
		},
		"aggregation over subquery with range vector selector": {
			expr:                        `max_over_time(rate(dense_series[1m30s])[5m:1m])`,
			expectedTotalSamples:        5,
			expectedTotalSamplesWithMQE: 10,
			isInstantQuery:              true,
		},
		"aggregation over subquery with range vector selector, range query": {
			expr:                        `max_over_time(rate(dense_series[1m30s])[5m:1m])`,
			expectedTotalSamples:        40,
			expectedTotalSamplesWithMQE: 21,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			prometheusSamplesStats := runQueryAndGetSamplesStats(t, prometheusEngine, testCase.expr, testCase.isInstantQuery)
			if testCase.skipCompareWithPrometheus == "" {
				require.Equal(t, testCase.expectedTotalSamples, prometheusSamplesStats.TotalSamples, "invalid test case: expected total samples does not match value from Prometheus' engine")
			}

			mimirSamplesStatsWithPlanning := runQueryAndGetSamplesStats(t, mimirEngine, testCase.expr, testCase.isInstantQuery)
			if testCase.expectedTotalSamplesWithMQE != 0 {
				require.Equal(t, testCase.expectedTotalSamplesWithMQE, mimirSamplesStatsWithPlanning.TotalSamples)
			} else {
				require.Equal(t, testCase.expectedTotalSamples, mimirSamplesStatsWithPlanning.TotalSamples)
			}
		})
	}
}

func TestQueryStatsUpstreamTestCases(t *testing.T) {
	// TestCases are taken from Prometheus' TestQueryStatistics.
	opts := NewTestEngineOpts()
	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	mimirEngine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	prometheusEngine := promql.NewEngine(opts.CommonOpts)

	storage := promqltest.LoadedStorage(t, `
		load 10s
		  metricWith1SampleEvery10Seconds 1+1x100
		  metricWith3SampleEvery10Seconds{a="1",b="1"} 1+1x100
		  metricWith3SampleEvery10Seconds{a="2",b="2"} 1+1x100
		  metricWith3SampleEvery10Seconds{a="3",b="2"} 1+1x100
		  metricWith1HistogramEvery10Seconds {{schema:1 count:5 sum:20 buckets:[1 2 1 1]}}+{{schema:1 count:10 sum:5 buckets:[1 2 3 4]}}x100
	`)
	t.Cleanup(func() { storage.Close() })

	runQueryAndGetSamplesStats := func(t *testing.T, engine promql.QueryEngine, expr string, start, end time.Time, interval time.Duration) *promstats.QuerySamples {
		var q promql.Query
		var err error
		opts := promql.NewPrometheusQueryOpts(false, 0)

		if interval == 0 {
			// Instant query
			q, err = engine.NewInstantQuery(context.Background(), storage, opts, expr, start)
		} else {
			// Range query
			q, err = engine.NewRangeQuery(context.Background(), storage, opts, expr, start, end, interval)
		}

		require.NoError(t, err)
		defer q.Close()

		res := q.Exec(context.Background())
		require.NoError(t, res.Err)

		return q.Stats().Samples
	}

	cases := []struct {
		query                string
		start                time.Time
		end                  time.Time
		interval             time.Duration
		expectedTotalSamples int64
		// ...WithMQE expectations are optional and should be set only if a query with MQE reports different stats
		// (eg. due to optimisations like common subexpression elimination, or due to the different way we count samples in subqueries)
		expectedTotalSamplesWithMQE int64
	}{
		{
			query:                `"literal string"`,
			start:                time.Unix(21, 0),
			expectedTotalSamples: 0,
		},
		{
			query:                "1",
			start:                time.Unix(21, 0),
			expectedTotalSamples: 0,
		},
		{
			query:                "metricWith1SampleEvery10Seconds",
			start:                time.Unix(21, 0),
			expectedTotalSamples: 1, // 1 sample / 10 seconds
		},
		{
			query:                "metricWith1HistogramEvery10Seconds",
			start:                time.Unix(21, 0),
			expectedTotalSamples: 13, // 1 histogram HPoint of size 13 / 10 seconds
		},
		{
			// timestamp function has a special handling.
			query:                "timestamp(metricWith1SampleEvery10Seconds)",
			start:                time.Unix(21, 0),
			expectedTotalSamples: 1, // 1 sample / 10 seconds
		},
		{
			query:                "timestamp(metricWith1HistogramEvery10Seconds)",
			start:                time.Unix(21, 0),
			expectedTotalSamples: 1, // 1 float sample (because of timestamp) / 10 seconds
		},
		{
			query:                "metricWith1SampleEvery10Seconds",
			start:                time.Unix(22, 0),
			expectedTotalSamples: 1, // 1 sample / 10 seconds
		},
		{
			query:                "metricWith1SampleEvery10Seconds offset 10s",
			start:                time.Unix(21, 0),
			expectedTotalSamples: 1, // 1 sample / 10 seconds
		},
		{
			query:                "metricWith1SampleEvery10Seconds @ 15",
			start:                time.Unix(21, 0),
			expectedTotalSamples: 1, // 1 sample / 10 seconds
		},
		{
			query:                `metricWith3SampleEvery10Seconds{a="1"}`,
			start:                time.Unix(21, 0),
			expectedTotalSamples: 1, // 1 sample / 10 seconds
		},
		{
			query:                `metricWith3SampleEvery10Seconds{a="1"} @ 19`,
			start:                time.Unix(21, 0),
			expectedTotalSamples: 1, // 1 sample / 10 seconds
		},
		{
			query:                `metricWith3SampleEvery10Seconds{a="1"}[20s] @ 19`,
			start:                time.Unix(21, 0),
			expectedTotalSamples: 2, // (1 sample / 10 seconds) * 20s
		},
		{
			query:                "metricWith3SampleEvery10Seconds",
			start:                time.Unix(21, 0),
			expectedTotalSamples: 3, // 3 samples / 10 seconds
		},
		{
			query:                "metricWith1SampleEvery10Seconds[60s]",
			start:                time.Unix(201, 0),
			expectedTotalSamples: 6, // 1 sample / 10 seconds * 60 seconds
		},
		{
			query:                "metricWith1HistogramEvery10Seconds[60s]",
			start:                time.Unix(201, 0),
			expectedTotalSamples: 78, // 1 histogram (size 13 HPoint) / 10 seconds * 60 seconds
		},
		{
			query:                "max_over_time(metricWith1SampleEvery10Seconds[60s])[20s:5s]",
			start:                time.Unix(201, 0),
			expectedTotalSamples: 24, // (1 sample / 10 seconds * 60 seconds) * 4
		},
		{
			query:                "max_over_time(metricWith1SampleEvery10Seconds[61s])[20s:5s]",
			start:                time.Unix(201, 0),
			expectedTotalSamples: 26, // (1 sample / 10 seconds * 60 seconds) * 4 + 2 as
		},
		{
			query:                "max_over_time(metricWith1HistogramEvery10Seconds[60s])[20s:5s]",
			start:                time.Unix(201, 0),
			expectedTotalSamples: 312, // (1 histogram (size 13) / 10 seconds * 60 seconds) * 4
		},
		{
			query:                "metricWith1SampleEvery10Seconds[60s] @ 30",
			start:                time.Unix(201, 0),
			expectedTotalSamples: 4, // @ modifier force the evaluation to at 30 seconds - So it brings 4 datapoints (0, 10, 20, 30 seconds) * 1 series
		},
		{
			query:                "metricWith1HistogramEvery10Seconds[60s] @ 30",
			start:                time.Unix(201, 0),
			expectedTotalSamples: 52, // @ modifier force the evaluation to at 30 seconds - So it brings 4 datapoints (0, 10, 20, 30 seconds) * 1 series
		},
		{
			query:                "sum(max_over_time(metricWith3SampleEvery10Seconds[60s] @ 30))",
			start:                time.Unix(201, 0),
			expectedTotalSamples: 12, // @ modifier force the evaluation to at 30 seconds - So it brings 4 datapoints (0, 10, 20, 30 seconds) * 3 series
		},
		{
			query:                "sum by (b) (max_over_time(metricWith3SampleEvery10Seconds[60s] @ 30))",
			start:                time.Unix(201, 0),
			expectedTotalSamples: 12, // @ modifier force the evaluation to at 30 seconds - So it brings 4 datapoints (0, 10, 20, 30 seconds) * 3 series
		},
		{
			query:                "metricWith1SampleEvery10Seconds[60s] offset 10s",
			start:                time.Unix(201, 0),
			expectedTotalSamples: 6, // 1 sample / 10 seconds * 60 seconds
		},
		{
			query:                "metricWith3SampleEvery10Seconds[60s]",
			start:                time.Unix(201, 0),
			expectedTotalSamples: 18, // 3 sample / 10 seconds * 60 seconds
		},
		{
			query:                "max_over_time(metricWith1SampleEvery10Seconds[60s])",
			start:                time.Unix(201, 0),
			expectedTotalSamples: 6, // 1 sample / 10 seconds * 60 seconds
		},
		{
			query:                "absent_over_time(metricWith1SampleEvery10Seconds[60s])",
			start:                time.Unix(201, 0),
			expectedTotalSamples: 6, // 1 sample / 10 seconds * 60 seconds
		},
		{
			query:                "max_over_time(metricWith3SampleEvery10Seconds[60s])",
			start:                time.Unix(201, 0),
			expectedTotalSamples: 18, // 3 sample / 10 seconds * 60 seconds
		},
		{
			query:                "metricWith1SampleEvery10Seconds[60s:5s]",
			start:                time.Unix(201, 0),
			expectedTotalSamples: 12, // 1 sample per query * 12 queries (60/5)
		},
		{
			query:                "metricWith1SampleEvery10Seconds[60s:5s] offset 10s",
			start:                time.Unix(201, 0),
			expectedTotalSamples: 12, // 1 sample per query * 12 queries (60/5)
		},
		{
			query:                "max_over_time(metricWith3SampleEvery10Seconds[60s:5s])",
			start:                time.Unix(201, 0),
			expectedTotalSamples: 36, // 3 sample per query * 12 queries (60/5)
		},
		{
			query:                       "sum(max_over_time(metricWith3SampleEvery10Seconds[60s:5s])) + sum(max_over_time(metricWith3SampleEvery10Seconds[60s:5s]))",
			start:                       time.Unix(201, 0),
			expectedTotalSamples:        72, // 2 * (3 sample per query * 12 queries (60/5))
			expectedTotalSamplesWithMQE: 36, // 72/2 due to common subexpression elimination
		},
		{
			query:                `metricWith3SampleEvery10Seconds{a="1"}`,
			start:                time.Unix(201, 0),
			end:                  time.Unix(220, 0),
			interval:             5 * time.Second,
			expectedTotalSamples: 4, // 1 sample per query * 4 steps
		},
		{
			query:                `metricWith3SampleEvery10Seconds{a="1"}`,
			start:                time.Unix(204, 0),
			end:                  time.Unix(223, 0),
			interval:             5 * time.Second,
			expectedTotalSamples: 4, // 1 sample per query * 4 steps
		},
		{
			query:                `metricWith1HistogramEvery10Seconds`,
			start:                time.Unix(204, 0),
			end:                  time.Unix(223, 0),
			interval:             5 * time.Second,
			expectedTotalSamples: 52, // 1 histogram (size 13 HPoint) per query * 4 steps
		},
		{
			// timestamp function has a special handling
			query:                "timestamp(metricWith1SampleEvery10Seconds)",
			start:                time.Unix(201, 0),
			end:                  time.Unix(220, 0),
			interval:             5 * time.Second,
			expectedTotalSamples: 4, // 1 sample per query * 4 steps
		},
		{
			// timestamp function has a special handling
			query:                "timestamp(metricWith1HistogramEvery10Seconds)",
			start:                time.Unix(201, 0),
			end:                  time.Unix(220, 0),
			interval:             5 * time.Second,
			expectedTotalSamples: 4, // 1 sample per query * 4 steps
		},
		{
			query:                `max_over_time(metricWith3SampleEvery10Seconds{a="1"}[10s])`,
			start:                time.Unix(991, 0),
			end:                  time.Unix(1021, 0),
			interval:             10 * time.Second,
			expectedTotalSamples: 2, // 1 sample per query * 2 steps with data
		},
		{
			query:                `metricWith3SampleEvery10Seconds{a="1"} offset 10s`,
			start:                time.Unix(201, 0),
			end:                  time.Unix(220, 0),
			interval:             5 * time.Second,
			expectedTotalSamples: 4, // 1 sample per query * 4 steps
		},
		{
			query:                       "max_over_time(metricWith3SampleEvery10Seconds[60s] @ 30)",
			start:                       time.Unix(201, 0),
			end:                         time.Unix(220, 0),
			interval:                    5 * time.Second,
			expectedTotalSamples:        48, // @ modifier force the evaluation timestamp at 30 seconds - So it brings 4 datapoints (0, 10, 20, 30 seconds) * 3 series * 4 steps
			expectedTotalSamplesWithMQE: 12, // the @ modifier allows for this range vector to be considered a step invariant
		},
		{
			query:                `metricWith3SampleEvery10Seconds`,
			start:                time.Unix(201, 0),
			end:                  time.Unix(220, 0),
			interval:             5 * time.Second,
			expectedTotalSamples: 12, // 3 sample per query * 4 steps
		},
		{
			query:                `max_over_time(metricWith3SampleEvery10Seconds[60s])`,
			start:                time.Unix(201, 0),
			end:                  time.Unix(220, 0),
			interval:             5 * time.Second,
			expectedTotalSamples: 72, // (3 sample / 10 seconds * 60 seconds) * 4 steps = 72
		},
		{
			query:                       "max_over_time(metricWith3SampleEvery10Seconds[60s:5s])",
			start:                       time.Unix(201, 0),
			end:                         time.Unix(220, 0),
			interval:                    5 * time.Second,
			expectedTotalSamples:        144, // 3 sample per query * 12 queries (60/5) * 4 steps
			expectedTotalSamplesWithMQE: 48,
		},
		{
			query:                       "max_over_time(metricWith1SampleEvery10Seconds[60s:5s])",
			start:                       time.Unix(201, 0),
			end:                         time.Unix(220, 0),
			interval:                    5 * time.Second,
			expectedTotalSamples:        48, // 1 sample per query * 12 queries (60/5) * 4 steps
			expectedTotalSamplesWithMQE: 16,
		},
		{
			query:                       "sum by (b) (max_over_time(metricWith1SampleEvery10Seconds[60s:5s]))",
			start:                       time.Unix(201, 0),
			end:                         time.Unix(220, 0),
			interval:                    5 * time.Second,
			expectedTotalSamples:        48, // 1 sample per query * 12 queries (60/5) * 4 steps
			expectedTotalSamplesWithMQE: 16,
		},
		{
			query:                       "sum(max_over_time(metricWith3SampleEvery10Seconds[60s:5s])) + sum(max_over_time(metricWith3SampleEvery10Seconds[60s:5s]))",
			start:                       time.Unix(201, 0),
			end:                         time.Unix(220, 0),
			interval:                    5 * time.Second,
			expectedTotalSamples:        288, // 2 * (3 sample per query * 12 queries (60/5) * 4 steps)
			expectedTotalSamplesWithMQE: 48,
		},
		{
			query:                       "sum(max_over_time(metricWith3SampleEvery10Seconds[60s:5s])) + sum(max_over_time(metricWith1SampleEvery10Seconds[60s:5s]))",
			start:                       time.Unix(201, 0),
			end:                         time.Unix(220, 0),
			interval:                    5 * time.Second,
			expectedTotalSamples:        192, // (1 sample per query * 12 queries (60/5) + 3 sample per query * 12 queries (60/5)) * 4 steps
			expectedTotalSamplesWithMQE: 64,
		},
	}

	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			prometheusSamplesStats := runQueryAndGetSamplesStats(t, prometheusEngine, tc.query, tc.start, tc.end, tc.interval)
			require.Equal(t, tc.expectedTotalSamples, prometheusSamplesStats.TotalSamples, "invalid test case: expected total samples does not match value from Prometheus' engine")

			mimirSamplesStatsWithPlanning := runQueryAndGetSamplesStats(t, mimirEngine, tc.query, tc.start, tc.end, tc.interval)
			if tc.expectedTotalSamplesWithMQE != 0 {
				require.Equal(t, tc.expectedTotalSamplesWithMQE, mimirSamplesStatsWithPlanning.TotalSamples)
			} else {
				require.Equal(t, tc.expectedTotalSamples, mimirSamplesStatsWithPlanning.TotalSamples)
			}
		})
	}
}

func TestQueryStatementLookbackDelta(t *testing.T) {
	stats := stats.NewQueryMetrics(nil)

	runTest := func(t *testing.T, engine promql.QueryEngine, queryOpts promql.QueryOpts, expectedLookbackDelta time.Duration) {
		q, err := engine.NewInstantQuery(context.Background(), nil, queryOpts, "1", time.Now())
		require.NoError(t, err)

		require.Equal(t, expectedLookbackDelta, q.Statement().(*parser.EvalStmt).LookbackDelta)
	}

	t.Run("engine with no lookback delta configured", func(t *testing.T) {
		engineOpts := NewTestEngineOpts()
		planner, err := NewQueryPlanner(engineOpts, NewMaximumSupportedVersionQueryPlanVersionProvider())
		require.NoError(t, err)
		engine, err := NewEngine(engineOpts, stats, planner)
		require.NoError(t, err)

		t.Run("lookback delta not set in query options", func(t *testing.T) {
			queryOpts := promql.NewPrometheusQueryOpts(false, 0)
			runTest(t, engine, queryOpts, defaultLookbackDelta)
		})

		t.Run("no query options provided", func(t *testing.T) {
			runTest(t, engine, nil, defaultLookbackDelta)
		})

		t.Run("lookback delta set in query options", func(t *testing.T) {
			queryOpts := promql.NewPrometheusQueryOpts(false, 14*time.Minute)
			runTest(t, engine, queryOpts, 14*time.Minute)
		})
	})

	t.Run("engine with lookback delta configured", func(t *testing.T) {
		engineOpts := NewTestEngineOpts()
		engineOpts.CommonOpts.LookbackDelta = 12 * time.Minute
		planner, err := NewQueryPlanner(engineOpts, NewMaximumSupportedVersionQueryPlanVersionProvider())
		require.NoError(t, err)
		engine, err := NewEngine(engineOpts, stats, planner)
		require.NoError(t, err)

		t.Run("lookback delta not set in query options", func(t *testing.T) {
			queryOpts := promql.NewPrometheusQueryOpts(false, 0)
			runTest(t, engine, queryOpts, 12*time.Minute)
		})

		t.Run("no query options provided", func(t *testing.T) {
			runTest(t, engine, nil, 12*time.Minute)
		})

		t.Run("lookback delta set in query options", func(t *testing.T) {
			queryOpts := promql.NewPrometheusQueryOpts(false, 14*time.Minute)
			runTest(t, engine, queryOpts, 14*time.Minute)
		})
	})
}

func TestQueryClose(t *testing.T) {
	storage := promqltest.LoadedStorage(t, `
		load 1m
			some_metric{idx="1"} 0+1x5
			some_metric{idx="2"} 0+1x5
			some_metric{idx="3"} 0+1x5
			some_metric{idx="4"} 0+1x5
			some_metric{idx="5"} 0+1x5
			some_histogram{idx="1"} {{schema:1 sum:10 count:9 buckets:[3 3 3]}}x5
			some_histogram{idx="2"} {{schema:1 sum:10 count:9 buckets:[3 3 3]}}x5
	`)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	opts := NewTestEngineOpts()
	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	start := timestamp.Time(0)
	end := start.Add(4 * time.Minute)
	step := time.Minute

	q, err := engine.NewRangeQuery(context.Background(), storage, nil, `count({__name__=~"some_.*"})`, start, end, step)
	require.NoError(t, err)

	res := q.Exec(context.Background())
	require.NoError(t, res.Err)

	q.Close()
	mqeQuery, ok := q.(*Query)
	require.True(t, ok)
	require.Equal(t, uint64(0), mqeQuery.memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

	// Close the query a second time, to ensure that closing the query again does not cause any issues.
	q.Close()
	require.Equal(t, uint64(0), mqeQuery.memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
}

func TestEagerLoadSelectors(t *testing.T) {
	storage := promqltest.LoadedStorage(t, `
		load 1m
			some_metric 0+1x5
			some_other_metric 0+2x5
	`)

	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	metrics := stats.NewQueryMetrics(nil)
	optsWithoutEagerLoading := NewTestEngineOpts()
	plannerWithoutEagerLoading, err := NewQueryPlanner(optsWithoutEagerLoading, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engineWithoutEagerLoading, err := NewEngine(optsWithoutEagerLoading, metrics, plannerWithoutEagerLoading)
	require.NoError(t, err)

	optsWithEagerLoading := NewTestEngineOpts()
	optsWithEagerLoading.EagerLoadSelectors = true
	plannerWithEagerLoading, err := NewQueryPlanner(optsWithEagerLoading, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engineWithEagerLoading, err := NewEngine(optsWithEagerLoading, metrics, plannerWithEagerLoading)
	require.NoError(t, err)

	testCases := []string{
		`sum(some_metric) + sum(some_other_metric)`,
		`sum(rate(some_metric[5m])) + sum(rate(some_other_metric[5m]))`,
	}

	ctx := context.Background()
	ts := timestamp.Time(0).Add(5 * time.Minute)

	for _, expr := range testCases {
		t.Run(expr, func(t *testing.T) {
			// First, run without eager loading to get expected result
			q, err := engineWithoutEagerLoading.NewInstantQuery(ctx, storage, nil, expr, ts)
			require.NoError(t, err)
			baselineResult := q.Exec(ctx)
			require.NoError(t, baselineResult.Err)
			defer q.Close()

			// Run with eager loading (as it would in query-frontends) and queryable that will return an error if both Select calls aren't run in parallel.
			synchronisingStorage := newSynchronisingQueryable(storage, 2)
			lazyStorage := lazyquery.NewLazyQueryable(synchronisingStorage)
			q, err = engineWithEagerLoading.NewInstantQuery(ctx, lazyStorage, nil, expr, ts)
			require.NoError(t, err)
			eagerLoadingResult := q.Exec(ctx)
			require.NoError(t, eagerLoadingResult.Err)
			defer q.Close()

			mqetest.RequireEqualResults(t, expr, baselineResult, eagerLoadingResult, false)
			require.True(t, synchronisingStorage.sawExpectedSelectCalls)
		})
	}
}

type synchronisingQueryable struct {
	inner                  storage.Queryable
	startGroup             *sync.WaitGroup // Incremented when each Select call is made
	releaseSelectCalls     <-chan struct{} // Closed once all expected Select calls have been made, to release Select calls
	sawExpectedSelectCalls bool
}

func newSynchronisingQueryable(inner storage.Queryable, expectedSelectCalls int) *synchronisingQueryable {
	startGroup := &sync.WaitGroup{}
	startGroup.Add(expectedSelectCalls)
	releaseSelectCalls := make(chan struct{})

	q := &synchronisingQueryable{
		inner:              inner,
		startGroup:         startGroup,
		releaseSelectCalls: releaseSelectCalls,
	}

	go func() {
		defer close(releaseSelectCalls) // Always close the channel, to ensure the test doesn't deadlock.

		err := syncutil.WaitWithTimeout(startGroup, 2*time.Second)
		if err == nil {
			q.sawExpectedSelectCalls = true
		}
	}()

	return q
}

func (s *synchronisingQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	q, err := s.inner.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}

	return &synchronisingQuerier{q, s.startGroup, s.releaseSelectCalls}, nil
}

type synchronisingQuerier struct {
	inner              storage.Querier
	startGroup         *sync.WaitGroup
	releaseSelectCalls <-chan struct{}
}

func (s *synchronisingQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	panic("not supported")
}

func (s *synchronisingQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	panic("not supported")
}

func (s *synchronisingQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	s.startGroup.Done()

	select {
	case <-s.releaseSelectCalls:
		return s.inner.Select(ctx, sortSeries, hints, matchers...)
	case <-ctx.Done():
		return storage.ErrSeriesSet(context.Cause(ctx))
	case <-time.After(time.Second):
		return storage.ErrSeriesSet(errors.New("gave up waiting for all Select calls to be running in parallel"))
	}
}

func (s *synchronisingQuerier) Close() error {
	return s.inner.Close()
}

func TestInstantQueryDurationExpression(t *testing.T) {
	// promqltest's "check an instant query works as a range query" behaviour makes it difficult to test step() in an instant query, so we do it here instead.

	storage := promqltest.LoadedStorage(t, `
		load 1ms
			some_metric 0+1x300
	`)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	opts := NewTestEngineOpts()
	prometheusEngine := promql.NewEngine(opts.CommonOpts)
	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	mimirEngine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	ctx := context.Background()
	expr := "count_over_time(some_metric[step()+1ms])"
	ts := timestamp.Time(0).Add(5 * time.Millisecond)

	prometheusQuery, err := prometheusEngine.NewInstantQuery(ctx, storage, nil, expr, ts)
	require.NoError(t, err)
	prometheusResult := prometheusQuery.Exec(ctx)
	require.NoError(t, prometheusResult.Err)
	t.Cleanup(prometheusQuery.Close)

	mimirQuery, err := mimirEngine.NewInstantQuery(ctx, storage, nil, expr, ts)
	require.NoError(t, err)
	mimirResult := mimirQuery.Exec(ctx)
	require.NoError(t, mimirResult.Err)
	t.Cleanup(mimirQuery.Close)

	mqetest.RequireEqualResults(t, expr, prometheusResult, mimirResult, false)
}

func TestEngine_RegisterNodeMaterializer(t *testing.T) {
	opts := NewTestEngineOpts()
	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	nodeType := planning.NodeType(1234)
	materializer := dummyMaterializer{}
	require.NoError(t, engine.RegisterNodeMaterializer(nodeType, materializer), "should not fail to register new node type")

	require.EqualError(t, engine.RegisterNodeMaterializer(nodeType, materializer), "materializer for node type 1234 already registered", "should fail to register materializer again if already registered")
}

// TestExtendedRangeSelectors has tests specific to the anchored and smoothed range modifiers.
// The results can have points which are not aligned to the step interval, and as such creating promql *.test scripts which inspect the raw range result is not possible.
func TestExtendedRangeSelectors(t *testing.T) {
	storage := promqltest.LoadedStorage(t, `
	load 1m
    	metric 1 2 _ 4 5
		another_metric{id="1"} 1+1x4 1+1x4
    	another_metric{id="2"} 3 2+2x9
    	another_metric{id="3"} 5+3x2 3+3x6
	`)
	t.Cleanup(func() { storage.Close() })

	tc := []struct {
		query    string
		t        time.Time
		expected promql.Matrix
	}{
		{
			// There is no values within the range of 1m-2m (left-open / right-closed).
			// Because of that no back-filling from the look-back is used
			query:    "metric[1m] anchored",
			t:        time.Unix(120, 0),
			expected: types.GetMatrix(0),
		},

		{
			// The range is 59s - 2m
			// The value of 1 (T=0) is picked up in the look-back <= 59s
			// The value of 2 (T=1m) is picked up as it's in this time range
			// The value of 2 (T=1m) is re-used for the value at the end of the range
			query: "metric[1m1s] anchored",
			t:     time.Unix(120, 0),
			expected: promql.Matrix{
				promql.Series{
					Floats: []promql.FPoint{{F: 1, T: 59000}, {F: 2, T: 60000}, {F: 2, T: 120000}},
					Metric: labels.FromStrings("__name__", "metric"),
				},
			},
		},

		{
			// There is no values within the range of 1m-2m (left-open / right-closed).
			// Because of that no back-filling from the look-back is used
			query:    "metric[59s] anchored",
			t:        time.Unix(120, 0),
			expected: types.GetMatrix(0),
		},

		{
			// Without the anchored modifier, these range queries only return a single point
			query: "another_metric[1m]",
			t:     time.Unix(90, 0),
			expected: promql.Matrix{
				promql.Series{
					Floats: []promql.FPoint{{F: 2, T: 60000}},
					Metric: labels.FromStrings("__name__", "another_metric", "id", "1"),
				},
				promql.Series{
					Floats: []promql.FPoint{{F: 2, T: 60000}},
					Metric: labels.FromStrings("__name__", "another_metric", "id", "2"),
				},
				promql.Series{
					Floats: []promql.FPoint{{F: 8, T: 60000}},
					Metric: labels.FromStrings("__name__", "another_metric", "id", "3"),
				},
			},
		},

		{
			query: "another_metric[1m] anchored",
			t:     time.Unix(90, 0),
			expected: promql.Matrix{
				promql.Series{
					Floats: []promql.FPoint{{F: 1, T: 30000}, {F: 2, T: 60000}, {F: 2, T: 90000}},
					Metric: labels.FromStrings("__name__", "another_metric", "id", "1"),
				},
				promql.Series{
					Floats: []promql.FPoint{{F: 3, T: 30000}, {F: 2, T: 60000}, {F: 2, T: 90000}},
					Metric: labels.FromStrings("__name__", "another_metric", "id", "2"),
				},
				promql.Series{
					Floats: []promql.FPoint{{F: 5, T: 30000}, {F: 8, T: 60000}, {F: 8, T: 90000}},
					Metric: labels.FromStrings("__name__", "another_metric", "id", "3"),
				},
			},
		},

		{
			query: "another_metric[1m] anchored",
			t:     time.Unix(60*3, 0),
			expected: promql.Matrix{
				promql.Series{
					Floats: []promql.FPoint{{F: 3, T: 120000}, {F: 4, T: 180000}},
					Metric: labels.FromStrings("__name__", "another_metric", "id", "1"),
				},
				promql.Series{
					Floats: []promql.FPoint{{F: 4, T: 120000}, {F: 6, T: 180000}},
					Metric: labels.FromStrings("__name__", "another_metric", "id", "2"),
				},
				promql.Series{
					Floats: []promql.FPoint{{F: 11, T: 120000}, {F: 3, T: 180000}},
					Metric: labels.FromStrings("__name__", "another_metric", "id", "3"),
				},
			},
		},

		{
			query: "another_metric[1m] anchored",
			t:     time.Unix(60*3-1, 0),
			expected: promql.Matrix{
				promql.Series{
					Floats: []promql.FPoint{{F: 2, T: 119000}, {F: 3, T: 120000}, {F: 3, T: 179000}},
					Metric: labels.FromStrings("__name__", "another_metric", "id", "1"),
				},
				promql.Series{
					Floats: []promql.FPoint{{F: 2, T: 119000}, {F: 4, T: 120000}, {F: 4, T: 179000}},
					Metric: labels.FromStrings("__name__", "another_metric", "id", "2"),
				},
				promql.Series{
					Floats: []promql.FPoint{{F: 8, T: 119000}, {F: 11, T: 120000}, {F: 11, T: 179000}},
					Metric: labels.FromStrings("__name__", "another_metric", "id", "3"),
				},
			},
		},

		{
			query: "another_metric[1m] anchored",
			t:     time.Unix(60*3+1, 0),
			expected: promql.Matrix{
				promql.Series{
					Floats: []promql.FPoint{{F: 3, T: 121000}, {F: 4, T: 180000}, {F: 4, T: 181000}},
					Metric: labels.FromStrings("__name__", "another_metric", "id", "1"),
				},
				promql.Series{
					Floats: []promql.FPoint{{F: 4, T: 121000}, {F: 6, T: 180000}, {F: 6, T: 181000}},
					Metric: labels.FromStrings("__name__", "another_metric", "id", "2"),
				},
				promql.Series{
					Floats: []promql.FPoint{{F: 11, T: 121000}, {F: 3, T: 180000}, {F: 3, T: 181000}},
					Metric: labels.FromStrings("__name__", "another_metric", "id", "3"),
				},
			},
		},

		{
			query: "another_metric[1m] smoothed",
			t:     time.Unix(90, 0),
			expected: promql.Matrix{
				promql.Series{
					Floats: []promql.FPoint{{F: 1.5, T: 30000}, {T: 60000, F: 2}, {T: 90000, F: 2.5}},
					Metric: labels.FromStrings("__name__", "another_metric", "id", "1"),
				},
				promql.Series{
					Floats: []promql.FPoint{{F: 2.5, T: 30000}, {T: 60000, F: 2}, {T: 90000, F: 3}},
					Metric: labels.FromStrings("__name__", "another_metric", "id", "2"),
				},
				promql.Series{
					Floats: []promql.FPoint{{F: 6.5, T: 30000}, {T: 60000, F: 8}, {T: 90000, F: 9.5}},
					Metric: labels.FromStrings("__name__", "another_metric", "id", "3"),
				},
			},
		},
	}

	for _, tc := range tc {
		t.Run(tc.query, func(t *testing.T) {

			opts := NewTestEngineOpts()
			planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
			require.NoError(t, err)
			engine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
			require.NoError(t, err)

			qry, err := engine.NewInstantQuery(context.Background(), storage, nil, tc.query, tc.t)
			require.NoError(t, err)
			res := qry.Exec(context.Background())
			require.NoError(t, res.Err)
			require.Equal(t, tc.expected, res.Value)
		})
	}
}

// TestExtendedRangeSelectorsIrregular has tests specific to the anchored and smoothed range modifiers.
// The results can have points which are not aligned to the step interval, and as such creating promql *.test scripts which inspect the raw range result is not possible.
// These tests also cover the anchored and smoothed errors. These errors are returned during the planning phase, rather than the execution phase so the *.test promql test harness does not handle this correctly.
func TestExtendedRangeSelectorsIrregular(t *testing.T) {
	storage := promqltest.LoadedStorage(t, `
	load 10s
		metric 1+1x10
		withreset 1+1x4 1+1x5
		notregular 0 5 100 2 8
		nans 1 2 3 NaN -NaN 4 5 6
	`)
	t.Cleanup(func() { storage.Close() })

	tc := []struct {
		query    string
		t        time.Time
		expected promql.Matrix
		error    string
	}{

		{
			query: "metric[10s] smoothed",
			t:     time.Unix(10, 0),
			expected: promql.Matrix{
				promql.Series{
					Floats: []promql.FPoint{{F: 1, T: 0}, {F: 2, T: 10000}},
					Metric: labels.FromStrings("__name__", "metric"),
				},
			},
		},
		{
			query: "metric[10s] smoothed",
			t:     time.Unix(15, 0),
			expected: promql.Matrix{
				promql.Series{
					Floats: []promql.FPoint{{F: 1.5, T: 5000}, {F: 2, T: 10000}, {F: 2.5, T: 15000}},
					Metric: labels.FromStrings("__name__", "metric"),
				},
			},
		},
		{
			query: "metric[10s] smoothed",
			t:     time.Unix(5, 0),
			expected: promql.Matrix{
				promql.Series{
					Floats: []promql.FPoint{{F: 1, T: -5000}, {F: 1, T: 0}, {F: 1.5, T: 5000}},
					Metric: labels.FromStrings("__name__", "metric"),
				},
			},
		},
		{
			query: "metric[10s] smoothed",
			t:     time.Unix(105, 0),
			expected: promql.Matrix{
				promql.Series{
					Floats: []promql.FPoint{{F: 10.5, T: 95000}, {F: 11, T: 100000}, {F: 11, T: 105000}},
					Metric: labels.FromStrings("__name__", "metric"),
				},
			},
		},
		{
			query: "withreset[10s] smoothed",
			t:     time.Unix(45, 0),
			expected: promql.Matrix{
				promql.Series{
					Floats: []promql.FPoint{{F: 4.5, T: 35000}, {F: 5, T: 40000}, {F: 3, T: 45000}},
					Metric: labels.FromStrings("__name__", "withreset"),
				},
			},
		},
		{
			query: "metric[10s] anchored",
			t:     time.Unix(10, 0),
			expected: promql.Matrix{
				promql.Series{
					Floats: []promql.FPoint{{F: 1, T: 0}, {F: 2, T: 10000}},
					Metric: labels.FromStrings("__name__", "metric"),
				},
			},
		},
		{
			query: "metric[10s] anchored",
			t:     time.Unix(15, 0),
			expected: promql.Matrix{
				promql.Series{
					Floats: []promql.FPoint{{F: 1, T: 5000}, {F: 2, T: 10000}, {F: 2, T: 15000}},
					Metric: labels.FromStrings("__name__", "metric"),
				},
			},
		},
		{
			query: "metric[10s] anchored",
			t:     time.Unix(5, 0),
			expected: promql.Matrix{
				promql.Series{
					Floats: []promql.FPoint{{F: 1, T: -5000}, {F: 1, T: 0}, {F: 1, T: 5000}},
					Metric: labels.FromStrings("__name__", "metric"),
				},
			},
		},
		{
			query: "metric[10s] anchored",
			t:     time.Unix(105, 0),
			expected: promql.Matrix{
				promql.Series{
					Floats: []promql.FPoint{{F: 10, T: 95000}, {F: 11, T: 100000}, {F: 11, T: 105000}},
					Metric: labels.FromStrings("__name__", "metric"),
				},
			},
		},
		{
			query: "withreset[10s] anchored",
			t:     time.Unix(45, 0),
			expected: promql.Matrix{
				promql.Series{
					Floats: []promql.FPoint{{F: 4, T: 35000}, {F: 5, T: 40000}, {F: 5, T: 45000}},
					Metric: labels.FromStrings("__name__", "withreset"),
				},
			},
		},
		{
			query: "notregular[20s] smoothed",
			t:     time.Unix(30, 0),
			expected: promql.Matrix{
				promql.Series{
					Floats: []promql.FPoint{{F: 5, T: 10000}, {F: 100, T: 20000}, {F: 2, T: 30000}},
					Metric: labels.FromStrings("__name__", "notregular"),
				},
			},
		},
		{
			query: "notregular[20s] anchored",
			t:     time.Unix(30, 0),
			expected: promql.Matrix{
				promql.Series{
					Floats: []promql.FPoint{{F: 5, T: 10000}, {F: 100, T: 20000}, {F: 2, T: 30000}},
					Metric: labels.FromStrings("__name__", "notregular"),
				},
			},
		},

		{
			query: "deriv(notregular[20s] anchored)",
			t:     time.Unix(30, 0),
			error: "anchored modifier can only be used with: changes, delta, increase, rate, resets - not with deriv",
		},

		{
			query: "max_over_time(notregular[20s] anchored)",
			t:     time.Unix(30, 0),
			error: "anchored modifier can only be used with: changes, delta, increase, rate, resets - not with max_over_time",
		},

		{
			query: "predict_linear(notregular[20s] anchored, 4)",
			t:     time.Unix(30, 0),
			error: "anchored modifier can only be used with: changes, delta, increase, rate, resets - not with predict_linear",
		},

		{
			query: "deriv(notregular[20s] smoothed)",
			t:     time.Unix(30, 0),
			error: "smoothed modifier can only be used with: delta, increase, rate - not with deriv",
		},

		{
			query: "changes(notregular[20s] smoothed)",
			t:     time.Unix(30, 0),
			error: "smoothed modifier can only be used with: delta, increase, rate - not with changes",
		},

		{
			query: "resets(notregular[20s] smoothed)",
			t:     time.Unix(30, 0),
			error: "smoothed modifier can only be used with: delta, increase, rate - not with resets",
		},
	}

	for _, tc := range tc {
		t.Run(tc.query, func(t *testing.T) {

			opts := NewTestEngineOpts()
			planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
			require.NoError(t, err)

			engine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
			require.NoError(t, err)

			qry, err := engine.NewInstantQuery(context.Background(), storage, nil, tc.query, tc.t)
			if len(tc.error) > 0 {
				require.ErrorContains(t, err, tc.error)
			} else {
				require.NoError(t, err)
			}
			if err != nil {
				return
			}
			res := qry.Exec(context.Background())
			require.NoError(t, res.Err)
			require.Equal(t, tc.expected, res.Value)

		})
	}
}

func TestStepInvariantMetrics(t *testing.T) {
	storage := promqltest.LoadedStorage(t, `
	load 1m
    	metric 0 1 2 3 4 5
		nodata _ _ _ _ _ _
		histogram {{schema:3 sum:4 count:4 buckets:[1 2 1]}}+{{schema:5 sum:2 count:1 buckets:[1] offset:1}}x6
	`)
	t.Cleanup(func() { storage.Close() })

	tc := []struct {
		query    string
		start    time.Time
		end      time.Time
		interval time.Duration

		expectedNodes      int
		expectedStepsSaved int
	}{
		{
			query:              "metric @ 20",
			start:              time.Unix(0, 0),
			end:                time.Unix(0, 0).Add(time.Second * 50),
			interval:           time.Second * 10,
			expectedNodes:      1,
			expectedStepsSaved: 5,
		},
		{
			query:         "metric @ 20",
			start:         time.Unix(0, 0).Add(time.Second * 50),
			end:           time.Unix(0, 0).Add(time.Second * 50),
			interval:      time.Second * 10,
			expectedNodes: 0, // step invariant operation has been removed
		},
		{
			query:              "nodata @ 20",
			start:              time.Unix(0, 0),
			end:                time.Unix(0, 0).Add(time.Second * 50),
			interval:           time.Second * 10,
			expectedNodes:      1,
			expectedStepsSaved: 5,
		},
		{
			query:              "metric @ 20 + metric @ 30",
			start:              time.Unix(0, 0),
			end:                time.Unix(0, 0).Add(time.Second * 50),
			interval:           time.Second * 10,
			expectedNodes:      1, // the left and right are all wrapped into a single step invariant
			expectedStepsSaved: 5,
		},
		{
			query:              "metric @ 20 * metric + metric @ 30",
			start:              time.Unix(0, 0),
			end:                time.Unix(0, 0).Add(time.Second * 50),
			interval:           time.Second * 10,
			expectedNodes:      2,
			expectedStepsSaved: 10,
		},
		{
			query:              "abs(metric @ 20 * metric + metric @ 30)",
			start:              time.Unix(0, 0),
			end:                time.Unix(0, 0).Add(time.Second * 50),
			interval:           time.Second * 10,
			expectedNodes:      2,
			expectedStepsSaved: 10,
		},
		{
			query:              "scalar(metric @ 20)",
			start:              time.Unix(0, 0),
			end:                time.Unix(0, 0).Add(time.Second * 50),
			interval:           time.Second * 10,
			expectedNodes:      1,
			expectedStepsSaved: 5,
		},
		{
			query:              "histogram @ 20",
			start:              time.Unix(0, 0),
			end:                time.Unix(0, 0).Add(time.Second * 50),
			interval:           time.Second * 10,
			expectedNodes:      1,
			expectedStepsSaved: 5,
		},
		{
			query:              "(metric @ 20 or metric) or (histogram @ 20 or histogram) or (vector(scalar(metric @ 30)) or metric)",
			start:              time.Unix(0, 0),
			end:                time.Unix(0, 0).Add(time.Second * 50),
			interval:           time.Second * 10,
			expectedNodes:      3,
			expectedStepsSaved: 15,
		},
		{
			query:              "timestamp(metric @ 20)",
			start:              time.Unix(0, 0),
			end:                time.Unix(0, 0).Add(time.Second * 50),
			interval:           time.Second * 10,
			expectedNodes:      1,
			expectedStepsSaved: 5,
		},
		{
			// the step invariant is relative to a sub-query. The sub-query step count is used
			query:              "avg_over_time((vector(1))[10m:1m])",
			start:              time.Unix(0, 0),
			end:                time.Unix(0, 0).Add(time.Second * 50),
			interval:           time.Second * 10,
			expectedNodes:      1,
			expectedStepsSaved: 9,
		},
		{
			// the step invariant is relative to a sub-query. The sub-query step count is used
			query:              "avg_over_time((rate(http_requests_total[5m]) + metric @ 10)[10m:1m])",
			start:              time.Unix(0, 0),
			end:                time.Unix(0, 0).Add(time.Second * 50),
			interval:           time.Second * 10,
			expectedNodes:      1,
			expectedStepsSaved: 9,
		},
	}

	for _, tc := range tc {
		t.Run(tc.query, func(t *testing.T) {

			registry := prometheus.NewRegistry()

			opts := NewTestEngineOpts()
			opts.CommonOpts.Reg = registry

			planner, err := NewQueryPlannerWithoutOptimizationPasses(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
			require.NoError(t, err)

			engine, err := NewEngine(opts, stats.NewQueryMetrics(nil), planner)
			require.NoError(t, err)

			qry, err := engine.NewRangeQuery(context.Background(), storage, nil, tc.query, tc.start, tc.end, tc.interval)
			require.NoError(t, err)
			res := qry.Exec(context.Background())
			require.NoError(t, res.Err)

			require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(fmt.Sprintf(`
							# HELP cortex_mimir_query_engine_step_invariant_nodes_total Total number of step invariant nodes.
							# TYPE cortex_mimir_query_engine_step_invariant_nodes_total counter
							cortex_mimir_query_engine_step_invariant_nodes_total %d
						`, tc.expectedNodes)), "cortex_mimir_query_engine_step_invariant_nodes_total"))

			require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(fmt.Sprintf(`
							# HELP cortex_mimir_query_engine_step_invariant_steps_saved_total Total number of steps which were saved from being evaluated due to step invariant handling.
							# TYPE cortex_mimir_query_engine_step_invariant_steps_saved_total counter
							cortex_mimir_query_engine_step_invariant_steps_saved_total %d
						`, tc.expectedStepsSaved)), "cortex_mimir_query_engine_step_invariant_steps_saved_total"))

			// Extra assertions to ensure we do not introduce bad test data
			if tc.expectedNodes == 0 {
				// If there are no step invariant nodes, then we do not expect to have any steps saved
				require.Equal(t, 0, tc.expectedStepsSaved)
			} else {
				// If there are step invariant nodes, then we expect to have saved steps
				require.Greater(t, tc.expectedStepsSaved, 0)
			}
		})
	}
}

type dummyMaterializer struct{}

func (d dummyMaterializer) Materialize(n planning.Node, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters, overrideTimeParams planning.RangeParams) (planning.OperatorFactory, error) {
	panic("not implemented")
}
