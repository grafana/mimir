// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"context"
	"errors"
	"fmt"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/util/teststorage"
	"io"
	"io/fs"
	"math"
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
	"github.com/prometheus/prometheus/model/histogram"
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
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/globalerror"
)

func TestUnsupportedPromQLFeatures(t *testing.T) {
	featureToggles := EnableAllFeatures

	// The goal of this is not to list every conceivable expression that is unsupported, but to cover all the
	// different cases and make sure we produce a reasonable error message when these cases are encountered.
	unsupportedExpressions := map[string]string{
		"1 + 2":                      "binary expression between two scalars",
		"1 + metric{}":               "binary expression between scalar and instant vector",
		"metric{} + 1":               "binary expression between scalar and instant vector",
		"metric{} < other_metric{}":  "binary expression with '<'",
		"metric{} or other_metric{}": "binary expression with many-to-many matching",
		"metric{} + on() group_left() other_metric{}":  "binary expression with many-to-one matching",
		"metric{} + on() group_right() other_metric{}": "binary expression with one-to-many matching",
		"avg(metric{})":                         "aggregation operation with 'avg'",
		"topk(5, metric{})":                     "'topk' aggregation with parameter",
		`count_values("foo", metric{})`:         "'count_values' aggregation with parameter",
		"rate(metric{}[5m:1m])":                 "PromQL expression type *parser.SubqueryExpr for range vectors",
		"quantile_over_time(0.4, metric{}[5m])": "'quantile_over_time' function",
		"-sum(metric{})":                        "PromQL expression type *parser.UnaryExpr for instant vectors",
	}

	for expression, expectedError := range unsupportedExpressions {
		t.Run(expression, func(t *testing.T) {
			requireRangeQueryIsUnsupported(t, featureToggles, expression, expectedError)
			requireInstantQueryIsUnsupported(t, featureToggles, expression, expectedError)
		})
	}

	// These expressions are also unsupported, but are only valid as instant queries.
	unsupportedInstantQueryExpressions := map[string]string{
		"'a'":             "string value as top-level expression",
		"metric{}[5m:1m]": "PromQL expression type *parser.SubqueryExpr for range vectors",
	}

	for expression, expectedError := range unsupportedInstantQueryExpressions {
		t.Run(expression, func(t *testing.T) {
			requireInstantQueryIsUnsupported(t, featureToggles, expression, expectedError)
		})
	}
}

func TestUnsupportedPromQLFeaturesWithFeatureToggles(t *testing.T) {
	t.Run("aggregation operations", func(t *testing.T) {
		featureToggles := EnableAllFeatures
		featureToggles.EnableAggregationOperations = false

		requireRangeQueryIsUnsupported(t, featureToggles, "sum by (label) (metric)", "aggregation operations")
		requireInstantQueryIsUnsupported(t, featureToggles, "sum by (label) (metric)", "aggregation operations")
	})

	t.Run("binary expressions", func(t *testing.T) {
		featureToggles := EnableAllFeatures
		featureToggles.EnableBinaryOperations = false

		requireRangeQueryIsUnsupported(t, featureToggles, "metric{} + other_metric{}", "binary expressions")
		requireInstantQueryIsUnsupported(t, featureToggles, "metric{} + other_metric{}", "binary expressions")
	})

	t.Run("..._over_time functions", func(t *testing.T) {
		featureToggles := EnableAllFeatures
		featureToggles.EnableOverTimeFunctions = false

		requireRangeQueryIsUnsupported(t, featureToggles, "count_over_time(metric[1m])", "'count_over_time' function")
		requireInstantQueryIsUnsupported(t, featureToggles, "count_over_time(metric[1m])", "'count_over_time' function")
	})

	t.Run("offset modifier", func(t *testing.T) {
		featureToggles := EnableAllFeatures
		featureToggles.EnableOffsetModifier = false

		requireRangeQueryIsUnsupported(t, featureToggles, "metric offset 1m", "instant vector selector with 'offset'")
		requireInstantQueryIsUnsupported(t, featureToggles, "metric offset 1m", "instant vector selector with 'offset'")
		requireInstantQueryIsUnsupported(t, featureToggles, "metric[2m] offset 1m", "range vector selector with 'offset'")

		requireRangeQueryIsUnsupported(t, featureToggles, "rate(metric[2m] offset 1m)", "range vector selector with 'offset'")
		requireInstantQueryIsUnsupported(t, featureToggles, "rate(metric[2m] offset 1m)", "range vector selector with 'offset'")
	})

	t.Run("scalars", func(t *testing.T) {
		featureToggles := EnableAllFeatures
		featureToggles.EnableScalars = false

		requireRangeQueryIsUnsupported(t, featureToggles, "2", "scalar values")
		requireInstantQueryIsUnsupported(t, featureToggles, "2", "scalar values")
	})
}

func requireRangeQueryIsUnsupported(t *testing.T, featureToggles FeatureToggles, expression string, expectedError string) {
	opts := NewTestEngineOpts()
	opts.FeatureToggles = featureToggles
	engine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
	require.NoError(t, err)

	qry, err := engine.NewRangeQuery(context.Background(), nil, nil, expression, time.Now().Add(-time.Hour), time.Now(), time.Minute)
	require.Error(t, err)
	require.ErrorIs(t, err, compat.NotSupportedError{})
	require.EqualError(t, err, "not supported by streaming engine: "+expectedError)
	require.Nil(t, qry)
}

func requireInstantQueryIsUnsupported(t *testing.T, featureToggles FeatureToggles, expression string, expectedError string) {
	opts := NewTestEngineOpts()
	opts.FeatureToggles = featureToggles
	engine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
	require.NoError(t, err)

	qry, err := engine.NewInstantQuery(context.Background(), nil, nil, expression, time.Now())
	require.Error(t, err)
	require.ErrorIs(t, err, compat.NotSupportedError{})
	require.EqualError(t, err, "not supported by streaming engine: "+expectedError)
	require.Nil(t, qry)
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

	prometheusEngine := promql.NewEngine(opts.CommonOpts)

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

			t.Run("Mimir's engine", func(t *testing.T) {
				promqltest.RunTest(t, testScript, mimirEngine)
			})

			// Run the tests against Prometheus' engine to ensure our test cases are valid.
			t.Run("Prometheus' engine", func(t *testing.T) {
				if strings.HasPrefix(testFile, "ours-only") {
					t.Skip("disabled for Prometheus' engine due to bug in Prometheus' engine")
				}

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

	prometheusEngine := promql.NewEngine(opts.CommonOpts)

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
		"histogram: matches series with points in range": {
			expr: "incr_histogram[1m]",
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
		"histogram: no samples in range": {
			expr: "incr_histogram[1m]",
			ts:   baseT.Add(20 * time.Minute),
			expected: &promql.Result{
				Value: promql.Matrix{},
			},
		},
		"histogram: does not return points outside range if last selected point does not align to end of range": {
			expr: "histogram_with_gaps[1m]",
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
		"histogram: metric with stale marker": {
			expr: "histogram_with_stale_marker[3m]",
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
			expr: "mixed_metric_float_first[2m]",
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
			expr: "some_metric[1m] offset 1m",
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
			expr: "some_metric[1m] offset -1m",
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
			expr: "some_metric[1m] @ 2m",
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
		"selector with @ modifier and offset": {
			expr: "some_metric[1m] @ 3m offset 1m",
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
	opts.CommonOpts.Timeout = 20 * time.Millisecond
	engine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
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
	return cancellationQuerier{onQueried: w.onQueried}, nil
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

			// Each series has five samples, which will be rounded up to 8 (the nearest power of two) by the bucketed pool, and we have five series.
			rangeQueryExpectedPeak: 5 * 8 * types.FPointSize,
			rangeQueryLimit:        0,

			// At peak, we'll hold all the output samples plus one series, which has one sample.
			// The output contains five samples, which will be rounded up to 8 (the nearest power of two).
			instantQueryExpectedPeak: types.FPointSize + 8*types.VectorSampleSize,
			instantQueryLimit:        0,
		},
		"limit enabled, but query does not exceed limit": {
			expr:          "some_metric",
			shouldSucceed: true,

			// Each series has five samples, which will be rounded up to 8 (the nearest power of two) by the bucketed pool, and we have five series.
			rangeQueryExpectedPeak: 5 * 8 * types.FPointSize,
			rangeQueryLimit:        1000,

			// At peak, we'll hold all the output samples plus one series, which has one sample.
			// The output contains five samples, which will be rounded up to 8 (the nearest power of two).
			instantQueryExpectedPeak: types.FPointSize + 8*types.VectorSampleSize,
			instantQueryLimit:        1000,
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

			// Each series has five samples, which will be rounded up to 8 (the nearest power of two) by the bucketed pool.
			// At peak we'll hold in memory:
			//  - the running total for the sum() (two floats (due to kahan) and a bool at each step, with the number of steps rounded to the nearest power of 2),
			//  - and the next series from the selector.
			rangeQueryExpectedPeak: 8*(2*types.Float64Size+types.BoolSize) + 8*types.FPointSize,
			rangeQueryLimit:        8*(2*types.Float64Size+types.BoolSize) + 8*types.FPointSize,

			// Each series has one sample, which is already a power of two.
			// At peak we'll hold in memory:
			//  - the running total for the sum() (two floats and a bool),
			//  - the next series from the selector,
			//  - and the output sample.
			instantQueryExpectedPeak: 2*types.Float64Size + types.BoolSize + types.FPointSize + types.VectorSampleSize,
			instantQueryLimit:        2*types.Float64Size + types.BoolSize + types.FPointSize + types.VectorSampleSize,
		},
		"limit enabled, query selects more samples than limit but should not load all of them into memory at once, and peak consumption is over limit": {
			expr:          "sum(some_metric)",
			shouldSucceed: false,

			// Each series has five samples, which will be rounded up to 8 (the nearest power of two) by the bucketed pool.
			// At peak we'll hold in memory:
			// - the running total for the sum() (two floats (due to kahan) and a bool at each step, with the number of steps rounded to the nearest power of 2),
			// - and the next series from the selector.
			// The last thing to be allocated is the bool slice for the running total, so that won't contribute to the peak before the query is aborted.
			rangeQueryExpectedPeak: 8*2*types.Float64Size + 8*types.FPointSize,
			rangeQueryLimit:        8*(2*types.Float64Size+types.BoolSize) + 8*types.FPointSize - 1,

			// Each series has one sample, which is already a power of two.
			// At peak we'll hold in memory:
			// - the running total for the sum() (two floats and a bool),
			// - the next series from the selector,
			// - and the output sample.
			// The last thing to be allocated is the bool slice for the running total, so that won't contribute to the peak before the query is aborted.
			instantQueryExpectedPeak: 2*types.Float64Size + types.FPointSize + types.VectorSampleSize,
			instantQueryLimit:        2*types.Float64Size + types.BoolSize + types.FPointSize + types.VectorSampleSize - 1,
		},
		"histogram: limit enabled, but query does not exceed limit": {
			expr:          "sum(some_histogram)",
			shouldSucceed: true,

			// Each series has five samples, which will be rounded up to 8 (the nearest power of two) by the bucketed pool.
			// At peak we'll hold in memory:
			//  - the running total for the sum() (a histogram pointer at each step, with the number of steps rounded to the nearest power of 2),
			//  - and the next series from the selector.
			rangeQueryExpectedPeak: 8*types.HistogramPointerSize + 8*types.HPointSize,
			rangeQueryLimit:        8*types.HistogramPointerSize + 8*types.HPointSize,
			// Each series has one sample, which is already a power of two.
			// At peak we'll hold in memory:
			//  - the running total for the sum() (a histogram pointer),
			//  - the next series from the selector,
			//  - and the output sample.
			instantQueryExpectedPeak: types.HistogramPointerSize + types.HPointSize + types.VectorSampleSize,
			instantQueryLimit:        types.HistogramPointerSize + types.HPointSize + types.VectorSampleSize,
		},
		"histogram: limit enabled, and query exceeds limit": {
			expr:          "sum(some_histogram)",
			shouldSucceed: false,

			// Each series has five samples, which will be rounded up to 8 (the nearest power of two) by the bucketed pool.
			// At peak we'll hold in memory:
			//  - the running total for the sum() (a histogram pointer at each step, with the number of steps rounded to the nearest power of 2),
			//  - and the next series from the selector.
			// The last thing to be allocated is the HistogramPointerSize slice for the running total, so that won't contribute to the peak before the query is aborted.
			rangeQueryExpectedPeak: 8 * types.HPointSize,
			rangeQueryLimit:        8*types.HistogramPointerSize + 8*types.HPointSize - 1,
			// Each series has one sample, which is already a power of two.
			// At peak we'll hold in memory:
			//  - the running total for the sum() (a histogram pointer),
			//  - the next series from the selector,
			//  - and the output sample.
			// The last thing to be allocated is the HistogramPointerSize slice for the running total, so that won't contribute to the peak before the query is aborted.
			instantQueryExpectedPeak: types.HPointSize + types.VectorSampleSize,
			instantQueryLimit:        types.HistogramPointerSize + types.HPointSize + types.VectorSampleSize - 1,
		},
	}

	createEngine := func(t *testing.T, limit uint64) (promql.QueryEngine, *prometheus.Registry, opentracing.Span, context.Context) {
		reg := prometheus.NewPedanticRegistry()
		opts := NewTestEngineOpts()
		opts.CommonOpts.Reg = reg

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
	opts.CommonOpts.Reg = reg

	limit := 3 * 8 * types.FPointSize // Allow up to three series with five points (which will be rounded up to 8, the nearest power of 2)
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
			opts.CommonOpts.ActiveQueryTracker = tracker
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
	opts.CommonOpts.Timeout = 10 * time.Millisecond
	opts.CommonOpts.ActiveQueryTracker = tracker
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

func TestAnnotations(t *testing.T) {
	startT := timestamp.Time(0).Add(time.Minute)
	step := time.Minute
	endT := startT.Add(2 * step)

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
		metric{series="incompatible-custom-buckets"} {{schema:-53 sum:1 count:1 custom_values:[5 10] buckets:[1]}} {{schema:-53 sum:1 count:1 custom_values:[2 3] buckets:[1]}} {{schema:-53 sum:5 count:4 custom_values:[5 10] buckets:[1 2 1]}}
    `

	testCases := map[string]struct {
		data                               string
		expr                               string
		expectedWarningAnnotations         []string
		expectedInfoAnnotations            []string
		skipComparisonWithPrometheusReason string
	}{
		"sum() with float and native histogram at same step": {
			data:                       mixedFloatHistogramData,
			expr:                       "sum by (series) (metric)",
			expectedWarningAnnotations: []string{"PromQL warning: encountered a mix of histograms and floats for aggregation (1:18)"},
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

		"rate() over metric without counter suffix containing only floats": {
			data:                    mixedFloatHistogramData,
			expr:                    `rate(metric{type="float"}[1m])`,
			expectedInfoAnnotations: []string{`PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "metric" (1:6)`},
		},
		"rate() over metric without counter suffix containing only native histograms": {
			data: mixedFloatHistogramData,
			expr: `rate(metric{type="histogram"}[1m])`,
		},
		"rate() over metric ending in _total": {
			data: `some_metric_total 0+1x3`,
			expr: `rate(some_metric_total[1m])`,
		},
		"rate() over metric ending in _sum": {
			data: `some_metric_sum 0+1x3`,
			expr: `rate(some_metric_sum[1m])`,
		},
		"rate() over metric ending in _count": {
			data: `some_metric_count 0+1x3`,
			expr: `rate(some_metric_count[1m])`,
		},
		"rate() over metric ending in _bucket": {
			data: `some_metric_bucket 0+1x3`,
			expr: `rate(some_metric_bucket[1m])`,
		},
		"rate() over multiple metric names": {
			data: `
				not_a_counter{env="prod", series="1"}      0+1x3
				a_total{series="2"}                        1+1x3
				a_sum{series="3"}                          2+1x3
				a_count{series="4"}                        3+1x3
				a_bucket{series="5"}                       4+1x3
				not_a_counter{env="test", series="6"}      5+1x3
				also_not_a_counter{env="test", series="7"} 6+1x3
			`,
			expr: `rate({__name__!=""}[1m])`,
			expectedInfoAnnotations: []string{
				`PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "not_a_counter" (1:6)`,
				`PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "also_not_a_counter" (1:6)`,
			},
		},
		"rate() over series with both floats and histograms": {
			data:                       `some_metric_count 10 {{schema:0 sum:1 count:1 buckets:[1]}}`,
			expr:                       `rate(some_metric_count[1m])`,
			expectedWarningAnnotations: []string{`PromQL warning: encountered a mix of histograms and floats for metric name "some_metric_count" (1:6)`},
		},
		"rate() over series with first histogram that is not a counter": {
			data:                       `some_metric {{schema:0 sum:1 count:1 buckets:[1] counter_reset_hint:gauge}} {{schema:0 sum:2 count:2 buckets:[2]}}`,
			expr:                       `rate(some_metric[1m])`,
			expectedWarningAnnotations: []string{`PromQL warning: this native histogram metric is not a counter: "some_metric" (1:6)`},
		},
		"rate() over series with last histogram that is not a counter": {
			data:                       `some_metric {{schema:0 sum:1 count:1 buckets:[1]}} {{schema:0 sum:2 count:2 buckets:[2] counter_reset_hint:gauge}}`,
			expr:                       `rate(some_metric[1m])`,
			expectedWarningAnnotations: []string{`PromQL warning: this native histogram metric is not a counter: "some_metric" (1:6)`},
		},
		"rate() over series with a histogram that is not a counter that is neither the first or last in the range": {
			data:                       `some_metric {{schema:0 sum:1 count:1 buckets:[1]}} {{schema:0 sum:2 count:2 buckets:[2] counter_reset_hint:gauge}} {{schema:0 sum:3 count:3 buckets:[3]}}`,
			expr:                       `rate(some_metric[2m] @ 2m)`,
			expectedWarningAnnotations: []string{`PromQL warning: this native histogram metric is not a counter: "some_metric" (1:6)`},
		},

		"sum() over native histograms with both exponential and custom buckets": {
			data: nativeHistogramsWithCustomBucketsData,
			expr: `sum(metric{series=~"exponential-buckets|custom-buckets-1"})`,
			expectedWarningAnnotations: []string{
				`PromQL warning: vector contains a mix of histograms with exponential and custom buckets schemas for metric name "metric" (1:5)`,
			},
		},
		"sum() over native histograms with incompatible custom buckets": {
			data: nativeHistogramsWithCustomBucketsData,
			expr: `sum(metric{series=~"custom-buckets-(1|2)"})`,
			expectedWarningAnnotations: []string{
				`PromQL warning: vector contains histograms with incompatible custom buckets for metric name "metric" (1:5)`,
			},
		},

		"rate() over native histograms with both exponential and custom buckets": {
			data: nativeHistogramsWithCustomBucketsData,
			expr: `rate(metric{series="mixed-exponential-custom-buckets"}[1m])`,
			expectedWarningAnnotations: []string{
				`PromQL warning: vector contains a mix of histograms with exponential and custom buckets schemas for metric name "metric" (1:6)`,
			},
		},
		"rate() over native histograms with incompatible custom buckets": {
			data: nativeHistogramsWithCustomBucketsData,
			expr: `rate(metric{series="incompatible-custom-buckets"}[1m])`,
			expectedWarningAnnotations: []string{
				`PromQL warning: vector contains histograms with incompatible custom buckets for metric name "metric" (1:6)`,
			},
		},

		"sum_over_time() over series with both floats and histograms": {
			data:                       `some_metric 10 {{schema:0 sum:1 count:1 buckets:[1]}}`,
			expr:                       `sum_over_time(some_metric[1m])`,
			expectedWarningAnnotations: []string{`PromQL warning: encountered a mix of histograms and floats for metric name "some_metric" (1:15)`},
		},
		"sum_over_time() over native histograms with both exponential and custom buckets": {
			data: nativeHistogramsWithCustomBucketsData,
			expr: `sum_over_time(metric{series="mixed-exponential-custom-buckets"}[1m])`,
			expectedWarningAnnotations: []string{
				`PromQL warning: vector contains a mix of histograms with exponential and custom buckets schemas for metric name "metric" (1:15)`,
			},
		},
		"sum_over_time() over native histograms with incompatible custom buckets": {
			data: nativeHistogramsWithCustomBucketsData,
			expr: `sum_over_time(metric{series="incompatible-custom-buckets"}[1m])`,
			expectedWarningAnnotations: []string{
				`PromQL warning: vector contains histograms with incompatible custom buckets for metric name "metric" (1:15)`,
			},
		},

		"avg_over_time() over series with both floats and histograms": {
			data:                       `some_metric 10 {{schema:0 sum:1 count:1 buckets:[1]}}`,
			expr:                       `avg_over_time(some_metric[1m])`,
			expectedWarningAnnotations: []string{`PromQL warning: encountered a mix of histograms and floats for metric name "some_metric" (1:15)`},
		},
		"avg_over_time() over native histograms with both exponential and custom buckets": {
			data: nativeHistogramsWithCustomBucketsData,
			expr: `avg_over_time(metric{series="mixed-exponential-custom-buckets"}[1m])`,
			expectedWarningAnnotations: []string{
				`PromQL warning: vector contains a mix of histograms with exponential and custom buckets schemas for metric name "metric" (1:15)`,
			},
		},
		"avg_over_time() over native histograms with incompatible custom buckets": {
			data: nativeHistogramsWithCustomBucketsData,
			expr: `avg_over_time(metric{series="incompatible-custom-buckets"}[1m])`,
			expectedWarningAnnotations: []string{
				`PromQL warning: vector contains histograms with incompatible custom buckets for metric name "metric" (1:15)`,
			},
		},

		"binary operation between native histograms with exponential and custom buckets": {
			data: nativeHistogramsWithCustomBucketsData,
			expr: `metric{series="exponential-buckets"} + ignoring(series) metric{series="custom-buckets-1"}`,
			expectedWarningAnnotations: []string{
				`PromQL warning: vector contains a mix of histograms with exponential and custom buckets schemas for metric name "" (1:1)`,
			},
		},
		"binary operation between native histograms with incompatible custom buckets": {
			data: nativeHistogramsWithCustomBucketsData,
			expr: `metric{series="custom-buckets-1"} + ignoring(series) metric{series="custom-buckets-2"}`,
			expectedWarningAnnotations: []string{
				`PromQL warning: vector contains histograms with incompatible custom buckets for metric name "" (1:1)`,
			},
		},

		"multiple annotations from different operators": {
			data: `
				mixed_metric_count       10 {{schema:0 sum:1 count:1 buckets:[1]}}
				other_mixed_metric_count 10 {{schema:0 sum:1 count:1 buckets:[1]}}
				float_metric             10 20
				other_float_metric       10 20
			`,
			expr: "rate(mixed_metric_count[1m]) + rate(other_mixed_metric_count[1m]) + rate(float_metric[1m]) + rate(other_float_metric[1m])",
			expectedWarningAnnotations: []string{
				`PromQL warning: encountered a mix of histograms and floats for metric name "mixed_metric_count" (1:6)`,
				`PromQL warning: encountered a mix of histograms and floats for metric name "other_mixed_metric_count" (1:37)`,
			},
			expectedInfoAnnotations: []string{
				`PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "float_metric" (1:74)`,
				`PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "other_float_metric" (1:99)`,
			},
		},
	}

	opts := NewTestEngineOpts()
	mimirEngine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
	require.NoError(t, err)
	prometheusEngine := promql.NewEngine(opts.CommonOpts)

	const prometheusEngineName = "Prometheus' engine"
	engines := map[string]promql.QueryEngine{
		"Mimir's engine": mimirEngine,

		// Compare against Prometheus' engine to verify our test cases are valid.
		prometheusEngineName: prometheusEngine,
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			store := promqltest.LoadedStorage(t, "load 1m\n"+strings.TrimSpace(testCase.data))
			t.Cleanup(func() { _ = store.Close() })

			for engineName, engine := range engines {
				t.Run(engineName, func(t *testing.T) {
					if engineName == prometheusEngineName && testCase.skipComparisonWithPrometheusReason != "" {
						t.Skipf("Skipping comparison with Prometheus' engine: %v", testCase.skipComparisonWithPrometheusReason)
					}

					queryTypes := map[string]func() (promql.Query, error){
						"range": func() (promql.Query, error) {
							return engine.NewRangeQuery(context.Background(), store, nil, testCase.expr, startT, endT, step)
						},
						"instant": func() (promql.Query, error) {
							return engine.NewInstantQuery(context.Background(), store, nil, testCase.expr, startT)
						},
					}

					for queryType, generator := range queryTypes {
						t.Run(queryType, func(t *testing.T) {
							query, err := generator()
							require.NoError(t, err)
							t.Cleanup(query.Close)

							res := query.Exec(context.Background())
							require.NoError(t, res.Err)

							warnings, infos := res.Warnings.AsStrings(testCase.expr, 0, 0)
							require.ElementsMatch(t, testCase.expectedWarningAnnotations, warnings)
							require.ElementsMatch(t, testCase.expectedInfoAnnotations, infos)
						})
					}
				})
			}
		})
	}
}

func TestWeirdQueryResults(t *testing.T) {
	storage := teststorage.New(t)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	appender := storage.Appender(context.Background())
	lbls := labels.FromStrings("__name__", "cortex_querytee_backend_response_relative_duration_proportional", "pod", "query-tee-mimir-query-engine-69fd5b5dd7-k4bwd")
	// Chunk: 2024-09-02T17:06:27Z - 2024-09-02T17:06:27Z
	negativeBuckets := []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 2, 1, 1, 2, 2, 1, 1, 1, 2, 1, 2, 3, 1, 2, 2, 3, 2, 2, 2, 2, 2, 1, 5, 2, 4, 1, 5, 4, 6, 10, 7, 5, 7, 4, 6, 2, 3, 6, 8, 1, 6, 5, 5, 8, 3, 7, 5, 3, 4, 4, 6, 2, 5, 3, 5, 1, 2, 1, 1}
	positiveBuckets := []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 1, 2, 3, 1, 4, 3, 2, 2, 3, 1, 2, 2, 2, 1, 2, 1, 4, 5, 4, 4, 4, 2, 1, 4, 9, 13, 9, 5, 4, 6, 6, 8, 8, 6, 4, 3, 5, 7, 7, 3, 3, 8, 4, 6, 7, 4, 6, 11, 6, 3, 4, 6, 5, 6, 1, 3, 2, 3, 2, 2, 3, 1, 2, 1, 2, 1, 1, 1, 1, 1}
	_, err := appender.AppendHistogram(0, lbls, 1725296787597, nil, &histogram.FloatHistogram{Schema: 3, Count: 478, Sum: 122.71878732247606, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -99, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -90, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.UnknownCounterReset})
	require.NoError(t, err)
	// Chunk: 2024-09-02T17:06:42Z - 2024-09-02T17:08:57Z
	negativeBuckets = []float64{1, 0, 0, 0, 0, 0, 2, 2, 2, 2, 2, 0, 2, 5, 2, 2, 3, 5, 4, 3, 6, 7, 7, 10, 8, 10, 15, 10, 16, 23, 21, 15, 21, 17, 23, 33, 30, 32, 32, 36, 24, 13, 12, 9, 7, 4}
	positiveBuckets = []float64{2, 0, 0, 0, 0, 0, 0, 0, 3, 2, 1, 0, 1, 1, 1, 3, 3, 1, 7, 3, 7, 2, 8, 7, 7, 11, 12, 7, 9, 29, 20, 16, 24, 20, 15, 12, 26, 13, 22, 20, 16, 23, 15, 14, 13, 11, 12, 10, 5, 6, 4, 4, 3, 1, 0, 1, 0, 0, 1, 0, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1}
	_, err = appender.AppendHistogram(0, lbls, 1725296802597, nil, &histogram.FloatHistogram{Schema: 2, Count: 941, Sum: 190.41461297563856, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -49, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -45, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.UnknownCounterReset})
	require.NoError(t, err)
	negativeBuckets = []float64{1, 3, 0, 0, 0, 1, 1, 3, 3, 4, 4, 2, 2, 4, 7, 5, 3, 10, 11, 8, 8, 9, 12, 12, 16, 15, 12, 22, 22, 28, 37, 34, 38, 37, 32, 38, 47, 44, 48, 54, 54, 37, 19, 23, 17, 9, 9}
	positiveBuckets = []float64{2, 2, 0, 0, 0, 0, 1, 0, 4, 2, 1, 1, 2, 1, 1, 4, 5, 3, 10, 4, 10, 5, 14, 9, 9, 20, 20, 14, 23, 40, 31, 28, 38, 39, 28, 25, 43, 29, 31, 33, 24, 33, 25, 24, 22, 24, 21, 12, 13, 8, 5, 6, 5, 4, 0, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 0, 0, 0, 0, 0, 0, 0, 1, 1}
	_, err = appender.AppendHistogram(0, lbls, 1725296817597, nil, &histogram.FloatHistogram{Schema: 2, Count: 1577, Sum: 218.1869299406697, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -49, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -46, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	negativeBuckets = []float64{1, 3, 1, 1, 0, 2, 2, 3, 4, 4, 4, 3, 4, 6, 7, 7, 5, 12, 13, 13, 13, 14, 17, 20, 21, 21, 20, 30, 28, 37, 43, 42, 47, 51, 41, 47, 63, 53, 57, 72, 62, 45, 29, 30, 20, 10, 9}
	positiveBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 2, 1, 0, 0, 1, 1, 0, 4, 2, 2, 1, 2, 1, 2, 5, 6, 5, 10, 7, 13, 9, 21, 13, 14, 24, 29, 25, 36, 45, 40, 41, 60, 55, 45, 40, 52, 51, 50, 53, 44, 45, 44, 33, 36, 29, 28, 17, 20, 14, 10, 12, 6, 6, 0, 3, 0, 1, 1, 2, 1, 1, 1, 1, 1, 1, 2, 1, 1}
	_, err = appender.AppendHistogram(0, lbls, 1725296832597, nil, &histogram.FloatHistogram{Schema: 2, Count: 2169, Sum: 267.10259459533535, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -55, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -46, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	negativeBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 1, 2, 3, 1, 1, 2, 4, 3, 4, 6, 4, 7, 4, 6, 7, 9, 9, 9, 16, 20, 15, 18, 18, 24, 31, 26, 29, 29, 39, 40, 47, 59, 64, 59, 63, 56, 71, 85, 74, 74, 91, 80, 64, 44, 39, 30, 13, 9}
	positiveBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 4, 1, 0, 0, 1, 1, 0, 5, 3, 4, 2, 5, 3, 4, 6, 11, 6, 10, 9, 16, 14, 27, 18, 19, 27, 32, 31, 40, 50, 46, 56, 77, 75, 61, 51, 62, 62, 62, 64, 59, 55, 55, 45, 48, 34, 39, 28, 29, 19, 12, 17, 7, 7, 1, 3, 0, 1, 1, 2, 1, 1, 1, 1, 1, 1, 2, 1, 1}
	_, err = appender.AppendHistogram(0, lbls, 1725296847597, nil, &histogram.FloatHistogram{Schema: 2, Count: 2853, Sum: 273.7908680723445, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -55, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -62, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	negativeBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 1, 2, 3, 1, 1, 4, 4, 3, 4, 8, 7, 7, 5, 7, 8, 13, 11, 10, 17, 21, 19, 19, 23, 30, 35, 31, 31, 37, 48, 47, 58, 69, 74, 64, 79, 73, 86, 107, 85, 96, 111, 91, 77, 63, 46, 34, 13, 9}
	positiveBuckets = []float64{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 5, 1, 0, 0, 1, 1, 0, 7, 5, 5, 3, 6, 3, 4, 7, 11, 9, 11, 10, 21, 20, 28, 23, 26, 37, 39, 38, 49, 55, 52, 69, 84, 86, 75, 59, 70, 73, 72, 74, 70, 67, 67, 54, 56, 43, 43, 33, 31, 22, 14, 23, 8, 8, 2, 5, 0, 1, 1, 2, 1, 2, 1, 1, 1, 1, 2, 2, 5, 4, 5, 2, 2, 3, 1, 3, 1, 1}
	_, err = appender.AppendHistogram(0, lbls, 1725296862597, nil, &histogram.FloatHistogram{Schema: 2, Count: 3427, Sum: 2130.4312719050345, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -56, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -62, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	negativeBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 1, 1, 1, 3, 3, 1, 1, 4, 7, 3, 5, 8, 7, 7, 8, 7, 10, 13, 11, 10, 17, 23, 21, 22, 26, 32, 39, 33, 33, 40, 53, 57, 61, 71, 81, 74, 90, 86, 96, 123, 95, 109, 124, 108, 86, 68, 52, 36, 14, 10}
	positiveBuckets = []float64{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 5, 1, 0, 1, 1, 1, 1, 7, 5, 6, 3, 6, 3, 6, 7, 12, 11, 14, 12, 24, 26, 31, 27, 35, 41, 44, 47, 54, 67, 59, 79, 94, 100, 89, 74, 82, 92, 81, 91, 85, 82, 77, 69, 68, 51, 55, 41, 34, 30, 20, 26, 10, 10, 2, 6, 1, 2, 2, 3, 1, 2, 1, 1, 1, 1, 3, 3, 5, 6, 5, 2, 2, 3, 1, 3, 1, 1}
	_, err = appender.AppendHistogram(0, lbls, 1725296877597, nil, &histogram.FloatHistogram{Schema: 2, Count: 3953, Sum: 2311.146955931289, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -56, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -62, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	negativeBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 3, 3, 1, 1, 4, 7, 3, 5, 9, 7, 7, 8, 9, 11, 15, 11, 10, 18, 28, 25, 25, 30, 37, 41, 36, 40, 45, 58, 58, 65, 81, 92, 86, 99, 97, 111, 134, 117, 122, 137, 120, 99, 74, 56, 37, 15, 20}
	positiveBuckets = []float64{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 5, 1, 1, 1, 1, 1, 1, 7, 6, 7, 6, 6, 5, 6, 7, 17, 14, 18, 12, 29, 30, 32, 33, 42, 47, 54, 53, 66, 80, 73, 88, 106, 110, 103, 83, 97, 104, 95, 103, 94, 96, 87, 84, 79, 60, 69, 52, 38, 39, 23, 31, 11, 14, 5, 10, 3, 5, 2, 3, 2, 4, 1, 1, 2, 2, 3, 3, 5, 6, 5, 2, 2, 3, 1, 3, 1, 1}
	_, err = appender.AppendHistogram(0, lbls, 1725296892597, nil, &histogram.FloatHistogram{Schema: 2, Count: 4532, Sum: 2425.7586829988863, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -56, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -62, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	negativeBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 3, 3, 1, 2, 4, 7, 3, 5, 9, 8, 8, 8, 10, 11, 15, 11, 10, 22, 29, 29, 29, 32, 42, 47, 41, 50, 53, 68, 73, 74, 89, 109, 95, 119, 111, 128, 147, 135, 138, 166, 140, 109, 81, 59, 42, 16, 21}
	positiveBuckets = []float64{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 2, 2, 5, 1, 1, 1, 1, 1, 1, 7, 7, 7, 7, 7, 5, 7, 11, 20, 15, 20, 13, 31, 30, 38, 43, 48, 52, 59, 64, 74, 93, 87, 101, 128, 129, 130, 103, 124, 115, 114, 117, 114, 113, 105, 103, 95, 65, 82, 60, 44, 50, 28, 34, 15, 15, 7, 12, 5, 5, 3, 4, 2, 4, 1, 1, 2, 2, 3, 3, 5, 6, 5, 2, 2, 3, 1, 3, 1, 1}
	_, err = appender.AppendHistogram(0, lbls, 1725296907597, nil, &histogram.FloatHistogram{Schema: 2, Count: 5249, Sum: 2490.576350931289, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -56, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -62, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	negativeBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 1, 1, 1, 2, 4, 3, 2, 3, 5, 7, 3, 5, 9, 9, 11, 8, 12, 13, 16, 13, 11, 22, 29, 33, 33, 35, 48, 50, 50, 56, 60, 77, 83, 81, 99, 118, 104, 142, 125, 139, 163, 147, 154, 185, 165, 127, 109, 81, 59, 23, 22}
	positiveBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 2, 5, 1, 2, 1, 1, 1, 1, 8, 9, 8, 7, 8, 7, 9, 13, 23, 16, 24, 17, 37, 33, 45, 46, 53, 64, 74, 73, 96, 105, 100, 119, 142, 140, 147, 121, 142, 130, 128, 146, 132, 132, 124, 122, 119, 80, 96, 74, 59, 58, 36, 45, 23, 17, 11, 16, 7, 5, 3, 5, 2, 4, 1, 3, 2, 2, 3, 3, 5, 6, 5, 2, 2, 3, 1, 3, 1, 1}
	_, err = appender.AppendHistogram(0, lbls, 1725296922597, nil, &histogram.FloatHistogram{Schema: 2, Count: 6085, Sum: 2587.049265841263, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -64, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -62, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	negativeBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 3, 1, 1, 1, 2, 4, 3, 2, 4, 5, 7, 3, 6, 9, 9, 14, 8, 13, 13, 17, 15, 13, 24, 31, 37, 36, 40, 50, 60, 57, 66, 69, 86, 98, 100, 118, 134, 122, 162, 143, 161, 187, 183, 179, 208, 195, 146, 133, 95, 63, 27, 26}
	positiveBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 2, 6, 1, 2, 1, 1, 1, 2, 8, 9, 8, 7, 10, 7, 9, 14, 25, 19, 26, 20, 44, 37, 48, 50, 59, 70, 83, 77, 104, 115, 107, 126, 154, 157, 160, 133, 164, 145, 141, 160, 150, 152, 137, 136, 136, 97, 106, 92, 63, 63, 43, 49, 27, 21, 13, 16, 7, 5, 4, 5, 2, 4, 1, 3, 2, 2, 3, 3, 5, 6, 5, 2, 2, 3, 1, 3, 1, 1}
	_, err = appender.AppendHistogram(0, lbls, 1725296937597, nil, &histogram.FloatHistogram{Schema: 2, Count: 6880, Sum: 2608.9410727216978, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -64, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -62, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)

	lbls = labels.FromStrings("__name__", "cortex_querytee_backend_response_relative_duration_proportional", "pod", "query-tee-mimir-query-engine-56666f668b-khs9c")
	// Chunk: 2024-09-02T17:00:23Z - 2024-09-02T17:02:38Z
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 723, 1006, 1425, 2061, 2858, 4033, 5854, 8303, 11660, 16159, 23022, 32510, 45877, 64588, 91714, 130342, 181824, 243330, 326852, 416942, 558325, 761609, 915098, 1.090916e+06, 1.255702e+06, 1.376588e+06, 1.407865e+06, 1.295578e+06, 1.052619e+06, 749883, 479919, 301966, 186512, 110605, 64666, 37970, 23884, 17961, 15028, 13359, 11412, 9833, 9204, 7522, 5050, 3030, 2061, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 118, 189, 274, 398, 524, 717, 1041, 1437, 2038, 2890, 4067, 5691, 8113, 11287, 16051, 22615, 31186, 43427, 58534, 79112, 104114, 139433, 202152, 291673, 380517, 492296, 669271, 899685, 1.180923e+06, 1.5202e+06, 1.901284e+06, 2.229923e+06, 2.264912e+06, 1.869512e+06, 1.377307e+06, 1.027156e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296423476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0223527e+07, Sum: 1.290813676466059e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.UnknownCounterReset})
	require.NoError(t, err)
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 723, 1006, 1425, 2061, 2858, 4033, 5854, 8304, 11660, 16159, 23022, 32511, 45878, 64588, 91716, 130346, 181828, 243340, 326861, 416951, 558350, 761640, 915121, 1.09094e+06, 1.255743e+06, 1.376623e+06, 1.407905e+06, 1.295604e+06, 1.052658e+06, 749907, 479933, 301978, 186519, 110608, 64670, 37973, 23885, 17961, 15030, 13361, 11412, 9834, 9204, 7522, 5050, 3030, 2061, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 118, 189, 274, 398, 524, 717, 1041, 1437, 2038, 2890, 4067, 5691, 8115, 11287, 16053, 22615, 31186, 43429, 58536, 79119, 104116, 139435, 202156, 291680, 380528, 492309, 669285, 899705, 1.18095e+06, 1.520219e+06, 1.901313e+06, 2.229944e+06, 2.264935e+06, 1.869534e+06, 1.377318e+06, 1.027162e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296438476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0224171e+07, Sum: 1.2908339888560826e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 723, 1006, 1425, 2061, 2858, 4034, 5855, 8305, 11660, 16159, 23023, 32514, 45880, 64591, 91719, 130346, 181835, 243344, 326867, 416962, 558368, 761654, 915144, 1.090975e+06, 1.25577e+06, 1.376656e+06, 1.40794e+06, 1.295627e+06, 1.052679e+06, 749932, 479945, 301988, 186523, 110609, 64671, 37974, 23885, 17962, 15030, 13361, 11413, 9835, 9204, 7522, 5050, 3030, 2061, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 118, 189, 274, 398, 524, 717, 1041, 1437, 2038, 2890, 4067, 5691, 8115, 11288, 16054, 22617, 31187, 43429, 58537, 79120, 104118, 139438, 202159, 291689, 380533, 492318, 669303, 899721, 1.180968e+06, 1.520249e+06, 1.90135e+06, 2.229971e+06, 2.264964e+06, 1.869552e+06, 1.377331e+06, 1.02718e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296453476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0224762e+07, Sum: 1.2908445885599129e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 723, 1006, 1425, 2061, 2858, 4034, 5855, 8305, 11660, 16159, 23023, 32516, 45881, 64596, 91722, 130348, 181843, 243354, 326877, 416979, 558393, 761677, 915184, 1.091002e+06, 1.255804e+06, 1.376691e+06, 1.407991e+06, 1.295666e+06, 1.052711e+06, 749945, 479953, 301990, 186525, 110610, 64673, 37976, 23885, 17962, 15031, 13361, 11413, 9835, 9204, 7522, 5050, 3030, 2061, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 118, 189, 274, 398, 524, 717, 1041, 1437, 2038, 2890, 4067, 5692, 8115, 11290, 16054, 22617, 31187, 43429, 58538, 79122, 104121, 139444, 202169, 291699, 380539, 492334, 669316, 899738, 1.180988e+06, 1.520277e+06, 1.901369e+06, 2.230002e+06, 2.26499e+06, 1.869571e+06, 1.37734e+06, 1.027185e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296468476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0225401e+07, Sum: 1.2908508849223273e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 723, 1006, 1425, 2061, 2859, 4035, 5855, 8306, 11661, 16159, 23025, 32517, 45883, 64597, 91725, 130353, 181851, 243367, 326888, 416993, 558415, 761701, 915221, 1.091034e+06, 1.255844e+06, 1.376735e+06, 1.40804e+06, 1.295704e+06, 1.052757e+06, 749967, 479965, 302003, 186532, 110611, 64673, 37978, 23885, 17962, 15031, 13362, 11413, 9841, 9209, 7522, 5050, 3030, 2061, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 118, 189, 274, 398, 524, 717, 1041, 1437, 2039, 2890, 4067, 5692, 8116, 11290, 16054, 22618, 31187, 43431, 58541, 79128, 104125, 139448, 202174, 291707, 380548, 492345, 669330, 899761, 1.181016e+06, 1.520312e+06, 1.901412e+06, 2.230038e+06, 2.265016e+06, 1.869588e+06, 1.377353e+06, 1.027203e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296483476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0226174e+07, Sum: 1.2909097459355239e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 723, 1006, 1425, 2061, 2859, 4035, 5855, 8306, 11661, 16160, 23027, 32517, 45884, 64600, 91730, 130360, 181854, 243371, 326901, 417005, 558429, 761718, 915251, 1.091063e+06, 1.255876e+06, 1.376771e+06, 1.408065e+06, 1.295732e+06, 1.05278e+06, 749989, 479978, 302013, 186538, 110617, 64677, 37983, 23887, 17965, 15032, 13362, 11416, 9842, 9209, 7523, 5050, 3030, 2061, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 118, 189, 274, 398, 524, 717, 1041, 1437, 2039, 2890, 4067, 5692, 8116, 11290, 16054, 22619, 31189, 43432, 58547, 79130, 104128, 139452, 202178, 291712, 380561, 492358, 669341, 899781, 1.181035e+06, 1.520339e+06, 1.901448e+06, 2.230063e+06, 2.265043e+06, 1.869603e+06, 1.377363e+06, 1.027225e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296498476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0226802e+07, Sum: 1.2909429832927415e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 723, 1006, 1425, 2061, 2859, 4035, 5855, 8307, 11661, 16161, 23027, 32519, 45887, 64603, 91732, 130366, 181860, 243375, 326911, 417017, 558440, 761745, 915288, 1.091092e+06, 1.255903e+06, 1.376797e+06, 1.408099e+06, 1.295756e+06, 1.052796e+06, 749998, 479987, 302020, 186542, 110619, 64679, 37984, 23887, 17965, 15032, 13362, 11419, 9842, 9211, 7524, 5050, 3033, 2065, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 118, 189, 274, 398, 524, 717, 1041, 1437, 2040, 2890, 4067, 5692, 8116, 11291, 16058, 22620, 31192, 43434, 58552, 79135, 104133, 139457, 202190, 291720, 380578, 492377, 669365, 899808, 1.181053e+06, 1.520365e+06, 1.901479e+06, 2.230097e+06, 2.265069e+06, 1.869621e+06, 1.377373e+06, 1.027261e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296513476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0227468e+07, Sum: 1.291103418241903e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 723, 1006, 1425, 2061, 2859, 4035, 5855, 8307, 11661, 16161, 23027, 32519, 45889, 64605, 91733, 130370, 181867, 243388, 326928, 417029, 558464, 761770, 915315, 1.091137e+06, 1.255943e+06, 1.376845e+06, 1.408143e+06, 1.295795e+06, 1.052832e+06, 750029, 480010, 302027, 186547, 110628, 64682, 37987, 23889, 17966, 15033, 13362, 11420, 9847, 9213, 7525, 5050, 3033, 2065, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 118, 189, 274, 398, 524, 717, 1041, 1437, 2040, 2891, 4067, 5692, 8116, 11291, 16059, 22623, 31195, 43437, 58557, 79138, 104137, 139463, 202196, 291727, 380595, 492391, 669383, 899830, 1.181078e+06, 1.520396e+06, 1.901503e+06, 2.23013e+06, 2.265096e+06, 1.869638e+06, 1.377392e+06, 1.027272e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296528476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0228248e+07, Sum: 1.2911576937331988e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 723, 1006, 1425, 2061, 2859, 4036, 5855, 8307, 11662, 16161, 23028, 32521, 45889, 64610, 91738, 130374, 181875, 243394, 326933, 417041, 558482, 761794, 915345, 1.091168e+06, 1.255974e+06, 1.376879e+06, 1.408177e+06, 1.295831e+06, 1.052861e+06, 750053, 480017, 302031, 186550, 110630, 64683, 37988, 23890, 17968, 15034, 13362, 11422, 9849, 9218, 7528, 5050, 3033, 2065, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 118, 189, 274, 398, 524, 718, 1041, 1437, 2040, 2891, 4067, 5692, 8116, 11292, 16059, 22623, 31196, 43442, 58560, 79140, 104141, 139473, 202200, 291743, 380608, 492407, 669398, 899843, 1.181096e+06, 1.520424e+06, 1.901519e+06, 2.230147e+06, 2.265114e+06, 1.869644e+06, 1.377399e+06, 1.027291e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296543479, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0228856e+07, Sum: 1.29122564292348e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 723, 1006, 1425, 2061, 2859, 4036, 5855, 8308, 11662, 16163, 23030, 32525, 45892, 64611, 91742, 130380, 181887, 243403, 326937, 417055, 558499, 761809, 915373, 1.091195e+06, 1.256023e+06, 1.376915e+06, 1.408225e+06, 1.295866e+06, 1.052887e+06, 750069, 480032, 302033, 186555, 110631, 64684, 37990, 23892, 17969, 15034, 13362, 11422, 9851, 9218, 7528, 5050, 3033, 2065, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 118, 189, 274, 398, 524, 718, 1041, 1437, 2040, 2891, 4067, 5692, 8116, 11292, 16059, 22624, 31198, 43444, 58562, 79144, 104146, 139476, 202208, 291752, 380619, 492422, 669419, 899868, 1.181127e+06, 1.520458e+06, 1.901547e+06, 2.230167e+06, 2.265129e+06, 1.869653e+06, 1.377405e+06, 1.027301e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296558476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0229507e+07, Sum: 1.2912422386167616e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	// Chunk: 2024-09-02T17:02:53Z - 2024-09-02T17:05:08Z
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 723, 1006, 1425, 2061, 2859, 4036, 5856, 8308, 11663, 16163, 23031, 32526, 45892, 64611, 91743, 130383, 181895, 243409, 326953, 417066, 558520, 761839, 915396, 1.09123e+06, 1.25607e+06, 1.376958e+06, 1.408267e+06, 1.29591e+06, 1.052906e+06, 750085, 480037, 302038, 186555, 110631, 64685, 37990, 23892, 17969, 15034, 13362, 11422, 9851, 9218, 7528, 5050, 3033, 2065, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 118, 189, 274, 398, 524, 718, 1041, 1437, 2040, 2891, 4068, 5692, 8116, 11292, 16060, 22624, 31199, 43446, 58564, 79145, 104150, 139479, 202212, 291761, 380634, 492437, 669433, 899895, 1.181151e+06, 1.520478e+06, 1.901585e+06, 2.230201e+06, 2.265155e+06, 1.869664e+06, 1.377408e+06, 1.027302e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296573476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0230143e+07, Sum: 1.2912458946280938e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.UnknownCounterReset})
	require.NoError(t, err)
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 723, 1006, 1425, 2061, 2859, 4036, 5856, 8308, 11663, 16164, 23032, 32526, 45893, 64615, 91746, 130386, 181901, 243415, 326963, 417071, 558540, 761861, 915414, 1.091267e+06, 1.256119e+06, 1.377002e+06, 1.408322e+06, 1.295951e+06, 1.052944e+06, 750112, 480055, 302058, 186568, 110633, 64689, 37993, 23892, 17969, 15034, 13362, 11423, 9852, 9222, 7528, 5050, 3033, 2065, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 118, 189, 274, 398, 524, 718, 1041, 1437, 2040, 2891, 4069, 5692, 8116, 11293, 16061, 22626, 31199, 43449, 58566, 79149, 104153, 139482, 202220, 291764, 380639, 492444, 669443, 899913, 1.181171e+06, 1.520502e+06, 1.901602e+06, 2.230235e+06, 2.265177e+06, 1.86968e+06, 1.377419e+06, 1.027307e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296588476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.023082e+07, Sum: 1.2912871152175834e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 724, 1006, 1425, 2061, 2859, 4036, 5856, 8308, 11665, 16164, 23032, 32528, 45895, 64616, 91752, 130388, 181913, 243421, 326974, 417082, 558564, 761890, 915447, 1.091305e+06, 1.256179e+06, 1.377062e+06, 1.408379e+06, 1.296001e+06, 1.052976e+06, 750131, 480069, 302063, 186571, 110635, 64691, 37994, 23892, 17969, 15034, 13362, 11423, 9852, 9222, 7528, 5050, 3033, 2065, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 118, 189, 274, 398, 524, 718, 1041, 1437, 2040, 2891, 4070, 5692, 8116, 11293, 16062, 22626, 31199, 43450, 58566, 79151, 104154, 139487, 202233, 291772, 380648, 492457, 669467, 899933, 1.181206e+06, 1.520528e+06, 1.901644e+06, 2.230263e+06, 2.265194e+06, 1.869688e+06, 1.377423e+06, 1.027309e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296603476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0231565e+07, Sum: 1.291295102250157e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 724, 1006, 1425, 2061, 2859, 4036, 5856, 8308, 11666, 16167, 23034, 32531, 45896, 64618, 91755, 130396, 181923, 243433, 326987, 417099, 558577, 761914, 915471, 1.091336e+06, 1.256216e+06, 1.377105e+06, 1.408408e+06, 1.29604e+06, 1.053014e+06, 750158, 480083, 302074, 186574, 110643, 64692, 37998, 23894, 17970, 15039, 13362, 11423, 9852, 9222, 7530, 5050, 3033, 2065, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 118, 189, 274, 398, 524, 718, 1041, 1437, 2040, 2891, 4070, 5692, 8116, 11293, 16062, 22628, 31202, 43453, 58567, 79154, 104155, 139496, 202242, 291778, 380656, 492465, 669482, 899947, 1.181235e+06, 1.520553e+06, 1.901676e+06, 2.2303e+06, 2.265214e+06, 1.869712e+06, 1.377446e+06, 1.027328e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296618476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0232287e+07, Sum: 1.2913268554744432e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 724, 1006, 1427, 2061, 2859, 4036, 5856, 8308, 11668, 16168, 23034, 32533, 45898, 64619, 91763, 130399, 181931, 243445, 326999, 417118, 558598, 761950, 915506, 1.091369e+06, 1.256255e+06, 1.377142e+06, 1.40844e+06, 1.296069e+06, 1.053053e+06, 750174, 480089, 302080, 186577, 110644, 64692, 37998, 23896, 17970, 15039, 13362, 11423, 9854, 9223, 7531, 5050, 3033, 2065, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 118, 189, 274, 398, 524, 718, 1041, 1437, 2040, 2891, 4070, 5693, 8116, 11294, 16062, 22630, 31204, 43454, 58570, 79158, 104156, 139499, 202248, 291786, 380668, 492477, 669500, 899971, 1.181267e+06, 1.520579e+06, 1.901704e+06, 2.230326e+06, 2.265243e+06, 1.869728e+06, 1.377457e+06, 1.027336e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296633476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0232972e+07, Sum: 1.2913509350764511e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 724, 1006, 1427, 2061, 2859, 4037, 5856, 8309, 11668, 16170, 23035, 32533, 45898, 64620, 91766, 130404, 181938, 243452, 327006, 417132, 558614, 761974, 915535, 1.091408e+06, 1.256295e+06, 1.377181e+06, 1.408471e+06, 1.296102e+06, 1.053087e+06, 750189, 480098, 302091, 186581, 110651, 64695, 37998, 23897, 17972, 15043, 13362, 11423, 9854, 9225, 7532, 5050, 3033, 2065, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 118, 189, 274, 398, 524, 718, 1041, 1437, 2040, 2891, 4070, 5693, 8116, 11295, 16063, 22630, 31206, 43455, 58571, 79161, 104160, 139505, 202256, 291798, 380679, 492490, 669520, 899992, 1.181286e+06, 1.520611e+06, 1.901736e+06, 2.230362e+06, 2.265273e+06, 1.869743e+06, 1.377463e+06, 1.027348e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296648476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0233651e+07, Sum: 1.291383621020075e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 724, 1006, 1427, 2061, 2859, 4037, 5856, 8309, 11668, 16170, 23035, 32535, 45901, 64622, 91774, 130410, 181948, 243463, 327015, 417143, 558624, 761995, 915553, 1.09143e+06, 1.256326e+06, 1.377215e+06, 1.408502e+06, 1.29613e+06, 1.053106e+06, 750203, 480104, 302097, 186587, 110655, 64697, 38000, 23898, 17972, 15044, 13364, 11426, 9855, 9225, 7533, 5050, 3033, 2065, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 118, 189, 274, 398, 524, 718, 1041, 1437, 2041, 2891, 4070, 5693, 8116, 11295, 16063, 22630, 31208, 43456, 58571, 79165, 104165, 139513, 202260, 291809, 380688, 492505, 669541, 900011, 1.181305e+06, 1.520632e+06, 1.901761e+06, 2.230386e+06, 2.26529e+06, 1.869759e+06, 1.377472e+06, 1.027368e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296663476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0234227e+07, Sum: 1.2914140260903617e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 724, 1006, 1427, 2061, 2860, 4038, 5856, 8309, 11668, 16170, 23035, 32538, 45902, 64624, 91779, 130414, 181961, 243474, 327022, 417155, 558633, 762027, 915579, 1.091455e+06, 1.256363e+06, 1.377245e+06, 1.408531e+06, 1.296152e+06, 1.053135e+06, 750225, 480116, 302102, 186590, 110657, 64699, 38001, 23899, 17973, 15045, 13366, 11428, 9855, 9227, 7534, 5050, 3033, 2065, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 118, 189, 274, 398, 524, 718, 1041, 1437, 2041, 2891, 4070, 5693, 8117, 11295, 16064, 22630, 31208, 43458, 58571, 79167, 104170, 139515, 202268, 291817, 380696, 492515, 669557, 900032, 1.181328e+06, 1.520661e+06, 1.901799e+06, 2.230419e+06, 2.265316e+06, 1.869778e+06, 1.37748e+06, 1.027382e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296678476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0234857e+07, Sum: 1.2914465215196822e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 724, 1006, 1427, 2062, 2861, 4039, 5858, 8309, 11668, 16173, 23036, 32540, 45906, 64628, 91781, 130423, 181966, 243484, 327035, 417169, 558666, 762046, 915611, 1.091504e+06, 1.2564e+06, 1.3773e+06, 1.408572e+06, 1.2962e+06, 1.053172e+06, 750254, 480126, 302112, 186594, 110659, 64701, 38001, 23899, 17974, 15047, 13368, 11428, 9858, 9227, 7535, 5050, 3033, 2065, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 118, 189, 274, 398, 524, 719, 1041, 1437, 2041, 2891, 4071, 5694, 8117, 11296, 16064, 22631, 31209, 43460, 58573, 79168, 104176, 139521, 202278, 291828, 380719, 492529, 669578, 900062, 1.181362e+06, 1.520693e+06, 1.901835e+06, 2.230462e+06, 2.265347e+06, 1.869794e+06, 1.377496e+06, 1.027395e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296693476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0235699e+07, Sum: 1.2914778211390054e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 724, 1006, 1427, 2062, 2861, 4039, 5858, 8310, 11670, 16174, 23038, 32546, 45911, 64631, 91789, 130435, 181976, 243491, 327050, 417186, 558693, 762069, 915649, 1.091545e+06, 1.256445e+06, 1.377333e+06, 1.408617e+06, 1.296235e+06, 1.053215e+06, 750283, 480141, 302121, 186606, 110668, 64708, 38002, 23900, 17975, 15048, 13368, 11428, 9860, 9228, 7535, 5050, 3033, 2065, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 118, 189, 274, 398, 524, 719, 1041, 1437, 2041, 2891, 4071, 5695, 8117, 11297, 16065, 22631, 31210, 43461, 58577, 79171, 104178, 139527, 202287, 291838, 380731, 492553, 669599, 900088, 1.181381e+06, 1.520729e+06, 1.901865e+06, 2.230505e+06, 2.265393e+06, 1.869825e+06, 1.377519e+06, 1.027415e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296708476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0236576e+07, Sum: 1.2915031666534344e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	// Chunk: 2024-09-02T17:05:23Z - 2024-09-02T17:06:38Z
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 724, 1006, 1427, 2062, 2861, 4039, 5858, 8310, 11670, 16175, 23039, 32546, 45913, 64633, 91792, 130443, 181985, 243498, 327065, 417199, 558708, 762090, 915683, 1.091563e+06, 1.256489e+06, 1.377367e+06, 1.408653e+06, 1.29626e+06, 1.053241e+06, 750306, 480146, 302126, 186607, 110671, 64708, 38002, 23904, 17976, 15048, 13368, 11428, 9860, 9229, 7537, 5050, 3033, 2065, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 118, 189, 274, 398, 524, 719, 1041, 1437, 2041, 2891, 4071, 5696, 8117, 11297, 16066, 22633, 31210, 43462, 58580, 79174, 104183, 139533, 202293, 291844, 380738, 492568, 669623, 900108, 1.18141e+06, 1.520765e+06, 1.901895e+06, 2.230531e+06, 2.265418e+06, 1.869841e+06, 1.377536e+06, 1.027431e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296723476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.023723e+07, Sum: 1.2915297923197575e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.UnknownCounterReset})
	require.NoError(t, err)
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 725, 1006, 1427, 2062, 2862, 4039, 5858, 8310, 11670, 16176, 23039, 32548, 45914, 64635, 91794, 130445, 181987, 243504, 327075, 417206, 558730, 762109, 915703, 1.091593e+06, 1.256513e+06, 1.37741e+06, 1.408687e+06, 1.296288e+06, 1.053274e+06, 750323, 480160, 302140, 186622, 110675, 64714, 38003, 23906, 17977, 15049, 13370, 11428, 9860, 9229, 7538, 5050, 3033, 2065, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 119, 189, 274, 398, 524, 719, 1041, 1437, 2041, 2891, 4071, 5696, 8117, 11297, 16066, 22634, 31212, 43464, 58582, 79177, 104186, 139538, 202298, 291852, 380746, 492577, 669637, 900121, 1.181439e+06, 1.520782e+06, 1.901918e+06, 2.230548e+06, 2.265438e+06, 1.86986e+06, 1.377554e+06, 1.027443e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296738476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0237829e+07, Sum: 1.2915542404894568e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 725, 1006, 1427, 2062, 2862, 4039, 5858, 8311, 11671, 16177, 23039, 32548, 45916, 64644, 91800, 130453, 181995, 243512, 327093, 417223, 558748, 762144, 915742, 1.091638e+06, 1.256565e+06, 1.377467e+06, 1.408742e+06, 1.296336e+06, 1.053311e+06, 750345, 480168, 302144, 186623, 110676, 64717, 38003, 23906, 17978, 15049, 13370, 11428, 9861, 9233, 7540, 5050, 3033, 2065, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 119, 189, 274, 398, 524, 719, 1041, 1437, 2041, 2891, 4071, 5698, 8117, 11297, 16067, 22635, 31213, 43467, 58584, 79182, 104191, 139541, 202307, 291859, 380755, 492592, 669652, 900143, 1.181465e+06, 1.520816e+06, 1.901949e+06, 2.230577e+06, 2.265457e+06, 1.869875e+06, 1.377566e+06, 1.027458e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296753476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0238622e+07, Sum: 1.2916013239289599e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 725, 1006, 1427, 2062, 2862, 4039, 5859, 8311, 11671, 16178, 23040, 32549, 45918, 64648, 91805, 130457, 182002, 243528, 327103, 417231, 558763, 762161, 915760, 1.091663e+06, 1.2566e+06, 1.377502e+06, 1.408772e+06, 1.296367e+06, 1.053339e+06, 750353, 480171, 302151, 186624, 110676, 64719, 38004, 23908, 17978, 15050, 13370, 11428, 9861, 9233, 7540, 5050, 3033, 2065, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 119, 189, 274, 398, 524, 719, 1041, 1437, 2041, 2891, 4072, 5698, 8117, 11297, 16067, 22635, 31214, 43469, 58590, 79183, 104194, 139544, 202316, 291868, 380765, 492604, 669680, 900170, 1.181502e+06, 1.520852e+06, 1.90199e+06, 2.230602e+06, 2.265496e+06, 1.869906e+06, 1.377588e+06, 1.027465e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296768476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0239291e+07, Sum: 1.2916046713998303e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	positiveBuckets = []float64{2, 2, 4, 1, 6, 7, 7, 17, 24, 30, 42, 74, 74, 138, 184, 267, 375, 490, 725, 1006, 1427, 2062, 2862, 4039, 5859, 8311, 11671, 16178, 23041, 32550, 45920, 64649, 91808, 130461, 182009, 243533, 327112, 417235, 558779, 762177, 915777, 1.091677e+06, 1.256611e+06, 1.377521e+06, 1.408788e+06, 1.296382e+06, 1.053347e+06, 750357, 480175, 302154, 186626, 110678, 64719, 38004, 23908, 17978, 15050, 13370, 11428, 9861, 9233, 7540, 5050, 3033, 2065, 1502, 1152, 933, 763, 510, 293, 168, 186, 274, 164, 136, 38, 13, 4, 9, 1}
	negativeBuckets = []float64{1, 0, 0, 1, 6, 5, 9, 10, 7, 13, 15, 38, 44, 65, 99, 119, 189, 274, 398, 524, 719, 1041, 1437, 2041, 2891, 4072, 5698, 8117, 11297, 16067, 22636, 31215, 43471, 58590, 79183, 104195, 139547, 202320, 291876, 380772, 492611, 669692, 900184, 1.181524e+06, 1.52088e+06, 1.902021e+06, 2.230627e+06, 2.265519e+06, 1.869926e+06, 1.377597e+06, 1.027465e+06}
	_, err = appender.AppendHistogram(0, lbls, 1725296783476, nil, &histogram.FloatHistogram{Schema: 1, Count: 3.0239693e+07, Sum: 1.2916045703004168e+07, ZeroCount: 9, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -48, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -50, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	_, err = appender.AppendHistogram(0, lbls, 1725296798477, nil, &histogram.FloatHistogram{Sum: math.Float64frombits(value.StaleNaN), CounterResetHint: histogram.UnknownCounterReset})
	require.NoError(t, err)

	lbls = labels.FromStrings("__name__", "cortex_querytee_backend_response_relative_duration_proportional", "pod", "query-tee-mimir-query-engine-69fd5b5dd7-wx55f")
	// Chunk: 2024-09-02T17:06:31Z - 2024-09-02T17:08:01Z
	negativeBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	positiveBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1}
	_, err = appender.AppendHistogram(0, lbls, 1725296791436, nil, &histogram.FloatHistogram{Schema: 3, Count: 7, Sum: 1.802267253842612, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -22, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -9, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.UnknownCounterReset})
	require.NoError(t, err)
	negativeBuckets = []float64{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 1}
	positiveBuckets = []float64{1, 0, 0, 0, 1, 0, 0, 2, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 0, 0, 2, 0, 2, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1}
	_, err = appender.AppendHistogram(0, lbls, 1725296806436, nil, &histogram.FloatHistogram{Schema: 3, Count: 30, Sum: 7.004673032818404, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -49, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -51, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	negativeBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 1, 2, 2, 1, 0, 0, 1, 1, 1, 0, 1, 1, 1, 2, 1, 3, 2, 2, 1, 2, 1, 1, 1, 1, 1, 2, 1, 0, 0, 1, 0, 1, 0, 3, 3, 1, 3, 0, 1, 1, 0, 0, 1, 0, 1, 1}
	positiveBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 2, 1, 0, 3, 0, 0, 0, 1, 0, 2, 1, 1, 0, 0, 2, 0, 1, 1, 0, 1, 0, 1, 3, 2, 2, 1, 1, 4, 1, 2, 1, 3, 2, 0, 1, 6, 1, 3, 1, 2, 1, 0, 1, 1, 0, 2, 1, 1, 1, 0, 1, 1, 2, 2}
	_, err = appender.AppendHistogram(0, lbls, 1725296821436, nil, &histogram.FloatHistogram{Schema: 3, Count: 126, Sum: 27.784442619966878, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -63, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -120, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	negativeBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 1, 2, 2, 1, 1, 0, 2, 1, 1, 0, 1, 1, 2, 2, 1, 3, 2, 2, 1, 2, 2, 1, 1, 1, 1, 3, 2, 1, 0, 2, 0, 3, 0, 3, 4, 1, 3, 0, 1, 1, 0, 0, 1, 0, 1, 1}
	positiveBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 2, 0, 1, 0, 1, 0, 3, 2, 0, 3, 0, 2, 0, 2, 0, 2, 1, 1, 1, 1, 2, 1, 1, 2, 1, 2, 0, 1, 3, 2, 2, 1, 2, 4, 2, 3, 2, 3, 2, 2, 1, 8, 2, 4, 1, 4, 1, 1, 1, 1, 0, 2, 1, 1, 1, 0, 1, 1, 2, 2}
	_, err = appender.AppendHistogram(0, lbls, 1725296836436, nil, &histogram.FloatHistogram{Schema: 3, Count: 164, Sum: 32.05918733122233, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -68, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -120, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	negativeBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 2, 1, 1, 0, 1, 1, 1, 1, 2, 1, 2, 2, 5, 1, 1, 1, 1, 2, 4, 5, 3, 0, 4, 2, 2, 4, 3, 6, 3, 6, 3, 4, 6, 4, 5, 6, 7, 7, 6, 7, 3, 7, 2, 9, 5, 10, 4, 4, 9, 3, 6, 4, 2, 6, 5, 7, 1, 1, 1, 1}
	positiveBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 1, 1, 2, 2, 2, 1, 1, 2, 1, 3, 0, 2, 2, 4, 0, 5, 5, 5, 3, 1, 3, 5, 4, 1, 5, 3, 6, 3, 4, 6, 4, 3, 7, 4, 7, 3, 9, 9, 7, 3, 4, 5, 12, 10, 6, 4, 11, 7, 8, 5, 11, 5, 11, 2, 8, 1, 3, 4, 2, 4, 5, 3, 1, 1, 4, 3, 1, 1, 3, 2, 1, 1, 1, 1, 1, 1}
	_, err = appender.AppendHistogram(0, lbls, 1725296851436, nil, &histogram.FloatHistogram{Schema: 3, Count: 513, Sum: 309.20054260388724, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -115, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -120, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	negativeBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 2, 1, 1, 0, 1, 1, 1, 1, 2, 1, 2, 3, 5, 2, 1, 1, 2, 3, 4, 5, 3, 1, 4, 3, 2, 4, 3, 6, 3, 6, 3, 4, 8, 5, 6, 6, 7, 8, 6, 7, 3, 7, 2, 10, 5, 10, 5, 4, 9, 3, 6, 4, 3, 6, 6, 7, 1, 1, 1, 1}
	positiveBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 2, 1, 2, 2, 2, 1, 1, 1, 1, 2, 1, 4, 1, 2, 2, 5, 1, 5, 5, 5, 4, 2, 3, 5, 4, 1, 5, 4, 6, 3, 4, 7, 4, 3, 7, 5, 7, 4, 9, 10, 7, 3, 4, 7, 13, 12, 7, 6, 11, 7, 8, 5, 11, 5, 11, 2, 8, 1, 3, 6, 2, 5, 5, 3, 1, 1, 4, 3, 1, 1, 3, 2, 1, 1, 1, 1, 1, 1, 1, 1}
	_, err = appender.AppendHistogram(0, lbls, 1725296866436, nil, &histogram.FloatHistogram{Schema: 3, Count: 556, Sum: 349.78324313754496, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -115, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -120, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)
	negativeBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 1, 3, 3, 5, 1, 2, 1, 1, 2, 3, 5, 6, 4, 1, 5, 4, 4, 5, 4, 7, 3, 7, 4, 7, 12, 7, 7, 7, 10, 8, 7, 10, 3, 8, 3, 12, 8, 11, 8, 7, 9, 4, 6, 5, 3, 7, 6, 9, 1, 1, 1, 1}
	positiveBuckets = []float64{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 1, 2, 2, 2, 1, 2, 1, 2, 4, 2, 4, 2, 4, 4, 5, 1, 5, 6, 5, 6, 5, 5, 5, 4, 2, 7, 4, 8, 4, 6, 9, 7, 7, 11, 8, 9, 5, 10, 11, 9, 3, 5, 8, 17, 14, 9, 8, 12, 8, 11, 7, 11, 6, 14, 4, 8, 3, 3, 8, 4, 5, 5, 3, 2, 1, 5, 3, 1, 1, 3, 2, 1, 1, 1, 1, 1, 1, 1, 1}
	_, err = appender.AppendHistogram(0, lbls, 1725296881436, nil, &histogram.FloatHistogram{Schema: 3, Count: 697, Sum: 358.89273472056067, ZeroThreshold: 2.938735877055719e-39, PositiveSpans: []histogram.Span{{Offset: -129, Length: uint32(len(positiveBuckets))}}, PositiveBuckets: positiveBuckets, NegativeSpans: []histogram.Span{{Offset: -120, Length: uint32(len(negativeBuckets))}}, NegativeBuckets: negativeBuckets, CounterResetHint: histogram.NotCounterReset})
	require.NoError(t, err)

	require.NoError(t, appender.Commit())

	opts := NewTestEngineOpts()
	mimirEngine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
	require.NoError(t, err)
	prometheusEngine := promql.NewEngine(opts.CommonOpts)

	engines := map[string]promql.QueryEngine{
		"Mimir's engine":     mimirEngine,
		"Prometheus' engine": prometheusEngine,
	}

	for name, engine := range engines {
		t.Run(name, func(t *testing.T) {
			if name == "Mimir's engine" {
				//t.Skip()
			}

			query, err := engine.NewRangeQuery(context.Background(), storage, nil, "rate(cortex_querytee_backend_response_relative_duration_proportional[2m15s])", time.Date(2024, 9, 2, 17, 4, 0, 0, time.UTC), time.Date(2024, 9, 2, 17, 8, 0, 0, time.UTC), 2*time.Minute)
			require.NoError(t, err)

			res := query.Exec(context.Background())
			require.NoError(t, res.Err)

			fmt.Println(res.Value.String())
		})
	}
}
