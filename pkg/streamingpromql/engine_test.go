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
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/globalerror"
)

func TestUnsupportedPromQLFeatures(t *testing.T) {
	featureToggles := EnableAllFeatures

	// The goal of this is not to list every conceivable expression that is unsupported, but to cover all the
	// different cases and make sure we produce a reasonable error message when these cases are encountered.
	unsupportedExpressions := map[string]string{
		"metric{} or other_metric{}":                   "binary expression with many-to-many matching",
		"metric{} + on() group_left() other_metric{}":  "binary expression with many-to-one matching",
		"metric{} + on() group_right() other_metric{}": "binary expression with one-to-many matching",
		"topk(5, metric{})":                            "'topk' aggregation with parameter",
		`count_values("foo", metric{})`:                "'count_values' aggregation with parameter",
		"rate(metric{}[5m:1m])":                        "PromQL expression type *parser.SubqueryExpr for range vectors",
		"quantile_over_time(0.4, metric{}[5m])":        "'quantile_over_time' function",
		"quantile(0.95, metric{})":                     "'quantile' aggregation with parameter",
	}

	for expression, expectedError := range unsupportedExpressions {
		t.Run(expression, func(t *testing.T) {
			requireQueryIsUnsupported(t, featureToggles, expression, expectedError)
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

		requireQueryIsUnsupported(t, featureToggles, "sum by (label) (metric)", "aggregation operations")
	})

	t.Run("binary expressions", func(t *testing.T) {
		featureToggles := EnableAllFeatures
		featureToggles.EnableBinaryOperations = false

		for _, op := range []string{"+", "> bool"} {
			requireQueryIsUnsupported(t, featureToggles, fmt.Sprintf("metric{} %s other_metric{}", op), "binary expressions")
			requireQueryIsUnsupported(t, featureToggles, fmt.Sprintf("metric{} %s 1", op), "binary expressions")
			requireQueryIsUnsupported(t, featureToggles, fmt.Sprintf("1 %s metric{}", op), "binary expressions")
			requireQueryIsUnsupported(t, featureToggles, fmt.Sprintf("2 %s 1", op), "binary expressions")
		}
	})

	t.Run("binary expressions with comparison operation", func(t *testing.T) {
		featureToggles := EnableAllFeatures
		featureToggles.EnableBinaryComparisonOperations = false

		requireQueryIsUnsupported(t, featureToggles, "metric{} > other_metric{}", "binary expression with '>'")
		requireQueryIsUnsupported(t, featureToggles, "metric{} > 1", "binary expression with '>'")
		requireQueryIsUnsupported(t, featureToggles, "1 > metric{}", "binary expression with '>'")
		requireQueryIsUnsupported(t, featureToggles, "2 > bool 1", "binary expression with '>'")

		// Other operations should still be supported.
		requireQueryIsSupported(t, featureToggles, "metric{} + other_metric{}")
		requireQueryIsSupported(t, featureToggles, "metric{} + 1")
		requireQueryIsSupported(t, featureToggles, "1 + metric{}")
		requireQueryIsSupported(t, featureToggles, "2 + 1")
	})

	t.Run("..._over_time functions", func(t *testing.T) {
		featureToggles := EnableAllFeatures
		featureToggles.EnableOverTimeFunctions = false

		requireQueryIsUnsupported(t, featureToggles, "count_over_time(metric[1m])", "'count_over_time' function")
	})

	t.Run("offset modifier", func(t *testing.T) {
		featureToggles := EnableAllFeatures
		featureToggles.EnableOffsetModifier = false

		requireQueryIsUnsupported(t, featureToggles, "metric offset 1m", "instant vector selector with 'offset'")
		requireQueryIsUnsupported(t, featureToggles, "rate(metric[2m] offset 1m)", "range vector selector with 'offset'")
		requireInstantQueryIsUnsupported(t, featureToggles, "metric[2m] offset 1m", "range vector selector with 'offset'")
	})

	t.Run("scalars", func(t *testing.T) {
		featureToggles := EnableAllFeatures
		featureToggles.EnableScalars = false

		requireQueryIsUnsupported(t, featureToggles, "2", "scalar values")
	})

	t.Run("unary negation", func(t *testing.T) {
		featureToggles := EnableAllFeatures
		featureToggles.EnableUnaryNegation = false

		requireQueryIsUnsupported(t, featureToggles, "-sum(metric{})", "unary negation of instant vectors")
		requireQueryIsUnsupported(t, featureToggles, "-(1)", "unary negation of scalars")
	})
}

func requireQueryIsUnsupported(t *testing.T, toggles FeatureToggles, expression string, expectedError string) {
	requireRangeQueryIsUnsupported(t, toggles, expression, expectedError)
	requireInstantQueryIsUnsupported(t, toggles, expression, expectedError)
}

func requireQueryIsSupported(t *testing.T, toggles FeatureToggles, expression string) {
	requireRangeQueryIsSupported(t, toggles, expression)
	requireInstantQueryIsSupported(t, toggles, expression)
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

func requireRangeQueryIsSupported(t *testing.T, featureToggles FeatureToggles, expression string) {
	opts := NewTestEngineOpts()
	opts.FeatureToggles = featureToggles
	engine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
	require.NoError(t, err)

	_, err = engine.NewRangeQuery(context.Background(), nil, nil, expression, time.Now().Add(-time.Hour), time.Now(), time.Minute)
	require.NoError(t, err)
}

func requireInstantQueryIsSupported(t *testing.T, featureToggles FeatureToggles, expression string) {
	opts := NewTestEngineOpts()
	opts.FeatureToggles = featureToggles
	engine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
	require.NoError(t, err)

	_, err = engine.NewInstantQuery(context.Background(), nil, nil, expression, time.Now())
	require.NoError(t, err)
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

func (qt *testQueryTracker) Close() error {
	return nil
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

func (t *timeoutTestingQueryTracker) Close() error {
	return nil
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
		"rate() over metric without counter suffix with single float or histogram in range": {
			data: `
				series 3 1 {{schema:3 sum:12 count:7 buckets:[2 2 3]}}
			`,
			expr:                       "rate(series[45s])",
			expectedWarningAnnotations: []string{},
			expectedInfoAnnotations:    []string{},
			// This can be removed once https://github.com/prometheus/prometheus/pull/14910 is vendored.
			skipComparisonWithPrometheusReason: "Prometheus only considers the type of the last point in the vector selector rather than the output value",
		},
		"rate() over one point in range": {
			data: `
				series 1
			`,
			expr:                       "rate(series[1m])",
			expectedWarningAnnotations: []string{},
			expectedInfoAnnotations:    []string{},
			// This can be removed once https://github.com/prometheus/prometheus/pull/14910 is vendored.
			skipComparisonWithPrometheusReason: "Prometheus only considers the type of the last point in the vector selector rather than the output value",
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

func TestCompareVariousMixedMetrics(t *testing.T) {
	// Although most tests are covered with the promql test files (both ours and upstream),
	// there is a lot of repetition around a few edge cases.
	// This is not intended to be comprehensive, but instead check for some common edge cases
	// ensuring MQE and Prometheus' engines return the same result when querying:
	// - Series with mixed floats and histograms
	// - Aggregations with mixed data types
	// - Points with NaN
	// - Stale markers
	// - Look backs

	opts := NewTestEngineOpts()
	mimirEngine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
	require.NoError(t, err)

	prometheusEngine := promql.NewEngine(opts.CommonOpts)

	// We're loading series with the follow combinations of values. This is difficult to visually see in the actual
	// data loaded, so it is represented in a table here.
	// f = float value, h = native histogram, _ = no value, N = NaN, s = stale
	// {a} f f f f f f
	// {b} h h h h h h
	// {c} f h f h N h
	// {d} f _ _ s f f
	// {e} h h _ s h N
	// {f} f N _ f f N
	// {g} N N N N N N
	// {h} N N N _ N s
	// {i} f h _ N h s
	// {j} f f s s s s
	// {k} 0 0 0 N s 0
	// {l} h _ f _ s N
	// {m} s s N _ _ f
	// {n} _ _ _ _ _ _

	pointsPerSeries := 6
	samples := `
		series{label="a", group="a"} 1 2 3 4 5 -50
		series{label="b", group="a"} {{schema:1 sum:15 count:10 buckets:[3 2 5 7 9]}} {{schema:2 sum:20 count:15 buckets:[4]}} {{schema:3 sum:25 count:20 buckets:[5 8]}} {{schema:4 sum:30 count:25 buckets:[6 9 10 11]}} {{schema:5 sum:35 count:30 buckets:[7 10 13]}} {{schema:6 sum:40 count:35 buckets:[8 11 14]}}
		series{label="c", group="a"} 1 {{schema:3 sum:5 count:3 buckets:[1 1 1]}} 3 {{schema:3 sum:10 count:6 buckets:[2 2 2]}} NaN {{schema:3 sum:12 count:7 buckets:[2 2 3]}}
		series{label="d", group="a"} 1 _ _ stale 5 6
		series{label="e", group="b"} {{schema:4 sum:12 count:8 buckets:[2 3 3]}} {{schema:4 sum:14 count:9 buckets:[3 3 3]}} _ stale {{schema:4 sum:18 count:11 buckets:[4 4 3]}} NaN
		series{label="f", group="b"} 1 NaN _ 4 5 NaN
		series{label="g", group="b"} NaN NaN NaN NaN NaN NaN
		series{label="h", group="b"} NaN NaN NaN _ NaN stale
		series{label="i", group="c"} 1 {{schema:5 sum:15 count:10 buckets:[3 2 5]}} _ NaN {{schema:2 sum:30 count:25 buckets:[6 9 10 9 1]}} stale
		series{label="j", group="c"} 1 -20 stale stale stale stale
		series{label="k", group="c"} 0 0 0 NaN stale 0
		series{label="l", group="d"} {{schema:1 sum:10 count:5 buckets:[1 2]}} _ 3 _ stale NaN
		series{label="m", group="d"} stale stale NaN _ _ 4
		series{label="n", group="d"}
	`

	// Labels for generating combinations
	labels := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"}

	// Generate combinations of 2 and 3 labels. (e.g., "a,b", "e,f", "c,d,e" etc)
	// These will be used for binary operations, so we can add up to 3 series.
	labelCombinations := testutils.Combinations(labels, 2)
	labelCombinations = append(labelCombinations, testutils.Combinations(labels, 3)...)

	expressions := []string{}

	// Binary operations
	for _, labels := range labelCombinations {
		if len(labels) >= 2 {
			for _, op := range []string{"+", "-", "*", "/"} {
				binaryExpr := fmt.Sprintf(`series{label="%s"}`, labels[0])
				for _, label := range labels[1:] {
					binaryExpr += fmt.Sprintf(` %s series{label="%s"}`, op, label)
				}
				expressions = append(expressions, binaryExpr)
			}
		}
	}

	// For aggregations, also add combinations of 4 labels. (e.g., "a,b,c,d", "c,d,e,f" etc)
	labelCombinations = append(labelCombinations, testutils.Combinations(labels, 4)...)

	for _, labels := range labelCombinations {
		labelRegex := strings.Join(labels, "|")
		// Aggregations
		for _, aggFunc := range []string{"avg", "count", "group", "min", "max", "sum"} {
			expressions = append(expressions, fmt.Sprintf(`%s(series{label=~"(%s)"})`, aggFunc, labelRegex))
			expressions = append(expressions, fmt.Sprintf(`%s by (group) (series{label=~"(%s)"})`, aggFunc, labelRegex))
			expressions = append(expressions, fmt.Sprintf(`%s without (group) (series{label=~"(%s)"})`, aggFunc, labelRegex))
		}
		// Multiple range-vector times are used to check lookbacks that only select single points, multiple points, and boundaries.
		expressions = append(expressions, fmt.Sprintf(`rate(series{label=~"(%s)"}[45s])`, labelRegex))
		expressions = append(expressions, fmt.Sprintf(`rate(series{label=~"(%s)"}[1m])`, labelRegex))
		expressions = append(expressions, fmt.Sprintf(`avg(rate(series{label=~"(%s)"}[2m15s]))`, labelRegex))
		expressions = append(expressions, fmt.Sprintf(`avg(rate(series{label=~"(%s)"}[5m]))`, labelRegex))
	}

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
			testName := fmt.Sprintf("Expr: %s, Start: %d, End: %d, Interval: %s", expr, start.Unix(), end.Unix(), tr.interval)
			t.Run(testName, func(t *testing.T) {
				q, err := prometheusEngine.NewRangeQuery(context.Background(), storage, nil, expr, start, end, tr.interval)
				require.NoError(t, err)
				defer q.Close()
				expectedResults := q.Exec(context.Background())

				q, err = mimirEngine.NewRangeQuery(context.Background(), storage, nil, expr, start, end, tr.interval)
				require.NoError(t, err)
				defer q.Close()
				mimirResults := q.Exec(context.Background())

				// We currently omit checking the annotations due to a difference between the engines.
				// This can be re-enabled once https://github.com/prometheus/prometheus/pull/14910 is vendored.
				testutils.RequireEqualResults(t, expr, expectedResults, mimirResults, false)
			})
		}
	}
}
