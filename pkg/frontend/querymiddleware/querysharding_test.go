// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/promql_test.go
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/querysharding_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/grafana/regexp"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/shardingtest"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/testdatagen"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/util"
)

var (
	start         = time.Now()
	end           = start.Add(30 * time.Minute)
	step          = 30 * time.Second
	lookbackDelta = 5 * time.Minute
)

func init() {
	// This enables duration arithmetic https://github.com/prometheus/prometheus/pull/16249.
	parser.ExperimentalDurationExpr = true
}

func mockHandlerWith(resp *PrometheusResponse, err error) MetricsQueryHandler {
	return HandlerFunc(func(ctx context.Context, _ MetricsQueryRequest) (Response, error) {
		if expired := ctx.Err(); expired != nil {
			return nil, expired
		}

		return resp, err
	})
}

func sampleStreamsStrings(ss []SampleStream) []string {
	strs := make([]string, len(ss))
	for i := range ss {
		strs[i] = mimirpb.FromLabelAdaptersToMetric(ss[i].Labels).String()
	}
	return strs
}

// approximatelyEqualsSamples ensures two responses are approximately equal, up to 6 decimals precision per sample,
// but only checks the samples and not the warning/info annotations.
func approximatelyEqualsSamples(t *testing.T, a, b *PrometheusResponse) {
	// Ensure both queries succeeded.
	require.Equal(t, statusSuccess, a.Status)
	require.Equal(t, statusSuccess, b.Status)

	as, err := ResponseToSamples(a)
	require.Nil(t, err)
	bs, err := ResponseToSamples(b)
	require.Nil(t, err)

	require.Equalf(t, len(as), len(bs), "expected same number of series: one contains %v, other %v", sampleStreamsStrings(as), sampleStreamsStrings(bs))

	for i := 0; i < len(as); i++ {
		a := as[i]
		b := bs[i]
		require.Equal(t, a.Labels, b.Labels)
		require.Equal(t, len(a.Samples), len(b.Samples), "expected same number of samples for series %s", a.Labels)
		require.Equal(t, len(a.Histograms), len(b.Histograms), "expected same number of histograms for series %s", a.Labels)
		require.NotEqual(t, len(a.Samples) > 0, len(a.Histograms) > 0, "expected either samples or histogram but not both for series %s, got %d samples and %d histograms", a.Labels, len(a.Samples), len(a.Histograms))

		for j := 0; j < len(a.Samples); j++ {
			expected := a.Samples[j]
			actual := b.Samples[j]
			compareExpectedAndActual(t, expected.TimestampMs, actual.TimestampMs, expected.Value, actual.Value, j, a.Labels, "sample", 1e-12)
		}

		for j := 0; j < len(a.Histograms); j++ {
			expected := a.Histograms[j]
			actual := b.Histograms[j]
			compareExpectedAndActual(t, expected.TimestampMs, actual.TimestampMs, expected.Histogram.Sum, actual.Histogram.Sum, j, a.Labels, "histogram", 1e-12)
		}
	}
}

// approximatelyEquals ensures two responses are approximately equal, up to 6 decimals precision per sample
func approximatelyEquals(t *testing.T, a, b *PrometheusResponse) {
	approximatelyEqualsSamples(t, a, b)
	require.ElementsMatch(t, a.Infos, b.Infos, "expected same info annotations")
	require.ElementsMatch(t, a.Warnings, b.Warnings, "expected same warning annotations")
}

func compareExpectedAndActual(t *testing.T, expectedTs, actualTs int64, expectedVal, actualVal float64, j int, labels []mimirpb.LabelAdapter, sampleType string, tolerance float64) {
	require.Equalf(t, expectedTs, actualTs, "%s timestamp at position %d for series %s", sampleType, j, labels)

	if value.IsStaleNaN(expectedVal) {
		require.Truef(t, value.IsStaleNaN(actualVal), "%s value at position %d is expected to be stale marker for series %s", sampleType, j, labels)
	} else if math.IsNaN(expectedVal) {
		require.Truef(t, math.IsNaN(actualVal), "%s value at position %d is expected to be NaN for series %s", sampleType, j, labels)
	} else {
		if expectedVal == 0 {
			require.Zero(t, actualVal, "%s value at position %d with timestamp %d for series %s", sampleType, j, expectedTs, labels)
			return
		}
		// InEpsilon means the relative error (see https://en.wikipedia.org/wiki/Relative_error#Example) must be less than epsilon (here 1e-12).
		// The relative error is calculated using: abs(actual-expected) / abs(expected)
		if math.IsInf(expectedVal, +1) || math.IsInf(expectedVal, -1) {
			require.Equal(t, expectedVal, actualVal)
		} else {
			require.InEpsilonf(t, expectedVal, actualVal, tolerance, "%s value at position %d with timestamp %d for series %s", sampleType, j, expectedTs, labels)
		}
	}
}

func TestQuerySharding_Correctness(t *testing.T) {
	shardingtest.RunCorrectnessTests(t, func(t *testing.T, testData shardingtest.CorrectnessTestCase, queryable storage.Queryable) {
		reqs := []MetricsQueryRequest{
			&PrometheusInstantQueryRequest{
				path:      "/query",
				time:      util.TimeToMillis(shardingtest.End),
				queryExpr: parseQuery(t, testData.Query),
			},
		}
		if !testData.NoRangeQuery {
			reqs = append(reqs, &PrometheusRangeQueryRequest{
				path:      "/query_range",
				start:     util.TimeToMillis(shardingtest.Start),
				end:       util.TimeToMillis(shardingtest.End),
				step:      shardingtest.Step.Milliseconds(),
				queryExpr: parseQuery(t, testData.Query),
			})
		}

		for _, req := range reqs {
			t.Run(fmt.Sprintf("%T", req), func(t *testing.T) {
				runForEngines(t, func(t *testing.T, opts promql.EngineOpts, eng promql.QueryEngine) {
					downstream := &downstreamHandler{
						engine:                                  eng,
						queryable:                               queryable,
						includePositionInformationInAnnotations: true,
					}

					// Run the query without sharding.
					expectedRes, err := downstream.Do(context.Background(), req)
					require.Nil(t, err)
					expectedPrometheusRes, ok := expectedRes.GetPrometheusResponse()
					require.True(t, ok)
					if !testData.ExpectSpecificOrder {
						sort.Sort(byLabels(expectedPrometheusRes.Data.Result))
					}

					// Ensure the query produces some results.
					require.NotEmpty(t, expectedPrometheusRes.Data.Result)
					requireValidSamples(t, expectedPrometheusRes.Data.Result)

					if testData.ExpectedShardedQueries > 0 {
						// Remove position information from annotations, to mirror what we expect from the sharded queries below.
						removeAllAnnotationPositionInformation(expectedPrometheusRes.Infos)
						removeAllAnnotationPositionInformation(expectedPrometheusRes.Warnings)
					}

					for _, numShards := range []int{2, 4, 8, 16} {
						t.Run(fmt.Sprintf("shards=%d", numShards), func(t *testing.T) {
							reg := prometheus.NewPedanticRegistry()
							shardingware := newQueryShardingMiddleware(
								log.NewNopLogger(),
								eng,
								mockLimits{totalShards: numShards},
								0,
								reg,
							)
							durationsware := newDurationsMiddleware(log.NewNopLogger())
							// Run the query with sharding.
							shardedRes, err := durationsware.Wrap(shardingware.Wrap(downstream)).Do(user.InjectOrgID(context.Background(), "test"), req)
							require.Nil(t, err)

							// Ensure the two results matches (float precision can slightly differ, there's no guarantee in PromQL engine too
							// if you rerun the same query twice).
							shardedPrometheusRes, ok := shardedRes.GetPrometheusResponse()
							require.True(t, ok)
							if !testData.ExpectSpecificOrder {
								sort.Sort(byLabels(shardedPrometheusRes.Data.Result))
							}
							approximatelyEquals(t, expectedPrometheusRes, shardedPrometheusRes)

							// Ensure the query has been sharded/not sharded as expected.
							shardingtest.AssertShardingMetrics(t, reg, testData.ExpectedShardedQueries, numShards)
						})
					}
				})
			})
		}
	})
}

func TestQuerySharding_NonMonotonicHistogramBuckets(t *testing.T) {
	queries := []string{
		`histogram_quantile(1, sum by(le) (rate(metric_histogram_bucket[1m])))`,
	}

	var series []storage.Series
	for i := 0; i < 100; i++ {
		series = append(series, testdatagen.NewSeries(labels.FromStrings(model.MetricNameLabel, "metric_histogram_bucket", "app", strconv.Itoa(i), "le", "10"), start.Add(-lookbackDelta), end, step, testdatagen.ArithmeticSequence(1)))
		series = append(series, testdatagen.NewSeries(labels.FromStrings(model.MetricNameLabel, "metric_histogram_bucket", "app", strconv.Itoa(i), "le", "20"), start.Add(-lookbackDelta), end, step, testdatagen.ArithmeticSequence(3)))
		series = append(series, testdatagen.NewSeries(labels.FromStrings(model.MetricNameLabel, "metric_histogram_bucket", "app", strconv.Itoa(i), "le", "30"), start.Add(-lookbackDelta), end, step, testdatagen.ArithmeticSequence(3)))
		series = append(series, testdatagen.NewSeries(labels.FromStrings(model.MetricNameLabel, "metric_histogram_bucket", "app", strconv.Itoa(i), "le", "40"), start.Add(-lookbackDelta), end, step, testdatagen.ArithmeticSequence(3)))
		series = append(series, testdatagen.NewSeries(labels.FromStrings(model.MetricNameLabel, "metric_histogram_bucket", "app", strconv.Itoa(i), "le", "+Inf"), start.Add(-lookbackDelta), end, step, testdatagen.ArithmeticSequence(3)))
	}

	// Create a queryable on the fixtures.
	queryable := testdatagen.StorageSeriesQueryable(series)

	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			runForEngines(t, func(t *testing.T, opts promql.EngineOpts, eng promql.QueryEngine) {
				downstream := &downstreamHandler{
					engine:    eng,
					queryable: queryable,
				}

				req := &PrometheusRangeQueryRequest{
					path:      "/query_range",
					start:     util.TimeToMillis(start),
					end:       util.TimeToMillis(end),
					step:      step.Milliseconds(),
					queryExpr: parseQuery(t, query),
				}

				// Run the query without sharding.
				expectedRes, err := downstream.Do(context.Background(), req)
				require.Nil(t, err)

				expectedPrometheusRes, ok := expectedRes.GetPrometheusResponse()
				require.True(t, ok)
				sort.Sort(byLabels(expectedPrometheusRes.Data.Result))

				// Ensure the query produces some results.
				require.NotEmpty(t, expectedPrometheusRes.Data.Result)
				requireValidSamples(t, expectedPrometheusRes.Data.Result)

				// Ensure the bucket monotonicity has not been fixed by PromQL engine.
				require.Len(t, expectedPrometheusRes.GetWarnings(), 0)

				for _, numShards := range []int{8, 16} {
					t.Run(fmt.Sprintf("shards=%d", numShards), func(t *testing.T) {
						reg := prometheus.NewPedanticRegistry()
						shardingware := newQueryShardingMiddleware(
							log.NewNopLogger(),
							eng,
							mockLimits{totalShards: numShards},
							0,
							reg,
						)

						// Run the query with sharding.
						shardedRes, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
						require.Nil(t, err)

						// Ensure the two results matches (float precision can slightly differ, there's no guarantee in PromQL engine too
						// if you rerun the same query twice).
						shardedPrometheusRes, ok := shardedRes.GetPrometheusResponse()
						require.True(t, ok)
						sort.Sort(byLabels(shardedPrometheusRes.Data.Result))
						approximatelyEquals(t, expectedPrometheusRes, shardedPrometheusRes)

						// Ensure the warning about bucket monotonicity from PromQL engine is hidden.
						require.Len(t, shardedPrometheusRes.GetWarnings(), 0)
					})
				}
			})
		})
	}
}

// requireValidSamples ensures the query produces some results which are not NaN.
func requireValidSamples(t *testing.T, result []SampleStream) {
	t.Helper()
	for _, stream := range result {
		for _, sample := range stream.Samples {
			if !math.IsNaN(sample.Value) {
				return
			}
		}
		for _, h := range stream.Histograms {
			if !math.IsNaN(h.Histogram.Sum) {
				return
			}
		}
	}
	t.Fatalf("Result should have some not-NaN samples")
}

type byLabels []SampleStream

func (b byLabels) Len() int      { return len(b) }
func (b byLabels) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b byLabels) Less(i, j int) bool {
	return labels.Compare(
		mimirpb.FromLabelAdaptersToLabels(b[i].Labels),
		mimirpb.FromLabelAdaptersToLabels(b[j].Labels),
	) < 0
}

func TestQueryshardingDeterminism(t *testing.T) {
	const shards = 16

	// These are "evil" floats found in production which are the result of a rate of 1 and 3 requests per 1m5s.
	// We push them as a gauge here to simplify the test scenario.
	const (
		evilFloatA = 0.03298
		evilFloatB = 0.09894
	)
	require.NotEqualf(t,
		evilFloatA+evilFloatA+evilFloatA,
		evilFloatA+evilFloatB+evilFloatA,
		"This test is based on the fact that given a=%f and b=%f, then a+a+b != a+b+a. If that is not true, this test is not testing anything.", evilFloatA, evilFloatB,
	)

	var (
		from = time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
		step = 30 * time.Second
		to   = from.Add(step)
	)

	labelsForShard := labelsForShardsGenerator([]labels.Label{{Name: model.MetricNameLabel, Value: "metric"}}, shards)
	storageSeries := []storage.Series{
		testdatagen.NewSeries(labelsForShard(0), from, to, step, constant(evilFloatA)),
		testdatagen.NewSeries(labelsForShard(1), from, to, step, constant(evilFloatA)),
		testdatagen.NewSeries(labelsForShard(2), from, to, step, constant(evilFloatB)),
	}
	queryable := testdatagen.StorageSeriesQueryable(storageSeries)

	runForEngines(t, func(t *testing.T, opts promql.EngineOpts, eng promql.QueryEngine) {
		shardingware := newQueryShardingMiddleware(log.NewNopLogger(), eng, mockLimits{totalShards: shards}, 0, prometheus.NewPedanticRegistry())
		downstream := &downstreamHandler{engine: eng, queryable: queryable}

		req := &PrometheusInstantQueryRequest{
			path:      "/query",
			time:      to.UnixMilli(),
			queryExpr: parseQuery(t, `sum(metric)`),
		}

		var lastVal float64
		for i := 0; i <= 100; i++ {
			shardedRes, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
			require.NoError(t, err)

			shardedPrometheusRes, ok := shardedRes.GetPrometheusResponse()
			require.True(t, ok)

			sampleStreams, err := ResponseToSamples(shardedPrometheusRes)
			require.NoError(t, err)

			require.Lenf(t, sampleStreams, 1, "There should be 1 samples stream (query %d)", i)
			require.Lenf(t, sampleStreams[0].Samples, 1, "There should be 1 sample in the first stream (query %d)", i)
			val := sampleStreams[0].Samples[0].Value

			if i > 0 {
				require.Equalf(t, lastVal, val, "Value differs on query %d", i)
			}
			lastVal = val
		}
	})
}

// labelsForShardsGenerator returns a function that provides labels.Labels for the shard requested
// A single generator instance generates different label sets.
func labelsForShardsGenerator(base []labels.Label, shards uint64) func(shard uint64) labels.Labels {
	i := 0
	builder := labels.ScratchBuilder{}
	return func(shard uint64) labels.Labels {
		for {
			i++
			builder.Reset()
			for _, l := range base {
				builder.Add(l.Name, l.Value)
			}
			builder.Add("__test_shard_adjuster__", fmt.Sprintf("adjusted to be %s by %d", sharding.FormatShardIDLabelValue(shard, shards), i))
			builder.Sort()
			ls := builder.Labels()
			// If this label value makes this labels combination fall into the desired shard, return it, otherwise keep trying.
			if labels.StableHash(ls)%shards == shard {
				return ls
			}
		}
	}
}

// TestQuerySharding_FunctionCorrectness is the old test that probably at some point inspired the TestQuerySharding_Correctness,
// we keep it here since it adds more test cases.
func TestQuerySharding_FunctionCorrectness(t *testing.T) {
	shardingtest.RunFunctionCorrectnessTests(t, func(t *testing.T, expr string, numShards int, allowedErr error, queryable storage.Queryable) {
		runForEngines(t, func(t *testing.T, opts promql.EngineOpts, eng promql.QueryEngine) {
			req := &PrometheusRangeQueryRequest{
				path:      "/query_range",
				start:     util.TimeToMillis(shardingtest.Start),
				end:       util.TimeToMillis(shardingtest.End),
				step:      shardingtest.Step.Milliseconds(),
				queryExpr: parseQuery(t, expr),
			}

			reg := prometheus.NewPedanticRegistry()
			shardingware := newQueryShardingMiddleware(
				log.NewNopLogger(),
				eng,
				mockLimits{totalShards: numShards},
				0,
				reg,
			)
			downstream := &downstreamHandler{
				engine:    eng,
				queryable: queryable,
			}

			// Run the query without sharding.
			expectedRes, err := downstream.Do(context.Background(), req)
			// The Mimir Query Engine doesn't (currently) support experimental functions
			// so it's expected for it to return an error in some cases. Short-circuit the
			// rest of this test in that case.
			if err != nil && allowedErr != nil {
				require.ErrorAs(t, err, &allowedErr)
				return
			}

			require.NoError(t, err)
			expectedPrometheusRes, ok := expectedRes.GetPrometheusResponse()
			require.True(t, ok)

			// Ensure the query produces some results.
			require.NotEmpty(t, expectedPrometheusRes.Data.Result)

			// Run the query with sharding.
			shardedRes, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
			require.Nil(t, err)
			shardedPrometheusRes, ok := shardedRes.GetPrometheusResponse()
			require.True(t, ok)

			// Ensure the two results match (float precision can slightly differ, there's no guarantee in PromQL engine too
			// if you rerun the same query twice).
			approximatelyEquals(t, expectedPrometheusRes, shardedPrometheusRes)
		})
	})
}

func TestQuerySharding_ShouldSkipShardingViaOption(t *testing.T) {
	req := &PrometheusRangeQueryRequest{
		path:      "/query_range",
		start:     util.TimeToMillis(start),
		end:       util.TimeToMillis(end),
		step:      step.Milliseconds(),
		queryExpr: parseQuery(t, "sum by (foo) (rate(bar{}[1m]))"), // shardable query.
		options: Options{
			ShardingDisabled: true,
		},
	}

	runForEngines(t, func(t *testing.T, opts promql.EngineOpts, eng promql.QueryEngine) {
		shardingware := newQueryShardingMiddleware(log.NewNopLogger(), eng, mockLimits{totalShards: 16}, 0, nil)
		downstream := &mockHandler{}
		downstream.On("Do", mock.Anything, mock.Anything).Return(&PrometheusResponse{Status: statusSuccess}, nil)

		res, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
		require.NoError(t, err)
		shardedPrometheusRes, ok := res.GetPrometheusResponse()
		require.True(t, ok)

		assert.Equal(t, statusSuccess, shardedPrometheusRes.GetStatus())
		// Ensure we get the same request downstream. No sharding
		downstream.AssertCalled(t, "Do", mock.Anything, req)
		downstream.AssertNumberOfCalls(t, "Do", 1)
	})
}

func TestQuerySharding_ShouldSkipShardingIfUnshardableAndMaxShardedQueriesIsZero(t *testing.T) {
	req := &PrometheusRangeQueryRequest{
		path:      "/query_range",
		start:     util.TimeToMillis(start),
		end:       util.TimeToMillis(end),
		step:      step.Milliseconds(),
		queryExpr: parseQuery(t, "clamp_max(max(foo), scalar(bar))"),
	}

	runForEngines(t, func(t *testing.T, opts promql.EngineOpts, eng promql.QueryEngine) {
		shardingware := newQueryShardingMiddleware(log.NewNopLogger(), eng, mockLimits{totalShards: 16, maxShardedQueries: 0}, 0, nil)
		downstream := &mockHandler{}
		downstream.On("Do", mock.Anything, mock.Anything).Return(&PrometheusResponse{Status: statusSuccess}, nil)

		res, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
		require.NoError(t, err)
		shardedPrometheusRes, ok := res.GetPrometheusResponse()
		require.True(t, ok)

		assert.Equal(t, statusSuccess, shardedPrometheusRes.GetStatus())
		// Ensure we get the same request downstream. No sharding
		downstream.AssertCalled(t, "Do", mock.Anything, req)
		downstream.AssertNumberOfCalls(t, "Do", 1)
	})
}

func TestQuerySharding_ShouldOverrideShardingSizeViaOption(t *testing.T) {
	req := &PrometheusRangeQueryRequest{
		path:      "/query_range",
		start:     util.TimeToMillis(start),
		end:       util.TimeToMillis(end),
		step:      step.Milliseconds(),
		queryExpr: parseQuery(t, "sum by (foo) (rate(bar{}[1m]))"), // shardable query.
		options: Options{
			TotalShards: 128,
		},
	}

	runForEngines(t, func(t *testing.T, opts promql.EngineOpts, eng promql.QueryEngine) {
		shardingware := newQueryShardingMiddleware(log.NewNopLogger(), eng, mockLimits{totalShards: 16}, 0, nil)

		downstream := &mockHandler{}
		downstream.On("Do", mock.Anything, mock.Anything).Return(&PrometheusResponse{
			Status: statusSuccess, Data: &PrometheusData{
				ResultType: string(parser.ValueTypeVector),
			},
		}, nil)

		res, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
		require.NoError(t, err)
		shardedPrometheusRes, ok := res.GetPrometheusResponse()
		require.True(t, ok)

		assert.Equal(t, statusSuccess, shardedPrometheusRes.GetStatus())
		downstream.AssertCalled(t, "Do", mock.Anything, mock.Anything)
		// we expect 128 calls to the downstream handler and not the original 16.
		downstream.AssertNumberOfCalls(t, "Do", 128)
	})
}

func TestQuerySharding_ShouldSupportMaxShardedQueries(t *testing.T) {
	tests := map[string]struct {
		query                         string
		hints                         *Hints
		totalShards                   int
		maxShardedQueries             int
		nativeHistograms              bool
		expectedShardsPerPartialQuery int
		compactorShards               int
	}{
		"query is not shardable": {
			query:                         "metric",
			hints:                         &Hints{TotalQueries: 1},
			totalShards:                   16,
			maxShardedQueries:             64,
			expectedShardsPerPartialQuery: 1,
		},
		"single splitted query, query has 1 shardable leg": {
			query:                         "sum(metric)",
			hints:                         &Hints{TotalQueries: 1},
			totalShards:                   16,
			maxShardedQueries:             64,
			expectedShardsPerPartialQuery: 16,
		},
		"single splitted query, query has many shardable legs": {
			query:                         "sum(metric_1) + sum(metric_2) + sum(metric_3) + sum(metric_4)",
			hints:                         &Hints{TotalQueries: 1},
			totalShards:                   16,
			maxShardedQueries:             16,
			expectedShardsPerPartialQuery: 4,
		},
		"multiple splitted queries, query has 1 shardable leg": {
			query:                         "sum(metric)",
			hints:                         &Hints{TotalQueries: 10},
			totalShards:                   16,
			maxShardedQueries:             64,
			expectedShardsPerPartialQuery: 6,
		},
		"multiple splitted queries, query has 2 shardable legs": {
			query:                         "sum(metric) / count(metric)",
			hints:                         &Hints{TotalQueries: 10},
			totalShards:                   16,
			maxShardedQueries:             64,
			expectedShardsPerPartialQuery: 3,
		},
		"multiple splitted queries, query has 2 shardable legs, no compactor shards": {
			query:                         "sum(metric) / count(metric)",
			hints:                         &Hints{TotalQueries: 3},
			totalShards:                   16,
			maxShardedQueries:             64,
			compactorShards:               0,
			expectedShardsPerPartialQuery: 10,
		},
		"multiple splitted queries, query has 2 shardable legs, 3 compactor shards": {
			query:                         "sum(metric) / count(metric)",
			hints:                         &Hints{TotalQueries: 3},
			totalShards:                   16,
			maxShardedQueries:             64,
			compactorShards:               3,
			expectedShardsPerPartialQuery: 9,
		},
		"multiple splitted queries, query has 2 shardable legs, 4 compactor shards": {
			query:                         "sum(metric) / count(metric)",
			hints:                         &Hints{TotalQueries: 3},
			totalShards:                   16,
			maxShardedQueries:             64,
			compactorShards:               4,
			expectedShardsPerPartialQuery: 8,
		},
		"multiple splitted queries, query has 2 shardable legs, 10 compactor shards": {
			query:                         "sum(metric) / count(metric)",
			hints:                         &Hints{TotalQueries: 3},
			totalShards:                   16,
			maxShardedQueries:             64,
			compactorShards:               10,
			expectedShardsPerPartialQuery: 10,
		},
		"multiple splitted queries, query has 2 shardable legs, 11 compactor shards": {
			query:                         "sum(metric) / count(metric)",
			hints:                         &Hints{TotalQueries: 3},
			totalShards:                   16,
			maxShardedQueries:             64,
			compactorShards:               11,
			expectedShardsPerPartialQuery: 10, // cannot be adjusted to make 11 multiple or divisible, keep original.
		},
		"multiple splitted queries, query has 2 shardable legs, 14 compactor shards": {
			query:                         "sum(metric) / count(metric)",
			hints:                         &Hints{TotalQueries: 3},
			totalShards:                   16,
			maxShardedQueries:             64,
			compactorShards:               14,
			expectedShardsPerPartialQuery: 7, // 7 divides 14
		},
		"query sharding is disabled": {
			query:                         "sum(metric)",
			hints:                         &Hints{TotalQueries: 1},
			totalShards:                   0, // Disabled.
			maxShardedQueries:             64,
			expectedShardsPerPartialQuery: 1,
		},
		"native histograms accepted": {
			query:                         "sum(metric) / count(metric)",
			hints:                         &Hints{TotalQueries: 3},
			totalShards:                   16,
			maxShardedQueries:             64,
			nativeHistograms:              true,
			compactorShards:               10,
			expectedShardsPerPartialQuery: 10,
		},
		"hints are missing, query has 1 shardable leg": {
			query:                         "count(metric)",
			hints:                         nil,
			totalShards:                   16,
			maxShardedQueries:             32,
			compactorShards:               8,
			expectedShardsPerPartialQuery: 16,
		},
		"hints are missing, query has 2 shardable legs": {
			query:                         "count(metric_1) or count(metric_2)",
			hints:                         nil,
			totalShards:                   16,
			maxShardedQueries:             32,
			compactorShards:               8,
			expectedShardsPerPartialQuery: 16, // 2 legs * 16 shards per query = 32 max sharded queries
		},
		"hints are missing, query has 4 shardable legs": {
			query:                         "count(metric_1) or count(metric_2) or count(metric_3) or count(metric_4)",
			hints:                         nil,
			totalShards:                   16,
			maxShardedQueries:             32,
			compactorShards:               8,
			expectedShardsPerPartialQuery: 8, // 4 legs * 8 shards per query = 32 max sharded queries
		},
		"total queries number is missing in hints, query has 1 shardable leg": {
			query:                         "count(metric)",
			hints:                         &Hints{},
			totalShards:                   16,
			maxShardedQueries:             32,
			compactorShards:               8,
			expectedShardsPerPartialQuery: 16,
		},
		"total queries number is missing in hints, query has 2 shardable legs": {
			query:                         "count(metric_1) or count(metric_2)",
			hints:                         &Hints{},
			totalShards:                   16,
			maxShardedQueries:             32,
			compactorShards:               8,
			expectedShardsPerPartialQuery: 16, // 2 legs * 16 shards per query = 32 max sharded queries
		},
		"total queries number is missing in hints, query has 4 shardable legs": {
			query:                         "count(metric_1) or count(metric_2) or count(metric_3) or count(metric_4)",
			hints:                         &Hints{},
			totalShards:                   16,
			maxShardedQueries:             32,
			compactorShards:               8,
			expectedShardsPerPartialQuery: 8, // 4 legs * 8 shards per query = 32 max sharded queries
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			runForEngines(t, func(t *testing.T, opts promql.EngineOpts, eng promql.QueryEngine) {
				req := &PrometheusRangeQueryRequest{
					path:      "/query_range",
					start:     util.TimeToMillis(start),
					end:       util.TimeToMillis(end),
					step:      step.Milliseconds(),
					queryExpr: parseQuery(t, testData.query),
					hints:     testData.hints,
				}

				limits := mockLimits{
					totalShards:                      testData.totalShards,
					maxShardedQueries:                testData.maxShardedQueries,
					compactorShards:                  testData.compactorShards,
					nativeHistogramsIngestionEnabled: testData.nativeHistograms,
				}

				shardingware := newQueryShardingMiddleware(log.NewNopLogger(), eng, limits, 0, nil)

				// Keep track of the unique number of shards queried to downstream.
				uniqueShardsMx := sync.Mutex{}
				uniqueShards := map[string]struct{}{}

				downstream := &mockHandler{}
				downstream.On("Do", mock.Anything, mock.Anything).Return(&PrometheusResponse{
					Status: statusSuccess, Data: &PrometheusData{
						ResultType: string(parser.ValueTypeVector),
					},
				}, nil).Run(func(args mock.Arguments) {
					req := args[1].(MetricsQueryRequest)
					reqShard := regexp.MustCompile(`__query_shard__="[^"]+"`).FindString(req.GetQuery())

					uniqueShardsMx.Lock()
					uniqueShards[reqShard] = struct{}{}
					uniqueShardsMx.Unlock()
				})

				res, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
				require.NoError(t, err)
				shardedPrometheusRes, ok := res.GetPrometheusResponse()
				require.True(t, ok)

				assert.Equal(t, statusSuccess, shardedPrometheusRes.GetStatus())
				assert.Equal(t, testData.expectedShardsPerPartialQuery, len(uniqueShards))
			})
		})
	}
}

func TestQuerySharding_ShouldSupportMaxRegexpSizeBytes(t *testing.T) {
	const (
		totalShards       = 16
		maxShardedQueries = 16
	)

	tests := map[string]struct {
		query              string
		maxRegexpSizeBytes int
		expectedShards     int
	}{
		"query is shardable and has no regexp matchers": {
			query:              `sum(metric{app="a-long-matcher-but-not-regexp"})`,
			maxRegexpSizeBytes: 10,
			expectedShards:     16,
		},
		"query is shardable and has short regexp matchers in vector selector": {
			query:              `sum(metric{app="test",namespace=~"short"})`,
			maxRegexpSizeBytes: 10,
			expectedShards:     16,
		},
		"query is shardable and has long regexp matchers in vector selector": {
			query:              `sum(metric{app="test",namespace=~"short",cluster!~"this-is-longer-than-limit"})`,
			maxRegexpSizeBytes: 10,
			expectedShards:     1,
		},
		"query is shardable, has long regexp matchers in vector selector but limit is disabled": {
			query:              `sum(metric{app="test",namespace=~"short",cluster!~"this-is-longer-than-limit"})`,
			maxRegexpSizeBytes: 0,
			expectedShards:     16,
		},
		"query is shardable and has short regexp matchers in matrix selector": {
			query:              `sum(sum_over_time(metric{app="test",namespace=~"short"}[5m]))`,
			maxRegexpSizeBytes: 10,
			expectedShards:     16,
		},
		"query is shardable and has long regexp matchers in matrix selector": {
			query:              `sum(sum_over_time(metric{app="test",namespace=~"short",cluster!~"this-is-longer-than-limit"}[5m]))`,
			maxRegexpSizeBytes: 10,
			expectedShards:     1,
		},
		"query is shardable, has long regexp matchers in matrix selector but limit is disabled": {
			query:              `sum(sum_over_time(metric{app="test",namespace=~"short",cluster!~"this-is-longer-than-limit"}[5m]))`,
			maxRegexpSizeBytes: 0,
			expectedShards:     16,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			runForEngines(t, func(t *testing.T, opts promql.EngineOpts, eng promql.QueryEngine) {

				req := &PrometheusRangeQueryRequest{
					path:      "/query_range",
					start:     util.TimeToMillis(start),
					end:       util.TimeToMillis(end),
					step:      step.Milliseconds(),
					queryExpr: parseQuery(t, testData.query),
				}

				limits := mockLimits{
					totalShards:                      totalShards,
					maxShardedQueries:                maxShardedQueries,
					maxRegexpSizeBytes:               testData.maxRegexpSizeBytes,
					compactorShards:                  0,
					nativeHistogramsIngestionEnabled: false,
				}

				shardingware := newQueryShardingMiddleware(log.NewNopLogger(), eng, limits, 0, nil)

				// Keep track of the unique number of shards queried to downstream.
				uniqueShardsMx := sync.Mutex{}
				uniqueShards := map[string]struct{}{}

				downstream := &mockHandler{}
				downstream.On("Do", mock.Anything, mock.Anything).Return(&PrometheusResponse{
					Status: statusSuccess, Data: &PrometheusData{
						ResultType: string(parser.ValueTypeVector),
					},
				}, nil).Run(func(args mock.Arguments) {
					req := args[1].(MetricsQueryRequest)
					reqShard := regexp.MustCompile(`__query_shard__="[^"]+"`).FindString(req.GetQuery())

					uniqueShardsMx.Lock()
					uniqueShards[reqShard] = struct{}{}
					uniqueShardsMx.Unlock()
				})

				res, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
				require.NoError(t, err)
				shardedPrometheusRes, ok := res.GetPrometheusResponse()
				require.True(t, ok)

				assert.Equal(t, statusSuccess, shardedPrometheusRes.GetStatus())
				assert.Equal(t, testData.expectedShards, len(uniqueShards))
			})
		})
	}
}

func TestQuerySharding_ShouldReturnErrorOnDownstreamHandlerFailure(t *testing.T) {
	req := &PrometheusRangeQueryRequest{
		path:      "/query_range",
		start:     util.TimeToMillis(start),
		end:       util.TimeToMillis(end),
		step:      step.Milliseconds(),
		queryExpr: parseQuery(t, "vector(1)"), // A non shardable query.
	}

	runForEngines(t, func(t *testing.T, opts promql.EngineOpts, eng promql.QueryEngine) {
		shardingware := newQueryShardingMiddleware(log.NewNopLogger(), eng, mockLimits{totalShards: 16}, 0, nil)

		// Mock the downstream handler to always return error.
		downstreamErr := errors.Errorf("some err")
		downstream := mockHandlerWith(nil, downstreamErr)

		// Run the query with sharding middleware wrapping the downstream one.
		// We expect to get the downstream error.
		_, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
		require.Error(t, err)
		assert.Equal(t, downstreamErr, err)
	})
}

func TestQuerySharding_ShouldReturnErrorInCorrectFormat(t *testing.T) {
	var (
		queryableInternalErr = storage.QueryableFunc(func(int64, int64) (storage.Querier, error) {
			return nil, apierror.New(apierror.TypeInternal, "some internal error")
		})
		queryablePrometheusExecErr = storage.QueryableFunc(func(int64, int64) (storage.Querier, error) {
			return nil, apierror.Newf(apierror.TypeExec, "expanding series: %s", querier.NewMaxQueryLengthError(744*time.Hour, 720*time.Hour))
		})
		queryable = testdatagen.StorageSeriesQueryable([]storage.Series{
			testdatagen.NewSeries(labels.FromStrings("__name__", "bar1"), start.Add(-lookbackDelta), end, step, testdatagen.Factor(5)),
		})
		queryableSlow = newMockShardedQueryable(
			2,
			2,
			[]string{"a", "b", "c"},
			1,
			time.Second,
		)
	)

	for _, tc := range []struct {
		name                 string
		skipMQE              bool
		engineDownstreamOpts []engineOpt
		engineShardingOpts   []engineOpt
		expError             *apierror.APIError
		queryable            storage.Queryable
	}{
		{
			name:                 "downstream - timeout",
			engineDownstreamOpts: []engineOpt{withTimeout(50 * time.Millisecond)},
			expError:             apierror.New(apierror.TypeTimeout, "query timed out in expression evaluation"),
			queryable:            queryableSlow,
		},
		{
			name:                 "downstream - sample limit",
			skipMQE:              true, // MQE doesn't have a sample limit
			engineDownstreamOpts: []engineOpt{withMaxSamples(1)},
			expError:             apierror.New(apierror.TypeExec, "query processing would load too many samples into memory in query execution"),
		},
		{
			name:               "sharding - timeout",
			engineShardingOpts: []engineOpt{withTimeout(50 * time.Millisecond)},
			expError:           apierror.New(apierror.TypeTimeout, "query timed out in expression evaluation"),
			queryable:          queryableSlow,
		},
		{
			name:      "downstream - storage internal error",
			queryable: queryableInternalErr,
			expError:  apierror.New(apierror.TypeInternal, "some internal error"),
		},
		{
			name:      "downstream - storage prometheus execution error",
			queryable: queryablePrometheusExecErr,
			expError:  apierror.Newf(apierror.TypeExec, "expanding series: %s", querier.NewMaxQueryLengthError(744*time.Hour, 720*time.Hour)),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, engineType := range []string{querier.PrometheusEngine, querier.MimirEngine} {
				t.Run(fmt.Sprintf("engine=%s", engineType), func(t *testing.T) {
					if tc.skipMQE && engineType == querier.MimirEngine {
						t.Skip(tc.name, " is not relevant for the Mimir Query Engine")
					}

					req := &PrometheusRangeQueryRequest{
						path:      "/query_range",
						start:     util.TimeToMillis(start),
						end:       util.TimeToMillis(end),
						step:      step.Milliseconds(),
						queryExpr: parseQuery(t, "sum(bar1)"),
					}

					_, engineSharding := newEngineForTesting(t, engineType, tc.engineShardingOpts...)
					_, engineDownstream := newEngineForTesting(t, engineType, tc.engineDownstreamOpts...)

					shardingware := newQueryShardingMiddleware(log.NewNopLogger(), engineSharding, mockLimits{totalShards: 3}, 0, nil)

					if tc.queryable == nil {
						tc.queryable = queryable
					}

					downstream := &downstreamHandler{
						engine:    engineDownstream,
						queryable: tc.queryable,
					}

					// Run the query with sharding middleware wrapping the downstream one.
					// We expect to get the downstream error.
					_, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
					if tc.expError == nil {
						require.NoError(t, err)
					} else {
						require.Error(t, err)

						// Convert the actual error to an APIError so that we can compare the error type.
						var apiErr *apierror.APIError
						require.ErrorAs(t, err, &apiErr)
						require.Equal(t, tc.expError.Type, apiErr.Type)

						expResp, ok := apierror.HTTPResponseFromError(tc.expError)
						require.True(t, ok, "expected error should be an api error")

						gotResp, ok := apierror.HTTPResponseFromError(err)
						require.True(t, ok, "got error should be an api error")

						// The Prometheus engine and MQE use different error messages in some cases. We don't
						// want to code the test to the exact error message being used at some particular time
						// so we only compare the HTTP status codes generated.
						assert.Equal(t, expResp.GetCode(), gotResp.GetCode())
					}

				})
			}
		})

	}
}

func TestQuerySharding_EngineErrorMapping(t *testing.T) {
	const (
		numSeries = 30
		numShards = 8
	)

	series := make([]storage.Series, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		series = append(series, testdatagen.NewSeries(testdatagen.NewTestCounterLabels(i), start.Add(-lookbackDelta), end, step, testdatagen.Factor(float64(i)*0.1)))
	}

	queryable := storage.QueryableFunc(func(int64, int64) (storage.Querier, error) {
		return &testdatagen.QuerierMock{Series: series}, nil
	})

	req := &PrometheusRangeQueryRequest{
		path:      "/query_range",
		start:     util.TimeToMillis(start),
		end:       util.TimeToMillis(end),
		step:      step.Milliseconds(),
		queryExpr: parseQuery(t, `sum by (group_1) (metric_counter) - on(group_1) group_right(unique) (sum by (group_1,unique) (metric_counter))`),
	}

	runForEngines(t, func(t *testing.T, opts promql.EngineOpts, eng promql.QueryEngine) {
		downstream := &downstreamHandler{engine: eng, queryable: queryable}
		reg := prometheus.NewPedanticRegistry()
		shardingware := newQueryShardingMiddleware(log.NewNopLogger(), eng, mockLimits{totalShards: numShards}, 0, reg)

		// Run the query with sharding.
		_, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
		assert.Equal(t, apierror.New(apierror.TypeExec, "multiple matches for labels: grouping labels must ensure unique matches"), err)
	})
}

func TestQuerySharding_WrapMultipleTime(t *testing.T) {
	req := &PrometheusRangeQueryRequest{
		path:      "/query_range",
		start:     util.TimeToMillis(start),
		end:       util.TimeToMillis(end),
		step:      step.Milliseconds(),
		queryExpr: parseQuery(t, "vector(1)"), // A non shardable query.
	}

	runForEngines(t, func(t *testing.T, opts promql.EngineOpts, eng promql.QueryEngine) {
		shardingware := newQueryShardingMiddleware(log.NewNopLogger(), eng, mockLimits{totalShards: 16}, 0, prometheus.NewRegistry())

		require.NotPanics(t, func() {
			_, err := shardingware.Wrap(mockHandlerWith(nil, nil)).Do(user.InjectOrgID(context.Background(), "test"), req)
			require.Nil(t, err)
			_, err = shardingware.Wrap(mockHandlerWith(nil, nil)).Do(user.InjectOrgID(context.Background(), "test"), req)
			require.Nil(t, err)
		})
	})
}

func TestQuerySharding_ShouldUseCardinalityEstimate(t *testing.T) {
	req := &PrometheusInstantQueryRequest{
		time:      util.TimeToMillis(start),
		queryExpr: parseQuery(t, "sum by (foo) (rate(bar{}[1m]))"), // shardable query.
	}

	tests := []struct {
		name          string
		req           MetricsQueryRequest
		expectedCalls int
	}{
		{
			"range query",
			mustSucceed(mustSucceed(req.WithStartEnd(util.TimeToMillis(start), util.TimeToMillis(end))).WithEstimatedSeriesCountHint(55_000)),
			6,
		},
		{
			"instant query",
			mustSucceed(req.WithEstimatedSeriesCountHint(29_000)),
			3,
		},
		{
			"no hints",
			req,
			16,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runForEngines(t, func(t *testing.T, opts promql.EngineOpts, eng promql.QueryEngine) {
				shardingware := newQueryShardingMiddleware(log.NewNopLogger(), eng, mockLimits{totalShards: 16}, 10_000, nil)
				downstream := &mockHandler{}
				downstream.On("Do", mock.Anything, mock.Anything).Return(&PrometheusResponse{
					Status: statusSuccess, Data: &PrometheusData{
						ResultType: string(parser.ValueTypeVector),
					},
				}, nil)

				res, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), tt.req)
				require.NoError(t, err)
				shardedPrometheusRes, ok := res.GetPrometheusResponse()
				require.True(t, ok)

				assert.Equal(t, statusSuccess, shardedPrometheusRes.GetStatus())
				downstream.AssertCalled(t, "Do", mock.Anything, mock.Anything)
				downstream.AssertNumberOfCalls(t, "Do", tt.expectedCalls)
			})
		})
	}
}

// TODO: share this with MQE
func TestQuerySharding_Annotations(t *testing.T) {
	numSeries := 10
	endTime := 100
	storageSeries := make([]storage.Series, 0, numSeries)
	floats := make([]promql.FPoint, 0, endTime)
	for i := 0; i < endTime; i++ {
		floats = append(floats, promql.FPoint{
			T: int64(i * 1000),
			F: float64(i),
		})
	}
	histograms := make([]promql.HPoint, 0)
	seriesName := `test_float`
	for i := 0; i < numSeries; i++ {
		nss := promql.NewStorageSeries(promql.Series{
			Metric:     labels.FromStrings("__name__", seriesName, "series", fmt.Sprint(i)),
			Floats:     floats,
			Histograms: histograms,
		})
		storageSeries = append(storageSeries, nss)
	}
	queryable := testdatagen.StorageSeriesQueryable(storageSeries)

	const numShards = 8
	const step = 20 * time.Second
	const splitInterval = 15 * time.Second

	type template struct {
		query     string
		isWarning bool
		isSharded bool
	}

	templates := []template{
		{
			query:     "quantile(10, %s)",
			isWarning: true,
		},
		{
			query:     "quantile(10, sum(%s))",
			isWarning: true,
			isSharded: true,
		},
		{
			query:     "rate(%s[1m])",
			isWarning: false,
		},
		{
			query:     "increase(%s[1m])",
			isWarning: false,
		},
		{
			query:     "sum(rate(%s[1m]))",
			isWarning: false,
			isSharded: true,
		},
	}
	for _, template := range templates {
		t.Run(template.query, func(t *testing.T) {
			runForEngines(t, func(t *testing.T, opts promql.EngineOpts, eng promql.QueryEngine) {
				reg := prometheus.NewPedanticRegistry()
				shardingware := newQueryShardingMiddleware(log.NewNopLogger(), eng, mockLimits{totalShards: numShards}, 0, reg)
				splitware := newSplitAndCacheMiddleware(
					true,
					false, // Cache disabled.
					splitInterval,
					mockLimits{},
					newTestCodec(),
					nil,
					DefaultCacheKeyGenerator{interval: day},
					nil,
					nil,
					log.NewNopLogger(),
					reg,
				)
				downstream := &downstreamHandler{
					engine:                                  eng,
					queryable:                               queryable,
					includePositionInformationInAnnotations: true,
				}

				query := fmt.Sprintf(template.query, seriesName)
				req := &PrometheusRangeQueryRequest{
					path:      "/query_range",
					start:     0,
					end:       int64(endTime * 1000),
					step:      step.Milliseconds(),
					queryExpr: parseQuery(t, query),
				}

				injectedContext := user.InjectOrgID(context.Background(), "test")

				// Run the query without sharding.
				expectedRes, err := downstream.Do(injectedContext, req)
				require.Nil(t, err)
				expectedPrometheusRes, ok := expectedRes.GetPrometheusResponse()
				require.True(t, ok)

				// Ensure the query produces some results.
				require.NotEmpty(t, expectedPrometheusRes.Data.Result)

				// Run the query with sharding.
				shardedRes, err := shardingware.Wrap(downstream).Do(injectedContext, req)
				require.Nil(t, err)
				shardedPrometheusRes, ok := shardedRes.GetPrometheusResponse()
				require.True(t, ok)

				// Ensure the query produces some results.
				require.NotEmpty(t, shardedPrometheusRes.Data.Result)

				// Run the query with splitting.
				splitRes, err := splitware.Wrap(downstream).Do(injectedContext, req)
				require.Nil(t, err)
				splitPrometheusRes, ok := splitRes.GetPrometheusResponse()
				require.True(t, ok)

				// Ensure the query produces some results.
				require.NotEmpty(t, splitPrometheusRes.Data.Result)

				expected := expectedPrometheusRes.Infos
				actualSharded := shardedPrometheusRes.Infos
				actualSplit := splitPrometheusRes.Infos

				if template.isWarning {
					expected = expectedPrometheusRes.Warnings
					actualSharded = shardedPrometheusRes.Warnings
					actualSplit = splitPrometheusRes.Warnings
				}

				require.NotEmpty(t, expected)
				require.Equal(t, expected, actualSplit)

				if template.isSharded {
					// Remove position information from annotations generated with the unsharded query, to mirror what we expect from the sharded query.
					removeAllAnnotationPositionInformation(expected)
				}

				require.Equal(t, expected, actualSharded)
			})
		})
	}
}

func BenchmarkQuerySharding(b *testing.B) {
	var shards []int

	// max out at half available cpu cores in order to minimize noisy neighbor issues while benchmarking
	for shard := 1; shard <= runtime.NumCPU()/2; shard = shard * 2 {
		shards = append(shards, shard)
	}

	for _, tc := range []struct {
		labelBuckets     int
		labels           []string
		samplesPerSeries int
		histsPerSeries   int
		query            string
		desc             string
	}{
		// Ensure you have enough cores to run these tests without blocking.
		// We want to simulate parallel computations and waiting in queue doesn't help

		// no-group
		{
			labelBuckets:     16,
			labels:           []string{"a", "b", "c"},
			samplesPerSeries: 100,
			histsPerSeries:   30,
			query:            `sum(rate(http_requests_total[5m]))`,
			desc:             "sum nogroup",
		},
		// sum by
		{
			labelBuckets:     16,
			labels:           []string{"a", "b", "c"},
			samplesPerSeries: 100,
			histsPerSeries:   30,
			query:            `sum by(a) (rate(http_requests_total[5m]))`,
			desc:             "sum by",
		},
		// sum without
		{
			labelBuckets:     16,
			labels:           []string{"a", "b", "c"},
			samplesPerSeries: 100,
			histsPerSeries:   30,
			query:            `sum without (a) (rate(http_requests_total[5m]))`,
			desc:             "sum without",
		},
	} {
		for _, delayPerSeries := range []time.Duration{
			0,
			time.Millisecond / 10,
		} {
			engine := promql.NewEngine(promql.EngineOpts{
				Logger:               promslog.NewNopLogger(),
				Reg:                  nil,
				MaxSamples:           100000000,
				Timeout:              time.Minute,
				EnableNegativeOffset: true,
				EnableAtModifier:     true,
			})

			queryable := newMockShardedQueryable(
				tc.samplesPerSeries,
				tc.histsPerSeries,
				tc.labels,
				tc.labelBuckets,
				delayPerSeries,
			)
			downstream := &downstreamHandler{
				engine:    engine,
				queryable: queryable,
			}

			var (
				start = int64(0)
				end   = int64(1000 * tc.samplesPerSeries)
				step  = (end - start) / 1000
			)

			req := &PrometheusRangeQueryRequest{
				path:      "/query_range",
				start:     start,
				end:       end,
				step:      step,
				queryExpr: parseQuery(b, tc.query),
			}

			for _, shardFactor := range shards {
				shardingware := newQueryShardingMiddleware(
					log.NewNopLogger(),
					engine,
					mockLimits{totalShards: shardFactor},
					0,
					nil,
				).Wrap(downstream)

				b.Run(
					fmt.Sprintf(
						"desc:[%s]---shards:[%d]---series:[%.0f]---delayPerSeries:[%s]---samplesPerSeries:[%d]---histsPerSeries:[%d]",
						tc.desc,
						shardFactor,
						math.Pow(float64(tc.labelBuckets), float64(len(tc.labels))),
						delayPerSeries,
						tc.samplesPerSeries,
						tc.histsPerSeries,
					),
					func(b *testing.B) {
						for n := 0; n < b.N; n++ {
							_, err := shardingware.Do(
								user.InjectOrgID(context.Background(), "test"),
								req,
							)
							if err != nil {
								b.Fatal(err.Error())
							}
						}
					},
				)
			}
			fmt.Println()
		}

		fmt.Print("--------------------------------\n\n")
	}
}

func BenchmarkQueryShardingRewriting(b *testing.B) {
	testCases := []string{
		`quantile(0.9,foo)`,
		`absent(foo)`,
		`absent_over_time(foo[1m])`,
		`histogram_quantile(0.5, rate(bar1{baz="blip"}[30s]))`,
		`sum by (foo) (histogram_quantile(0.9, rate(http_request_duration_seconds_bucket[10m])))`,
		`sum by (foo,bar) (min_over_time(bar1{baz="blip"}[1m]))`,
		`sum(rate(bar1[1m])) or rate(bar2[1m])`,
		`sum(rate(bar1[1m])) or sum(rate(bar2[1m]))`,
		`histogram_quantile(0.5, sum(rate(cortex_cache_value_size_bytes_bucket[5m])) by (le))`,
		`sum(
				  count(
				    count(
				      bar1
				    )  by (drive,instance)
				  )  by (instance)
				)`,
		`sum(rate(foo[1m]))`,
		`count(rate(foo[1m]))`,
		`count(up)`,
		`avg(count(test))`,
		`count by (foo) (rate(foo[1m]))`,
		`count without (foo) (rate(foo[1m]))`,
		`max(rate(foo[1m]))`,
		`max by (foo) (rate(foo[1m]))`,
		`max without (foo) (rate(foo[1m]))`,
		`sum by (foo) (rate(foo[1m]))`,
		`sum without (foo) (rate(foo[1m]))`,
		`avg without (foo) (rate(foo[1m]))`,
		`avg by (foo) (rate(foo[1m]))`,
		`avg(rate(foo[1m]))`,
		`topk(10,avg by (foo)(rate(foo[1m])))`,
		`min_over_time(metric_counter[5m])`,
		`sum by (user, cluster, namespace) (quantile_over_time(0.99, cortex_ingester_active_series[7d]))`,
		`min_over_time(
				sum by(group_1) (
					rate(metric_counter[5m])
				)[10m:2m]
			)`,
		`max_over_time(
				stddev_over_time(
					deriv(
						rate(metric_counter[10m])
					[5m:1m])
				[2m:])
			[10m:])`,
		`max_over_time((
				stddev_over_time(
					deriv(
						rate(metric_counter[10m])
					[5m:1m])
				[2m:])
			[10m:]))`,
		`rate(
				sum by(group_1) (
					rate(metric_counter[5m])
				)[10m:]
			)`,
		`absent_over_time(rate(metric_counter[5m])[10m:])`,
		`max_over_time(
				stddev_over_time(
					deriv(
						sort(metric_counter)
					[5m:1m])
				[2m:])
			[10m:])`,
		`max_over_time(
				absent_over_time(
					deriv(
						rate(metric_counter[1m])
					[5m:1m])
				[2m:])
			[10m:])`,
		`quantile_over_time(0.99, cortex_ingester_active_series[1w])`,
		`ceil(sum by (foo) (rate(cortex_ingester_active_series[1w])))`,
		`ln(bar) - resets(foo[1d])`,
		`predict_linear(foo[10m],3600)`,
		`label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")`,
		`ln(exp(label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")))`,
		`ln(
				label_replace(
					sum by (cluster) (up{job="api-server",service="a:c"})
				, "foo", "$1", "service", "(.*):.*")
			)`,
		`sum by (job)(rate(http_requests_total[1h] @ end()))`,
		`sum by (job)(rate(http_requests_total[1h] offset 1w @ 10))`,
		`sum by (job)(rate(http_requests_total[1h] offset 1w @ 10)) / 2`,
		`sum by (job)(rate(http_requests_total[1h] offset 1w @ 10)) / 2 ^ 2`,
		`sum by (group_1) (rate(metric_counter[1m]) / time() * 2)`,
		`sum by (group_1) (rate(metric_counter[1m])) / time() *2`,
		`sum by (group_1) (rate(metric_counter[1m])) / time()`,
		`sum by (group_1) (rate(metric_counter[1m])) / vector(3) ^ month()`,
		`vector(3) ^ month()`,
		`sum(rate(metric_counter[1m])) / vector(3) ^ year(foo)`,
		`foo > bar`,
		`foo * 2`,
		`foo > 0`,
		`0 <= foo`,
		`foo > (2 * 2)`,
		`sum by (label) (foo > 0)`,
		`sum by (label) (foo > 0) > 0`,
		`sum_over_time(foo[1m]) > (2 * 2)`,
		`foo > sum(bar)`,
		`foo > scalar(sum(bar))`,
		`scalar(min(foo)) > bool scalar(sum(bar))`,
		`foo * on(a, b) group_left(c) avg by(a, b, c) (bar)`,
		`vector(1) > 0 and vector(1)`,
		`sum(foo) > 0 and vector(1)`,
		`max by(pod) (
                    max without(prometheus_replica, instance, node) (kube_pod_labels{namespace="test"})
                    *
                    on(cluster, pod, namespace) group_right() pod:container_cpu_usage:sum
            )`,
		`sum(rate(metric[1m])) and max(metric) > 0`,
		`sum(rate(metric[1m])) > avg(rate(metric[1m]))`,
		`group by (a, b) (metric)`,
		`count by (a) (group by (a, b) (metric))`,
		`count(group without () ({namespace="foo"}))`,
		`label_replace(vector(1), "label_0", "value_0", "", "") or label_replace(vector(1), "label_1", "value_1", "", "") or label_replace(vector(1), "label_2", "value_2", "", "") or label_replace(vector(1), "label_3", "value_3", "", "") or label_replace(vector(1), "label_4", "value_4", "", "") or label_replace(vector(1), "label_5", "value_5", "", "") or label_replace(vector(1), "label_6", "value_6", "", "") or label_replace(vector(1), "label_7", "value_7", "", "") or label_replace(vector(1), "label_8", "value_8", "", "") or label_replace(vector(1), "label_9", "value_9", "", "") or label_replace(vector(1), "label_10", "value_10", "", "") or label_replace(vector(1), "label_11", "value_11", "", "") or label_replace(vector(1), "label_12", "value_12", "", "") or label_replace(vector(1), "label_13", "value_13", "", "") or label_replace(vector(1), "label_14", "value_14", "", "") or label_replace(vector(1), "label_15", "value_15", "", "") or label_replace(vector(1), "label_16", "value_16", "", "") or label_replace(vector(1), "label_17", "value_17", "", "") or label_replace(vector(1), "label_18", "value_18", "", "") or label_replace(vector(1), "label_19", "value_19", "", "") or label_replace(vector(1), "label_20", "value_20", "", "") or label_replace(vector(1), "label_21", "value_21", "", "") or label_replace(vector(1), "label_22", "value_22", "", "") or label_replace(vector(1), "label_23", "value_23", "", "") or label_replace(vector(1), "label_24", "value_24", "", "") or label_replace(vector(1), "label_25", "value_25", "", "") or label_replace(vector(1), "label_26", "value_26", "", "") or label_replace(vector(1), "label_27", "value_27", "", "") or label_replace(vector(1), "label_28", "value_28", "", "") or label_replace(vector(1), "label_29", "value_29", "", "") or label_replace(vector(1), "label_30", "value_30", "", "") or label_replace(vector(1), "label_31", "value_31", "", "") or label_replace(vector(1), "label_32", "value_32", "", "") or label_replace(vector(1), "label_33", "value_33", "", "") or label_replace(vector(1), "label_34", "value_34", "", "") or label_replace(vector(1), "label_35", "value_35", "", "") or label_replace(vector(1), "label_36", "value_36", "", "") or label_replace(vector(1), "label_37", "value_37", "", "") or label_replace(vector(1), "label_38", "value_38", "", "") or label_replace(vector(1), "label_39", "value_39", "", "") or label_replace(vector(1), "label_40", "value_40", "", "") or label_replace(vector(1), "label_41", "value_41", "", "") or label_replace(vector(1), "label_42", "value_42", "", "") or label_replace(vector(1), "label_43", "value_43", "", "") or label_replace(vector(1), "label_44", "value_44", "", "") or label_replace(vector(1), "label_45", "value_45", "", "") or label_replace(vector(1), "label_46", "value_46", "", "") or label_replace(vector(1), "label_47", "value_47", "", "") or label_replace(vector(1), "label_48", "value_48", "", "") or label_replace(vector(1), "label_49", "value_49", "", "") or label_replace(vector(1), "label_50", "value_50", "", "") or label_replace(vector(1), "label_51", "value_51", "", "") or label_replace(vector(1), "label_52", "value_52", "", "") or label_replace(vector(1), "label_53", "value_53", "", "") or label_replace(vector(1), "label_54", "value_54", "", "") or label_replace(vector(1), "label_55", "value_55", "", "") or label_replace(vector(1), "label_56", "value_56", "", "") or label_replace(vector(1), "label_57", "value_57", "", "") or label_replace(vector(1), "label_58", "value_58", "", "") or label_replace(vector(1), "label_59", "value_59", "", "") or label_replace(vector(1), "label_60", "value_60", "", "") or label_replace(vector(1), "label_61", "value_61", "", "") or label_replace(vector(1), "label_62", "value_62", "", "") or label_replace(vector(1), "label_63", "value_63", "", "") or label_replace(vector(1), "label_64", "value_64", "", "") or label_replace(vector(1), "label_65", "value_65", "", "") or label_replace(vector(1), "label_66", "value_66", "", "") or label_replace(vector(1), "label_67", "value_67", "", "") or label_replace(vector(1), "label_68", "value_68", "", "") or label_replace(vector(1), "label_69", "value_69", "", "") or label_replace(vector(1), "label_70", "value_70", "", "") or label_replace(vector(1), "label_71", "value_71", "", "") or label_replace(vector(1), "label_72", "value_72", "", "") or label_replace(vector(1), "label_73", "value_73", "", "") or label_replace(vector(1), "label_74", "value_74", "", "") or label_replace(vector(1), "label_75", "value_75", "", "") or label_replace(vector(1), "label_76", "value_76", "", "") or label_replace(vector(1), "label_77", "value_77", "", "") or label_replace(vector(1), "label_78", "value_78", "", "") or label_replace(vector(1), "label_79", "value_79", "", "") or label_replace(vector(1), "label_80", "value_80", "", "") or label_replace(vector(1), "label_81", "value_81", "", "") or label_replace(vector(1), "label_82", "value_82", "", "") or label_replace(vector(1), "label_83", "value_83", "", "") or label_replace(vector(1), "label_84", "value_84", "", "") or label_replace(vector(1), "label_85", "value_85", "", "") or label_replace(vector(1), "label_86", "value_86", "", "") or label_replace(vector(1), "label_87", "value_87", "", "") or label_replace(vector(1), "label_88", "value_88", "", "") or label_replace(vector(1), "label_89", "value_89", "", "") or label_replace(vector(1), "label_90", "value_90", "", "") or label_replace(vector(1), "label_91", "value_91", "", "") or label_replace(vector(1), "label_92", "value_92", "", "") or label_replace(vector(1), "label_93", "value_93", "", "") or label_replace(vector(1), "label_94", "value_94", "", "") or label_replace(vector(1), "label_95", "value_95", "", "") or label_replace(vector(1), "label_96", "value_96", "", "") or label_replace(vector(1), "label_97", "value_97", "", "") or label_replace(vector(1), "label_98", "value_98", "", "") or label_replace(vector(1), "label_99", "value_99", "", "") > 0`,
	}

	limits := mockLimits{totalShards: 3}
	reg := prometheus.NewPedanticRegistry()
	sharder := NewQuerySharder(astmapper.EmbeddedQueriesSquasher, limits, 0, reg, log.NewNopLogger())
	tenants := []string{"tenant-1"}
	ctx := context.Background()

	for _, expr := range testCases {
		b.Run(expr, func(b *testing.B) {
			for b.Loop() {
				// We must parse the expression each time as shard() mutates the expression.
				parsedExpr, err := parser.ParseExpr(expr)
				if err != nil {
					require.NoError(b, err)
				}

				_, err = sharder.Shard(ctx, tenants, parsedExpr, 0, nil, 1)
				if err != nil {
					require.NoError(b, err)
				}
			}
		})
	}
}

func TestPromqlResultToSampleStreams(t *testing.T) {
	var testExpr = []struct {
		input    *promql.Result
		err      bool
		expected []SampleStream
	}{
		// error
		{
			input: &promql.Result{Err: errors.New("foo")},
			err:   true,
		},
		// String
		{
			input: &promql.Result{Value: promql.String{T: 1, V: "hi"}},
			expected: []SampleStream{
				{
					Labels: []mimirpb.LabelAdapter{
						{
							Name:  "value",
							Value: "hi",
						},
					},
					Samples: []mimirpb.Sample{
						{
							TimestampMs: 1,
						},
					},
				},
			},
		},
		// Scalar
		{
			input: &promql.Result{Value: promql.Scalar{T: 1, V: 1}},
			err:   false,
			expected: []SampleStream{
				{
					Samples: []mimirpb.Sample{
						{
							Value:       1,
							TimestampMs: 1,
						},
					},
				},
			},
		},
		// Vector
		{
			input: &promql.Result{
				Value: promql.Vector{
					promql.Sample{
						T:      1,
						F:      1,
						Metric: labels.FromStrings("a", "a1", "b", "b1"),
					},
					promql.Sample{
						T:      2,
						F:      2,
						Metric: labels.FromStrings("a", "a2", "b", "b2"),
					},
				},
			},
			err: false,
			expected: []SampleStream{
				{
					Labels: []mimirpb.LabelAdapter{
						{Name: "a", Value: "a1"},
						{Name: "b", Value: "b1"},
					},
					Samples: []mimirpb.Sample{
						{
							Value:       1,
							TimestampMs: 1,
						},
					},
				},
				{
					Labels: []mimirpb.LabelAdapter{
						{Name: "a", Value: "a2"},
						{Name: "b", Value: "b2"},
					},
					Samples: []mimirpb.Sample{
						{
							Value:       2,
							TimestampMs: 2,
						},
					},
				},
			},
		},
		// Matrix
		{
			input: &promql.Result{
				Value: promql.Matrix{
					{
						Metric: labels.FromStrings("a", "a1", "b", "b1"),
						Floats: []promql.FPoint{
							{T: 1, F: 1},
							{T: 2, F: 2},
						},
					},
					{
						Metric: labels.FromStrings("a", "a2", "b", "b2"),
						Floats: []promql.FPoint{
							{T: 1, F: 8},
							{T: 2, F: 9},
						},
					},
				},
			},
			err: false,
			expected: []SampleStream{
				{
					Labels: []mimirpb.LabelAdapter{
						{Name: "a", Value: "a1"},
						{Name: "b", Value: "b1"},
					},
					Samples: []mimirpb.Sample{
						{
							Value:       1,
							TimestampMs: 1,
						},
						{
							Value:       2,
							TimestampMs: 2,
						},
					},
				},
				{
					Labels: []mimirpb.LabelAdapter{
						{Name: "a", Value: "a2"},
						{Name: "b", Value: "b2"},
					},
					Samples: []mimirpb.Sample{
						{
							Value:       8,
							TimestampMs: 1,
						},
						{
							Value:       9,
							TimestampMs: 2,
						},
					},
				},
			},
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			result, err := promqlResultToSamples(c.input)
			if c.err {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				require.Equal(t, c.expected, result)
			}
		})
	}
}

func TestLongestRegexpMatcherBytes(t *testing.T) {
	tests := map[string]struct {
		expr     string
		expected int
	}{
		"should return 0 if the query has no vector selectors": {
			expr:     "1",
			expected: 0,
		},
		"should return 0 if the query has regexp matchers": {
			expr:     `count(metric{app="test"})`,
			expected: 0,
		},
		"should return the longest regexp matcher for a query with vector selectors": {
			expr:     `avg(metric{app="test",namespace=~"short"}) / count(metric{app="very-very-long-but-ignored",namespace!~"longest-regexp"})`,
			expected: 14,
		},
		"should return the longest regexp matcher for a query with matrix selectors": {
			expr:     `avg_over_time(metric{app="test",namespace!~"short"}[5m]) / count_over_time(metric{app="very-very-long-but-ignored",namespace=~"longest-regexp"}[5m])`,
			expected: 14,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			parsed, err := parser.ParseExpr(testData.expr)
			require.NoError(t, err)

			actual := longestRegexpMatcherBytes(parsed)
			assert.Equal(t, testData.expected, actual)
		})
	}
}

type downstreamHandler struct {
	engine                                  promql.QueryEngine
	queryable                               storage.Queryable
	includePositionInformationInAnnotations bool
}

func (h *downstreamHandler) Do(ctx context.Context, r MetricsQueryRequest) (Response, error) {
	qry, err := newQuery(ctx, r, h.engine, h.queryable)
	if err != nil {
		return nil, err
	}

	res := qry.Exec(ctx)
	extracted, err := promqlResultToSamples(res)
	if err != nil {
		return nil, err
	}

	resp := &PrometheusResponse{
		Status: statusSuccess,
		Data: &PrometheusData{
			ResultType: string(res.Value.Type()),
			Result:     extracted,
		},
	}

	qs := ""

	if h.includePositionInformationInAnnotations {
		qs = r.GetQuery()
	}

	warnings, infos := res.Warnings.AsStrings(qs, 0, 0)
	if len(warnings) > 0 {
		resp.Warnings = warnings
	}
	if len(infos) > 0 {
		resp.Infos = infos
	}
	return resp, nil
}

// constant returns a generator that generates a constant value
func constant(value float64) testdatagen.Generator {
	return func(int64) float64 {
		return value
	}
}

func TestRemoveAnnotationPositionInformation(t *testing.T) {
	testCases := map[string]string{
		"":                "",
		"foo":             "foo",
		"foo (1:1)":       "foo",
		"foo (123:456)":   "foo",
		"foo (1:1) (2:2)": "foo (1:1)",
		"foo (1:1":        "foo (1:1",
		"foo (1:":         "foo (1:",
		"foo (1":          "foo (1",
		"foo (":           "foo (",
		"foo(1:1)":        "foo(1:1)",
	}

	for input, expectedOutput := range testCases {
		t.Run(input, func(t *testing.T) {
			require.Equal(t, expectedOutput, removeAnnotationPositionInformation(input))
		})
	}
}

func TestMapEngineError(t *testing.T) {
	type testCase struct {
		err      error
		expected error
	}

	testCases := map[string]testCase{
		"already APIError": {
			err:      apierror.New(apierror.TypeNotFound, "not found"),
			expected: apierror.New(apierror.TypeNotFound, "not found"),
		},
		"context canceled": {
			err:      context.Canceled,
			expected: apierror.New(apierror.TypeCanceled, "context canceled"),
		},
		"context canceled wrapped": {
			err:      fmt.Errorf("%w: oh no", context.Canceled),
			expected: apierror.New(apierror.TypeCanceled, "context canceled"),
		},
		"deadline exceeded": {
			err:      context.DeadlineExceeded,
			expected: apierror.New(apierror.TypeTimeout, "context deadline exceeded"),
		},
		"deadline exceeded wrapped": {
			err:      fmt.Errorf("%w: oh no", context.DeadlineExceeded),
			expected: apierror.New(apierror.TypeTimeout, "context deadline exceeded"),
		},
		"promql canceled": {
			err:      promql.ErrQueryCanceled("something"),
			expected: apierror.New(apierror.TypeCanceled, "query was canceled in something"),
		},
		"promql canceled wrapped": {
			err:      fmt.Errorf("%w: oh no", promql.ErrQueryCanceled("something")),
			expected: apierror.New(apierror.TypeCanceled, "query was canceled in something"),
		},
		"promql timeout": {
			err:      promql.ErrQueryTimeout("something"),
			expected: apierror.New(apierror.TypeTimeout, "query timed out in something"),
		},
		"promql timeout wrapped": {
			err:      fmt.Errorf("%w: oh no", promql.ErrQueryTimeout("something")),
			expected: apierror.New(apierror.TypeTimeout, "query timed out in something"),
		},
		"promql storage": {
			err:      promql.ErrStorage{Err: errors.New("storage")},
			expected: apierror.New(apierror.TypeInternal, "storage"),
		},
		"promql storage wrapped": {
			err:      fmt.Errorf("%w: oh no", promql.ErrStorage{Err: errors.New("storage")}),
			expected: apierror.New(apierror.TypeInternal, "storage"),
		},
		"promql too many samples": {
			err:      promql.ErrTooManySamples("something"),
			expected: apierror.New(apierror.TypeExec, "query processing would load too many samples into memory in something"),
		},
		"promql too many samples wrapped": {
			err:      fmt.Errorf("%w: oh no", promql.ErrTooManySamples("something")),
			expected: apierror.New(apierror.TypeExec, "query processing would load too many samples into memory in something"),
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			res := mapEngineError(tc.err)
			require.Equal(t, res, tc.expected)
		})
	}
}

// TestConflictingCounterResets verifies the expected handling of sharded histogram aggregates
// when counter-reset hints conflict.
//
// When PromQL evaluates sum() or avg() over histograms, it inspects the histogram counter reset
// hints for each series in the aggregation group. If both histogram.CounterReset and
// histogram.NotCounterReset appear within the same group, PromQL emits a
// "conflicting counter resets during histogram aggregation" warning.
//
// Whenever histogram_count() is used, the PromQL engine enables the histogram_stats_iterator.
// This iterator inflates histogram samples and updates their CounterResetHint values.
//
// When a query like:
//
//	histogram_count(sum(rate(<hist>)))
//
// is sharded in the Mimir query-frontend, the inner `sum(rate(...))` portion runs once per shard,
// and the results are merged back together. Because the outer expression is `histogram_count(...)`,
// the histogram_stats_iterator is applied over the *already rate-adjusted* histograms.
//
// This is incorrect: rate() has already compensated for all counter resets, and the histograms
// produced by rate() must *not* have their CounterResetHint rewritten. Instead, the histograms
// returned by rate() must have their CounterResetHint explicitly set to histogram.GaugeType.
// This prevents the outer sum() or avg() from detecting false counter-reset conflicts.
//
// This test sets up a scenario where the query is intentionally sharded by the query-frontend
// to validate that the correct (GaugeType) hints prevent spurious reset-conflict warnings.
//
// Note that this test will fail until these are vendored in.
// https://github.com/prometheus/prometheus/pull/17608 and https://github.com/grafana/mimir/pull/13676
func TestConflictingCounterResets(t *testing.T) {

	queries := []string{
		`histogram_count(sum(rate(metric[1m1s])) by (foo))`,
		`histogram_count(avg(rate(metric[1m1s])) by (foo))`,
	}

	// create 2 histogram series. One has counter resets
	series := make([]storage.Series, 0, 2)
	values1 := []float64{0, 5, 12, 15, 20, 25}
	values2 := []float64{0, 10, 0, 1, 3, 5}
	gen1 := func(ts int64) float64 {
		return values1[ts/60000]
	}

	gen2 := func(ts int64) float64 {
		return values2[ts/60000]
	}

	series = append(series, testdatagen.NewNativeHistogramSeries(labels.FromStrings("__name__", "metric", "sample", "3", "foo", "x"), time.UnixMilli(0), time.UnixMilli(1000*60*5), time.Minute, gen1))
	series = append(series, testdatagen.NewNativeHistogramSeries(labels.FromStrings("__name__", "metric", "sample", "2", "foo", "x"), time.UnixMilli(0), time.UnixMilli(1000*60*5), time.Minute, gen2))

	shardingAwareQueryable := testdatagen.StorageSeriesQueryable(series)

	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			runForEngines(t, func(t *testing.T, opts promql.EngineOpts, eng promql.QueryEngine) {

				downstream := &downstreamHandler{
					engine:    eng,
					queryable: shardingAwareQueryable,
				}

				req := &PrometheusRangeQueryRequest{
					path:      "/query_range",
					start:     0,
					end:       60 * 6 * 1000,
					step:      1000,
					queryExpr: parseQuery(t, query),
				}

				// Run the query without sharding.
				expectedRes, err := downstream.Do(context.Background(), req)
				require.Nil(t, err)

				expectedPrometheusRes, ok := expectedRes.GetPrometheusResponse()
				require.True(t, ok)
				sort.Sort(byLabels(expectedPrometheusRes.Data.Result))

				// Ensure the query produces some results.
				require.NotEmpty(t, expectedPrometheusRes.Data.Result)
				requireValidSamples(t, expectedPrometheusRes.Data.Result)

				require.Len(t, expectedPrometheusRes.GetWarnings(), 0)

				for _, numShards := range []int{8, 16} {
					t.Run(fmt.Sprintf("shards=%d", numShards), func(t *testing.T) {
						reg := prometheus.NewPedanticRegistry()
						shardingware := newQueryShardingMiddleware(
							log.NewNopLogger(),
							eng,
							mockLimits{totalShards: numShards},
							0,
							reg,
						)

						// Run the query with sharding.
						shardedRes, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "anonymous"), req)
						require.Nil(t, err)

						shardedPrometheusRes, ok := shardedRes.GetPrometheusResponse()
						require.True(t, ok)

						sort.Sort(byLabels(shardedPrometheusRes.Data.Result))
						approximatelyEquals(t, expectedPrometheusRes, shardedPrometheusRes)

						require.Len(t, shardedPrometheusRes.GetWarnings(), 0)
					})
				}
			})
		})
	}
}

func TestQuerySharding_ShouldNotPanicOnNilQueryExpression(t *testing.T) {
	_, engine := newEngineForTesting(t, querier.PrometheusEngine)
	reg := prometheus.NewPedanticRegistry()

	middleware := newQueryShardingMiddleware(log.NewNopLogger(), engine, mockLimits{totalShards: 16}, 0, reg)
	handler := middleware.Wrap(mockHandlerWith(nil, nil))

	// Create a request with a nil queryExpr to simulate a failed parse.
	req := &PrometheusRangeQueryRequest{
		path:      "/query_range",
		start:     util.TimeToMillis(start),
		end:       util.TimeToMillis(end),
		step:      step.Milliseconds(),
		queryExpr: nil, // Simulates a query that failed to parse
	}

	ctx := user.InjectOrgID(context.Background(), "test")

	require.NotPanics(t, func() {
		resp, err := handler.Do(ctx, req)
		require.ErrorContains(t, err, errRequestNoQuery.Error())
		require.Nil(t, resp)
	})
}
