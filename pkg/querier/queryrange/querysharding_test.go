// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/promql_test.go
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/querysharding_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queryrange

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/queryrange/astmapper"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/util"
)

var (
	start         = time.Now()
	end           = start.Add(30 * time.Minute)
	step          = 30 * time.Second
	lookbackDelta = 5 * time.Minute
)

func mockHandlerWith(resp *PrometheusResponse, err error) Handler {
	return HandlerFunc(func(ctx context.Context, req Request) (Response, error) {
		if expired := ctx.Err(); expired != nil {
			return nil, expired
		}

		return resp, err
	})
}

// approximatelyEquals ensures two responses are approximately equal, up to 6 decimals precision per sample
func approximatelyEquals(t *testing.T, a, b *PrometheusResponse) {
	// Ensure both queries succeeded.
	require.Equal(t, statusSuccess, a.Status)
	require.Equal(t, statusSuccess, b.Status)

	as, err := responseToSamples(a)
	require.Nil(t, err)
	bs, err := responseToSamples(b)
	require.Nil(t, err)

	require.Equal(t, len(as), len(bs), "expected same number of series")

	for i := 0; i < len(as); i++ {
		a := as[i]
		b := bs[i]
		require.Equal(t, a.Labels, b.Labels)
		require.Equal(t, len(a.Samples), len(b.Samples), "expected same number of samples for series %s", a.Labels)

		for j := 0; j < len(a.Samples); j++ {
			expected := a.Samples[j]
			actual := b.Samples[j]
			require.Equalf(t, expected.TimestampMs, actual.TimestampMs, "sample timestamp at position %d for series %s", j, a.Labels)

			if value.IsStaleNaN(expected.Value) {
				require.Truef(t, value.IsStaleNaN(actual.Value), "sample value at position %d is expected to be stale marker for series %s", j, a.Labels)
			} else if math.IsNaN(expected.Value) {
				require.Truef(t, math.IsNaN(actual.Value), "sample value at position %d is expected to be NaN for series %s", j, a.Labels)
			} else {
				if expected.Value == 0 {
					require.Zero(t, actual.Value, "sample value at position %d with timestamp %d for series %s", j, expected.TimestampMs, a.Labels)
					continue
				}
				// InEpsilon means the relative error (see https://en.wikipedia.org/wiki/Relative_error#Example) must be less than epsilon (here 1e-12).
				// The relative error is calculated using: abs(actual-expected) / abs(expected)
				require.InEpsilonf(t, expected.Value, actual.Value, 1e-12, "sample value at position %d with timestamp %d for series %s", j, expected.TimestampMs, a.Labels)
			}
		}
	}
}

func TestQueryShardingCorrectness(t *testing.T) {
	var (
		numSeries          = 1000
		numStaleSeries     = 100
		numHistograms      = 1000
		numStaleHistograms = 100
		histogramBuckets   = []float64{1.0, 2.0, 4.0, 10.0, 100.0, math.Inf(1)}
	)

	tests := map[string]struct {
		query string

		// Expected number of sharded queries per shard (the final expected
		// number will be multiplied for the number of shards).
		expectedShardedQueries int

		// expectSpecificOrder disables result sorting and checks that both results are returned in same order.
		expectSpecificOrder bool

		// noRangeQuery skips the range query (specially made for "string" query as it can't be used for a range query)
		noRangeQuery bool
	}{
		"sum() no grouping": {
			query:                  `sum(metric_counter)`,
			expectedShardedQueries: 1,
		},
		"sum() grouping 'by'": {
			query:                  `sum by(group_1) (metric_counter)`,
			expectedShardedQueries: 1,
		},
		"sum() grouping 'without'": {
			query:                  `sum without(unique) (metric_counter)`,
			expectedShardedQueries: 1,
		},
		"sum(rate()) no grouping": {
			query:                  `sum(rate(metric_counter[1m]))`,
			expectedShardedQueries: 1,
		},
		"sum(rate()) grouping 'by'": {
			query:                  `sum by(group_1) (rate(metric_counter[1m]))`,
			expectedShardedQueries: 1,
		},
		"sum(rate()) grouping 'without'": {
			query:                  `sum without(unique) (rate(metric_counter[1m]))`,
			expectedShardedQueries: 1,
		},
		"sum(rate()) with no effective grouping because all groups have 1 series": {
			query:                  `sum by(unique) (rate(metric_counter{group_1="0"}[1m]))`,
			expectedShardedQueries: 1,
		},
		"histogram_quantile() grouping only 'by' le": {
			query:                  `histogram_quantile(0.5, sum by(le) (rate(metric_histogram_bucket[1m])))`,
			expectedShardedQueries: 1,
		},
		"histogram_quantile() grouping 'by'": {
			query:                  `histogram_quantile(0.5, sum by(group_1, le) (rate(metric_histogram_bucket[1m])))`,
			expectedShardedQueries: 1,
		},
		"histogram_quantile() grouping 'without'": {
			query:                  `histogram_quantile(0.5, sum without(group_1, group_2, unique) (rate(metric_histogram_bucket[1m])))`,
			expectedShardedQueries: 1,
		},
		"histogram_quantile() with no effective grouping because all groups have 1 series": {
			query:                  `histogram_quantile(0.5, sum by(unique, le) (rate(metric_histogram_bucket{group_1="0"}[1m])))`,
			expectedShardedQueries: 1,
		},
		"min() no grouping": {
			query:                  `min(metric_counter{group_1="0"})`,
			expectedShardedQueries: 1,
		},
		"min() grouping 'by'": {
			query:                  `min by(group_2) (metric_counter{group_1="0"})`,
			expectedShardedQueries: 1,
		},
		"min() grouping 'without'": {
			query:                  `min without(unique) (metric_counter{group_1="0"})`,
			expectedShardedQueries: 1,
		},
		"max() no grouping": {
			query:                  `max(metric_counter{group_1="0"})`,
			expectedShardedQueries: 1,
		},
		"max() grouping 'by'": {
			query:                  `max by(group_2) (metric_counter{group_1="0"})`,
			expectedShardedQueries: 1,
		},
		"max() grouping 'without'": {
			query:                  `max without(unique) (metric_counter{group_1="0"})`,
			expectedShardedQueries: 1,
		},
		"count() no grouping": {
			query:                  `count(metric_counter)`,
			expectedShardedQueries: 1,
		},
		"count() grouping 'by'": {
			query:                  `count by(group_2) (metric_counter)`,
			expectedShardedQueries: 1,
		},
		"count() grouping 'without'": {
			query:                  `count without(unique) (metric_counter)`,
			expectedShardedQueries: 1,
		},
		"sum(count())": {
			query:                  `sum(count by(group_1) (metric_counter))`,
			expectedShardedQueries: 1,
		},
		"avg() no grouping": {
			query:                  `avg(metric_counter)`,
			expectedShardedQueries: 2, // avg() is parallelized as sum()/count().
		},
		"avg() grouping 'by'": {
			query:                  `avg by(group_2) (metric_counter)`,
			expectedShardedQueries: 2, // avg() is parallelized as sum()/count().
		},
		"avg() grouping 'without'": {
			query:                  `avg without(unique) (metric_counter)`,
			expectedShardedQueries: 2, // avg() is parallelized as sum()/count().
		},
		"sum(min_over_time())": {
			query:                  `sum by (group_1, group_2) (min_over_time(metric_counter{const="fixed"}[2m]))`,
			expectedShardedQueries: 1,
		},
		"sum(max_over_time())": {
			query:                  `sum by (group_1, group_2) (max_over_time(metric_counter{const="fixed"}[2m]))`,
			expectedShardedQueries: 1,
		},
		"sum(avg_over_time())": {
			query:                  `sum by (group_1, group_2) (avg_over_time(metric_counter{const="fixed"}[2m]))`,
			expectedShardedQueries: 1,
		},
		"or": {
			query:                  `sum(rate(metric_counter{group_1="0"}[1m])) or sum(rate(metric_counter{group_1="1"}[1m]))`,
			expectedShardedQueries: 2,
		},
		"and": {
			query: `
				sum without(unique) (rate(metric_counter{group_1="0"}[1m]))
				and
				max without(unique) (metric_counter) > 0`,
			expectedShardedQueries: 2,
		},
		"sum(rate()) > avg(rate())": {
			query: `
				sum(rate(metric_counter[1m]))
				>
				avg(rate(metric_counter[1m]))`,
			expectedShardedQueries: 3, // avg() is parallelized as sum()/count().
		},
		"sum by (rate()) / 2 ^ 2": {
			query: `
			sum by (group_1) (rate(metric_counter[1m])) / 2 ^ 2`,
			expectedShardedQueries: 1,
		},
		"sum by (rate()) / time() *2": {
			query: `
			sum by (group_1) (rate(metric_counter[1m])) / time() *2`,
			expectedShardedQueries: 1,
		},
		"sum(rate()) / vector(3) ^ month()": {
			query:                  `sum(rate(metric_counter[1m])) / vector(3) ^ month()`,
			expectedShardedQueries: 1,
		},
		"sum(rate(metric_counter[1m])) / vector(3) ^ vector(2) + sum(ln(metric_counter))": {
			query:                  `sum(rate(metric_counter[1m])) / vector(3) ^ vector(2) + sum(ln(metric_counter))`,
			expectedShardedQueries: 2,
		},
		"nested count()": {
			query: `sum(
				  count(
				    count(metric_counter) by (group_1, group_2)
				  ) by (group_1)
				)`,
			expectedShardedQueries: 1,
		},
		"subquery max": {
			query: `max_over_time(
							rate(metric_counter[1m])
						[5m:1m]
					)`,
			expectedShardedQueries: 1,
		},
		"subquery min": {
			query: `min_over_time(
							rate(metric_counter[1m])
						[5m:1m]
					)`,
			expectedShardedQueries: 1,
		},
		"sum of subquery min": {
			query:                  `sum by(group_1) (min_over_time((changes(metric_counter[5m]))[10m:2m]))`,
			expectedShardedQueries: 1,
		},
		"triple subquery": {
			query: `max_over_time(
						stddev_over_time(
							deriv(
								rate(metric_counter[10m])
							[5m:1m])
						[2m:])
					[10m:])`,
			expectedShardedQueries: 1,
		},
		"double subquery deriv": {
			query:                  `max_over_time( deriv( rate(metric_counter[10m])[5m:1m] )[10m:] )`,
			expectedShardedQueries: 1,
		},
		"@ modifier": {
			query:                  `sum by (group_1)(rate(metric_counter[1h] @ end())) + sum by (group_1)(rate(metric_counter[1h] @ start()))`,
			expectedShardedQueries: 2,
		},
		"@ modifier and offset": {
			query:                  `sum by (group_1)(rate(metric_counter[1h] @ end() offset 1m))`,
			expectedShardedQueries: 1,
		},
		"label_replace": {
			query: `sum by (foo)(
					 	label_replace(
									rate(metric_counter{group_1="0"}[1m]),
									"foo", "bar$1", "group_2", "(.*)"
								)
							)`,
			expectedShardedQueries: 1,
		},
		"label_join": {
			query: `sum by (foo)(
							label_join(
									rate(metric_counter{group_1="0"}[1m]),
									"foo", ",", "group_1", "group_2", "const"
								)
							)`,
			expectedShardedQueries: 1,
		},
		`query with sort() expects specific order`: {
			query:                  `sort(sum(metric_histogram_bucket) by (le))`,
			expectedShardedQueries: 1,
			expectSpecificOrder:    true,
		},
		"scalar(aggregation)": {
			query:                  `scalar(sum(metric_counter))`,
			expectedShardedQueries: 1,
		},
		`filtering binary operation with constant scalar`: {
			query:                  `count(metric_counter > 0)`,
			expectedShardedQueries: 1,
		},
		`filtering binary operation of a function result with scalar`: {
			query:                  `max_over_time(metric_counter[5m]) > 0`,
			expectedShardedQueries: 1,
		},
		`binary operation with an aggregation on one hand`: {
			query:                  `sum(metric_counter) > 1`,
			expectedShardedQueries: 1,
		},
		`binary operation with an aggregation on the other hand`: {
			query:                  `0 < sum(metric_counter)`,
			expectedShardedQueries: 1,
		},
		`binary operation with an aggregation by some label on one hand`: {
			query:                  `count by (unique) (metric_counter) > 0`,
			expectedShardedQueries: 1,
		},
		`filtering binary operation with non constant`: {
			query:                  `max_over_time(metric_counter[5m]) > scalar(min(metric_counter))`,
			expectedShardedQueries: 1, // scalar on the right should be sharded, but not the binary op itself, hence 1
		},
		//
		// The following queries are not expected to be shardable.
		//
		"subquery min_over_time with aggr": {
			query: `min_over_time(
						sum by(group_1) (
							rate(metric_counter[5m])
						)[10m:]
					)`,
			expectedShardedQueries: 0,
		},
		"stddev()": {
			query:                  `stddev(metric_counter{const="fixed"})`,
			expectedShardedQueries: 0,
		},
		"stdvar()": {
			query:                  `stdvar(metric_counter{const="fixed"})`,
			expectedShardedQueries: 0,
		},
		"topk()": {
			query:                  `topk(2, metric_counter{const="fixed"})`,
			expectedShardedQueries: 0,
		},
		"bottomk()": {
			query:                  `bottomk(2, metric_counter{const="fixed"})`,
			expectedShardedQueries: 0,
		},
		"vector()": {
			query:                  `vector(1)`,
			expectedShardedQueries: 0,
		},
		"scalar(single metric)": {
			query:                  `scalar(metric_counter{unique="1"})`, // Select a single metric.
			expectedShardedQueries: 0,
		},
		"histogram_quantile() no grouping": {
			query:                  fmt.Sprintf(`histogram_quantile(0.99, metric_histogram_bucket{unique="%d"})`, numSeries+10), // Select a single histogram metric.
			expectedShardedQueries: 0,
		},
		"histogram_quantile with inner aggregation": {
			query:                  `sum by (group_1) (histogram_quantile(0.9, rate(metric_histogram_bucket[1m])))`,
			expectedShardedQueries: 0,
		},
		"histogram_quantile without aggregation": {
			query:                  `histogram_quantile(0.5, rate(metric_histogram_bucket{group_1="0"}[1m]))`,
			expectedShardedQueries: 0,
		},
		`subqueries with non parallelizable function in children`: {
			query: `max_over_time(
				absent_over_time(
					deriv(
						rate(metric_counter[1m])
					[5m:1m])
				[2m:1m])
			[10m:1m] offset 25m)`,
			expectedShardedQueries: 0,
		},
		"string literal": {
			query:                  `"test"`,
			expectedShardedQueries: 0,
			noRangeQuery:           true,
		},
	}

	series := make([]*promql.StorageSeries, 0, numSeries+(numHistograms*len(histogramBuckets)))
	seriesID := 0

	// Add counter series.
	for i := 0; i < numSeries; i++ {
		gen := factor(float64(i) * 0.1)
		if i >= numSeries-numStaleSeries {
			// Wrap the generator to inject the staleness marker between minute 10 and 20.
			gen = stale(start.Add(10*time.Minute), start.Add(20*time.Minute), gen)
		}

		series = append(series, newSeries(newTestCounterLabels(seriesID), start.Add(-lookbackDelta), end, step, gen))
		seriesID++
	}

	// Add a special series whose data points end earlier than the end of the queried time range
	// and has NO stale marker.
	series = append(series, newSeries(newTestCounterLabels(seriesID),
		start.Add(-lookbackDelta), end.Add(-5*time.Minute), step, factor(2)))
	seriesID++

	// Add a special series whose data points end earlier than the end of the queried time range
	// and HAS a stale marker at the end.
	series = append(series, newSeries(newTestCounterLabels(seriesID),
		start.Add(-lookbackDelta), end.Add(-5*time.Minute), step, stale(end.Add(-6*time.Minute), end.Add(-4*time.Minute), factor(2))))
	seriesID++

	// Add a special series whose data points start later than the start of the queried time range.
	series = append(series, newSeries(newTestCounterLabels(seriesID),
		start.Add(5*time.Minute), end, step, factor(2)))
	seriesID++

	// Add histogram series.
	for i := 0; i < numHistograms; i++ {
		for bucketIdx, bucketLe := range histogramBuckets {
			// We expect each bucket to have a value higher than the previous one.
			gen := factor(float64(i) * float64(bucketIdx) * 0.1)
			if i >= numHistograms-numStaleHistograms {
				// Wrap the generator to inject the staleness marker between minute 10 and 20.
				gen = stale(start.Add(10*time.Minute), start.Add(20*time.Minute), gen)
			}

			series = append(series, newSeries(newTestHistogramLabels(seriesID, bucketLe),
				start.Add(-lookbackDelta), end, step, gen))
		}

		// Increase the series ID after all per-bucket series have been created.
		seriesID++
	}

	// Create a queryable on the fixtures.
	queryable := storageSeriesQueryable(series)

	for testName, testData := range tests {
		// Change scope to ensure it work fine when test cases are executed concurrently.
		testData := testData

		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			reqs := []Request{
				&PrometheusInstantQueryRequest{
					Path:  "/query",
					Time:  util.TimeToMillis(end),
					Query: testData.query,
				},
			}
			if !testData.noRangeQuery {
				reqs = append(reqs, &PrometheusRangeQueryRequest{
					Path:  "/query_range",
					Start: util.TimeToMillis(start),
					End:   util.TimeToMillis(end),
					Step:  step.Milliseconds(),
					Query: testData.query,
				})
			}

			for _, req := range reqs {
				t.Run(fmt.Sprintf("%T", req), func(t *testing.T) {
					engine := newEngine()
					downstream := &downstreamHandler{
						engine:    engine,
						queryable: queryable,
					}

					// Run the query without sharding.
					expectedRes, err := downstream.Do(context.Background(), req)
					require.Nil(t, err)
					expectedPrometheusRes := expectedRes.(*PrometheusResponse)
					if !testData.expectSpecificOrder {
						sort.Sort(byLabels(expectedPrometheusRes.Data.Result))
					}

					// Ensure the query produces some results.
					require.NotEmpty(t, expectedPrometheusRes.Data.Result)
					requireValidSamples(t, expectedPrometheusRes.Data.Result)

					for _, numShards := range []int{2, 4, 8, 16} {
						t.Run(fmt.Sprintf("shards=%d", numShards), func(t *testing.T) {
							reg := prometheus.NewPedanticRegistry()
							shardingware := newQueryShardingMiddleware(
								log.NewNopLogger(),
								engine,
								mockLimits{totalShards: numShards},
								reg,
							)

							// Run the query with sharding.
							shardedRes, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
							require.Nil(t, err)

							// Ensure the two results matches (float precision can slightly differ, there's no guarantee in PromQL engine too
							// if you rerun the same query twice).
							shardedPrometheusRes := shardedRes.(*PrometheusResponse)
							if !testData.expectSpecificOrder {
								sort.Sort(byLabels(shardedPrometheusRes.Data.Result))
							}
							approximatelyEquals(t, expectedPrometheusRes, shardedPrometheusRes)

							// Ensure the query has been sharded/not sharded as expected.
							expectedSharded := 0
							if testData.expectedShardedQueries > 0 {
								expectedSharded = 1
							}

							assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP cortex_frontend_query_sharding_rewrites_attempted_total Total number of queries the query-frontend attempted to shard.
					# TYPE cortex_frontend_query_sharding_rewrites_attempted_total counter
					cortex_frontend_query_sharding_rewrites_attempted_total 1
					# HELP cortex_frontend_query_sharding_rewrites_succeeded_total Total number of queries the query-frontend successfully rewritten in a shardable way.
					# TYPE cortex_frontend_query_sharding_rewrites_succeeded_total counter
					cortex_frontend_query_sharding_rewrites_succeeded_total %d
					# HELP cortex_frontend_sharded_queries_total Total number of sharded queries.
					# TYPE cortex_frontend_sharded_queries_total counter
					cortex_frontend_sharded_queries_total %d
				`, expectedSharded, testData.expectedShardedQueries*numShards)),
								"cortex_frontend_query_sharding_rewrites_attempted_total",
								"cortex_frontend_query_sharding_rewrites_succeeded_total",
								"cortex_frontend_sharded_queries_total"))
						})
					}
				})
			}
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

	labelsForShard := labelsForShardsGenerator(labels.FromStrings(labels.MetricName, "metric"), shards)
	storageSeries := []*promql.StorageSeries{
		newSeries(labelsForShard(0), from, to, step, constant(evilFloatA)),
		newSeries(labelsForShard(1), from, to, step, constant(evilFloatA)),
		newSeries(labelsForShard(2), from, to, step, constant(evilFloatB)),
	}

	shardingware := newQueryShardingMiddleware(log.NewNopLogger(), newEngine(), mockLimits{totalShards: shards}, prometheus.NewPedanticRegistry())
	downstream := &downstreamHandler{engine: newEngine(), queryable: storageSeriesQueryable(storageSeries)}

	req := &PrometheusInstantQueryRequest{
		Path:  "/query",
		Time:  to.UnixMilli(),
		Query: `sum(metric)`,
	}

	var lastVal float64
	for i := 0; i <= 100; i++ {
		shardedRes, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
		require.NoError(t, err)

		shardedPrometheusRes := shardedRes.(*PrometheusResponse)

		sampleStreams, err := responseToSamples(shardedPrometheusRes)
		require.NoError(t, err)

		require.Lenf(t, sampleStreams, 1, "There should be 1 samples stream (query %d)", i)
		require.Lenf(t, sampleStreams[0].Samples, 1, "There should be 1 sample in the first stream (query %d)", i)
		val := sampleStreams[0].Samples[0].Value

		if i > 0 {
			require.Equalf(t, lastVal, val, "Value differs on query %d", i)
		}
		lastVal = val
	}
}

// labelsForShardsGenerator returns a function that provides labels.Labels for the shard requested
// A single generator instance generates different label sets.
func labelsForShardsGenerator(base labels.Labels, shards uint64) func(shard uint64) labels.Labels {
	i := 0
	return func(shard uint64) labels.Labels {
		for {
			i++
			ls := make(labels.Labels, len(base)+1)
			copy(ls, base)
			ls[len(ls)-1] = labels.Label{Name: "__test_shard_adjuster__", Value: fmt.Sprintf("adjusted to be %s by %d", sharding.FormatShardIDLabelValue(shard, shards), i)}
			sort.Sort(ls)
			// If this label value makes this labels combination fall into the desired shard, return it, otherwise keep trying.
			if ls.Hash()%shards == shard {
				return ls
			}
		}
	}
}

// TestQuerySharding_FunctionCorrectness is the old test that probably at some point inspired the TestQuerySharding_Correctness,
// we keep it here since it adds more test cases.
func TestQuerySharding_FunctionCorrectness(t *testing.T) {
	mkQueries, tests := func(tpl, fn string, testMatrix bool, fArgs []string) []string {
		if tpl == "" {
			tpl = `(<fn>(bar1{}<args>))`
		}
		result := strings.Replace(tpl, "<fn>", fn, -1)

		if testMatrix {
			// turn selectors into ranges
			result = strings.Replace(result, "}", "}[1m]", -1)
		}

		if len(fArgs) > 0 {
			args := "," + strings.Join(fArgs, ",")
			result = strings.Replace(result, "<args>", args, -1)
		} else {
			result = strings.Replace(result, "<args>", "", -1)
		}

		return []string{
			result,
			"sum" + result,
			"sum by (bar)" + result,
			"count" + result,
			"count by (bar)" + result,
		}
	}, []struct {
		fn         string
		args       []string
		rangeQuery bool
		tpl        string
	}{
		{fn: "abs"},
		{fn: "avg_over_time", rangeQuery: true},
		{fn: "ceil"},
		{fn: "changes", rangeQuery: true},
		{fn: "count_over_time", rangeQuery: true},
		{fn: "days_in_month"},
		{fn: "day_of_month"},
		{fn: "day_of_week"},
		{fn: "delta", rangeQuery: true},
		{fn: "deriv", rangeQuery: true},
		{fn: "exp"},
		{fn: "floor"},
		{fn: "hour"},
		{fn: "idelta", rangeQuery: true},
		{fn: "increase", rangeQuery: true},
		{fn: "irate", rangeQuery: true},
		{fn: "ln"},
		{fn: "log10"},
		{fn: "log2"},
		{fn: "max_over_time", rangeQuery: true},
		{fn: "min_over_time", rangeQuery: true},
		{fn: "minute"},
		{fn: "month"},
		{fn: "rate", rangeQuery: true},
		{fn: "resets", rangeQuery: true},
		{fn: "sort"},
		{fn: "sort_desc"},
		{fn: "sqrt"},
		{fn: "deg"},
		{fn: "asinh"},
		{fn: "rad"},
		{fn: "cosh"},
		{fn: "atan"},
		{fn: "atanh"},
		{fn: "asin"},
		{fn: "sinh"},
		{fn: "cos"},
		{fn: "acosh"},
		{fn: "sin"},
		{fn: "tanh"},
		{fn: "tan"},
		{fn: "acos"},
		{fn: "stddev_over_time", rangeQuery: true},
		{fn: "stdvar_over_time", rangeQuery: true},
		{fn: "sum_over_time", rangeQuery: true},
		{fn: "last_over_time", rangeQuery: true},
		{fn: "present_over_time", rangeQuery: true},
		{fn: "quantile_over_time", rangeQuery: true, tpl: `(<fn>(0.5,bar1{}))`},
		{fn: "quantile_over_time", rangeQuery: true, tpl: `(<fn>(0.99,bar1{}))`},
		{fn: "timestamp"},
		{fn: "year"},
		{fn: "sgn"},
		{fn: "clamp", args: []string{"5", "10"}},
		{fn: "clamp_max", args: []string{"5"}},
		{fn: "clamp_min", args: []string{"5"}},
		{fn: "predict_linear", args: []string{"1"}, rangeQuery: true},
		{fn: "round", args: []string{"20"}},
		{fn: "holt_winters", args: []string{"0.5", "0.7"}, rangeQuery: true},
		{fn: "label_replace", args: []string{`"fuzz"`, `"$1"`, `"foo"`, `"b(.*)"`}},
		{fn: "label_join", args: []string{`"fuzz"`, `","`, `"foo"`, `"bar"`}},
	}

	for _, tc := range tests {
		const numShards = 4
		for _, query := range mkQueries(tc.tpl, tc.fn, tc.rangeQuery, tc.args) {
			t.Run(query, func(t *testing.T) {
				queryable := storageSeriesQueryable([]*promql.StorageSeries{
					newSeries(labels.Labels{{Name: "__name__", Value: "bar1"}, {Name: "baz", Value: "blip"}, {Name: "bar", Value: "blop"}, {Name: "foo", Value: "barr"}}, start.Add(-lookbackDelta), end, step, factor(5)),
					newSeries(labels.Labels{{Name: "__name__", Value: "bar1"}, {Name: "baz", Value: "blip"}, {Name: "bar", Value: "blop"}, {Name: "foo", Value: "bazz"}}, start.Add(-lookbackDelta), end, step, factor(7)),
					newSeries(labels.Labels{{Name: "__name__", Value: "bar1"}, {Name: "baz", Value: "blip"}, {Name: "bar", Value: "blap"}, {Name: "foo", Value: "buzz"}}, start.Add(-lookbackDelta), end, step, factor(12)),
					newSeries(labels.Labels{{Name: "__name__", Value: "bar1"}, {Name: "baz", Value: "blip"}, {Name: "bar", Value: "blap"}, {Name: "foo", Value: "bozz"}}, start.Add(-lookbackDelta), end, step, factor(11)),
					newSeries(labels.Labels{{Name: "__name__", Value: "bar1"}, {Name: "baz", Value: "blip"}, {Name: "bar", Value: "blop"}, {Name: "foo", Value: "buzz"}}, start.Add(-lookbackDelta), end, step, factor(8)),
					newSeries(labels.Labels{{Name: "__name__", Value: "bar1"}, {Name: "baz", Value: "blip"}, {Name: "bar", Value: "blap"}, {Name: "foo", Value: "bazz"}}, start.Add(-lookbackDelta), end, step, arithmeticSequence(10)),
				})

				req := &PrometheusRangeQueryRequest{
					Path:  "/query_range",
					Start: util.TimeToMillis(start),
					End:   util.TimeToMillis(end),
					Step:  step.Milliseconds(),
					Query: query,
				}

				reg := prometheus.NewPedanticRegistry()
				engine := newEngine()
				shardingware := newQueryShardingMiddleware(
					log.NewNopLogger(),
					engine,
					mockLimits{totalShards: numShards},
					reg,
				)
				downstream := &downstreamHandler{
					engine:    engine,
					queryable: queryable,
				}

				// Run the query without sharding.
				expectedRes, err := downstream.Do(context.Background(), req)
				require.Nil(t, err)

				// Ensure the query produces some results.
				require.NotEmpty(t, expectedRes.(*PrometheusResponse).Data.Result)

				// Run the query with sharding.
				shardedRes, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
				require.Nil(t, err)

				// Ensure the two results matches (float precision can slightly differ, there's no guarantee in PromQL engine too
				// if you rerun the same query twice).
				approximatelyEquals(t, expectedRes.(*PrometheusResponse), shardedRes.(*PrometheusResponse))
			})
		}
	}

	// Ensure all PromQL functions have been tested.
	testedFns := make(map[string]struct{}, len(tests))
	for _, tc := range tests {
		testedFns[tc.fn] = struct{}{}
	}

	fnToIgnore := map[string]struct{}{
		"time":   {},
		"scalar": {},
		"vector": {},
		"pi":     {},
	}

	for expectedFn := range promql.FunctionCalls {
		if _, ok := fnToIgnore[expectedFn]; ok {
			continue
		}
		// It's OK if it's tested. Ignore if it's one of the non parallelizable functions.
		_, ok := testedFns[expectedFn]
		assert.Truef(t, ok || util.StringsContain(astmapper.NonParallelFuncs, expectedFn), "%s should be tested", expectedFn)
	}
}

func TestQuerySharding_ShouldFallbackToDownstreamHandlerOnMappingFailure(t *testing.T) {
	req := &PrometheusRangeQueryRequest{
		Path:  "/query_range",
		Start: util.TimeToMillis(start),
		End:   util.TimeToMillis(end),
		Step:  step.Milliseconds(),
		Query: "aaa{", // Invalid query.
	}

	shardingware := newQueryShardingMiddleware(log.NewNopLogger(), newEngine(), mockLimits{totalShards: 16}, nil)

	// Mock the downstream handler, always returning success (regardless the query is valid or not).
	downstream := &mockHandler{}
	downstream.On("Do", mock.Anything, mock.Anything).Return(&PrometheusResponse{Status: statusSuccess}, nil)

	// Run the query with sharding middleware wrapping the downstream one.
	// We expect the query parsing done by the query sharding middleware to fail
	// but to fallback on the downstream one which always returns success.
	res, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
	require.NoError(t, err)
	assert.Equal(t, statusSuccess, res.(*PrometheusResponse).GetStatus())
	downstream.AssertCalled(t, "Do", mock.Anything, mock.Anything)
}

func TestQuerySharding_ShouldSkipShardingViaOption(t *testing.T) {
	req := &PrometheusRangeQueryRequest{
		Path:  "/query_range",
		Start: util.TimeToMillis(start),
		End:   util.TimeToMillis(end),
		Step:  step.Milliseconds(),
		Query: "sum by (foo) (rate(bar{}[1m]))", // shardable query.
		Options: Options{
			ShardingDisabled: true,
		},
	}

	shardingware := newQueryShardingMiddleware(log.NewNopLogger(), newEngine(), mockLimits{totalShards: 16}, nil)

	downstream := &mockHandler{}
	downstream.On("Do", mock.Anything, mock.Anything).Return(&PrometheusResponse{Status: statusSuccess}, nil)

	res, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
	require.NoError(t, err)
	assert.Equal(t, statusSuccess, res.(*PrometheusResponse).GetStatus())
	// Ensure we get the same request downstream. No sharding
	downstream.AssertCalled(t, "Do", mock.Anything, req)
	downstream.AssertNumberOfCalls(t, "Do", 1)
}

func TestQuerySharding_ShouldOverrideShardingSizeViaOption(t *testing.T) {
	req := &PrometheusRangeQueryRequest{
		Path:  "/query_range",
		Start: util.TimeToMillis(start),
		End:   util.TimeToMillis(end),
		Step:  step.Milliseconds(),
		Query: "sum by (foo) (rate(bar{}[1m]))", // shardable query.
		Options: Options{
			TotalShards: 128,
		},
	}

	shardingware := newQueryShardingMiddleware(log.NewNopLogger(), newEngine(), mockLimits{totalShards: 16}, nil)

	downstream := &mockHandler{}
	downstream.On("Do", mock.Anything, mock.Anything).Return(&PrometheusResponse{
		Status: statusSuccess, Data: &PrometheusData{
			ResultType: string(parser.ValueTypeVector),
		},
	}, nil)

	res, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
	require.NoError(t, err)
	assert.Equal(t, statusSuccess, res.(*PrometheusResponse).GetStatus())
	downstream.AssertCalled(t, "Do", mock.Anything, mock.Anything)
	// we expect 128 calls to the downstream handler and not the original 16.
	downstream.AssertNumberOfCalls(t, "Do", 128)
}

func TestQuerySharding_ShouldSupportMaxShardedQueries(t *testing.T) {
	tests := map[string]struct {
		query             string
		hints             *Hints
		totalShards       int
		maxShardedQueries int
		expectedShards    int
		compactorShards   int
	}{
		"query is not shardable": {
			query:             "metric",
			hints:             &Hints{TotalQueries: 1},
			totalShards:       16,
			maxShardedQueries: 64,
			expectedShards:    1,
		},
		"single splitted query, query has 1 shardable leg": {
			query:             "sum(metric)",
			hints:             &Hints{TotalQueries: 1},
			totalShards:       16,
			maxShardedQueries: 64,
			expectedShards:    16,
		},
		"single splitted query, query has many shardable legs": {
			query:             "sum(metric_1) + sum(metric_2) + sum(metric_3) + sum(metric_4)",
			hints:             &Hints{TotalQueries: 1},
			totalShards:       16,
			maxShardedQueries: 16,
			expectedShards:    4,
		},
		"multiple splitted queries, query has 1 shardable leg": {
			query:             "sum(metric)",
			hints:             &Hints{TotalQueries: 10},
			totalShards:       16,
			maxShardedQueries: 64,
			expectedShards:    6,
		},
		"multiple splitted queries, query has 2 shardable legs": {
			query:             "sum(metric) / count(metric)",
			hints:             &Hints{TotalQueries: 10},
			totalShards:       16,
			maxShardedQueries: 64,
			expectedShards:    3,
		},
		"multiple splitted queries, query has 2 shardable legs, no compactor shards": {
			query:             "sum(metric) / count(metric)",
			hints:             &Hints{TotalQueries: 3},
			totalShards:       16,
			maxShardedQueries: 64,
			compactorShards:   0,
			expectedShards:    10,
		},
		"multiple splitted queries, query has 2 shardable legs, 3 compactor shards": {
			query:             "sum(metric) / count(metric)",
			hints:             &Hints{TotalQueries: 3},
			totalShards:       16,
			maxShardedQueries: 64,
			compactorShards:   3,
			expectedShards:    9,
		},
		"multiple splitted queries, query has 2 shardable legs, 4 compactor shards": {
			query:             "sum(metric) / count(metric)",
			hints:             &Hints{TotalQueries: 3},
			totalShards:       16,
			maxShardedQueries: 64,
			compactorShards:   4,
			expectedShards:    8,
		},
		"multiple splitted queries, query has 2 shardable legs, 10 compactor shards": {
			query:             "sum(metric) / count(metric)",
			hints:             &Hints{TotalQueries: 3},
			totalShards:       16,
			maxShardedQueries: 64,
			compactorShards:   10,
			expectedShards:    10,
		},
		"multiple splitted queries, query has 2 shardable legs, 11 compactor shards": {
			query:             "sum(metric) / count(metric)",
			hints:             &Hints{TotalQueries: 3},
			totalShards:       16,
			maxShardedQueries: 64,
			compactorShards:   11,
			expectedShards:    10, // cannot be adjusted to make 11 multiple or divisible, keep original.
		},
		"multiple splitted queries, query has 2 shardable legs, 14 compactor shards": {
			query:             "sum(metric) / count(metric)",
			hints:             &Hints{TotalQueries: 3},
			totalShards:       16,
			maxShardedQueries: 64,
			compactorShards:   14,
			expectedShards:    7, // 7 divides 14
		},
		"query sharding is disabled": {
			query:             "sum(metric)",
			hints:             &Hints{TotalQueries: 1},
			totalShards:       0, // Disabled.
			maxShardedQueries: 64,
			expectedShards:    1,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			req := &PrometheusRangeQueryRequest{
				Path:  "/query_range",
				Start: util.TimeToMillis(start),
				End:   util.TimeToMillis(end),
				Step:  step.Milliseconds(),
				Query: testData.query,
				Hints: testData.hints,
			}

			limits := mockLimits{
				totalShards:       testData.totalShards,
				maxShardedQueries: testData.maxShardedQueries,
				compactorShards:   testData.compactorShards,
			}
			shardingware := newQueryShardingMiddleware(log.NewNopLogger(), newEngine(), limits, nil)

			// Keep track of the unique number of shards queried to downstream.
			uniqueShardsMx := sync.Mutex{}
			uniqueShards := map[string]struct{}{}

			downstream := &mockHandler{}
			downstream.On("Do", mock.Anything, mock.Anything).Return(&PrometheusResponse{
				Status: statusSuccess, Data: &PrometheusData{
					ResultType: string(parser.ValueTypeVector),
				},
			}, nil).Run(func(args mock.Arguments) {
				req := args[1].(Request)
				reqShard := regexp.MustCompile(`__query_shard__="[^"]+"`).FindString(req.GetQuery())

				uniqueShardsMx.Lock()
				uniqueShards[reqShard] = struct{}{}
				uniqueShardsMx.Unlock()
			})

			res, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
			require.NoError(t, err)
			assert.Equal(t, statusSuccess, res.(*PrometheusResponse).GetStatus())
			assert.Equal(t, testData.expectedShards, len(uniqueShards))
		})
	}
}

func TestQuerySharding_ShouldReturnErrorOnDownstreamHandlerFailure(t *testing.T) {
	req := &PrometheusRangeQueryRequest{
		Path:  "/query_range",
		Start: util.TimeToMillis(start),
		End:   util.TimeToMillis(end),
		Step:  step.Milliseconds(),
		Query: "vector(1)", // A non shardable query.
	}

	shardingware := newQueryShardingMiddleware(log.NewNopLogger(), newEngine(), mockLimits{totalShards: 16}, nil)

	// Mock the downstream handler to always return error.
	downstreamErr := errors.Errorf("some err")
	downstream := mockHandlerWith(nil, downstreamErr)

	// Run the query with sharding middleware wrapping the downstream one.
	// We expect to get the downstream error.
	_, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
	require.Error(t, err)
	assert.Equal(t, downstreamErr, err)
}

func TestQuerySharding_ShouldReturnErrorInCorrectFormat(t *testing.T) {
	var (
		engine        = newEngine()
		engineTimeout = promql.NewEngine(promql.EngineOpts{
			Logger:             log.NewNopLogger(),
			Reg:                nil,
			MaxSamples:         10e6,
			Timeout:            50 * time.Millisecond,
			ActiveQueryTracker: nil,
			LookbackDelta:      lookbackDelta,
			EnableAtModifier:   true,
			NoStepSubqueryIntervalFn: func(rangeMillis int64) int64 {
				return int64(1 * time.Minute / (time.Millisecond / time.Nanosecond))
			},
		})
		engineSampleLimit = promql.NewEngine(promql.EngineOpts{
			Logger:             log.NewNopLogger(),
			Reg:                nil,
			MaxSamples:         1,
			Timeout:            time.Hour,
			ActiveQueryTracker: nil,
			LookbackDelta:      lookbackDelta,
			EnableAtModifier:   true,
			NoStepSubqueryIntervalFn: func(rangeMillis int64) int64 {
				return int64(1 * time.Minute / (time.Millisecond / time.Nanosecond))
			},
		})
		queryableErr = storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
			return nil, errors.New("fatal queryable error")
		})
		queryable = storageSeriesQueryable([]*promql.StorageSeries{
			newSeries(labels.Labels{{Name: "__name__", Value: "bar1"}}, start.Add(-lookbackDelta), end, step, factor(5)),
		})
		queryableSlow = newMockShardedQueryable(
			2,
			[]string{"a", "b", "c"},
			1,
			time.Second,
		)
	)

	for _, tc := range []struct {
		name             string
		engineDownstream *promql.Engine
		engineSharding   *promql.Engine
		expError         error
		queryable        storage.Queryable
	}{
		{
			name:             "downstream - timeout",
			engineDownstream: engineTimeout,
			engineSharding:   engine,
			expError:         apierror.New(apierror.TypeTimeout, "query timed out in expression evaluation"),
			queryable:        queryableSlow,
		},
		{
			name:             "downstream - sample limit",
			engineDownstream: engineSampleLimit,
			engineSharding:   engine,
			expError:         apierror.New(apierror.TypeExec, "query processing would load too many samples into memory in query execution"),
		},
		{
			name:             "sharding - timeout",
			engineDownstream: engine,
			engineSharding:   engineTimeout,
			expError:         apierror.New(apierror.TypeTimeout, "query timed out in expression evaluation"),
			queryable:        queryableSlow,
		},
		{
			name:             "downstream - storage",
			engineDownstream: engine,
			engineSharding:   engineSampleLimit,
			queryable:        queryableErr,
			expError:         apierror.New(apierror.TypeInternal, "fatal queryable error"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &PrometheusRangeQueryRequest{
				Path:  "/query_range",
				Start: util.TimeToMillis(start),
				End:   util.TimeToMillis(end),
				Step:  step.Milliseconds(),
				Query: "sum(bar1)",
			}

			shardingware := newQueryShardingMiddleware(log.NewNopLogger(), tc.engineSharding, mockLimits{totalShards: 3}, nil)

			if tc.queryable == nil {
				tc.queryable = queryable
			}

			downstream := &downstreamHandler{
				engine:    tc.engineDownstream,
				queryable: tc.queryable,
			}

			// Run the query with sharding middleware wrapping the downstream one.
			// We expect to get the downstream error.
			_, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
			if tc.expError == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Equal(t, tc.expError, err)
			}
		})
	}
}

func TestQuerySharding_EngineErrorMapping(t *testing.T) {
	const (
		numSeries = 30
		numShards = 8
	)
	var (
		engine = newEngine()
	)

	series := make([]*promql.StorageSeries, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		series = append(series, newSeries(newTestCounterLabels(i), start.Add(-lookbackDelta), end, step, factor(float64(i)*0.1)))
	}

	queryable := storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return &querierMock{series: series}, nil
	})

	req := &PrometheusRangeQueryRequest{
		Path:  "/query_range",
		Start: util.TimeToMillis(start),
		End:   util.TimeToMillis(end),
		Step:  step.Milliseconds(),
		Query: `sum by (group_1) (metric_counter) - on(group_1) group_right(unique) (sum by (group_1,unique) (metric_counter))`,
	}

	downstream := &downstreamHandler{engine: newEngine(), queryable: queryable}
	reg := prometheus.NewPedanticRegistry()
	shardingware := newQueryShardingMiddleware(log.NewNopLogger(), engine, mockLimits{totalShards: numShards}, reg)

	// Run the query with sharding.
	_, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
	assert.Equal(t, apierror.New(apierror.TypeExec, "multiple matches for labels: grouping labels must ensure unique matches"), err)
}

func TestQuerySharding_WrapMultipleTime(t *testing.T) {
	req := &PrometheusRangeQueryRequest{
		Path:  "/query_range",
		Start: util.TimeToMillis(start),
		End:   util.TimeToMillis(end),
		Step:  step.Milliseconds(),
		Query: "vector(1)", // A non shardable query.
	}

	shardingware := newQueryShardingMiddleware(log.NewNopLogger(), newEngine(), mockLimits{totalShards: 16}, prometheus.NewRegistry())

	require.NotPanics(t, func() {
		_, err := shardingware.Wrap(mockHandlerWith(nil, nil)).Do(user.InjectOrgID(context.Background(), "test"), req)
		require.Nil(t, err)
		_, err = shardingware.Wrap(mockHandlerWith(nil, nil)).Do(user.InjectOrgID(context.Background(), "test"), req)
		require.Nil(t, err)
	})
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
			query:            `sum(rate(http_requests_total[5m]))`,
			desc:             "sum nogroup",
		},
		// sum by
		{
			labelBuckets:     16,
			labels:           []string{"a", "b", "c"},
			samplesPerSeries: 100,
			query:            `sum by(a) (rate(http_requests_total[5m]))`,
			desc:             "sum by",
		},
		// sum without
		{
			labelBuckets:     16,
			labels:           []string{"a", "b", "c"},
			samplesPerSeries: 100,
			query:            `sum without (a) (rate(http_requests_total[5m]))`,
			desc:             "sum without",
		},
	} {
		for _, delayPerSeries := range []time.Duration{
			0,
			time.Millisecond / 10,
		} {
			engine := promql.NewEngine(promql.EngineOpts{
				Logger:     log.NewNopLogger(),
				Reg:        nil,
				MaxSamples: 100000000,
				Timeout:    time.Minute,
			})

			queryable := newMockShardedQueryable(
				tc.samplesPerSeries,
				tc.labels,
				tc.labelBuckets,
				delayPerSeries,
			)
			downstream := &downstreamHandler{
				engine:    engine,
				queryable: queryable,
			}

			var (
				start int64 = 0
				end         = int64(1000 * tc.samplesPerSeries)
				step        = (end - start) / 1000
			)

			req := &PrometheusRangeQueryRequest{
				Path:    "/query_range",
				Start:   start,
				End:     end,
				Step:    step,
				Timeout: time.Minute,
				Query:   tc.query,
			}

			for _, shardFactor := range shards {
				shardingware := newQueryShardingMiddleware(
					log.NewNopLogger(),
					engine,
					mockLimits{totalShards: shardFactor},
					nil,
				).Wrap(downstream)

				b.Run(
					fmt.Sprintf(
						"desc:[%s]---shards:[%d]---series:[%.0f]---delayPerSeries:[%s]---samplesPerSeries:[%d]",
						tc.desc,
						shardFactor,
						math.Pow(float64(tc.labelBuckets), float64(len(tc.labels))),
						delayPerSeries,
						tc.samplesPerSeries,
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
						Point: promql.Point{T: 1, V: 1},
						Metric: labels.Labels{
							{Name: "a", Value: "a1"},
							{Name: "b", Value: "b1"},
						},
					},
					promql.Sample{
						Point: promql.Point{T: 2, V: 2},
						Metric: labels.Labels{
							{Name: "a", Value: "a2"},
							{Name: "b", Value: "b2"},
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
						Metric: labels.Labels{
							{Name: "a", Value: "a1"},
							{Name: "b", Value: "b1"},
						},
						Points: []promql.Point{
							{T: 1, V: 1},
							{T: 2, V: 2},
						},
					},
					{
						Metric: labels.Labels{
							{Name: "a", Value: "a2"},
							{Name: "b", Value: "b2"},
						},
						Points: []promql.Point{
							{T: 1, V: 8},
							{T: 2, V: 9},
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

type downstreamHandler struct {
	engine    *promql.Engine
	queryable storage.Queryable
}

func (h *downstreamHandler) Do(ctx context.Context, r Request) (Response, error) {
	qry, err := newQuery(r, h.engine, h.queryable)
	if err != nil {
		return nil, err
	}

	res := qry.Exec(ctx)
	extracted, err := promqlResultToSamples(res)
	if err != nil {
		return nil, err
	}

	return &PrometheusResponse{
		Status: statusSuccess,
		Data: &PrometheusData{
			ResultType: string(res.Value.Type()),
			Result:     extracted,
		},
	}, nil
}

func storageSeriesQueryable(series []*promql.StorageSeries) storage.Queryable {
	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return &querierMock{series: series}, nil
	})
}

type querierMock struct {
	series []*promql.StorageSeries
}

func (m *querierMock) Select(sorted bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	shard, matchers, err := sharding.RemoveShardFromMatchers(matchers)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	// Filter series by label matchers.
	var filtered []*promql.StorageSeries

	for _, series := range m.series {
		if seriesMatches(series, matchers...) {
			filtered = append(filtered, series)
		}
	}

	// Filter series by shard (if any)
	filtered = filterSeriesByShard(filtered, shard)

	// Honor the sorting.
	if sorted {
		sort.Slice(filtered, func(i, j int) bool {
			return labels.Compare(filtered[i].Labels(), filtered[j].Labels()) < 0
		})
	}

	return newSeriesIteratorMock(filtered)
}

func (m *querierMock) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (m *querierMock) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (m *querierMock) Close() error { return nil }

func seriesMatches(series *promql.StorageSeries, matchers ...*labels.Matcher) bool {
	for _, m := range matchers {
		if !m.Matches(series.Labels().Get(m.Name)) {
			return false
		}
	}

	return true
}

func filterSeriesByShard(series []*promql.StorageSeries, shard *sharding.ShardSelector) []*promql.StorageSeries {
	if shard == nil {
		return series
	}

	var filtered []*promql.StorageSeries

	for _, s := range series {
		if s.Labels().Hash()%shard.ShardCount == shard.ShardIndex {
			filtered = append(filtered, s)
		}
	}

	return filtered
}

func newSeries(metric labels.Labels, from, to time.Time, step time.Duration, gen generator) *promql.StorageSeries {
	var (
		points    []promql.Point
		prevValue *float64
	)

	for ts := from; ts.Unix() <= to.Unix(); ts = ts.Add(step) {
		t := ts.Unix() * 1e3
		v := gen(t)

		// If both the previous and current values are the stale marker, then we omit the
		// point completely (we just keep the 1st one in a consecutive series of stale markers).
		shouldSkip := prevValue != nil && value.IsStaleNaN(*prevValue) && value.IsStaleNaN(v)
		prevValue = &v
		if shouldSkip {
			continue
		}

		points = append(points, promql.Point{
			T: t,
			V: v,
		})
	}

	// Ensure series labels are sorted.
	sort.Sort(metric)

	return promql.NewStorageSeries(promql.Series{
		Metric: metric,
		Points: points,
	})
}

// newTestCounterLabels generates series labels for a counter metric used in tests.
func newTestCounterLabels(id int) labels.Labels {
	return labels.Labels{
		{Name: "__name__", Value: "metric_counter"},
		{Name: "const", Value: "fixed"},                 // A constant label.
		{Name: "unique", Value: strconv.Itoa(id)},       // A unique label.
		{Name: "group_1", Value: strconv.Itoa(id % 10)}, // A first grouping label.
		{Name: "group_2", Value: strconv.Itoa(id % 3)},  // A second grouping label.
	}
}

// newTestCounterLabels generates series labels for an histogram metric used in tests.
func newTestHistogramLabels(id int, bucketLe float64) labels.Labels {
	return labels.Labels{
		{Name: "__name__", Value: "metric_histogram_bucket"},
		{Name: "le", Value: fmt.Sprintf("%f", bucketLe)},
		{Name: "const", Value: "fixed"},                 // A constant label.
		{Name: "unique", Value: strconv.Itoa(id)},       // A unique label.
		{Name: "group_1", Value: strconv.Itoa(id % 10)}, // A first grouping label.
		{Name: "group_2", Value: strconv.Itoa(id % 5)},  // A second grouping label.
	}
}

// generator defined a function used to generate sample values in tests.
type generator func(ts int64) float64

func factor(f float64) generator {
	i := 0.
	return func(int64) float64 {
		i++
		res := i * f
		return res
	}
}

func arithmeticSequence(f float64) generator {
	i := 0.
	return func(int64) float64 {
		i++
		res := i + f
		return res
	}
}

// stale wraps the input generator and injects stale marker between from and to.
func stale(from, to time.Time, wrap generator) generator {
	return func(ts int64) float64 {
		// Always get the next value from the wrapped generator.
		v := wrap(ts)

		// Inject the stale marker if we're at the right time.
		if ts >= util.TimeToMillis(from) && ts <= util.TimeToMillis(to) {
			return math.Float64frombits(value.StaleNaN)
		}

		return v
	}
}

// constant returns a generator that generates a constant value
func constant(value float64) generator {
	return func(ts int64) float64 {
		return value
	}
}

type seriesIteratorMock struct {
	idx    int
	series []*promql.StorageSeries
}

func newSeriesIteratorMock(series []*promql.StorageSeries) *seriesIteratorMock {
	return &seriesIteratorMock{
		idx:    -1,
		series: series,
	}
}

func (i *seriesIteratorMock) Next() bool {
	i.idx++
	return i.idx < len(i.series)
}

func (i *seriesIteratorMock) At() storage.Series {
	if i.idx >= len(i.series) {
		return nil
	}

	return i.series[i.idx]
}

func (i *seriesIteratorMock) Err() error {
	return nil
}

func (i *seriesIteratorMock) Warnings() storage.Warnings {
	return nil
}

// newEngine creates and return a new promql.Engine used for testing.
func newEngine() *promql.Engine {
	return promql.NewEngine(promql.EngineOpts{
		Logger:             log.NewNopLogger(),
		Reg:                nil,
		MaxSamples:         10e6,
		Timeout:            1 * time.Hour,
		ActiveQueryTracker: nil,
		LookbackDelta:      lookbackDelta,
		EnableAtModifier:   true,
		NoStepSubqueryIntervalFn: func(rangeMillis int64) int64 {
			return int64(1 * time.Minute / (time.Millisecond / time.Nanosecond))
		},
	})
}
