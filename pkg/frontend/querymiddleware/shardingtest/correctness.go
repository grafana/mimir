// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/promql_test.go
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/querysharding_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package shardingtest

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/testdatagen"
	"github.com/grafana/mimir/pkg/streamingpromql/compat"
)

var (
	Start         = time.Now()
	End           = Start.Add(30 * time.Minute)
	Step          = 30 * time.Second
	lookbackDelta = 5 * time.Minute
)

func RunCorrectnessTests(t *testing.T, runTestCase func(t *testing.T, testCase CorrectnessTestCase, queryable storage.Queryable)) {
	var (
		numSeries                = 1000
		numStaleSeries           = 100
		numConvHistograms        = 1000
		numStaleConvHistograms   = 100
		histogramBuckets         = []float64{1.0, 2.0, 4.0, 10.0, 100.0, math.Inf(1)}
		numNativeHistograms      = 1000
		numStaleNativeHistograms = 100
	)

	tests := map[string]CorrectnessTestCase{
		"sum() no grouping": {
			Query:                  `sum(metric_counter)`,
			ExpectedShardedQueries: 1,
		},
		"sum() offset": {
			Query:                  `sum(metric_counter offset 5s)`,
			ExpectedShardedQueries: 1,
		},
		"sum() negative offset": {
			Query:                  `sum(metric_counter offset -5s)`,
			ExpectedShardedQueries: 1,
		},
		"sum() offset arithmetic": {
			Query:                  `sum(metric_counter offset (10s - 5s))`,
			ExpectedShardedQueries: 1,
		},
		"sum() grouping 'by'": {
			Query:                  `sum by(group_1) (metric_counter)`,
			ExpectedShardedQueries: 1,
		},
		"sum() grouping 'without'": {
			Query:                  `sum without(unique) (metric_counter)`,
			ExpectedShardedQueries: 1,
		},
		"sum(rate()) no grouping": {
			Query:                  `sum(rate(metric_counter[1m]))`,
			ExpectedShardedQueries: 1,
		},
		"sum(rate()) range arithmetic": {
			Query:                  `sum(rate(metric_counter[30s+30s]))`,
			ExpectedShardedQueries: 1,
		},
		"sum(rate()) grouping 'by'": {
			Query:                  `sum by(group_1) (rate(metric_counter[1m]))`,
			ExpectedShardedQueries: 1,
		},
		"sum(rate()) grouping 'without'": {
			Query:                  `sum without(unique) (rate(metric_counter[1m]))`,
			ExpectedShardedQueries: 1,
		},
		"sum(rate()) with no effective grouping because all groups have 1 series": {
			Query:                  `sum by(unique) (rate(metric_counter{group_1="0"}[1m]))`,
			ExpectedShardedQueries: 1,
		},
		`group by (group_1) (metric_counter)`: {
			Query:                  `group by (group_1) (metric_counter)`,
			ExpectedShardedQueries: 1,
		},
		`group by (group_1) (group by (group_1, group_2) (metric_counter))`: {
			Query:                  `group by (group_1) (group by (group_1, group_2) (metric_counter))`,
			ExpectedShardedQueries: 1,
		},
		`count by (group_1) (group by (group_1, group_2) (metric_counter))`: {
			Query:                  `count by (group_1) (group by (group_1, group_2) (metric_counter))`,
			ExpectedShardedQueries: 1,
		},
		"histogram_quantile() grouping only 'by' le": {
			Query:                  `histogram_quantile(0.5, sum by(le) (rate(metric_histogram_bucket[1m])))`,
			ExpectedShardedQueries: 1,
		},
		"histogram_quantile() grouping 'by'": {
			Query:                  `histogram_quantile(0.5, sum by(group_1, le) (rate(metric_histogram_bucket[1m])))`,
			ExpectedShardedQueries: 1,
		},
		"histogram_quantile() grouping 'without'": {
			Query:                  `histogram_quantile(0.5, sum without(group_1, group_2, unique) (rate(metric_histogram_bucket[1m])))`,
			ExpectedShardedQueries: 1,
		},
		"histogram_quantile() with no effective grouping because all groups have 1 series": {
			Query:                  `histogram_quantile(0.5, sum by(unique, le) (rate(metric_histogram_bucket{group_1="0"}[1m])))`,
			ExpectedShardedQueries: 1,
		},
		"min() no grouping": {
			Query:                  `min(metric_counter{group_1="0"})`,
			ExpectedShardedQueries: 1,
		},
		"min() grouping 'by'": {
			Query:                  `min by(group_2) (metric_counter{group_1="0"})`,
			ExpectedShardedQueries: 1,
		},
		"min() grouping 'without'": {
			Query:                  `min without(unique) (metric_counter{group_1="0"})`,
			ExpectedShardedQueries: 1,
		},
		"max() no grouping": {
			Query:                  `max(metric_counter{group_1="0"})`,
			ExpectedShardedQueries: 1,
		},
		"max() grouping 'by'": {
			Query:                  `max by(group_2) (metric_counter{group_1="0"})`,
			ExpectedShardedQueries: 1,
		},
		"max() grouping 'without'": {
			Query:                  `max without(unique) (metric_counter{group_1="0"})`,
			ExpectedShardedQueries: 1,
		},
		"count() no grouping": {
			Query:                  `count(metric_counter)`,
			ExpectedShardedQueries: 1,
		},
		"count() grouping 'by'": {
			Query:                  `count by(group_2) (metric_counter)`,
			ExpectedShardedQueries: 1,
		},
		"count() grouping 'without'": {
			Query:                  `count without(unique) (metric_counter)`,
			ExpectedShardedQueries: 1,
		},
		"sum(count())": {
			Query:                  `sum(count by(group_1) (metric_counter))`,
			ExpectedShardedQueries: 1,
		},
		"avg() no grouping": {
			Query:                  `avg(metric_counter)`,
			ExpectedShardedQueries: 2, // avg() is parallelized as sum()/count().
		},
		"avg() grouping 'by'": {
			Query:                  `avg by(group_2) (metric_counter)`,
			ExpectedShardedQueries: 2, // avg() is parallelized as sum()/count().
		},
		"avg() grouping 'without'": {
			Query:                  `avg without(unique) (metric_counter)`,
			ExpectedShardedQueries: 2, // avg() is parallelized as sum()/count().
		},
		"sum(min_over_time())": {
			Query:                  `sum by (group_1, group_2) (min_over_time(metric_counter{const="fixed"}[2m]))`,
			ExpectedShardedQueries: 1,
		},
		"sum(max_over_time())": {
			Query:                  `sum by (group_1, group_2) (max_over_time(metric_counter{const="fixed"}[2m]))`,
			ExpectedShardedQueries: 1,
		},
		"sum(avg_over_time())": {
			Query:                  `sum by (group_1, group_2) (avg_over_time(metric_counter{const="fixed"}[2m]))`,
			ExpectedShardedQueries: 1,
		},
		"or": {
			Query:                  `sum(rate(metric_counter{group_1="0"}[1m])) or sum(rate(metric_counter{group_1="1"}[1m]))`,
			ExpectedShardedQueries: 2,
		},
		"and": {
			Query: `
				sum without(unique) (rate(metric_counter{group_1="0"}[1m]))
				and
				max without(unique) (metric_counter) > 0`,
			ExpectedShardedQueries: 2,
		},
		"sum(rate()) > avg(rate())": {
			Query: `
				sum(rate(metric_counter[1m]))
				>
				avg(rate(metric_counter[1m]))`,
			ExpectedShardedQueries: 3, // avg() is parallelized as sum()/count().
		},
		"sum by(unique) * on (unique) group_left (group_1) avg by (unique, group_1)": {
			// ensure that avg transformation into sum/count does not break label matching in previous binop.
			Query: `
				sum by(unique) (metric_counter)
				*
				on (unique) group_left (group_1)
				avg by (unique, group_1) (metric_counter)`,
			ExpectedShardedQueries: 3,
		},
		"sum by (rate()) / 2 ^ 2": {
			Query: `
			sum by (group_1) (rate(metric_counter[1m])) / 2 ^ 2`,
			ExpectedShardedQueries: 1,
		},
		"sum by (rate()) / time() *2": {
			Query: `
			sum by (group_1) (rate(metric_counter[1m])) / time() *2`,
			ExpectedShardedQueries: 1,
		},
		"sum(rate()) / vector(3) ^ month()": {
			Query:                  `sum(rate(metric_counter[1m])) / vector(3) ^ month()`,
			ExpectedShardedQueries: 1,
		},
		"sum(rate(metric_counter[1m])) / vector(3) ^ vector(2) + sum(ln(metric_counter))": {
			Query:                  `sum(rate(metric_counter[1m])) / vector(3) ^ vector(2) + sum(ln(metric_counter))`,
			ExpectedShardedQueries: 2,
		},
		"nested count()": {
			Query: `sum(
				  count(
				    count(metric_counter) by (group_1, group_2)
				  ) by (group_1)
				)`,
			ExpectedShardedQueries: 1,
		},
		"subquery max": {
			Query: `max_over_time(
							rate(metric_counter[1m])
						[5m:1m]
					)`,
			ExpectedShardedQueries: 1,
		},
		"subquery min": {
			Query: `min_over_time(
							rate(metric_counter[1m])
						[5m:1m]
					)`,
			ExpectedShardedQueries: 1,
		},
		"sum of subquery min": {
			Query:                  `sum by(group_1) (min_over_time((changes(metric_counter[5m]))[10m:2m]))`,
			ExpectedShardedQueries: 1,
		},
		"triple subquery": {
			Query: `max_over_time(
						stddev_over_time(
							deriv(
								rate(metric_counter[10m])
							[5m:1m])
						[2m:])
					[10m:])`,
			ExpectedShardedQueries: 1,
		},
		"double subquery deriv": {
			Query:                  `max_over_time( deriv( rate(metric_counter[10m])[5m:1m] )[10m:] )`,
			ExpectedShardedQueries: 1,
		},
		"@ modifier": {
			Query:                  `sum by (group_1)(rate(metric_counter[1h] @ end())) + sum by (group_1)(rate(metric_counter[1h] @ start()))`,
			ExpectedShardedQueries: 2,
		},
		"@ modifier and offset": {
			Query:                  `sum by (group_1)(rate(metric_counter[1h] @ end() offset 1m))`,
			ExpectedShardedQueries: 1,
		},
		"@ modifier and negative offset": {
			Query:                  `sum by (group_1)(rate(metric_counter[1h] @ start() offset -1m))`,
			ExpectedShardedQueries: 1,
		},
		"label_replace": {
			Query: `sum by (foo)(
					 	label_replace(
									rate(metric_counter{group_1="0"}[1m]),
									"foo", "bar$1", "group_2", "(.*)"
								)
							)`,
			ExpectedShardedQueries: 1,
		},
		"label_join": {
			Query: `sum by (foo)(
							label_join(
									rate(metric_counter{group_1="0"}[1m]),
									"foo", ",", "group_1", "group_2", "const"
								)
							)`,
			ExpectedShardedQueries: 1,
		},
		`query with sort() expects specific order`: {
			Query:                  `sort(sum(metric_histogram_bucket) by (le))`,
			ExpectedShardedQueries: 1,
			ExpectSpecificOrder:    true,
		},
		"scalar(aggregation)": {
			Query:                  `scalar(sum(metric_counter))`,
			ExpectedShardedQueries: 1,
		},
		`filtering binary operation with constant scalar`: {
			Query:                  `count(metric_counter > 0)`,
			ExpectedShardedQueries: 1,
		},
		`filtering binary operation of a function result with scalar`: {
			Query:                  `max_over_time(metric_counter[5m]) > 0`,
			ExpectedShardedQueries: 1,
		},
		`binary operation with an aggregation on one hand`: {
			Query:                  `sum(metric_counter) > 1`,
			ExpectedShardedQueries: 1,
		},
		`binary operation with an aggregation on the other hand`: {
			Query:                  `0 < sum(metric_counter)`,
			ExpectedShardedQueries: 1,
		},
		`binary operation with an aggregation by some label on one hand`: {
			Query:                  `count by (unique) (metric_counter) > 0`,
			ExpectedShardedQueries: 1,
		},
		`filtering binary operation with non constant`: {
			Query:                  `max by(unique) (max_over_time(metric_counter[5m])) > scalar(min(metric_counter))`,
			ExpectedShardedQueries: 2,
		},
		//
		// The following queries are not expected to be shardable.
		//
		"subquery min_over_time with aggr": {
			Query: `min_over_time(
						sum by(group_1) (
							rate(metric_counter[5m])
						)[10m:]
					)`,
			ExpectedShardedQueries: 0,
		},
		"outer subquery on top of sum": {
			Query:                  `sum(metric_counter) by (group_1)[5m:1m]`,
			ExpectedShardedQueries: 0,
			NoRangeQuery:           true,
		},
		"outer subquery on top of avg": {
			Query:                  `avg(metric_counter) by (group_1)[5m:1m]`,
			ExpectedShardedQueries: 0,
			NoRangeQuery:           true,
		},
		"stddev()": {
			Query:                  `stddev(metric_counter{const="fixed"})`,
			ExpectedShardedQueries: 0,
		},
		"stdvar()": {
			Query:                  `stdvar(metric_counter{const="fixed"})`,
			ExpectedShardedQueries: 0,
		},
		"topk()": {
			Query:                  `topk(2, metric_counter{const="fixed"})`,
			ExpectedShardedQueries: 0,
		},
		"bottomk()": {
			Query:                  `bottomk(2, metric_counter{const="fixed"})`,
			ExpectedShardedQueries: 0,
		},
		"vector()": {
			Query:                  `vector(1)`,
			ExpectedShardedQueries: 0,
		},
		"scalar(single metric)": {
			Query:                  `scalar(metric_counter{unique="1"})`, // Select a single metric.
			ExpectedShardedQueries: 0,
		},
		"histogram_quantile no grouping": {
			Query:                  fmt.Sprintf(`histogram_quantile(0.99, metric_histogram_bucket{unique="%d"})`, numSeries+10), // Select a single histogram metric.
			ExpectedShardedQueries: 0,
		},
		"histogram_quantile with inner aggregation": {
			Query:                  `sum by (group_1) (histogram_quantile(0.9, rate(metric_histogram_bucket[1m])))`,
			ExpectedShardedQueries: 0,
		},
		"histogram_quantile without aggregation": {
			Query:                  `histogram_quantile(0.5, rate(metric_histogram_bucket{group_1="0"}[1m]))`,
			ExpectedShardedQueries: 0,
		},
		`subqueries with non parallelizable function in children`: {
			Query: `max_over_time(
				absent_over_time(
					deriv(
						rate(metric_counter[1m])
					[5m:1m])
				[2m:1m])
			[10m:1m] offset 25m)`,
			ExpectedShardedQueries: 0,
		},
		"string literal": {
			Query:                  `"test"`,
			ExpectedShardedQueries: 0,
			NoRangeQuery:           true,
		},
		"day_of_month() >= 1 and day_of_month()": {
			Query:                  `day_of_month() >= 1 and day_of_month()`,
			ExpectedShardedQueries: 0,
		},
		"month() >= 1 and month()": {
			Query:                  `month() >= 1 and month()`,
			ExpectedShardedQueries: 0,
		},
		"vector(1) > 0 and vector(1)": {
			Query:                  `vector(1) > 0 and vector(1)`,
			ExpectedShardedQueries: 0,
		},
		"sum(metric_counter) > 0 and vector(1)": {
			Query:                  `sum(metric_counter) > 0 and vector(1)`,
			ExpectedShardedQueries: 1,
		},
		"vector(1)": {
			Query:                  `vector(1)`,
			ExpectedShardedQueries: 0,
		},
		"time()": {
			Query:                  `time()`,
			ExpectedShardedQueries: 0,
		},
		"month(sum(metric_counter))": {
			Query:                  `month(sum(metric_counter))`,
			ExpectedShardedQueries: 1, // Sharded because the contents of `sum()` is sharded.
		},
		"month(sum(metric_counter)) > 0 and vector(1)": {
			Query:                  `month(sum(metric_counter)) > 0 and vector(1)`,
			ExpectedShardedQueries: 1, // Sharded because the contents of `sum()` is sharded.
		},
		"0 < bool 1": {
			Query:                  `0 < bool 1`,
			ExpectedShardedQueries: 0,
		},
		"scalar(metric_counter{const=\"fixed\"}) < bool 1": {
			Query:                  `scalar(metric_counter{const="fixed"}) < bool 1`,
			ExpectedShardedQueries: 0,
		},
		"scalar(sum(metric_counter)) < bool 1": {
			Query:                  `scalar(sum(metric_counter)) < bool 1`,
			ExpectedShardedQueries: 1,
		},
		// Summing floats and native histograms together makes no sense, see
		// https://prometheus.io/docs/prometheus/latest/querying/operators/#operators-for-native-histograms
		// so we exclude native histograms here and in some subsequent tests
		`sum({__name__!=""}) excluding native histograms`: {
			Query:                  `sum({__name__!="",__name__!="metric_native_histogram"})`,
			ExpectedShardedQueries: 1,
		},
		`sum by (group_1) ({__name__!=""}) excluding native histograms`: {
			Query:                  `sum by (group_1) ({__name__!="",__name__!="metric_native_histogram"})`,
			ExpectedShardedQueries: 1,
		},
		`sum by (group_1) (count_over_time({__name__!=""}[1m])) excluding native histograms`: {
			Query:                  `sum by (group_1) (count_over_time({__name__!="",__name__!="metric_native_histogram"}[1m]))`,
			ExpectedShardedQueries: 1,
		},
		`sum(metric_native_histogram)`: {
			Query:                  `sum(metric_native_histogram)`,
			ExpectedShardedQueries: 1,
		},
		`sum(histogram_sum(metric_native_histogram))`: {
			Query:                  `sum(histogram_sum(metric_native_histogram))`,
			ExpectedShardedQueries: 1,
		},
		`sum by (group_1) (metric_native_histogram)`: {
			Query:                  `sum by (group_1) (metric_native_histogram)`,
			ExpectedShardedQueries: 1,
		},
		`sum by (group_1) (count_over_time(metric_native_histogram[1m]))`: {
			Query:                  `sum by (group_1) (count_over_time(metric_native_histogram[1m]))`,
			ExpectedShardedQueries: 1,
		},
		`count(metric_native_histogram)`: {
			Query:                  `count(metric_native_histogram)`,
			ExpectedShardedQueries: 1,
		},
		`count by (group_1) (metric_native_histogram)`: {
			Query:                  `count by (group_1) (metric_native_histogram)`,
			ExpectedShardedQueries: 1,
		},
		`count by (group_1) (count_over_time(metric_native_histogram[1m]))`: {
			Query:                  `count by (group_1) (count_over_time(metric_native_histogram[1m]))`,
			ExpectedShardedQueries: 1,
		},
		`histogram_sum(sum(metric_native_histogram))`: {
			Query:                  `histogram_sum(sum(metric_native_histogram))`,
			ExpectedShardedQueries: 1,
		},
		`histogram_count(sum(metric_native_histogram))`: {
			Query:                  `histogram_count(sum(metric_native_histogram))`,
			ExpectedShardedQueries: 1,
		},
		`histogram_quantile(0.5, sum(metric_native_histogram))`: {
			Query:                  `histogram_quantile(0.5, sum(metric_native_histogram))`,
			ExpectedShardedQueries: 1,
		},
		`histogram_fraction(0, 0.5, sum(metric_native_histogram))`: {
			Query:                  `histogram_fraction(0, 0.5, sum(metric_native_histogram))`,
			ExpectedShardedQueries: 1,
		},
		`histogram_stdvar`: {
			Query:                  `histogram_stdvar(metric_native_histogram)`,
			ExpectedShardedQueries: 0,
		},
		`histogram_stdvar on sum of metrics`: {
			Query:                  `histogram_stdvar(sum(metric_native_histogram))`,
			ExpectedShardedQueries: 1,
		},
		`histogram_stddev`: {
			Query:                  `histogram_stddev(metric_native_histogram)`,
			ExpectedShardedQueries: 0,
		},
		`histogram_stddev on sum of metrics`: {
			Query:                  `histogram_stddev(sum(metric_native_histogram))`,
			ExpectedShardedQueries: 1,
		},
	}

	series := make([]storage.Series, 0, numSeries+(numConvHistograms*len(histogramBuckets))+numNativeHistograms)
	seriesID := 0

	// Add counter series.
	for i := 0; i < numSeries; i++ {
		gen := testdatagen.Factor(float64(i) * 0.1)
		if i >= numSeries-numStaleSeries {
			// Wrap the generator to inject the staleness marker between minute 10 and 20.
			gen = testdatagen.Stale(Start.Add(10*time.Minute), Start.Add(20*time.Minute), gen)
		}

		series = append(series, testdatagen.NewSeries(testdatagen.NewTestCounterLabels(seriesID), Start.Add(-lookbackDelta), End, Step, gen))
		seriesID++
	}

	// Add a special series whose data points end earlier than the end of the queried time range
	// and has NO stale marker.
	series = append(series, testdatagen.NewSeries(testdatagen.NewTestCounterLabels(seriesID),
		Start.Add(-lookbackDelta), End.Add(-5*time.Minute), Step, testdatagen.Factor(2)))
	seriesID++

	// Add a special series whose data points end earlier than the end of the queried time range
	// and HAS a stale marker at the end.
	series = append(series, testdatagen.NewSeries(testdatagen.NewTestCounterLabels(seriesID),
		Start.Add(-lookbackDelta), End.Add(-5*time.Minute), Step, testdatagen.Stale(End.Add(-6*time.Minute), End.Add(-4*time.Minute), testdatagen.Factor(2))))
	seriesID++

	// Add a special series whose data points start later than the start of the queried time range.
	series = append(series, testdatagen.NewSeries(testdatagen.NewTestCounterLabels(seriesID),
		Start.Add(5*time.Minute), End, Step, testdatagen.Factor(2)))
	seriesID++

	// Add conventional histogram series.
	for i := 0; i < numConvHistograms; i++ {
		for bucketIdx, bucketLe := range histogramBuckets {
			// We expect each bucket to have a value higher than the previous one.
			gen := testdatagen.Factor(float64(i) * float64(bucketIdx) * 0.1)
			if i >= numConvHistograms-numStaleConvHistograms {
				// Wrap the generator to inject the staleness marker between minute 10 and 20.
				gen = testdatagen.Stale(Start.Add(10*time.Minute), Start.Add(20*time.Minute), gen)
			}

			series = append(series, testdatagen.NewSeries(testdatagen.NewTestConventionalHistogramLabels(seriesID, bucketLe),
				Start.Add(-lookbackDelta), End, Step, gen))
		}

		// Increase the series ID after all per-bucket series have been created.
		seriesID++
	}

	// Add native histogram series.
	for i := 0; i < numNativeHistograms; i++ {
		gen := testdatagen.Factor(float64(i) * 0.5)
		if i >= numNativeHistograms-numStaleNativeHistograms {
			// Wrap the generator to inject the staleness marker between minute 10 and 20.
			gen = testdatagen.Stale(Start.Add(10*time.Minute), Start.Add(20*time.Minute), gen)
		}

		series = append(series, testdatagen.NewNativeHistogramSeries(testdatagen.NewTestNativeHistogramLabels(seriesID), Start.Add(-lookbackDelta), End, Step, gen))
		seriesID++
	}

	// Create a queryable on the fixtures.
	queryable := testdatagen.StorageSeriesQueryable(series)

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			runTestCase(t, testData, queryable)
		})
	}
}

func AssertShardingMetrics(t *testing.T, reg *prometheus.Registry, expectedShardedQueries int, numShards int) {
	expectedSharded := 0
	if expectedShardedQueries > 0 {
		expectedSharded = 1
	}

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
			# HELP cortex_frontend_query_sharding_rewrites_attempted_total Total number of queries the query-frontend attempted to shard.
			# TYPE cortex_frontend_query_sharding_rewrites_attempted_total counter
			cortex_frontend_query_sharding_rewrites_attempted_total 1
			# HELP cortex_frontend_query_sharding_rewrites_succeeded_total Total number of queries the query-frontend successfully rewritten in a shardable way.
			# TYPE cortex_frontend_query_sharding_rewrites_succeeded_total counter
			cortex_frontend_query_sharding_rewrites_succeeded_total %d
			# HELP cortex_frontend_sharded_queries_total Total number of sharded queries.
			# TYPE cortex_frontend_sharded_queries_total counter
			cortex_frontend_sharded_queries_total %d
		`, expectedSharded, expectedShardedQueries*numShards)),
		"cortex_frontend_query_sharding_rewrites_attempted_total",
		"cortex_frontend_query_sharding_rewrites_succeeded_total",
		"cortex_frontend_sharded_queries_total",
	))
}

type CorrectnessTestCase struct {
	Query string

	// Expected number of sharded queries per shard (the final expected
	// number will be multiplied for the number of shards).
	ExpectedShardedQueries int

	// ExpectSpecificOrder disables result sorting and checks that both results are returned in same order.
	ExpectSpecificOrder bool

	// NoRangeQuery skips the range query (specially made for "string" query as it can't be used for a range query)
	NoRangeQuery bool
}

type queryShardingFunctionCorrectnessTest struct {
	fn         string
	args       []string
	rangeQuery bool
	tpl        string
	allowedErr error
}

type functionCorrectnessTestRunner func(t *testing.T, expr string, numShards int, allowedErr error, queryable storage.Queryable)

func RunFunctionCorrectnessTests(t *testing.T, runTestCase functionCorrectnessTestRunner) {
	testsForBoth := []queryShardingFunctionCorrectnessTest{
		{fn: "count_over_time", rangeQuery: true},
		{fn: "delta", rangeQuery: true},
		{fn: "increase", rangeQuery: true},
		{fn: "rate", rangeQuery: true},
		{fn: "resets", rangeQuery: true},
		{fn: "sort_by_label", allowedErr: compat.NotSupportedError{}},
		{fn: "sort_by_label_desc", allowedErr: compat.NotSupportedError{}},
		{fn: "first_over_time", rangeQuery: true},
		{fn: "last_over_time", rangeQuery: true},
		{fn: "present_over_time", rangeQuery: true},
		{fn: "timestamp"},
		{fn: "label_replace", args: []string{`"fuzz"`, `"$1"`, `"foo"`, `"b(.*)"`}},
		{fn: "label_join", args: []string{`"fuzz"`, `","`, `"foo"`, `"bar"`}},
		{fn: "ts_of_first_over_time", rangeQuery: true},
		{fn: "ts_of_last_over_time", rangeQuery: true},
	}
	testsForFloatsOnly := []queryShardingFunctionCorrectnessTest{
		{fn: "abs"},
		{fn: "avg_over_time", rangeQuery: true},
		{fn: "ceil"},
		{fn: "clamp", args: []string{"5", "10"}},
		{fn: "clamp_max", args: []string{"5"}},
		{fn: "clamp_min", args: []string{"5"}},
		{fn: "changes", rangeQuery: true},
		{fn: "days_in_month"},
		{fn: "day_of_month"},
		{fn: "day_of_week"},
		{fn: "day_of_year"},
		{fn: "deriv", rangeQuery: true},
		{fn: "exp"},
		{fn: "floor"},
		{fn: "hour"},
		{fn: "idelta", rangeQuery: true},
		{fn: "irate", rangeQuery: true},
		{fn: "ln"},
		{fn: "log10"},
		{fn: "log2"},
		{fn: "max_over_time", rangeQuery: true},
		{fn: "min_over_time", rangeQuery: true},
		{fn: "minute"},
		{fn: "month"},
		{fn: "round", args: []string{"20"}},
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
		{fn: "quantile_over_time", rangeQuery: true, tpl: `(<fn>(0.5,bar1{}))`},
		{fn: "quantile_over_time", rangeQuery: true, tpl: `(<fn>(0.99,bar1{}))`},
		{fn: "mad_over_time", rangeQuery: true, tpl: `(<fn>(bar1{}))`},
		{fn: "sgn"},
		{fn: "predict_linear", args: []string{"1"}, rangeQuery: true},
		{fn: "double_exponential_smoothing", args: []string{"0.5", "0.7"}, rangeQuery: true},
		// holt_winters is a backwards compatible alias for double_exponential_smoothing.
		{fn: "holt_winters", args: []string{"0.5", "0.7"}, rangeQuery: true},
		{fn: "year"},
		{fn: "ts_of_min_over_time", rangeQuery: true},
		{fn: "ts_of_max_over_time", rangeQuery: true},
	}
	testsForNativeHistogramsOnly := []queryShardingFunctionCorrectnessTest{
		{fn: "histogram_count"},
		{fn: "histogram_sum"},
		{fn: "histogram_fraction", tpl: `(<fn>(0,0.5,bar1{}))`},
		{fn: "histogram_quantile", tpl: `(<fn>(0.5,bar1{}))`},
		{fn: "histogram_stdvar"},
		{fn: "histogram_stddev"},
	}

	t.Run("floats", func(t *testing.T) {
		queryableFloats := testdatagen.StorageSeriesQueryable([]storage.Series{
			testdatagen.NewSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blop", "foo", "barr"), Start.Add(-lookbackDelta), End, Step, testdatagen.Factor(5)),
			testdatagen.NewSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blop", "foo", "bazz"), Start.Add(-lookbackDelta), End, Step, testdatagen.Factor(7)),
			testdatagen.NewSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blap", "foo", "buzz"), Start.Add(-lookbackDelta), End, Step, testdatagen.Factor(12)),
			testdatagen.NewSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blap", "foo", "bozz"), Start.Add(-lookbackDelta), End, Step, testdatagen.Factor(11)),
			testdatagen.NewSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blop", "foo", "buzz"), Start.Add(-lookbackDelta), End, Step, testdatagen.Factor(8)),
			testdatagen.NewSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blap", "foo", "bazz"), Start.Add(-lookbackDelta), End, Step, testdatagen.ArithmeticSequence(10)),
		})

		testQueryShardingFunctionCorrectness(t, runTestCase, queryableFloats, append(testsForBoth, testsForFloatsOnly...), testsForNativeHistogramsOnly)
	})

	t.Run("native histograms", func(t *testing.T) {
		queryableNativeHistograms := testdatagen.StorageSeriesQueryable([]storage.Series{
			testdatagen.NewNativeHistogramSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blop", "foo", "barr"), Start.Add(-lookbackDelta), End, Step, testdatagen.Factor(5)),
			testdatagen.NewNativeHistogramSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blop", "foo", "bazz"), Start.Add(-lookbackDelta), End, Step, testdatagen.Factor(7)),
			testdatagen.NewNativeHistogramSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blap", "foo", "buzz"), Start.Add(-lookbackDelta), End, Step, testdatagen.Factor(12)),
			testdatagen.NewNativeHistogramSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blap", "foo", "bozz"), Start.Add(-lookbackDelta), End, Step, testdatagen.Factor(11)),
			testdatagen.NewNativeHistogramSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blop", "foo", "buzz"), Start.Add(-lookbackDelta), End, Step, testdatagen.Factor(8)),
			testdatagen.NewNativeHistogramSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blap", "foo", "bazz"), Start.Add(-lookbackDelta), End, Step, testdatagen.ArithmeticSequence(10)),
		})

		testQueryShardingFunctionCorrectness(t, runTestCase, queryableNativeHistograms, append(testsForBoth, testsForNativeHistogramsOnly...), testsForFloatsOnly)
	})
}

func testQueryShardingFunctionCorrectness(t *testing.T, runTestCase functionCorrectnessTestRunner, queryable storage.Queryable, tests []queryShardingFunctionCorrectnessTest, testsToIgnore []queryShardingFunctionCorrectnessTest) {
	mkQueries := func(tpl, fn string, testMatrix bool, fArgs []string) []string {
		if tpl == "" {
			tpl = `(<fn>(bar1{}<args>))`
		}
		result := strings.ReplaceAll(tpl, "<fn>", fn)

		if testMatrix {
			// turn selectors into ranges
			result = strings.ReplaceAll(result, "}", "}[1m]")
		}

		if len(fArgs) > 0 {
			args := "," + strings.Join(fArgs, ",")
			result = strings.ReplaceAll(result, "<args>", args)
		} else {
			result = strings.ReplaceAll(result, "<args>", "")
		}

		return []string{
			result,
			"sum" + result,
			"sum by (bar)" + result,
			"count" + result,
			"count by (bar)" + result,
		}
	}
	for _, tc := range tests {
		const numShards = 4
		for _, query := range mkQueries(tc.tpl, tc.fn, tc.rangeQuery, tc.args) {
			t.Run(query, func(t *testing.T) {
				runTestCase(t, query, numShards, tc.allowedErr, queryable)
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
	for _, tc := range testsToIgnore {
		fnToIgnore[tc.fn] = struct{}{}
	}

	for expectedFn := range promql.FunctionCalls {
		if _, ok := fnToIgnore[expectedFn]; ok {
			continue
		}
		// It's OK if it's tested. Ignore if it's one of the non parallelizable functions.
		_, ok := testedFns[expectedFn]
		assert.Truef(t, ok || slices.Contains(astmapper.NonParallelFuncs, expectedFn), "%s should be tested", expectedFn)
	}
}
