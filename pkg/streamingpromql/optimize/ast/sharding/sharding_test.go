// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/promql_test.go
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/querysharding_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package sharding

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
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

// This test is identical to the one of the same name in the querymiddleware package, but it is kept here as we can't
// test the sharding optimisation pass from the querymiddleware package as it would create a circular dependency.
//
// Once we only support sharding running inside MQE, all of the sharding-related tests in the querymiddleware package
// should be moved here.
func TestQuerySharding_Correctness(t *testing.T) {
	var (
		numSeries                = 1000
		numStaleSeries           = 100
		numConvHistograms        = 1000
		numStaleConvHistograms   = 100
		histogramBuckets         = []float64{1.0, 2.0, 4.0, 10.0, 100.0, math.Inf(1)}
		numNativeHistograms      = 1000
		numStaleNativeHistograms = 100
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
		"sum() offset": {
			query:                  `sum(metric_counter offset 5s)`,
			expectedShardedQueries: 1,
		},
		"sum() negative offset": {
			query:                  `sum(metric_counter offset -5s)`,
			expectedShardedQueries: 1,
		},
		"sum() offset aritmetic": {
			query:                  `sum(metric_counter offset (10s - 5s))`,
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
		"sum(rate()) range aritmetic": {
			query:                  `sum(rate(metric_counter[30s+30s]))`,
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
		`group by (group_1) (metric_counter)`: {
			query:                  `group by (group_1) (metric_counter)`,
			expectedShardedQueries: 1,
		},
		`group by (group_1) (group by (group_1, group_2) (metric_counter))`: {
			query:                  `group by (group_1) (group by (group_1, group_2) (metric_counter))`,
			expectedShardedQueries: 1,
		},
		`count by (group_1) (group by (group_1, group_2) (metric_counter))`: {
			query:                  `count by (group_1) (group by (group_1, group_2) (metric_counter))`,
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
		"sum by(unique) * on (unique) group_left (group_1) avg by (unique, group_1)": {
			// ensure that avg transformation into sum/count does not break label matching in previous binop.
			query: `
				sum by(unique) (metric_counter)
				*
				on (unique) group_left (group_1)
				avg by (unique, group_1) (metric_counter)`,
			expectedShardedQueries: 3,
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
		"@ modifier and negative offset": {
			query:                  `sum by (group_1)(rate(metric_counter[1h] @ start() offset -1m))`,
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
			query:                  `max by(unique) (max_over_time(metric_counter[5m])) > scalar(min(metric_counter))`,
			expectedShardedQueries: 2,
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
		"outer subquery on top of sum": {
			query:                  `sum(metric_counter) by (group_1)[5m:1m]`,
			expectedShardedQueries: 0,
			noRangeQuery:           true,
		},
		"outer subquery on top of avg": {
			query:                  `avg(metric_counter) by (group_1)[5m:1m]`,
			expectedShardedQueries: 0,
			noRangeQuery:           true,
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
		"histogram_quantile no grouping": {
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
		"day_of_month() >= 1 and day_of_month()": {
			query:                  `day_of_month() >= 1 and day_of_month()`,
			expectedShardedQueries: 0,
		},
		"month() >= 1 and month()": {
			query:                  `month() >= 1 and month()`,
			expectedShardedQueries: 0,
		},
		"vector(1) > 0 and vector(1)": {
			query:                  `vector(1) > 0 and vector(1)`,
			expectedShardedQueries: 0,
		},
		"sum(metric_counter) > 0 and vector(1)": {
			query:                  `sum(metric_counter) > 0 and vector(1)`,
			expectedShardedQueries: 1,
		},
		"vector(1)": {
			query:                  `vector(1)`,
			expectedShardedQueries: 0,
		},
		"time()": {
			query:                  `time()`,
			expectedShardedQueries: 0,
		},
		"month(sum(metric_counter))": {
			query:                  `month(sum(metric_counter))`,
			expectedShardedQueries: 1, // Sharded because the contents of `sum()` is sharded.
		},
		"month(sum(metric_counter)) > 0 and vector(1)": {
			query:                  `month(sum(metric_counter)) > 0 and vector(1)`,
			expectedShardedQueries: 1, // Sharded because the contents of `sum()` is sharded.
		},
		"0 < bool 1": {
			query:                  `0 < bool 1`,
			expectedShardedQueries: 0,
		},
		"scalar(metric_counter{const=\"fixed\"}) < bool 1": {
			query:                  `scalar(metric_counter{const="fixed"}) < bool 1`,
			expectedShardedQueries: 0,
		},
		"scalar(sum(metric_counter)) < bool 1": {
			query:                  `scalar(sum(metric_counter)) < bool 1`,
			expectedShardedQueries: 1,
		},
		// Summing floats and native histograms together makes no sense, see
		// https://prometheus.io/docs/prometheus/latest/querying/operators/#operators-for-native-histograms
		// so we exclude native histograms here and in some subsequent tests
		`sum({__name__!=""}) excluding native histograms`: {
			query:                  `sum({__name__!="",__name__!="metric_native_histogram"})`,
			expectedShardedQueries: 1,
		},
		`sum by (group_1) ({__name__!=""}) excluding native histograms`: {
			query:                  `sum by (group_1) ({__name__!="",__name__!="metric_native_histogram"})`,
			expectedShardedQueries: 1,
		},
		`sum by (group_1) (count_over_time({__name__!=""}[1m])) excluding native histograms`: {
			query:                  `sum by (group_1) (count_over_time({__name__!="",__name__!="metric_native_histogram"}[1m]))`,
			expectedShardedQueries: 1,
		},
		`sum(metric_native_histogram)`: {
			query:                  `sum(metric_native_histogram)`,
			expectedShardedQueries: 1,
		},
		`sum(histogram_sum(metric_native_histogram))`: {
			query:                  `sum(histogram_sum(metric_native_histogram))`,
			expectedShardedQueries: 1,
		},
		`sum by (group_1) (metric_native_histogram)`: {
			query:                  `sum by (group_1) (metric_native_histogram)`,
			expectedShardedQueries: 1,
		},
		`sum by (group_1) (count_over_time(metric_native_histogram[1m]))`: {
			query:                  `sum by (group_1) (count_over_time(metric_native_histogram[1m]))`,
			expectedShardedQueries: 1,
		},
		`count(metric_native_histogram)`: {
			query:                  `count(metric_native_histogram)`,
			expectedShardedQueries: 1,
		},
		`count by (group_1) (metric_native_histogram)`: {
			query:                  `count by (group_1) (metric_native_histogram)`,
			expectedShardedQueries: 1,
		},
		`count by (group_1) (count_over_time(metric_native_histogram[1m]))`: {
			query:                  `count by (group_1) (count_over_time(metric_native_histogram[1m]))`,
			expectedShardedQueries: 1,
		},
		`histogram_sum(sum(metric_native_histogram))`: {
			query:                  `histogram_sum(sum(metric_native_histogram))`,
			expectedShardedQueries: 1,
		},
		`histogram_count(sum(metric_native_histogram))`: {
			query:                  `histogram_count(sum(metric_native_histogram))`,
			expectedShardedQueries: 1,
		},
		`histogram_quantile(0.5, sum(metric_native_histogram))`: {
			query:                  `histogram_quantile(0.5, sum(metric_native_histogram))`,
			expectedShardedQueries: 1,
		},
		`histogram_fraction(0, 0.5, sum(metric_native_histogram))`: {
			query:                  `histogram_fraction(0, 0.5, sum(metric_native_histogram))`,
			expectedShardedQueries: 1,
		},
		`histogram_stdvar`: {
			query:                  `histogram_stdvar(metric_native_histogram)`,
			expectedShardedQueries: 0,
		},
		`histogram_stdvar on sum of metrics`: {
			query:                  `histogram_stdvar(sum(metric_native_histogram))`,
			expectedShardedQueries: 1,
		},
		`histogram_stddev`: {
			query:                  `histogram_stddev(metric_native_histogram)`,
			expectedShardedQueries: 0,
		},
		`histogram_stddev on sum of metrics`: {
			query:                  `histogram_stddev(sum(metric_native_histogram))`,
			expectedShardedQueries: 1,
		},
	}

	series := make([]storage.Series, 0, numSeries+(numConvHistograms*len(histogramBuckets))+numNativeHistograms)
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

	// Add conventional histogram series.
	for i := 0; i < numConvHistograms; i++ {
		for bucketIdx, bucketLe := range histogramBuckets {
			// We expect each bucket to have a value higher than the previous one.
			gen := factor(float64(i) * float64(bucketIdx) * 0.1)
			if i >= numConvHistograms-numStaleConvHistograms {
				// Wrap the generator to inject the staleness marker between minute 10 and 20.
				gen = stale(start.Add(10*time.Minute), start.Add(20*time.Minute), gen)
			}

			series = append(series, newSeries(newTestConventionalHistogramLabels(seriesID, bucketLe),
				start.Add(-lookbackDelta), end, step, gen))
		}

		// Increase the series ID after all per-bucket series have been created.
		seriesID++
	}

	// Add native histogram series.
	for i := 0; i < numNativeHistograms; i++ {
		gen := factor(float64(i) * 0.5)
		if i >= numNativeHistograms-numStaleNativeHistograms {
			// Wrap the generator to inject the staleness marker between minute 10 and 20.
			gen = stale(start.Add(10*time.Minute), start.Add(20*time.Minute), gen)
		}

		series = append(series, newNativeHistogramSeries(newTestNativeHistogramLabels(seriesID), start.Add(-lookbackDelta), end, step, gen))
		seriesID++
	}

	// Create a queryable on the fixtures.
	queryable := storageSeriesQueryable(series)

	createEngine := func(t *testing.T, shardCount int) (promql.QueryEngine, *prometheus.Registry) {
		reg := prometheus.NewPedanticRegistry()
		opts := streamingpromql.NewTestEngineOpts()
		opts.CommonOpts.Reg = reg
		planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
		require.NoError(t, err)

		if shardCount > 0 {
			limits := &mockLimits{totalShards: shardCount}
			planner.RegisterASTOptimizationPass(NewOptimizationPass(limits, 0, reg, log.NewNopLogger()))
		}

		engine, err := streamingpromql.NewEngine(opts, streamingpromql.NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(reg), planner)
		require.NoError(t, err)

		return engine, reg
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			generators := map[string]func(t *testing.T, ctx context.Context, engine promql.QueryEngine) promql.Query{
				"instant query": func(t *testing.T, ctx context.Context, engine promql.QueryEngine) promql.Query {
					q, err := engine.NewInstantQuery(ctx, queryable, nil, testData.query, end)
					require.NoError(t, err)
					return q
				},
			}

			if !testData.noRangeQuery {
				generators["range query"] = func(t *testing.T, ctx context.Context, engine promql.QueryEngine) promql.Query {
					q, err := engine.NewRangeQuery(ctx, queryable, nil, testData.query, start, end, step)
					require.NoError(t, err)
					return q
				}
			}

			for name, generator := range generators {
				t.Run(name, func(t *testing.T) {
					ctx := user.InjectOrgID(context.Background(), "test-user")

					// Run the query without sharding.
					unshardedEngine, _ := createEngine(t, 0)
					unshardedQuery := generator(t, ctx, unshardedEngine)
					unshardedResult := unshardedQuery.Exec(ctx)
					require.NoError(t, unshardedResult.Err)

					// Ensure the query produces some results.
					require.NotEmpty(t, unshardedResult.Value)
					requireValidSamples(t, unshardedResult.Value)

					for _, numShards := range []int{2, 4, 8, 16} {
						t.Run(fmt.Sprintf("shards=%d", numShards), func(t *testing.T) {
							shardedEngine, reg := createEngine(t, numShards)

							// Run the query with sharding.
							shardedQuery := generator(t, ctx, shardedEngine)
							shardedResult := shardedQuery.Exec(ctx)
							require.NoError(t, shardedResult.Err)

							// Ensure the two results match (float precision can slightly differ, there's no guarantee in PromQL engine too
							// if you rerun the same query twice).
							testutils.RequireEqualResults(t, testData.query, unshardedResult, shardedResult, false)

							// Ensure the query has been sharded/not sharded as expected.
							expectedSharded := 0
							if testData.expectedShardedQueries > 0 {
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

// newTestCounterLabels generates series labels for a counter metric used in tests.
func newTestCounterLabels(id int) labels.Labels {
	return labels.FromStrings(
		"__name__", "metric_counter",
		"const", "fixed", // A constant label.
		"unique", strconv.Itoa(id), // A unique label.
		"group_1", strconv.Itoa(id%10), // A first grouping label.
		"group_2", strconv.Itoa(id%3), // A second grouping label.
	)
}

// newTestConventionalHistogramLabels generates series labels for a conventional histogram metric used in tests.
func newTestConventionalHistogramLabels(id int, bucketLe float64) labels.Labels {
	return labels.FromStrings(
		"__name__", "metric_histogram_bucket",
		"le", fmt.Sprintf("%f", bucketLe),
		"const", "fixed", // A constant label.
		"unique", strconv.Itoa(id), // A unique label.
		"group_1", strconv.Itoa(id%10), // A first grouping label.
		"group_2", strconv.Itoa(id%3), // A second grouping label.
	)
}

// newTestNativeHistogramLabels generates series labels for a native histogram metric used in tests.
func newTestNativeHistogramLabels(id int) labels.Labels {
	return labels.FromStrings(
		"__name__", "metric_native_histogram",
		"const", "fixed", // A constant label.
		"unique", strconv.Itoa(id), // A unique label.
		"group_1", strconv.Itoa(id%10), // A first grouping label.
		"group_2", strconv.Itoa(id%3), // A second grouping label.
	)
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
	return func(int64) float64 {
		return value
	}
}

type seriesIteratorMock struct {
	idx    int
	series []storage.Series
}

func newSeriesIteratorMock(series []storage.Series) *seriesIteratorMock {
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

func (i *seriesIteratorMock) Warnings() annotations.Annotations {
	return nil
}

func newSeries(metric labels.Labels, from, to time.Time, step time.Duration, gen generator) storage.Series {
	return newSeriesInner(metric, from, to, step, gen, false)
}

func newNativeHistogramSeries(metric labels.Labels, from, to time.Time, step time.Duration, gen generator) storage.Series {
	return newSeriesInner(metric, from, to, step, gen, true)
}

func newSeriesInner(metric labels.Labels, from, to time.Time, step time.Duration, gen generator, histogram bool) storage.Series {
	var (
		floats     []promql.FPoint
		histograms []promql.HPoint
		prevValue  *float64
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

		if histogram {
			histograms = append(histograms, promql.HPoint{
				T: t,
				H: generateTestHistogram(v),
			})
		} else {
			floats = append(floats, promql.FPoint{
				T: t,
				F: v,
			})
		}
	}

	return NewThreadSafeStorageSeries(promql.Series{
		Metric:     metric,
		Floats:     floats,
		Histograms: histograms,
	})
}

func generateTestHistogram(v float64) *histogram.FloatHistogram {
	//based on util_test.GenerateTestFloatHistogram(int(v)) but without converting to int
	h := &histogram.FloatHistogram{
		Count:         10 + (v * 8),
		ZeroCount:     2 + v,
		ZeroThreshold: 0.001,
		Sum:           18.4 * (v + 1),
		Schema:        1,
		PositiveSpans: []histogram.Span{
			{Offset: 0, Length: 2},
			{Offset: 1, Length: 2},
		},
		PositiveBuckets: []float64{v + 1, v + 2, v + 1, v + 1},
		NegativeSpans: []histogram.Span{
			{Offset: 0, Length: 2},
			{Offset: 1, Length: 2},
		},
		NegativeBuckets: []float64{v + 1, v + 2, v + 1, v + 1},
	}
	if value.IsStaleNaN(v) {
		h.Sum = v
	}
	return h
}

// Usually series are read by a single engine in a single goroutine but in
// sharding tests we have multiple engines in multiple goroutines. Thus we
// need a series iterator that doesn't share pointers between goroutines.
type ThreadSafeStorageSeries struct {
	storageSeries *promql.StorageSeries
}

// NewStorageSeries returns a StorageSeries from a Series.
func NewThreadSafeStorageSeries(series promql.Series) *ThreadSafeStorageSeries {
	return &ThreadSafeStorageSeries{
		storageSeries: promql.NewStorageSeries(series),
	}
}

func (ss *ThreadSafeStorageSeries) Labels() labels.Labels {
	return ss.storageSeries.Labels()
}

// Iterator returns a new iterator of the data of the series. In case of
// multiple samples with the same timestamp, it returns the float samples first.
func (ss *ThreadSafeStorageSeries) Iterator(it chunkenc.Iterator) chunkenc.Iterator {
	if ssi, ok := it.(*ThreadSafeStorageSeriesIterator); ok {
		return &ThreadSafeStorageSeriesIterator{underlying: ss.storageSeries.Iterator(ssi.underlying)}
	}
	return &ThreadSafeStorageSeriesIterator{underlying: ss.storageSeries.Iterator(nil)}
}

type ThreadSafeStorageSeriesIterator struct {
	underlying chunkenc.Iterator
}

func (ssi *ThreadSafeStorageSeriesIterator) Seek(t int64) chunkenc.ValueType {
	return ssi.underlying.Seek(t)
}

func (ssi *ThreadSafeStorageSeriesIterator) At() (t int64, v float64) {
	return ssi.underlying.At()
}

func (ssi *ThreadSafeStorageSeriesIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	panic(errors.New("storageSeriesIterator: AtHistogram not supported"))
}

// AtFloatHistogram returns the timestamp and the float histogram at the current position.
// This is different from the underlying iterator in that it does a copy so that the user
// can modify the returned histogram without affecting the underlying series.
func (ssi *ThreadSafeStorageSeriesIterator) AtFloatHistogram(toFH *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	t, fh := ssi.underlying.AtFloatHistogram(nil)
	if toFH == nil {
		return t, fh.Copy()
	}
	fh.CopyTo(toFH)
	return t, toFH
}

func (ssi *ThreadSafeStorageSeriesIterator) AtT() int64 {
	return ssi.underlying.AtT()
}

func (ssi *ThreadSafeStorageSeriesIterator) Next() chunkenc.ValueType {
	return ssi.underlying.Next()
}

func (ssi *ThreadSafeStorageSeriesIterator) Err() error {
	return nil
}

func storageSeriesQueryable(series []storage.Series) storage.Queryable {
	return storage.QueryableFunc(func(int64, int64) (storage.Querier, error) {
		return &querierMock{series: series}, nil
	})
}

type querierMock struct {
	series []storage.Series
}

func (m *querierMock) Select(_ context.Context, sorted bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	shard, matchers, err := sharding.RemoveShardFromMatchers(matchers)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	// Filter series by label matchers.
	var filtered []storage.Series

	for _, series := range m.series {
		if seriesMatches(series, matchers...) {
			filtered = append(filtered, series)
		}
	}

	// Filter series by shard (if any)
	filtered = filterSeriesByShard(filtered, shard)

	// Honor the sorting.
	if sorted {
		slices.SortFunc(filtered, func(a, b storage.Series) int {
			return labels.Compare(a.Labels(), b.Labels())
		})
	}

	return newSeriesIteratorMock(filtered)
}

func (m *querierMock) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (m *querierMock) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (m *querierMock) Close() error { return nil }

func seriesMatches(series storage.Series, matchers ...*labels.Matcher) bool {
	for _, m := range matchers {
		if !m.Matches(series.Labels().Get(m.Name)) {
			return false
		}
	}

	return true
}

func filterSeriesByShard(series []storage.Series, shard *sharding.ShardSelector) []storage.Series {
	if shard == nil {
		return series
	}

	var filtered []storage.Series

	for _, s := range series {
		if labels.StableHash(s.Labels())%shard.ShardCount == shard.ShardIndex {
			filtered = append(filtered, s)
		}
	}

	return filtered
}

// requireValidSamples ensures the query produces some results which are not NaN.
func requireValidSamples(t *testing.T, result parser.Value) {
	t.Helper()

	switch result := result.(type) {
	case promql.Matrix:
		for _, series := range result {
			for _, f := range series.Floats {
				if !math.IsNaN(f.F) {
					return
				}
			}

			for _, h := range series.Histograms {
				if !math.IsNaN(h.H.Sum) {
					return
				}
			}
		}

	case promql.Vector:
		for _, series := range result {
			if series.H != nil && !math.IsNaN(series.H.Sum) {
				return
			}

			if series.H == nil && !math.IsNaN(series.F) {
				return
			}
		}

	case promql.Scalar:
		if !math.IsNaN(result.V) {
			return
		}

	case promql.String:
		return

	default:
		require.Fail(t, "unexpected result type", "expected Matrix or Vector, got %T", result)
	}

	t.Fatalf("Result should have some not-NaN samples")
}
