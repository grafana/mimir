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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/grafana/regexp"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
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
	// TODO: Propagate annotations
	// require.ElementsMatch(t, a.Infos, b.Infos, "expected same info annotations")
	// require.ElementsMatch(t, a.Warnings, b.Warnings, "expected same warning annotations")
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
		require.InEpsilonf(t, expectedVal, actualVal, tolerance, "%s value at position %d with timestamp %d for series %s", sampleType, j, expectedTs, labels)
	}
}

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

		// expectedSubqueries is the number of subqueries that are expected to be spun off.
		expectedSpunOffSubqueries int

		// expectSpecificOrder disables result sorting and checks that both results are returned in same order.
		expectSpecificOrder bool

		// noRangeQuery skips the range query (specially made for "string" query as it can't be used for a range query)
		noRangeQuery bool

		// spinOffSubqueries means that subqueries should be rerun on the full range handler
		spinOffSubqueries bool
	}{
		"aggregation over subquery": {
			query:                     `sum_over_time(max(metric_counter)[5m:1m])`,
			spinOffSubqueries:         true,
			noRangeQuery:              true,
			expectedShardedQueries:    1, // max() is sharded
			expectedSpunOffSubqueries: 1,
		},
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
		"triple subquery (with subquery spin off)": {
			query: `max_over_time(
							stddev_over_time(
								deriv(
									rate(metric_counter[10m])
								[5m:1m])
							[2m:])
						[10m:])`,
			expectedShardedQueries:    1,
			expectedSpunOffSubqueries: 1,
			spinOffSubqueries:         true,
			noRangeQuery:              true,
		},
		"double subquery deriv": {
			query:                  `max_over_time( deriv( rate(metric_counter[10m])[5m:1m] )[10m:] )`,
			expectedShardedQueries: 1,
		},
		"double subquery deriv (with subquery spin off)": {
			query:                     `max_over_time( deriv( rate(metric_counter[10m])[5m:1m] )[10m:] )`,
			expectedShardedQueries:    1,
			expectedSpunOffSubqueries: 1,
			spinOffSubqueries:         true,
			noRangeQuery:              true,
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

	series := make([]*promql.StorageSeries, 0, numSeries+(numConvHistograms*len(histogramBuckets))+numNativeHistograms)
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
		gen := factor(float64(i) * 0.1)
		if i >= numNativeHistograms-numStaleNativeHistograms {
			// Wrap the generator to inject the staleness marker between minute 10 and 20.
			gen = stale(start.Add(10*time.Minute), start.Add(20*time.Minute), gen)
		}

		series = append(series, newNativeHistogramSeries(newTestNativeHistogramLabels(seriesID), start.Add(-lookbackDelta), end, step, gen))
		seriesID++
	}

	// Create a queryable on the fixtures.
	queryable := storageSeriesQueryable(series)

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			reqs := []MetricsQueryRequest{}
			reqs = append(reqs, &PrometheusInstantQueryRequest{
				path:      instantQueryPathSuffix,
				time:      util.TimeToMillis(end),
				queryExpr: parseQuery(t, testData.query),
			})
			if !testData.noRangeQuery {
				reqs = append(reqs, &PrometheusRangeQueryRequest{
					path:      queryRangePathSuffix,
					start:     util.TimeToMillis(start),
					end:       util.TimeToMillis(end),
					step:      step.Milliseconds(),
					queryExpr: parseQuery(t, testData.query),
				})
			}

			for _, req := range reqs {
				t.Run(fmt.Sprintf("%T", req), func(t *testing.T) {
					engine := newEngine()
					downstream := &downstreamHandler{
						engine:                                  engine,
						queryable:                               queryable,
						includePositionInformationInAnnotations: true,
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

					// Remove position information from annotations, to mirror what we expect from the sharded queries below.
					removeAllAnnotationPositionInformation(expectedPrometheusRes.Infos)
					removeAllAnnotationPositionInformation(expectedPrometheusRes.Warnings)

					for _, numShards := range []int{2, 4, 8, 16} {
						t.Run(fmt.Sprintf("shards=%d", numShards), func(t *testing.T) {
							reg := prometheus.NewPedanticRegistry()

							shardingware := newQueryShardingMiddleware(
								log.NewNopLogger(),
								engine,
								defaultStepFunc,
								mockLimits{totalShards: numShards},
								0,
								reg,
							)

							fullHandler := shardingware.Wrap(downstream)
							ctx := context.Background()
							if testData.spinOffSubqueries {
								ctx = context.WithValue(ctx, fullRangeHandlerContextKey, fullHandler)
							}

							// Run the query with sharding.
							shardedRes, err := fullHandler.Do(user.InjectOrgID(ctx, "test"), req)
							require.Nil(t, err)

							// Ensure the two results matches (float precision can slightly differ, there's no guarantee in PromQL engine too
							// if you rerun the same query twice).
							shardedPrometheusRes := shardedRes.(*PrometheusResponse)
							if !testData.expectSpecificOrder {
								sort.Sort(byLabels(shardedPrometheusRes.Data.Result))
							}
							removeAllAnnotationPositionInformation(shardedPrometheusRes.Infos)
							approximatelyEquals(t, expectedPrometheusRes, shardedPrometheusRes)

							// Ensure the query has been sharded/not sharded as expected.
							expectedAttempts := 1 + testData.expectedSpunOffSubqueries
							expectedSharded := testData.expectedSpunOffSubqueries
							if testData.expectedShardedQueries > 0 {
								expectedSharded += 1
							}

							assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP cortex_frontend_query_sharding_rewrites_attempted_total Total number of queries the query-frontend attempted to shard.
					# TYPE cortex_frontend_query_sharding_rewrites_attempted_total counter
					cortex_frontend_query_sharding_rewrites_attempted_total %d
					# HELP cortex_frontend_query_sharding_rewrites_succeeded_total Total number of queries the query-frontend successfully rewritten in a shardable way.
					# TYPE cortex_frontend_query_sharding_rewrites_succeeded_total counter
					cortex_frontend_query_sharding_rewrites_succeeded_total %d
					# HELP cortex_frontend_sharded_queries_total Total number of sharded queries.
					# TYPE cortex_frontend_sharded_queries_total counter
					cortex_frontend_sharded_queries_total %d
					# HELP cortex_frontend_query_sharding_spun_off_subqueries_total Total number of subqueries spun off as range queries.
					# TYPE cortex_frontend_query_sharding_spun_off_subqueries_total counter
					cortex_frontend_query_sharding_spun_off_subqueries_total %d
				`, expectedAttempts, expectedSharded, testData.expectedShardedQueries*numShards, testData.expectedSpunOffSubqueries)),
								"cortex_frontend_query_sharding_rewrites_attempted_total",
								"cortex_frontend_query_sharding_rewrites_succeeded_total",
								"cortex_frontend_sharded_queries_total",
								"cortex_frontend_query_sharding_spun_off_subqueries_total",
							))
						})
					}
				})
			}
		})
	}
}

func TestQuerySharding_NonMonotonicHistogramBuckets(t *testing.T) {
	queries := []string{
		`histogram_quantile(1, sum by(le) (rate(metric_histogram_bucket[1m])))`,
	}

	series := []*promql.StorageSeries{}
	for i := 0; i < 100; i++ {
		series = append(series, newSeries(labels.FromStrings(labels.MetricName, "metric_histogram_bucket", "app", strconv.Itoa(i), "le", "10"), start.Add(-lookbackDelta), end, step, arithmeticSequence(1)))
		series = append(series, newSeries(labels.FromStrings(labels.MetricName, "metric_histogram_bucket", "app", strconv.Itoa(i), "le", "20"), start.Add(-lookbackDelta), end, step, arithmeticSequence(3)))
		series = append(series, newSeries(labels.FromStrings(labels.MetricName, "metric_histogram_bucket", "app", strconv.Itoa(i), "le", "30"), start.Add(-lookbackDelta), end, step, arithmeticSequence(3)))
		series = append(series, newSeries(labels.FromStrings(labels.MetricName, "metric_histogram_bucket", "app", strconv.Itoa(i), "le", "40"), start.Add(-lookbackDelta), end, step, arithmeticSequence(3)))
		series = append(series, newSeries(labels.FromStrings(labels.MetricName, "metric_histogram_bucket", "app", strconv.Itoa(i), "le", "+Inf"), start.Add(-lookbackDelta), end, step, arithmeticSequence(3)))
	}

	// Create a queryable on the fixtures.
	queryable := storageSeriesQueryable(series)

	engine := newEngine()
	downstream := &downstreamHandler{
		engine:    engine,
		queryable: queryable,
	}

	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			req := &PrometheusRangeQueryRequest{
				path:      queryRangePathSuffix,
				start:     util.TimeToMillis(start),
				end:       util.TimeToMillis(end),
				step:      step.Milliseconds(),
				queryExpr: parseQuery(t, query),
			}

			// Run the query without sharding.
			expectedRes, err := downstream.Do(context.Background(), req)
			require.Nil(t, err)

			expectedPrometheusRes := expectedRes.(*PrometheusResponse)
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
						engine,
						nil,
						mockLimits{totalShards: numShards},
						0,
						reg,
					)

					// Run the query with sharding.
					shardedRes, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
					require.Nil(t, err)

					// Ensure the two results matches (float precision can slightly differ, there's no guarantee in PromQL engine too
					// if you rerun the same query twice).
					shardedPrometheusRes := shardedRes.(*PrometheusResponse)
					sort.Sort(byLabels(shardedPrometheusRes.Data.Result))
					approximatelyEquals(t, expectedPrometheusRes, shardedPrometheusRes)

					// Ensure the warning about bucket monotonicity from PromQL engine is hidden.
					require.Len(t, shardedPrometheusRes.GetWarnings(), 0)
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

	labelsForShard := labelsForShardsGenerator([]labels.Label{{Name: labels.MetricName, Value: "metric"}}, shards)
	storageSeries := []*promql.StorageSeries{
		newSeries(labelsForShard(0), from, to, step, constant(evilFloatA)),
		newSeries(labelsForShard(1), from, to, step, constant(evilFloatA)),
		newSeries(labelsForShard(2), from, to, step, constant(evilFloatB)),
	}

	shardingware := newQueryShardingMiddleware(log.NewNopLogger(), newEngine(), nil, mockLimits{totalShards: shards}, 0, prometheus.NewPedanticRegistry())
	downstream := &downstreamHandler{engine: newEngine(), queryable: storageSeriesQueryable(storageSeries)}

	req := &PrometheusInstantQueryRequest{
		path:      instantQueryPathSuffix,
		time:      to.UnixMilli(),
		queryExpr: parseQuery(t, `sum(metric)`),
	}

	var lastVal float64
	for i := 0; i <= 100; i++ {
		shardedRes, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
		require.NoError(t, err)

		shardedPrometheusRes := shardedRes.(*PrometheusResponse)

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

type queryShardingFunctionCorrectnessTest struct {
	fn         string
	args       []string
	rangeQuery bool
	tpl        string
}

// TestQuerySharding_FunctionCorrectness is the old test that probably at some point inspired the TestQuerySharding_Correctness,
// we keep it here since it adds more test cases.
func TestQuerySharding_FunctionCorrectness(t *testing.T) {
	// We want to test experimental functions too.
	t.Cleanup(func() { parser.EnableExperimentalFunctions = false })
	parser.EnableExperimentalFunctions = true

	testsForBoth := []queryShardingFunctionCorrectnessTest{
		{fn: "count_over_time", rangeQuery: true},
		{fn: "days_in_month"},
		{fn: "day_of_month"},
		{fn: "day_of_week"},
		{fn: "day_of_year"},
		{fn: "delta", rangeQuery: true},
		{fn: "hour"},
		{fn: "increase", rangeQuery: true},
		{fn: "minute"},
		{fn: "month"},
		{fn: "rate", rangeQuery: true},
		{fn: "resets", rangeQuery: true},
		{fn: "sort"},
		{fn: "sort_desc"},
		{fn: "sort_by_label"},
		{fn: "sort_by_label_desc"},
		{fn: "last_over_time", rangeQuery: true},
		{fn: "present_over_time", rangeQuery: true},
		{fn: "timestamp"},
		{fn: "year"},
		{fn: "clamp", args: []string{"5", "10"}},
		{fn: "clamp_max", args: []string{"5"}},
		{fn: "clamp_min", args: []string{"5"}},
		{fn: "round", args: []string{"20"}},
		{fn: "label_replace", args: []string{`"fuzz"`, `"$1"`, `"foo"`, `"b(.*)"`}},
		{fn: "label_join", args: []string{`"fuzz"`, `","`, `"foo"`, `"bar"`}},
	}
	testsForFloatsOnly := []queryShardingFunctionCorrectnessTest{
		{fn: "abs"},
		{fn: "avg_over_time", rangeQuery: true},
		{fn: "ceil"},
		{fn: "changes", rangeQuery: true},
		{fn: "deriv", rangeQuery: true},
		{fn: "exp"},
		{fn: "floor"},
		{fn: "idelta", rangeQuery: true},
		{fn: "irate", rangeQuery: true},
		{fn: "ln"},
		{fn: "log10"},
		{fn: "log2"},
		{fn: "max_over_time", rangeQuery: true},
		{fn: "min_over_time", rangeQuery: true},
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
		{fn: "holt_winters", args: []string{"0.5", "0.7"}, rangeQuery: true},
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
		queryableFloats := storageSeriesQueryable([]*promql.StorageSeries{
			newSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blop", "foo", "barr"), start.Add(-lookbackDelta), end, step, factor(5)),
			newSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blop", "foo", "bazz"), start.Add(-lookbackDelta), end, step, factor(7)),
			newSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blap", "foo", "buzz"), start.Add(-lookbackDelta), end, step, factor(12)),
			newSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blap", "foo", "bozz"), start.Add(-lookbackDelta), end, step, factor(11)),
			newSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blop", "foo", "buzz"), start.Add(-lookbackDelta), end, step, factor(8)),
			newSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blap", "foo", "bazz"), start.Add(-lookbackDelta), end, step, arithmeticSequence(10)),
		})
		testQueryShardingFunctionCorrectness(t, queryableFloats, append(testsForBoth, testsForFloatsOnly...), testsForNativeHistogramsOnly)
	})

	t.Run("native histograms", func(t *testing.T) {
		queryableNativeHistograms := storageSeriesQueryable([]*promql.StorageSeries{
			newNativeHistogramSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blop", "foo", "barr"), start.Add(-lookbackDelta), end, step, factor(5)),
			newNativeHistogramSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blop", "foo", "bazz"), start.Add(-lookbackDelta), end, step, factor(7)),
			newNativeHistogramSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blap", "foo", "buzz"), start.Add(-lookbackDelta), end, step, factor(12)),
			newNativeHistogramSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blap", "foo", "bozz"), start.Add(-lookbackDelta), end, step, factor(11)),
			newNativeHistogramSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blop", "foo", "buzz"), start.Add(-lookbackDelta), end, step, factor(8)),
			newNativeHistogramSeries(labels.FromStrings("__name__", "bar1", "baz", "blip", "bar", "blap", "foo", "bazz"), start.Add(-lookbackDelta), end, step, arithmeticSequence(10)),
		})

		testQueryShardingFunctionCorrectness(t, queryableNativeHistograms, append(testsForBoth, testsForNativeHistogramsOnly...), testsForFloatsOnly)
	})
}

func testQueryShardingFunctionCorrectness(t *testing.T, queryable storage.Queryable, tests []queryShardingFunctionCorrectnessTest, testsToIgnore []queryShardingFunctionCorrectnessTest) {
	mkQueries := func(tpl, fn string, testMatrix bool, fArgs []string) []string {
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
	}
	for _, tc := range tests {
		const numShards = 4
		for _, query := range mkQueries(tc.tpl, tc.fn, tc.rangeQuery, tc.args) {
			t.Run(query, func(t *testing.T) {
				req := &PrometheusRangeQueryRequest{
					path:      queryRangePathSuffix,
					start:     util.TimeToMillis(start),
					end:       util.TimeToMillis(end),
					step:      step.Milliseconds(),
					queryExpr: parseQuery(t, query),
				}

				reg := prometheus.NewPedanticRegistry()
				engine := newEngine()
				shardingware := newQueryShardingMiddleware(
					log.NewNopLogger(),
					engine,
					nil,
					mockLimits{totalShards: numShards},
					0,
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
	for _, tc := range testsToIgnore {
		fnToIgnore[tc.fn] = struct{}{}
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

func TestQuerySharding_ShouldSkipShardingViaOption(t *testing.T) {
	req := &PrometheusRangeQueryRequest{
		path:      queryRangePathSuffix,
		start:     util.TimeToMillis(start),
		end:       util.TimeToMillis(end),
		step:      step.Milliseconds(),
		queryExpr: parseQuery(t, "sum by (foo) (rate(bar{}[1m]))"), // shardable query.
		options: Options{
			ShardingDisabled: true,
		},
	}

	shardingware := newQueryShardingMiddleware(log.NewNopLogger(), newEngine(), nil, mockLimits{totalShards: 16}, 0, nil)

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
		path:      queryRangePathSuffix,
		start:     util.TimeToMillis(start),
		end:       util.TimeToMillis(end),
		step:      step.Milliseconds(),
		queryExpr: parseQuery(t, "sum by (foo) (rate(bar{}[1m]))"), // shardable query.
		options: Options{
			TotalShards: 128,
		},
	}

	shardingware := newQueryShardingMiddleware(log.NewNopLogger(), newEngine(), nil, mockLimits{totalShards: 16}, 0, nil)

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
		nativeHistograms  bool
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
		"native histograms accepted": {
			query:             "sum(metric) / count(metric)",
			hints:             &Hints{TotalQueries: 3},
			totalShards:       16,
			maxShardedQueries: 64,
			nativeHistograms:  true,
			compactorShards:   10,
			expectedShards:    10,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			req := &PrometheusRangeQueryRequest{
				path:      queryRangePathSuffix,
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
			shardingware := newQueryShardingMiddleware(log.NewNopLogger(), newEngine(), nil, limits, 0, nil)

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
			assert.Equal(t, statusSuccess, res.(*PrometheusResponse).GetStatus())
			assert.Equal(t, testData.expectedShards, len(uniqueShards))
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
			req := &PrometheusRangeQueryRequest{
				path:      queryRangePathSuffix,
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
			shardingware := newQueryShardingMiddleware(log.NewNopLogger(), newEngine(), nil, limits, 0, nil)

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
			assert.Equal(t, statusSuccess, res.(*PrometheusResponse).GetStatus())
			assert.Equal(t, testData.expectedShards, len(uniqueShards))
		})
	}
}

func TestQuerySharding_ShouldReturnErrorOnDownstreamHandlerFailure(t *testing.T) {
	req := &PrometheusRangeQueryRequest{
		path:      queryRangePathSuffix,
		start:     util.TimeToMillis(start),
		end:       util.TimeToMillis(end),
		step:      step.Milliseconds(),
		queryExpr: parseQuery(t, "vector(1)"), // A non shardable query.
	}

	shardingware := newQueryShardingMiddleware(log.NewNopLogger(), newEngine(), nil, mockLimits{totalShards: 16}, 0, nil)

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
			Logger:               log.NewNopLogger(),
			Reg:                  nil,
			MaxSamples:           10e6,
			Timeout:              50 * time.Millisecond,
			ActiveQueryTracker:   nil,
			LookbackDelta:        lookbackDelta,
			EnableAtModifier:     true,
			EnableNegativeOffset: true,
			NoStepSubqueryIntervalFn: func(int64) int64 {
				return int64(1 * time.Minute / (time.Millisecond / time.Nanosecond))
			},
		})
		engineSampleLimit = promql.NewEngine(promql.EngineOpts{
			Logger:               log.NewNopLogger(),
			Reg:                  nil,
			MaxSamples:           1,
			Timeout:              time.Hour,
			ActiveQueryTracker:   nil,
			LookbackDelta:        lookbackDelta,
			EnableAtModifier:     true,
			EnableNegativeOffset: true,
			NoStepSubqueryIntervalFn: func(int64) int64 {
				return int64(1 * time.Minute / (time.Millisecond / time.Nanosecond))
			},
		})
		queryableInternalErr = storage.QueryableFunc(func(int64, int64) (storage.Querier, error) {
			return nil, apierror.New(apierror.TypeInternal, "some internal error")
		})
		queryablePrometheusExecErr = storage.QueryableFunc(func(int64, int64) (storage.Querier, error) {
			return nil, apierror.Newf(apierror.TypeExec, "expanding series: %s", querier.NewMaxQueryLengthError(744*time.Hour, 720*time.Hour))
		})
		queryable = storageSeriesQueryable([]*promql.StorageSeries{
			newSeries(labels.FromStrings("__name__", "bar1"), start.Add(-lookbackDelta), end, step, factor(5)),
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
			name:             "sharding - sample limit",
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
			name:             "downstream - storage internal error",
			engineDownstream: engine,
			engineSharding:   engineSampleLimit,
			queryable:        queryableInternalErr,
			expError:         apierror.New(apierror.TypeInternal, "some internal error"),
		},
		{
			name:             "downstream - storage prometheus execution error",
			engineDownstream: engine,
			engineSharding:   engineSampleLimit,
			queryable:        queryablePrometheusExecErr,
			expError:         apierror.Newf(apierror.TypeExec, "expanding series: %s", querier.NewMaxQueryLengthError(744*time.Hour, 720*time.Hour)),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &PrometheusRangeQueryRequest{
				path:      queryRangePathSuffix,
				start:     util.TimeToMillis(start),
				end:       util.TimeToMillis(end),
				step:      step.Milliseconds(),
				queryExpr: parseQuery(t, "sum(bar1)"),
			}

			shardingware := newQueryShardingMiddleware(log.NewNopLogger(), tc.engineSharding, nil, mockLimits{totalShards: 3}, 0, nil)

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

				// We don't really care about the specific error returned,
				// all we care is that they produce the same http response.
				expResp, ok := apierror.HTTPResponseFromError(tc.expError)
				require.True(t, ok, "expected error should be an api error")

				gotResp, ok := apierror.HTTPResponseFromError(err)
				require.True(t, ok, "got error should be an api error")

				assert.Equal(t, expResp.GetCode(), gotResp.GetCode())
				assert.JSONEq(t, string(expResp.GetBody()), string(gotResp.GetBody()))
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

	queryable := storage.QueryableFunc(func(int64, int64) (storage.Querier, error) {
		return &querierMock{series: series}, nil
	})

	req := &PrometheusRangeQueryRequest{
		path:      queryRangePathSuffix,
		start:     util.TimeToMillis(start),
		end:       util.TimeToMillis(end),
		step:      step.Milliseconds(),
		queryExpr: parseQuery(t, `sum by (group_1) (metric_counter) - on(group_1) group_right(unique) (sum by (group_1,unique) (metric_counter))`),
	}

	downstream := &downstreamHandler{engine: newEngine(), queryable: queryable}
	reg := prometheus.NewPedanticRegistry()
	shardingware := newQueryShardingMiddleware(log.NewNopLogger(), engine, nil, mockLimits{totalShards: numShards}, 0, reg)

	// Run the query with sharding.
	_, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
	assert.Equal(t, apierror.New(apierror.TypeExec, "multiple matches for labels: grouping labels must ensure unique matches"), err)
}

func TestQuerySharding_WrapMultipleTime(t *testing.T) {
	req := &PrometheusRangeQueryRequest{
		path:      queryRangePathSuffix,
		start:     util.TimeToMillis(start),
		end:       util.TimeToMillis(end),
		step:      step.Milliseconds(),
		queryExpr: parseQuery(t, "vector(1)"), // A non shardable query.
	}

	shardingware := newQueryShardingMiddleware(log.NewNopLogger(), newEngine(), nil, mockLimits{totalShards: 16}, 0, prometheus.NewRegistry())

	require.NotPanics(t, func() {
		_, err := shardingware.Wrap(mockHandlerWith(nil, nil)).Do(user.InjectOrgID(context.Background(), "test"), req)
		require.Nil(t, err)
		_, err = shardingware.Wrap(mockHandlerWith(nil, nil)).Do(user.InjectOrgID(context.Background(), "test"), req)
		require.Nil(t, err)
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

			shardingware := newQueryShardingMiddleware(log.NewNopLogger(), newEngine(), nil, mockLimits{totalShards: 16}, 10_000, nil)
			downstream := &mockHandler{}
			downstream.On("Do", mock.Anything, mock.Anything).Return(&PrometheusResponse{
				Status: statusSuccess, Data: &PrometheusData{
					ResultType: string(parser.ValueTypeVector),
				},
			}, nil)

			res, err := shardingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), tt.req)
			require.NoError(t, err)
			assert.Equal(t, statusSuccess, res.(*PrometheusResponse).GetStatus())
			downstream.AssertCalled(t, "Do", mock.Anything, mock.Anything)
			downstream.AssertNumberOfCalls(t, "Do", tt.expectedCalls)
		})
	}

}

func TestQuerySharding_Annotations(t *testing.T) {
	numSeries := 10
	endTime := 100
	storageSeries := make([]*promql.StorageSeries, 0, numSeries)
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
	queryable := storageSeriesQueryable(storageSeries)

	const numShards = 8
	const step = 20 * time.Second
	const splitInterval = 15 * time.Second

	reg := prometheus.NewPedanticRegistry()
	engine := newEngine()
	shardingware := newQueryShardingMiddleware(
		log.NewNopLogger(),
		engine,
		nil,
		mockLimits{totalShards: numShards},
		0,
		reg,
	)
	splitware := newSplitAndCacheMiddleware(
		true,
		false, // Cache disabled.
		splitInterval,
		mockLimits{},
		newTestPrometheusCodec(),
		nil,
		nil,
		nil,
		nil,
		log.NewNopLogger(),
		reg,
	)
	downstream := &downstreamHandler{
		engine:                                  engine,
		queryable:                               queryable,
		includePositionInformationInAnnotations: true,
	}

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
			query := fmt.Sprintf(template.query, seriesName)
			req := &PrometheusRangeQueryRequest{
				path:      queryRangePathSuffix,
				start:     0,
				end:       int64(endTime * 1000),
				step:      step.Milliseconds(),
				queryExpr: parseQuery(t, query),
			}

			injectedContext := user.InjectOrgID(context.Background(), "test")

			// Run the query without sharding.
			expectedRes, err := downstream.Do(injectedContext, req)
			require.Nil(t, err)
			expectedPrometheusRes := expectedRes.(*PrometheusResponse)

			// Ensure the query produces some results.
			require.NotEmpty(t, expectedRes.(*PrometheusResponse).Data.Result)

			// Run the query with sharding.
			shardedRes, err := shardingware.Wrap(downstream).Do(injectedContext, req)
			require.Nil(t, err)

			// Ensure the query produces some results.
			require.NotEmpty(t, shardedRes.(*PrometheusResponse).Data.Result)

			// Run the query with splitting.
			splitRes, err := splitware.Wrap(downstream).Do(injectedContext, req)
			require.Nil(t, err)

			// Ensure the query produces some results.
			require.NotEmpty(t, splitRes.(*PrometheusResponse).Data.Result)

			expected := expectedPrometheusRes.Infos
			actualSharded := shardedRes.(*PrometheusResponse).Infos
			actualSplit := splitRes.(*PrometheusResponse).Infos

			if template.isWarning {
				expected = expectedPrometheusRes.Warnings
				actualSharded = shardedRes.(*PrometheusResponse).Warnings
				actualSplit = splitRes.(*PrometheusResponse).Warnings
			}

			require.NotEmpty(t, expected)
			require.Equal(t, expected, actualSplit)

			if template.isSharded {
				// Remove position information from annotations generated with the unsharded query, to mirror what we expect from the sharded query.
				removeAllAnnotationPositionInformation(expected)
			}

			require.Equal(t, expected, actualSharded)
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
				Logger:               log.NewNopLogger(),
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
				path:      queryRangePathSuffix,
				start:     start,
				end:       end,
				step:      step,
				queryExpr: parseQuery(b, tc.query),
			}

			for _, shardFactor := range shards {
				shardingware := newQueryShardingMiddleware(
					log.NewNopLogger(),
					engine,
					nil,
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
	engine                                  *promql.Engine
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

func storageSeriesQueryable(series []*promql.StorageSeries) storage.Queryable {
	return storage.QueryableFunc(func(int64, int64) (storage.Querier, error) {
		return &querierMock{series: series}, nil
	})
}

type querierMock struct {
	series []*promql.StorageSeries
}

func (m *querierMock) Select(_ context.Context, sorted bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
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

func (m *querierMock) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (m *querierMock) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
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
		if labels.StableHash(s.Labels())%shard.ShardCount == shard.ShardIndex {
			filtered = append(filtered, s)
		}
	}

	return filtered
}

func newSeries(metric labels.Labels, from, to time.Time, step time.Duration, gen generator) *promql.StorageSeries {
	return newSeriesInner(metric, from, to, step, gen, false)
}

func newNativeHistogramSeries(metric labels.Labels, from, to time.Time, step time.Duration, gen generator) *promql.StorageSeries {
	return newSeriesInner(metric, from, to, step, gen, true)
}

func newSeriesInner(metric labels.Labels, from, to time.Time, step time.Duration, gen generator, histogram bool) *promql.StorageSeries {
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

	return promql.NewStorageSeries(promql.Series{
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

func (i *seriesIteratorMock) Warnings() annotations.Annotations {
	return nil
}

var defaultStepFunc = func(int64) int64 {
	return (1 * time.Minute).Milliseconds()
}

// newEngine creates and return a new promql.Engine used for testing.
func newEngine() *promql.Engine {
	return promql.NewEngine(promql.EngineOpts{
		Logger:                   log.NewNopLogger(),
		Reg:                      nil,
		MaxSamples:               10e6,
		Timeout:                  1 * time.Hour,
		ActiveQueryTracker:       nil,
		LookbackDelta:            lookbackDelta,
		EnableAtModifier:         true,
		EnableNegativeOffset:     true,
		NoStepSubqueryIntervalFn: defaultStepFunc,
	})
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
