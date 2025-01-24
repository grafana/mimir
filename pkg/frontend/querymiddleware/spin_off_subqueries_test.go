// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util"
)

func TestSubquerySpinOff_Correctness(t *testing.T) {
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
		query                     string
		expectedSkippedReason     string
		expectedSpunOffSubqueries int
		expectedDownstreamQueries int
		expectSpecificOrder       bool
	}{
		"sum() no grouping": {
			query:                 `sum(metric_counter)`,
			expectedSkippedReason: "no-subquery",
		},
		"sum() offset": {
			query:                 `sum(metric_counter offset 5s)`,
			expectedSkippedReason: "no-subquery",
		},
		"sum() negative offset": {
			query:                 `sum(metric_counter offset -5s)`,
			expectedSkippedReason: "no-subquery",
		},
		"sum() grouping 'by'": {
			query:                 `sum by(group_1) (metric_counter)`,
			expectedSkippedReason: "no-subquery",
		},
		"sum() grouping 'without'": {
			query:                 `sum without(unique) (metric_counter)`,
			expectedSkippedReason: "no-subquery",
		},
		"sum(rate()) no grouping": {
			query:                 `sum(rate(metric_counter[1m]))`,
			expectedSkippedReason: "no-subquery",
		},
		"sum(rate()) grouping 'by'": {
			query:                 `sum by(group_1) (rate(metric_counter[1m]))`,
			expectedSkippedReason: "no-subquery",
		},
		"sum(rate()) grouping 'without'": {
			query:                 `sum without(unique) (rate(metric_counter[1m]))`,
			expectedSkippedReason: "no-subquery",
		},
		"sum(rate()) with no effective grouping because all groups have 1 series": {
			query:                 `sum by(unique) (rate(metric_counter{group_1="0"}[1m]))`,
			expectedSkippedReason: "no-subquery",
		},
		`group by (group_1) (metric_counter)`: {
			query:                 `group by (group_1) (metric_counter)`,
			expectedSkippedReason: "no-subquery",
		},
		`group by (group_1) (group by (group_1, group_2) (metric_counter))`: {
			query:                 `group by (group_1) (group by (group_1, group_2) (metric_counter))`,
			expectedSkippedReason: "no-subquery",
		},
		`count by (group_1) (group by (group_1, group_2) (metric_counter))`: {
			query:                 `count by (group_1) (group by (group_1, group_2) (metric_counter))`,
			expectedSkippedReason: "no-subquery",
		},
		"histogram_quantile() grouping only 'by' le": {
			query:                 `histogram_quantile(0.5, sum by(le) (rate(metric_histogram_bucket[1m])))`,
			expectedSkippedReason: "no-subquery",
		},
		"histogram_quantile() grouping 'by'": {
			query:                 `histogram_quantile(0.5, sum by(group_1, le) (rate(metric_histogram_bucket[1m])))`,
			expectedSkippedReason: "no-subquery",
		},
		"histogram_quantile() grouping 'without'": {
			query:                 `histogram_quantile(0.5, sum without(group_1, group_2, unique) (rate(metric_histogram_bucket[1m])))`,
			expectedSkippedReason: "no-subquery",
		},
		"histogram_quantile() with no effective grouping because all groups have 1 series": {
			query:                 `histogram_quantile(0.5, sum by(unique, le) (rate(metric_histogram_bucket{group_1="0"}[1m])))`,
			expectedSkippedReason: "no-subquery",
		},
		"min() no grouping": {
			query:                 `min(metric_counter{group_1="0"})`,
			expectedSkippedReason: "no-subquery",
		},
		"min() grouping 'by'": {
			query:                 `min by(group_2) (metric_counter{group_1="0"})`,
			expectedSkippedReason: "no-subquery",
		},
		"min() grouping 'without'": {
			query:                 `min without(unique) (metric_counter{group_1="0"})`,
			expectedSkippedReason: "no-subquery",
		},
		"max() no grouping": {
			query:                 `max(metric_counter{group_1="0"})`,
			expectedSkippedReason: "no-subquery",
		},
		"max() grouping 'by'": {
			query:                 `max by(group_2) (metric_counter{group_1="0"})`,
			expectedSkippedReason: "no-subquery",
		},
		"max() grouping 'without'": {
			query:                 `max without(unique) (metric_counter{group_1="0"})`,
			expectedSkippedReason: "no-subquery",
		},
		"count() no grouping": {
			query:                 `count(metric_counter)`,
			expectedSkippedReason: "no-subquery",
		},
		"count() grouping 'by'": {
			query:                 `count by(group_2) (metric_counter)`,
			expectedSkippedReason: "no-subquery",
		},
		"count() grouping 'without'": {
			query:                 `count without(unique) (metric_counter)`,
			expectedSkippedReason: "no-subquery",
		},
		"sum(count())": {
			query:                 `sum(count by(group_1) (metric_counter))`,
			expectedSkippedReason: "no-subquery",
		},
		"avg() no grouping": {
			query:                 `avg(metric_counter)`,
			expectedSkippedReason: "no-subquery", // avg() is parallelized as sum()/count().
		},
		"avg() grouping 'by'": {
			query:                 `avg by(group_2) (metric_counter)`,
			expectedSkippedReason: "no-subquery", // avg() is parallelized as sum()/count().
		},
		"avg() grouping 'without'": {
			query:                 `avg without(unique) (metric_counter)`,
			expectedSkippedReason: "no-subquery", // avg() is parallelized as sum()/count().
		},
		"sum(min_over_time())": {
			query:                 `sum by (group_1, group_2) (min_over_time(metric_counter{const="fixed"}[2m]))`,
			expectedSkippedReason: "no-subquery",
		},
		"sum(max_over_time())": {
			query:                 `sum by (group_1, group_2) (max_over_time(metric_counter{const="fixed"}[2m]))`,
			expectedSkippedReason: "no-subquery",
		},
		"sum(avg_over_time())": {
			query:                 `sum by (group_1, group_2) (avg_over_time(metric_counter{const="fixed"}[2m]))`,
			expectedSkippedReason: "no-subquery",
		},
		"or": {
			query:                 `sum(rate(metric_counter{group_1="0"}[1m])) or sum(rate(metric_counter{group_1="1"}[1m]))`,
			expectedSkippedReason: "no-subquery",
		},
		"and": {
			query: `
				sum without(unique) (rate(metric_counter{group_1="0"}[1m]))
				and
				max without(unique) (metric_counter) > 0`,
			expectedSkippedReason: "no-subquery",
		},
		"sum(rate()) > avg(rate())": {
			query: `
				sum(rate(metric_counter[1m]))
				>
				avg(rate(metric_counter[1m]))`,
			expectedSkippedReason: "no-subquery", // avg() is parallelized as sum()/count().
		},
		"sum by(unique) * on (unique) group_left (group_1) avg by (unique, group_1)": {
			// ensure that avg transformation into sum/count does not break label matching in previous binop.
			query: `
				sum by(unique) (metric_counter)
				*
				on (unique) group_left (group_1)
				avg by (unique, group_1) (metric_counter)`,
			expectedSkippedReason: "no-subquery",
		},
		"sum by (rate()) / 2 ^ 2": {
			query: `
			sum by (group_1) (rate(metric_counter[1m])) / 2 ^ 2`,
			expectedSkippedReason: "no-subquery",
		},
		"sum by (rate()) / time() *2": {
			query: `
			sum by (group_1) (rate(metric_counter[1m])) / time() *2`,
			expectedSkippedReason: "no-subquery",
		},
		"sum(rate()) / vector(3) ^ month()": {
			query:                 `sum(rate(metric_counter[1m])) / vector(3) ^ month()`,
			expectedSkippedReason: "no-subquery",
		},
		"sum(rate(metric_counter[1m])) / vector(3) ^ vector(2) + sum(ln(metric_counter))": {
			query:                 `sum(rate(metric_counter[1m])) / vector(3) ^ vector(2) + sum(ln(metric_counter))`,
			expectedSkippedReason: "no-subquery",
		},
		"nested count()": {
			query: `sum(
				  count(
				    count(metric_counter) by (group_1, group_2)
				  ) by (group_1)
				)`,
			expectedSkippedReason: "no-subquery",
		},
		"subquery max: too short range": {
			query: `max_over_time(
							rate(metric_counter[1m])
						[30m:1m]
					)`,
			expectedSkippedReason: "no-subquery",
		},
		"subquery max: too few steps": {
			query: `max_over_time(
							rate(metric_counter[1m])
						[2h:15m]
					)`,
			expectedSkippedReason: "no-subquery",
		},
		"subquery max": {
			query: `max_over_time(
							rate(metric_counter[1m])
						[3d:1m]
					)`,
			expectedSpunOffSubqueries: 1,
		},
		"subquery min": {
			query: `min_over_time(
							rate(metric_counter[1m])
						[3d:1m]
					)`,
			expectedSpunOffSubqueries: 1,
		},
		"sum of subquery min": {
			query:                     `sum by(group_1) (min_over_time((changes(metric_counter[5m]))[3d:2m]))`,
			expectedSpunOffSubqueries: 1,
		},
		"triple subquery": {
			query: `max_over_time(
						stddev_over_time(
							deriv(
								rate(metric_counter[10m])
							[5m:1m])
						[2m:])
					[3d:])`,
			expectedSpunOffSubqueries: 1,
		},
		"double subquery deriv": {
			query:                     `max_over_time( deriv( rate(metric_counter[10m])[5m:1m] )[3d:] )`,
			expectedSpunOffSubqueries: 1,
		},
		"@ modifier": {
			query:                 `sum by (group_1)(rate(metric_counter[1h] @ end())) + sum by (group_1)(rate(metric_counter[1h] @ start()))`,
			expectedSkippedReason: "no-subquery",
		},
		"@ modifier and offset": {
			query:                 `sum by (group_1)(rate(metric_counter[1h] @ end() offset 1m))`,
			expectedSkippedReason: "no-subquery",
		},
		"@ modifier and negative offset": {
			query:                 `sum by (group_1)(rate(metric_counter[1h] @ start() offset -1m))`,
			expectedSkippedReason: "no-subquery",
		},
		"label_replace": {
			query: `sum by (foo)(
					 	label_replace(
									rate(metric_counter{group_1="0"}[1m]),
									"foo", "bar$1", "group_2", "(.*)"
								)
							)`,
			expectedSkippedReason: "no-subquery",
		},
		"label_join": {
			query: `sum by (foo)(
							label_join(
									rate(metric_counter{group_1="0"}[1m]),
									"foo", ",", "group_1", "group_2", "const"
								)
							)`,
			expectedSkippedReason: "no-subquery",
		},
		`query with sort() expects specific order`: {
			query:                 `sort(sum(metric_histogram_bucket) by (le))`,
			expectedSkippedReason: "no-subquery",
			expectSpecificOrder:   true,
		},
		"scalar(aggregation)": {
			query:                 `scalar(sum(metric_counter))`,
			expectedSkippedReason: "no-subquery",
		},
		`filtering binary operation with constant scalar`: {
			query:                 `count(metric_counter > 0)`,
			expectedSkippedReason: "no-subquery",
		},
		`filtering binary operation of a function result with scalar`: {
			query:                 `max_over_time(metric_counter[5m]) > 0`,
			expectedSkippedReason: "no-subquery",
		},
		`binary operation with an aggregation on one hand`: {
			query:                 `sum(metric_counter) > 1`,
			expectedSkippedReason: "no-subquery",
		},
		`binary operation with an aggregation on the other hand`: {
			query:                 `0 < sum(metric_counter)`,
			expectedSkippedReason: "no-subquery",
		},
		`binary operation with an aggregation by some label on one hand`: {
			query:                 `count by (unique) (metric_counter) > 0`,
			expectedSkippedReason: "no-subquery",
		},
		`filtering binary operation with non constant`: {
			query:                 `max by(unique) (max_over_time(metric_counter[5m])) > scalar(min(metric_counter))`,
			expectedSkippedReason: "no-subquery",
		},
		"subquery min_over_time with aggr": {
			query: `min_over_time(
						sum by(group_1) (
							rate(metric_counter[5m])
						)[3d:]
					)`,
			expectedSpunOffSubqueries: 1,
		},
		"outer subquery on top of sum": {
			query:                 `sum(metric_counter) by (group_1)[5m:1m]`,
			expectedSkippedReason: "no-subquery",
		},
		"outer subquery on top of avg": {
			query:                 `avg(metric_counter) by (group_1)[5m:1m]`,
			expectedSkippedReason: "no-subquery",
		},
		"stddev()": {
			query:                 `stddev(metric_counter{const="fixed"})`,
			expectedSkippedReason: "no-subquery",
		},
		"stdvar()": {
			query:                 `stdvar(metric_counter{const="fixed"})`,
			expectedSkippedReason: "no-subquery",
		},
		"topk()": {
			query:                 `topk(2, metric_counter{const="fixed"})`,
			expectedSkippedReason: "no-subquery",
		},
		"bottomk()": {
			query:                 `bottomk(2, metric_counter{const="fixed"})`,
			expectedSkippedReason: "no-subquery",
		},
		"vector()": {
			query:                 `vector(1)`,
			expectedSkippedReason: "no-subquery",
		},
		"scalar(single metric)": {
			query:                 `scalar(metric_counter{unique="1"})`, // Select a single metric.
			expectedSkippedReason: "no-subquery",
		},
		"histogram_quantile no grouping": {
			query:                 fmt.Sprintf(`histogram_quantile(0.99, metric_histogram_bucket{unique="%d"})`, numSeries+10), // Select a single histogram metric.
			expectedSkippedReason: "no-subquery",
		},
		"histogram_quantile with inner aggregation": {
			query:                 `sum by (group_1) (histogram_quantile(0.9, rate(metric_histogram_bucket[1m])))`,
			expectedSkippedReason: "no-subquery",
		},
		"histogram_quantile without aggregation": {
			query:                 `histogram_quantile(0.5, rate(metric_histogram_bucket{group_1="0"}[1m]))`,
			expectedSkippedReason: "no-subquery",
		},
		`subqueries with non parallelizable function in children`: {
			query: `max_over_time(
				absent_over_time(
					deriv(
						rate(metric_counter[1m])
					[5m:1m])
				[2m:1m])
			[10m:1m] offset 25m)`,
			expectedSkippedReason: "no-subquery",
		},
		"string literal": {
			query:                 `"test"`,
			expectedSkippedReason: "no-subquery",
		},
		"day_of_month() >= 1 and day_of_month()": {
			query:                 `day_of_month() >= 1 and day_of_month()`,
			expectedSkippedReason: "no-subquery",
		},
		"month() >= 1 and month()": {
			query:                 `month() >= 1 and month()`,
			expectedSkippedReason: "no-subquery",
		},
		"vector(1) > 0 and vector(1)": {
			query:                 `vector(1) > 0 and vector(1)`,
			expectedSkippedReason: "no-subquery",
		},
		"sum(metric_counter) > 0 and vector(1)": {
			query:                 `sum(metric_counter) > 0 and vector(1)`,
			expectedSkippedReason: "no-subquery",
		},
		"vector(1)": {
			query:                 `vector(1)`,
			expectedSkippedReason: "no-subquery",
		},
		"time()": {
			query:                 `time()`,
			expectedSkippedReason: "no-subquery",
		},
		"month(sum(metric_counter))": {
			query:                 `month(sum(metric_counter))`,
			expectedSkippedReason: "no-subquery", // Sharded because the contents of `sum()` is sharded.
		},
		"month(sum(metric_counter)) > 0 and vector(1)": {
			query:                 `month(sum(metric_counter)) > 0 and vector(1)`,
			expectedSkippedReason: "no-subquery", // Sharded because the contents of `sum()` is sharded.
		},
		"0 < bool 1": {
			query:                 `0 < bool 1`,
			expectedSkippedReason: "no-subquery",
		},
		"scalar(metric_counter{const=\"fixed\"}) < bool 1": {
			query:                 `scalar(metric_counter{const="fixed"}) < bool 1`,
			expectedSkippedReason: "no-subquery",
		},
		"scalar(sum(metric_counter)) < bool 1": {
			query:                 `scalar(sum(metric_counter)) < bool 1`,
			expectedSkippedReason: "no-subquery",
		},
		// Summing floats and native histograms together makes no sense, see
		// https://prometheus.io/docs/prometheus/latest/querying/operators/#operators-for-native-histograms
		// so we exclude native histograms here and in some subsequent tests
		`sum({__name__!=""}) excluding native histograms`: {
			query:                 `sum({__name__!="",__name__!="metric_native_histogram"})`,
			expectedSkippedReason: "no-subquery",
		},
		`sum by (group_1) ({__name__!=""}) excluding native histograms`: {
			query:                 `sum by (group_1) ({__name__!="",__name__!="metric_native_histogram"})`,
			expectedSkippedReason: "no-subquery",
		},
		`sum by (group_1) (count_over_time({__name__!=""}[1m])) excluding native histograms`: {
			query:                 `sum by (group_1) (count_over_time({__name__!="",__name__!="metric_native_histogram"}[1m]))`,
			expectedSkippedReason: "no-subquery",
		},
		`sum(metric_native_histogram)`: {
			query:                 `sum(metric_native_histogram)`,
			expectedSkippedReason: "no-subquery",
		},
		`sum by (group_1) (metric_native_histogram)`: {
			query:                 `sum by (group_1) (metric_native_histogram)`,
			expectedSkippedReason: "no-subquery",
		},
		`sum by (group_1) (count_over_time(metric_native_histogram[1m]))`: {
			query:                 `sum by (group_1) (count_over_time(metric_native_histogram[1m]))`,
			expectedSkippedReason: "no-subquery",
		},
		`count(metric_native_histogram)`: {
			query:                 `count(metric_native_histogram)`,
			expectedSkippedReason: "no-subquery",
		},
		`count by (group_1) (metric_native_histogram)`: {
			query:                 `count by (group_1) (metric_native_histogram)`,
			expectedSkippedReason: "no-subquery",
		},
		`count by (group_1) (count_over_time(metric_native_histogram[1m]))`: {
			query:                 `count by (group_1) (count_over_time(metric_native_histogram[1m]))`,
			expectedSkippedReason: "no-subquery",
		},
		`histogram_sum(sum(metric_native_histogram))`: {
			query:                 `histogram_sum(sum(metric_native_histogram))`,
			expectedSkippedReason: "no-subquery",
		},
		`histogram_count(sum(metric_native_histogram))`: {
			query:                 `histogram_count(sum(metric_native_histogram))`,
			expectedSkippedReason: "no-subquery",
		},
		`histogram_quantile(0.5, sum(metric_native_histogram))`: {
			query:                 `histogram_quantile(0.5, sum(metric_native_histogram))`,
			expectedSkippedReason: "no-subquery",
		},
		`histogram_fraction(0, 0.5, sum(metric_native_histogram))`: {
			query:                 `histogram_fraction(0, 0.5, sum(metric_native_histogram))`,
			expectedSkippedReason: "no-subquery",
		},
		`histogram_stdvar`: {
			query:                 `histogram_stdvar(metric_native_histogram)`,
			expectedSkippedReason: "no-subquery",
		},
		`histogram_stdvar on sum of metrics`: {
			query:                 `histogram_stdvar(sum(metric_native_histogram))`,
			expectedSkippedReason: "no-subquery",
		},
		`histogram_stddev`: {
			query:                 `histogram_stddev(metric_native_histogram)`,
			expectedSkippedReason: "no-subquery",
		},
		`histogram_stddev on sum of metrics`: {
			query:                 `histogram_stddev(sum(metric_native_histogram))`,
			expectedSkippedReason: "no-subquery",
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

	engine := newEngine()
	downstream := &downstreamHandler{
		engine:    engine,
		queryable: queryable,
	}

	codec := newTestPrometheusCodec()
	// Create a local server that handles queries.
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/prometheus/api/v1/query_range" {
			http.Error(w, "invalid path", http.StatusNotFound)
			return
		}

		metricsReq, err := codec.DecodeMetricsQueryRequest(r.Context(), r)
		if err != nil {
			http.Error(w, errors.Wrap(err, "failed to decode request").Error(), http.StatusBadRequest)
			return
		}

		resp, err := downstream.Do(r.Context(), metricsReq)
		if err != nil {
			http.Error(w, errors.Wrap(err, "failed to execute request").Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		httpResp, err := codec.EncodeMetricsQueryResponse(r.Context(), r, resp)
		if err != nil {
			http.Error(w, errors.Wrap(err, "failed to encode response").Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", httpResp.Header.Get("Content-Type"))
		w.Header().Set("Content-Length", httpResp.Header.Get("Content-Length"))
		io.Copy(w, httpResp.Body)
		httpResp.Body.Close()
	}))
	t.Cleanup(httpServer.Close)

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			req := &PrometheusInstantQueryRequest{
				path:      "/query",
				time:      util.TimeToMillis(end),
				queryExpr: parseQuery(t, testData.query),
			}

			// Run the query without subquery spin-off.
			expectedRes, err := downstream.Do(context.Background(), req)
			require.Nil(t, err)
			expectedPrometheusRes := expectedRes.(*PrometheusResponse)
			if !testData.expectSpecificOrder {
				sort.Sort(byLabels(expectedPrometheusRes.Data.Result))
			}

			// Ensure the query produces some results.
			require.NotEmpty(t, expectedPrometheusRes.Data.Result)
			requireValidSamples(t, expectedPrometheusRes.Data.Result)

			if testData.expectedSpunOffSubqueries > 0 {
				// Remove position information from annotations, to mirror what we expect from the sharded queries below.
				removeAllAnnotationPositionInformation(expectedPrometheusRes.Infos)
				removeAllAnnotationPositionInformation(expectedPrometheusRes.Warnings)
			}

			spinOffQueryHandler, err := newSpinOffQueryHandler(codec, log.NewLogfmtLogger(os.Stdout), httpServer.URL)
			require.NoError(t, err)

			reg := prometheus.NewPedanticRegistry()
			spinoffMiddleware := newSpinOffSubqueriesMiddleware(
				mockLimits{
					instantQueriesWithSubquerySpinOff: []string{".*"},
				},
				log.NewNopLogger(),
				engine,
				spinOffQueryHandler,
				reg,
				defaultStepFunc,
			)

			ctx := user.InjectOrgID(context.Background(), "test")
			spinoffRes, err := spinoffMiddleware.Wrap(downstream).Do(ctx, req)
			require.Nil(t, err)

			// Ensure the two results matches (float precision can slightly differ, there's no guarantee in PromQL engine too
			// if you rerun the same query twice).
			shardedPrometheusRes := spinoffRes.(*PrometheusResponse)
			if !testData.expectSpecificOrder {
				sort.Sort(byLabels(shardedPrometheusRes.Data.Result))
			}
			approximatelyEquals(t, expectedPrometheusRes, shardedPrometheusRes)

			var noSubqueries int
			if testData.expectedSkippedReason == "no-subquery" {
				noSubqueries = 1
			}

			assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
# HELP cortex_frontend_spun_off_subqueries_total Total number of subqueries that were spun off.
# TYPE cortex_frontend_spun_off_subqueries_total counter
cortex_frontend_spun_off_subqueries_total %d
# HELP cortex_frontend_subquery_spinoff_attempts_total Total number of queries the query-frontend attempted to spin off subqueries from.
# TYPE cortex_frontend_subquery_spinoff_attempts_total counter
cortex_frontend_subquery_spinoff_attempts_total 1
# HELP cortex_frontend_subquery_spinoff_skipped_total Total number of queries the query-frontend skipped or failed to spin off subqueries from.
# TYPE cortex_frontend_subquery_spinoff_skipped_total counter
cortex_frontend_subquery_spinoff_skipped_total{reason="mapping-failed"} 0
cortex_frontend_subquery_spinoff_skipped_total{reason="no-subqueries"} %d
cortex_frontend_subquery_spinoff_skipped_total{reason="parsing-failed"} 0
cortex_frontend_subquery_spinoff_skipped_total{reason="too-many-downstream-queries"} 0
# HELP cortex_frontend_subquery_spinoff_successes_total Total number of queries the query-frontend successfully spun off subqueries from.
# TYPE cortex_frontend_subquery_spinoff_successes_total counter
cortex_frontend_subquery_spinoff_successes_total %d
				`, testData.expectedSpunOffSubqueries, noSubqueries, testData.expectedSpunOffSubqueries)),
				"cortex_frontend_subquery_spinoff_attempts_total",
				"cortex_frontend_subquery_spinoff_successes_total",
				"cortex_frontend_subquery_spinoff_skipped_total",
				"cortex_frontend_spun_off_subqueries_total"))
		})

	}
}

func TestSubquerySpinOff_ShouldReturnErrorOnDownstreamHandlerFailure(t *testing.T) {
	req := &PrometheusInstantQueryRequest{
		path:      "/query",
		time:      util.TimeToMillis(end),
		queryExpr: parseQuery(t, "vector(1)"),
	}

	// Mock the downstream handler to always return error.
	downstreamErr := errors.Errorf("some err")
	downstream := mockHandlerWith(nil, downstreamErr)

	spinoffMiddleware := newSpinOffSubqueriesMiddleware(mockLimits{instantQueriesWithSubquerySpinOff: []string{".*"}}, log.NewNopLogger(), newEngine(), downstream, nil, defaultStepFunc)

	// Run the query with subquery spin-off middleware wrapping the downstream one.
	// We expect to get the downstream error.
	ctx := user.InjectOrgID(context.Background(), "test")
	_, err := spinoffMiddleware.Wrap(downstream).Do(ctx, req)
	require.Error(t, err)
	assert.Equal(t, downstreamErr, err)
}

var defaultStepFunc = func(int64) int64 {
	return (1 * time.Minute).Milliseconds()
}
