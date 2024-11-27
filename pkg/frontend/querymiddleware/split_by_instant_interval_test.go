// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util"
)

func TestInstantQuerySplittingCorrectness(t *testing.T) {
	for _, startString := range []string{
		"2020-01-01T03:00:00.100Z",
		"2020-01-01T03:00:00Z",
		time.Now().Format(time.RFC3339Nano),
	} {
		t.Run(fmt.Sprintf("start=%s", startString), func(t *testing.T) {
			start, err := time.Parse(time.RFC3339Nano, startString)
			require.NoError(t, err)

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
				query                        string
				expectedSplitQueries         int
				expectedSkippedSmallInterval int
				expectedSkippedSubquery      int
				expectedSkippedNonSplittable int
			}{
				// Splittable range vector aggregators
				"avg_over_time": {
					query:                `avg_over_time(metric_counter[3m])`,
					expectedSplitQueries: 6,
				},
				"count_over_time": {
					query:                `count_over_time(metric_counter[3m])`,
					expectedSplitQueries: 3,
				},
				"increase": {
					query:                `increase(metric_counter[3m])`,
					expectedSplitQueries: 3,
				},
				"max_over_time": {
					query:                `max_over_time(metric_counter[3m])`,
					expectedSplitQueries: 3,
				},
				"min_over_time": {
					query:                `min_over_time(metric_counter[3m])`,
					expectedSplitQueries: 3,
				},
				"present_over_time": {
					query:                `present_over_time(metric_counter[3m])`,
					expectedSplitQueries: 3,
				},
				"rate": {
					query:                `rate(metric_counter[3m])`,
					expectedSplitQueries: 3,
				},
				"sum_over_time": {
					query:                `sum_over_time(metric_counter[3m])`,
					expectedSplitQueries: 3,
				},
				// Splittable aggregations wrapped by non-aggregative functions.
				"absent": {
					query:                `absent(sum_over_time(nonexistent[3m]))`,
					expectedSplitQueries: 3,
				},
				"ceil(sum(sum_over_time()))": {
					query:                `ceil(sum(sum_over_time(metric_counter[3m])))`,
					expectedSplitQueries: 3,
				},
				"ceil(sum(sum_over_time()) + sum(sum_over_time())) and both legs of the binary operation are splittable": {
					query:                `ceil(sum(sum_over_time(metric_counter[3m])) + sum(sum_over_time(metric_counter[3m])))`,
					expectedSplitQueries: 6,
				},
				"ceil(sum(sum_over_time()) + sum(sum_over_time())) and only right leg of the binary operation is splittable": {
					query:                `ceil(sum(sum_over_time(metric_counter[1m])) + sum(sum_over_time(metric_counter[3m])))`,
					expectedSplitQueries: 3,
				},
				"ceil(sum(sum_over_time()) + sum(sum_over_time())) and only left leg of the binary operation is splittable": {
					query:                `ceil(sum(sum_over_time(metric_counter[3m])) + sum(sum_over_time(metric_counter[1m])))`,
					expectedSplitQueries: 3,
				},
				"clamp": {
					query:                `clamp(sum_over_time(metric_counter[3m]), 1, 10)`,
					expectedSplitQueries: 3,
				},
				"clamp_max": {
					query:                `clamp_max(sum_over_time(metric_counter[3m]), 10)`,
					expectedSplitQueries: 3,
				},
				"clamp_min": {
					query:                `clamp_min(sum_over_time(metric_counter[3m]), 1)`,
					expectedSplitQueries: 3,
				},
				"exp": {
					query:                `exp(sum_over_time(metric_counter[3m]))`,
					expectedSplitQueries: 3,
				},
				"floor": {
					query:                `floor(sum_over_time(metric_counter[3m]))`,
					expectedSplitQueries: 3,
				},
				"label_join": {
					query:                `label_join(sum_over_time(metric_counter{group_1="0"}[3m]), "foo", ",", "group_1", "group_2", "const")`,
					expectedSplitQueries: 3,
				},
				"label_replace": {
					query:                `label_replace(sum_over_time(metric_counter{group_1="0"}[3m]), "foo", "bar$1", "group_2", "(.*)")`,
					expectedSplitQueries: 3,
				},
				"ln": {
					query:                `ln(sum_over_time(metric_counter[3m]))`,
					expectedSplitQueries: 3,
				},
				"log2": {
					query:                `log2(sum_over_time(metric_counter[3m]))`,
					expectedSplitQueries: 3,
				},
				"round": {
					query:                `round(sum_over_time(metric_counter[3m]))`,
					expectedSplitQueries: 3,
				},
				"sgn": {
					query:                `sgn(sum_over_time(metric_counter[3m]))`,
					expectedSplitQueries: 3,
				},
				"sort": {
					query:                `sort(sum_over_time(metric_counter[3m]))`,
					expectedSplitQueries: 3,
				},
				"sort_desc": {
					query:                `sort_desc(sum_over_time(metric_counter[3m]))`,
					expectedSplitQueries: 3,
				},
				"sqrt": {
					query:                `sqrt(sum_over_time(metric_counter[3m]))`,
					expectedSplitQueries: 3,
				},
				// Vector aggregators
				"avg(rate)": {
					query:                `avg(rate(metric_counter[3m]))`,
					expectedSplitQueries: 3,
				},
				"avg(rate) grouping 'by'": {
					query:                `avg by(group_1) (rate(metric_counter[3m]))`,
					expectedSplitQueries: 3,
				},
				"count(rate)": {
					query:                `count(rate(metric_counter[3m]))`,
					expectedSplitQueries: 3,
				},
				"count(rate) grouping 'by'": {
					query:                `count by(group_1) (rate(metric_counter[3m]))`,
					expectedSplitQueries: 3,
				},
				"max(rate)": {
					query:                `max(rate(metric_counter[3m]))`,
					expectedSplitQueries: 3,
				},
				"max(rate) grouping 'by'": {
					query:                `max by(group_1) (rate(metric_counter[3m]))`,
					expectedSplitQueries: 3,
				},
				"min(rate)": {
					query:                `min(rate(metric_counter[3m]))`,
					expectedSplitQueries: 3,
				},
				"min(rate) grouping 'by'": {
					query:                `min by(group_1) (rate(metric_counter[3m]))`,
					expectedSplitQueries: 3,
				},
				"sum(rate)": {
					query:                `sum(rate(metric_counter[3m]))`,
					expectedSplitQueries: 3,
				},
				"sum(rate) grouping 'by'": {
					query:                `sum by(group_1) (rate(metric_counter[3m]))`,
					expectedSplitQueries: 3,
				},
				"sum(rate() or rate())": {
					query:                `sum(rate(metric_counter{group_2="0"}[3m]) or rate(metric_counter{group_2="1"}[3m]))`,
					expectedSplitQueries: 6,
				},
				"topk(rate)": {
					query:                `topk(2, rate(metric_counter[3m]))`,
					expectedSplitQueries: 3,
				},
				"topk(sum(rate))": {
					query:                `topk(2, sum(rate(metric_counter[3m])))`,
					expectedSplitQueries: 3,
				},
				"stddev(rate)": {
					query:                `stddev(rate(metric_counter[3m]))`,
					expectedSplitQueries: 3,
				},
				"count_values(count_over_time)": {
					query:                `count_values("dst", count_over_time(metric_counter[3m]))`,
					expectedSplitQueries: 3,
				},
				// Binary operations
				"rate / rate": {
					query:                `rate(metric_counter[3m]) / rate(metric_counter[6m])`,
					expectedSplitQueries: 9,
				},
				"rate / 10": {
					query:                `rate(metric_counter[3m]) / 10`,
					expectedSplitQueries: 3,
				},
				"10 / rate": {
					query:                `10 / rate(metric_counter[3m])`,
					expectedSplitQueries: 3,
				},
				"sum(sum_over_time + count_over_time)": {
					query:                `sum(sum_over_time(metric_counter[3m]) + count_over_time(metric_counter[3m]))`,
					expectedSplitQueries: 6,
				},
				"(avg_over_time)": {
					query:                `(avg_over_time(metric_counter[3m]))`,
					expectedSplitQueries: 6,
				},
				"sum(avg_over_time)": {
					query:                `sum(avg_over_time(metric_counter[3m]))`,
					expectedSplitQueries: 6,
				},
				"sum(max(rate))": {
					query:                `sum(max(rate(metric_counter[3m])))`,
					expectedSplitQueries: 3,
				},
				"rate(3m) / rate(3m) > 0.5": {
					query:                `rate(metric_counter[3m]) / rate(metric_counter[3m]) > 0.5`,
					expectedSplitQueries: 6,
				},
				// Offset operator
				"sum_over_time[3m] offset 3m": {
					query:                `sum_over_time(metric_counter[3m] offset 3m)`,
					expectedSplitQueries: 3,
				},
				"avg_over_time[3m] offset 5m": {
					query:                `avg_over_time(metric_counter[3m] offset 5m)`,
					expectedSplitQueries: 6,
				},
				"sum_over_time[3m] offset 30s": {
					query:                `sum_over_time(metric_counter[3m] offset 30s)`,
					expectedSplitQueries: 3,
				},
				"count_over_time[3m] offset -2m": {
					query:                `sum_over_time(metric_counter[3m] offset -2m)`,
					expectedSplitQueries: 3,
				},
				"avg_over_time[3m] offset -1m": {
					query:                `avg_over_time(metric_counter[3m] offset -1m)`,
					expectedSplitQueries: 6,
				},
				"count_over_time[3m] offset -30s": {
					query:                `count_over_time(metric_counter[3m] offset -30s)`,
					expectedSplitQueries: 3,
				},
				// @ modifier
				"sum_over_time @ start()": {
					query:                `sum_over_time(metric_counter[3m] @ start())`,
					expectedSplitQueries: 3,
				},
				"sum(sum_over_time @ end())": {
					query:                `sum(sum_over_time(metric_counter[3m] @ end()))`,
					expectedSplitQueries: 3,
				},
				"avg(avg_over_time @ `start`)": {
					query:                fmt.Sprintf(`avg(avg_over_time(metric_counter[3m] @ %v))`, start.Unix()),
					expectedSplitQueries: 6,
				},
				"max_over_time @ `start` offset 1m)": {
					query:                fmt.Sprintf(`max_over_time(metric_counter[3m] @ %v offset 1m)`, start.Unix()),
					expectedSplitQueries: 3,
				},
				"min_over_time offset 1m @ `start`)": {
					query:                fmt.Sprintf(`min_over_time(metric_counter[3m] offset 1m @ %v)`, start.Unix()),
					expectedSplitQueries: 3,
				},
				// Conventional Histograms
				"histogram_quantile": {
					query:                `histogram_quantile(0.5, rate(metric_histogram_bucket[3m]))`,
					expectedSplitQueries: 3,
				},
				"histogram_quantile() grouping only 'by' le": {
					query:                `histogram_quantile(0.5, sum by(le) (rate(metric_histogram_bucket[3m])))`,
					expectedSplitQueries: 3,
				},
				"histogram_quantile() grouping 'by'": {
					query:                `histogram_quantile(0.5, sum by(group_1, le) (rate(metric_histogram_bucket[3m])))`,
					expectedSplitQueries: 3,
				},
				"histogram_quantile() grouping 'without'": {
					query:                `histogram_quantile(0.5, sum without(group_1, group_2, unique) (rate(metric_histogram_bucket[3m])))`,
					expectedSplitQueries: 3,
				},
				"histogram_quantile() with no effective grouping because all groups have 1 series": {
					query:                `histogram_quantile(0.5, sum by(unique, le) (rate(metric_histogram_bucket{group_1="0"}[3m])))`,
					expectedSplitQueries: 3,
				},
				// Native Histograms
				"sum(rate) for native histogram": {
					query:                `sum(rate(metric_native_histogram[3m]))`,
					expectedSplitQueries: 3,
				},
				"sum(rate) grouping 'by' for native histogram": {
					query:                `sum by(group_1) (rate(metric_native_histogram[3m]))`,
					expectedSplitQueries: 3,
				},
				// Subqueries
				"subquery sum_over_time": {
					query:                   `sum_over_time(metric_counter[1h:5m])`,
					expectedSplitQueries:    0,
					expectedSkippedSubquery: 1,
				},
				"subquery sum(rate)": {
					query:                   `sum(rate(metric_counter[30m:5s]))`,
					expectedSplitQueries:    0,
					expectedSkippedSubquery: 1,
				},
				"subquery sum grouping 'by'": {
					query:                   `sum(sum_over_time(metric_counter[1h:5m]) * 60) by (group_1)`,
					expectedSplitQueries:    0,
					expectedSkippedSubquery: 1,
				},
				// should not be mapped if both operands are not splittable
				//   - first operand `rate(metric_counter[1m])` has a smaller range interval than the configured splitting
				//   - second operand `rate(metric_counter[5h:5m])` is a subquery
				"rate(1m) / rate(subquery) > 0.5": {
					query:                        `rate(metric_counter[1m]) / rate(metric_counter[5h:5m]) > 0.5`,
					expectedSplitQueries:         0,
					expectedSkippedSmallInterval: 1,
				},
				// should not be mapped if range vector aggregator is not splittable
				"absent_over_time": {
					query:                        `absent_over_time(nonexistent[1m])`,
					expectedSplitQueries:         0,
					expectedSkippedNonSplittable: 1,
				},
				"changes": {
					query:                        `changes(metric_counter[1m])`,
					expectedSplitQueries:         0,
					expectedSkippedNonSplittable: 1,
				},
				"delta": {
					query:                        `delta(metric_counter[1m])`,
					expectedSplitQueries:         0,
					expectedSkippedNonSplittable: 1,
				},
				"deriv": {
					query:                        `deriv(metric_counter[1m])`,
					expectedSplitQueries:         0,
					expectedSkippedNonSplittable: 1,
				},
				"double_exponential_smoothing": {
					query:                        `double_exponential_smoothing(metric_counter[1m], 0.5, 0.9)`,
					expectedSplitQueries:         0,
					expectedSkippedNonSplittable: 1,
				},
				"idelta": {
					query:                        `idelta(metric_counter[1m])`,
					expectedSplitQueries:         0,
					expectedSkippedNonSplittable: 1,
				},
				"irate": {
					query:                        `irate(metric_counter[3m])`,
					expectedSplitQueries:         0,
					expectedSkippedNonSplittable: 1,
				},
				"last_over_time": {
					query:                        `last_over_time(metric_counter[1m])`,
					expectedSplitQueries:         0,
					expectedSkippedNonSplittable: 1,
				},
				"predict_linear": {
					query:                        `last_over_time(metric_counter[1m])`,
					expectedSplitQueries:         0,
					expectedSkippedNonSplittable: 1,
				},
				"quantile_over_time": {
					query:                        `quantile_over_time(0.95, metric_counter[1m])`,
					expectedSplitQueries:         0,
					expectedSkippedNonSplittable: 1,
				},
				"resets": {
					query:                        `resets(metric_counter[3m])`,
					expectedSplitQueries:         0,
					expectedSkippedNonSplittable: 1,
				},
				"stddev_over_time": {
					query:                        `stddev_over_time(metric_counter[1m])`,
					expectedSplitQueries:         0,
					expectedSkippedNonSplittable: 1,
				},
				"stdvar_over_time": {
					query:                        `stdvar_over_time(metric_counter[1m])`,
					expectedSplitQueries:         0,
					expectedSkippedNonSplittable: 1,
				},
				"time()": {
					query:                        `time()`,
					expectedSplitQueries:         0,
					expectedSkippedNonSplittable: 1,
				},
				"vector(10)": {
					query:                        `vector(10)`,
					expectedSplitQueries:         0,
					expectedSkippedNonSplittable: 1,
				},
			}

			series := make([]*promql.StorageSeries, 0, numSeries+(numConvHistograms*len(histogramBuckets))+numNativeHistograms)
			seriesID := 0
			end := start.Add(30 * time.Minute)

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
					reqs := []MetricsQueryRequest{
						&PrometheusInstantQueryRequest{
							path:      "/query",
							time:      util.TimeToMillis(end),
							queryExpr: parseQuery(t, testData.query),
						},
					}

					for _, req := range reqs {
						t.Run(fmt.Sprintf("%T", req), func(t *testing.T) {
							reg := prometheus.NewPedanticRegistry()
							engine := newEngine()
							downstream := &downstreamHandler{
								engine:    engine,
								queryable: queryable,
							}

							// Run the query with the normal engine
							_, ctx := stats.ContextWithEmptyStats(context.Background())
							expectedRes, err := downstream.Do(ctx, req)
							require.Nil(t, err)
							expectedPrometheusRes := expectedRes.(*PrometheusResponse)
							sort.Sort(byLabels(expectedPrometheusRes.Data.Result))

							// Ensure the query produces some results.
							require.NotEmpty(t, expectedPrometheusRes.Data.Result)
							requireValidSamples(t, expectedPrometheusRes.Data.Result)

							splittingware := newSplitInstantQueryByIntervalMiddleware(mockLimits{splitInstantQueriesInterval: 1 * time.Minute}, log.NewNopLogger(), engine, reg)

							// Run the query with splitting
							splitRes, err := splittingware.Wrap(downstream).Do(user.InjectOrgID(ctx, "test"), req)
							require.Nil(t, err)

							splitPrometheusRes := splitRes.(*PrometheusResponse)
							sort.Sort(byLabels(splitPrometheusRes.Data.Result))

							approximatelyEquals(t, expectedPrometheusRes, splitPrometheusRes)

							// Assert metrics
							expectedSucceeded := 1
							if testData.expectedSplitQueries == 0 {
								expectedSucceeded = 0
							}

							assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
						# HELP cortex_frontend_instant_query_splitting_rewrites_attempted_total Total number of instant queries the query-frontend attempted to split by interval.
						# TYPE cortex_frontend_instant_query_splitting_rewrites_attempted_total counter
						cortex_frontend_instant_query_splitting_rewrites_attempted_total 1

						# HELP cortex_frontend_instant_query_split_queries_total Total number of split partial queries.
        	            # TYPE cortex_frontend_instant_query_split_queries_total counter
						cortex_frontend_instant_query_split_queries_total %d

						# HELP cortex_frontend_instant_query_splitting_rewrites_succeeded_total Total number of instant queries the query-frontend successfully split by interval.
        	            # TYPE cortex_frontend_instant_query_splitting_rewrites_succeeded_total counter
						cortex_frontend_instant_query_splitting_rewrites_succeeded_total %d

						# HELP cortex_frontend_instant_query_splitting_rewrites_skipped_total Total number of instant queries the query-frontend skipped or failed to split by interval.
						# TYPE cortex_frontend_instant_query_splitting_rewrites_skipped_total counter
						cortex_frontend_instant_query_splitting_rewrites_skipped_total{reason="mapping-failed"} 0
						cortex_frontend_instant_query_splitting_rewrites_skipped_total{reason="non-splittable"} %d
						cortex_frontend_instant_query_splitting_rewrites_skipped_total{reason="small-interval"} %d
						cortex_frontend_instant_query_splitting_rewrites_skipped_total{reason="subquery"} %d
						cortex_frontend_instant_query_splitting_rewrites_skipped_total{reason="parsing-failed"} 0
					`, testData.expectedSplitQueries, expectedSucceeded, testData.expectedSkippedNonSplittable,
								testData.expectedSkippedSmallInterval, testData.expectedSkippedSubquery)),
								"cortex_frontend_instant_query_splitting_rewrites_attempted_total",
								"cortex_frontend_instant_query_split_queries_total",
								"cortex_frontend_instant_query_splitting_rewrites_succeeded_total",
								"cortex_frontend_instant_query_splitting_rewrites_skipped_total"))

							// Assert query stats from context
							queryStats := stats.FromContext(ctx)
							assert.Equal(t, uint32(testData.expectedSplitQueries), queryStats.LoadSplitQueries())
						})
					}
				})
			}
		})
	}
}

func TestInstantQuerySplittingHTTPOptions(t *testing.T) {
	for _, tt := range []struct {
		name                   string
		httpOptions            Options
		data                   *PrometheusData
		expectedDownstreamCall int
	}{
		{
			name: "should skip instant query splitting if disabled via HTTP option",
			httpOptions: Options{
				InstantSplitDisabled: true,
			},
			data:                   nil,
			expectedDownstreamCall: 1,
		},
		{
			name: "should override instant query splitting interval specified in HTTP option",
			httpOptions: Options{
				InstantSplitInterval: time.Hour.Nanoseconds(),
			},
			data: &PrometheusData{
				ResultType: string(parser.ValueTypeVector),
			},
			expectedDownstreamCall: 3, // [3h] range interval with 1h split interval should be split in 3 partial queries
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := &PrometheusInstantQueryRequest{
				path:      "/query",
				time:      time.Now().UnixNano(),
				queryExpr: parseQuery(t, "sum_over_time(metric_counter[3h])"), // splittable instant query
				options:   tt.httpOptions,
			}

			// Split by interval middleware with a limit configuration of split instant query interval of 1m
			splittingware := newSplitInstantQueryByIntervalMiddleware(mockLimits{splitInstantQueriesInterval: 1 * time.Minute}, log.NewNopLogger(), newEngine(), nil)

			downstream := &mockHandler{}
			downstream.On("Do", mock.Anything, mock.Anything).Return(&PrometheusResponse{
				Status: statusSuccess, Data: tt.data,
			}, nil)

			res, err := splittingware.Wrap(downstream).Do(user.InjectOrgID(context.Background(), "test"), req)
			require.NoError(t, err)
			assert.Equal(t, statusSuccess, res.(*PrometheusResponse).GetStatus())

			downstream.AssertCalled(t, "Do", mock.Anything, mock.Anything)
			downstream.AssertNumberOfCalls(t, "Do", tt.expectedDownstreamCall)
		})
	}
}
