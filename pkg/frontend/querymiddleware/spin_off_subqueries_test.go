// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/testdatagen"
	"github.com/grafana/mimir/pkg/util"
)

func TestSubquerySpinOff_Correctness(t *testing.T) {
	tests := map[string]subquerySpinOffTest{
		"skipped: no subquery": {
			query: `sum(
				  count(
				    count(metric_counter) by (group_1, group_2)
				  ) by (group_1)
				)`,
			expectedSkippedReason: "no-subquery",
			expectEmptyResult:     true,
		},
		"skipped: subquery max: too short range": {
			query: `max_over_time(
							rate(metric_counter[1m])
						[30m:1m]
					)`,
			expectedSkippedReason: "no-subquery",
			expectEmptyResult:     true,
		},
		"skipped: subquery max: too few steps": {
			query: `max_over_time(
							rate(metric_counter[1m])
						[2h:15m]
					)`,
			expectedSkippedReason: "no-subquery",
		},
		"subquery max with downstream join": {
			query: `max_over_time(
							rate(metric_counter[1m])
						[2h:1m]
					)
					* on (group_1) group_left()
					max by (group_1)(
							rate(metric_counter[1m])
					)`,
			expectedSpunOffSubqueries: 1,
		},
		"subquery max": {
			query: `max_over_time(
							rate(metric_counter[1m])
						[2h:1m]
					)`,
			expectedSpunOffSubqueries: 1,
		},
		"subquery max with offset": {
			query: `max_over_time(
							rate(metric_counter[1m])
						[2h:1m] offset 1h
					)`,
			expectedSpunOffSubqueries: 1,
		},
		"subquery min": {
			query: `min_over_time(
							rate(metric_counter[1m])
						[2h:1m]
					)`,
			expectedSpunOffSubqueries: 1,
		},
		"subquery min 2": {
			query:                     `min_over_time((changes(metric_counter[5m]))[2h:2m])`,
			expectedSpunOffSubqueries: 1,
		},
		"subquery min with small offset": {
			query:                     `min_over_time((changes(metric_counter[5m]))[1h:1m] offset 20s)`,
			expectedSpunOffSubqueries: 1,
		},
		"subquery min with offset": {
			query:                     `min_over_time((changes(metric_counter[5m]))[1h:1m] offset 1h)`,
			expectedSpunOffSubqueries: 1,
		},
		"subquery min: offset query time -10m": {
			query:                     `min_over_time((changes(metric_counter[5m]))[2h:2m])`,
			expectedSpunOffSubqueries: 1,
			offsetQueryTime:           -10 * time.Minute,
		},
		"subquery min: offset query time +10m": {
			query:                     `min_over_time((changes(metric_counter[5m]))[2h:2m])`,
			expectedSpunOffSubqueries: 1,
			offsetQueryTime:           10 * time.Minute,
		},
		"subquery min: offset query time -33s": {
			query:                     `min_over_time((changes(metric_counter[5m]))[2h:2m])`,
			expectedSpunOffSubqueries: 1,
			offsetQueryTime:           -33 * time.Second,
		},
		"subquery min: offset query time +33s": {
			query:                     `min_over_time((changes(metric_counter[5m]))[2h:2m])`,
			expectedSpunOffSubqueries: 1,
			offsetQueryTime:           33 * time.Second,
		},
		"subquery min: offset query time +1h": {
			query:                     `min_over_time((changes(metric_counter[5m]))[2h:2m])`,
			expectedSpunOffSubqueries: 1,
			offsetQueryTime:           1 * time.Hour,
		},
		"triple subquery": {
			query: `max_over_time(
						stddev_over_time(
							deriv(
								rate(metric_counter[10m])
							[5m:1m])
						[2m:])
					[2h:])`,
			expectedSpunOffSubqueries: 1,
		},
		"double subquery deriv": {
			query:                     `max_over_time( deriv( rate(metric_counter[10m])[5m:1m] )[2h:] )`,
			expectedSpunOffSubqueries: 1,
		},
		"subquery min_over_time with aggr": {
			query: `min_over_time(
						sum by(group_1) (
							rate(metric_counter[5m])
						)[2h:]
					)`,
			expectedSpunOffSubqueries: 1,
		},
		"subquery max with offset shorter than step": {
			query: `
sum by (group_1) (
      sum_over_time(
        avg by (group_1) (metric_counter{group_2="1"})[1h:5m] offset 1m
      )
    *
      avg by (group_1) (
        avg_over_time(metric_counter{group_2="2"}[1h:5m] offset 1m)
      )
  *
    0.083333
)`,
			expectedSpunOffSubqueries: 1,
		},
	}

	queryable := setupSubquerySpinOffTestSeries(t, 2*time.Hour)
	runSubquerySpinOffTests(t, tests, queryable)
}

func TestSubquerySpinOff_LongRangeQuery(t *testing.T) {
	tests := map[string]subquerySpinOffTest{
		"subquery max: multiple range queries": {
			query: `max_over_time(
							rate(metric_counter[1m])
						[10h:1m]
					)`,
			expectedSpunOffSubqueries: 1,
		},
	}

	queryable := setupSubquerySpinOffTestSeries(t, 10*time.Hour)
	runSubquerySpinOffTests(t, tests, queryable)
}

func TestSubquerySpinOff_ShouldReturnErrorOnDownstreamHandlerFailure(t *testing.T) {
	req := &PrometheusInstantQueryRequest{
		path:      "/query",
		time:      util.TimeToMillis(end),
		queryExpr: parseQuery(t, "vector(1)"),
	}

	runForEngines(t, func(t *testing.T, opts promql.EngineOpts, eng promql.QueryEngine) {
		// Mock the downstream handler to always return error.
		downstreamErr := errors.Errorf("some err")
		downstream := mockHandlerWith(nil, downstreamErr)

		spinoffMiddleware := newSpinOffSubqueriesMiddleware(mockLimits{subquerySpinOffEnabled: true}, log.NewNopLogger(), eng, nil, nil, defaultStepFunc)

		// Run the query with subquery spin-off middleware wrapping the downstream one.
		// We expect to get the downstream error.
		ctx := user.InjectOrgID(context.Background(), "test")
		_, err := spinoffMiddleware.Wrap(downstream).Do(ctx, req)
		require.Error(t, err)
		assert.Equal(t, downstreamErr, err)
	})
}

var defaultStepFunc = func(int64) int64 {
	return (1 * time.Minute).Milliseconds()
}

type subquerySpinOffTest struct {
	query                     string
	expectedSkippedReason     string
	expectedSpunOffSubqueries int
	expectSpecificOrder       bool
	expectEmptyResult         bool
	offsetQueryTime           time.Duration
}

func setupSubquerySpinOffTestSeries(t *testing.T, timeRange time.Duration) storage.Queryable {
	t.Helper()

	var (
		numSeries      = 1000
		numStaleSeries = 100
		now            = time.Now()
		samplesStart   = now.Add(-timeRange)
		samplesEnd     = now.Add(30 * time.Minute)
		samplesStep    = 30 * time.Second
	)

	series := make([]storage.Series, 0, numSeries)
	seriesID := 0

	// Add counter series.
	for i := 0; i < numSeries; i++ {
		gen := testdatagen.Factor(float64(i) * 0.1)
		if i >= numSeries-numStaleSeries {
			// Wrap the generator to inject the staleness marker between minute 10 and 20.
			gen = testdatagen.Stale(time.Now().Add(10*time.Minute), time.Now().Add(20*time.Minute), gen)
		}

		series = append(series, testdatagen.NewSeries(testdatagen.NewTestCounterLabels(seriesID), samplesStart, samplesEnd, samplesStep, gen))
		seriesID++
	}

	// Add a special series whose data points end earlier than the end of the queried time range
	// and has NO stale marker.
	series = append(series, testdatagen.NewSeries(testdatagen.NewTestCounterLabels(seriesID), samplesStart, time.Now().Add(-5*time.Minute), samplesStep, testdatagen.Factor(2)))
	seriesID++

	// Add a special series whose data points end earlier than the end of the queried time range
	// and HAS a stale marker at the end.
	series = append(series, testdatagen.NewSeries(testdatagen.NewTestCounterLabels(seriesID), samplesStart, time.Now().Add(-5*time.Minute), samplesStep, testdatagen.Stale(time.Now().Add(-6*time.Minute), time.Now().Add(-4*time.Minute), testdatagen.Factor(2))))
	seriesID++

	// Add a special series whose data points start later than the start of the queried time range.
	series = append(series, testdatagen.NewSeries(testdatagen.NewTestCounterLabels(seriesID), time.Now().Add(5*time.Minute), samplesEnd, samplesStep, testdatagen.Factor(2)))

	// Create a queryable on the fixtures.
	queryable := testdatagen.StorageSeriesQueryable(series)

	return queryable
}

func runSubquerySpinOffTests(t *testing.T, tests map[string]subquerySpinOffTest, queryable storage.Queryable) {
	t.Helper()

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			runForEngines(t, func(t *testing.T, opts promql.EngineOpts, eng promql.QueryEngine) {
				downstream := &downstreamHandler{engine: eng, queryable: queryable}
				req := &PrometheusInstantQueryRequest{
					path:      "/query",
					time:      util.TimeToMillis(time.Now().Add(testData.offsetQueryTime)),
					queryExpr: parseQuery(t, testData.query),
				}

				// Run the query without subquery spin-off.
				expectedRes, err := downstream.Do(context.Background(), req)
				require.Nil(t, err)
				expectedPrometheusRes, ok := expectedRes.GetPrometheusResponse()
				require.True(t, ok)
				if !testData.expectSpecificOrder {
					sort.Sort(byLabels(expectedPrometheusRes.Data.Result))
				}

				// Ensure the query produces some results.
				if !testData.expectEmptyResult {
					require.NotEmpty(t, expectedPrometheusRes.Data.Result)
					requireValidSamples(t, expectedPrometheusRes.Data.Result)
				}

				if testData.expectedSpunOffSubqueries > 0 {
					// Remove position information from annotations, to mirror what we expect from the sharded queries below.
					removeAllAnnotationPositionInformation(expectedPrometheusRes.Infos)
					removeAllAnnotationPositionInformation(expectedPrometheusRes.Warnings)
				}

				// Create a fake middleware that tracks if it was called
				called := false
				fakeMiddleware := MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
					return HandlerFunc(func(ctx context.Context, req MetricsQueryRequest) (Response, error) {
						called = true
						return next.Do(ctx, req)
					})
				})

				reg := prometheus.NewPedanticRegistry()
				spinoffMiddleware := newSpinOffSubqueriesMiddleware(
					mockLimits{
						subquerySpinOffEnabled: true,
					},
					log.NewNopLogger(),
					eng,
					reg,
					fakeMiddleware,
					defaultStepFunc,
				)

				ctx := user.InjectOrgID(context.Background(), "test")
				spinoffRes, err := spinoffMiddleware.Wrap(downstream).Do(ctx, req)
				require.Nil(t, err)

				if testData.expectedSpunOffSubqueries > 0 {
					assert.True(t, called, "the fake range middleware should have been called")
				} else {
					assert.False(t, called, "the fake range middleware should not have been called")
				}

				// Ensure the two results matches (float precision can slightly differ, there's no guarantee in PromQL engine too
				// if you rerun the same query twice).
				shardedPrometheusRes, _ := spinoffRes.GetPrometheusResponse()
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
# HELP cortex_frontend_subquery_spinoff_attempts_total Total number of queries the query-frontend attempted to spin-off subqueries from.
# TYPE cortex_frontend_subquery_spinoff_attempts_total counter
cortex_frontend_subquery_spinoff_attempts_total 1
# HELP cortex_frontend_subquery_spinoff_skipped_total Total number of queries the query-frontend skipped or failed to spin-off subqueries from.
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
		})
	}
}
