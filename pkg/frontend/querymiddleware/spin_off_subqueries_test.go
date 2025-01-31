// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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

	"github.com/grafana/mimir/pkg/util"
)

func TestSubquerySpinOff_Correctness(t *testing.T) {
	t.Parallel()

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
		"subquery max": {
			query: `max_over_time(
							rate(metric_counter[1m])
						[2d:1m]
					)`,
			expectedSpunOffSubqueries: 1,
		},
		"subquery max with offset": {
			query: `max_over_time(
							rate(metric_counter[1m])
						[2d:1m] offset 1d
					)`,
			expectedSpunOffSubqueries: 1,
		},
		"subquery min": {
			query: `min_over_time(
							rate(metric_counter[1m])
						[2d:1m]
					)`,
			expectedSpunOffSubqueries: 1,
		},
		"sum of subquery min": {
			query:                     `sum by(group_1) (min_over_time((changes(metric_counter[5m]))[2d:2m]))`,
			expectedSpunOffSubqueries: 1,
		},
		"sum of subquery min with small offset": {
			query:                     `sum by(group_1) (min_over_time((changes(metric_counter[5m]))[1d:1m] offset 20s))`,
			expectedSpunOffSubqueries: 1,
		},
		"sum of subquery min with offset": {
			query:                     `sum by(group_1) (min_over_time((changes(metric_counter[5m]))[1d:1m] offset 1d))`,
			expectedSpunOffSubqueries: 1,
		},
		"sum of subquery min: offset query time -10m": {
			query:                     `sum by(group_1) (min_over_time((changes(metric_counter[5m]))[2d:2m]))`,
			expectedSpunOffSubqueries: 1,
			offsetQueryTime:           -10 * time.Minute,
		},
		"sum of subquery min: offset query time +10m": {
			query:                     `sum by(group_1) (min_over_time((changes(metric_counter[5m]))[2d:2m]))`,
			expectedSpunOffSubqueries: 1,
			offsetQueryTime:           10 * time.Minute,
		},
		"sum of subquery min: offset query time -33s": {
			query:                     `sum by(group_1) (min_over_time((changes(metric_counter[5m]))[2d:2m]))`,
			expectedSpunOffSubqueries: 1,
			offsetQueryTime:           -33 * time.Second,
		},
		"sum of subquery min: offset query time +33s": {
			query:                     `sum by(group_1) (min_over_time((changes(metric_counter[5m]))[2d:2m]))`,
			expectedSpunOffSubqueries: 1,
			offsetQueryTime:           33 * time.Second,
		},
		"sum of subquery min: offset query time +1h": {
			query:                     `sum by(group_1) (min_over_time((changes(metric_counter[5m]))[2d:2m]))`,
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
					[2d:])`,
			expectedSpunOffSubqueries: 1,
		},
		"double subquery deriv": {
			query:                     `max_over_time( deriv( rate(metric_counter[10m])[5m:1m] )[2d:] )`,
			expectedSpunOffSubqueries: 1,
		},
		"subquery min_over_time with aggr": {
			query: `min_over_time(
						sum by(group_1) (
							rate(metric_counter[5m])
						)[2d:]
					)`,
			expectedSpunOffSubqueries: 1,
		},
	}

	queryable := setupSubquerySpinOffTestSeries(t, 2*24*time.Hour)
	engine := newEngine()
	downstream := &downstreamHandler{
		engine:    engine,
		queryable: queryable,
	}

	runSubquerySpinOffTests(t, tests, engine, downstream)
}

func TestSubquerySpinOff_LongRangeQuery(t *testing.T) {
	t.Parallel()

	tests := map[string]subquerySpinOffTest{
		"subquery max: multiple range queries": {
			query: `max_over_time(
							rate(metric_counter[1m])
						[10d:1m]
					)`,
			expectedSpunOffSubqueries: 1,
		},
	}

	queryable := setupSubquerySpinOffTestSeries(t, 10*24*time.Hour)
	engine := newEngine()
	downstream := &downstreamHandler{
		engine:    engine,
		queryable: queryable,
	}

	runSubquerySpinOffTests(t, tests, engine, downstream)
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

func TestSpinOffQueryHandler(t *testing.T) {
	now := time.Now()

	numSeries := 5
	series := make([]storage.Series, 0, numSeries)
	// Add counter series.
	for i := 0; i < numSeries; i++ {
		gen := factor(float64(i) * 0.1)
		series = append(series, newSeries(newTestCounterLabels(len(series)), now.Add(-5*time.Minute), now, 30*time.Second, gen))
	}
	// Create a queryable on the fixtures.
	queryable := storageSeriesQueryable(series)

	engine := newEngine()
	downstream := &downstreamHandler{
		engine:    engine,
		queryable: queryable,
	}

	gotRequestCt := 0
	codec := newTestPrometheusCodec()
	// Create a local server that handles queries.
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRequestCt++
		if gotRequestCt == 1 {
			// Test a failure case where the first request fails.
			http.Error(w, "unexpected request", http.StatusInternalServerError)
			return
		}

		if r.URL.Path != "/prometheus/api/v1/query_range" {
			http.Error(w, "invalid path", http.StatusNotFound)
			return
		}

		if org := r.Header.Get(user.OrgIDHeaderName); org != "test" {
			http.Error(w, "invalid org", http.StatusUnauthorized)
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

	spinOffQueryHandler, err := newSpinOffQueryHandler(
		codec, log.NewNopLogger(), httpServer.URL+"/prometheus/api/v1/query_range", 3, &mockRetryMetrics{})
	require.NoError(t, err)

	// Ensure we have had no requests yet.
	require.Equal(t, 0, gotRequestCt)

	req := &PrometheusRangeQueryRequest{
		path:      "/query_range",
		start:     util.TimeToMillis(now.Add(-5 * time.Minute)),
		end:       util.TimeToMillis(now),
		step:      30 * time.Second.Milliseconds(),
		queryExpr: parseQuery(t, "max_over_time(rate(metric_counter[1m])[5m:1m])"),
	}
	ctx := user.InjectOrgID(context.Background(), "test")
	resp, err := spinOffQueryHandler.Do(ctx, req)
	require.NoError(t, err)

	// Ensure we got the expected number of requests.
	require.Equal(t, 2, gotRequestCt)

	// Ensure the query produces some results.
	require.NotEmpty(t, resp.(*PrometheusResponse).Data.Result)
	requireValidSamples(t, resp.(*PrometheusResponse).Data.Result)

	// Ensure the result is the same as the one produced by the downstream handler.
	expectedRes, err := downstream.Do(context.Background(), req)
	require.Nil(t, err)

	expectedPrometheusRes := expectedRes.(*PrometheusResponse)
	sort.Sort(byLabels(expectedPrometheusRes.Data.Result))
	sort.Sort(byLabels(resp.(*PrometheusResponse).Data.Result))
	approximatelyEquals(t, expectedPrometheusRes, resp.(*PrometheusResponse))
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
		gen := factor(float64(i) * 0.1)
		if i >= numSeries-numStaleSeries {
			// Wrap the generator to inject the staleness marker between minute 10 and 20.
			gen = stale(time.Now().Add(10*time.Minute), time.Now().Add(20*time.Minute), gen)
		}

		series = append(series, newSeries(newTestCounterLabels(seriesID), samplesStart, samplesEnd, samplesStep, gen))
		seriesID++
	}

	// Add a special series whose data points end earlier than the end of the queried time range
	// and has NO stale marker.
	series = append(series, newSeries(newTestCounterLabels(seriesID),
		samplesStart, time.Now().Add(-5*time.Minute), samplesStep, factor(2)))
	seriesID++

	// Add a special series whose data points end earlier than the end of the queried time range
	// and HAS a stale marker at the end.
	series = append(series, newSeries(newTestCounterLabels(seriesID),
		samplesStart, time.Now().Add(-5*time.Minute), samplesStep, stale(time.Now().Add(-6*time.Minute), time.Now().Add(-4*time.Minute), factor(2))))
	seriesID++

	// Add a special series whose data points start later than the start of the queried time range.
	series = append(series, newSeries(newTestCounterLabels(seriesID),
		time.Now().Add(5*time.Minute), samplesEnd, samplesStep, factor(2)))

	// Create a queryable on the fixtures.
	queryable := storageSeriesQueryable(series)

	return queryable
}

func runSubquerySpinOffTests(t *testing.T, tests map[string]subquerySpinOffTest, engine *promql.Engine, downstream MetricsQueryHandler) {
	t.Helper()

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			req := &PrometheusInstantQueryRequest{
				path:      "/query",
				time:      util.TimeToMillis(time.Now().Add(testData.offsetQueryTime)),
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
			if !testData.expectEmptyResult {
				require.NotEmpty(t, expectedPrometheusRes.Data.Result)
				requireValidSamples(t, expectedPrometheusRes.Data.Result)
			}

			if testData.expectedSpunOffSubqueries > 0 {
				// Remove position information from annotations, to mirror what we expect from the sharded queries below.
				removeAllAnnotationPositionInformation(expectedPrometheusRes.Infos)
				removeAllAnnotationPositionInformation(expectedPrometheusRes.Warnings)
			}

			reg := prometheus.NewPedanticRegistry()
			spinoffMiddleware := newSpinOffSubqueriesMiddleware(
				mockLimits{
					instantQueriesWithSubquerySpinOff: []string{".*"},
				},
				log.NewNopLogger(),
				engine,
				downstream,
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
	}
}
