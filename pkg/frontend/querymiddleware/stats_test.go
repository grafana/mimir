// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	querierapi "github.com/grafana/mimir/pkg/querier/api"
	querier_stats "github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util"
)

func Test_queryStatsMiddleware_Do(t *testing.T) {
	const tenantID = "test"
	type args struct {
		ctx context.Context
		req []MetricsQueryRequest // Stats are cumulative, in particular because we don't split up remote read queries.
	}
	tests := map[string]struct {
		args                 args
		expectedMetrics      string
		expectedQueryDetails QueryDetails
	}{
		"happy path range query": {
			args: args{
				req: []MetricsQueryRequest{&PrometheusRangeQueryRequest{
					path:      "/query_range",
					start:     util.TimeToMillis(start),
					end:       util.TimeToMillis(end),
					step:      step.Milliseconds(),
					queryExpr: parseQuery(t, `sum(sum_over_time(metric{app="test",namespace=~"short"}[5m]))`),
				}},
			},
			expectedMetrics: `
			# HELP cortex_query_frontend_queries_expression_bytes Histogram of the length of query expressions requested.
			# TYPE cortex_query_frontend_queries_expression_bytes histogram
			cortex_query_frontend_queries_expression_bytes_bucket{user="test",le="+Inf"} 1
			cortex_query_frontend_queries_expression_bytes_sum{user="test"} 61
			cortex_query_frontend_queries_expression_bytes_count{user="test"} 1
			# HELP cortex_query_frontend_regexp_matchers_total Total number of regexp matchers
			# TYPE cortex_query_frontend_regexp_matchers_total counter
			cortex_query_frontend_regexp_matchers_total 1
			# HELP cortex_query_frontend_regexp_matchers_optimized_total Total number of optimized regexp matchers
			# TYPE cortex_query_frontend_regexp_matchers_optimized_total counter
			cortex_query_frontend_regexp_matchers_optimized_total 1
			`,
			expectedQueryDetails: QueryDetails{
				QuerierStats: &querier_stats.SafeStats{},
				Start:        start.Truncate(time.Millisecond),
				End:          end.Truncate(time.Millisecond),
				MinT:         start.Truncate(time.Millisecond).Add(-5 * time.Minute).Add(time.Millisecond), // query range is left-open, but minT is inclusive
				MaxT:         end.Truncate(time.Millisecond),
				Step:         step,
			},
		},
		"explicit consistency range query": {
			args: args{
				ctx: querierapi.ContextWithReadConsistencyLevel(context.Background(), querierapi.ReadConsistencyStrong),
				req: []MetricsQueryRequest{&PrometheusRangeQueryRequest{
					path:      "/query_range",
					start:     util.TimeToMillis(start),
					end:       util.TimeToMillis(end),
					step:      step.Milliseconds(),
					queryExpr: parseQuery(t, `sum(sum_over_time(metric{app="test",namespace=~"short"}[5m]))`),
				}},
			},
			expectedMetrics: `
			# HELP cortex_query_frontend_queries_expression_bytes Histogram of the length of query expressions requested.
			# TYPE cortex_query_frontend_queries_expression_bytes histogram
			cortex_query_frontend_queries_expression_bytes_bucket{user="test",le="+Inf"} 1
			cortex_query_frontend_queries_expression_bytes_sum{user="test"} 61
			cortex_query_frontend_queries_expression_bytes_count{user="test"} 1
			# HELP cortex_query_frontend_regexp_matchers_total Total number of regexp matchers
			# TYPE cortex_query_frontend_regexp_matchers_total counter
			cortex_query_frontend_regexp_matchers_total 1
			# HELP cortex_query_frontend_regexp_matchers_optimized_total Total number of optimized regexp matchers
			# TYPE cortex_query_frontend_regexp_matchers_optimized_total counter
			cortex_query_frontend_regexp_matchers_optimized_total 1
			# HELP cortex_query_frontend_queries_consistency_total Total number of queries that explicitly request a level of consistency.
			# TYPE cortex_query_frontend_queries_consistency_total counter
			cortex_query_frontend_queries_consistency_total{consistency="strong",user="test"} 1
			`,
			expectedQueryDetails: QueryDetails{
				QuerierStats: &querier_stats.SafeStats{},
				Start:        start.Truncate(time.Millisecond),
				End:          end.Truncate(time.Millisecond),
				MinT:         start.Truncate(time.Millisecond).Add(-5 * time.Minute).Add(time.Millisecond), // query range is left-open, but minT is inclusive
				MaxT:         end.Truncate(time.Millisecond),
				Step:         step,
			},
		},
		"instant query": {
			args: args{
				req: []MetricsQueryRequest{NewPrometheusInstantQueryRequest(
					"/query",
					nil,
					start.Truncate(time.Millisecond).UnixMilli(),
					5*time.Minute,
					parseQuery(t, `sum(metric{app="test",namespace=~"short"})`),
					Options{},
					nil,
					"",
				)},
			},
			expectedMetrics: `
			# HELP cortex_query_frontend_queries_expression_bytes Histogram of the length of query expressions requested.
			# TYPE cortex_query_frontend_queries_expression_bytes histogram
			cortex_query_frontend_queries_expression_bytes_bucket{user="test",le="+Inf"} 1
			cortex_query_frontend_queries_expression_bytes_sum{user="test"} 42
			cortex_query_frontend_queries_expression_bytes_count{user="test"} 1
			# HELP cortex_query_frontend_regexp_matchers_total Total number of regexp matchers
			# TYPE cortex_query_frontend_regexp_matchers_total counter
			cortex_query_frontend_regexp_matchers_total 1
			# HELP cortex_query_frontend_regexp_matchers_optimized_total Total number of optimized regexp matchers
			# TYPE cortex_query_frontend_regexp_matchers_optimized_total counter
			cortex_query_frontend_regexp_matchers_optimized_total 1
			`,
			expectedQueryDetails: QueryDetails{
				QuerierStats: &querier_stats.SafeStats{},
				Start:        start.Truncate(time.Millisecond),
				End:          start.Truncate(time.Millisecond),
				MinT:         start.Truncate(time.Millisecond).Add(-5 * time.Minute).Add(time.Millisecond), // query range is left-open, but minT is inclusive
				MaxT:         start.Truncate(time.Millisecond),
			},
		},
		"remote read queries without hints": {
			args: args{
				req: []MetricsQueryRequest{
					mustSucceed(remoteReadToMetricsQueryRequest(
						"/read",
						&prompb.Query{
							StartTimestampMs: start.Truncate(time.Millisecond).UnixMilli(),
							EndTimestampMs:   end.Truncate(time.Millisecond).Add(10 * time.Minute).UnixMilli(),
							Matchers: []*prompb.LabelMatcher{
								{
									Type:  prompb.LabelMatcher_RE,
									Name:  "app",
									Value: "test",
								},
							},
						},
					)),
					mustSucceed(remoteReadToMetricsQueryRequest(
						"/read",
						&prompb.Query{
							StartTimestampMs: start.Truncate(time.Millisecond).Add(-30 * time.Minute).UnixMilli(),
							EndTimestampMs:   end.Truncate(time.Millisecond).UnixMilli(),
							Matchers: []*prompb.LabelMatcher{
								{
									Type:  prompb.LabelMatcher_RE,
									Name:  "app",
									Value: "test",
								},
							},
						},
					)),
				},
			},
			expectedMetrics: `
			# HELP cortex_query_frontend_queries_expression_bytes Histogram of the length of query expressions requested.
			# TYPE cortex_query_frontend_queries_expression_bytes histogram
			cortex_query_frontend_queries_expression_bytes_bucket{user="test",le="+Inf"} 2
			cortex_query_frontend_queries_expression_bytes_sum{user="test"} 26
			cortex_query_frontend_queries_expression_bytes_count{user="test"} 2
			# HELP cortex_query_frontend_regexp_matchers_total Total number of regexp matchers
			# TYPE cortex_query_frontend_regexp_matchers_total counter
			cortex_query_frontend_regexp_matchers_total 2
			# HELP cortex_query_frontend_regexp_matchers_optimized_total Total number of optimized regexp matchers
			# TYPE cortex_query_frontend_regexp_matchers_optimized_total counter
			cortex_query_frontend_regexp_matchers_optimized_total 2
			`,
			expectedQueryDetails: QueryDetails{
				QuerierStats: &querier_stats.SafeStats{},
				Start:        start.Truncate(time.Millisecond).Add(-30 * time.Minute),
				End:          end.Truncate(time.Millisecond).Add(10 * time.Minute),
				MinT:         start.Truncate(time.Millisecond).Add(-30 * time.Minute).Add(time.Millisecond), // query range is left-open, but minT is inclusive
				MaxT:         end.Truncate(time.Millisecond).Add(10 * time.Minute),
			},
		},
		"remote read queries with hints": {
			args: args{
				req: []MetricsQueryRequest{
					mustSucceed(remoteReadToMetricsQueryRequest(
						"/read",
						&prompb.Query{
							StartTimestampMs: start.Truncate(time.Millisecond).UnixMilli(),
							EndTimestampMs:   end.Truncate(time.Millisecond).Add(10 * time.Minute).UnixMilli(),
							Matchers: []*prompb.LabelMatcher{
								{
									Type:  prompb.LabelMatcher_RE,
									Name:  "app",
									Value: "test",
								},
							},
							Hints: &prompb.ReadHints{
								// These are ignored in queries, we expect no effect on statistics.
								StartMs: start.Truncate(time.Millisecond).Add(-10 * time.Minute).UnixMilli(),
								EndMs:   end.Truncate(time.Millisecond).Add(20 * time.Minute).UnixMilli(),
							},
						},
					)),
				},
			},
			expectedMetrics: `
			# HELP cortex_query_frontend_queries_expression_bytes Histogram of the length of query expressions requested.
			# TYPE cortex_query_frontend_queries_expression_bytes histogram
			cortex_query_frontend_queries_expression_bytes_bucket{user="test",le="+Inf"} 1
			cortex_query_frontend_queries_expression_bytes_sum{user="test"} 13
			cortex_query_frontend_queries_expression_bytes_count{user="test"} 1
			# HELP cortex_query_frontend_regexp_matchers_total Total number of regexp matchers
			# TYPE cortex_query_frontend_regexp_matchers_total counter
			cortex_query_frontend_regexp_matchers_total 1
			# HELP cortex_query_frontend_regexp_matchers_optimized_total Total number of optimized regexp matchers
			# TYPE cortex_query_frontend_regexp_matchers_optimized_total counter
			cortex_query_frontend_regexp_matchers_optimized_total 1
			`,
			expectedQueryDetails: QueryDetails{
				QuerierStats: &querier_stats.SafeStats{},
				Start:        start.Truncate(time.Millisecond),
				End:          end.Truncate(time.Millisecond).Add(10 * time.Minute),
				MinT:         start.Truncate(time.Millisecond).Add(time.Millisecond), // query range is left-open, but minT is inclusive
				MaxT:         end.Truncate(time.Millisecond).Add(10 * time.Minute),
			},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			runForEngines(t, func(t *testing.T, opts promql.EngineOpts, eng promql.QueryEngine) {
				reg := prometheus.NewPedanticRegistry()
				mw := newQueryStatsMiddleware(reg, opts)
				ctx := context.Background()
				if tt.args.ctx != nil {
					ctx = tt.args.ctx
				}
				actualDetails, ctx := ContextWithEmptyDetails(ctx)
				ctx = user.InjectOrgID(ctx, tenantID)

				for _, req := range tt.args.req {
					_, err := mw.Wrap(mockHandlerWith(nil, nil)).Do(ctx, req)
					require.NoError(t, err)
				}

				assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(tt.expectedMetrics)))
				assert.Equal(t, tt.expectedQueryDetails, *actualDetails)
			})
		})
	}
}
