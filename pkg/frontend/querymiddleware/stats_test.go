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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	querier_stats "github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util"
)

func Test_queryStatsMiddleware_Do(t *testing.T) {
	type args struct {
		req Request
	}
	tests := []struct {
		name                 string
		args                 args
		expectedMetrics      *strings.Reader
		expectedQueryDetails QueryDetails
	}{
		{
			name: "happy path",
			args: args{
				req: &PrometheusRangeQueryRequest{
					Path:  "/query_range",
					Start: util.TimeToMillis(start),
					End:   util.TimeToMillis(end),
					Step:  step.Milliseconds(),
					Query: `sum(sum_over_time(metric{app="test",namespace=~"short"}[5m]))`,
				},
			},
			expectedMetrics: strings.NewReader(`
			# HELP cortex_query_frontend_non_step_aligned_queries_total Total queries sent that are not step aligned.
			# TYPE cortex_query_frontend_non_step_aligned_queries_total counter
			cortex_query_frontend_non_step_aligned_queries_total 1
			# HELP cortex_query_frontend_regexp_matcher_count Total number of regexp matchers
			# TYPE cortex_query_frontend_regexp_matcher_count counter
			cortex_query_frontend_regexp_matcher_count 1
			# HELP cortex_query_frontend_regexp_matcher_optimized_count Total number of optimized regexp matchers
			# TYPE cortex_query_frontend_regexp_matcher_optimized_count counter
			cortex_query_frontend_regexp_matcher_optimized_count 1
			`),
			expectedQueryDetails: QueryDetails{
				QuerierStats: &querier_stats.Stats{},
				Start:        start.Truncate(time.Millisecond),
				End:          end.Truncate(time.Millisecond),
				MinT:         start.Truncate(time.Millisecond).Add(-5 * time.Minute),
				MaxT:         end.Truncate(time.Millisecond),
				Step:         step,
			},
		},
		{
			name: "parseExpr failed",
			args: args{
				req: &PrometheusRangeQueryRequest{
					Path:  "/query_range",
					Start: util.TimeToMillis(start),
					End:   util.TimeToMillis(end),
					Step:  step.Milliseconds(),
					Query: `?`,
				},
			},
			expectedMetrics: strings.NewReader(`
			# HELP cortex_query_frontend_non_step_aligned_queries_total Total queries sent that are not step aligned.
			# TYPE cortex_query_frontend_non_step_aligned_queries_total counter
			cortex_query_frontend_non_step_aligned_queries_total 1
			# HELP cortex_query_frontend_regexp_matcher_count Total number of regexp matchers
			# TYPE cortex_query_frontend_regexp_matcher_count counter
			cortex_query_frontend_regexp_matcher_count 0
			# HELP cortex_query_frontend_regexp_matcher_optimized_count Total number of optimized regexp matchers
			# TYPE cortex_query_frontend_regexp_matcher_optimized_count counter
			cortex_query_frontend_regexp_matcher_optimized_count 0
			`),
			expectedQueryDetails: QueryDetails{
				QuerierStats: &querier_stats.Stats{},
				Start:        start.Truncate(time.Millisecond),
				End:          end.Truncate(time.Millisecond),
				MinT:         time.Time{}, // empty because the query is invalid
				MaxT:         time.Time{}, // empty because the query is invalid
				Step:         step,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			mw := newQueryStatsMiddleware(reg, newEngine())
			actualDetails, ctx := ContextWithEmptyDetails(context.Background())
			_, err := mw.Wrap(mockHandlerWith(nil, nil)).Do(user.InjectOrgID(ctx, "test"), tt.args.req)
			require.NoError(t, err)
			assert.NoError(t, testutil.GatherAndCompare(reg, tt.expectedMetrics))
			assert.Equal(t, tt.expectedQueryDetails, *actualDetails)
		})
	}
}
