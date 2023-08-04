// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"strings"
	"testing"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util"
)

func Test_queryStatsMiddleware_Do(t *testing.T) {
	type args struct {
		req Request
	}
	tests := []struct {
		name            string
		args            args
		expectedMetrics *strings.Reader
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
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			mw := newQueryStatsMiddleware(reg)
			_, err := mw.Wrap(mockHandlerWith(nil, nil)).Do(user.InjectOrgID(context.Background(), "test"), tt.args.req)
			require.NoError(t, err)
			assert.NoError(t, testutil.GatherAndCompare(reg, tt.expectedMetrics))
		})
	}
}
