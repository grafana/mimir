// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/util"
)

func Test_queryStatsMiddleware_Do(t *testing.T) {
	type args struct {
		req Request
	}
	tests := []struct {
		name    string
		args    args
		want    *strings.Reader
		wantErr bool
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
			want: strings.NewReader(`
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
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			mw := newQueryStatsMiddleware(reg)
			_, err := mw.Wrap(mockHandlerWith(nil, nil)).Do(user.InjectOrgID(context.Background(), "test"), tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("queryStatsMiddleware_Do() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.want != nil {
				assert.NoError(t, testutil.GatherAndCompare(reg, tt.want))
			}
		})
	}
}
