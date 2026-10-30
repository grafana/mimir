// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

func TestQuerySharder_InjectedPlanner(t *testing.T) {
	plannerErr := errors.New("planning failed")
	rewriteErr := errors.New("rewrite failed")

	for _, tc := range []struct {
		name        string
		query       string
		limits      mockLimits
		legs        int
		generated   int
		nilExpr     bool
		plannerErr  error
		rewriteErr  error
		canceled    bool
		skipPlanner bool
		wantShards  int // Zero means Rewrite must not be called.
		wantQueries int
		wantErr     error
	}{
		{
			name: "custom planner can shard a bare selector", query: "foo",
			limits: mockLimits{totalShards: 8}, legs: 1, generated: 3,
			wantShards: 8, wantQueries: 3,
		},
		{
			name: "budget uses planner legs rather than ordinary analysis", query: "foo",
			limits: mockLimits{totalShards: 8, maxShardedQueries: 4}, legs: 2, generated: 4,
			wantShards: 2, wantQueries: 4,
		},
		{
			name: "nil expression falls back despite nonzero count", query: "foo",
			limits: mockLimits{totalShards: 8}, legs: 1, generated: 3, nilExpr: true,
			wantShards: 8,
		},
		{
			name: "zero generated queries falls back", query: "foo",
			limits: mockLimits{totalShards: 8}, legs: 1,
			wantShards: 8,
		},
		{
			name: "zero planner legs suppresses rewrite", query: "foo",
			limits: mockLimits{totalShards: 8}, generated: 3,
		},
		{
			name: "budget leaving one shard suppresses rewrite", query: "foo",
			limits: mockLimits{totalShards: 8, maxShardedQueries: 2}, legs: 2, generated: 4,
		},
		{
			name: "planner error", query: "foo",
			limits: mockLimits{totalShards: 8}, legs: 1, generated: 3,
			plannerErr: plannerErr, wantErr: plannerErr,
		},
		{
			name: "rewrite error does not record returned query count", query: "foo",
			limits: mockLimits{totalShards: 8}, legs: 1, generated: 3,
			rewriteErr: rewriteErr, wantShards: 8, wantErr: rewriteErr,
		},
		{
			name: "tenant disabled", query: "foo",
			limits: mockLimits{totalShards: 0}, legs: 1, generated: 3, skipPlanner: true,
		},
		{
			name: "tenant configured for one shard", query: "foo",
			limits: mockLimits{totalShards: 1}, legs: 1, generated: 3, skipPlanner: true,
		},
		{
			name: "regexp limit suppresses custom rewrite", query: `foo{job=~"long"}`,
			limits: mockLimits{totalShards: 8, maxRegexpSizeBytes: 3}, legs: 1, generated: 3, skipPlanner: true,
		},
		{
			name: "rewrite sees canceled context", query: "foo",
			limits: mockLimits{totalShards: 8}, legs: 1, generated: 3, canceled: true,
			wantShards: 8, wantErr: context.Canceled,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := promqlext.NewPromQLParser().ParseExpr(tc.query)
			require.NoError(t, err)
			rewritten, err := promqlext.NewPromQLParser().ParseExpr("bar")
			require.NoError(t, err)
			queryStats, ctx := stats.ContextWithEmptyStats(t.Context())
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			if tc.canceled {
				cancel()
			}

			var plannerDeadline time.Time
			plannerCalls, rewriteCalls := 0, 0
			planner := func(ctx context.Context, input parser.Expr) (QueryShardingPlan, error) {
				plannerCalls++
				require.Same(t, expr, input)
				deadline, ok := ctx.Deadline()
				require.True(t, ok, "planner must run under the sharding timeout")
				require.LessOrEqual(t, time.Until(deadline), shardingTimeout)
				plannerDeadline = deadline
				return QueryShardingPlan{
					ShardedSelectors: tc.legs,
					Rewrite: func(ctx context.Context, shards int) (parser.Expr, int, error) {
						rewriteCalls++
						require.Equal(t, tc.wantShards, shards)
						deadline, ok := ctx.Deadline()
						require.True(t, ok, "rewrite must run under the sharding timeout")
						require.Equal(t, plannerDeadline, deadline, "planner and rewrite share one timeout")
						if err := ctx.Err(); err != nil {
							return nil, 0, err
						}
						if tc.nilExpr {
							return nil, tc.generated, tc.rewriteErr
						}
						return rewritten, tc.generated, tc.rewriteErr
					},
				}, tc.plannerErr
			}
			reg := prometheus.NewPedanticRegistry()
			sharder := NewQuerySharder(planner, tc.limits, 0, reg, log.NewNopLogger())
			actual, err := sharder.Shard(ctx, []string{"tenant"}, expr, 0, nil, 1)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			if tc.skipPlanner {
				require.Zero(t, plannerCalls)
			} else {
				require.Equal(t, 1, plannerCalls)
			}
			if tc.wantShards > 1 {
				require.Equal(t, 1, rewriteCalls)
			} else {
				require.Zero(t, rewriteCalls)
			}
			successes := 0
			if tc.wantQueries > 0 {
				require.Same(t, rewritten, actual)
				successes = 1
			} else {
				require.Nil(t, actual)
			}
			require.Equal(t, uint32(tc.wantQueries), queryStats.LoadShardedQueries())
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
`, successes, tc.wantQueries)),
				"cortex_frontend_query_sharding_rewrites_attempted_total",
				"cortex_frontend_query_sharding_rewrites_succeeded_total",
				"cortex_frontend_sharded_queries_total"))
		})
	}
}
