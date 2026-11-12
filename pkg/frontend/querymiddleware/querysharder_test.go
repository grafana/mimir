// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

func TestQuerySharder(t *testing.T) {
	for _, tc := range []struct {
		name        string
		query       string
		limits      mockLimits
		options     QueryShardingOptions
		canceled    bool
		wantQueries int
		wantErr     error
	}{
		{
			name: "ordinary sum", query: "sum(foo)",
			limits: mockLimits{totalShards: 8}, wantQueries: 8,
		},
		{
			name: "ordinary analysis counts avg as two legs", query: "avg(foo)",
			limits: mockLimits{totalShards: 8, maxShardedQueries: 8}, wantQueries: 8,
		},
		{
			name: "unsupported selector", query: "foo",
			limits: mockLimits{totalShards: 8},
		},
		{
			name: "budget includes time splits", query: "sum(foo)",
			limits:  mockLimits{totalShards: 8, maxShardedQueries: 4},
			options: QueryShardingOptions{TotalQueries: 2}, wantQueries: 2,
		},
		{
			name: "budget leaving one shard suppresses rewrite", query: "avg(foo)",
			limits: mockLimits{totalShards: 8, maxShardedQueries: 2},
		},
		{
			name: "tenant disabled even with requested shards", query: "sum(foo)",
			limits: mockLimits{totalShards: 0}, options: QueryShardingOptions{RequestedShardCount: 8},
		},
		{
			name: "tenant configured for one shard", query: "sum(foo)",
			limits: mockLimits{totalShards: 1},
		},
		{
			name: "regexp limit suppresses rewrite", query: `sum(foo{job=~"long"})`,
			limits: mockLimits{totalShards: 8, maxRegexpSizeBytes: 3},
		},
		{
			name: "canceled rewrite", query: "sum(foo)",
			limits: mockLimits{totalShards: 8}, canceled: true, wantErr: context.Canceled,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := promqlext.NewPromQLParser().ParseExpr(tc.query)
			require.NoError(t, err)
			original := expr.String()
			queryStats, ctx := stats.ContextWithEmptyStats(t.Context())
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			if tc.canceled {
				cancel()
			}
			reg := prometheus.NewPedanticRegistry()
			sharder := NewQuerySharder(astmapper.EmbeddedQueriesSquasher, tc.limits, 0, reg, log.NewNopLogger())
			actual, err := sharder.Shard(ctx, []string{"tenant"}, expr, tc.options)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			successes := 0
			if tc.wantQueries > 0 {
				require.NotNil(t, actual)
				successes = 1
			} else {
				require.Nil(t, actual)
				require.Equal(t, original, expr.String(), "fallback must not mutate the input")
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
