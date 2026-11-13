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
	const histogram = `histogram_quantile(0.9, sum by (le, span_name) (rate(metric[5m])))`
	for _, tc := range []struct {
		name         string
		query        string
		limits       mockLimits
		options      QueryShardingOptions
		canceled     bool
		wantQueries  int
		wantSubset   bool
		wantMultiple bool
		wantErr      error
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
			name: "subset rewrite", query: histogram,
			limits: mockLimits{totalShards: 8}, options: QueryShardingOptions{SubsetLabelSharding: true},
			wantQueries: 8, wantSubset: true,
		},
		{
			name: "subset rewrite with multiple labels", query: `histogram_quantile(0.9, sum by (le, span_name, job) (rate(metric[5m])))`,
			limits: mockLimits{totalShards: 8}, options: QueryShardingOptions{SubsetLabelSharding: true},
			wantQueries: 8, wantSubset: true, wantMultiple: true,
		},
		{
			name: "duplicate grouping labels use single-label strategy", query: `histogram_quantile(0.9, sum by (le, span_name, span_name) (rate(metric[5m])))`,
			limits: mockLimits{totalShards: 8}, options: QueryShardingOptions{SubsetLabelSharding: true},
			wantQueries: 8, wantSubset: true,
		},
		{
			name: "subset budget includes time splits", query: histogram,
			limits:      mockLimits{totalShards: 8, maxShardedQueries: 4},
			options:     QueryShardingOptions{SubsetLabelSharding: true, TotalQueries: 2},
			wantQueries: 2, wantSubset: true,
		},
		{
			name: "subset unsupported shape falls back to ordinary sharding", query: "avg(foo)",
			limits:  mockLimits{totalShards: 8, maxShardedQueries: 8},
			options: QueryShardingOptions{SubsetLabelSharding: true}, wantQueries: 8,
		},
		{
			name: "budget leaving one shard suppresses rewrite", query: histogram,
			limits:  mockLimits{totalShards: 8, maxShardedQueries: 1},
			options: QueryShardingOptions{SubsetLabelSharding: true},
		},
		{
			name: "tenant disabled even with requested shards", query: histogram,
			limits:  mockLimits{totalShards: 0},
			options: QueryShardingOptions{SubsetLabelSharding: true, RequestedShardCount: 8},
		},
		{
			name: "tenant configured for one shard", query: histogram,
			limits: mockLimits{totalShards: 1}, options: QueryShardingOptions{SubsetLabelSharding: true},
		},
		{
			name: "regexp limit suppresses subset rewrite", query: `histogram_quantile(0.9, sum by (le, span_name) (rate(metric{job=~"long"}[5m])))`,
			limits:  mockLimits{totalShards: 8, maxRegexpSizeBytes: 3},
			options: QueryShardingOptions{SubsetLabelSharding: true},
		},
		{
			name: "canceled ordinary rewrite", query: "sum(foo)",
			limits: mockLimits{totalShards: 8}, canceled: true, wantErr: context.Canceled,
		},
		{
			name: "canceled subset rewrite", query: histogram,
			limits: mockLimits{totalShards: 8}, options: QueryShardingOptions{SubsetLabelSharding: true},
			canceled: true, wantErr: context.Canceled,
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
				if tc.wantSubset {
					if tc.wantMultiple {
						require.Contains(t, actual.String(), "_by_job,span_name")
					} else {
						require.Contains(t, actual.String(), "_by_span_name")
					}
				} else {
					require.NotContains(t, actual.String(), "_by_")
				}
			} else {
				require.Nil(t, actual)
			}
			if tc.wantSubset || tc.wantQueries == 0 {
				require.Equal(t, original, expr.String(), "subset rewriting and fallback must not mutate the input")
			}
			require.Equal(t, uint32(tc.wantQueries), queryStats.LoadShardedQueries())
			ordinarySuccesses, ordinaryQueries := successes, tc.wantQueries
			singleSuccesses, singleQueries, multipleSuccesses, multipleQueries := 0, 0, 0, 0
			if tc.wantSubset {
				ordinarySuccesses, ordinaryQueries = 0, 0
				if tc.wantMultiple {
					multipleSuccesses, multipleQueries = successes, tc.wantQueries
				} else {
					singleSuccesses, singleQueries = successes, tc.wantQueries
				}
			}
			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
# HELP cortex_frontend_query_sharding_rewrites_attempted_total Total number of queries the query-frontend attempted to shard.
# TYPE cortex_frontend_query_sharding_rewrites_attempted_total counter
cortex_frontend_query_sharding_rewrites_attempted_total 1
# HELP cortex_frontend_query_sharding_rewrites_succeeded_total Total number of queries the query-frontend successfully rewritten in a shardable way.
# TYPE cortex_frontend_query_sharding_rewrites_succeeded_total counter
cortex_frontend_query_sharding_rewrites_succeeded_total{strategy="ordinary"} %d
cortex_frontend_query_sharding_rewrites_succeeded_total{strategy="subset_labels_single"} %d
cortex_frontend_query_sharding_rewrites_succeeded_total{strategy="subset_labels_multiple"} %d
# HELP cortex_frontend_sharded_queries_total Total number of sharded queries.
# TYPE cortex_frontend_sharded_queries_total counter
cortex_frontend_sharded_queries_total{strategy="ordinary"} %d
cortex_frontend_sharded_queries_total{strategy="subset_labels_single"} %d
cortex_frontend_sharded_queries_total{strategy="subset_labels_multiple"} %d
`, ordinarySuccesses, singleSuccesses, multipleSuccesses, ordinaryQueries, singleQueries, multipleQueries)),
				"cortex_frontend_query_sharding_rewrites_attempted_total",
				"cortex_frontend_query_sharding_rewrites_succeeded_total",
				"cortex_frontend_sharded_queries_total"))

			var histogram strings.Builder
			histogram.WriteString("# HELP cortex_frontend_sharded_queries_per_query Number of sharded queries a single query has been rewritten to.\n# TYPE cortex_frontend_sharded_queries_per_query histogram\n")
			for _, expected := range []struct {
				strategy string
				count    int
				sum      int
			}{
				{"ordinary", ordinarySuccesses, ordinaryQueries},
				{"subset_labels_single", singleSuccesses, singleQueries},
				{"subset_labels_multiple", multipleSuccesses, multipleQueries},
			} {
				for i := range 10 {
					upper := 2 << i
					count := 0
					if expected.sum <= upper {
						count = expected.count
					}
					fmt.Fprintf(&histogram, "cortex_frontend_sharded_queries_per_query_bucket{strategy=%q,le=%q} %d\n", expected.strategy, fmt.Sprint(upper), count)
				}
				fmt.Fprintf(&histogram, "cortex_frontend_sharded_queries_per_query_bucket{strategy=%q,le=\"+Inf\"} %d\n", expected.strategy, expected.count)
				fmt.Fprintf(&histogram, "cortex_frontend_sharded_queries_per_query_sum{strategy=%q} %d\n", expected.strategy, expected.sum)
				fmt.Fprintf(&histogram, "cortex_frontend_sharded_queries_per_query_count{strategy=%q} %d\n", expected.strategy, expected.count)
			}
			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(histogram.String()), "cortex_frontend_sharded_queries_per_query"))
		})
	}
}
