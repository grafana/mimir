// SPDX-License-Identifier: AGPL-3.0-only

package sharding

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

func TestOptimizationPass(t *testing.T) {
	// Most of the sharding logic is already tested in the query-frontend package, so we don't need to repeat this here.
	const seriesPerShard = 1000

	testCases := map[string]struct {
		input          string
		options        querymiddleware.Options
		hints          *querymiddleware.Hints
		expectedOutput string
	}{
		"unshardable expression": {
			input:          `foo`,
			expectedOutput: `foo`,
		},
		"shardable expression": {
			input:          `sum(foo)`,
			expectedOutput: `sum(__sharded_concat__(sum(foo{__query_shard__="1_of_4"}), sum(foo{__query_shard__="2_of_4"}), sum(foo{__query_shard__="3_of_4"}), sum(foo{__query_shard__="4_of_4"})))`,
		},
		"shardable expression with sharding disabled": {
			input: `sum(foo)`,
			options: querymiddleware.Options{
				ShardingDisabled: true,
			},
			expectedOutput: `sum(foo)`,
		},
		"shardable expression with specific requested shard count": {
			input: `sum(foo)`,
			options: querymiddleware.Options{
				TotalShards: 3,
			},
			expectedOutput: `sum(__sharded_concat__(sum(foo{__query_shard__="1_of_3"}), sum(foo{__query_shard__="2_of_3"}), sum(foo{__query_shard__="3_of_3"})))`,
		},
		"shardable expression with estimated series count available": {
			input: `sum(foo)`,
			hints: &querymiddleware.Hints{
				CardinalityEstimate: &querymiddleware.EstimatedSeriesCount{
					EstimatedSeriesCount: seriesPerShard * 1.5,
				},
			},
			expectedOutput: `sum(__sharded_concat__(sum(foo{__query_shard__="1_of_2"}), sum(foo{__query_shard__="2_of_2"})))`,
		},
		"shardable expression with total query count available": {
			input: `sum(foo)`,
			hints: &querymiddleware.Hints{
				TotalQueries: 3,
			},
			expectedOutput: `sum(__sharded_concat__(sum(foo{__query_shard__="1_of_2"}), sum(foo{__query_shard__="2_of_2"})))`,
		},
		"shardable expression eligible for subquery spin-off": {
			input: `sum(sum_over_time(sum(foo)[5h:30s]))`,
			// We should not rewrite the expression, because it's not valid to try to shard the __subquery_spinoff__ selector.
			expectedOutput: `sum(sum_over_time(__subquery_spinoff__{__query__="sum(foo)",__range__="5h0m0s",__step__="30s"}[5h]))`,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			logger := log.NewNopLogger()
			limits := &mockLimits{
				totalShards:         4,
				maxShardedQueries:   6,
				splitAndMergeShards: 1,
			}
			pass := NewOptimizationPass(limits, seriesPerShard, reg, logger)

			ctx := user.InjectOrgID(context.Background(), "tenant-1")
			afterRewrite, err := rewriteForSubquerySpinoff(ctx, testCase.input)
			require.NoError(t, err)

			ctx = querymiddleware.ContextWithRequestHintsAndOptions(ctx, testCase.hints, testCase.options)
			output, err := pass.Apply(ctx, afterRewrite)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedOutput, output.String())
		})
	}
}

type mockLimits struct {
	totalShards         int
	regexpSizeBytes     int
	maxShardedQueries   int
	splitAndMergeShards int
}

func (m *mockLimits) QueryShardingTotalShards(userID string) int {
	return m.totalShards
}

func (m *mockLimits) QueryShardingMaxRegexpSizeBytes(userID string) int {
	return m.regexpSizeBytes
}

func (m *mockLimits) QueryShardingMaxShardedQueries(userID string) int {
	return m.maxShardedQueries
}

func (m *mockLimits) CompactorSplitAndMergeShards(userID string) int {
	return m.splitAndMergeShards
}

func rewriteForSubquerySpinoff(ctx context.Context, expr string) (parser.Expr, error) {
	stats := astmapper.NewSubquerySpinOffMapperStats()
	defaultStepFunc := func(rangeMillis int64) int64 { return 1000 }
	mapper := astmapper.NewSubquerySpinOffMapper(defaultStepFunc, log.NewNopLogger(), stats)
	ast, err := promqlext.NewExperimentalParser().ParseExpr(expr)
	if err != nil {
		return nil, err
	}

	rewrittenQuery, err := mapper.Map(ctx, ast)
	if err != nil {
		return nil, err
	}

	if stats.SpunOffSubqueries() == 0 {
		return ast, nil
	}

	return rewrittenQuery, nil
}
