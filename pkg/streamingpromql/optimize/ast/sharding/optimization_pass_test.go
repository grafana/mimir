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
	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

func TestOptimizationPass(t *testing.T) {
	// Most of the sharding logic is already tested in the query-frontend package, so we don't need to repeat this here.
	const seriesPerShard = 1000

	testCases := map[string]struct {
		input          string
		options        requestoptions.Options
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
			options: requestoptions.Options{
				ShardingDisabled: true,
			},
			expectedOutput: `sum(foo)`,
		},
		"shardable expression with specific requested shard count": {
			input: `sum(foo)`,
			options: requestoptions.Options{
				TotalShards: 3,
			},
			// The requested shard count is rounded up to the next power of two.
			expectedOutput: `sum(__sharded_concat__(sum(foo{__query_shard__="1_of_4"}), sum(foo{__query_shard__="2_of_4"}), sum(foo{__query_shard__="3_of_4"}), sum(foo{__query_shard__="4_of_4"})))`,
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

			ctx = querymiddleware.ContextWithRequestHints(ctx, testCase.hints)
			ctx = requestoptions.ContextWithOptions(ctx, testCase.options)
			output, err := pass.Apply(ctx, afterRewrite, types.QueryTimeRange{})
			require.NoError(t, err)
			require.Equal(t, testCase.expectedOutput, output.String())
		})
	}
}

// TestOptimizationPass_EvaluationRoots confirms that, when a query has been rewritten to spin off
// subqueries, each __vector_evaluation_root__/__scalar_evaluation_root__ subtree is sharded independently and everything outside the
// markers is left unchanged.
func TestOptimizationPass_EvaluationRoots(t *testing.T) {
	const seriesPerShard = 1000

	testCases := map[string]struct {
		input          string
		options        requestoptions.Options
		expectedOutput string
	}{
		"single shardable vector evaluation root": {
			input:          `__vector_evaluation_root__(sum(foo))`,
			expectedOutput: `__vector_evaluation_root__(sum(__sharded_concat__(sum(foo{__query_shard__="1_of_4"}), sum(foo{__query_shard__="2_of_4"}), sum(foo{__query_shard__="3_of_4"}), sum(foo{__query_shard__="4_of_4"}))))`,
		},
		"single shardable scalar evaluation root": {
			input:          `__scalar_evaluation_root__(scalar(sum(foo)))`,
			expectedOutput: `__scalar_evaluation_root__(scalar(sum(__sharded_concat__(sum(foo{__query_shard__="1_of_4"}), sum(foo{__query_shard__="2_of_4"}), sum(foo{__query_shard__="3_of_4"}), sum(foo{__query_shard__="4_of_4"})))))`,
		},
		"single unshardable evaluation root": {
			input:          `__vector_evaluation_root__(foo)`,
			expectedOutput: `__vector_evaluation_root__(foo)`,
		},
		"multiple evaluation roots, some shardable and some not": {
			// Emulates the shape produced when spinning off a subquery alongside a downstream query:
			// the shardable subtree is sharded, the unshardable one is left as-is.
			input:          `__vector_evaluation_root__(sum(foo)) + __vector_evaluation_root__(bar)`,
			expectedOutput: `__vector_evaluation_root__(sum(__sharded_concat__(sum(foo{__query_shard__="1_of_4"}), sum(foo{__query_shard__="2_of_4"}), sum(foo{__query_shard__="3_of_4"}), sum(foo{__query_shard__="4_of_4"})))) + __vector_evaluation_root__(bar)`,
		},
		"evaluation root inside a spun-off subquery": {
			// Only the marker's subtree is sharded; the surrounding subquery and outer function are left
			// to run on the query-frontend.
			input:          `max_over_time((__vector_evaluation_root__(sum(foo)))[2h:1m])`,
			expectedOutput: `max_over_time((__vector_evaluation_root__(sum(__sharded_concat__(sum(foo{__query_shard__="1_of_4"}), sum(foo{__query_shard__="2_of_4"}), sum(foo{__query_shard__="3_of_4"}), sum(foo{__query_shard__="4_of_4"})))))[2h:1m])`,
		},
		"sharding disabled": {
			input:          `__vector_evaluation_root__(sum(foo))`,
			options:        requestoptions.Options{ShardingDisabled: true},
			expectedOutput: `__vector_evaluation_root__(sum(foo))`,
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

			input, err := promqlext.NewPromQLParser().ParseExpr(testCase.input)
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), "tenant-1")
			ctx = requestoptions.ContextWithOptions(ctx, testCase.options)
			output, err := pass.Apply(ctx, input, types.QueryTimeRange{})
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
	mapper := astmapper.NewSubquerySpinOffMapper(astmapper.NewSelectorSubquerySpinOffWrapper(), defaultStepFunc, log.NewNopLogger(), stats)
	ast, err := promqlext.NewPromQLParser().ParseExpr(expr)
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
