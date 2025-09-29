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
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			logger := log.NewNopLogger()
			pass := NewOptimizationPass(&mockLimits{}, seriesPerShard, reg, logger)

			expr, err := parser.ParseExpr(testCase.input)
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), "tenant-1")
			ctx = querymiddleware.ContextWithRequestHintsAndOptions(ctx, testCase.hints, testCase.options)
			output, err := pass.Apply(ctx, expr)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedOutput, output.String())
		})
	}
}

type mockLimits struct{}

func (m *mockLimits) QueryShardingTotalShards(userID string) int {
	return 4
}

func (m *mockLimits) QueryShardingMaxRegexpSizeBytes(userID string) int {
	return 0
}

func (m *mockLimits) QueryShardingMaxShardedQueries(userID string) int {
	return 6
}

func (m *mockLimits) CompactorSplitAndMergeShards(userID string) int {
	return 1
}
