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
)

func TestOptimizationPass(t *testing.T) {
	// Most of the sharding logic is already tested in the query-frontend package, so we don't need to repeat this here.

	testCases := map[string]struct {
		input          string
		expectedOutput string
	}{
		"unshardable expression": {
			input:          `foo`,
			expectedOutput: `foo`,
		},
		"shardable expression": {
			input:          `sum(foo)`,
			expectedOutput: `sum(__sharded_concat__(sum(foo{__query_shard__="1_of_2"}), sum(foo{__query_shard__="2_of_2"})))`,
		},
	}

	const maxSeriesPerShard = 0

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			logger := log.NewNopLogger()
			pass := NewOptimizationPass(&mockLimits{}, maxSeriesPerShard, reg, logger)

			expr, err := parser.ParseExpr(testCase.input)
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), "tenant-1")
			output, err := pass.Apply(ctx, expr)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedOutput, output.String())
		})
	}
}

type mockLimits struct{}

func (m *mockLimits) QueryShardingTotalShards(userID string) int {
	return 2
}

func (m *mockLimits) QueryShardingMaxRegexpSizeBytes(userID string) int {
	return 0
}

func (m *mockLimits) QueryShardingMaxShardedQueries(userID string) int {
	return 0
}

func (m *mockLimits) CompactorSplitAndMergeShards(userID string) int {
	return 1
}
