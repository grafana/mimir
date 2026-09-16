// SPDX-License-Identifier: AGPL-3.0-only

package plan_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestNodeIdentifierOptimizationPass_Apply(t *testing.T) {
	testCases := map[string]struct {
		expr string
	}{
		"raw vector selector": {
			expr: `some_metric`,
		},
		"binary expression": {
			expr: "some_metric + other_metric",
		},
		"binary expression with functions and aggregation": {
			expr: "sum(rate(some_metric[5m])) + sum(rate(other_metric[5m]))",
		},
		"top-level aggregation": {
			expr: `group by (container) (build_info{cluster=~".*"})`,
		},
		"subquery with @ modifier": {
			expr: `avg_over_time(rate(metric[1m])[1s:60s] @ 60)`,
		},
	}

	for name, testCase := range testCases {
		timeRange := types.NewInstantQueryTimeRange(time.Now())
		observer := streamingpromql.NoopPlanningObserver{}

		opts := streamingpromql.NewTestEngineOpts()
		planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
		require.NoError(t, err)
		planner.RegisterQueryPlanOptimizationPass(plan.NewNodeIdentifierOptimizationPass())

		t.Run(name, func(t *testing.T) {
			p, err := planner.NewQueryPlan(t.Context(), testCase.expr, timeRange, streamingpromql.DefaultLookbackDelta, false, observer)
			require.NoError(t, err)
			verifyDepthFirstTree(t, 1, p.Root)
		})
	}
}

// verifyDepthFirstTree traverses the tree starting with node in depth first order and
// ensures that each node has the expected, sequentially numbered, planning ID.
func verifyDepthFirstTree(t *testing.T, expected int64, node planning.Node) int64 {
	require.Equal(t, expected, node.GetPlanningId(), "expected planning node ID %d for %s but got %d", expected, node.Describe(), node.GetPlanningId())

	for child := range planning.ChildrenIter(node) {
		expected++
		expected = verifyDepthFirstTree(t, expected, child)
	}

	return expected
}
