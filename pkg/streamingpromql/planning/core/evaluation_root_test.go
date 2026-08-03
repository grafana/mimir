// SPDX-License-Identifier: AGPL-3.0-only

package core_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// TestEvaluationRootNode_PlanningAndRoundTrip confirms that the __vector_evaluation_root__ marker function is
// converted to an EvaluationRoot node when building the query plan, and that the resulting plan
// round-trips through encoding and decoding.
//
// The pass-through evaluation behaviour of EvaluationRoot is exercised by the end-to-end subquery
// spin-off tests.
func TestEvaluationRootNode_VectorResult_Planning(t *testing.T) {
	opts := streamingpromql.NewTestEngineOpts()
	planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	ctx := context.Background()
	plan, err := planner.NewQueryPlan(ctx, `__vector_evaluation_root__(sum(foo))`, types.NewInstantQueryTimeRange(time.Now()), streamingpromql.DefaultLookbackDelta, false, streamingpromql.NoopPlanningObserver{})
	require.NoError(t, err)

	root, ok := plan.Root.(*core.EvaluationRoot)
	require.True(t, ok, "expected plan root to be an EvaluationRoot, but got %T", plan.Root)
	require.IsType(t, &core.AggregateExpression{}, root.Inner, "expected EvaluationRoot's child to be the wrapped aggregation")
}

func TestEvaluationRootNode_ScalarResult_Planning(t *testing.T) {
	opts := streamingpromql.NewTestEngineOpts()
	planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	ctx := context.Background()
	plan, err := planner.NewQueryPlan(ctx, `__scalar_evaluation_root__(scalar(sum(foo)))`, types.NewInstantQueryTimeRange(time.Now()), streamingpromql.DefaultLookbackDelta, false, streamingpromql.NoopPlanningObserver{})
	require.NoError(t, err)

	root, ok := plan.Root.(*core.EvaluationRoot)
	require.True(t, ok, "expected plan root to be an EvaluationRoot, but got %T", plan.Root)
	require.IsType(t, &core.FunctionCall{}, root.Inner, "expected EvaluationRoot's child to be the wrapped aggregation")
	require.Equal(t, "scalar", root.Inner.(*core.FunctionCall).Function.PromQLName())
}
