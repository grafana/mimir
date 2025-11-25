package plan

import (
	"context"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
)

type EagerLoadMiddlewareSelectorsOptimizationPass struct{}

func (e *EagerLoadMiddlewareSelectorsOptimizationPass) Name() string {
	return "Eager load middleware selectors"
}

func (e *EagerLoadMiddlewareSelectorsOptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, maximumSupportedQueryPlanVersion planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	inspectResult := optimize.Inspect(plan.Root)
	if !inspectResult.HasSelectors || !inspectResult.IsRewrittenByMiddleware {
		return plan, nil
	}

	EnableEagerLoadingOnAllSelectors(plan.Root)

	return plan, nil
}

func EnableEagerLoadingOnAllSelectors(node planning.Node) {
	switch node := node.(type) {
	case *core.VectorSelector:
		node.EagerLoad = true
	case *core.MatrixSelector:
		node.EagerLoad = true
	default:
		for child := range planning.ChildrenIter(node) {
			EnableEagerLoadingOnAllSelectors(child)
		}
	}
}
