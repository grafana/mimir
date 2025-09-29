// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
)

// OptimizationPass identifies subplans of the provided query plan that can be executed remotely.
type OptimizationPass struct {
}

func NewOptimizationPass() *OptimizationPass {
	return &OptimizationPass{}
}

func (o *OptimizationPass) Name() string {
	return "Remote execution"
}

func (o *OptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan) (*planning.QueryPlan, error) {
	inspectResult := optimize.Inspect(plan.Root)
	if !inspectResult.HasSelectors || inspectResult.IsRewrittenByMiddleware {
		return plan, nil
	}

	var err error
	plan.Root, err = o.wrapInRemoteExecutionNode(plan.Root)
	if err != nil {
		return nil, err
	}

	return plan, nil
}

func (o *OptimizationPass) wrapInRemoteExecutionNode(child planning.Node) (planning.Node, error) {
	n := &RemoteExecution{}
	if err := n.SetChildren([]planning.Node{child}); err != nil {
		return nil, err
	}

	return n, nil
}
