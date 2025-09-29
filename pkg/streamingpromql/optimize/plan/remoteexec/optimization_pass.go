// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
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

	if wrappedAnyChild, err := o.wrapShardedExpressions(plan.Root); err != nil {
		return nil, err
	} else if wrappedAnyChild {
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

// wrapShardedExpressions wraps sharded legs in a RemoteExecution node.
// It returns true if any node was wrapped, or false otherwise.
func (o *OptimizationPass) wrapShardedExpressions(n planning.Node) (bool, error) {
	functionCall, isFunctionCall := n.(*core.FunctionCall)
	if isFunctionCall && functionCall.Function == functions.FUNCTION_SHARDING_CONCAT {
		if err := o.wrapShardedConcat(functionCall); err != nil {
			return false, err
		}

		// We don't expect any nested sharded expressions, so once we've found one, we can return early.
		return true, nil
	}

	wrappedAnyChild := false

	for _, child := range n.Children() {
		wrapped, err := o.wrapShardedExpressions(child)
		if err != nil {
			return false, err
		}

		wrappedAnyChild = wrappedAnyChild || wrapped
	}

	return wrappedAnyChild, nil
}

func (o *OptimizationPass) wrapShardedConcat(functionCall *core.FunctionCall) error {
	children := functionCall.Children()

	for idx, child := range children {
		var err error
		children[idx], err = o.wrapInRemoteExecutionNode(child)
		if err != nil {
			return err
		}
	}

	return functionCall.SetChildren(children)
}
