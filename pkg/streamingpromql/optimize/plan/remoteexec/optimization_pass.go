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

func (o *OptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, maximumSupportedQueryPlanVersion planning.QueryPlanVersion) (*planning.QueryPlan, error) {
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
	plan.Root, err = o.wrapInRemoteExecutionNode(plan.Root, false)
	if err != nil {
		return nil, err
	}

	return plan, nil
}

func (o *OptimizationPass) wrapInRemoteExecutionNode(child planning.Node, eagerLoad bool) (planning.Node, error) {
	n := &RemoteExecution{RemoteExecutionDetails: &RemoteExecutionDetails{EagerLoad: eagerLoad}}
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

	for child := range planning.ChildrenIter(n) {
		wrapped, err := o.wrapShardedExpressions(child)
		if err != nil {
			return false, err
		}

		wrappedAnyChild = wrappedAnyChild || wrapped
	}

	return wrappedAnyChild, nil
}

func (o *OptimizationPass) wrapShardedConcat(functionCall *core.FunctionCall) error {
	if len(functionCall.Args) == 0 {
		// It shouldn't happen that there are no children, but the condition below will panic if there are no children,
		// so check it just to be safe.
		return nil
	}

	if _, isRemoteExec := functionCall.Args[0].(*RemoteExecution); isRemoteExec {
		// We've already wrapped the first child, which means we've wrapped all of the children as well.
		// This can happen if the sharded expression is duplicated, in which case we can visit it multiple times.
		return nil
	}

	for idx, child := range functionCall.Args {
		child, err := o.wrapInRemoteExecutionNode(child, true)
		if err != nil {
			return err
		}

		if err := functionCall.ReplaceChild(idx, child); err != nil {
			return err
		}
	}

	return nil
}
