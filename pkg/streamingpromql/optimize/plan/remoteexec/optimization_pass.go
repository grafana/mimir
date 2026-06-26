// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"
	"fmt"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/splitandcache"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
)

// OptimizationPass identifies subplans of the provided query plan that can be executed remotely.
type OptimizationPass struct {
	enableMultipleNodeRequests bool
}

func NewOptimizationPass(enableMultipleNodeRequests bool) *OptimizationPass {
	return &OptimizationPass{
		enableMultipleNodeRequests: enableMultipleNodeRequests,
	}
}

func (o *OptimizationPass) Name() string {
	return "Remote execution"
}

func (o *OptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, maximumSupportedQueryPlanVersion planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	inspectResult := optimize.InspectSelectors(plan.Root)
	if !inspectResult.HasSelectors || inspectResult.IsRewrittenByMiddleware {
		return plan, nil
	}

	multiNodeGroupsEnabled := o.enableMultipleNodeRequests && maximumSupportedQueryPlanVersion >= planning.QueryPlanV3

	// When a query has been rewritten to spin off subqueries, each evaluation root is a separate
	// query and must have remote execution applied to it independently: if its subtree is sharded, each
	// sharded leg is executed remotely; otherwise the entire subtree is. The rest of the plan (the outer
	// instant query) runs on the query-frontend.
	if evaluationRoots := collectEvaluationRoots(plan.Root); len(evaluationRoots) > 0 {
		for _, evaluationRoot := range evaluationRoots {
			if err := o.wrapEvaluationRoot(evaluationRoot, multiNodeGroupsEnabled); err != nil {
				return nil, err
			}
		}

		return plan, nil
	}

	if err := o.wrapPlanRoot(plan, multiNodeGroupsEnabled); err != nil {
		return nil, err
	}

	return plan, nil
}

func (o *OptimizationPass) wrapPlanRoot(plan *planning.QueryPlan, multiNodeGroupsEnabled bool) error {
	newRoot, err := o.applyToRootNode(plan.Root, multiNodeGroupsEnabled)
	if err != nil {
		return err
	}

	plan.Root = newRoot
	return nil
}

// wrapEvaluationRoot applies remote execution to a single EvaluationRoot's subtree, mirroring the
// behaviour applied to the whole plan when there are no EvaluationRoot markers.
//
// Each EvaluationRoot is a separate query, so a fresh remote execution group set is used per
// EvaluationRoot when grouping nodes that share a selector into the same request.
// FIXME: in the future we could share groups between roots and avoid evaluating duplicate expressions shared across
// roots.
func (o *OptimizationPass) wrapEvaluationRoot(evaluationRoot *core.EvaluationRoot, multiNodeGroupsEnabled bool) error {
	newRoot, err := o.applyToRootNode(evaluationRoot.Inner, multiNodeGroupsEnabled)
	if err != nil {
		return err
	}

	evaluationRoot.Inner = newRoot
	return nil
}

// applyToRootNode applies remote execution nodes to the provided root node.
//
// If the expression beneath the provided node is sharded, each sharded leg is wrapped in a RemoteExecutionGroup node.
// Otherwise the entire child is wrapped (beneath any splitting and caching nodes, which run on the query-frontend).
//
// The new root node is returned, which may or may not be the same as the provided root node.
func (o *OptimizationPass) applyToRootNode(root planning.Node, multiNodeGroupsEnabled bool) (planning.Node, error) {
	var groups remoteExecutionGroupSet

	if multiNodeGroupsEnabled {
		groups = remoteExecutionGroupSet{}
	}

	if wrappedAnyChild, err := o.wrapShardedExpressions(root, groups); err != nil {
		return nil, err
	} else if wrappedAnyChild {
		return root, nil
	}

	child := root
	var parent planning.Node

	// We want the splitting and caching nodes to run on the query-frontend, so the remote execution
	// node should be a child of any splitting and caching nodes.
	for isSplittingOrCachingNode(child) {
		parent = child
		child = child.Child(0)
	}

	wrappedChild, err := o.wrapInRemoteExecutionNode(
		child,
		false,
		nil, // No need to pass groups here as we'll wrap the whole child in a single group.
	)
	if err != nil {
		return nil, err
	}

	if parent == nil {
		// We want to replace the root with the wrapped child.
		return wrappedChild, nil
	}

	if err := parent.ReplaceChild(0, wrappedChild); err != nil {
		return nil, err
	}

	return root, nil
}

// collectEvaluationRoots returns the EvaluationRoot nodes in the plan.
func collectEvaluationRoots(node planning.Node) []*core.EvaluationRoot {
	var roots []*core.EvaluationRoot
	var visit func(planning.Node)
	visit = func(n planning.Node) {
		if evaluationRoot, ok := n.(*core.EvaluationRoot); ok {
			roots = append(roots, evaluationRoot)

			// EvaluationRoot markers are never nested inside one another, so no need to visit children.
			return
		}

		for child := range planning.ChildrenIter(n) {
			visit(child)
		}
	}

	visit(node)

	return roots
}

func (o *OptimizationPass) wrapInRemoteExecutionNode(child planning.Node, eagerLoad bool, groups remoteExecutionGroupSet) (planning.Node, error) {
	group, err := groups.GetGroupForNode(child, eagerLoad)
	if err != nil {
		return nil, err
	}

	group.Nodes = append(group.Nodes, child)

	consumer := &RemoteExecutionConsumer{
		RemoteExecutionConsumerDetails: &RemoteExecutionConsumerDetails{NodeIndex: uint64(len(group.Nodes) - 1)},
		Group:                          group,
	}

	return consumer, nil
}

// wrapShardedExpressions wraps sharded legs in a RemoteExecutionGroup node.
// It returns true if any node was wrapped, or false otherwise.
func (o *OptimizationPass) wrapShardedExpressions(n planning.Node, groups remoteExecutionGroupSet) (bool, error) {
	functionCall, isFunctionCall := n.(*core.FunctionCall)
	if isFunctionCall && functionCall.Function == functions.FUNCTION_SHARDING_CONCAT {
		if err := o.wrapShardedConcat(functionCall, groups); err != nil {
			return false, err
		}

		// We don't expect any nested sharded expressions, so once we've found one, we can return early.
		return true, nil
	}

	wrappedAnyChild := false

	for child := range planning.ChildrenIter(n) {
		wrapped, err := o.wrapShardedExpressions(child, groups)
		if err != nil {
			return false, err
		}

		wrappedAnyChild = wrappedAnyChild || wrapped
	}

	return wrappedAnyChild, nil
}

func (o *OptimizationPass) wrapShardedConcat(functionCall *core.FunctionCall, groups remoteExecutionGroupSet) error {
	if len(functionCall.Args) == 0 {
		// It shouldn't happen that there are no children, but the condition below will panic if there are no children,
		// so check it just to be safe.
		return nil
	}

	if _, isRemoteExec := functionCall.Args[0].(*RemoteExecutionConsumer); isRemoteExec {
		// We've already wrapped the first child, which means we've wrapped all of the children as well.
		// This can happen if the sharded expression is duplicated, in which case we can visit it multiple times.
		return nil
	}

	for idx, child := range functionCall.Args {
		child, err := o.wrapInRemoteExecutionNode(child, true, groups)
		if err != nil {
			return err
		}

		if err := functionCall.ReplaceChild(idx, child); err != nil {
			return err
		}
	}

	return nil
}

type remoteExecutionGroupSet map[planning.Node]*RemoteExecutionGroup

// GetGroupForNode finds or creates a RemoteExecutionGroup for node.
//
// It groups nodes that share a common selector into the same group.
//
// This method assumes that common subexpression elimination has already been applied,
// and therefore nodes that share a selector refer to the same planning.Node instance
// through a Duplicate node.
func (s remoteExecutionGroupSet) GetGroupForNode(node planning.Node, eagerLoad bool) (*RemoteExecutionGroup, error) {
	if s == nil {
		// Multi-node remote execution is disabled or not supported, or the expression isn't sharded, so just return a new group.
		return s.createGroup(eagerLoad), nil
	}

	selector, err := s.findSelector(node)
	if err != nil {
		return nil, err
	}

	if selector == nil {
		return nil, fmt.Errorf("could not find selector for node of type %T (this is a bug)", node)
	}

	group, haveGroup := s[selector]
	if haveGroup {
		return group, nil
	}

	group = s.createGroup(eagerLoad)
	s[selector] = group

	return group, nil
}

func (s remoteExecutionGroupSet) findSelector(node planning.Node) (planning.Node, error) {
	switch node.(type) {
	case *core.VectorSelector, *core.MatrixSelector:
		return node, nil
	default:
		for child := range planning.ChildrenIter(node) {
			if selector, err := s.findSelector(child); err != nil {
				return nil, err
			} else if selector != nil {
				// Sharded expressions will only ever have one selector, so once we've found the first one, we can stop.
				return selector, nil
			}
		}

		// Couldn't find a selector in this branch.
		return nil, nil
	}
}

func (s remoteExecutionGroupSet) createGroup(eagerLoad bool) *RemoteExecutionGroup {
	return &RemoteExecutionGroup{
		RemoteExecutionGroupDetails: &RemoteExecutionGroupDetails{EagerLoad: eagerLoad},
	}
}

func isSplittingOrCachingNode(n planning.Node) bool {
	_, isTimeRangeSplit := n.(*splitandcache.TimeRangeSplit)
	_, isCache := n.(*splitandcache.Cache)
	return isTimeRangeSplit || isCache
}
