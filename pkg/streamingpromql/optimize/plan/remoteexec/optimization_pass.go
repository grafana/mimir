// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/splitandcache"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
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
	inspectResult := optimize.InspectSelectors(plan.Root)
	if !inspectResult.HasSelectors || inspectResult.IsRewrittenByMiddleware {
		return plan, nil
	}

	multiNodeGroupsEnabled := maximumSupportedQueryPlanVersion >= planning.QueryPlanV3

	var groups remoteExecutionGroupSet

	if multiNodeGroupsEnabled {
		groups = remoteExecutionGroupSet{}
	}

	// When a query has been rewritten to spin off subqueries, each evaluation root is a separate
	// query and must have remote execution applied to it independently: if its subtree is sharded, each
	// sharded leg is executed remotely; otherwise the entire subtree is. The rest of the plan (the outer
	// instant query) runs on the query-frontend.
	if evaluationRoots := collectEvaluationRoots(plan.Root, plan.Parameters.TimeRange); len(evaluationRoots) > 0 {
		for _, evaluationRoot := range evaluationRoots {
			if err := o.wrapEvaluationRoot(evaluationRoot.root, groups, len(evaluationRoots) > 1, maximumSupportedQueryPlanVersion, evaluationRoot.timeRange); err != nil {
				return nil, err
			}
		}

		return plan, nil
	}

	if err := o.wrapPlanRoot(plan, groups, maximumSupportedQueryPlanVersion); err != nil {
		return nil, err
	}

	return plan, nil
}

func (o *OptimizationPass) wrapPlanRoot(plan *planning.QueryPlan, groups remoteExecutionGroupSet, maximumSupportedQueryPlanVersion planning.QueryPlanVersion) error {
	newRoot, err := o.applyToRootNodeForVersion(plan.Root, groups, false, maximumSupportedQueryPlanVersion, plan.Parameters.TimeRange)
	if err != nil {
		return err
	}

	plan.Root = newRoot
	return nil
}

// wrapEvaluationRoot applies remote execution to a single EvaluationRoot's subtree, mirroring the
// behaviour applied to the whole plan when there are no EvaluationRoot markers.
func (o *OptimizationPass) wrapEvaluationRoot(evaluationRoot *core.EvaluationRoot, groups remoteExecutionGroupSet, haveMultipleRoots bool, maximumSupportedQueryPlanVersion planning.QueryPlanVersion, timeRange types.QueryTimeRange) error {
	newRoot, err := o.applyToRootNodeForVersion(evaluationRoot.Inner, groups, haveMultipleRoots, maximumSupportedQueryPlanVersion, timeRange)
	if err != nil {
		return err
	}

	evaluationRoot.Inner = newRoot
	return nil
}

func (o *OptimizationPass) applyToRootNodeForVersion(root planning.Node, groups remoteExecutionGroupSet, eagerLoad bool, maximumSupportedQueryPlanVersion planning.QueryPlanVersion, timeRange types.QueryTimeRange) (planning.Node, error) {
	requiredVersion, err := planning.MinimumRequiredPlanVersion(root, timeRange)
	if err != nil {
		return nil, fmt.Errorf("determine the minimum plan version for remote execution: %w", err)
	}

	if requiredVersion <= maximumSupportedQueryPlanVersion {
		return o.applyToRootNode(root, groups, eagerLoad)
	}

	if err := o.wrapCompatibleDescendants(root, groups, eagerLoad, maximumSupportedQueryPlanVersion, timeRange); err != nil {
		return nil, err
	}

	return root, nil
}

func (o *OptimizationPass) wrapCompatibleDescendants(node planning.Node, groups remoteExecutionGroupSet, eagerLoad bool, maximumSupportedQueryPlanVersion planning.QueryPlanVersion, timeRange types.QueryTimeRange) error {
	childTimeRange := node.ChildrenTimeRange(timeRange)
	childEagerLoad := eagerLoad || isSplittingNode(node) || isShardingConcatNode(node)

	for idx := 0; idx < node.ChildCount(); idx++ {
		child := node.Child(idx)
		if _, alreadyRemote := child.(*RemoteExecutionConsumer); alreadyRemote {
			continue
		}
		if !optimize.InspectSelectors(child).HasSelectors {
			continue
		}

		wrappedChild, err := o.applyToRootNodeForVersion(child, groups, childEagerLoad, maximumSupportedQueryPlanVersion, childTimeRange)
		if err != nil {
			return fmt.Errorf("place a compatible remote execution subtree: %w", err)
		}
		if err := node.ReplaceChild(idx, wrappedChild); err != nil {
			return fmt.Errorf("replace a compatible remote execution subtree: %w", err)
		}
	}

	return nil
}

// applyToRootNode applies remote execution nodes to the provided root node.
//
// If the expression beneath the provided node is sharded, each sharded leg is wrapped in a RemoteExecutionGroup node.
// Otherwise the entire child is wrapped (beneath any splitting and caching nodes, which run on the query-frontend).
//
// The new root node is returned, which may or may not be the same as the provided root node.
func (o *OptimizationPass) applyToRootNode(root planning.Node, groups remoteExecutionGroupSet, eagerLoad bool) (planning.Node, error) {
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
		// If a splitting node is present, then we need to eagerly load this instance for the same reason as for sharded expressions.
		eagerLoad = eagerLoad || isSplittingNode(child)

		parent = child
		child = child.Child(0)
	}

	wrappedChild, err := o.wrapInRemoteExecutionNode(
		child,
		eagerLoad,
		groups,
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
type evaluationRootWithTimeRange struct {
	root      *core.EvaluationRoot
	timeRange types.QueryTimeRange
}

func collectEvaluationRoots(node planning.Node, timeRange types.QueryTimeRange) []evaluationRootWithTimeRange {
	seen := map[*core.EvaluationRoot]struct{}{}
	var roots []evaluationRootWithTimeRange

	var collect func(planning.Node, types.QueryTimeRange)
	collect = func(node planning.Node, timeRange types.QueryTimeRange) {
		if evaluationRoot, ok := node.(*core.EvaluationRoot); ok {
			if _, exists := seen[evaluationRoot]; !exists {
				seen[evaluationRoot] = struct{}{}
				roots = append(roots, evaluationRootWithTimeRange{
					root:      evaluationRoot,
					timeRange: evaluationRoot.ChildrenTimeRange(timeRange),
				})
			}
			return
		}

		childTimeRange := node.ChildrenTimeRange(timeRange)
		for child := range planning.ChildrenIter(node) {
			collect(child, childTimeRange)
		}
	}

	collect(node, timeRange)
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
		assert.Unreachable("could not find selector for node", map[string]any{
			"eager_load": eagerLoad,
			"node_type":  fmt.Sprintf("%T", node),
		})
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
	_, isCache := n.(*splitandcache.Cache)
	return isSplittingNode(n) || isCache
}

func isSplittingNode(n planning.Node) bool {
	_, isTimeRangeSplit := n.(*splitandcache.TimeRangeSplit)
	return isTimeRangeSplit
}

func isShardingConcatNode(n planning.Node) bool {
	functionCall, ok := n.(*core.FunctionCall)
	return ok && functionCall.Function == functions.FUNCTION_SHARDING_CONCAT
}
