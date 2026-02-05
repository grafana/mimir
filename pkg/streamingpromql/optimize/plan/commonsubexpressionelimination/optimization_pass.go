// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// This file implements common subexpression elimination.
//
// This allows us to skip evaluating the same expression multiple times.
// For example, in the expression "sum(a) / (sum(a) + sum(b))", we can skip evaluating the "sum(a)" expression a second time
// and instead reuse the result from the first evaluation.
//
// OptimizationPass is an optimization pass that identifies common subexpressions and injects Duplicate nodes
// into the query plan where needed.
//
// When the query plan is materialized, a InstantVectorDuplicationBuffer or RangeVectorDuplicationBuffer is created to
// buffer the results of the common subexpression, and a InstantVectorDuplicationConsumer or
// RangeVectorDuplicationConsumer is created for each consumer of the common subexpression.

type OptimizationPass struct {
	duplicationNodesIntroduced prometheus.Counter
	selectorsEliminated        prometheus.Counter
	selectorsInspected         prometheus.Counter

	logger log.Logger
}

func NewOptimizationPass(reg prometheus.Registerer, logger log.Logger) *OptimizationPass {
	return &OptimizationPass{
		duplicationNodesIntroduced: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_common_subexpression_elimination_duplication_nodes_introduced_total",
			Help: "Number of duplication nodes introduced by the common subexpression elimination optimization pass.",
		}),
		selectorsEliminated: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_common_subexpression_elimination_selectors_eliminated_total",
			Help: "Number of selectors eliminated by the common subexpression elimination optimization pass.",
		}),
		selectorsInspected: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_common_subexpression_elimination_selectors_inspected_total",
			Help: "Number of selectors inspected by the common subexpression elimination optimization pass, before elimination.",
		}),
		logger: logger,
	}
}

func (e *OptimizationPass) Name() string {
	return "Eliminate common subexpressions"
}

func (e *OptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, maximumSupportedQueryPlanVersion planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	// Find all the paths to leaves
	paths := e.accumulatePaths(plan)

	// For each path: find all the other paths that terminate in the same selector, then inject a duplication node
	selectorsEliminated, err := e.groupAndApplyDeduplication(paths, 0)
	if err != nil {
		return nil, err
	}

	e.selectorsInspected.Add(float64(len(paths)))
	e.selectorsEliminated.Add(float64(selectorsEliminated))

	spanLog := spanlogger.FromContext(ctx, e.logger)
	spanLog.DebugLog("msg", "attempted common subexpression elimination", "selectors_inspected", len(paths), "selectors_eliminated", selectorsEliminated)

	return plan, nil
}

// accumulatePaths returns a list of paths from root that terminate in VectorSelector or MatrixSelector nodes.
func (e *OptimizationPass) accumulatePaths(plan *planning.QueryPlan) []path {
	return e.accumulatePath(path{
		{
			node:       plan.Root,
			childIndex: 0,
			timeRange:  plan.Parameters.TimeRange,
		},
	})
}

func (e *OptimizationPass) accumulatePath(soFar path) []path {
	node, nodeTimeRange := soFar.NodeAtOffsetFromLeaf(0)

	_, isVS := node.(*core.VectorSelector)
	_, isMS := node.(*core.MatrixSelector)

	if isVS || isMS {
		return []path{soFar}
	}

	childCount := node.ChildCount()
	if childCount == 0 {
		// No children and not a vector selector or matrix selector, we're not interested in this path.
		return nil
	}

	childTimeRange := node.ChildrenTimeRange(nodeTimeRange)

	if childCount == 1 {
		// If there's only one child, we can reuse soFar.
		soFar = soFar.Append(node.Child(0), 0, childTimeRange)
		return e.accumulatePath(soFar)
	}

	paths := make([]path, 0, childCount)

	for childIdx := range childCount {
		if e.ShouldSkipChild(node, childIdx) {
			continue
		}
		path := soFar.Clone()
		path = path.Append(node.Child(childIdx), childIdx, childTimeRange)
		childPaths := e.accumulatePath(path)
		paths = append(paths, childPaths...)
	}

	return paths
}

// ShouldSkipChild determines if a child node should be skipped during common subexpression elimination.
// Currently this is only used to skip the 2nd argument to info function calls, as we don't want to
// deduplicate these as we need the matchers calculated by the info function at evaluation time to be
// applied, and these are ignored by the Duplicate operator.
func (e *OptimizationPass) ShouldSkipChild(node planning.Node, childIdx int) bool {
	functionCall, ok := node.(*core.FunctionCall)
	if !ok {
		return false
	}

	if functionCall.Function == functions.FUNCTION_INFO && childIdx == 1 {
		return true
	}

	return false
}

func (e *OptimizationPass) groupAndApplyDeduplication(paths []path, offset int) (int, error) {
	groups := e.groupPaths(paths, offset)
	totalPathsEliminated := 0

	for _, group := range groups {
		pathsEliminated, err := e.applyDeduplication(group, offset)
		if err != nil {
			return 0, err
		}

		totalPathsEliminated += pathsEliminated
	}

	return totalPathsEliminated, nil
}

// groupPaths returns paths grouped by the node at offset from the leaf.
// offset 0 means group by the leaf, offset 1 means group by the leaf node's parent etc.
// paths that have a unique grouping node are not returned.
func (e *OptimizationPass) groupPaths(paths []path, offset int) [][]path {
	alreadyGrouped := make([]bool, len(paths)) // ignoreunpooledslice
	groups := make([][]path, 0)

	// FIXME: find a better way to do this, this is currently O(n!) in the worst case (where n is the number of paths)
	// Maybe generate some kind of key for each node and then sort and group by that? (use same key that we'd use for caching?)
	for pathIdx, p := range paths {
		if alreadyGrouped[pathIdx] {
			continue
		}

		alreadyGrouped[pathIdx] = true
		leaf, leafTimeRange := p.NodeAtOffsetFromLeaf(offset)
		var group []path

		for otherPathOffset, otherPath := range paths[pathIdx+1:] {
			otherPathIdx := otherPathOffset + pathIdx + 1

			if alreadyGrouped[otherPathIdx] {
				continue
			}

			otherPathLeaf, otherPathTimeRange := otherPath.NodeAtOffsetFromLeaf(offset)
			if leaf == otherPathLeaf {
				// If we've found two paths with the same ancestor, then we don't consider these the same, as there's no duplication.
				// eg. if the expression is "a + a":
				// - if offset is 0 (group by leaf nodes): these are duplicates, group them together
				// - if offset is 1 (group by parent of leaf nodes): same node, no duplication, don't group together
				continue
			}

			if !equivalentNodes(leaf, otherPathLeaf) || !leafTimeRange.Equal(otherPathTimeRange) {
				continue
			}

			if group == nil {
				group = make([]path, 0, 2)
				group = append(group, p)
			}

			group = append(group, otherPath)
			alreadyGrouped[otherPathIdx] = true
		}

		if group != nil {
			groups = append(groups, group)
		}
	}

	return groups
}

// applyDeduplication replaces duplicate expressions at the tails of paths in group with a single expression.
// It searches for duplicate expressions from offset, and returns the number of duplicates eliminated.
func (e *OptimizationPass) applyDeduplication(group []path, offset int) (int, error) {
	duplicatePathLength := e.findCommonSubexpressionLength(group, offset+1)

	firstPath := group[0]
	duplicatedExpression, timeRange := firstPath.NodeAtOffsetFromLeaf(duplicatePathLength - 1)
	resultType, err := duplicatedExpression.ResultType()

	if err != nil {
		return 0, err
	}

	var skipLongerExpressions bool
	pathsEliminated := len(group) - 1

	// We only want to deduplicate instant vectors, or range vectors in an instant query.
	if resultType == parser.ValueTypeVector || (resultType == parser.ValueTypeMatrix && timeRange.IsInstant) {
		skipLongerExpressions, err = e.introduceDuplicateNode(group, duplicatePathLength)
	} else if _, isSubquery := duplicatedExpression.(*core.Subquery); isSubquery {
		// We've identified a subquery is duplicated (but not the function that encloses it), and the parent is not an instant
		// query.
		// We don't want to deduplicate the subquery itself, but we do want to deduplicate the inner expression of the
		// subquery.
		skipLongerExpressions, err = e.introduceDuplicateNode(group, duplicatePathLength-1)
	} else {
		// Duplicated range vector selector in a range query, but the function that encloses each instance isn't the same (or isn't the same on all paths).
		pathsEliminated = 0
	}

	if err != nil {
		return 0, nil
	}

	if skipLongerExpressions {
		return pathsEliminated, nil
	}

	if len(group) <= 2 {
		// Can't possibly have any more common subexpressions. We're done.
		return pathsEliminated, nil
	}

	// Check if a subset of the paths we just examined share an even longer common subexpression.
	// eg. if the expression is "a + max(a) + max(a)", then we may have just deduplicated the "a" selectors,
	// but we can also deduplicate the "max(a)" expressions.
	// This applies even if we just saw a common subexpression that returned something other than an instant vector
	// eg. in "rate(foo[5m]) + rate(foo[5m]) + increase(foo[5m])", we may have just identified the "foo[5m]" expression,
	// but we can also deduplicate the "rate(foo[5m])" expressions.
	if nextLevelPathsEliminated, err := e.groupAndApplyDeduplication(group, duplicatePathLength); err != nil {
		return 0, err
	} else if pathsEliminated == 0 {
		// If we didn't eliminate any paths at this level (because the duplicate expression was a range vector selector),
		// return the number returned by the next level.
		pathsEliminated = nextLevelPathsEliminated
	}

	return pathsEliminated, nil
}

// introduceDuplicateNode introduces a Duplicate node for each path in the group and returns false.
// If a Duplicate node already exists at the expected location, then introduceDuplicateNode does not introduce a new node and returns true.
func (e *OptimizationPass) introduceDuplicateNode(group []path, duplicatePathLength int) (skipLongerExpressions bool, err error) {
	// Check that we haven't already applied deduplication here because we found this subexpression earlier.
	// For example, if the original expression is "(a + b) + (a + b)", then we will have already found the
	// duplicate "a + b" subexpression when searching from the "a" selectors, so we don't need to do this again
	// when searching from the "b" selectors.
	firstPath := group[0]
	parentOfDuplicate, _ := firstPath.NodeAtOffsetFromLeaf(duplicatePathLength)
	expectedDuplicatedExpression := parentOfDuplicate.Child(firstPath.ChildIndexAtOffsetFromLeaf(duplicatePathLength - 1)) // Note that we can't take this from the path, as the path will not reflect any Duplicate nodes introduced previously.
	if isDuplicateNode(expectedDuplicatedExpression) {
		return true, nil
	}

	duplicatedExpressionOffset := duplicatePathLength - 1
	duplicatedExpression, _ := firstPath.NodeAtOffsetFromLeaf(duplicatedExpressionOffset)
	duplicate := &Duplicate{Inner: duplicatedExpression, DuplicateDetails: &DuplicateDetails{}}
	e.duplicationNodesIntroduced.Inc()

	for _, path := range group {
		parentOfDuplicate, _ := path.NodeAtOffsetFromLeaf(duplicatePathLength)
		err := parentOfDuplicate.ReplaceChild(path.ChildIndexAtOffsetFromLeaf(duplicatedExpressionOffset), duplicate)
		if err != nil {
			return false, err
		}

		eliminatedExpression, _ := path.NodeAtOffsetFromLeaf(duplicatedExpressionOffset)
		if err := mergeHints(duplicatedExpression, eliminatedExpression); err != nil {
			return false, err
		}
	}

	return false, nil
}

// findCommonSubexpressionLength returns the length of the common expression present at the end of each path
// in group, starting at offset.
// offset 0 means start from leaf of all paths.
// If a non-zero offset is provided, then it is assumed all paths in group already have a common subexpression of length offset.
func (e *OptimizationPass) findCommonSubexpressionLength(group []path, offset int) int {
	length := offset
	firstPath := group[0]

	for length < len(firstPath)-1 { // -1 to exclude root node (otherwise the longest common subexpression for "a + a" would be 2, not 1)
		firstNode, firstNodeTimeRange := firstPath.NodeAtOffsetFromLeaf(length)

		for _, path := range group[1:] {
			if length >= len(path) {
				// We've reached the end of this path, so the longest common subexpression is the length of this path.
				return length
			}

			otherNode, otherNodeTimeRange := path.NodeAtOffsetFromLeaf(length)
			if firstNode == otherNode {
				// If we've found two paths with the same ancestor, then we don't consider these the same, as there's no duplication.
				// eg. if the expression is "a + a":
				// - if offset is 0 (group by leaf nodes): these are duplicates, group them together
				// - if offset is 1 (group by parent of leaf nodes): same node, no duplication, don't group together
				return length
			}

			if !equivalentNodes(firstNode, otherNode) || !firstNodeTimeRange.Equal(otherNodeTimeRange) {
				// Nodes aren't the same, so the longest common subexpression is the length of the path not including the current node.
				return length
			}
		}

		length++
	}

	return length
}

func mergeHints(retainedNode planning.Node, eliminatedNode planning.Node) error {
	if isDuplicateNode(retainedNode) {
		// If we reach another Duplicate node, then we don't need to continue, as we would
		// have previously merged hints for the children of this node.
		return nil
	}

	if err := retainedNode.MergeHints(eliminatedNode); err != nil {
		return err
	}

	retainedNodeChildCount := retainedNode.ChildCount()
	eliminatedNodeChildCount := eliminatedNode.ChildCount()

	if retainedNodeChildCount != eliminatedNodeChildCount {
		return fmt.Errorf("retained and eliminated nodes have different number of children: %d vs %d", retainedNodeChildCount, eliminatedNodeChildCount)
	}

	for idx := range retainedNodeChildCount {
		if err := mergeHints(retainedNode.Child(idx), eliminatedNode.Child(idx)); err != nil {
			return err
		}
	}

	return nil
}

func isDuplicateNode(node planning.Node) bool {
	_, isDuplicate := node.(*Duplicate)
	return isDuplicate
}

type path []pathElement

type pathElement struct {
	node       planning.Node
	childIndex int // The position of node in its parent. 0 for root nodes.
	timeRange  types.QueryTimeRange
}

func (p path) Append(n planning.Node, childIndex int, timeRange types.QueryTimeRange) path {
	return append(p, pathElement{
		node:       n,
		childIndex: childIndex,
		timeRange:  timeRange,
	})
}

func (p path) NodeAtOffsetFromLeaf(offset int) (planning.Node, types.QueryTimeRange) {
	idx := len(p) - offset - 1
	e := p[idx]
	return e.node, e.timeRange
}

func (p path) ChildIndexAtOffsetFromLeaf(offset int) int {
	return p[len(p)-offset-1].childIndex
}

func (p path) Clone() path {
	return slices.Clone(p)
}

// String returns a string representation of the path, useful for debugging.
func (p path) String() string {
	b := &strings.Builder{}
	b.WriteRune('[')

	for i, e := range p {
		if i != 0 {
			b.WriteString(" -> ")
		}

		b.WriteString(planning.NodeTypeName(e.node))
		desc := e.node.Describe()

		if desc != "" {
			b.WriteString(": ")
			b.WriteString(desc)
		}
	}

	b.WriteRune(']')
	return b.String()
}

// equivalentNodes returns true if a and b are equivalent, including their corresponding children.
func equivalentNodes(a, b planning.Node) bool {
	if !a.EquivalentToIgnoringHintsAndChildren(b) {
		return false
	}

	aChildCount := a.ChildCount()
	bChildCount := b.ChildCount()

	if aChildCount != bChildCount {
		return false
	}

	for idx := range aChildCount {
		if !equivalentNodes(a.Child(idx), b.Child(idx)) {
			return false
		}
	}

	return true
}
