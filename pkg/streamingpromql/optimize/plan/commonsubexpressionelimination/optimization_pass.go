// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"context"
	"slices"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
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
// When the query plan is materialized, a DuplicationBuffer is created to buffer the results of the common subexpression,
// and a DuplicationConsumer is created for each consumer of the common subexpression.

type OptimizationPass struct {
	duplicationNodesIntroduced prometheus.Counter
}

func NewOptimizationPass(reg prometheus.Registerer) *OptimizationPass {
	return &OptimizationPass{
		duplicationNodesIntroduced: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_common_subexpression_elimination_duplication_nodes_introduced",
			Help: "Number of duplication nodes introduced by the common subexpression elimination optimization pass.",
		}),
	}
}

func (e *OptimizationPass) Name() string {
	return "Eliminate common subexpressions"
}

func (e *OptimizationPass) Apply(_ context.Context, plan *planning.QueryPlan) (*planning.QueryPlan, error) {
	// Find all the paths to leaves
	paths := e.accumulatePaths(plan)

	// For each path: find all the other paths that terminate in the same selector, then inject a duplication node
	err := e.groupAndApplyDeduplication(paths, 0)
	if err != nil {
		return nil, err
	}

	return plan, nil
}

// accumulatePaths returns a list of paths from root that terminate in VectorSelector or MatrixSelector nodes.
func (e *OptimizationPass) accumulatePaths(plan *planning.QueryPlan) []*path {
	return e.accumulatePath(&path{
		nodes:        []planning.Node{plan.Root},
		childIndices: []int{0},
		timeRanges:   []types.QueryTimeRange{plan.TimeRange},
	})
}

func (e *OptimizationPass) accumulatePath(soFar *path) []*path {
	nodeIdx := len(soFar.nodes) - 1
	node := soFar.nodes[nodeIdx]

	_, isVS := node.(*core.VectorSelector)
	_, isMS := node.(*core.MatrixSelector)

	if isVS || isMS {
		return []*path{soFar}
	}

	children := node.Children()
	if len(children) == 0 {
		// No children and not a vector selector or matrix selector, we're not interested in this path.
		return nil
	}

	childTimeRange := node.ChildrenTimeRange(soFar.timeRanges[nodeIdx])

	if len(children) == 1 {
		// If there's only one child, we can reuse soFar.
		soFar.Append(children[0], 0, childTimeRange)
		return e.accumulatePath(soFar)
	}

	paths := make([]*path, 0, len(children))

	for childIdx, child := range children {
		path := soFar.Clone()
		path.Append(child, childIdx, childTimeRange)
		childPaths := e.accumulatePath(path)
		paths = append(paths, childPaths...)
	}

	return paths
}

func (e *OptimizationPass) groupAndApplyDeduplication(paths []*path, offset int) error {
	groups := e.groupPaths(paths, offset)

	for _, group := range groups {
		if err := e.applyDeduplication(group, offset); err != nil {
			return err
		}
	}

	return nil
}

// groupPaths returns paths grouped by the node at offset from the leaf.
// offset 0 means group by the leaf, offset 1 means group by the leaf node's parent etc.
// paths that have a unique grouping node are not returned.
func (e *OptimizationPass) groupPaths(paths []*path, offset int) [][]*path {
	alreadyGrouped := make([]bool, len(paths)) // ignoreunpooledslice
	groups := make([][]*path, 0)

	// FIXME: find a better way to do this, this is currently O(n!) in the worst case (where n is the number of paths)
	// Maybe generate some kind of key for each node and then sort and group by that? (use same key that we'd use for caching?)
	for pathIdx, p := range paths {
		if alreadyGrouped[pathIdx] {
			continue
		}

		alreadyGrouped[pathIdx] = true
		leaf, leafTimeRange := p.NodeAtOffsetFromLeaf(offset)
		var group []*path

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

			if !leaf.EquivalentTo(otherPathLeaf) || !leafTimeRange.Equal(otherPathTimeRange) {
				continue
			}

			if group == nil {
				group = make([]*path, 0, 2)
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

func (e *OptimizationPass) applyDeduplication(group []*path, offset int) error {
	duplicatePathLength := e.findCommonSubexpressionLength(group, offset+1)

	firstPath := group[0]
	duplicatedExpression, _ := firstPath.NodeAtOffsetFromLeaf(duplicatePathLength - 1)
	resultType, err := duplicatedExpression.ResultType()

	if err != nil {
		return err
	}

	// We only want to deduplicate instant vectors.
	if resultType == parser.ValueTypeVector {
		if skipLongerExpressions, err := e.introduceDuplicateNode(group, duplicatePathLength); err != nil {
			return err
		} else if skipLongerExpressions {
			return nil
		}
	} else if _, isSubquery := duplicatedExpression.(*core.Subquery); isSubquery {
		// If we've identified a subquery is duplicated (but not the function that encloses it), we don't want to deduplicate
		// the subquery itself, but we do want to deduplicate the inner expression of the subquery.
		if skipLongerExpressions, err := e.introduceDuplicateNode(group, duplicatePathLength-1); err != nil {
			return err
		} else if skipLongerExpressions {
			return nil
		}
	}

	if len(group) <= 2 {
		// Can't possibly have any more common subexpressions. We're done.
		return nil
	}

	// Check if a subset of the paths we just examined share an even longer common subexpression.
	// eg. if the expression is "a + max(a) + max(a)", then we may have just deduplicated the "a" selectors,
	// but we can also deduplicate the "max(a)" expressions.
	// This applies even if we just saw a common subexpression that returned something other than an instant vector
	// eg. in "rate(foo[5m]) + rate(foo[5m]) + increase(foo[5m])", we may have just identified the "foo[5m]" expression,
	// but we can also deduplicate the "rate(foo[5m])" expressions.
	if err := e.groupAndApplyDeduplication(group, duplicatePathLength); err != nil {
		return err
	}

	return nil
}

// introduceDuplicateNode introduces a Duplicate node for each path in the group and returns false.
// If a Duplicate node already exists at the expected location, then introduceDuplicateNode does not introduce a new node and returns true.
func (e *OptimizationPass) introduceDuplicateNode(group []*path, duplicatePathLength int) (skipLongerExpressions bool, err error) {
	// Check that we haven't already applied deduplication here because we found this subexpression earlier.
	// For example, if the original expression is "(a + b) + (a + b)", then we will have already found the
	// duplicate "a + b" subexpression when searching from the "a" selectors, so we don't need to do this again
	// when searching from the "b" selectors.
	firstPath := group[0]
	parentOfDuplicate, _ := firstPath.NodeAtOffsetFromLeaf(duplicatePathLength)
	expectedDuplicatedExpression := parentOfDuplicate.Children()[firstPath.ChildIndexAtOffsetFromLeaf(duplicatePathLength-1)] // Note that we can't take this from the path, as the path will not reflect any Duplicate nodes introduced previously.
	if _, isDuplicate := expectedDuplicatedExpression.(*Duplicate); isDuplicate {
		return true, nil
	}

	duplicatedExpression, _ := firstPath.NodeAtOffsetFromLeaf(duplicatePathLength - 1)
	duplicate := &Duplicate{Inner: duplicatedExpression, DuplicateDetails: &DuplicateDetails{}}
	e.duplicationNodesIntroduced.Inc()

	for _, path := range group {
		parentOfDuplicate, _ := path.NodeAtOffsetFromLeaf(duplicatePathLength)
		err := replaceChild(parentOfDuplicate, path.ChildIndexAtOffsetFromLeaf(duplicatePathLength-1), duplicate)
		if err != nil {
			return false, err
		}
	}

	return false, nil
}

// findCommonSubexpressionLength returns the length of the common expression present at the end of each path
// in group, starting at offset.
// offset 0 means start from leaf of all paths.
// If a non-zero offset is provided, then it is assumed all paths in group already have a common subexpression of length offset.
func (e *OptimizationPass) findCommonSubexpressionLength(group []*path, offset int) int {
	length := offset
	firstPath := group[0]

	for length < len(firstPath.nodes)-1 { // -1 to exclude root node (otherwise the longest common subexpression for "a + a" would be 2, not 1)
		firstNode, firstNodeTimeRange := firstPath.NodeAtOffsetFromLeaf(length)

		for _, path := range group[1:] {
			if length >= len(path.nodes) {
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

			if !firstNode.EquivalentTo(otherNode) || !firstNodeTimeRange.Equal(otherNodeTimeRange) {
				// Nodes aren't the same, so the longest common subexpression is the length of the path not including the current node.
				return length
			}
		}

		length++
	}

	return length
}

func replaceChild(parent planning.Node, childIndex int, newChild planning.Node) error {
	children := parent.Children()
	children[childIndex] = newChild
	return parent.SetChildren(children)
}

type path struct {
	nodes        []planning.Node
	childIndices []int                  // childIndices[x] contains the position of node x in its parent
	timeRanges   []types.QueryTimeRange // timeRanges[x] contains the time range that node x will be evaluated over
}

func (p *path) Append(n planning.Node, childIndex int, timeRange types.QueryTimeRange) {
	p.nodes = append(p.nodes, n)
	p.childIndices = append(p.childIndices, childIndex)
	p.timeRanges = append(p.timeRanges, timeRange)
}

func (p *path) NodeAtOffsetFromLeaf(offset int) (planning.Node, types.QueryTimeRange) {
	idx := len(p.nodes) - offset - 1
	return p.nodes[idx], p.timeRanges[idx]
}

func (p *path) ChildIndexAtOffsetFromLeaf(offset int) int {
	return p.childIndices[len(p.nodes)-offset-1]
}

func (p *path) Clone() *path {
	return &path{
		nodes:        slices.Clone(p.nodes),
		childIndices: slices.Clone(p.childIndices),
		timeRanges:   slices.Clone(p.timeRanges),
	}
}

// String returns a string representation of the path, useful for debugging.
func (p *path) String() string {
	b := &strings.Builder{}
	b.WriteRune('[')

	for i, n := range p.nodes {
		if i != 0 {
			b.WriteString(" -> ")
		}

		b.WriteString(planning.NodeTypeName(n))
		desc := n.Describe()

		if desc != "" {
			b.WriteString(": ")
			b.WriteString(desc)
		}
	}

	b.WriteRune(']')
	return b.String()
}
