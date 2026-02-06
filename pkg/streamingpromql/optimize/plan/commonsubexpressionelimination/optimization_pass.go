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
	duplicationNodesIntroduced   prometheus.Counter
	duplicateSelectorsEliminated prometheus.Counter
	subsetSelectorsEliminated    prometheus.Counter
	selectorsInspected           prometheus.Counter

	subsetSelectorEliminationEnabled bool
	logger                           log.Logger
}

func NewOptimizationPass(subsetSelectorEliminationEnabled bool, reg prometheus.Registerer, logger log.Logger) *OptimizationPass {
	selectorsEliminated := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_mimir_query_engine_common_subexpression_elimination_selectors_eliminated_total",
		Help: "Number of selectors eliminated by the common subexpression elimination optimization pass.",
	}, []string{"reason"})

	return &OptimizationPass{
		duplicationNodesIntroduced: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_common_subexpression_elimination_duplication_nodes_introduced_total",
			Help: "Number of duplication nodes introduced by the common subexpression elimination optimization pass.",
		}),
		duplicateSelectorsEliminated: selectorsEliminated.WithLabelValues("duplicate"),
		subsetSelectorsEliminated:    selectorsEliminated.WithLabelValues("subset"),
		selectorsInspected: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_common_subexpression_elimination_selectors_inspected_total",
			Help: "Number of selectors inspected by the common subexpression elimination optimization pass, before elimination.",
		}),

		subsetSelectorEliminationEnabled: subsetSelectorEliminationEnabled,
		logger:                           logger,
	}
}

func (e *OptimizationPass) Name() string {
	return "Eliminate common subexpressions"
}

func (e *OptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, maximumSupportedQueryPlanVersion planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	// Find all the paths to leaves
	paths := e.accumulatePaths(plan)

	// For each path: find all the other paths that terminate in duplicate or subset selectors, then inject a duplication node
	groups, err := e.groupPathsForFirstIteration(paths, maximumSupportedQueryPlanVersion >= planning.QueryPlanV6)
	if err != nil {
		return nil, err
	}

	stats, err := e.applyDeduplicationToGroups(groups, 0)
	if err != nil {
		return nil, err
	}

	e.selectorsInspected.Add(float64(len(paths)))
	e.duplicateSelectorsEliminated.Add(float64(stats.duplicateSelectorsEliminated))
	e.subsetSelectorsEliminated.Add(float64(stats.subsetSelectorsEliminated))

	spanLog := spanlogger.FromContext(ctx, e.logger)
	spanLog.DebugLog(
		"msg", "attempted common subexpression elimination",
		"selectors_inspected", len(paths),
		"duplicate_selectors_eliminated", stats.duplicateSelectorsEliminated,
		"subset_selectors_eliminated", stats.subsetSelectorsEliminated,
	)

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

func (e *OptimizationPass) applyDeduplicationToGroups(groups []sharedSelectorGroup, offset int) (deduplicationStats, error) {
	totalStats := deduplicationStats{}

	for _, group := range groups {
		groupStats, err := e.applyDeduplication(group, offset)
		if err != nil {
			return deduplicationStats{}, err
		}

		totalStats.add(groupStats)
	}

	return totalStats, nil
}

type deduplicationStats struct {
	duplicateSelectorsEliminated int
	subsetSelectorsEliminated    int
}

func (s *deduplicationStats) add(other deduplicationStats) {
	s.duplicateSelectorsEliminated += other.duplicateSelectorsEliminated
	s.subsetSelectorsEliminated += other.subsetSelectorsEliminated
}

type sharedSelectorGroup struct {
	paths   []path
	filters [][]*core.LabelMatcher // Will be nil if all selectors are exact duplicates, and not nil if any selector is a subset of another.
}

func (g *sharedSelectorGroup) getFilterForPath(pathIdx int) []*core.LabelMatcher {
	if g.filters == nil {
		return nil
	}

	return g.filters[pathIdx]
}

func (g *sharedSelectorGroup) computeStats() deduplicationStats {
	stats := deduplicationStats{}

	for idx := range g.paths {
		if filters := g.getFilterForPath(idx); len(filters) > 0 {
			stats.subsetSelectorsEliminated++
		} else {
			stats.duplicateSelectorsEliminated++
		}
	}

	stats.duplicateSelectorsEliminated-- // Don't count the original selector.

	return stats
}

// groupPathsForFirstIteration returns paths grouped by each path's leaf nodes.
// Paths with leaf nodes that don't match any other leaf nodes are not returned.
// It considers subset selectors, if SSE is enabled and supported.
func (e *OptimizationPass) groupPathsForFirstIteration(paths []path, subsetSelectorEliminationSupported bool) ([]sharedSelectorGroup, error) {
	// If SSE is disabled or not supported, we can just use groupPathsForSubsequentIteration.
	if !e.subsetSelectorEliminationEnabled || !subsetSelectorEliminationSupported {
		return e.groupPathsForSubsequentIteration(paths, 0), nil
	}

	alreadyGrouped := make([]bool, len(paths)) // ignoreunpooledslice
	var groups []sharedSelectorGroup

	for pathIdx, p := range paths {
		if alreadyGrouped[pathIdx] {
			continue
		}

		alreadyGrouped[pathIdx] = true
		selector, selectorTimeRange, err := p.Selector()
		if err != nil {
			return nil, err
		}

		group := sharedSelectorGroup{}

		for otherPathOffset, otherPath := range paths[pathIdx+1:] {
			otherPathIdx := otherPathOffset + pathIdx + 1

			if alreadyGrouped[otherPathIdx] {
				continue
			}

			otherSelector, otherTimeRange, err := otherPath.Selector()
			if err != nil {
				return nil, err
			}

			if !selectorTimeRange.Equal(otherTimeRange) || !selector.EquivalentToIgnoringMatchersAndHints(otherSelector) {
				continue
			}

			relationship, additionalMatchers := SelectorsAreDuplicateOrSubset(selector.GetMatchers(), otherSelector.GetMatchers())
			if relationship == NotDuplicateOrSubset {
				continue
			}

			if len(group.paths) == 0 {
				// First duplicate or subset selector we've seen, create the list of paths.
				group.paths = make([]path, 0, 2)
				group.paths = append(group.paths, p)
			}

			if group.filters == nil && relationship == SubsetSelectors {
				// First subset selector we've seen, create a slice of filters for all the other nodes.
				group.filters = make([][]*core.LabelMatcher, len(group.paths))
			}

			group.paths = append(group.paths, otherPath)

			if group.filters != nil {
				// If any part of this group has a subset selector, we need to keep track of the additional matchers that apply to this path.
				group.filters = append(group.filters, additionalMatchers)
			}

			alreadyGrouped[otherPathIdx] = true
		}

		if len(group.paths) > 0 {
			groups = append(groups, group)
		}
	}

	return groups, nil
}

// groupPathsForSubsequentIteration returns paths grouped by the node at offset from the leaf.
// offset 0 means group by the leaf, offset 1 means group by the leaf node's parent etc.
// Paths that don't match any other paths are not returned.
// It does not consider subset selectors, as they are not relevant for subsequent iterations.
func (e *OptimizationPass) groupPathsForSubsequentIteration(paths []path, offset int) []sharedSelectorGroup {
	alreadyGrouped := make([]bool, len(paths)) // ignoreunpooledslice
	var groups []sharedSelectorGroup

	// FIXME: is there a better way to do this? This is currently O(n!) in the worst case (where n is the number of paths)
	for pathIdx, p := range paths {
		if alreadyGrouped[pathIdx] {
			continue
		}

		alreadyGrouped[pathIdx] = true
		leaf, leafTimeRange := p.NodeAtOffsetFromLeaf(offset)
		group := sharedSelectorGroup{}

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

			if group.paths == nil {
				group.paths = make([]path, 0, 2)
				group.paths = append(group.paths, p)
			}

			group.paths = append(group.paths, otherPath)
			alreadyGrouped[otherPathIdx] = true
		}

		if group.paths != nil {
			groups = append(groups, group)
		}
	}

	return groups
}

// applyDeduplication replaces duplicate expressions at the tails of paths in group with a single expression.
// It searches for duplicate expressions from offset, and returns the number of duplicates eliminated.
func (e *OptimizationPass) applyDeduplication(group sharedSelectorGroup, offset int) (deduplicationStats, error) {
	duplicatePathLength := e.findCommonSubexpressionLength(group.paths, offset+1)

	firstPath := group.paths[0]
	duplicatedExpression, timeRange := firstPath.NodeAtOffsetFromLeaf(duplicatePathLength - 1)
	resultType, err := duplicatedExpression.ResultType()

	if err != nil {
		return deduplicationStats{}, err
	}

	skipLongerExpressions := false
	skippedBecauseRangeVectorSelectorInRangeQuery := false
	stats := group.computeStats()

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
		skippedBecauseRangeVectorSelectorInRangeQuery = true
		stats = deduplicationStats{}
	}

	if err != nil {
		return deduplicationStats{}, nil
	}

	if skipLongerExpressions {
		return stats, nil
	}

	if len(group.paths) <= 2 {
		// Can't possibly have any more common subexpressions. We're done.
		return stats, nil
	}

	// Check if a subset of the paths we just examined share an even longer common subexpression.
	// eg. if the expression is "a + max(a) + max(a)", then we may have just deduplicated the "a" selectors,
	// but we can also deduplicate the "max(a)" expressions.
	// This applies even if we just saw a common subexpression that returned something other than an instant vector
	// eg. in "rate(foo[5m]) + rate(foo[5m]) + increase(foo[5m])", we may have just identified the "foo[5m]" expression,
	// but we can also deduplicate the "rate(foo[5m])" expressions.
	subsequentGroups := e.groupPathsForSubsequentIteration(group.paths, duplicatePathLength)
	if nextLevelStats, err := e.applyDeduplicationToGroups(subsequentGroups, duplicatePathLength); err != nil {
		return deduplicationStats{}, err
	} else if skippedBecauseRangeVectorSelectorInRangeQuery {
		// If we didn't eliminate any paths at this level (because the duplicate expression was a range vector selector),
		// return the number returned by the next level.
		stats = nextLevelStats
	}

	return stats, nil
}

// introduceDuplicateNode introduces a Duplicate node for each path in the group and returns false.
// If a Duplicate node already exists at the expected location, then introduceDuplicateNode does not introduce a new node and returns true.
func (e *OptimizationPass) introduceDuplicateNode(group sharedSelectorGroup, duplicatePathLength int) (skipLongerExpressions bool, err error) {
	// Check that we haven't already applied deduplication here because we found this subexpression earlier.
	// For example, if the original expression is "(a + b) + (a + b)", then we will have already found the
	// duplicate "a + b" subexpression when searching from the "a" selectors, so we don't need to do this again
	// when searching from the "b" selectors.
	firstPath := group.paths[0]
	parentOfDuplicate, _ := firstPath.NodeAtOffsetFromLeaf(duplicatePathLength)
	expectedDuplicatedExpression := parentOfDuplicate.Child(firstPath.ChildIndexAtOffsetFromLeaf(duplicatePathLength - 1)) // Note that we can't take this from the path, as the path will not reflect any Duplicate nodes introduced previously.

	// TODO: will need to check for DuplicateFilter here - add test cases for this
	if isDuplicateNode(expectedDuplicatedExpression) {
		return true, nil
	}

	duplicatedExpressionOffset := duplicatePathLength - 1
	duplicatedExpression, _ := firstPath.NodeAtOffsetFromLeaf(duplicatedExpressionOffset)
	duplicate := &Duplicate{Inner: duplicatedExpression, DuplicateDetails: &DuplicateDetails{}}
	e.duplicationNodesIntroduced.Inc()

	for pathIdx, path := range group.paths {
		parentOfDuplicate, _ := path.NodeAtOffsetFromLeaf(duplicatePathLength)
		var newChild planning.Node = duplicate

		if filters := group.getFilterForPath(pathIdx); len(filters) > 0 {
			newChild = &DuplicateFilter{
				Inner:                  duplicate,
				DuplicateFilterDetails: &DuplicateFilterDetails{Filters: filters},
			}
		}

		err := parentOfDuplicate.ReplaceChild(path.ChildIndexAtOffsetFromLeaf(duplicatedExpressionOffset), newChild)
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

func (p path) Selector() (selector, types.QueryTimeRange, error) {
	leaf, leafTimeRange := p.NodeAtOffsetFromLeaf(0)

	selector, ok := leaf.(selector)
	if !ok {
		return nil, types.QueryTimeRange{}, fmt.Errorf("node at leaf position is not a selector: %T", leaf)
	}

	return selector, leafTimeRange, nil
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

type SelectorRelationship int

const (
	NotDuplicateOrSubset SelectorRelationship = iota
	ExactDuplicateSelectors
	SubsetSelectors
)

// SelectorsAreDuplicateOrSubset returns ExactDuplicateSelectors if first and second are the same,
// SubsetSelectors if second is a subset of first, or NotDuplicateOrSubset otherwise.
//
// The matchers in first and second must be sorted in the order produced by core.CompareMatchers.
func SelectorsAreDuplicateOrSubset(first, second []*core.LabelMatcher) (SelectorRelationship, []*core.LabelMatcher) {
	if len(first) == len(second) {
		same := slices.EqualFunc(first, second, func(a, b *core.LabelMatcher) bool {
			return a.Equal(b)
		})

		if same {
			return ExactDuplicateSelectors, nil
		}

		return NotDuplicateOrSubset, nil
	}

	if len(first) > len(second) {
		return NotDuplicateOrSubset, nil
	}

	nextSecondIdx := 0
	var subsetMatchers []*core.LabelMatcher // We deliberately don't pre-allocate this to avoid allocating if second isn't a subset of first, which is expected to be common.

	for _, firstMatcher := range first {
		foundMatch := false

		for nextSecondIdx < len(second) {
			secondMatcher := second[nextSecondIdx]
			comparisonResult := core.CompareMatchers(firstMatcher.Name, secondMatcher.Name, firstMatcher.Type, secondMatcher.Type, firstMatcher.Value, secondMatcher.Value)

			if comparisonResult == 0 {
				// Same matcher in both selectors.
				nextSecondIdx++
				foundMatch = true
				break
			} else if comparisonResult < 0 {
				// First matcher sorts before second matcher, so it can't appear in second,
				// so second is not a subset of first.
				return NotDuplicateOrSubset, nil
			}

			// Second matcher sorts before first matcher, so it can't appear in first,
			// so it could be a subset matcher.
			if subsetMatchers == nil {
				// First time we've seen a possible subset matcher, allocate the slice now.
				subsetMatchers = make([]*core.LabelMatcher, 0, len(second)-len(first))
			}

			subsetMatchers = append(subsetMatchers, secondMatcher)
			nextSecondIdx++
		}

		if !foundMatch {
			// First matcher doesn't appear in second, so second is not a subset of first.
			return NotDuplicateOrSubset, nil
		}
	}

	// If there are any matchers in second that sort after the last matcher in first, then they are all
	// subset matchers, so add them to the list.
	subsetMatchers = append(subsetMatchers, second[nextSecondIdx:]...)

	return SubsetSelectors, subsetMatchers
}

type selector interface {
	planning.Node
	EquivalentToIgnoringMatchersAndHints(other planning.Node) bool
	GetMatchers() []*core.LabelMatcher
}
