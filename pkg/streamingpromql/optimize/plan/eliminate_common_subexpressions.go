// SPDX-License-Identifier: AGPL-3.0-only

package plan

import (
	"context"
	"fmt"
	"slices"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
)

type EliminateCommonSubexpressions struct{}

func (d *EliminateCommonSubexpressions) Name() string {
	return "Eliminate common subexpressions"
}

// TODO: tests
// Some interesting test cases:
//
// # Duplicate expression
// foo + foo
//
// # Duplicated many times
// foo + foo + foo
//
// foo + foo + foo + bar + foo
//
// # Duplicated with aggregation
// max(foo) - min(foo)
//
// # Duplicated aggregation
// max(foo) + max(foo)
//
// # Multiple levels of duplication: a and sum(a)
// a + sum(a) + sum(a)
//
// # Duplicated binary operations
// (a - a) + (a - a)
//
// (a * b) + (a * b)
//
// (a - a) + (a - a) + (a * b) + (a * b)
// ((a - a) + (a - a)) + ((a * b) + (a * b))
// (a - a) + ((a - a) / (a * b)) % (a * b)
// ((aa - ab) + (ac - ad)) + ((a * b) + (a * b))
//
// # Combinations of all of the above
// b + b + sum(a) + sum(a) + sum(rate(foo[1m])) + max(rate(foo[1m])) + a

func (d *EliminateCommonSubexpressions) Apply(ctx context.Context, plan *planning.QueryPlan) (*planning.QueryPlan, error) {
	// FIXME: there's certainly a more efficient way to do this
	// FIXME: need to consider selector time ranges when doing this (eg. if subqueries are involved)
	// - introduce "TimeRange(parent types.QueryTimeRange) types.QueryTimeRange" method on Node?
	// FIXME: when expression is something like (a + b) / (a + b), we'll do some duplicate work or potentially do the wrong thing:
	// - first we'll process the duplicate "a" selectors
	//   - we'll replace the "a" selectors with a single reference
	//   - then we'll continue up the path and replace the (a + b) expressions with a single reference
	// - then we'll process the duplicate "b" selectors
	//   - we'll replace the "b" selectors with a single reference (but this is not necessary any more - there's only one reference to "b")
	//   - we'll replace the (a + b) expressions with another single reference (wasteful but not wrong)
	// TODO: don't deduplicate range vector selectors or subqueries directly - only deduplicate expressions that produce instant vectors
	// TODO: subset selectors
	// TODO: exploit associativity to find more common expressions? (eg. (a + b) / (a + c + b) --> (a + b) / (a + b + c) --> X / (X + c))
	// - likely tricky to do correctly with series matching rules, may not be worth it

	// Figure out all the paths to leaves
	paths := d.accumulatePaths(plan.Root)

	// For each path: find all the other paths that terminate in the same selector, then inject a duplication node
	err := d.groupAndApplyDeduplication(paths, 0)
	if err != nil {
		return nil, err
	}

	return plan, nil
}

func (d *EliminateCommonSubexpressions) groupAndApplyDeduplication(paths []*path, offset int) error {
	groups := d.groupPaths(paths, offset)

	for _, group := range groups {
		if err := d.applyDeduplication(group, offset); err != nil {
			return err
		}
	}

	return nil
}

// accumulatePaths returns a list of paths from root that terminate in VectorSelector or MatrixSelector nodes.
func (d *EliminateCommonSubexpressions) accumulatePaths(root planning.Node) []*path {
	return d.accumulatePath(root, &path{
		nodes:        []planning.Node{root},
		childIndices: []int{0},
	})
}

func (d *EliminateCommonSubexpressions) accumulatePath(node planning.Node, soFar *path) []*path {
	_, isVS := node.(*planning.VectorSelector)
	_, isMS := node.(*planning.MatrixSelector)

	if isVS || isMS {
		return []*path{soFar}
	}

	children := node.Children()
	if len(children) == 0 {
		// No children and not a vector selector or matrix selector, we're not interested in this path.
		return nil
	}

	paths := make([]*path, 0, len(children)) // FIXME: could avoid an allocation here when there's only one child: just return the slice from the recursive call

	for childIdx, child := range children {
		path := soFar.Clone()
		path.nodes = append(path.nodes, child)
		path.childIndices = append(path.childIndices, childIdx)
		childPaths := d.accumulatePath(child, path)
		paths = append(paths, childPaths...)
	}

	return paths
}

// groupPaths returns paths grouped by the node at offset from the leaf.
// offset 0 means group by the leaf, offset 1 means group by the leaf node's parent etc.
// paths that have a unique grouping node are not returned.
func (d *EliminateCommonSubexpressions) groupPaths(paths []*path, offset int) [][]*path {
	alreadyGrouped := make([]bool, len(paths))
	groups := make([][]*path, 0)

	// FIXME: find a better way to do this, this is currently O(n!) in the worst case
	// Maybe generate some kind of key for each node and then sort and group by that? (use same key that we'd use for caching?)
	for pathIdx, p := range paths {
		if alreadyGrouped[pathIdx] {
			continue
		}

		alreadyGrouped[pathIdx] = true
		leaf := p.NodeAtOffsetFromLeaf(offset)
		var group []*path

		for otherPathOffset, otherPath := range paths[pathIdx+1:] {
			otherPathIdx := otherPathOffset + pathIdx + 1

			if alreadyGrouped[otherPathIdx] {
				continue
			}

			otherPathLeaf := otherPath.NodeAtOffsetFromLeaf(offset)
			if leaf == otherPathLeaf {
				// If we've found two paths with the same ancestor, then we don't consider these the same, as there's no duplication.
				// eg. if the expression is "a + a":
				// - if offset is 0 (group by leaf nodes): these are duplicates, group them together
				// - if offset is 1 (group by parent of leaf nodes): same node, no duplication, don't group together
				continue
			}

			if !leaf.Equals(otherPathLeaf) {
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

func (d *EliminateCommonSubexpressions) applyDeduplication(group []*path, offset int) error {
	duplicatePathLength := d.findCommonSubexpressionLength(group, offset+1)
	duplicate := &Duplicate{Inner: group[0].NodeAtOffsetFromLeaf(duplicatePathLength - 1)}

	for _, path := range group {
		parentOfDuplicate := path.NodeAtOffsetFromLeaf(duplicatePathLength)
		err := replaceChild(parentOfDuplicate, path.ChildIndexAtOffsetFromLeaf(duplicatePathLength-1), duplicate)
		if err != nil {
			return err
		}
	}

	if len(group) <= 2 {
		// Can't possibly have any more common subexpressions. We're done.
		return nil
	}

	// Check if a subset of the paths we just examined share an even longer common subexpression.
	// eg. if the expression is "a + max(a) + max(a)", then we may have just deduplicated the "a" selectors,
	// but we can also deduplicate the "max(a)" expressions.
	if err := d.groupAndApplyDeduplication(group, duplicatePathLength); err != nil {
		return err
	}

	return nil
}

// findCommonSubexpressionLength returns the length of the common expression present at the end of each path
// in group, starting at offset.
// offset 0 means start from leaf of all paths.
// If a non-zero offset is provided, then it is assumed all paths in group already have a common subexpression of length offset.
func (d *EliminateCommonSubexpressions) findCommonSubexpressionLength(group []*path, offset int) int {
	length := offset
	firstPath := group[0]

	for length < len(firstPath.nodes)-1 { // -1 to exclude root node (otherwise the longest common subexpression for "a + a" would be 2, not 1)
		firstNode := firstPath.NodeAtOffsetFromLeaf(length)

		for _, path := range group[1:] {
			if length >= len(path.nodes) {
				// We've reached the end of this path, so the longest common subexpression is the length of this path.
				return length
			}

			otherNode := path.NodeAtOffsetFromLeaf(length)
			if firstNode == otherNode {
				// If we've found two paths with the same ancestor, then we don't consider these the same, as there's no duplication.
				// eg. if the expression is "a + a":
				// - if offset is 0 (group by leaf nodes): these are duplicates, group them together
				// - if offset is 1 (group by parent of leaf nodes): same node, no duplication, don't group together
				return length
			}

			if !firstNode.Equals(otherNode) {
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
	childIndices []int // childIndices[x] contains the position of node x in its parent
}

func (p *path) NodeAtOffsetFromLeaf(offset int) planning.Node {
	return p.nodes[len(p.nodes)-offset-1]
}

func (p *path) ChildIndexAtOffsetFromLeaf(offset int) int {
	return p.childIndices[len(p.nodes)-offset-1]
}

// TODO: pooling?
func (p *path) Clone() *path {
	return &path{
		nodes:        slices.Clone(p.nodes),
		childIndices: slices.Clone(p.childIndices),
	}
}

type Duplicate struct {
	Inner planning.Node
}

func (d *Duplicate) Type() string {
	return "Duplicate"
}

func (d *Duplicate) Children() []planning.Node {
	return []planning.Node{d.Inner}
}

func (d *Duplicate) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type Duplicate supports 1 child, but got %d", len(children))
	}

	d.Inner = children[0]

	return nil
}

func (d *Duplicate) Equals(other planning.Node) bool {
	otherDuplicate, ok := other.(*Duplicate)

	return ok && d.Inner.Equals(otherDuplicate.Inner)
}
