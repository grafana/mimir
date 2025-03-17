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

func (d *EliminateCommonSubexpressions) Apply(ctx context.Context, plan *planning.QueryPlan) (*planning.QueryPlan, error) {
	// FIXME: there's certainly a more efficient way to do this
	// FIXME: one easy improvement: alternative to Equals() (or parameter?) that doesn't consider children
	// FIXME: need to consider selector time ranges when doing this (eg. if subqueries are involved)
	// FIXME: handle duplicate binary expressions like (a + a) + (a + a) or (a + b) + (a + b)
	// FIXME: handle nested duplication cases like min(a) + min(a) + max(a) + max(a)

	// Figure out all the paths to leaves
	paths := d.accumulatePaths(plan.Root)

	// For each path: find all the other paths that terminate in the same selector
	groups := d.groupPaths(paths, 0)

	for _, group := range groups {
		if err := d.applyDeduplication(group); err != nil {
			return nil, err
		}
	}

	return plan, nil
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

func (d *EliminateCommonSubexpressions) applyDeduplication(group []*path) error {
	// TODO: search upwards to find highest common point and insert deduplication there
	// TODO: recursively apply deduplication after doing this
	duplicatePathLength := 1 // 1 means only leaf is common to all, 2 means leaf + parent is common etc.

	duplicate := &Duplicate{Inner: group[0].NodeAtOffsetFromLeaf(duplicatePathLength - 1)}

	for _, path := range group {
		parentOfDuplicate := path.NodeAtOffsetFromLeaf(duplicatePathLength)
		err := replaceChild(parentOfDuplicate, path.ChildIndexAtOffsetFromLeaf(duplicatePathLength-1), duplicate)
		if err != nil {
			return err
		}
	}

	return nil
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
