// SPDX-License-Identifier: AGPL-3.0-only

package plan

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
)

// EliminateDeduplicateAndMergeOptimizationPass removes redundant DeduplicateAndMerge nodes from the plan.
// DeduplicateAndMerge by default are wrapped around operations which manipulate labels (name-dropping functions, some binary operations, etc) and therefore could produce duplicate series.
// These nodes are unnecessary if it can be proven that each input series will produce a unique output series.
// For example, the expression `rate(foo[5m])` produces only unique series because each input series has the same metric name `foo`,
// and each series with the name 'foo' must have a unique set of labels.
// Thus, when rate() function drops the name label, the output is still guaranteed to be unique.
// Primary goal of this optimization is to unlock "labels projection" - ability to load only needed labels into memory.
type EliminateDeduplicateAndMergeOptimizationPass struct{}

type SelectorType int

const (
	NotSelector SelectorType = iota
	SelectorWithoutExactName
	SelectorWithExactName
)

type dedupNodeInfo struct {
	node       *core.DeduplicateAndMerge
	parent     planning.Node
	childIndex int // childIndex is the index of a node in its parent's children array.
}

func NewEliminateDeduplicateAndMergeOptimizationPass() *EliminateDeduplicateAndMergeOptimizationPass {
	return &EliminateDeduplicateAndMergeOptimizationPass{}
}

func (e *EliminateDeduplicateAndMergeOptimizationPass) Name() string {
	return "Eliminate DeduplicateAndMerge"
}

func (e *EliminateDeduplicateAndMergeOptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan) (*planning.QueryPlan, error) {
	var nodesToRemove []dedupNodeInfo
	e.collectNodesToRemove(plan.Root, nil, -1, &nodesToRemove)
	newRoot, err := e.eliminate(nodesToRemove)
	if err != nil {
		return nil, err
	}
	if newRoot != nil {
		plan.Root = newRoot
	}
	return plan, nil
}

func (e *EliminateDeduplicateAndMergeOptimizationPass) collectNodesToRemove(node planning.Node, parent planning.Node, childIndex int, nodesToRemove *[]dedupNodeInfo) {
	// Binary operations are not supported yet.
	// 1. It makes elimination logic much more complex - it's needed to track if name was dropped on both sides of the binary operation.
	// 2. We likely can't do label projections when binary operation is involved, becaue of conflict error messages.
	if _, isBinaryOp := node.(*core.BinaryExpression); isBinaryOp {
		*nodesToRemove = nil
		return
	}

	if dedupNode, isDedup := node.(*core.DeduplicateAndMerge); isDedup {
		*nodesToRemove = append(*nodesToRemove, dedupNodeInfo{
			node:       dedupNode,
			parent:     parent,
			childIndex: childIndex,
		})
	}

	selectorType := getSelectorType(node)
	switch selectorType {
	case SelectorWithExactName:
		// For selectors with exact name matchers – eliminate all collectedDeduplicateAndMerge nodes in the path from root to this selector.
		return
	case SelectorWithoutExactName:
		// If it's a selector without an exact name matcher – keep the DeduplicateAndMerge closest to the selector.
		// There are bunch of exceptions, handled below.
		if len(*nodesToRemove) >= 1 {
			*nodesToRemove = (*nodesToRemove)[:len(*nodesToRemove)-1]
		}
		return
	}

	// Keep the last two DeduplicateAndMerge nodes before the label_replace or label_join function.
	// First is needed because label_replace and label_join might introduce duplicates on its own.
	// Second is needed because they might re-introduce name label even if it was dropped before.
	if isLabelReplaceOrJoinFunction(node) {
		if len(*nodesToRemove) >= 2 {
			*nodesToRemove = (*nodesToRemove)[:len(*nodesToRemove)-2]
		} else {
			*nodesToRemove = nil
		}
	}

	for i, child := range node.Children() {
		e.collectNodesToRemove(child, node, i, nodesToRemove)
	}
}

func (e *EliminateDeduplicateAndMergeOptimizationPass) eliminate(dedupNodes []dedupNodeInfo) (planning.Node, error) {
	var newRoot planning.Node
	for _, dedupInfo := range dedupNodes {
		// TODO: case where the DeduplicateAndMerge node is the root should be reworked to support delayed name removal.
		if dedupInfo.parent == nil {
			newRoot = dedupInfo.node.Inner
			continue
		}
		if err := e.replaceChildAtIndex(dedupInfo.parent, dedupInfo.childIndex, dedupInfo.node.Inner); err != nil {
			return nil, err
		}
	}
	return newRoot, nil
}

func (e *EliminateDeduplicateAndMergeOptimizationPass) replaceChildAtIndex(parent planning.Node, childIndex int, newChild planning.Node) error {
	children := parent.Children()
	children[childIndex] = newChild
	return parent.SetChildren(children)
}

// getSelectorType determines if node is a selector and whether it has an exact name matcher.
func getSelectorType(node planning.Node) SelectorType {
	var matchers []*core.LabelMatcher

	if vs, isVectorSelector := node.(*core.VectorSelector); isVectorSelector {
		matchers = vs.Matchers
	} else if ms, isMatrixSelector := node.(*core.MatrixSelector); isMatrixSelector {
		matchers = ms.Matchers
	} else {
		return NotSelector
	}

	for _, matcher := range matchers {
		if matcher.Name == labels.MetricName && matcher.Type == labels.MatchEqual {
			return SelectorWithExactName
		}
	}

	return SelectorWithoutExactName
}

func isLabelReplaceOrJoinFunction(node planning.Node) bool {
	if funcNode, isFunctionCall := node.(*core.FunctionCall); isFunctionCall {
		fn := funcNode.Function
		return fn == functions.FUNCTION_LABEL_REPLACE || fn == functions.FUNCTION_LABEL_JOIN
	}
	return false
}
