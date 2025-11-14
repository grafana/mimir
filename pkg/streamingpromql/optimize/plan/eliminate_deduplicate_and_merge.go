// SPDX-License-Identifier: AGPL-3.0-only

package plan

import (
	"context"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
)

// EliminateDeduplicateAndMergeOptimizationPass removes redundant DeduplicateAndMerge nodes from the plan.
// DeduplicateAndMerge by default are wrapped around operations which manipulate labels (name-dropping functions, some binary operations, etc) and therefore could produce duplicate series.
// These nodes are unnecessary if it can be proven that each input series produces a unique output series.
// For example, the expression `rate(foo[5m])` produces only unique series because each input series has the same metric name `foo`,
// and each series with the name 'foo' must have a unique set of labels.
// Thus, when rate() function drops the name label, the output is still guaranteed to be unique.
// Primary goal of this optimization is to unlock "labels projection" - ability to load only needed labels into memory.
type EliminateDeduplicateAndMergeOptimizationPass struct {
	enableDelayedNameRemoval bool
}

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
	keep       bool
}

func NewEliminateDeduplicateAndMergeOptimizationPass(enableDelayedNameRemoval bool) *EliminateDeduplicateAndMergeOptimizationPass {
	return &EliminateDeduplicateAndMergeOptimizationPass{
		enableDelayedNameRemoval: enableDelayedNameRemoval,
	}
}

func (e *EliminateDeduplicateAndMergeOptimizationPass) Name() string {
	return "Eliminate DeduplicateAndMerge"
}

func (e *EliminateDeduplicateAndMergeOptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, _ planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	// nodes is a list of DeduplicateAndMerge nodes in the order of their appearance in the plan.
	var nodes []dedupNodeInfo
	e.collect(plan.Root, nil, -1, &nodes)
	newRoot, err := e.eliminate(nodes)
	if err != nil {
		return nil, err
	}
	if newRoot != nil {
		plan.Root = newRoot
	}
	return plan, nil
}

// collect collects DeduplicateAndMerge nodes from the plan and marks them for removal or keeping.
func (e *EliminateDeduplicateAndMergeOptimizationPass) collect(node planning.Node, parent planning.Node, childIndex int, nodes *[]dedupNodeInfo) {
	// Binary operations are not supported yet. When we encounter a binary operation, we stop the elimination
	// and keep all DeduplicateAndMerge nodes. It's done just to keep initial implementation simple.
	// TODO:
	// 1. Remove DeduplicateAndMerge nodes provided they don't contain binary operations - rate(foo[5m]) / rate(bar[5m])
	// 2. Handle all binary operations by inspecting whether each side produces series with a __name__ label that could cause duplicates.
	if _, isBinaryOp := node.(*core.BinaryExpression); isBinaryOp {
		*nodes = nil
		return
	}

	if dedupNode, isDedup := node.(*core.DeduplicateAndMerge); isDedup {
		*nodes = append(*nodes, dedupNodeInfo{
			node:       dedupNode,
			parent:     parent,
			childIndex: childIndex,
		})
	}

	selectorType := getSelectorType(node)
	switch selectorType {
	case SelectorWithExactName:
		// Series with the same name are guaranteed to have the same labels, so we can eliminate all DeduplicateAndMerge nodes.
		return
	case SelectorWithoutExactName:
		if e.enableDelayedNameRemoval {
			// With Delayed Name Removal name is dropped at the very end of query execution.
			// Keep the DeduplicateAndMerge closest to root to handle final deduplication.
			if len(*nodes) > 0 {
				(*nodes)[0].keep = true
			}
		} else {
			// Without delayed name removal name is dropped immediately, so there should be no duplicates after.
			// So, keep only the DeduplicateAndMerge closest to the selector.
			if len(*nodes) >= 1 {
				(*nodes)[len(*nodes)-1].keep = true
			}
		}
		return
	}

	// label_replace or label_join manipulate labels, so they could cause duplicates or reintroduce the __name__ label.
	// Keep DeduplicateAndMerge wrapping label_replace or label_join to handle potential duplicates.
	if isLabelReplaceOrJoinFunction(node) {
		switch len(*nodes) {
		case 0:
		case 1:
			(*nodes)[0].keep = true
		default:
			if e.enableDelayedNameRemoval {
				// Also keep the root DeduplicateAndMerge to handle final deduplication, if delayed name removal is enabled.
				// Name is dropped at the very end of query execution and label_replace or label_join might mess with it even if selector guarantees unique series.
				(*nodes)[len(*nodes)-1].keep = true
			} else {
				// Also keep DeduplicateAndMerge wrapping closest __name__ dropping operation, in case __name__ is reintroduced by label_replace or label_join and operation's result should be deduplicated.
				(*nodes)[len(*nodes)-2].keep = true
				(*nodes)[len(*nodes)-1].keep = true
			}
		}
	}

	for i := range node.ChildCount() {
		e.collect(node.Child(i), node, i, nodes)
	}
}

func (e *EliminateDeduplicateAndMergeOptimizationPass) eliminate(dedupNodes []dedupNodeInfo) (planning.Node, error) {
	var newRoot planning.Node

	for _, dedupInfo := range dedupNodes {
		if dedupInfo.keep {
			continue
		}
		if dedupInfo.parent == nil {
			newRoot = dedupInfo.node.Inner
			continue
		}
		if err := dedupInfo.parent.ReplaceChild(dedupInfo.childIndex, dedupInfo.node.Inner); err != nil {
			return nil, err
		}
	}
	return newRoot, nil
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
		if matcher.Name == model.MetricNameLabel && matcher.Type == labels.MatchEqual {
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
