// SPDX-License-Identifier: AGPL-3.0-only

package plan

import (
	"context"
	"slices"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
)

type selectorType int

const (
	notSelector selectorType = iota
	selectorWithoutExactName
	selectorWithExactName
)

type dedupNodeInfo struct {
	node       *core.DeduplicateAndMerge
	parent     planning.Node
	childIndex int // childIndex is the index of a node in its parent's children array.
	keep       bool
}

// EliminateDeduplicateAndMergeOptimizationPass removes redundant DeduplicateAndMerge nodes from the plan.
// DeduplicateAndMerge by default are wrapped around operations which manipulate labels (name-dropping functions, some binary operations, etc) and therefore could produce duplicate series.
// These nodes are unnecessary if it can be proven that each input series produces a unique output series.
// For example, the expression `rate(foo[5m])` produces only unique series because each input series has the same metric name `foo`,
// and each series with the name 'foo' must have a unique set of labels.
// Thus, when rate() function drops the name label, the output is still guaranteed to be unique.
// Primary goal of this optimization is to unlock "labels projection" - ability to load only needed labels into memory.
type EliminateDeduplicateAndMergeOptimizationPass struct {
	attempts prometheus.Counter
	modified prometheus.Counter
}

func NewEliminateDeduplicateAndMergeOptimizationPass(reg prometheus.Registerer) *EliminateDeduplicateAndMergeOptimizationPass {
	return &EliminateDeduplicateAndMergeOptimizationPass{
		attempts: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_eliminate_dedupe_attempted_total",
			Help: "Total number of queries that the optimization pass has attempted to eliminate DeduplicateAndMerge nodes for.",
		}),
		modified: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_eliminate_dedupe_modified_total",
			Help: "Total number of queries where the optimization pass has been able to eliminate DeduplicateAndMerge nodes for.",
		}),
	}
}

func (e *EliminateDeduplicateAndMergeOptimizationPass) Name() string {
	return "Eliminate DeduplicateAndMerge"
}

func (e *EliminateDeduplicateAndMergeOptimizationPass) Apply(_ context.Context, plan *planning.QueryPlan, _ planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	e.attempts.Inc()

	// nodes is a list of DeduplicateAndMerge nodes in the order of their appearance in the plan.
	var nodes []dedupNodeInfo
	e.collect(plan.Root, nil, -1, &nodes, plan.Parameters.EnableDelayedNameRemoval)

	// If there are any DeduplicateAndMerge nodes we are not keeping, increment the modified counter.
	if slices.ContainsFunc(nodes, func(n dedupNodeInfo) bool { return !n.keep }) {
		e.modified.Inc()
	}

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
func (e *EliminateDeduplicateAndMergeOptimizationPass) collect(node planning.Node, parent planning.Node, childIndex int, nodes *[]dedupNodeInfo, enableDelayedNameRemoval bool) {
	// If this is a binary expression, we need to retain the top-level (delayed name removal)
	// or closest (non-delayed name removal) deduplicate and merge node. However, we can potentially
	// remove deduplicate and merge nodes on either side of the binary expression.
	if _, isBinaryOp := node.(*core.BinaryExpression); isBinaryOp {
		if enableDelayedNameRemoval {
			if len(*nodes) > 0 {
				(*nodes)[0].keep = true
			}
		} else {
			if len(*nodes) >= 1 {
				(*nodes)[len(*nodes)-1].keep = true
			}
		}
	}

	if dedupNode, isDedup := node.(*core.DeduplicateAndMerge); isDedup {
		*nodes = append(*nodes, dedupNodeInfo{
			node:       dedupNode,
			parent:     parent,
			childIndex: childIndex,
		})
	}

	selector := getSelectorType(node)
	switch selector {
	case selectorWithExactName:
		// Series with the same name are guaranteed to have the same labels, so we can eliminate all DeduplicateAndMerge nodes.
		return
	case selectorWithoutExactName:
		if enableDelayedNameRemoval {
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
			if enableDelayedNameRemoval {
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
		e.collect(node.Child(i), node, i, nodes, enableDelayedNameRemoval)
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
func getSelectorType(node planning.Node) selectorType {
	var matchers []*core.LabelMatcher

	if vs, isVectorSelector := node.(*core.VectorSelector); isVectorSelector {
		matchers = vs.Matchers
	} else if ms, isMatrixSelector := node.(*core.MatrixSelector); isMatrixSelector {
		matchers = ms.Matchers
	} else {
		return notSelector
	}

	for _, matcher := range matchers {
		if matcher.Name == model.MetricNameLabel && matcher.Type == labels.MatchEqual {
			return selectorWithExactName
		}
	}

	return selectorWithoutExactName
}

func isLabelReplaceOrJoinFunction(node planning.Node) bool {
	if funcNode, isFunctionCall := node.(*core.FunctionCall); isFunctionCall {
		fn := funcNode.Function
		return fn == functions.FUNCTION_LABEL_REPLACE || fn == functions.FUNCTION_LABEL_JOIN
	}
	return false
}
