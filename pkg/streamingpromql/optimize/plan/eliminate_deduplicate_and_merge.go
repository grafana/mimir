// SPDX-License-Identifier: AGPL-3.0-only

package plan

import (
	"context"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

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

	newRoot, eliminatedAny, err := e.apply(plan.Root, plan.Parameters.EnableDelayedNameRemoval)

	if err != nil {
		return nil, err
	}

	if newRoot != nil {
		plan.Root = newRoot
	}

	if eliminatedAny {
		e.modified.Inc()
	}

	return plan, nil
}

func (e *EliminateDeduplicateAndMergeOptimizationPass) apply(node planning.Node, delayedNameRemoval bool) (planning.Node, bool, error) {
	// Try to eliminate DeduplicateAndMerge nodes in children first.
	var eliminatedAny bool

	for idx := range node.ChildCount() {
		replacement, eliminatedInChild, err := e.apply(node.Child(idx), delayedNameRemoval)
		if err != nil {
			return nil, false, err
		}

		eliminatedAny = eliminatedAny || eliminatedInChild

		if replacement != nil {
			if err := node.ReplaceChild(idx, replacement); err != nil {
				return nil, false, err
			}
		}
	}

	deduplicateAndMerge, isDeduplicateAndMerge := node.(*core.DeduplicateAndMerge)
	if !isDeduplicateAndMerge {
		return nil, eliminatedAny, nil
	}

	if ok, err := canEliminateDeduplicateAndMerge(deduplicateAndMerge.Inner, delayedNameRemoval); err != nil {
		return nil, false, err
	} else if ok {
		return deduplicateAndMerge.Inner, true, nil
	}

	return nil, eliminatedAny, nil
}

func canEliminateDeduplicateAndMerge(node planning.Node, delayedNameRemoval bool) (bool, error) {
	switch node := node.(type) {
	case *core.VectorSelector:
		return hasExactNameMatcher(node.Matchers) || delayedNameRemoval, nil
	case *core.MatrixSelector:
		return hasExactNameMatcher(node.Matchers) || delayedNameRemoval, nil
	case *core.FunctionCall:
		if isLabelReplaceOrJoinFunction(node) {
			return false, nil
		}

		for _, child := range node.Args {
			if ok, err := canEliminateDeduplicateAndMerge(child, delayedNameRemoval); err != nil {
				return false, err
			} else if !ok {
				return false, nil
			}
		}

		return isNameUniqueForFunction(node) || delayedNameRemoval, nil
	case *core.UnaryExpression:
		return canEliminateDeduplicateAndMerge(node.Inner, delayedNameRemoval)
	case *core.DeduplicateAndMerge, *core.AggregateExpression:
		return true, nil
	case *core.DropName:
		// TODO: Need a function with logic specific to this case
		return isNameUniqueForDropName(node), nil
	case *core.BinaryExpression:
		if node.Op == core.BINARY_LOR && !delayedNameRemoval {
			return false, nil
		}

		lhsType, err := node.LHS.ResultType()
		if err != nil {
			return false, err
		}

		rhsType, err := node.RHS.ResultType()
		if err != nil {
			return false, err
		}

		isVectorScalar := lhsType != rhsType && (lhsType == parser.ValueTypeScalar || rhsType == parser.ValueTypeScalar)
		if isVectorScalar {
			return false, nil
		}

		return true, nil

	default:
		return false, nil
	}
}

func hasExactNameMatcher(matchers []*core.LabelMatcher) bool {
	for _, matcher := range matchers {
		if matcher.Name == model.MetricNameLabel && matcher.Type == labels.MatchEqual {
			return true
		}
	}

	return false
}

func isNameUniqueForDropName(n *core.DropName) bool {
	unique := false

	_ = optimize.Walk(n, optimize.VisitorFunc(func(node planning.Node, path []planning.Node) error {
		switch e := node.(type) {
		case *core.VectorSelector:
			if hasExactNameMatcher(e.Matchers) {
				unique = isNameUniqueForDropNamePath(0, path)
			} else {
				unique = isNameUniqueForDropNamePath(1, path)
			}
		case *core.MatrixSelector:
			if hasExactNameMatcher(e.Matchers) {
				unique = isNameUniqueForDropNamePath(0, path)
			} else {
				unique = isNameUniqueForDropNamePath(1, path)
			}
		}

		return nil
	}))

	return unique
}

func isNameUniqueForFunction(n *core.FunctionCall) bool {
	unique := false

	_ = optimize.Walk(n, optimize.VisitorFunc(func(node planning.Node, path []planning.Node) error {
		switch e := node.(type) {
		case *core.VectorSelector:
			if hasExactNameMatcher(e.Matchers) {
				unique = isNameUniqueForFunctionPath(0, path)
			} else {
				unique = isNameUniqueForFunctionPath(1, path)
			}
		case *core.MatrixSelector:
			if hasExactNameMatcher(e.Matchers) {
				unique = isNameUniqueForFunctionPath(0, path)
			} else {
				unique = isNameUniqueForFunctionPath(1, path)
			}
		}

		return nil
	}))

	return unique
}

func isNameUniqueForFunctionPath(needed int, path []planning.Node) bool {
	firstLabelManip := true

	for i := len(path) - 1; i >= 0; i-- {
		switch e := path[i].(type) {
		case *core.FunctionCall:
			if isLabelReplaceOrJoinFunction(e) {
				// TODO: WTF
				if firstLabelManip {
					firstLabelManip = false
					needed += 2
				} else {
					needed++
				}
			}

		case *core.BinaryExpression:
			if e.Op == core.BINARY_LOR {
				needed++
			}
		case *core.DeduplicateAndMerge:
			needed--
		}
	}

	return needed <= 0
}

func isNameUniqueForDropNamePath(needed int, path []planning.Node) bool {
	for i := len(path) - 1; i >= 0; i-- {
		switch e := path[i].(type) {
		case *core.FunctionCall:
			if isLabelReplaceOrJoinFunction(e) {
				needed++
			}
		case *core.BinaryExpression:
			needed++
		case *core.DeduplicateAndMerge:
			needed--
		}
	}

	return needed <= 0
}

func isLabelReplaceOrJoinFunction(node planning.Node) bool {
	if funcNode, isFunctionCall := node.(*core.FunctionCall); isFunctionCall {
		fn := funcNode.Function
		return fn == functions.FUNCTION_LABEL_REPLACE || fn == functions.FUNCTION_LABEL_JOIN
	}
	return false
}
