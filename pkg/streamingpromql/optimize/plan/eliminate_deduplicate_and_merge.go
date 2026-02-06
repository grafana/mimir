// SPDX-License-Identifier: AGPL-3.0-only

package plan

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
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

	var (
		ok  bool
		err error
	)

	if delayedNameRemoval {
		ok = canEliminateDeduplicateAndMergeDelayedNameRemoval(deduplicateAndMerge.Inner)
	} else {
		ok, err = canEliminateDeduplicateAndMerge(deduplicateAndMerge.Inner)
	}

	if err != nil {
		return nil, false, err
	} else if ok {
		return deduplicateAndMerge.Inner, true, nil
	}

	return nil, eliminatedAny, nil
}

func canEliminateDeduplicateAndMergeDelayedNameRemoval(node planning.Node) bool {
	switch node := node.(type) {
	case *core.FunctionCall:
		if isLabelReplaceOrJoinFunction(node) {
			return false
		}

		for _, child := range node.Args {
			if !canEliminateDeduplicateAndMergeDelayedNameRemoval(child) {
				return false
			}
		}

		return true
	case *core.DropName:
		return areSeriesUniqueDropName(node)
	default:
		return true
	}
}

func canEliminateDeduplicateAndMerge(node planning.Node) (bool, error) {
	switch node := node.(type) {
	case *core.VectorSelector:
		return hasExactNameMatcher(node.Matchers), nil
	case *core.MatrixSelector:
		return hasExactNameMatcher(node.Matchers), nil
	case *core.FunctionCall:
		if isLabelReplaceOrJoinFunction(node) {
			return false, nil
		}

		for _, child := range node.Args {
			if ok, err := canEliminateDeduplicateAndMerge(child); err != nil {
				return false, err
			} else if !ok {
				return false, nil
			}
		}

		return areSeriesUniqueFunctionCall(node), nil
	case *core.Subquery:
		return canEliminateDeduplicateAndMerge(node.Inner)
	case *core.UnaryExpression:
		return canEliminateDeduplicateAndMerge(node.Inner)
	case *core.DeduplicateAndMerge, *core.AggregateExpression:
		return true, nil
	case *core.BinaryExpression:
		if node.Op == core.BINARY_LOR {
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

// areSeriesUniqueFunctionCall returns true if the operations between the provided function
// call and any contained selectors are guaranteed to be unique and hence don't need any
// additional DeduplicateAndMerge nodes, false otherwise.
func areSeriesUniqueFunctionCall(node *core.FunctionCall) bool {
	return walkToSelectors(node, func(dedupeNeeded int, path []planning.Node) bool {
		for i := len(path) - 1; i >= 0; i-- {
			switch e := path[i].(type) {
			case *core.FunctionCall:
				if isLabelReplaceOrJoinFunction(e) {
					// If series are currently unique and we see a call to label_replace or
					// label_join, we need _two_ subsequent DeduplicateAndMerge nodes in the
					// query plan. One that ensures that results from the function are unique
					// since it modifies labels and one that ensures results are unique after
					// the __name__ label is dropped.
					if dedupeNeeded == 0 {
						dedupeNeeded += 2
					} else {
						dedupeNeeded++
					}
				}
			case *core.BinaryExpression:
				if e.Op == core.BINARY_LOR {
					dedupeNeeded++
				}
			case *core.DeduplicateAndMerge:
				dedupeNeeded--
			}
		}

		return dedupeNeeded <= 0
	})
}

// areSeriesUniqueDropName returns true if the operations between the DropName operator and any
// contained selectors are guaranteed to be unique and hence don't need any additional
// DeduplicateAndMerge nodes, false otherwise.
func areSeriesUniqueDropName(node *core.DropName) bool {
	return walkToSelectors(node, func(dedupeNeeded int, path []planning.Node) bool {
		for i := len(path) - 1; i >= 0; i-- {
			switch e := path[i].(type) {
			case *core.FunctionCall:
				if isLabelReplaceOrJoinFunction(e) {
					dedupeNeeded++
				}
			case *core.BinaryExpression:
				dedupeNeeded++
			case *core.DeduplicateAndMerge:
				dedupeNeeded--
			}
		}

		return dedupeNeeded <= 0
	})
}

func walkToSelectors(n planning.Node, examine func(dedupeNeeded int, path []planning.Node) bool) bool {
	unique := true

	_ = optimize.Walk(n, optimize.VisitorFunc(func(node planning.Node, path []planning.Node) error {
		switch e := node.(type) {
		case *core.VectorSelector:
			var dedupeNeeded int
			if !hasExactNameMatcher(e.Matchers) {
				dedupeNeeded = 1
			}

			unique = unique && examine(dedupeNeeded, path)
		case *core.MatrixSelector:
			var dedupeNeeded int
			if !hasExactNameMatcher(e.Matchers) {
				dedupeNeeded = 1
			}

			unique = unique && examine(dedupeNeeded, path)
		}

		return nil
	}))

	return unique
}

func isLabelReplaceOrJoinFunction(node planning.Node) bool {
	if funcNode, isFunctionCall := node.(*core.FunctionCall); isFunctionCall {
		fn := funcNode.Function
		return fn == functions.FUNCTION_LABEL_REPLACE || fn == functions.FUNCTION_LABEL_JOIN
	}
	return false
}

func hasExactNameMatcher(matchers []*core.LabelMatcher) bool {
	for _, matcher := range matchers {
		if matcher.Name == model.MetricNameLabel && matcher.Type == labels.MatchEqual {
			return true
		}
	}

	return false
}
