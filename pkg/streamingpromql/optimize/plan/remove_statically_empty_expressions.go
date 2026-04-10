// SPDX-License-Identifier: AGPL-3.0-only

package plan

import (
	"context"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// RemoveStaticallyEmptyExpressionsOptimizationPass replaces subexpressions that can be statically
// determined to return no results with a NoOp node, avoiding unnecessary computation.
//
// Currently it detects the following pattern in both "and" operands:
//
//	timestamp(<inner>) < <constant>   (or the symmetric form <constant> > timestamp(<inner>))
//	timestamp(<inner>) <= <constant>  (or the symmetric form <constant> >= timestamp(<inner>))
//
// where <constant> is a NumberLiteral (possibly wrapped in transparent nodes).
//
// The check is conservative: the optimization applies only when the query start is far enough
// after <constant> that even a sample as old as one lookback-delta before the step cannot
// satisfy the comparison:
//
//	timestamp(v) < C  →  always false when StartT (in ms) >= C*1000 + lookback delta (in ms)
//	timestamp(v) <= C →  always false when StartT (in ms) >  C*1000 + lookback delta (in ms)
//
// This allows the optimization pass to work correctly when v is an instant vector selector
// (which returns the underlying sample timestamp and so could return a value as early as
// StartT - lookback delta), and when v is any other kind of expression (which will
// return the output timestamp, and so could return a value as early as StartT).
//
// For simplicity, the optimization pass does not descend into Subquery nodes, because the effective time range for
// expressions inside a subquery differs from the outer query time range.
type RemoveStaticallyEmptyExpressionsOptimizationPass struct {
	attempts prometheus.Counter
	modified prometheus.Counter
	logger   log.Logger
}

func NewRemoveStaticallyEmptyExpressionsOptimizationPass(reg prometheus.Registerer, logger log.Logger) *RemoveStaticallyEmptyExpressionsOptimizationPass {
	return &RemoveStaticallyEmptyExpressionsOptimizationPass{
		attempts: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_remove_statically_empty_expressions_attempted_total",
			Help: "Total number of queries that the optimization pass has attempted to skip statically empty expressions for.",
		}),
		modified: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_remove_statically_empty_expressions_modified_total",
			Help: "Total number of queries where the optimization pass has replaced one or more statically empty expressions with a no-op.",
		}),
		logger: logger,
	}
}

func (s *RemoveStaticallyEmptyExpressionsOptimizationPass) Name() string {
	return "remove statically empty expressions"
}

func (s *RemoveStaticallyEmptyExpressionsOptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, maximumSupportedQueryPlanVersion planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	if maximumSupportedQueryPlanVersion < planning.QueryPlanV9 {
		// NoOp node is not supported by the downstream querier.
		return plan, nil
	}

	logger := spanlogger.FromContext(ctx, s.logger)
	s.attempts.Inc()

	newRoot, modified, err := s.apply(plan.Root, plan.Parameters)
	if err != nil {
		return nil, err
	}

	if newRoot != nil {
		plan.Root = newRoot
	}

	if modified {
		logger.DebugLog("msg", "replaced statically empty expression(s) with no-op", "count", modified)
		s.modified.Inc()
	}

	return plan, nil
}

// apply recursively walks the plan tree, replacing statically-empty "and" binary expressions
// with a NoOp node. It returns the replacement node (non-nil if this node should be replaced),
// the number of replacements made, and any error.
func (s *RemoveStaticallyEmptyExpressionsOptimizationPass) apply(node planning.Node, params *planning.QueryParameters) (planning.Node, bool, error) {
	// Do not descend into subqueries for simplicity: their children are evaluated over a different time range
	// (shifted backwards by the subquery range), so params.TimeRange does not apply there.
	// FIXME: we could handle this case
	if _, isSubquery := node.(*core.Subquery); isSubquery {
		return nil, false, nil
	}

	modified := false

	for idx := range node.ChildCount() {
		replacement, modifiedInChild, err := s.apply(node.Child(idx), params)
		if err != nil {
			return nil, false, err
		}

		modified = modified || modifiedInChild

		if replacement != nil {
			if err := node.ReplaceChild(idx, replacement); err != nil {
				return nil, false, err
			}
		}
	}

	if isAlwaysEmpty(node, params) {
		noOp := &core.NoOp{NoOpDetails: &core.NoOpDetails{}}
		return noOp, true, nil
	}

	return nil, modified, nil
}

// isAlwaysEmpty returns true if node can be statically determined to produce an empty instant
// vector for the entire query time range described by params.
func isAlwaysEmpty(node planning.Node, params *planning.QueryParameters) bool {
	node = unwrap(node)

	switch node := node.(type) {
	case *core.NoOp:
		return true
	case *core.BinaryExpression:
		return isAlwaysEmptyBinaryExpression(node, params)
	default:
		return false
	}
}

func isAlwaysEmptyBinaryExpression(node *core.BinaryExpression, params *planning.QueryParameters) bool {
	earliestPossibleTimestampValueInMilliseconds := float64(params.TimeRange.StartT - params.LookbackDelta.Milliseconds())

	if node.ReturnBool {
		return false
	}

	switch node.Op {
	case core.BINARY_LAND:
		return isAlwaysEmpty(node.LHS, params) || isAlwaysEmpty(node.RHS, params)

	case core.BINARY_LSS:
		// timestamp(v) < C: always empty when C <= the earliest value that timestamp() could return
		// timestamp() returns the value in seconds since the epoch, so we need to convert to milliseconds.
		if constant, ok := isTimestampComparison(node.LHS, node.RHS); ok {
			return constant*1000 <= earliestPossibleTimestampValueInMilliseconds
		}

	case core.BINARY_LTE:
		// timestamp(v) <= C: always empty when C < the earliest value that timestamp() could return
		if constant, ok := isTimestampComparison(node.LHS, node.RHS); ok {
			return constant*1000 < earliestPossibleTimestampValueInMilliseconds
		}

	case core.BINARY_GTR:
		// C > timestamp(v): equivalent to timestamp(v) < C.
		if constant, ok := isTimestampComparison(node.RHS, node.LHS); ok {
			return constant*1000 <= earliestPossibleTimestampValueInMilliseconds
		}

	case core.BINARY_GTE:
		// C >= timestamp(v): equivalent to timestamp(v) <= C.
		if constant, ok := isTimestampComparison(node.RHS, node.LHS); ok {
			return constant*1000 < earliestPossibleTimestampValueInMilliseconds
		}
	}

	return false
}

// isTimestampComparison checks whether timestampSide is (or wraps) a timestamp()
// function call and constantSide is (or wraps) a NumberLiteral. If so, it returns the constant
// value and true.
func isTimestampComparison(timestampSide, constantSide planning.Node) (float64, bool) {
	if !isTimestampCall(timestampSide) {
		return 0, false
	}

	constantSide = unwrap(constantSide)
	literal, ok := constantSide.(*core.NumberLiteral)
	if !ok {
		return 0, false
	}

	return literal.Value, true
}

// isTimestampCall returns true if node is (or wraps) a FunctionCall for the timestamp() function.
// It unwraps DeduplicateAndMerge, DropName, and StepInvariantExpression layers transparently.
func isTimestampCall(node planning.Node) bool {
	node = unwrap(node)

	fc, ok := node.(*core.FunctionCall)
	return ok && fc.Function == functions.FUNCTION_TIMESTAMP
}

// unwrap removes transparent wrapper nodes returning the innermost non-wrapper node.
func unwrap(node planning.Node) planning.Node {
	for {
		switch n := node.(type) {
		case *core.DeduplicateAndMerge:
			node = n.Inner
		case *core.DropName:
			node = n.Inner
		case *core.StepInvariantExpression:
			node = n.Inner
		default:
			return node
		}
	}
}
