// SPDX-License-Identifier: AGPL-3.0-only

package plan

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// RemoveStaticallyEmptyExpressionsOptimizationPass replaces subexpressions that can be statically
// determined to return no results with a NoOp node, avoiding unnecessary computation.
//
// It detects the following patterns for timestamp() calls:
//
//	timestamp(<inner>) < <constant>   (or the symmetric form <constant> > timestamp(<inner>))
//	timestamp(<inner>) <= <constant>  (or the symmetric form <constant> >= timestamp(<inner>))
//
// where <constant> is a NumberLiteral (possibly wrapped in transparent nodes) that is before the start of the possible
// timestamps that could be returned given the query's time range.
//
// For simplicity, the optimization pass does not descend into Subquery nodes, because the effective time range for
// expressions inside a subquery differs from the outer query time range.
//
// It detects the following pattern in vector and matrix selectors:
//
//	metric{label1="example", label1="another"}
//
// This exact match selector is guaranteed to not match any results.
//
// It also detects combinations of the above with 'and', 'or' and 'unless' operations:
//
//	empty AND anything is empty
//	anything AND empty is empty
//	empty OR RHS is RHS
//	LHS OR empty is LHS
//	empty UNLESS anything is empty
//	LHS UNLESS empty is LHS
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
	return "Remove statically empty expressions"
}

func (s *RemoveStaticallyEmptyExpressionsOptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, maximumSupportedQueryPlanVersion planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	if maximumSupportedQueryPlanVersion < planning.QueryPlanV10 {
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
		logger.DebugLog("msg", "replaced statically empty expression(s) with no-op")
		s.modified.Inc()
	}

	return plan, nil
}

// apply recursively walks the plan tree, replacing statically-empty binary expressions
// with a NoOp node or simplified equivalent. It returns the replacement node (non-nil if this
// node should be replaced), whether any modification was made, and any error.
func (s *RemoveStaticallyEmptyExpressionsOptimizationPass) apply(node planning.Node, params *planning.QueryParameters) (planning.Node, bool, error) {
	// Do not descend into subqueries for simplicity: their children are evaluated over a different time range
	// (shifted backwards by the subquery range), so params.TimeRange does not apply there.
	// FIXME: we could handle this case
	if _, isSubquery := node.(*core.Subquery); isSubquery {
		return nil, false, nil
	}

	// The info function expects an actual selector as its second argument so we can't replace it
	// with a no-op node or operator even if the matchers would cause it to return no results.
	if isInfoFunction(node) {
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

	if empty, matrix := isAlwaysEmptySelector(node); empty {
		noOp := &core.NoOp{NoOpDetails: &core.NoOpDetails{MatrixSelector: matrix}}
		return noOp, true, nil
	}

	if empty, err := isAlwaysEmpty(node, params); err != nil {
		return nil, false, err
	} else if empty {
		noOp := &core.NoOp{NoOpDetails: &core.NoOpDetails{}}
		return noOp, true, nil
	}

	if replacement := simplify(node); replacement != nil {
		return replacement, true, nil
	}

	return nil, modified, nil
}

func isInfoFunction(node planning.Node) bool {
	if funcNode, isFunctionCall := node.(*core.FunctionCall); isFunctionCall {
		return funcNode.Function == functions.FUNCTION_INFO
	}
	return false
}

// isAlwaysEmptySelector returns true if a node is a selector and has matchers that can be
// determined to produce an empty result and a boolean indicating if the selector is a matrix
// selector or not.
func isAlwaysEmptySelector(node planning.Node) (bool, bool) {
	node = unwrap(node)

	switch node := node.(type) {
	case *core.MatrixSelector:
		return hasConflictingEqualsMatchers(node.Matchers), true
	case *core.VectorSelector:
		return hasConflictingEqualsMatchers(node.Matchers), false
	default:
		return false, false
	}
}

// isAlwaysEmpty returns true if node can be statically determined to produce an empty instant
// vector for the entire query time range described by params.
func isAlwaysEmpty(node planning.Node, params *planning.QueryParameters) (bool, error) {
	node = unwrap(node)

	switch node := node.(type) {
	case *core.NoOp:
		return true, nil
	case *core.BinaryExpression:
		return isAlwaysEmptyBinaryExpression(node, params)
	default:
		return false, nil
	}
}

func hasConflictingEqualsMatchers(matchers []*core.LabelMatcher) bool {
	equals := make(map[string]string)

	for _, m := range matchers {
		if m.Type != labels.MatchEqual {
			continue
		}

		if v, ok := equals[m.Name]; ok && m.Value != v {
			return true
		}

		equals[m.Name] = m.Value
	}

	return false
}

func isAlwaysEmptyBinaryExpression(node *core.BinaryExpression, params *planning.QueryParameters) (bool, error) {
	if node.ReturnBool {
		return false, nil
	}

	switch node.Op {
	case core.BINARY_LAND:
		lhsEmpty, err := isAlwaysEmpty(node.LHS, params)
		if err != nil {
			return false, err
		}

		if lhsEmpty {
			return true, nil
		}

		return isAlwaysEmpty(node.RHS, params)

	case core.BINARY_LOR:
		// A or B is empty only when both sides are empty.
		lhsEmpty, err := isAlwaysEmpty(node.LHS, params)
		if err != nil {
			return false, err
		}

		if !lhsEmpty {
			return false, nil
		}

		return isAlwaysEmpty(node.RHS, params)

	case core.BINARY_LUNLESS:
		// A unless B is empty whenever A is empty, regardless of B.
		return isAlwaysEmpty(node.LHS, params)

	case core.BINARY_LSS:
		// Check for timestamp(v) < C.
		return isAlwaysEmptyTimestampComparison(node.LHS, node.RHS, false, params)

	case core.BINARY_LTE:
		// Check for timestamp(v) <= C.
		return isAlwaysEmptyTimestampComparison(node.LHS, node.RHS, true, params)

	case core.BINARY_GTR:
		// Check for C > timestamp(v), equivalent to timestamp(v) < C.
		return isAlwaysEmptyTimestampComparison(node.RHS, node.LHS, false, params)

	case core.BINARY_GTE:
		// Check for C >= timestamp(v), equivalent to timestamp(v) <= C.
		return isAlwaysEmptyTimestampComparison(node.RHS, node.LHS, true, params)
	}

	return false, nil
}

// isAlwaysEmptyTimestampComparison returns true if timestampSide and constantSide represent
// a timestamp(...) invocation and number literal respectively, and the value of constantSide
// is such that the expression timestampSide < constantSide (inclusive=false) or
// timestampSide <= constantSide (inclusive=true) can never return any results.
func isAlwaysEmptyTimestampComparison(timestampSide, constantSide planning.Node, inclusive bool, params *planning.QueryParameters) (bool, error) {
	timestampCall, ok := findTimestampCall(timestampSide)
	if !ok {
		return false, nil
	}

	constant, ok := findConstant(constantSide)
	if !ok {
		return false, nil
	}

	if len(timestampCall.Args) < 1 {
		// Should never happen, but check to avoid panicking here.
		return false, fmt.Errorf("expected at least one argument in call to timestamp(), got %d", len(timestampCall.Args))
	}

	selector, timestampWrapsSelector := timestampCall.Args[0].(*core.VectorSelector)

	// The expression timestamp(X) < C is guaranteed to return no results if the lowest possible
	// value of timestamp(X) is greater than or equal to C.
	//
	// The expression timestamp(X) <= C is guaranteed to return no results if the lowest possible
	// value of timestamp is greater than C.
	//
	// If X is a selector, then timestamp(X) will return the timestamps of the underlying samples, so we need to check
	// the time range queried to account for the lookback window, offsets and @ modifiers.
	//
	// If X is not a selector, then timestamp(X) can only return timestamps of the steps in the query time range.

	var earliestPossibleTimestampValueInMilliseconds float64
	if timestampWrapsSelector {
		timeRange, err := selector.QueriedTimeRange(params.TimeRange, params.LookbackDelta)
		if err != nil {
			return false, err
		}
		earliestPossibleTimestampValueInMilliseconds = float64(timestamp.FromTime(timeRange.MinT))
	} else {
		earliestPossibleTimestampValueInMilliseconds = float64(params.TimeRange.StartT)
	}

	constantInMilliseconds := constant.Value * 1000

	if inclusive {
		return earliestPossibleTimestampValueInMilliseconds > constantInMilliseconds, nil
	}

	return earliestPossibleTimestampValueInMilliseconds >= constantInMilliseconds, nil
}

// findTimestampCall returns the function node and true if node is (or wraps) a FunctionCall for the timestamp() function.
// It unwraps DeduplicateAndMerge, DropName, and StepInvariantExpression layers transparently.
func findTimestampCall(node planning.Node) (*core.FunctionCall, bool) {
	node = unwrap(node)

	f, ok := node.(*core.FunctionCall)
	if !ok {
		return nil, false
	}

	if f.Function == functions.FUNCTION_TIMESTAMP {
		return f, true
	}

	return nil, false
}

// findConstant returns the number literal node and true if node is (or wraps) a NumberLiteral.
// It unwraps DeduplicateAndMerge, DropName, and StepInvariantExpression layers transparently.
func findConstant(node planning.Node) (*core.NumberLiteral, bool) {
	node = unwrap(node)

	literal, ok := node.(*core.NumberLiteral)
	return literal, ok
}

// simplify returns a simpler version of node, or nil if no simplification applies.
func simplify(node planning.Node) planning.Node {
	switch node := node.(type) {
	case *core.DeduplicateAndMerge:
		// 'or' operations are wrapped in a DeduplicateAndMerge node.
		// If we can optimize the 'or' away, then there's no need for the DeduplicateAndMerge either.

		inner, isBinOp := node.Inner.(*core.BinaryExpression)
		if !isBinOp || inner.Op != core.BINARY_LOR {
			return nil
		}

		// empty OR RHS is equivalent to RHS.
		if _, noOp := inner.LHS.(*core.NoOp); noOp {
			return inner.RHS
		}

		// LHS or empty is equivalent to LHS.
		if _, noOp := inner.RHS.(*core.NoOp); noOp {
			return inner.LHS
		}

	case *core.BinaryExpression:
		if node.Op != core.BINARY_LUNLESS {
			return nil
		}

		// If the LHS is a no-op this means the whole expression is a no-op, and that is
		// handled by isAlwaysEmptyBinaryExpression.
		if _, noOp := node.RHS.(*core.NoOp); noOp {
			return node.LHS
		}
	}

	return nil
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
		case *core.FunctionCall:
			if (n.Function == functions.FUNCTION_ADAPTIVE_METRICS_RESERVED_1 || n.Function == functions.FUNCTION_ADAPTIVE_METRICS_RESERVED_2) && len(n.Args) > 0 {
				// The Adaptive Metrics query rewriting can wrap a timestamp() call (eg. expression becomes wrapper(timestamp(...)) < T), so unwrap it.
				node = n.Args[0]
			} else {
				return node
			}
		default:
			return node
		}
	}
}
