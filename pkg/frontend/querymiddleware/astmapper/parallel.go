// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/parallel.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/promql/parser"
)

var summableAggregates = map[parser.ItemType]struct{}{
	parser.GROUP: {},
	parser.SUM:   {},
	parser.MIN:   {},
	parser.MAX:   {},
	parser.COUNT: {},
	parser.AVG:   {},
}

// NonParallelFuncs is the list of functions that shouldn't be parallelized.
var NonParallelFuncs = []string{
	// The following functions are not safe to parallelize.
	"absent",
	"absent_over_time",
	"info", // TODO: Find out whether can be parallelizable.
	"histogram_quantile",
	"limitk",
	"limit_ratio",
	"sort_desc",
	"sort_by_label",
	"sort_by_label_desc",
	"sort",
	"time",
	"vector",

	// The following function may be parallelized using a strategy similar to avg().
	"histogram_avg",
}

// FuncsWithDefaultTimeArg is the list of functions that extract date information from a variadic list of params,
// which defaults to be just time() otherwise.
var FuncsWithDefaultTimeArg = []string{
	"day_of_month",
	"day_of_week",
	"day_of_year",
	"days_in_month",
	"hour",
	"minute",
	"month",
	"year",
}

// CanParallelize tests if a subtree is parallelizable.
// A subtree is parallelizable if all of its components are parallelizable.
func CanParallelize(expr parser.Expr, logger log.Logger) bool {
	switch e := expr.(type) {
	case nil:
		// nil handles cases where we check optional fields that are not set
		return true

	case *parser.AggregateExpr:
		_, ok := summableAggregates[e.Op]
		if !ok {
			return false
		}

		// Ensure there are no nested aggregations
		nestedAggrs, err := anyNode(e.Expr, isAggregateExpr)

		return err == nil && !nestedAggrs && CanParallelize(e.Expr, logger)

	case *parser.BinaryExpr:
		// Binary expressions can be parallelised when:
		// - It's not a bool expr: bool expression should yield only one result, but sharding would provide many.
		// - One of the sides is a constant scalar value
		// - The other side:
		//   - Is not a constant scalar value (because why would we shard then?)
		//   - Does not contain aggregations
		//
		// If one side contained aggregations, like sum(foo) > 0, then aggregated values from different shards can cancel
		// each other, like foo{shard="1"}=1 and foo{shard="2"}=-1: aggregated sum is zero, but if we concat results from different shards it's not.
		//
		// Since we don't care about the order in which binary op is written, we extract the condition into a lambda and check both ways.
		parallelisable := func(a, b parser.Expr) bool {
			return CanParallelize(a, logger) && noAggregates(a) && !isConstantScalar(a) && isConstantScalar(b)
		}
		// If e.VectorMatching is not nil, then both hands are vector operators, so none of them is a constant scalar, so we can't shard it.
		// It is just a shortcut, but the other two operations should imply the same.
		return e.VectorMatching == nil && !e.ReturnBool && (parallelisable(e.LHS, e.RHS) || parallelisable(e.RHS, e.LHS))

	case *parser.Call:
		if e.Func == nil {
			return false
		}
		if !ParallelizableFunc(*e.Func) {
			return false
		}

		for _, e := range argsWithDefaults(e) {
			if !CanParallelize(e, logger) {
				return false
			}
		}
		return true

	case *parser.SubqueryExpr:
		// Subqueries are parallelizable if they are parallelizable themselves
		// and they don't contain aggregations over series in children exprs.
		return !containsAggregateExpr(e) && CanParallelize(e.Expr, logger)

	case *parser.ParenExpr:
		return CanParallelize(e.Expr, logger)

	case *parser.UnaryExpr:
		// Since these are only currently supported for Scalars, should be parallel-compatible
		return true

	case *parser.MatrixSelector, *parser.NumberLiteral, *parser.StringLiteral, *parser.VectorSelector:
		return true

	default:
		level.Error(logger).Log("err", fmt.Sprintf("CanParallelize: unhandled expr type %T", expr)) //lint:ignore faillint allow global logger for now
		return false
	}
}

// containsAggregateExpr returns true if the given expr contains an aggregate expression within its children.
func containsAggregateExpr(e parser.Expr) bool {
	containsAggregate, _ := anyNode(e, isAggregateExpr)
	return containsAggregate
}

// countVectorSelectors returns the number of vector selectors in the input expression.
func countVectorSelectors(e parser.Expr) int {
	count := 0

	visitNode(e, func(node parser.Node) {
		if ok, _ := isVectorSelector(node); ok {
			count++
		}
	})

	return count
}

func isAggregateExpr(n parser.Node) (bool, error) {
	_, ok := n.(*parser.AggregateExpr)
	return ok, nil
}

// ParallelizableFunc ensures that a promql function can be part of a parallel query.
func ParallelizableFunc(f parser.Function) bool {
	for _, v := range NonParallelFuncs {
		if v == f.Name {
			return false
		}
	}
	return true
}

// argsWithDefaults returns the arguments of the call, including the omitted defaults.
func argsWithDefaults(call *parser.Call) parser.Expressions {
	for _, fn := range FuncsWithDefaultTimeArg {
		if fn == call.Func.Name && len(call.Args) == 0 {
			return parser.Expressions{
				&parser.Call{Func: parser.Functions["time"]},
			}
		}
	}
	return call.Args
}

func noAggregates(n parser.Node) bool {
	hasAggregates, _ := anyNode(n, isAggregateExpr)
	return !hasAggregates
}

func isConstantScalar(n parser.Node) bool {
	isNot, _ := anyNode(n, isNotConstantNumber)
	return !isNot
}

func isNotConstantNumber(n parser.Node) (bool, error) {
	switch n := n.(type) {
	case nil,
		*parser.NumberLiteral,
		*parser.UnaryExpr,
		*parser.ParenExpr:
		return false, nil
	case *parser.BinaryExpr:
		// if ReturnBool then not a number, otherwise, it will be a number if both sides are numbers
		return n.ReturnBool, nil
	case *parser.Call:
		// The only function we consider as a constant number is `time()`, everything else is not a constant number.
		return n.Func.Name != "time", nil
	default:
		return true, nil
	}
}
