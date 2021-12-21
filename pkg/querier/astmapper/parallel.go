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
	"histogram_quantile",
	"sort_desc",
	"sort",
}

// CanParallelize tests if a subtree is parallelizable.
// A subtree is parallelizable if all of its components are parallelizable.
func CanParallelize(node parser.Node, logger log.Logger) bool {
	switch n := node.(type) {
	case nil:
		// nil handles cases where we check optional fields that are not set
		return true

	case parser.Expressions:
		for _, e := range n {
			if !CanParallelize(e, logger) {
				return false
			}
		}
		return true

	case *parser.AggregateExpr:
		_, ok := summableAggregates[n.Op]
		if !ok {
			return false
		}

		// Ensure there are no nested aggregations
		nestedAggrs, err := anyNode(n.Expr, isAggregateExpr)

		return err == nil && !nestedAggrs && CanParallelize(n.Expr, logger)

	case *parser.BinaryExpr:
		// Binary expressions can be parallelised when one of the sides is a constant scalar value
		// and the other one can be parallelised and does not contain agggregations.
		// If one side contained aggregations, like sum(foo) > 0, then aggregated values from different shards can cancel
		// each other, like foo{shard="1"}=1 and foo{shard="1"}=-1: aggregated sum is zero, but if we concat results from different shards it's not.
		// Both comparison and arithmetic binary operations _can_ be parallelised, but not all of them are worth parallelising,
		// this function doesn't decide that.
		// Since we don't care the order in which binary op is written, we extract the condition into a lambda and check both ways.
		parallelisable := func(a, b parser.Node) bool {
			return CanParallelize(a, logger) && noAggregates(a) && isConstantScalar(b)
		}
		return parallelisable(n.LHS, n.RHS) || parallelisable(n.RHS, n.LHS)

	case *parser.Call:
		if n.Func == nil {
			return false
		}
		if !ParallelizableFunc(*n.Func) {
			return false
		}

		for _, e := range n.Args {
			if !CanParallelize(e, logger) {
				return false
			}
		}
		return true

	case *parser.SubqueryExpr:
		// Subqueries are parallelizable if they are parallelizable themselves
		// and they don't contain aggregations over series in children nodes.
		return !containsAggregateExpr(n) && CanParallelize(n.Expr, logger)

	case *parser.ParenExpr:
		return CanParallelize(n.Expr, logger)

	case *parser.UnaryExpr:
		// Since these are only currently supported for Scalars, should be parallel-compatible
		return true

	case *parser.EvalStmt:
		return CanParallelize(n.Expr, logger)

	case *parser.MatrixSelector, *parser.NumberLiteral, *parser.StringLiteral, *parser.VectorSelector:
		return true

	default:
		level.Error(logger).Log("err", fmt.Sprintf("CanParallel: unhandled node type %T", node)) //lint:ignore faillint allow global logger for now
		return false
	}
}

// containsAggregateExpr returns true if the given node contains an aggregate expression within its children.
func containsAggregateExpr(n parser.Node) bool {
	containsAggregate, _ := anyNode(n, isAggregateExpr)
	return containsAggregate
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
