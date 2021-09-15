// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/parallel.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"fmt"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/prometheus/promql/parser"

	util_log "github.com/grafana/mimir/pkg/util/log"
)

var summableAggregates = map[parser.ItemType]struct{}{
	parser.SUM:   {},
	parser.MIN:   {},
	parser.MAX:   {},
	parser.COUNT: {},
	parser.AVG:   {},
}

// NonParallelFuncs is the list of functions that shouldn't be parallelized at any level.
// They basically work across multiple time series.
var NonParallelFuncs = []string{
	"absent",
	"absent_over_time",
	"histogram_quantile",
	"sort_desc",
	"sort",
}

// NonRootParallelFuncs are functions not worth to parallelize at the root of the expression.
// They are functions that work on a single time series at a time.
var NonRootParallelFuncs = []string{
	"label_join",
	"label_replace",
	"abs",
	"ceil",
	"clamp",
	"clamp_max",
	"clamp_min",
	"days_in_month",
	"day_of_month",
	"day_of_week",
	"exp",
	"floor",
	"hour",
	"minute",
	"month",
	"round",
	"scalar",
	"sgn",
	"time",
	"timestamp",
	"vector",
	"year",
}

// CanParallelize tests if a subtree is parallelizable.
// When it returns false, it means the subtree is not parallelizable at this level but could be at another lower level.
func CanParallelize(node parser.Node) bool {
	switch n := node.(type) {
	case nil:
		// nil handles cases where we check optional fields that are not set
		return true

	case parser.Expressions:
		for _, e := range n {
			if !CanParallelize(e) {
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
		if containsAggregateExpr(n.Expr) {
			return false
		}

		return canParallelizeChildren(n.Expr)

	case *parser.BinaryExpr:
		// since binary exprs use each side for merging, they cannot be parallelized
		return false

	case *parser.Call:
		if n.Func == nil {
			return false
		}
		if containsFunction(*n.Func, NonParallelFuncs) || containsFunction(*n.Func, NonRootParallelFuncs) {
			return false
		}

		for _, e := range n.Args {
			if !canParallelizeChildren(e) {
				return false
			}
		}
		return true
	case *parser.ParenExpr:
		return CanParallelize(n.Expr)

	case *parser.UnaryExpr:
		// Since these are only currently supported for Scalars, should be parallel-compatible
		return true

	case *parser.EvalStmt:
		return CanParallelize(n.Expr)

	case *parser.MatrixSelector, *parser.NumberLiteral, *parser.StringLiteral, *parser.VectorSelector:
		return true

	default:
		level.Error(util_log.Logger).Log("err", fmt.Sprintf("CanParallel: unhandled node type %T", node)) //lint:ignore faillint allow global logger for now
		return false
	}
}

// containsAggregateExpr returns true if the given node contains an aggregate expression within its children.
func containsAggregateExpr(n parser.Node) bool {
	containsAggregate, _ := EvalPredicate(n, func(node parser.Node) (bool, error) {
		_, ok := node.(*parser.AggregateExpr)
		return ok, nil
	})
	return containsAggregate
}

// containsFunction tells if the given function is part of a set of functions.
func containsFunction(f parser.Function, functionSet []string) bool {
	for _, v := range functionSet {
		if v == f.Name {
			return true
		}
	}
	return false
}

// canParallelizeChildren tests if a subtree is parallelizable.
func canParallelizeChildren(n parser.Expr) bool {
	containsNonParallelizable, err := EvalPredicate(n, func(node parser.Node) (bool, error) {
		switch n := node.(type) {
		case *parser.Call:
			return containsFunction(*n.Func, NonParallelFuncs), nil
		case *parser.SubqueryExpr:
			return containsAggregateExpr(n), nil
		default:
			return false, nil
		}
	})
	return err == nil && !containsNonParallelizable
}
