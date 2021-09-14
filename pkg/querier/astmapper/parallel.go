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

// NonParallelFuncs is the list of functions that are not supported for parallelization.
var NonParallelFuncs = []string{
	"absent",
	"absent_over_time",
	"histogram_quantile",
	"vector",
	"time",
	"sort_desc",
	"sort",
	"scalar",
	"label_join",
	"label_replace",
}

// CanParallelize tests if a subtree is parallelizable.
// A subtree is parallelizable if all of its components are parallelizable.
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
		nestedAggrs, err := EvalPredicate(n.Expr, func(node parser.Node) (bool, error) {
			_, ok := node.(*parser.AggregateExpr)
			return ok, nil
		})

		return err == nil && !nestedAggrs && CanParallelize(n.Expr)

	case *parser.BinaryExpr:
		// since binary exprs use each side for merging, they cannot be parallelized
		return false

	case *parser.Call:
		if n.Func == nil {
			return false
		}
		if !ParallelizableFunc(*n.Func) {
			return false
		}

		for _, e := range n.Args {
			if !CanParallelize(e) {
				return false
			}
		}
		return true

	case *parser.SubqueryExpr:
		// Subqueries are parallelizable if they are parallelizable themselves
		// and they don't contain aggregations over series in children nodes.
		return !containsAggregateExpr(n) && CanParallelize(n.Expr)

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

// ParallelizableFunc ensures that a promql function can be part of a parallel query.
func ParallelizableFunc(f parser.Function) bool {
	for _, v := range NonParallelFuncs {
		if v == f.Name {
			return false
		}
	}
	return true
}
