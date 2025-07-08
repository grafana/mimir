// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser"
)

// ReorderHistogramAgg optimizes queries by reordering histogram functions and aggregations
// for more efficient execution.
type ReorderHistogramAgg struct{}

func (r *ReorderHistogramAgg) Name() string {
	return "Histogram aggregation reordering"
}

func (r *ReorderHistogramAgg) Apply(ctx context.Context, expr parser.Expr) (parser.Expr, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return r.apply(expr), nil
}

func (r *ReorderHistogramAgg) apply(expr parser.Expr) parser.Expr {
	switch e := expr.(type) {
	case *parser.Call:
		if !r.isSwitchableCall(e.Func) {
			return mapChildren(e, r.apply)
		}

		if len(e.Args) != 1 {
			return mapChildren(e, r.apply)
		}

		agg, ok := e.Args[0].(*parser.AggregateExpr)
		if !ok || !r.isSwitchableAgg(agg.Op) {
			return mapChildren(e, r.apply)
		}

		// Rewrite from: histogram_func(agg(...)) to: agg(histogram_func(...))
		newExpr := &parser.AggregateExpr{
			Op: agg.Op,
			Expr: &parser.Call{
				Func: e.Func,
				Args: []parser.Expr{agg.Expr},
			},
			Param:    agg.Param,
			Grouping: agg.Grouping,
			Without:  agg.Without,
		}

		return newExpr

	case *parser.AggregateExpr, *parser.MatrixSelector, *parser.SubqueryExpr,
		*parser.NumberLiteral, *parser.StringLiteral, *parser.StepInvariantExpr, *parser.UnaryExpr, *parser.VectorSelector:
		return mapChildren(e, r.apply)

	case *parser.BinaryExpr:
		e.LHS = r.apply(e.LHS)
		e.RHS = r.apply(e.RHS)
		return e

	case *parser.ParenExpr:
		e.Expr = r.apply(e.Expr)
		return e
	}

	return expr
}

func (r *ReorderHistogramAgg) isSwitchableCall(callFunc *parser.Function) bool {
	return callFunc.Name == "histogram_sum" || callFunc.Name == "histogram_count"
}

func (r *ReorderHistogramAgg) isSwitchableAgg(op parser.ItemType) bool {
	return op == parser.SUM || op == parser.AVG
}

// Helper functions for mapping AST nodes recursively
func mapChildren(expr parser.Expr, mapFunc func(parser.Expr) parser.Expr) parser.Expr {
	switch e := expr.(type) {
	case *parser.Call:
		for i, arg := range e.Args {
			e.Args[i] = mapFunc(arg)
		}
	case *parser.AggregateExpr:
		e.Expr = mapFunc(e.Expr)
	case *parser.BinaryExpr:
		e.LHS = mapFunc(e.LHS)
		e.RHS = mapFunc(e.RHS)
	case *parser.SubqueryExpr:
		e.Expr = mapFunc(e.Expr)
	case *parser.ParenExpr:
		e.Expr = mapFunc(e.Expr)
	case *parser.UnaryExpr:
		e.Expr = mapFunc(e.Expr)
	case *parser.StepInvariantExpr:
		e.Expr = mapFunc(e.Expr)
	}
	return expr
}
