// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/promql/parser"
)

type InsertOmittedTargetInfoSelector struct{}

func (h *InsertOmittedTargetInfoSelector) Name() string {
	return "Insert Omitted Target Info Selector"
}

func (h *InsertOmittedTargetInfoSelector) Apply(_ context.Context, expr parser.Expr) (parser.Expr, error) {
	return h.apply(expr), nil
}

func (h *InsertOmittedTargetInfoSelector) apply(expr parser.Expr) parser.Expr {
	switch expr := expr.(type) {
	case *parser.Call:
		if expr.Func.Name == "info" {
			if len(expr.Args) == 1 {
				infoExpr, err := parser.ParseExpr("target_info")
				if err != nil {
					panic(fmt.Sprintf("failed to parse target_info expression: %v", err))
				}
				expr.Args = append(expr.Args, infoExpr)
			}
		}
		for i, arg := range expr.Args {
			expr.Args[i] = h.apply(arg)
		}
		return expr
	case *parser.ParenExpr:
		expr.Expr = h.apply(expr.Expr)
		return expr
	case *parser.UnaryExpr:
		expr.Expr = h.apply(expr.Expr)
		return expr
	case *parser.BinaryExpr:
		expr.LHS = h.apply(expr.LHS)
		expr.RHS = h.apply(expr.RHS)
		return expr
	case *parser.AggregateExpr:
		expr.Expr = h.apply(expr.Expr)
		return expr
	case *parser.SubqueryExpr:
		expr.Expr = h.apply(expr.Expr)
		return expr
	case *parser.StepInvariantExpr:
		expr.Expr = h.apply(expr.Expr)
		return expr
	default:
		return expr
	}
}
