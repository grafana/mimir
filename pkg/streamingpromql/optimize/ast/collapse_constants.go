// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"
	"fmt"
	"math"

	"github.com/prometheus/prometheus/promql/parser"
)

type CollapseConstants struct{}

func (c *CollapseConstants) Name() string {
	return "Collapse constants"
}

func (c *CollapseConstants) Apply(_ context.Context, expr parser.Expr) (parser.Expr, error) {
	return c.apply(expr), nil
}

func (c *CollapseConstants) apply(expr parser.Expr) parser.Expr {
	switch expr := expr.(type) {
	case *parser.BinaryExpr:
		expr.LHS = c.apply(expr.LHS)
		expr.RHS = c.apply(expr.RHS)

		lhs, ok := expr.LHS.(*parser.NumberLiteral)
		if !ok {
			return expr
		}

		rhs, ok := expr.RHS.(*parser.NumberLiteral)
		if !ok {
			return expr
		}

		f, ok := c.convertToConstant(lhs, rhs, expr.Op)
		if !ok {
			return expr
		}

		return &parser.NumberLiteral{
			Val:      f,
			PosRange: expr.PositionRange(),
		}
	case *parser.AggregateExpr:
		expr.Expr = c.apply(expr.Expr)

		if expr.Param != nil {
			expr.Param = c.apply(expr.Param)
		}

		return expr
	case *parser.Call:
		for i := range expr.Args {
			expr.Args[i] = c.apply(expr.Args[i])
		}

		return expr
	case *parser.SubqueryExpr:
		expr.Expr = c.apply(expr.Expr)
		return expr
	case *parser.UnaryExpr:
		expr.Expr = c.apply(expr.Expr)

		if e, ok := expr.Expr.(*parser.NumberLiteral); ok {
			switch expr.Op {
			case parser.SUB:
				// Apply the negation directly and drop the outer UnaryExpr.
				e.Val = -e.Val

				return e
			case parser.ADD:
				// Drop the outer UnaryExpr, unary addition is a no-op.
				return e
			default:
				// Should never happen, but if it does, return the expression as-is.
			}
		}

		return expr
	case *parser.ParenExpr:
		expr.Expr = c.apply(expr.Expr)

		if _, ok := expr.Expr.(*parser.NumberLiteral); ok {
			// Drop the parentheses, they're not needed.
			return expr.Expr
		}

		return expr
	case *parser.StepInvariantExpr:
		expr.Expr = c.apply(expr.Expr)
		return expr
	case *parser.VectorSelector, *parser.MatrixSelector, *parser.StringLiteral, *parser.NumberLiteral:
		// Nothing to do.
		return expr
	default:
		panic(fmt.Sprintf("unknown expression type: %T", expr))
	}
}

func (c *CollapseConstants) convertToConstant(lhs, rhs *parser.NumberLiteral, op parser.ItemType) (float64, bool) {
	switch op {
	case parser.ADD:
		return lhs.Val + rhs.Val, true
	case parser.SUB:
		return lhs.Val - rhs.Val, true
	case parser.MUL:
		return lhs.Val * rhs.Val, true
	case parser.DIV:
		return lhs.Val / rhs.Val, true
	case parser.MOD:
		return math.Mod(lhs.Val, rhs.Val), true
	case parser.POW:
		return math.Pow(lhs.Val, rhs.Val), true
	case parser.ATAN2:
		return math.Atan2(lhs.Val, rhs.Val), true
	default:
		return 0, false
	}
}
