// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

import (
	"context"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql/parser"
)

func NewQueryPruner(ctx context.Context, logger log.Logger) ASTMapper {
	pruner := newQueryPruner(ctx, logger)
	return NewASTExprMapper(pruner)
}

type queryPruner struct {
	ctx    context.Context
	logger log.Logger
}

func newQueryPruner(ctx context.Context, logger log.Logger) *queryPruner {
	return &queryPruner{
		ctx:    ctx,
		logger: logger,
	}
}

func (pruner *queryPruner) MapExpr(expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
	if err := pruner.ctx.Err(); err != nil {
		return nil, false, err
	}

	e, ok := expr.(*parser.BinaryExpr)
	if !ok {
		return expr, false, nil
	}

	if e.Op != parser.LAND || e.VectorMatching == nil ||
		!e.VectorMatching.On || len(e.VectorMatching.MatchingLabels) != 0 {
		// Return if not "<lhs> and on() <rhs>"
		return expr, false, nil
	}

	isConst, isEmpty := pruner.isConst(e.RHS)
	if !isConst {
		return expr, false, nil
	}
	if isEmpty {
		// The right hand side is empty, so the whole expression is empty due to
		// "and on()", return the right hand side.
		return e.RHS, false, nil
	}
	// The right hand side is const no empty, so the whole expression is just the
	// left side.
	return e.LHS, false, nil
}

func (pruner *queryPruner) isConst(expr parser.Expr) (isConst, isEmpty bool) {
	var lhs, rhs parser.Expr
	switch e := expr.(type) {
	case *parser.ParenExpr:
		return pruner.isConst(e.Expr)
	case *parser.BinaryExpr:
		if e.Op != parser.EQLC || e.ReturnBool {
			return false, false
		}
		lhs = e.LHS
		rhs = e.RHS
	default:
		return false, false
	}

	lIsVector, lValue := pruner.isConstVector(lhs)
	if lIsVector {
		rIsConst, rValue := pruner.isNumber(rhs)
		if rIsConst {
			return true, rValue != lValue
		}
		return false, false
	}
	var lIsConst bool
	lIsConst, lValue = pruner.isNumber(lhs)
	if !lIsConst {
		return false, false
	}
	rIsVector, rValue := pruner.isConstVector(rhs)
	if !rIsVector {
		return false, false
	}
	return true, lValue != rValue
}

func (pruner *queryPruner) isConstVector(expr parser.Expr) (isVector bool, value float64) {
	switch e := expr.(type) {
	case *parser.ParenExpr:
		return pruner.isConstVector(e.Expr)
	case *parser.Call:
		if e.Func.Name != "vector" || len(e.Args) != 1 {
			return false, 0
		}
		lit, ok := e.Args[0].(*parser.NumberLiteral)
		if !ok {
			return false, 0
		}
		return true, lit.Val
	}
	return false, 0
}

func (pruner *queryPruner) isNumber(expr parser.Expr) (isNumber bool, value float64) {
	switch e := expr.(type) {
	case *parser.ParenExpr:
		return pruner.isNumber(e.Expr)
	case *parser.NumberLiteral:
		return true, e.Val
	}
	return false, 0
}
