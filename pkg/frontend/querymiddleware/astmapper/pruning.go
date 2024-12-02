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
	// The right hand side is const and not empty, so the whole expression is
	// just the left side.
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

	if vectorAndConst, equals := pruner.isVectorAndNumberEqual(lhs, rhs); vectorAndConst {
		return true, !equals
	}
	if vectorAndConst, equals := pruner.isVectorAndNumberEqual(rhs, lhs); vectorAndConst {
		return true, !equals
	}
	return false, false
}

// isVectorAndNumberEqual returns whether the lhs is a const vector like
// "vector(5)"" and the right hand size is a number like "2". Also returns
// if the values are equal.
func (pruner *queryPruner) isVectorAndNumberEqual(lhs, rhs parser.Expr) (bool, bool) {
	lIsVector, lValue := pruner.isConstVector(lhs)
	if !lIsVector {
		return false, false
	}
	rIsConst, rValue := pruner.isNumber(rhs)
	if !rIsConst {
		return false, false
	}
	return true, rValue == lValue
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
