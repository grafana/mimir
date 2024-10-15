// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

import (
	"context"
	"math"
	"strconv"

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

	switch e := expr.(type) {
	case *parser.ParenExpr:
		mapped, finished, err = pruner.MapExpr(e.Expr)
		if err != nil {
			return e, false, err
		}
		return &parser.ParenExpr{Expr: mapped, PosRange: e.PosRange}, finished, nil
	case *parser.BinaryExpr:
		return pruner.pruneBinOp(e)
	default:
		return e, false, nil
	}
}

func (pruner *queryPruner) pruneBinOp(expr *parser.BinaryExpr) (mapped parser.Expr, finished bool, err error) {
	switch expr.Op {
	case parser.MUL:
		return pruner.handleMultiplyOp(expr), false, nil
	case parser.GTR, parser.LSS:
		return pruner.handleCompOp(expr), false, nil
	case parser.LOR:
		return pruner.handleOrOp(expr), false, nil
	case parser.LAND:
		return pruner.handleAndOp(expr), false, nil
	case parser.LUNLESS:
		return pruner.handleUnlessOp(expr), false, nil
	default:
		return expr, false, nil
	}
}

// The bool signifies if the number evaluates to infinity, and if it does
// we return the infinity of the correct sign.
func calcInf(isPositive bool, num string) (*parser.NumberLiteral, bool) {
	coeff, err := strconv.Atoi(num)
	if err != nil || coeff == 0 {
		return nil, false
	}
	switch {
	case isPositive && coeff > 0:
		return &parser.NumberLiteral{Val: math.Inf(1)}, true
	case isPositive && coeff < 0:
		return &parser.NumberLiteral{Val: math.Inf(-1)}, true
	case !isPositive && coeff > 0:
		return &parser.NumberLiteral{Val: math.Inf(-1)}, true
	case !isPositive && coeff < 0:
		return &parser.NumberLiteral{Val: math.Inf(1)}, true
	default:
		return nil, false
	}
}

func (pruner *queryPruner) handleMultiplyOp(expr *parser.BinaryExpr) parser.Expr {
	isInfR, signR := pruner.isInfinite(expr.RHS)
	if isInfR {
		newExpr, ok := calcInf(signR, expr.LHS.String())
		if ok {
			return newExpr
		}
	}
	isInfL, signL := pruner.isInfinite(expr.LHS)
	if isInfL {
		newExpr, ok := calcInf(signL, expr.RHS.String())
		if ok {
			return newExpr
		}
	}
	return expr
}

func (pruner *queryPruner) handleCompOp(expr *parser.BinaryExpr) parser.Expr {
	var refNeg, refPos parser.Expr
	switch expr.Op {
	case parser.LSS:
		refNeg = expr.RHS
		refPos = expr.LHS
	case parser.GTR:
		refNeg = expr.LHS
		refPos = expr.RHS
	default:
		return expr
	}

	// foo < -Inf or -Inf > foo => vector(0) < -Inf
	isInf, sign := pruner.isInfinite(refNeg)
	if isInf && !sign {
		return &parser.BinaryExpr{
			LHS: &parser.Call{
				Func: parser.Functions["vector"],
				Args: []parser.Expr{&parser.NumberLiteral{Val: 0}},
			},
			Op:         parser.LSS,
			RHS:        &parser.NumberLiteral{Val: math.Inf(-1)},
			ReturnBool: false,
		}
	}

	// foo > +Inf or +Inf < foo => vector(0) > +Inf => vector(0) < -Inf
	isInf, sign = pruner.isInfinite(refPos)
	if isInf && sign {
		return &parser.BinaryExpr{
			LHS: &parser.Call{
				Func: parser.Functions["vector"],
				Args: []parser.Expr{&parser.NumberLiteral{Val: 0}},
			},
			Op:         parser.LSS,
			RHS:        &parser.NumberLiteral{Val: math.Inf(-1)},
			ReturnBool: false,
		}
	}

	return expr
}

// 1st bool is true if the number is infinite.
// 2nd bool is true if the number is positive infinity.
func (pruner *queryPruner) isInfinite(expr parser.Expr) (bool, bool) {
	mapped, _, err := pruner.MapExpr(expr)
	if err == nil {
		expr = mapped
	}
	switch e := expr.(type) {
	case *parser.ParenExpr:
		return pruner.isInfinite(e.Expr)
	case *parser.NumberLiteral:
		if math.IsInf(e.Val, 1) {
			return true, true
		}
		if math.IsInf(e.Val, -1) {
			return true, false
		}
		return false, false
	default:
		return false, false
	}
}

func (pruner *queryPruner) handleOrOp(expr *parser.BinaryExpr) parser.Expr {
	switch {
	case pruner.isEmpty(expr.LHS):
		return expr.RHS
	case pruner.isEmpty(expr.RHS):
		return expr.LHS
	}
	return expr
}

func (pruner *queryPruner) handleAndOp(expr *parser.BinaryExpr) parser.Expr {
	switch {
	case pruner.isEmpty(expr.LHS):
		return expr.LHS
	case pruner.isEmpty(expr.RHS):
		return expr.RHS
	}
	return expr
}

func (pruner *queryPruner) handleUnlessOp(expr *parser.BinaryExpr) parser.Expr {
	switch {
	case pruner.isEmpty(expr.LHS):
		return expr.LHS
	case pruner.isEmpty(expr.RHS):
		return expr.LHS
	}
	return expr
}

func (pruner *queryPruner) isEmpty(expr parser.Expr) bool {
	mapped, _, err := pruner.MapExpr(expr)
	if err == nil {
		expr = mapped
	}
	switch e := expr.(type) {
	case *parser.ParenExpr:
		return pruner.isEmpty(e.Expr)
	default:
		if e.String() == `vector(0) < -Inf` {
			return true
		}
		return false
	}
}
