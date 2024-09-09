// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/shard_summer.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"context"
	"math"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql/parser"
)

func NewQueryPruner(ctx context.Context, logger log.Logger) (ASTMapper, error) {
	pruner := newQueryPruner(ctx, logger)
	return NewASTExprMapper(pruner), nil
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
	default:
		return expr, false, nil
	}
}

func calcInf(sign bool, coeff string) (*parser.NumberLiteral, bool) {
	switch {
	case sign && (coeff == "1" || coeff == "+1"):
		return &parser.NumberLiteral{Val: math.Inf(1)}, true
	case sign && coeff == "-1":
		return &parser.NumberLiteral{Val: math.Inf(-1)}, true
	case !sign && (coeff == "1" || coeff == "+1"):
		return &parser.NumberLiteral{Val: math.Inf(-1)}, true
	case !sign && coeff == "-1":
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
		panic("unhandled")
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
			RHS:        refNeg,
			ReturnBool: false,
		}
	}

	// foo > +Inf or +Inf < foo => vector(0) > +Inf
	isInf, sign = pruner.isInfinite(refPos)
	if isInf && sign {
		return &parser.BinaryExpr{
			LHS: &parser.Call{
				Func: parser.Functions["vector"],
				Args: []parser.Expr{&parser.NumberLiteral{Val: 0}},
			},
			Op:         parser.GTR,
			RHS:        refPos,
			ReturnBool: false,
		}
	}

	return expr
}

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
