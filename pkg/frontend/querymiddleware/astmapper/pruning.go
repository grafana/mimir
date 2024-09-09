// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/shard_summer.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"context"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql/parser"
)

func NewQueryPruner(ctx context.Context, logger log.Logger, stats *MapperStats) (ASTMapper, error) {
	pruner := newQueryPruner(ctx, logger, stats)
	return NewASTExprMapper(pruner), nil
}

type queryPruner struct {
	ctx context.Context

	logger log.Logger
	stats  *MapperStats

	canShardAllVectorSelectorsCache map[string]bool
}

func newQueryPruner(ctx context.Context, logger log.Logger, stats *MapperStats) *queryPruner {
	return &queryPruner{
		ctx: ctx,

		logger: logger,
		stats:  stats,

		canShardAllVectorSelectorsCache: make(map[string]bool),
	}
}

func (pruner *queryPruner) Clone() *queryPruner {
	s := *pruner
	s.stats = NewMapperStats()
	return &s
}

func (pruner *queryPruner) Copy() *queryPruner {
	s := *pruner
	return &s
}

func (pruner *queryPruner) MapExpr(expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
	if err := pruner.ctx.Err(); err != nil {
		return nil, false, err
	}

	switch e := expr.(type) {
	case *parser.AggregateExpr:
		return e, true, nil

	case *parser.VectorSelector:
		return e, true, nil

	case *parser.Call:
		return e, true, nil

	case *parser.BinaryExpr:
		return pruner.pruneBinOp(e)

	case *parser.SubqueryExpr:
		return e, true, nil

	default:
		return e, false, nil
	}
}

func (pruner *queryPruner) pruneBinOp(expr *parser.BinaryExpr) (mapped parser.Expr, finished bool, err error) {
	switch expr.Op {
	case parser.MUL:
		return pruner.handleMultiplyOp(expr), true, nil
	case parser.DIV:
		return pruner.handleDivideOp(expr), true, nil
	case parser.SUB:
		return pruner.handleSubtractOp(expr), true, nil
	default:
		return expr, false, nil
	}
}

func (pruner *queryPruner) handleMultiplyOp(expr *parser.BinaryExpr) parser.Expr {
	if expr.LHS.String() == "0" {
		return expr.LHS
	}
	if expr.RHS.String() == "0" {
		return expr.RHS
	}
	return expr
}

func (pruner *queryPruner) handleDivideOp(expr *parser.BinaryExpr) parser.Expr {
	if expr.LHS.String() == "0" {
		return expr.LHS
	}
	return expr
}

func (pruner *queryPruner) handleSubtractOp(expr *parser.BinaryExpr) parser.Expr {
	if expr.LHS.String() == expr.RHS.String() {
		return &parser.NumberLiteral{Val: 0}
	}
	return expr
}
