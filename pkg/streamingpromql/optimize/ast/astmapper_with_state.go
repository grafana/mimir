// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
)

// ExprMapperWithState wraps ExprMapper to include a method for checking if the mapper has changed the expression.
type ExprMapperWithState interface {
	astmapper.ExprMapper
	// HasChanged returns whether the mapper made any changes during mapping
	HasChanged() bool
}

// ASTExprMapperWithState is a wrapper around an ASTExprMapper that stores the original mapper to provide a method to check if any changes were made.
type ASTExprMapperWithState struct {
	mapper    ExprMapperWithState
	astMapper astmapper.ASTExprMapper
}

func NewASTExprMapperWithState(mapper ExprMapperWithState) *ASTExprMapperWithState {
	return &ASTExprMapperWithState{
		mapper:    mapper,
		astMapper: astmapper.NewASTExprMapper(mapper),
	}
}

func (w *ASTExprMapperWithState) Map(ctx context.Context, expr parser.Expr) (parser.Expr, error) {
	return w.astMapper.Map(ctx, expr)
}

func (w *ASTExprMapperWithState) HasChanged() bool {
	return w.mapper.HasChanged()
}
