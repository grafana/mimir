// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/astmapper.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"context"
	"fmt"
	"slices"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/util/promqlext"
)

func init() {
	promqlext.ExtendPromQL()
}

// ASTMapper is the exported interface for mapping between multiple AST representations
type ASTMapper interface {
	// Map the input expr and returns the mapped expr.
	Map(ctx context.Context, expr parser.Expr) (mapped parser.Expr, err error)
}

// MultiMapper can compose multiple ASTMappers
type MultiMapper struct {
	mappers []ASTMapper
}

// Map implements ASTMapper
func (m *MultiMapper) Map(ctx context.Context, expr parser.Expr) (parser.Expr, error) {
	var result = expr
	var err error

	if len(m.mappers) == 0 {
		return nil, errors.New("MultiMapper: No mappers registered")
	}

	for _, x := range m.mappers {
		result, err = x.Map(ctx, result)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// Register adds ASTMappers into a multimapper.
// Since registered functions are applied in the order they're registered, it's advised to register them
// in decreasing priority and only operate on exprs that each function cares about, defaulting to cloneExpr.
func (m *MultiMapper) Register(xs ...ASTMapper) {
	m.mappers = append(m.mappers, xs...)
}

// NewMultiMapper instantiates an ASTMapper from multiple ASTMappers.
func NewMultiMapper(xs ...ASTMapper) *MultiMapper {
	m := &MultiMapper{}
	m.Register(xs...)
	return m
}

// CloneExpr is a helper function to clone an expression.
func CloneExpr(expr parser.Expr) (parser.Expr, error) {
	switch e := expr.(type) {
	case nil:
		return nil, nil

	case *parser.BinaryExpr:
		lhs, err := CloneExpr(e.LHS)
		if err != nil {
			return nil, err
		}

		rhs, err := CloneExpr(e.RHS)
		if err != nil {
			return nil, err
		}

		var vectorMatching *parser.VectorMatching

		if e.VectorMatching != nil {
			vectorMatching = &parser.VectorMatching{
				Card:           e.VectorMatching.Card,
				MatchingLabels: slices.Clone(e.VectorMatching.MatchingLabels),
				On:             e.VectorMatching.On,
				Include:        slices.Clone(e.VectorMatching.Include),
			}
		}

		return &parser.BinaryExpr{
			LHS:            lhs,
			RHS:            rhs,
			Op:             e.Op,
			ReturnBool:     e.ReturnBool,
			VectorMatching: vectorMatching,
		}, nil

	case *parser.Call:
		args := make([]parser.Expr, 0, len(e.Args))
		for _, arg := range e.Args {
			cloned, err := CloneExpr(arg)
			if err != nil {
				return nil, err
			}
			args = append(args, cloned)
		}

		return &parser.Call{
			Func:     e.Func, // No need to clone this, this points to information about the called function shared by all queries.
			Args:     args,
			PosRange: e.PosRange,
		}, nil

	case *parser.VectorSelector:
		matchers := make([]*labels.Matcher, 0, len(e.LabelMatchers))

		for _, matcher := range e.LabelMatchers {
			cloned, err := labels.NewMatcher(matcher.Type, matcher.Name, matcher.Value)
			if err != nil {
				return nil, err
			}

			matchers = append(matchers, cloned)
		}

		offsetExpr, err := cloneDurationExpr(e.OriginalOffsetExpr)
		if err != nil {
			return nil, err
		}

		return &parser.VectorSelector{
			Name:                    e.Name,
			OriginalOffset:          e.OriginalOffset,
			OriginalOffsetExpr:      offsetExpr,
			Offset:                  e.Offset,
			Timestamp:               cloneTimestamp(e.Timestamp),
			StartOrEnd:              e.StartOrEnd,
			LabelMatchers:           matchers,
			BypassEmptyMatcherCheck: e.BypassEmptyMatcherCheck,
			PosRange:                e.PosRange,
			Anchored:                e.Anchored,
			Smoothed:                e.Smoothed,
		}, nil

	case *parser.MatrixSelector:
		vs, err := CloneExpr(e.VectorSelector)
		if err != nil {
			return nil, err
		}

		rangeExpr, err := cloneDurationExpr(e.RangeExpr)
		if err != nil {
			return nil, err
		}

		return &parser.MatrixSelector{
			VectorSelector: vs,
			Range:          e.Range,
			RangeExpr:      rangeExpr,
			EndPos:         e.EndPos,
		}, nil

	case *parser.SubqueryExpr:
		expr, err := CloneExpr(e.Expr)
		if err != nil {
			return nil, err
		}

		rangeExpr, err := cloneDurationExpr(e.RangeExpr)
		if err != nil {
			return nil, err
		}

		originalOffsetExpr, err := cloneDurationExpr(e.OriginalOffsetExpr)
		if err != nil {
			return nil, err
		}

		stepExpr, err := cloneDurationExpr(e.StepExpr)
		if err != nil {
			return nil, err
		}

		return &parser.SubqueryExpr{
			Expr:               expr,
			Range:              e.Range,
			RangeExpr:          rangeExpr,
			OriginalOffset:     e.OriginalOffset,
			OriginalOffsetExpr: originalOffsetExpr,
			Offset:             e.Offset,
			Timestamp:          cloneTimestamp(e.Timestamp),
			StartOrEnd:         e.StartOrEnd,
			Step:               e.Step,
			StepExpr:           stepExpr,
			EndPos:             e.EndPos,
		}, nil

	case *parser.AggregateExpr:
		expr, err := CloneExpr(e.Expr)
		if err != nil {
			return nil, err
		}

		param, err := CloneExpr(e.Param)
		if err != nil {
			return nil, err
		}

		return &parser.AggregateExpr{
			Op:       e.Op,
			Expr:     expr,
			Param:    param,
			Grouping: slices.Clone(e.Grouping),
			Without:  e.Without,
			PosRange: e.PosRange,
		}, nil

	case *parser.NumberLiteral:
		return &parser.NumberLiteral{
			Val:      e.Val,
			Duration: e.Duration,
			PosRange: e.PosRange,
		}, nil

	case *parser.StringLiteral:
		return &parser.StringLiteral{
			Val:      e.Val,
			PosRange: e.PosRange,
		}, nil

	case *parser.UnaryExpr:
		expr, err := CloneExpr(e.Expr)
		if err != nil {
			return nil, err
		}

		return &parser.UnaryExpr{
			Op:       e.Op,
			Expr:     expr,
			StartPos: e.StartPos,
		}, nil

	case *parser.ParenExpr:
		expr, err := CloneExpr(e.Expr)
		if err != nil {
			return nil, err
		}

		return &parser.ParenExpr{
			Expr:     expr,
			PosRange: e.PosRange,
		}, nil

	case *parser.StepInvariantExpr:
		expr, err := CloneExpr(e.Expr)
		if err != nil {
			return nil, err
		}

		return &parser.StepInvariantExpr{
			Expr: expr,
		}, nil

	case *parser.DurationExpr:
		return cloneDurationExpr(e)

	default:
		return nil, fmt.Errorf("cloneExpr: unknown expression type %T", expr)
	}
}

func cloneDurationExpr(expr *parser.DurationExpr) (*parser.DurationExpr, error) {
	if expr == nil {
		return nil, nil
	}

	lhs, err := CloneExpr(expr.LHS)
	if err != nil {
		return nil, err
	}

	rhs, err := CloneExpr(expr.RHS)
	if err != nil {
		return nil, err
	}

	return &parser.DurationExpr{
		Op:       expr.Op,
		LHS:      lhs,
		RHS:      rhs,
		Wrapped:  expr.Wrapped,
		StartPos: expr.StartPos,
		EndPos:   expr.EndPos,
	}, nil
}

func cloneTimestamp(ts *int64) *int64 {
	if ts == nil {
		return nil
	}

	cloned := *ts
	return &cloned
}

func cloneAndMap(ctx context.Context, mapper ASTExprMapper, expr parser.Expr) (parser.Expr, error) {
	cloned, err := CloneExpr(expr)
	if err != nil {
		return nil, err
	}
	return mapper.Map(ctx, cloned)
}

type ExprMapper interface {
	// MapExpr either maps a single AST expr or returns the unaltered expr.
	// It returns a finished bool to signal whether no further recursion is necessary.
	MapExpr(ctx context.Context, expr parser.Expr) (mapped parser.Expr, finished bool, err error)
}

// NewASTExprMapper creates an ASTMapper from a ExprMapper
func NewASTExprMapper(mapper ExprMapper) ASTExprMapper {
	return ASTExprMapper{mapper}
}

// ASTExprMapper is an ASTMapper adapter which uses a ExprMapper internally.
type ASTExprMapper struct {
	ExprMapper
}

// Map implements ASTMapper from a ExprMapper
func (em ASTExprMapper) Map(ctx context.Context, expr parser.Expr) (parser.Expr, error) {
	// Check for context cancellation before mapping expressions.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	expr, finished, err := em.MapExpr(ctx, expr)
	if err != nil {
		return nil, err
	}

	if finished {
		return expr, nil
	}

	switch e := expr.(type) {
	case nil:
		// nil handles cases where we check optional fields that are not set
		return nil, nil

	case *parser.AggregateExpr:
		expr, err := em.Map(ctx, e.Expr)
		if err != nil {
			return nil, err
		}
		e.Expr = expr

		if e.Param != nil {
			param, err := em.Map(ctx, e.Param)
			if err != nil {
				return nil, err
			}
			e.Param = param
		}

		return e, nil

	case *parser.BinaryExpr:
		lhs, err := em.Map(ctx, e.LHS)
		if err != nil {
			return nil, err
		}
		e.LHS = lhs

		rhs, err := em.Map(ctx, e.RHS)
		if err != nil {
			return nil, err
		}
		e.RHS = rhs

		return e, nil

	case *parser.Call:
		for i, arg := range e.Args {
			mapped, err := em.Map(ctx, arg)
			if err != nil {
				return nil, err
			}
			e.Args[i] = mapped
		}
		return e, nil

	case *parser.SubqueryExpr:
		mapped, err := em.Map(ctx, e.Expr)
		if err != nil {
			return nil, err
		}
		e.Expr = mapped
		return e, nil

	case *parser.ParenExpr:
		mapped, err := em.Map(ctx, e.Expr)
		if err != nil {
			return nil, err
		}
		e.Expr = mapped
		return e, nil

	case *parser.UnaryExpr:
		mapped, err := em.Map(ctx, e.Expr)
		if err != nil {
			return nil, err
		}
		e.Expr = mapped
		return e, nil

	case *parser.StepInvariantExpr:
		mapped, err := em.Map(ctx, e.Expr)
		if err != nil {
			return nil, err
		}
		e.Expr = mapped
		return e, nil

	case *parser.MatrixSelector:
		mapped, err := em.Map(ctx, e.VectorSelector)
		if err != nil {
			return nil, err
		}
		e.VectorSelector = mapped
		return e, nil

	case *parser.NumberLiteral, *parser.StringLiteral, *parser.VectorSelector:
		return e, nil

	default:
		return nil, errors.Errorf("ASTExprMapper: unhandled expr type %T", expr)
	}
}

// ExprMapperWithState wraps ExprMapper to include a method for checking if the mapper has changed the expression.
type ExprMapperWithState interface {
	ExprMapper
	// HasChanged returns whether the mapper made any changes during mapping
	HasChanged() bool
	Stats() (int, int, int)
}

// ASTExprMapperWithState is a wrapper around an ASTExprMapper that stores the original mapper to provide a method to check if any changes were made.
type ASTExprMapperWithState struct {
	mapper    ExprMapperWithState
	astMapper ASTExprMapper
}

func NewASTExprMapperWithState(mapper ExprMapperWithState) *ASTExprMapperWithState {
	return &ASTExprMapperWithState{
		mapper:    mapper,
		astMapper: NewASTExprMapper(mapper),
	}
}

func (w *ASTExprMapperWithState) Map(ctx context.Context, expr parser.Expr) (parser.Expr, error) {
	return w.astMapper.Map(ctx, expr)
}

func (w *ASTExprMapperWithState) HasChanged() bool {
	return w.mapper.HasChanged()
}

func (w *ASTExprMapperWithState) Stats() (int, int, int) {
	return w.mapper.Stats()
}
