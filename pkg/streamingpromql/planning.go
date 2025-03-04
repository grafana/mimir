// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func (e *Engine) NewQueryPlan(ctx context.Context, qs string, timeRange types.QueryTimeRange) (*planning.QueryPlan, error) {
	expr, err := parser.ParseExpr(qs)
	if err != nil {
		return nil, err
	}

	expr = promql.PreprocessExpr(expr, timestamp.Time(timeRange.StartT), timestamp.Time(timeRange.EndT))

	root, err := e.nodeFromExpr(expr)
	if err != nil {
		return nil, err
	}

	// TODO: apply optimisations

	plan := &planning.QueryPlan{
		TimeRange: timeRange,
		Root:      root,
	}

	return plan, nil
}

func (e *Engine) nodeFromExpr(expr parser.Expr) (planning.Node, error) {
	switch expr := expr.(type) {
	case *parser.VectorSelector:
		return &planning.VectorSelector{
			Matchers:           expr.LabelMatchers,
			Timestamp:          expr.Timestamp,
			Offset:             expr.OriginalOffset,
			ExpressionPosition: expr.PositionRange(),
		}, nil

	case *parser.MatrixSelector:
		vs, ok := expr.VectorSelector.(*parser.VectorSelector)
		if !ok {
			return nil, fmt.Errorf("expected expression for MatrixSelector to be of type VectorSelector, got %T", expr.VectorSelector)
		}

		return &planning.MatrixSelector{
			Matchers:           vs.LabelMatchers,
			Timestamp:          vs.Timestamp,
			Offset:             vs.OriginalOffset,
			Range:              expr.Range,
			ExpressionPosition: expr.PositionRange(),
		}, nil

	case *parser.AggregateExpr:
		inner, err := e.nodeFromExpr(expr.Expr)
		if err != nil {
			return nil, err
		}

		var param planning.Node

		if expr.Param != nil {
			param, err = e.nodeFromExpr(expr.Param)
			if err != nil {
				return nil, err
			}
		}

		return &planning.AggregateExpression{
			Op:                 expr.Op,
			Inner:              inner,
			Param:              param,
			Grouping:           expr.Grouping,
			Without:            expr.Without,
			ExpressionPosition: expr.PositionRange(),
		}, nil

	case *parser.BinaryExpr:
		lhs, err := e.nodeFromExpr(expr.LHS)
		if err != nil {
			return nil, err
		}

		rhs, err := e.nodeFromExpr(expr.RHS)
		if err != nil {
			return nil, err
		}

		return &planning.BinaryExpression{
			Op:             expr.Op,
			LHS:            lhs,
			RHS:            rhs,
			VectorMatching: planning.VectorMatchingFromParserType(expr.VectorMatching),
			ReturnBool:     expr.ReturnBool,
		}, nil

	case *parser.Call:
		args := make([]planning.Node, 0, len(expr.Args))

		for _, arg := range expr.Args {
			node, err := e.nodeFromExpr(arg)
			if err != nil {
				return nil, err
			}

			args = append(args, node)
		}

		return &planning.FunctionCall{
			FunctionName:       expr.Func.Name,
			Args:               args,
			ExpressionPosition: expr.PositionRange(),
		}, nil

	case *parser.SubqueryExpr:
		inner, err := e.nodeFromExpr(expr.Expr)
		if err != nil {
			return nil, err
		}

		step := expr.Step

		if step == 0 {
			step = time.Duration(e.noStepSubqueryIntervalFn(expr.Range.Milliseconds())) * time.Millisecond
		}

		return &planning.Subquery{
			Inner:              inner,
			Timestamp:          expr.Timestamp,
			Offset:             expr.OriginalOffset,
			Range:              expr.Range,
			Step:               step,
			ExpressionPosition: expr.PositionRange(),
		}, nil

	case *parser.UnaryExpr:
		inner, err := e.nodeFromExpr(expr.Expr)
		if err != nil {
			return nil, err
		}

		return &planning.UnaryExpression{
			Op:                 expr.Op,
			Inner:              inner,
			ExpressionPosition: expr.PositionRange(),
		}, nil

	case *parser.NumberLiteral:
		return &planning.NumberLiteral{
			Value:              expr.Val,
			ExpressionPosition: expr.PositionRange(),
		}, nil

	case *parser.StringLiteral:
		return &planning.StringLiteral{
			Value:              expr.Val,
			ExpressionPosition: expr.PositionRange(),
		}, nil

	case *parser.ParenExpr:
		return e.nodeFromExpr(expr.Expr)

	case *parser.StepInvariantExpr:
		// FIXME: make use of the fact the expression is step invariant
		return e.nodeFromExpr(expr.Expr)

	default:
		return nil, fmt.Errorf("unknown expression type: %T", expr)
	}
}

func (e *Engine) EncodeQueryPlan(plan *planning.QueryPlan) ([]byte, error) {
	return e.jsonConfig.Marshal(plan)
}

func (e *Engine) DecodeQueryPlan(data []byte) (*planning.QueryPlan, error) {
	plan := &planning.QueryPlan{}

	if err := e.jsonConfig.Unmarshal(data, plan); err != nil {
		return nil, err
	}

	return plan, nil
}

func (e *Engine) Materialize(ctx context.Context, plan *planning.QueryPlan, q storage.Queryable, opts promql.QueryOpts) (promql.Query, error) {
	// TODO
	panic("TODO")
}
