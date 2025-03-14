// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"context"
	"fmt"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type PlanningObserver interface {
	OnASTStageComplete(stageName string, updatedExpr parser.Expr, duration time.Duration)
	OnAllASTStagesComplete(finalExpr parser.Expr)
	OnPlanningStageComplete(stageName string, updatedPlan *planning.QueryPlan, duration time.Duration)
	OnAllPlanningStagesComplete(finalPlan *planning.QueryPlan)
}

// TODO: timeout
func (e *Engine) NewQueryPlan(ctx context.Context, qs string, timeRange types.QueryTimeRange, observer PlanningObserver) (*planning.QueryPlan, error) {
	expr, err := runASTStage("Parsing", observer, func() (parser.Expr, error) { return parser.ParseExpr(qs) })
	if err != nil {
		return nil, err
	}

	expr, err = runASTStage("Pre-processing", observer, func() (parser.Expr, error) {
		return promql.PreprocessExpr(expr, timestamp.Time(timeRange.StartT), timestamp.Time(timeRange.EndT)), nil
	})

	if err != nil {
		return nil, err
	}

	for _, o := range e.astOptimizers {
		expr, err = runASTStage(o.Name(), observer, func() (parser.Expr, error) { return o.Apply(ctx, expr) })

		if err != nil {
			return nil, err
		}
	}

	observer.OnAllASTStagesComplete(expr)

	plan, err := runPlanningStage("Original plan", observer, func() (*planning.QueryPlan, error) {
		root, err := e.nodeFromExpr(expr)
		if err != nil {
			return nil, err
		}

		plan := &planning.QueryPlan{
			TimeRange: timeRange,
			Root:      root,
		}

		return plan, nil
	})

	if err != nil {
		return nil, err
	}

	for _, o := range e.planOptimizers {
		plan, err = runPlanningStage(o.Name(), observer, func() (*planning.QueryPlan, error) { return o.Apply(ctx, plan) })

		if err != nil {
			return nil, err
		}
	}

	observer.OnAllPlanningStagesComplete(plan)

	return plan, err
}

func runASTStage(stageName string, observer PlanningObserver, stage func() (parser.Expr, error)) (parser.Expr, error) {
	start := time.Now()
	expr, err := stage()
	if err != nil {
		return nil, err
	}

	duration := time.Since(start)
	observer.OnASTStageComplete(stageName, expr, duration)

	return expr, nil
}

func runPlanningStage(stageName string, observer PlanningObserver, stage func() (*planning.QueryPlan, error)) (*planning.QueryPlan, error) {
	start := time.Now()
	plan, err := stage()
	if err != nil {
		return nil, err
	}

	duration := time.Since(start)
	observer.OnPlanningStageComplete(stageName, plan, duration)

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

// Materialize converts a query plan into an executable query.
func (e *Engine) Materialize(ctx context.Context, plan *planning.QueryPlan, q storage.Queryable, opts promql.QueryOpts) (promql.Query, error) {
	// TODO
	panic("TODO")
}

type AnalysisResult struct {
	OriginalExpression string               `json:"originalExpression"`
	TimeRange          types.QueryTimeRange `json:"timeRange"`

	ASTStages      []ASTStage      `json:"astStages"`
	PlanningStages []PlanningStage `json:"planningStages"`
}

type ASTStage struct {
	Name             string         `json:"name"`
	Duration         *time.Duration `json:"duration"` // nil if this stage has no associated duration (eg. represents final AST)
	OutputExpression string         `json:"outputExpression"`
}

type PlanningStage struct {
	Name       string              `json:"name"`
	Duration   *time.Duration      `json:"duration"`   // nil if this stage has no associated duration (eg. represents final plan)
	OutputPlan jsoniter.RawMessage `json:"outputPlan"` // Store the encoded JSON so we don't have to deal with cloning the entire query plan each time.
}

// Analyze performs query planning and produces a report on the query planning process.
func (e *Engine) Analyze(ctx context.Context, qs string, timeRange types.QueryTimeRange) (*AnalysisResult, error) {
	observer := NewAnalysisPlanningObserver(qs, timeRange, e)
	_, err := e.NewQueryPlan(ctx, qs, timeRange, observer)
	if err != nil {
		return nil, err
	}

	return observer.Result, nil
}

type NoopPlanningObserver struct{}

func (n NoopPlanningObserver) OnASTStageComplete(stageName string, updatedExpr parser.Expr, duration time.Duration) {
	// Nothing to do.
}

func (n NoopPlanningObserver) OnAllASTStagesComplete(finalExpr parser.Expr) {
	// Nothing to do.
}

func (n NoopPlanningObserver) OnPlanningStageComplete(stageName string, updatedPlan *planning.QueryPlan, duration time.Duration) {
	// Nothing to do.
}

func (n NoopPlanningObserver) OnAllPlanningStagesComplete(finalPlan *planning.QueryPlan) {
	// Nothing to do.
}

type AnalysisPlanningObserver struct {
	Result *AnalysisResult
	Engine *Engine
}

func NewAnalysisPlanningObserver(expr string, timeRange types.QueryTimeRange, engine *Engine) *AnalysisPlanningObserver {
	return &AnalysisPlanningObserver{
		Result: &AnalysisResult{
			OriginalExpression: expr,
			TimeRange:          timeRange,
		},
		Engine: engine,
	}
}

func (o *AnalysisPlanningObserver) OnASTStageComplete(stageName string, updatedExpr parser.Expr, duration time.Duration) {
	o.Result.ASTStages = append(o.Result.ASTStages, ASTStage{
		Name:             stageName,
		Duration:         &duration,
		OutputExpression: updatedExpr.Pretty(0),
	})
}

func (o *AnalysisPlanningObserver) OnAllASTStagesComplete(finalExpr parser.Expr) {
	o.Result.ASTStages = append(o.Result.ASTStages, ASTStage{
		Name:             "Final expression",
		OutputExpression: finalExpr.Pretty(0),
	})
}

func (o *AnalysisPlanningObserver) OnPlanningStageComplete(stageName string, updatedPlan *planning.QueryPlan, duration time.Duration) {
	plan, _ := o.Engine.EncodeQueryPlan(updatedPlan) // TODO: what to do if encoding fails?

	o.Result.PlanningStages = append(o.Result.PlanningStages, PlanningStage{
		Name:       stageName,
		Duration:   &duration,
		OutputPlan: plan,
	})
}

func (o *AnalysisPlanningObserver) OnAllPlanningStagesComplete(finalPlan *planning.QueryPlan) {
	plan, _ := o.Engine.EncodeQueryPlan(finalPlan) // TODO: what to do if encoding fails?

	o.Result.PlanningStages = append(o.Result.PlanningStages, PlanningStage{
		Name:       "Final plan",
		OutputPlan: plan,
	})
}
