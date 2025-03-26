// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"context"
	"fmt"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
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
	OnASTStageComplete(stageName string, updatedExpr parser.Expr, duration time.Duration) error
	OnAllASTStagesComplete(finalExpr parser.Expr) error
	OnPlanningStageComplete(stageName string, updatedPlan *planning.QueryPlan, duration time.Duration) error
	OnAllPlanningStagesComplete(finalPlan *planning.QueryPlan) error
}

func (e *Engine) NewQueryPlan(ctx context.Context, qs string, timeRange types.QueryTimeRange, observer PlanningObserver) (*planning.QueryPlan, error) {
	expr, err := runASTStage("Parsing", observer, func() (parser.Expr, error) { return parser.ParseExpr(qs) })
	if err != nil {
		return nil, err
	}

	if !timeRange.IsInstant {
		if expr.Type() != parser.ValueTypeVector && expr.Type() != parser.ValueTypeScalar {
			return nil, fmt.Errorf("query expression produces a %s, but expression for range queries must produce an instant vector or scalar", parser.DocumentedType(expr.Type()))
		}
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

	if err := observer.OnAllASTStagesComplete(expr); err != nil {
		return nil, err
	}

	plan, err := runPlanningStage("Original plan", observer, func() (*planning.QueryPlan, error) {
		root, err := e.nodeFromExpr(expr)
		if err != nil {
			return nil, err
		}

		plan := &planning.QueryPlan{
			TimeRange: timeRange,
			Root:      root,

			OriginalExpression: qs,
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

	if err := observer.OnAllPlanningStagesComplete(plan); err != nil {
		return nil, err
	}

	return plan, err
}

func runASTStage(stageName string, observer PlanningObserver, stage func() (parser.Expr, error)) (parser.Expr, error) {
	start := time.Now()
	expr, err := stage()
	if err != nil {
		return nil, err
	}

	duration := time.Since(start)

	if err := observer.OnASTStageComplete(stageName, expr, duration); err != nil {
		return nil, err
	}

	return expr, nil
}

func runPlanningStage(stageName string, observer PlanningObserver, stage func() (*planning.QueryPlan, error)) (*planning.QueryPlan, error) {
	start := time.Now()
	plan, err := stage()
	if err != nil {
		return nil, err
	}

	duration := time.Since(start)

	if err := observer.OnPlanningStageComplete(stageName, plan, duration); err != nil {
		return nil, err
	}

	return plan, nil
}

func (e *Engine) nodeFromExpr(expr parser.Expr) (planning.Node, error) {
	switch expr := expr.(type) {
	case *parser.VectorSelector:
		return &core.VectorSelector{
			VectorSelectorDetails: &core.VectorSelectorDetails{
				Matchers:           core.LabelMatchersFrom(expr.LabelMatchers),
				Timestamp:          core.TimestampFrom(expr.Timestamp),
				Offset:             expr.OriginalOffset,
				ExpressionPosition: core.PositionRangeFrom(expr.PositionRange()),
			},
		}, nil

	case *parser.MatrixSelector:
		vs, ok := expr.VectorSelector.(*parser.VectorSelector)
		if !ok {
			return nil, fmt.Errorf("expected expression for MatrixSelector to be of type VectorSelector, got %T", expr.VectorSelector)
		}

		return &core.MatrixSelector{
			MatrixSelectorDetails: &core.MatrixSelectorDetails{
				Matchers:           core.LabelMatchersFrom(vs.LabelMatchers),
				Timestamp:          core.TimestampFrom(vs.Timestamp),
				Offset:             vs.OriginalOffset,
				Range:              expr.Range,
				ExpressionPosition: core.PositionRangeFrom(expr.PositionRange()),
			},
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

		op, err := core.AggregationOperationFrom(expr.Op)
		if err != nil {
			return nil, err
		}

		return &core.AggregateExpression{
			Inner: inner,
			Param: param,
			AggregateExpressionDetails: &core.AggregateExpressionDetails{
				Op:                 op,
				Grouping:           expr.Grouping,
				Without:            expr.Without,
				ExpressionPosition: core.PositionRangeFrom(expr.PositionRange()),
			},
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

		op, err := core.BinaryOperationFrom(expr.Op)
		if err != nil {
			return nil, err
		}

		return &core.BinaryExpression{
			LHS: lhs,
			RHS: rhs,
			BinaryExpressionDetails: &core.BinaryExpressionDetails{
				Op:                 op,
				VectorMatching:     core.VectorMatchingFrom(expr.VectorMatching),
				ReturnBool:         expr.ReturnBool,
				ExpressionPosition: core.PositionRangeFrom(expr.PositionRange()),
			},
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

		f := &core.FunctionCall{
			Args: args,
			FunctionCallDetails: &core.FunctionCallDetails{
				FunctionName:       expr.Func.Name,
				ExpressionPosition: core.PositionRangeFrom(expr.PositionRange()),
			},
		}

		if expr.Func.Name == "absent" || expr.Func.Name == "absent_over_time" {
			f.AbsentLabels = mimirpb.FromLabelsToLabelAdapters(functions.CreateLabelsForAbsentFunction(expr.Args[0]))
		}

		return f, nil

	case *parser.SubqueryExpr:
		inner, err := e.nodeFromExpr(expr.Expr)
		if err != nil {
			return nil, err
		}

		step := expr.Step

		if step == 0 {
			step = time.Duration(e.noStepSubqueryIntervalFn(expr.Range.Milliseconds())) * time.Millisecond
		}

		return &core.Subquery{
			Inner: inner,
			SubqueryDetails: &core.SubqueryDetails{
				Timestamp:          core.TimestampFrom(expr.Timestamp),
				Offset:             expr.OriginalOffset,
				Range:              expr.Range,
				Step:               step,
				ExpressionPosition: core.PositionRangeFrom(expr.PositionRange()),
			},
		}, nil

	case *parser.UnaryExpr:
		inner, err := e.nodeFromExpr(expr.Expr)
		if err != nil {
			return nil, err
		}

		if expr.Op == parser.ADD {
			// +(anything) is the same as (anything), so don't bother creating a unary expression node.
			return inner, nil
		}

		op, err := core.UnaryOperationFrom(expr.Op)
		if err != nil {
			return nil, err
		}

		return &core.UnaryExpression{
			Inner: inner,
			UnaryExpressionDetails: &core.UnaryExpressionDetails{
				Op:                 op,
				ExpressionPosition: core.PositionRangeFrom(expr.PositionRange()),
			},
		}, nil

	case *parser.NumberLiteral:
		return &core.NumberLiteral{
			NumberLiteralDetails: &core.NumberLiteralDetails{
				Value:              expr.Val,
				ExpressionPosition: core.PositionRangeFrom(expr.PositionRange()),
			},
		}, nil

	case *parser.StringLiteral:
		return &core.StringLiteral{
			StringLiteralDetails: &core.StringLiteralDetails{
				Value:              expr.Val,
				ExpressionPosition: core.PositionRangeFrom(expr.PositionRange()),
			},
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

// Materialize converts a query plan into an executable query.
func (e *Engine) Materialize(ctx context.Context, plan *planning.QueryPlan, queryable storage.Queryable, opts promql.QueryOpts) (promql.Query, error) {
	if opts == nil {
		opts = promql.NewPrometheusQueryOpts(false, 0)
	}

	q, err := e.newQuery(ctx, queryable, opts, plan.TimeRange)
	if err != nil {
		return nil, err
	}

	q.operatorFactories = make(map[planning.Node]planning.OperatorFactory)
	q.operatorParams = &planning.OperatorParameters{
		Queryable:                q.queryable,
		MemoryConsumptionTracker: q.memoryConsumptionTracker,
		Annotations:              q.annotations,
		Stats:                    q.stats,
		LookbackDelta:            q.lookbackDelta,
	}

	// HACK: we need an expression to use in the active query tracker, but there's no guarantee the plan we're working with
	// is for the original expression (the plan may represent a subexpression of the original plan, for example).
	// This is good enough for now, but something to revisit later - perhaps we can use some kind of request ID?
	q.originalExpression = plan.OriginalExpression

	q.statement = &parser.EvalStmt{
		Expr:          nil, // Nothing seems to use this, and we don't have a good expression to use here anyway, so don't bother setting this.
		Start:         timestamp.Time(plan.TimeRange.StartT),
		End:           timestamp.Time(plan.TimeRange.EndT),
		Interval:      time.Duration(plan.TimeRange.IntervalMilliseconds) * time.Millisecond,
		LookbackDelta: q.lookbackDelta,
	}

	if plan.TimeRange.IsInstant {
		q.statement.Interval = 0
	}

	q.root, err = q.convertNodeToOperator(plan.Root, plan.TimeRange)
	if err != nil {
		return nil, err
	}

	return q, nil
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

func (n NoopPlanningObserver) OnASTStageComplete(stageName string, updatedExpr parser.Expr, duration time.Duration) error {
	// Nothing to do.
	return nil
}

func (n NoopPlanningObserver) OnAllASTStagesComplete(finalExpr parser.Expr) error {
	// Nothing to do.
	return nil
}

func (n NoopPlanningObserver) OnPlanningStageComplete(stageName string, updatedPlan *planning.QueryPlan, duration time.Duration) error {
	// Nothing to do.
	return nil
}

func (n NoopPlanningObserver) OnAllPlanningStagesComplete(finalPlan *planning.QueryPlan) error {
	// Nothing to do.
	return nil
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

func (o *AnalysisPlanningObserver) OnASTStageComplete(stageName string, updatedExpr parser.Expr, duration time.Duration) error {
	o.Result.ASTStages = append(o.Result.ASTStages, ASTStage{
		Name:             stageName,
		Duration:         &duration,
		OutputExpression: updatedExpr.Pretty(0),
	})

	return nil
}

func (o *AnalysisPlanningObserver) OnAllASTStagesComplete(finalExpr parser.Expr) error {
	o.Result.ASTStages = append(o.Result.ASTStages, ASTStage{
		Name:             "Final expression",
		OutputExpression: finalExpr.Pretty(0),
	})

	return nil
}

func (o *AnalysisPlanningObserver) OnPlanningStageComplete(stageName string, updatedPlan *planning.QueryPlan, duration time.Duration) error {
	plan, err := updatedPlan.ToEncodedPlan(true, false)
	if err != nil {
		return err
	}

	planBytes, err := jsoniter.Marshal(plan)
	if err != nil {
		return err
	}

	o.Result.PlanningStages = append(o.Result.PlanningStages, PlanningStage{
		Name:       stageName,
		Duration:   &duration,
		OutputPlan: planBytes,
	})

	return nil
}

func (o *AnalysisPlanningObserver) OnAllPlanningStagesComplete(finalPlan *planning.QueryPlan) error {
	plan, err := finalPlan.ToEncodedPlan(true, false)
	if err != nil {
		return err
	}

	planBytes, err := jsoniter.Marshal(plan)
	if err != nil {
		return err
	}

	o.Result.PlanningStages = append(o.Result.PlanningStages, PlanningStage{
		Name:       "Final plan",
		OutputPlan: planBytes,
	})

	return nil
}
