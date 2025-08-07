// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/engineopts"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/commonsubexpressionelimination"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// Replaced during testing to ensure timing produces consistent results.
var timeSince = time.Since

type QueryPlanner struct {
	activeQueryTracker       promql.QueryTracker
	noStepSubqueryIntervalFn func(rangeMillis int64) int64
	astOptimizationPasses    []optimize.ASTOptimizationPass
	planOptimizationPasses   []optimize.QueryPlanOptimizationPass
	planStageLatency         *prometheus.HistogramVec
}

func NewQueryPlanner(opts engineopts.EngineOpts) *QueryPlanner {
	planner := NewQueryPlannerWithoutOptimizationPasses(opts)

	// FIXME: it makes sense to register these common optimization passes here, but we'll likely need to rework this once
	// we introduce query-frontend-specific optimization passes like sharding and splitting for two reasons:
	//  1. We want to avoid a circular dependency between this package and the query-frontend package where most of the logic for these optimization passes lives.
	//  2. We don't want to register these optimization passes in queriers.
	planner.RegisterASTOptimizationPass(&ast.CollapseConstants{}) // We expect this to be the first to simplify the logic for the rest of the optimization passes.
	if opts.EnablePruneToggles {
		planner.RegisterASTOptimizationPass(ast.NewPruneToggles()) // Do this next to ensure that toggled off expressions are removed before the other optimization passes are applied.
	}
	planner.RegisterASTOptimizationPass(&ast.SortLabelsAndMatchers{}) // This is a prerequisite for other optimization passes such as common subexpression elimination.
	// The following is applied in query pruning middleware for now as we need to run them before query sharding, but we leave most of the code here as we plan to move them here later. Note that these do not track the changed state properly.
	// if opts.EnablePropagateMatchers {
	// 	planner.RegisterASTOptimizationPass(ast.NewPropagateMatchers())
	// }

	if opts.EnableCommonSubexpressionElimination {
		planner.RegisterQueryPlanOptimizationPass(commonsubexpressionelimination.NewOptimizationPass(opts.EnableCommonSubexpressionEliminationForRangeVectorExpressionsInInstantQueries, opts.CommonOpts.Reg))
	}

	if opts.EnableSkippingHistogramDecoding {
		// This optimization pass must be registered after common subexpression elimination, if that is enabled.
		planner.RegisterQueryPlanOptimizationPass(plan.NewSkipHistogramDecodingOptimizationPass())
	}

	return planner
}

// NewQueryPlannerWithoutOptimizationPasses creates a new query planner without any optimization passes registered.
//
// This is intended for use in tests only.
func NewQueryPlannerWithoutOptimizationPasses(opts engineopts.EngineOpts) *QueryPlanner {
	activeQueryTracker := opts.CommonOpts.ActiveQueryTracker
	if activeQueryTracker == nil {
		activeQueryTracker = &NoopQueryTracker{}
	}

	return &QueryPlanner{
		activeQueryTracker:       activeQueryTracker,
		noStepSubqueryIntervalFn: opts.CommonOpts.NoStepSubqueryIntervalFn,
		planStageLatency: promauto.With(opts.CommonOpts.Reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                        "cortex_mimir_query_engine_plan_stage_latency_seconds",
			Help:                        "Latency of each stage of the query planning process.",
			NativeHistogramBucketFactor: 1.1,
		}, []string{"stage_type", "stage"}),
	}
}

// RegisterASTOptimizationPass registers an AST optimization pass used with this engine.
//
// This method is not thread-safe and must not be called concurrently with any other method on this type.
func (p *QueryPlanner) RegisterASTOptimizationPass(o optimize.ASTOptimizationPass) {
	p.astOptimizationPasses = append(p.astOptimizationPasses, o)
}

// RegisterQueryPlanOptimizationPass registers a query plan optimization pass used with this engine.
//
// This method is not thread-safe and must not be called concurrently with any other method on this type.
func (p *QueryPlanner) RegisterQueryPlanOptimizationPass(o optimize.QueryPlanOptimizationPass) {
	p.planOptimizationPasses = append(p.planOptimizationPasses, o)
}

type PlanningObserver interface {
	OnASTStageComplete(stageName string, updatedExpr parser.Expr, duration time.Duration) error
	OnAllASTStagesComplete(finalExpr parser.Expr) error
	OnPlanningStageComplete(stageName string, updatedPlan *planning.QueryPlan, duration time.Duration) error
	OnAllPlanningStagesComplete(finalPlan *planning.QueryPlan) error
}

func (p *QueryPlanner) NewQueryPlan(ctx context.Context, qs string, timeRange types.QueryTimeRange, observer PlanningObserver) (*planning.QueryPlan, error) {
	queryID, err := p.activeQueryTracker.Insert(ctx, qs+" # (planning)")
	if err != nil {
		return nil, err
	}

	defer p.activeQueryTracker.Delete(queryID)

	expr, err := p.runASTStage("Parsing", observer, func() (parser.Expr, error) { return parser.ParseExpr(qs) })
	if err != nil {
		return nil, err
	}

	if !timeRange.IsInstant {
		if expr.Type() != parser.ValueTypeVector && expr.Type() != parser.ValueTypeScalar {
			return nil, fmt.Errorf("query expression produces a %s, but expression for range queries must produce an instant vector or scalar", parser.DocumentedType(expr.Type()))
		}
	}

	expr, err = p.runASTStage("Pre-processing", observer, func() (parser.Expr, error) {
		step := time.Duration(timeRange.IntervalMilliseconds) * time.Millisecond

		if timeRange.IsInstant {
			// timeRange.IntervalMilliseconds is 1 for instant queries, but we need to pass 0 for instant queries to PreprocessExpr.
			step = 0
		}

		return promql.PreprocessExpr(expr, timestamp.Time(timeRange.StartT), timestamp.Time(timeRange.EndT), step)
	})

	if err != nil {
		return nil, err
	}

	for _, o := range p.astOptimizationPasses {
		expr, err = p.runASTStage(o.Name(), observer, func() (parser.Expr, error) { return o.Apply(ctx, expr) })

		if err != nil {
			return nil, err
		}
	}

	if err := observer.OnAllASTStagesComplete(expr); err != nil {
		return nil, err
	}

	plan, err := p.runPlanningStage("Original plan", observer, func() (*planning.QueryPlan, error) {
		root, err := p.nodeFromExpr(expr)
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

	for _, o := range p.planOptimizationPasses {
		plan, err = p.runPlanningStage(o.Name(), observer, func() (*planning.QueryPlan, error) { return o.Apply(ctx, plan) })

		if err != nil {
			return nil, err
		}
	}

	if err := observer.OnAllPlanningStagesComplete(plan); err != nil {
		return nil, err
	}

	return plan, err
}

func (p *QueryPlanner) runASTStage(stageName string, observer PlanningObserver, stage func() (parser.Expr, error)) (parser.Expr, error) {
	start := time.Now()
	expr, err := stage()
	if err != nil {
		return nil, err
	}

	duration := timeSince(start)
	p.planStageLatency.WithLabelValues("AST", stageName).Observe(duration.Seconds())

	if err := observer.OnASTStageComplete(stageName, expr, duration); err != nil {
		return nil, err
	}

	return expr, nil
}

func (p *QueryPlanner) runPlanningStage(stageName string, observer PlanningObserver, stage func() (*planning.QueryPlan, error)) (*planning.QueryPlan, error) {
	start := time.Now()
	plan, err := stage()
	if err != nil {
		return nil, err
	}

	duration := timeSince(start)
	p.planStageLatency.WithLabelValues("Plan", stageName).Observe(duration.Seconds())

	if err := observer.OnPlanningStageComplete(stageName, plan, duration); err != nil {
		return nil, err
	}

	return plan, nil
}

func (p *QueryPlanner) nodeFromExpr(expr parser.Expr) (planning.Node, error) {
	switch expr := expr.(type) {
	case *parser.VectorSelector:
		return &core.VectorSelector{
			VectorSelectorDetails: &core.VectorSelectorDetails{
				Matchers:           core.LabelMatchersFrom(expr.LabelMatchers),
				Timestamp:          core.TimeFromTimestamp(expr.Timestamp),
				Offset:             expr.OriginalOffset,
				ExpressionPosition: core.PositionRangeFrom(expr.PositionRange()),
				// Note that we deliberately do not propagate SkipHistogramBuckets from the expression here.
				// This is done in the skip histogram buckets optimization pass, after common subexpression elimination is applied,
				// to simplify the logic in the common subexpression elimination optimization pass. Otherwise it would have to deal
				// with merging selectors that can and can't skip histogram buckets.
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
				Timestamp:          core.TimeFromTimestamp(vs.Timestamp),
				Offset:             vs.OriginalOffset,
				Range:              expr.Range,
				ExpressionPosition: core.PositionRangeFrom(expr.PositionRange()),
				// Note that we deliberately do not propagate SkipHistogramBuckets from the expression here. See the explanation above.
			},
		}, nil

	case *parser.AggregateExpr:
		inner, err := p.nodeFromExpr(expr.Expr)
		if err != nil {
			return nil, err
		}

		var param planning.Node

		if expr.Param != nil {
			param, err = p.nodeFromExpr(expr.Param)
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
		lhs, err := p.nodeFromExpr(expr.LHS)
		if err != nil {
			return nil, err
		}

		rhs, err := p.nodeFromExpr(expr.RHS)
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
		fnc, ok := findFunction(expr.Func.Name)

		if !ok {
			return nil, compat.NewNotSupportedError(fmt.Sprintf("'%s' function", expr.Func.Name))
		}

		args := make([]planning.Node, 0, len(expr.Args))

		for _, arg := range expr.Args {
			node, err := p.nodeFromExpr(arg)
			if err != nil {
				return nil, err
			}

			args = append(args, node)
		}

		f := &core.FunctionCall{
			Args: args,
			FunctionCallDetails: &core.FunctionCallDetails{
				Function:           fnc,
				ExpressionPosition: core.PositionRangeFrom(expr.PositionRange()),
			},
		}

		switch fnc {
		case functions.FUNCTION_ABSENT, functions.FUNCTION_ABSENT_OVER_TIME:
			f.AbsentLabels = mimirpb.FromLabelsToLabelAdapters(functions.CreateLabelsForAbsentFunction(expr.Args[0]))
		case functions.FUNCTION_TIMESTAMP:
			vs, isVectorSelector := args[0].(*core.VectorSelector)
			if isVectorSelector {
				vs.VectorSelectorDetails.ReturnSampleTimestamps = true
			}
		}

		return f, nil

	case *parser.SubqueryExpr:
		inner, err := p.nodeFromExpr(expr.Expr)
		if err != nil {
			return nil, err
		}

		step := expr.Step

		if step == 0 {
			step = time.Duration(p.noStepSubqueryIntervalFn(expr.Range.Milliseconds())) * time.Millisecond
		}

		return &core.Subquery{
			Inner: inner,
			SubqueryDetails: &core.SubqueryDetails{
				Timestamp:          core.TimeFromTimestamp(expr.Timestamp),
				Offset:             expr.OriginalOffset,
				Range:              expr.Range,
				Step:               step,
				ExpressionPosition: core.PositionRangeFrom(expr.PositionRange()),
			},
		}, nil

	case *parser.UnaryExpr:
		inner, err := p.nodeFromExpr(expr.Expr)
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
		return p.nodeFromExpr(expr.Expr)

	case *parser.StepInvariantExpr:
		// FIXME: make use of the fact the expression is step invariant
		return p.nodeFromExpr(expr.Expr)

	default:
		return nil, fmt.Errorf("unknown expression type: %T", expr)
	}
}

func findFunction(name string) (functions.Function, bool) {
	f, ok := functions.FromPromQLName(name)
	if !ok {
		return functions.FUNCTION_UNKNOWN, false
	}

	if _, ok := functions.RegisteredFunctions[f]; ok {
		return f, true
	}

	return functions.FUNCTION_UNKNOWN, false
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
func (p *QueryPlanner) Analyze(ctx context.Context, qs string, timeRange types.QueryTimeRange) (*AnalysisResult, error) {
	observer := NewAnalysisPlanningObserver(qs, timeRange)
	_, err := p.NewQueryPlan(ctx, qs, timeRange, observer)
	if err != nil {
		return nil, err
	}

	return observer.Result, nil
}

type NoopPlanningObserver struct{}

func (n NoopPlanningObserver) OnASTStageComplete(string, parser.Expr, time.Duration) error {
	// Nothing to do.
	return nil
}

func (n NoopPlanningObserver) OnAllASTStagesComplete(parser.Expr) error {
	// Nothing to do.
	return nil
}

func (n NoopPlanningObserver) OnPlanningStageComplete(string, *planning.QueryPlan, time.Duration) error {
	// Nothing to do.
	return nil
}

func (n NoopPlanningObserver) OnAllPlanningStagesComplete(*planning.QueryPlan) error {
	// Nothing to do.
	return nil
}

type AnalysisPlanningObserver struct {
	Result *AnalysisResult
}

func NewAnalysisPlanningObserver(expr string, timeRange types.QueryTimeRange) *AnalysisPlanningObserver {
	return &AnalysisPlanningObserver{
		Result: &AnalysisResult{
			OriginalExpression: expr,
			TimeRange:          timeRange,
		},
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

func AnalysisHandler(planner *QueryPlanner) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, status, err := handleAnalysis(w, r, planner)

		if err != nil {
			body = []byte(err.Error())
			w.Header().Set("Content-Type", "text/plain")
		}

		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(status)
		_, _ = w.Write(body)
	})
}

func handleAnalysis(w http.ResponseWriter, r *http.Request, planner *QueryPlanner) ([]byte, int, error) {
	if planner == nil {
		// Handle the case where query planning is disabled.
		return nil, http.StatusNotFound, errors.New("query planning is disabled, analysis is not available")
	}

	if err := r.ParseForm(); err != nil {
		return nil, http.StatusBadRequest, fmt.Errorf("could not parse request: %w", err)
	}

	qs := r.Form.Get("query")
	if qs == "" {
		return nil, http.StatusBadRequest, errors.New("missing 'query' parameter")
	}

	var timeRange types.QueryTimeRange

	if r.Form.Has("time") && (r.Form.Has("start") || r.Form.Has("end") || r.Form.Has("step")) {
		return nil, http.StatusBadRequest, errors.New("cannot provide a mixture of parameters for instant query ('time') and range query ('start', 'end' and 'step')")
	}

	if r.Form.Has("time") {
		t, err := parseTime(r.Form.Get("time"))
		if err != nil {
			return nil, http.StatusBadRequest, fmt.Errorf("could not parse 'time' parameter: %w", err)
		}

		timeRange = types.NewInstantQueryTimeRange(t)
	} else if r.Form.Has("start") && r.Form.Has("end") && r.Form.Has("step") {
		start, err := parseTime(r.Form.Get("start"))
		if err != nil {
			return nil, http.StatusBadRequest, fmt.Errorf("could not parse 'start' parameter: %w", err)
		}

		end, err := parseTime(r.Form.Get("end"))
		if err != nil {
			return nil, http.StatusBadRequest, fmt.Errorf("could not parse 'end' parameter: %w", err)
		}

		step, err := parseDuration(r.Form.Get("step"))
		if err != nil {
			return nil, http.StatusBadRequest, fmt.Errorf("could not parse 'step' parameter: %w", err)
		}

		if end.Before(start) {
			return nil, http.StatusBadRequest, errors.New("end time must be not be before start time")
		}

		if step <= 0 {
			return nil, http.StatusBadRequest, errors.New("step must be greater than 0")
		}

		timeRange = types.NewRangeQueryTimeRange(start, end, step)
	} else {
		return nil, http.StatusBadRequest, errors.New("missing 'time' parameter for instant query or 'start', 'end' and 'step' parameters for range query")
	}

	result, err := planner.Analyze(r.Context(), qs, timeRange)
	if err != nil {
		var perr parser.ParseErrors
		if errors.As(err, &perr) {
			return nil, http.StatusBadRequest, fmt.Errorf("parsing expression failed: %w", perr)
		}

		return nil, http.StatusInternalServerError, fmt.Errorf("analysis failed: %w", err)
	}

	b, err := jsoniter.Marshal(result)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("could not marshal response: %w", err)
	}

	w.Header().Set("Content-Type", "application/json")
	return b, http.StatusOK, nil
}

// Based on Prometheus' web/api/v1/api.go
func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}
