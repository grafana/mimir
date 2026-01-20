// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streamingpromql

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/commonsubexpressionelimination"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/metrics"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

var (
	anchoredIncompatibleFunctionPrefix = fmt.Sprintf("anchored modifier can only be used with: %s - not with ", sortImplode(promql.AnchoredSafeFunctions))
	smoothedIncompatibleFunctionPrefix = fmt.Sprintf("smoothed modifier can only be used with: %s - not with ", sortImplode(promql.SmoothedSafeFunctions))
)

type ErrAnchoredIncompatibleFunction struct {
	functionName string
}

func (e ErrAnchoredIncompatibleFunction) Error() string {
	return anchoredIncompatibleFunctionPrefix + e.functionName
}

type ErrSmoothedIncompatibleFunction struct {
	functionName string
}

func (e ErrSmoothedIncompatibleFunction) Error() string {
	return smoothedIncompatibleFunctionPrefix + e.functionName
}

type QueryPlanner struct {
	activeQueryTracker       QueryTracker
	noStepSubqueryIntervalFn func(rangeMillis int64) int64
	enableDelayedNameRemoval bool
	astOptimizationPasses    []optimize.ASTOptimizationPass
	planOptimizationPasses   []optimize.QueryPlanOptimizationPass
	planStageLatency         *prometheus.HistogramVec
	generatedPlans           *prometheus.CounterVec
	versionProvider          QueryPlanVersionProvider
	planningMetricsTracker   *planningmetrics.MetricsTracker
	logger                   log.Logger

	// Replaced during testing to ensure timing produces consistent results.
	TimeSince func(time.Time) time.Duration
}

func NewQueryPlanner(opts EngineOpts, versionProvider QueryPlanVersionProvider) (*QueryPlanner, error) {
	planner, err := NewQueryPlannerWithoutOptimizationPasses(opts, versionProvider)
	if err != nil {
		return nil, err
	}

	// FIXME: it makes sense to register these common optimization passes here, but we'll likely need to rework this once
	// we introduce query-frontend-specific optimization passes like sharding and splitting for two reasons:
	//  1. We want to avoid a circular dependency between this package and the query-frontend package where most of the logic for these optimization passes lives.
	//  2. We don't want to register these optimization passes in queriers.
	planner.RegisterASTOptimizationPass(&ast.CollapseConstants{}) // We expect this to be the first to simplify the logic for the rest of the optimization passes.
	if opts.EnablePruneToggles {
		planner.RegisterASTOptimizationPass(ast.NewPruneToggles(opts.CommonOpts.Reg)) // Do this next to ensure that toggled off expressions are removed before the other optimization passes are applied.
	}
	// NOTE: This optimization pass MUST run before SortLabelsAndMatchers since it does not preserve the order of matchers.
	if opts.EnableReduceMatchers {
		planner.RegisterASTOptimizationPass(ast.NewReduceMatchers(opts.CommonOpts.Reg, opts.Logger))
	}
	planner.RegisterASTOptimizationPass(&ast.SortLabelsAndMatchers{}) // This is a prerequisite for other optimization passes such as common subexpression elimination.
	// After query sharding is moved here, we want to move propagate matchers and reorder histogram aggregation here as well before query sharding.

	// This optimization pass is registered before CSE to keep the query plan as a simple tree structure.
	// After CSE, the query plan may no longer be a tree due to multiple paths culminating in the same Duplicate node,
	// which would make the elimination logic more complex.
	if opts.EnableEliminateDeduplicateAndMerge {
		planner.RegisterQueryPlanOptimizationPass(plan.NewEliminateDeduplicateAndMergeOptimizationPass(opts.CommonOpts.EnableDelayedNameRemoval, opts.CommonOpts.Reg))
	}

	if opts.EnableSkippingHistogramDecoding {
		// This optimization pass must be registered before common subexpression elimination, if that is enabled.
		planner.RegisterQueryPlanOptimizationPass(plan.NewSkipHistogramDecodingOptimizationPass())
	}

	if opts.EnableCommonSubexpressionElimination {
		planner.RegisterQueryPlanOptimizationPass(commonsubexpressionelimination.NewOptimizationPass(opts.EnableCommonSubexpressionEliminationForRangeVectorExpressionsInInstantQueries, opts.CommonOpts.Reg, opts.Logger))
	}

	if opts.EnableNarrowBinarySelectors {
		planner.RegisterQueryPlanOptimizationPass(plan.NewNarrowSelectorsOptimizationPass(opts.CommonOpts.Reg, opts.Logger))
	}

	return planner, nil
}

// NewQueryPlannerWithoutOptimizationPasses creates a new query planner without any optimization passes registered.
//
// This is intended for use in tests only.
func NewQueryPlannerWithoutOptimizationPasses(opts EngineOpts, versionProvider QueryPlanVersionProvider) (*QueryPlanner, error) {
	activeQueryTracker := opts.ActiveQueryTracker
	if activeQueryTracker == nil {
		if opts.CommonOpts.ActiveQueryTracker != nil {
			return nil, errors.New("no MQE-style active query tracker provided, but one conforming to Prometheus' interface was provided, this is likely a bug")
		}

		activeQueryTracker = &NoopQueryTracker{}
	}

	return &QueryPlanner{
		activeQueryTracker:       activeQueryTracker,
		noStepSubqueryIntervalFn: opts.CommonOpts.NoStepSubqueryIntervalFn,
		enableDelayedNameRemoval: opts.CommonOpts.EnableDelayedNameRemoval,
		planStageLatency: promauto.With(opts.CommonOpts.Reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                        "cortex_mimir_query_engine_plan_stage_latency_seconds",
			Help:                        "Latency of each stage of the query planning process.",
			NativeHistogramBucketFactor: 1.1,
		}, []string{"stage_type", "stage"}),
		generatedPlans: promauto.With(opts.CommonOpts.Reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_plans_generated_total",
			Help: "Total number of query plans generated.",
		}, []string{"version"}),
		planningMetricsTracker: planningmetrics.NewMetricsTracker(opts.CommonOpts.Reg),
		versionProvider:        versionProvider,

		logger:    opts.Logger,
		TimeSince: time.Since,
	}, nil
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

// ParseAndApplyASTOptimizationPasses runs the AST optimization passes on the input string and outputs
// an expression and any error encountered. This is separated into its own method to allow testing of
// AST optimization passes.
func (p *QueryPlanner) ParseAndApplyASTOptimizationPasses(ctx context.Context, qs string, timeRange types.QueryTimeRange, observer PlanningObserver) (parser.Expr, error) {
	expr, err := p.runASTStage("Parsing", observer, func() (parser.Expr, error) { return parser.ParseExpr(qs) })
	if err != nil {
		return nil, err
	}

	if !timeRange.IsInstant {
		if expr.Type() != parser.ValueTypeVector && expr.Type() != parser.ValueTypeScalar {
			return nil, apierror.Newf(apierror.TypeBadData, "query expression produces a %s, but expression for range queries must produce an instant vector or scalar", parser.DocumentedType(expr.Type()))
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

	return expr, nil
}

func (p *QueryPlanner) NewQueryPlan(ctx context.Context, qs string, timeRange types.QueryTimeRange, observer PlanningObserver) (*planning.QueryPlan, error) {
	spanLogger, ctx := spanlogger.New(ctx, p.logger, tracer, "QueryPlanner.NewQueryPlan")
	defer spanLogger.Finish()
	spanLogger.SetTag("query", qs)

	queryID, err := p.activeQueryTracker.InsertWithDetails(ctx, qs, "planning", true, timeRange)
	if err != nil {
		return nil, err
	}

	defer p.activeQueryTracker.Delete(queryID)

	maximumSupportedQueryPlanVersion, err := p.versionProvider.GetMaximumSupportedQueryPlanVersion(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not determine maximum supported query plan version: %w", err)
	}

	spanLogger.DebugLog("msg", "starting planning", "expression", qs, "maximum_supported_query_plan_version", maximumSupportedQueryPlanVersion)

	expr, err := p.ParseAndApplyASTOptimizationPasses(ctx, qs, timeRange, observer)
	if err != nil {
		return nil, err
	}

	spanLogger.DebugLog("msg", "AST optimisation passes completed", "expression", expr)

	plan, err := p.runPlanningStage("Original plan", observer, func() (*planning.QueryPlan, error) {
		root, err := p.nodeFromExpr(expr, timeRange)
		if err != nil {
			return nil, err
		}

		if p.enableDelayedNameRemoval {
			var err error
			root, err = p.insertDropNameOperator(root)
			if err != nil {
				return nil, err
			}
		}

		plan := &planning.QueryPlan{
			Root: root,
			Parameters: &planning.QueryParameters{
				TimeRange:                timeRange,
				OriginalExpression:       qs,
				EnableDelayedNameRemoval: p.enableDelayedNameRemoval,
			},
		}

		return plan, nil
	})

	if err != nil {
		return nil, err
	}

	spanLogger.DebugLog("msg", "original plan completed", "plan", plan)

	for _, o := range p.planOptimizationPasses {
		plan, err = p.runPlanningStage(o.Name(), observer, func() (*planning.QueryPlan, error) { return o.Apply(ctx, plan, maximumSupportedQueryPlanVersion) })

		if err != nil {
			return nil, err
		}
	}

	if err := plan.DeterminePlanVersion(); err != nil {
		return nil, err
	}

	if plan.Version > maximumSupportedQueryPlanVersion {
		return nil, fmt.Errorf("maximum supported query plan version is %d, but generated plan version is %d - this is a bug", maximumSupportedQueryPlanVersion, plan.Version)
	}

	p.generatedPlans.WithLabelValues(plan.Version.String()).Inc()

	if err := observer.OnAllPlanningStagesComplete(plan); err != nil {
		return nil, err
	}

	spanLogger.DebugLog("msg", "planning completed", "plan", plan, "version", plan.Version)

	return plan, err
}

func (p *QueryPlanner) insertDropNameOperator(root planning.Node) (planning.Node, error) {
	if dedupAndMerge, ok := root.(*core.DeduplicateAndMerge); ok {
		// If root is already DeduplicateAndMerge, insert DropName between it and its inner node
		return &core.DeduplicateAndMerge{
			Inner: &core.DropName{
				Inner:           dedupAndMerge.Inner,
				DropNameDetails: &core.DropNameDetails{},
			},
			DeduplicateAndMergeDetails: dedupAndMerge.DeduplicateAndMergeDetails,
		}, nil
	}

	// Don't run delayed name removal or deduplicate and merge where there are no
	// vector selectors.
	shouldWrap, err := shouldWrapInDedupAndMerge(root)
	if err != nil {
		return nil, err
	}
	if shouldWrap {
		return &core.DeduplicateAndMerge{
			Inner: &core.DropName{
				Inner:           root,
				DropNameDetails: &core.DropNameDetails{},
			},
			DeduplicateAndMergeDetails: &core.DeduplicateAndMergeDetails{},
		}, nil
	}

	return root, nil
}

func shouldWrapInDedupAndMerge(root planning.Node) (bool, error) {
	switch node := root.(type) {
	case *core.NumberLiteral, *core.StringLiteral, *core.MatrixSelector:
		return false, nil
	case *core.BinaryExpression:
		resL, err := node.LHS.ResultType()
		if err != nil {
			return false, err
		}
		if resL != parser.ValueTypeScalar {
			break
		}
		resR, err := node.RHS.ResultType()
		if err != nil {
			return false, err
		}
		if resR == parser.ValueTypeScalar {
			return false, nil
		}
	case *core.FunctionCall:
		res, err := root.ResultType()
		if err == nil && res == parser.ValueTypeScalar {
			return false, nil
		}
	case *core.StepInvariantExpression:
		return shouldWrapInDedupAndMerge(node.Inner)
	}
	return true, nil
}

func (p *QueryPlanner) runASTStage(stageName string, observer PlanningObserver, stage func() (parser.Expr, error)) (parser.Expr, error) {
	start := time.Now()
	expr, err := stage()
	if err != nil {
		return nil, err
	}

	duration := p.TimeSince(start)
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

	duration := p.TimeSince(start)
	p.planStageLatency.WithLabelValues("Plan", stageName).Observe(duration.Seconds())

	if err := observer.OnPlanningStageComplete(stageName, plan, duration); err != nil {
		return nil, err
	}

	return plan, nil
}

func (p *QueryPlanner) nodeFromExpr(expr parser.Expr, timeRange types.QueryTimeRange) (planning.Node, error) {
	switch expr := expr.(type) {
	case *parser.VectorSelector:
		return &core.VectorSelector{
			VectorSelectorDetails: &core.VectorSelectorDetails{
				Matchers:           core.LabelMatchersFromPrometheusType(expr.LabelMatchers),
				Timestamp:          core.TimeFromTimestamp(expr.Timestamp),
				Offset:             expr.OriginalOffset,
				ExpressionPosition: core.PositionRangeFrom(expr.PositionRange()),
				Smoothed:           expr.Smoothed,
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
				Matchers:           core.LabelMatchersFromPrometheusType(vs.LabelMatchers),
				Timestamp:          core.TimeFromTimestamp(vs.Timestamp),
				Offset:             vs.OriginalOffset,
				Range:              expr.Range,
				ExpressionPosition: core.PositionRangeFrom(expr.PositionRange()),
				Anchored:           vs.Anchored,
				Smoothed:           vs.Smoothed,
				// Note that we deliberately do not propagate SkipHistogramBuckets from the expression here. See the explanation above.
			},
		}, nil

	case *parser.AggregateExpr:
		inner, err := p.nodeFromExpr(expr.Expr, timeRange)
		if err != nil {
			return nil, err
		}

		var param planning.Node

		if expr.Param != nil {
			param, err = p.nodeFromExpr(expr.Param, timeRange)
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
		lhs, err := p.nodeFromExpr(expr.LHS, timeRange)
		if err != nil {
			return nil, err
		}

		rhs, err := p.nodeFromExpr(expr.RHS, timeRange)
		if err != nil {
			return nil, err
		}

		op, err := core.BinaryOperationFrom(expr.Op)
		if err != nil {
			return nil, err
		}

		binExpr := &core.BinaryExpression{
			LHS: lhs,
			RHS: rhs,
			BinaryExpressionDetails: &core.BinaryExpressionDetails{
				Op:                 op,
				VectorMatching:     core.VectorMatchingFrom(expr.VectorMatching),
				ReturnBool:         expr.ReturnBool,
				ExpressionPosition: core.PositionRangeFrom(expr.PositionRange()),
			},
		}
		// Only 'or' and scalar/vector expressions need deduplication.
		// All other variations either can't produce duplicate series (e.g., 'and' and 'unless')
		// or deduplication is handled by the operator (e.g., vector/vector expressions).
		if expr.Op == parser.LOR {
			return &core.DeduplicateAndMerge{
				Inner:                      binExpr,
				DeduplicateAndMergeDetails: &core.DeduplicateAndMergeDetails{},
			}, nil
		}

		lhsType := expr.LHS.Type()
		rhsType := expr.RHS.Type()
		isVectorScalar := (lhsType == parser.ValueTypeVector && rhsType == parser.ValueTypeScalar) ||
			(lhsType == parser.ValueTypeScalar && rhsType == parser.ValueTypeVector)

		if isVectorScalar {
			// Comparison vector-scalar operations without bool modifier don't drop the __name__ label.
			// So don't need to wrap in DeduplicateAndMerge.
			if expr.Op.IsComparisonOperator() && !expr.ReturnBool {
				return binExpr, nil
			}

			return &core.DeduplicateAndMerge{
				Inner:                      binExpr,
				DeduplicateAndMergeDetails: &core.DeduplicateAndMergeDetails{},
			}, nil
		}

		return binExpr, nil

	case *parser.Call:
		fnc, ok := findFunction(expr.Func.Name)
		if !ok {
			return nil, compat.NewNotSupportedError(fmt.Sprintf("'%s' function", expr.Func.Name))
		}

		args := make([]planning.Node, 0, len(expr.Args))

		for _, arg := range expr.Args {
			node, err := p.nodeFromExpr(arg, timeRange)
			if err != nil {
				return nil, err
			}
			matrixSelector, ok := node.(*core.MatrixSelector)
			if ok && matrixSelector.Anchored {
				_, supported := promql.AnchoredSafeFunctions[expr.Func.Name]
				if !supported {
					return nil, ErrAnchoredIncompatibleFunction{functionName: expr.Func.Name}
				}
			}
			if ok && matrixSelector.Smoothed {
				_, supported := promql.SmoothedSafeFunctions[expr.Func.Name]
				if !supported {
					return nil, ErrSmoothedIncompatibleFunction{functionName: expr.Func.Name}
				}
				// We only need to calculate the counter-aware interpolated smoothed points if the outer function is rate or increase.
				// The delta function does not require the counter adjusted points.
				// See Prometheus function.go - funcDelta(), funcRate(), funcIncrease() - only rate and increase set isCounter=true
				matrixSelector.CounterAware = expr.Func.Name == "rate" || expr.Func.Name == "increase"
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

			// A special case to re-order when we have a timestamp(StepInvariantExpression(VectorSelector)).
			// The StepInvariantExpression is moved up to encase the entire function call.
			// Note that the DeduplicateAndMerge still wraps the function call as the timestamp function returns true under functionNeedsDeduplication().
			// This can be removed once https://github.com/prometheus/prometheus/pull/17313 is vendored into mimir
			stepInvariantExpression, ok := args[0].(*core.StepInvariantExpression)
			if ok {
				vectorSelector, ok := stepInvariantExpression.Inner.(*core.VectorSelector)
				if ok {
					vectorSelector.ReturnSampleTimestamps = true
					f.Args[0] = stepInvariantExpression.Inner

					// Note - we do not update the step invariant metrics tracker as this will already have been updated when the
					// argument expression was created.

					return &core.StepInvariantExpression{
						Inner: &core.DeduplicateAndMerge{
							Inner:                      f,
							DeduplicateAndMergeDetails: &core.DeduplicateAndMergeDetails{}},
						StepInvariantExpressionDetails: &core.StepInvariantExpressionDetails{},
					}, nil
				}
			}

			vectorSelector, ok := args[0].(*core.VectorSelector)
			if ok {
				vectorSelector.ReturnSampleTimestamps = true
			}
		}

		if functionNeedsDeduplication(fnc) {
			return &core.DeduplicateAndMerge{
				Inner:                      f,
				DeduplicateAndMergeDetails: &core.DeduplicateAndMergeDetails{},
			}, nil
		}

		return f, nil

	case *parser.SubqueryExpr:

		step := expr.Step

		if step == 0 {
			step = time.Duration(p.noStepSubqueryIntervalFn(expr.Range.Milliseconds())) * time.Millisecond
		}

		// Construct the Subquery in 2 phases.
		// The first step initializes the SubqueryDetails, which allows us to determine the children time range.
		// The second step then creates the inner expression, passing in this child time range.
		// This is needed by the step invariant expression handler which will require this child time range to
		// accurately calculate the number of steps saved.
		subquery := &core.Subquery{
			SubqueryDetails: &core.SubqueryDetails{
				Timestamp:          core.TimeFromTimestamp(expr.Timestamp),
				Offset:             expr.OriginalOffset,
				Range:              expr.Range,
				Step:               step,
				ExpressionPosition: core.PositionRangeFrom(expr.PositionRange()),
			},
		}

		childTimeRange := subquery.ChildrenTimeRange(timeRange)

		inner, err := p.nodeFromExpr(expr.Expr, childTimeRange)
		if err != nil {
			return nil, err
		}

		subquery.Inner = inner
		return subquery, nil

	case *parser.UnaryExpr:
		inner, err := p.nodeFromExpr(expr.Expr, timeRange)
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

		unaryExpr := &core.UnaryExpression{
			Inner: inner,
			UnaryExpressionDetails: &core.UnaryExpressionDetails{
				Op:                 op,
				ExpressionPosition: core.PositionRangeFrom(expr.PositionRange()),
			},
		}

		// Unary negation of vectors drops the __name__ label, so wrap in DeduplicateAndMerge.
		if expr.Op == parser.SUB && expr.Expr.Type() == parser.ValueTypeVector {
			return &core.DeduplicateAndMerge{
				Inner:                      unaryExpr,
				DeduplicateAndMergeDetails: &core.DeduplicateAndMergeDetails{},
			}, nil
		}

		return unaryExpr, nil

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
		return p.nodeFromExpr(expr.Expr, timeRange)

	case *parser.StepInvariantExpr:
		inner, err := p.nodeFromExpr(expr.Expr, timeRange)
		if err != nil {
			return nil, err
		}

		// There is no advantage to wrapping an instant query in a step invariant.
		if timeRange.StepCount <= 1 {
			return inner, nil
		}

		p.planningMetricsTracker.StepInvariantTracker.OnStepInvariantExpressionAdded(timeRange.StepCount)

		return &core.StepInvariantExpression{
			Inner:                          inner,
			StepInvariantExpressionDetails: &core.StepInvariantExpressionDetails{},
		}, nil

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

// functionNeedsDeduplication checks if a function needs deduplication and merge operator.
// This is determined by whether the function drops the __name__ label or otherwise manipulates it.
func functionNeedsDeduplication(fnc functions.Function) bool {
	switch fnc {
	// Functions that need deduplication (manipulate labels)
	case
		// Time transformation functions
		functions.FUNCTION_DAY_OF_MONTH,
		functions.FUNCTION_DAY_OF_WEEK,
		functions.FUNCTION_DAY_OF_YEAR,
		functions.FUNCTION_DAYS_IN_MONTH,
		functions.FUNCTION_HOUR,
		functions.FUNCTION_MINUTE,
		functions.FUNCTION_MONTH,
		functions.FUNCTION_YEAR,
		// Range vector functions
		functions.FUNCTION_AVG_OVER_TIME,
		functions.FUNCTION_CHANGES,
		functions.FUNCTION_COUNT_OVER_TIME,
		functions.FUNCTION_DELTA,
		functions.FUNCTION_DERIV,
		functions.FUNCTION_IDELTA,
		functions.FUNCTION_INCREASE,
		functions.FUNCTION_IRATE,
		functions.FUNCTION_MAD_OVER_TIME,
		functions.FUNCTION_MAX_OVER_TIME,
		functions.FUNCTION_MIN_OVER_TIME,
		functions.FUNCTION_PRESENT_OVER_TIME,
		functions.FUNCTION_QUANTILE_OVER_TIME,
		functions.FUNCTION_RATE,
		functions.FUNCTION_RESETS,
		functions.FUNCTION_STDDEV_OVER_TIME,
		functions.FUNCTION_STDVAR_OVER_TIME,
		functions.FUNCTION_SUM_OVER_TIME,
		functions.FUNCTION_TS_OF_FIRST_OVER_TIME,
		functions.FUNCTION_TS_OF_LAST_OVER_TIME,
		functions.FUNCTION_TS_OF_MAX_OVER_TIME,
		functions.FUNCTION_TS_OF_MIN_OVER_TIME,
		// Instant vector transformations
		functions.FUNCTION_ABS,
		functions.FUNCTION_ACOS,
		functions.FUNCTION_ACOSH,
		functions.FUNCTION_ASIN,
		functions.FUNCTION_ASINH,
		functions.FUNCTION_ATAN,
		functions.FUNCTION_ATANH,
		functions.FUNCTION_CEIL,
		functions.FUNCTION_COS,
		functions.FUNCTION_COSH,
		functions.FUNCTION_DEG,
		functions.FUNCTION_EXP,
		functions.FUNCTION_FLOOR,
		functions.FUNCTION_LN,
		functions.FUNCTION_LOG10,
		functions.FUNCTION_LOG2,
		functions.FUNCTION_RAD,
		functions.FUNCTION_SGN,
		functions.FUNCTION_SIN,
		functions.FUNCTION_SINH,
		functions.FUNCTION_SQRT,
		functions.FUNCTION_TAN,
		functions.FUNCTION_TANH,
		functions.FUNCTION_CLAMP,
		functions.FUNCTION_CLAMP_MAX,
		functions.FUNCTION_CLAMP_MIN,
		functions.FUNCTION_ROUND,
		functions.FUNCTION_PREDICT_LINEAR,
		functions.FUNCTION_TIMESTAMP,
		functions.FUNCTION_DOUBLE_EXPONENTIAL_SMOOTHING,
		// Histogram functions
		functions.FUNCTION_HISTOGRAM_AVG,
		functions.FUNCTION_HISTOGRAM_COUNT,
		functions.FUNCTION_HISTOGRAM_FRACTION,
		functions.FUNCTION_HISTOGRAM_QUANTILE,
		functions.FUNCTION_HISTOGRAM_STDDEV,
		functions.FUNCTION_HISTOGRAM_STDVAR,
		functions.FUNCTION_HISTOGRAM_SUM,
		// Label manipulation functions
		functions.FUNCTION_LABEL_JOIN,
		functions.FUNCTION_LABEL_REPLACE,
		// Adaptive metrics reserved functions
		functions.FUNCTION_ADAPTIVE_METRICS_RESERVED_1,
		functions.FUNCTION_ADAPTIVE_METRICS_RESERVED_2:
		return true

	// Functions that do NOT need deduplication (don't manipulate labels or are guaranteed to return only one series)
	case
		functions.FUNCTION_ABSENT,
		functions.FUNCTION_ABSENT_OVER_TIME,
		functions.FUNCTION_FIRST_OVER_TIME,
		functions.FUNCTION_INFO,
		functions.FUNCTION_LAST_OVER_TIME,
		functions.FUNCTION_LIMITK,
		functions.FUNCTION_LIMIT_RATIO,
		functions.FUNCTION_PI,
		functions.FUNCTION_SCALAR,
		functions.FUNCTION_SHARDING_CONCAT, // Might return duplicate series, but this is OK and desired, and aggregation operators will handle this correctly.
		functions.FUNCTION_SORT,
		functions.FUNCTION_SORT_BY_LABEL,
		functions.FUNCTION_SORT_BY_LABEL_DESC,
		functions.FUNCTION_SORT_DESC,
		functions.FUNCTION_TIME,
		functions.FUNCTION_VECTOR:
		return false

	case functions.FUNCTION_UNKNOWN:
		return false

	default:
		panic(fmt.Sprintf("functionNeedsDeduplication: unexpected function %v", fnc))
	}
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

type QueryPlanVersionProvider interface {
	GetMaximumSupportedQueryPlanVersion(ctx context.Context) (planning.QueryPlanVersion, error)
}

// NewStaticQueryPlanVersionProvider returns a QueryPlanVersionProvider that always returns the given version.
// This is intended to be used only in tests.
func NewStaticQueryPlanVersionProvider(version planning.QueryPlanVersion) QueryPlanVersionProvider {
	return &staticQueryPlanVersionProvider{
		version: version,
	}
}

// NewMaximumSupportedVersionQueryPlanVersionProvider returns a QueryPlanVersionProvider that always returns the maximum supported query plan version.
// This is intended to be used only in tests.
func NewMaximumSupportedVersionQueryPlanVersionProvider() QueryPlanVersionProvider {
	return NewStaticQueryPlanVersionProvider(planning.MaximumSupportedQueryPlanVersion)
}

type staticQueryPlanVersionProvider struct {
	version planning.QueryPlanVersion
}

func (s *staticQueryPlanVersionProvider) GetMaximumSupportedQueryPlanVersion(ctx context.Context) (planning.QueryPlanVersion, error) {
	return s.version, nil
}

func sortImplode(m map[string]struct{}) string {
	tmp := slices.Sorted(maps.Keys(m))
	return strings.Join(tmp, ", ")
}
