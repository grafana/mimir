// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streamingpromql

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cancellation"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/prometheus/prometheus/util/stats"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/aggregations"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/aggregations/topkbottomk"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/binops"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/scalars"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/selectors"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

var errQueryCancelled = cancellation.NewErrorf("query execution cancelled")
var errQueryClosed = cancellation.NewErrorf("Query.Close() called")
var errQueryFinished = cancellation.NewErrorf("query execution finished")

type Query struct {
	queryable storage.Queryable
	statement *parser.EvalStmt
	root      types.Operator
	engine    *Engine

	// The original PromQL expression for this query.
	// May not accurately represent the query being executed if this query was built from a query plan representing a subexpression of a query.
	originalExpression string

	cancel                   context.CancelCauseFunc
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker
	annotations              *annotations.Annotations
	stats                    *types.QueryStats
	lookbackDelta            time.Duration

	// Time range of the top-level query.
	// Subqueries may use a different range.
	topLevelQueryTimeRange types.QueryTimeRange

	operatorFactories map[planning.Node]planning.OperatorFactory
	operatorParams    *planning.OperatorParameters

	result *promql.Result
}

func (e *Engine) newQuery(ctx context.Context, queryable storage.Queryable, opts promql.QueryOpts, timeRange types.QueryTimeRange) (*Query, error) {
	if opts == nil {
		opts = promql.NewPrometheusQueryOpts(false, 0)
	}

	lookbackDelta := opts.LookbackDelta()
	if lookbackDelta == 0 {
		lookbackDelta = e.lookbackDelta
	}

	maxEstimatedMemoryConsumptionPerQuery, err := e.limitsProvider.GetMaxEstimatedMemoryConsumptionPerQuery(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not get memory consumption limit for query: %w", err)
	}

	q := &Query{
		queryable:                queryable,
		engine:                   e,
		memoryConsumptionTracker: limiting.NewMemoryConsumptionTracker(maxEstimatedMemoryConsumptionPerQuery, e.queriesRejectedDueToPeakMemoryConsumption),
		annotations:              annotations.New(),
		stats:                    &types.QueryStats{},
		topLevelQueryTimeRange:   timeRange,
		lookbackDelta:            lookbackDelta,
	}

	return q, nil
}

func (e *Engine) newQueryFromExpression(ctx context.Context, queryable storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (*Query, error) {
	expr, err := parser.ParseExpr(qs)
	if err != nil {
		return nil, err
	}

	expr, err = promql.PreprocessExpr(expr, start, end)
	if err != nil {
		return nil, err
	}
	var timeRange types.QueryTimeRange

	if start.Equal(end) && interval == 0 {
		timeRange = types.NewInstantQueryTimeRange(start)
	} else {
		timeRange = types.NewRangeQueryTimeRange(start, end, interval)

		if expr.Type() != parser.ValueTypeVector && expr.Type() != parser.ValueTypeScalar {
			return nil, fmt.Errorf("query expression produces a %s, but expression for range queries must produce an instant vector or scalar", parser.DocumentedType(expr.Type()))
		}
	}

	q, err := e.newQuery(ctx, queryable, opts, timeRange)
	if err != nil {
		return nil, err
	}

	q.originalExpression = qs
	q.statement = &parser.EvalStmt{
		Expr:          expr,
		Start:         start,
		End:           end,
		Interval:      interval, // 0 for instant queries
		LookbackDelta: q.lookbackDelta,
	}

	q.root, err = q.convertToOperator(expr, q.topLevelQueryTimeRange)
	if err != nil {
		return nil, err
	}

	return q, nil
}

func (q *Query) convertToOperator(expr parser.Expr, timeRange types.QueryTimeRange) (types.Operator, error) {
	switch expr.Type() {
	case parser.ValueTypeMatrix:
		return q.convertToRangeVectorOperator(expr, timeRange)
	case parser.ValueTypeVector:
		return q.convertToInstantVectorOperator(expr, timeRange)
	case parser.ValueTypeScalar:
		return q.convertToScalarOperator(expr, timeRange)
	case parser.ValueTypeString:
		return q.convertToStringOperator(expr)
	default:
		return nil, compat.NewNotSupportedError(fmt.Sprintf("%s value as top-level expression", parser.DocumentedType(expr.Type())))
	}
}

func (q *Query) convertToStringOperator(expr parser.Expr) (types.StringOperator, error) {
	if expr.Type() != parser.ValueTypeString {
		return nil, fmt.Errorf("cannot create string operator for expression that produces a %s", parser.DocumentedType(expr.Type()))
	}

	switch e := expr.(type) {
	case *parser.StringLiteral:
		return operators.NewStringLiteral(e.Val, e.PositionRange()), nil
	case *parser.StepInvariantExpr:
		// One day, we'll do something smarter here.
		return q.convertToStringOperator(e.Expr)
	case *parser.ParenExpr:
		return q.convertToStringOperator(e.Expr)
	default:
		return nil, compat.NewNotSupportedError(fmt.Sprintf("PromQL expression type %T for string", e))
	}
}

func (q *Query) convertToInstantVectorOperator(expr parser.Expr, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
	if expr.Type() != parser.ValueTypeVector {
		return nil, fmt.Errorf("cannot create instant vector operator for expression that produces a %s", parser.DocumentedType(expr.Type()))
	}

	switch e := expr.(type) {
	case *parser.VectorSelector:
		lookbackDelta := q.lookbackDelta

		return &selectors.InstantVectorSelector{
			MemoryConsumptionTracker: q.memoryConsumptionTracker,
			Selector: &selectors.Selector{
				Queryable:     q.queryable,
				TimeRange:     timeRange,
				Timestamp:     e.Timestamp,
				Offset:        e.OriginalOffset.Milliseconds(),
				LookbackDelta: lookbackDelta,
				Matchers:      e.LabelMatchers,

				ExpressionPosition:       e.PositionRange(),
				MemoryConsumptionTracker: q.memoryConsumptionTracker,
			},
			Stats: q.stats,
		}, nil
	case *parser.AggregateExpr:
		inner, err := q.convertToInstantVectorOperator(e.Expr, timeRange)
		if err != nil {
			return nil, err
		}

		if e.Param != nil {
			switch e.Op {
			case parser.TOPK, parser.BOTTOMK:
				param, err := q.convertToScalarOperator(e.Param, timeRange)
				if err != nil {
					return nil, err
				}

				return topkbottomk.New(inner, param, timeRange, e.Grouping, e.Without, e.Op == parser.TOPK, q.memoryConsumptionTracker, q.annotations, e.PosRange), nil
			case parser.QUANTILE:
				param, err := q.convertToScalarOperator(e.Param, timeRange)
				if err != nil {
					return nil, err
				}
				return aggregations.NewQuantileAggregation(
					inner,
					param,
					timeRange,
					e.Grouping,
					e.Without,
					q.memoryConsumptionTracker,
					q.annotations,
					e.PosRange,
				)
			case parser.COUNT_VALUES:
				param, err := q.convertToStringOperator(e.Param)
				if err != nil {
					return nil, err
				}

				return aggregations.NewCountValues(inner, param, timeRange, e.Grouping, e.Without, q.memoryConsumptionTracker, e.PosRange), nil
			default:
				return nil, compat.NewNotSupportedError(fmt.Sprintf("'%s' aggregation with parameter", e.Op))
			}
		}

		return aggregations.NewAggregation(
			inner,
			timeRange,
			e.Grouping,
			e.Without,
			e.Op,
			q.memoryConsumptionTracker,
			q.annotations,
			e.PosRange,
		)
	case *parser.Call:
		return q.convertFunctionCallToInstantVectorOperator(e, timeRange)
	case *parser.BinaryExpr:
		// We only need to handle three combinations of types here:
		// Scalar on left, vector on right
		// Vector on left, scalar on right
		// Vector on both sides
		//
		// We don't need to handle scalars on both sides here, as that would produce a scalar and so is handled in convertToScalarOperator.

		if e.LHS.Type() == parser.ValueTypeScalar || e.RHS.Type() == parser.ValueTypeScalar {
			var scalar types.ScalarOperator
			var vector types.InstantVectorOperator
			var err error

			if e.LHS.Type() == parser.ValueTypeScalar {
				scalar, err = q.convertToScalarOperator(e.LHS, timeRange)
				if err != nil {
					return nil, err
				}

				vector, err = q.convertToInstantVectorOperator(e.RHS, timeRange)
				if err != nil {
					return nil, err
				}
			} else {
				scalar, err = q.convertToScalarOperator(e.RHS, timeRange)
				if err != nil {
					return nil, err
				}

				vector, err = q.convertToInstantVectorOperator(e.LHS, timeRange)
				if err != nil {
					return nil, err
				}
			}

			scalarIsLeftSide := e.LHS.Type() == parser.ValueTypeScalar

			o, err := binops.NewVectorScalarBinaryOperation(scalar, vector, scalarIsLeftSide, e.Op, e.ReturnBool, timeRange, q.memoryConsumptionTracker, q.annotations, e.PositionRange())
			if err != nil {
				return nil, err
			}

			return operators.NewDeduplicateAndMerge(o, q.memoryConsumptionTracker), err
		}

		// Vectors on both sides.
		lhs, err := q.convertToInstantVectorOperator(e.LHS, timeRange)
		if err != nil {
			return nil, err
		}

		rhs, err := q.convertToInstantVectorOperator(e.RHS, timeRange)
		if err != nil {
			return nil, err
		}

		switch e.Op {
		case parser.LAND, parser.LUNLESS:
			return binops.NewAndUnlessBinaryOperation(lhs, rhs, *e.VectorMatching, q.memoryConsumptionTracker, e.Op == parser.LUNLESS, timeRange, e.PositionRange()), nil
		case parser.LOR:
			return binops.NewOrBinaryOperation(lhs, rhs, *e.VectorMatching, q.memoryConsumptionTracker, timeRange, e.PositionRange()), nil
		default:
			switch e.VectorMatching.Card {
			case parser.CardOneToMany, parser.CardManyToOne:
				return binops.NewGroupedVectorVectorBinaryOperation(lhs, rhs, *e.VectorMatching, e.Op, e.ReturnBool, q.memoryConsumptionTracker, q.annotations, e.PositionRange(), timeRange)
			case parser.CardOneToOne:
				return binops.NewOneToOneVectorVectorBinaryOperation(lhs, rhs, *e.VectorMatching, e.Op, e.ReturnBool, q.memoryConsumptionTracker, q.annotations, e.PositionRange(), timeRange)
			default:
				return nil, compat.NewNotSupportedError(fmt.Sprintf("binary expression with %v matching for '%v'", e.VectorMatching.Card, e.Op))
			}
		}

	case *parser.UnaryExpr:
		if e.Op == parser.ADD {
			// Unary addition (+value): there's nothing to do, just return the inner expression.
			return q.convertToInstantVectorOperator(e.Expr, timeRange)
		}

		if e.Op != parser.SUB {
			return nil, compat.NewNotSupportedError(fmt.Sprintf("unary expression with '%s'", e.Op))
		}

		inner, err := q.convertToInstantVectorOperator(e.Expr, timeRange)
		if err != nil {
			return nil, err
		}

		return functions.UnaryNegationOfInstantVectorOperatorFactory(inner, q.memoryConsumptionTracker, e.PositionRange(), timeRange), nil

	case *parser.StepInvariantExpr:
		// One day, we'll do something smarter here.
		return q.convertToInstantVectorOperator(e.Expr, timeRange)
	case *parser.ParenExpr:
		return q.convertToInstantVectorOperator(e.Expr, timeRange)
	default:
		return nil, compat.NewNotSupportedError(fmt.Sprintf("PromQL expression type %T for instant vectors", e))
	}
}

func (q *Query) convertFunctionCallToInstantVectorOperator(e *parser.Call, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error) {
	switch e.Func.Name {
	case "absent":
		inner, err := q.convertToInstantVectorOperator(e.Args[0], timeRange)
		if err != nil {
			return nil, err
		}

		lbls := functions.CreateLabelsForAbsentFunction(e.Args[0])

		return functions.NewAbsent(inner, lbls, timeRange, q.memoryConsumptionTracker, e.PosRange), nil

	case "absent_over_time":
		inner, err := q.convertToRangeVectorOperator(e.Args[0], timeRange)
		if err != nil {
			return nil, err
		}

		lbls := functions.CreateLabelsForAbsentFunction(e.Args[0])

		return functions.NewAbsentOverTime(inner, lbls, timeRange, q.memoryConsumptionTracker, e.PosRange), nil

	default:
		factory, ok := functions.InstantVectorFunctionOperatorFactories[e.Func.Name]
		if !ok {
			return nil, compat.NewNotSupportedError(fmt.Sprintf("'%s' function", e.Func.Name))
		}

		args := make([]types.Operator, len(e.Args))
		for i := range e.Args {
			a, err := q.convertToOperator(e.Args[i], timeRange)
			if err != nil {
				return nil, err
			}
			args[i] = a
		}

		return factory(args, q.memoryConsumptionTracker, q.annotations, e.PosRange, timeRange)
	}
}

func (q *Query) convertToRangeVectorOperator(expr parser.Expr, timeRange types.QueryTimeRange) (types.RangeVectorOperator, error) {
	if expr.Type() != parser.ValueTypeMatrix {
		return nil, fmt.Errorf("cannot create range vector operator for expression that produces a %s", parser.DocumentedType(expr.Type()))
	}

	switch e := expr.(type) {
	case *parser.MatrixSelector:
		vectorSelector := e.VectorSelector.(*parser.VectorSelector)
		selector := &selectors.Selector{
			Queryable: q.queryable,
			TimeRange: timeRange,
			Timestamp: vectorSelector.Timestamp,
			Offset:    vectorSelector.OriginalOffset.Milliseconds(),
			Range:     e.Range,
			Matchers:  vectorSelector.LabelMatchers,

			ExpressionPosition:       e.PositionRange(),
			MemoryConsumptionTracker: q.memoryConsumptionTracker,
		}

		return selectors.NewRangeVectorSelector(selector, q.memoryConsumptionTracker, q.stats), nil

	case *parser.SubqueryExpr:
		step := e.Step

		if step == 0 {
			step = time.Duration(q.engine.noStepSubqueryIntervalFn(e.Range.Milliseconds())) * time.Millisecond
		}

		subqueryTimeRange := core.SubqueryTimeRange(timeRange, e.Range, step, core.TimeFromTimestamp(e.Timestamp), e.OriginalOffset)
		inner, err := q.convertToInstantVectorOperator(e.Expr, subqueryTimeRange)
		if err != nil {
			return nil, err
		}

		subquery := operators.NewSubquery(
			inner,
			timeRange,
			e.Timestamp,
			e.OriginalOffset,
			e.Range,
			e.PositionRange(),
			q.memoryConsumptionTracker,
		)

		return subquery, nil

	case *parser.StepInvariantExpr:
		// One day, we'll do something smarter here.
		return q.convertToRangeVectorOperator(e.Expr, timeRange)
	case *parser.ParenExpr:
		return q.convertToRangeVectorOperator(e.Expr, timeRange)
	default:
		return nil, compat.NewNotSupportedError(fmt.Sprintf("PromQL expression type %T for range vectors", e))
	}
}

func (q *Query) convertToScalarOperator(expr parser.Expr, timeRange types.QueryTimeRange) (types.ScalarOperator, error) {
	if expr.Type() != parser.ValueTypeScalar {
		return nil, fmt.Errorf("cannot create scalar operator for expression that produces a %s", parser.DocumentedType(expr.Type()))
	}

	switch e := expr.(type) {
	case *parser.NumberLiteral:
		o := scalars.NewScalarConstant(
			e.Val,
			timeRange,
			q.memoryConsumptionTracker,
			e.PositionRange(),
		)

		return o, nil

	case *parser.Call:
		return q.convertFunctionCallToScalarOperator(e, timeRange)

	case *parser.UnaryExpr:
		if e.Op == parser.ADD {
			// Unary addition (+value): there's nothing to do, just return the inner expression.
			return q.convertToScalarOperator(e.Expr, timeRange)
		}

		if e.Op != parser.SUB {
			return nil, compat.NewNotSupportedError(fmt.Sprintf("unary expression with '%s'", e.Op))
		}

		inner, err := q.convertToScalarOperator(e.Expr, timeRange)
		if err != nil {
			return nil, err
		}

		return scalars.NewUnaryNegationOfScalar(inner, e.PositionRange()), nil

	case *parser.StepInvariantExpr:
		// One day, we'll do something smarter here.
		return q.convertToScalarOperator(e.Expr, timeRange)
	case *parser.ParenExpr:
		return q.convertToScalarOperator(e.Expr, timeRange)
	case *parser.BinaryExpr:
		lhs, err := q.convertToScalarOperator(e.LHS, timeRange)
		if err != nil {
			return nil, err
		}

		rhs, err := q.convertToScalarOperator(e.RHS, timeRange)
		if err != nil {
			return nil, err
		}

		return binops.NewScalarScalarBinaryOperation(lhs, rhs, e.Op, q.memoryConsumptionTracker, e.PositionRange())

	default:
		return nil, compat.NewNotSupportedError(fmt.Sprintf("PromQL expression type %T for scalars", e))
	}
}

func (q *Query) convertFunctionCallToScalarOperator(e *parser.Call, timeRange types.QueryTimeRange) (types.ScalarOperator, error) {
	factory, ok := functions.ScalarFunctionOperatorFactories[e.Func.Name]
	if !ok {
		return nil, compat.NewNotSupportedError(fmt.Sprintf("'%s' function", e.Func.Name))
	}

	args := make([]types.Operator, len(e.Args))
	for i := range e.Args {
		a, err := q.convertToOperator(e.Args[i], timeRange)
		if err != nil {
			return nil, err
		}
		args[i] = a
	}

	return factory(args, q.memoryConsumptionTracker, q.annotations, e.PosRange, timeRange)
}

func (q *Query) convertNodeToOperator(node planning.Node, timeRange types.QueryTimeRange) (types.Operator, error) {
	if f, ok := q.operatorFactories[node]; ok {
		return f.Produce()
	}

	childTimeRange := node.ChildrenTimeRange(timeRange)
	childrenNodes := node.Children()
	childrenOperators := make([]types.Operator, 0, len(childrenNodes))
	for _, child := range childrenNodes {
		o, err := q.convertNodeToOperator(child, childTimeRange)
		if err != nil {
			return nil, err
		}

		childrenOperators = append(childrenOperators, o)
	}

	f, err := node.OperatorFactory(childrenOperators, timeRange, q.operatorParams)
	if err != nil {
		return nil, err
	}

	q.operatorFactories[node] = f

	return f.Produce()
}

func (q *Query) Exec(ctx context.Context) *promql.Result {
	defer q.root.Close()

	if q.engine.pedantic {
		// Close the root operator a second time to ensure all operators behave correctly if Close is called multiple times.
		defer q.root.Close()
	}

	ctx, cancel := context.WithCancelCause(ctx)
	q.cancel = cancel

	if q.engine.timeout != 0 {
		var cancelTimeoutCtx context.CancelFunc
		ctx, cancelTimeoutCtx = context.WithTimeoutCause(ctx, q.engine.timeout, fmt.Errorf("%w: query timed out", context.DeadlineExceeded))

		defer cancelTimeoutCtx()
	}

	// The order of the deferred cancellations is important: we want to cancel with errQueryFinished first, so we must defer this cancellation last
	// (so that it runs before the cancellation of the context with timeout created above).
	defer cancel(errQueryFinished)

	queryID, err := q.engine.activeQueryTracker.Insert(ctx, q.originalExpression)
	if err != nil {
		return &promql.Result{Err: err}
	}

	defer q.engine.activeQueryTracker.Delete(queryID)

	defer func() {
		logger := spanlogger.FromContext(ctx, q.engine.logger)
		msg := make([]interface{}, 0, 2*(3+4)) // 3 fields for all query types, plus worst case of 4 fields for range queries

		msg = append(msg,
			"msg", "query stats",
			"estimatedPeakMemoryConsumption", q.memoryConsumptionTracker.PeakEstimatedMemoryConsumptionBytes,
			"expr", q.originalExpression,
		)

		if q.topLevelQueryTimeRange.IsInstant {
			msg = append(msg,
				"queryType", "instant",
				"time", q.topLevelQueryTimeRange.StartT,
			)
		} else {
			msg = append(msg,
				"queryType", "range",
				"start", q.topLevelQueryTimeRange.StartT,
				"end", q.topLevelQueryTimeRange.EndT,
				"step", q.topLevelQueryTimeRange.IntervalMilliseconds,
			)
		}

		level.Info(logger).Log(msg...)
		q.engine.estimatedPeakMemoryConsumption.Observe(float64(q.memoryConsumptionTracker.PeakEstimatedMemoryConsumptionBytes))
	}()

	switch root := q.root.(type) {
	case types.RangeVectorOperator:
		series, err := root.SeriesMetadata(ctx)
		if err != nil {
			return &promql.Result{Err: err}
		}
		defer types.SeriesMetadataSlicePool.Put(series, q.memoryConsumptionTracker)

		v, err := q.populateMatrixFromRangeVectorOperator(ctx, root, series)
		if err != nil {
			return &promql.Result{Err: err}
		}

		q.result = &promql.Result{Value: v}
	case types.InstantVectorOperator:
		series, err := root.SeriesMetadata(ctx)
		if err != nil {
			return &promql.Result{Err: err}
		}
		defer types.SeriesMetadataSlicePool.Put(series, q.memoryConsumptionTracker)

		if q.topLevelQueryTimeRange.IsInstant {
			v, err := q.populateVectorFromInstantVectorOperator(ctx, root, series)
			if err != nil {
				return &promql.Result{Err: err}
			}

			q.result = &promql.Result{Value: v}
		} else {
			v, err := q.populateMatrixFromInstantVectorOperator(ctx, root, series)
			if err != nil {
				return &promql.Result{Err: err}
			}

			q.result = &promql.Result{Value: v}
		}
	case types.ScalarOperator:
		d, err := root.GetValues(ctx)
		if err != nil {
			return &promql.Result{Err: err}
		}

		if q.topLevelQueryTimeRange.IsInstant {
			q.result = &promql.Result{Value: q.populateScalarFromScalarOperator(d)}
		} else {
			q.result = &promql.Result{Value: q.populateMatrixFromScalarOperator(d)}
		}
	case types.StringOperator:
		q.result = &promql.Result{Value: q.populateStringFromStringOperator(root)}
	default:
		return &promql.Result{Err: fmt.Errorf("operator %T produces unknown result type", root)}
	}

	// To make comparing to Prometheus' engine easier, only return the annotations if there are some, otherwise, return nil.
	if len(*q.annotations) > 0 {
		q.result.Warnings = *q.annotations
	}

	return q.result
}

func (q *Query) populateStringFromStringOperator(o types.StringOperator) promql.String {
	return promql.String{
		T: q.topLevelQueryTimeRange.StartT,
		V: o.GetValue(),
	}
}

func (q *Query) populateVectorFromInstantVectorOperator(ctx context.Context, o types.InstantVectorOperator, series []types.SeriesMetadata) (promql.Vector, error) {
	ts := q.topLevelQueryTimeRange.StartT
	v, err := types.VectorPool.Get(len(series), q.memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	for i, s := range series {
		d, err := o.NextSeries(ctx)
		if err != nil {
			if errors.Is(err, types.EOS) {
				return nil, fmt.Errorf("expected %v series, but only received %v", len(series), i)
			}

			return nil, err
		}

		if len(d.Floats) == 1 && len(d.Histograms) == 0 {
			point := d.Floats[0]
			v = append(v, promql.Sample{
				Metric: s.Labels,
				T:      ts,
				F:      point.F,
			})
		} else if len(d.Floats) == 0 && len(d.Histograms) == 1 {
			point := d.Histograms[0]
			v = append(v, promql.Sample{
				Metric: s.Labels,
				T:      ts,
				H:      point.H,
			})

			// Remove histogram from slice to ensure it's not mutated when the slice is reused.
			d.Histograms[0].H = nil
		} else {
			types.PutInstantVectorSeriesData(d, q.memoryConsumptionTracker)

			// A series may have no data points.
			if len(d.Floats) == 0 && len(d.Histograms) == 0 {
				continue
			}

			return nil, fmt.Errorf("expected exactly one sample for series %s, but got %v floats, %v histograms", s.Labels.String(), len(d.Floats), len(d.Histograms))
		}

		types.PutInstantVectorSeriesData(d, q.memoryConsumptionTracker)
	}

	return v, nil
}

func (q *Query) populateMatrixFromInstantVectorOperator(ctx context.Context, o types.InstantVectorOperator, series []types.SeriesMetadata) (promql.Matrix, error) {
	m := types.GetMatrix(len(series))

	for i, s := range series {
		d, err := o.NextSeries(ctx)
		if err != nil {
			if errors.Is(err, types.EOS) {
				return nil, fmt.Errorf("expected %v series, but only received %v", len(series), i)
			}

			return nil, err
		}

		if len(d.Floats) == 0 && len(d.Histograms) == 0 {
			types.PutInstantVectorSeriesData(d, q.memoryConsumptionTracker)
			continue
		}

		m = append(m, promql.Series{
			Metric:     s.Labels,
			Floats:     d.Floats,
			Histograms: d.Histograms,
		})
	}

	if len(m) == 0 {
		return nil, nil
	}

	slices.SortFunc(m, func(a, b promql.Series) int {
		return labels.Compare(a.Metric, b.Metric)
	})

	return m, nil
}

func (q *Query) populateMatrixFromRangeVectorOperator(ctx context.Context, o types.RangeVectorOperator, series []types.SeriesMetadata) (promql.Matrix, error) {
	m := types.GetMatrix(len(series))

	for i, s := range series {
		err := o.NextSeries(ctx)
		if err != nil {
			if errors.Is(err, types.EOS) {
				return nil, fmt.Errorf("expected %v series, but only received %v", len(series), i)
			}

			return nil, err
		}

		step, err := o.NextStepSamples()
		if err != nil {
			return nil, err
		}

		floats, err := step.Floats.CopyPoints()
		if err != nil {
			return nil, err
		}

		histograms, err := step.Histograms.CopyPoints()
		if err != nil {
			return nil, err
		}

		if len(floats) == 0 && len(histograms) == 0 {
			types.FPointSlicePool.Put(floats, q.memoryConsumptionTracker)
			types.HPointSlicePool.Put(histograms, q.memoryConsumptionTracker)
			continue
		}

		m = append(m, promql.Series{
			Metric:     s.Labels,
			Floats:     floats,
			Histograms: histograms,
		})
	}

	slices.SortFunc(m, func(a, b promql.Series) int {
		return labels.Compare(a.Metric, b.Metric)
	})

	return m, nil
}

func (q *Query) populateMatrixFromScalarOperator(d types.ScalarData) promql.Matrix {
	return promql.Matrix{
		{
			Metric: labels.EmptyLabels(),
			Floats: d.Samples,
		},
	}
}

func (q *Query) populateScalarFromScalarOperator(d types.ScalarData) promql.Scalar {
	defer types.FPointSlicePool.Put(d.Samples, q.memoryConsumptionTracker)

	p := d.Samples[0]

	return promql.Scalar{
		T: p.T,
		V: p.F,
	}
}

func (q *Query) Close() {
	if q.cancel != nil {
		q.cancel(errQueryClosed)
	}

	if q.result == nil {
		return
	}

	switch v := q.result.Value.(type) {
	case promql.Matrix:
		for _, s := range v {
			types.FPointSlicePool.Put(s.Floats, q.memoryConsumptionTracker)
			types.HPointSlicePool.Put(s.Histograms, q.memoryConsumptionTracker)
		}

		types.PutMatrix(v)
	case promql.Vector:
		types.VectorPool.Put(v, q.memoryConsumptionTracker)
	case promql.Scalar:
		// Nothing to do, we already returned the slice in populateScalarFromScalarOperator.
	case promql.String:
		// Nothing to do as strings don't come from a pool
	default:
		panic(fmt.Sprintf("unknown result value type %T", q.result.Value))
	}

	if q.engine.pedantic && q.result.Err == nil {
		if q.memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes > 0 {
			panic("Memory consumption tracker still estimates > 0 bytes used. This indicates something has not been returned to a pool.")
		}
	}
}

func (q *Query) Statement() parser.Statement {
	return q.statement
}

func (q *Query) Stats() *stats.Statistics {
	return &stats.Statistics{
		Timers: stats.NewQueryTimers(),
		Samples: &stats.QuerySamples{
			TotalSamples: q.stats.TotalSamples,
		},
	}
}

func (q *Query) Cancel() {
	if q.cancel != nil {
		q.cancel(errQueryCancelled)
	}
}

func (q *Query) String() string {
	return q.originalExpression
}
