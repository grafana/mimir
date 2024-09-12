// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streamingpromql

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cancellation"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/prometheus/prometheus/util/stats"
	"golang.org/x/exp/slices"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

var errQueryCancelled = cancellation.NewErrorf("query execution cancelled")
var errQueryClosed = cancellation.NewErrorf("Query.Close() called")
var errQueryFinished = cancellation.NewErrorf("query execution finished")

type Query struct {
	queryable                storage.Queryable
	opts                     promql.QueryOpts
	statement                *parser.EvalStmt
	root                     types.Operator
	engine                   *Engine
	qs                       string
	cancel                   context.CancelCauseFunc
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker
	annotations              *annotations.Annotations

	startT     int64 // Start timestamp, in milliseconds since Unix epoch.
	endT       int64 // End timestamp, in milliseconds since Unix epoch.
	intervalMs int64 // Range query interval, or 1 for instant queries. Note that this is deliberately different to statement.Interval for instant queries (where it is 0) to simplify some loop conditions.

	result *promql.Result
}

func newQuery(ctx context.Context, queryable storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration, engine *Engine) (*Query, error) {
	if opts == nil {
		opts = promql.NewPrometheusQueryOpts(false, 0)
	}

	maxEstimatedMemoryConsumptionPerQuery, err := engine.limitsProvider.GetMaxEstimatedMemoryConsumptionPerQuery(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not get memory consumption limit for query: %w", err)
	}

	expr, err := parser.ParseExpr(qs)
	if err != nil {
		return nil, err
	}

	expr = promql.PreprocessExpr(expr, start, end)

	q := &Query{
		queryable:                queryable,
		opts:                     opts,
		engine:                   engine,
		qs:                       qs,
		memoryConsumptionTracker: limiting.NewMemoryConsumptionTracker(maxEstimatedMemoryConsumptionPerQuery, engine.queriesRejectedDueToPeakMemoryConsumption),
		annotations:              annotations.New(),

		statement: &parser.EvalStmt{
			Expr:          expr,
			Start:         start,
			End:           end,
			Interval:      interval, // 0 for instant queries
			LookbackDelta: opts.LookbackDelta(),
		},

		startT:     timestamp.FromTime(start),
		endT:       timestamp.FromTime(end),
		intervalMs: interval.Milliseconds(), // 1 for instant queries (set below)
	}

	if q.IsInstant() {
		q.intervalMs = 1
	}

	if !q.IsInstant() {
		if expr.Type() != parser.ValueTypeVector && expr.Type() != parser.ValueTypeScalar {
			return nil, fmt.Errorf("query expression produces a %s, but expression for range queries must produce an instant vector or scalar", parser.DocumentedType(expr.Type()))
		}
	}
	q.root, err = q.convertToOperator(expr)
	if err != nil {
		return nil, err
	}

	return q, nil
}

func (q *Query) convertToOperator(expr parser.Expr) (types.Operator, error) {
	switch expr.Type() {
	case parser.ValueTypeMatrix:
		return q.convertToRangeVectorOperator(expr)
	case parser.ValueTypeVector:
		return q.convertToInstantVectorOperator(expr)
	case parser.ValueTypeScalar:
		return q.convertToScalarOperator(expr)
	default:
		return nil, compat.NewNotSupportedError(fmt.Sprintf("%s value as top-level expression", parser.DocumentedType(expr.Type())))
	}
}

func (q *Query) convertToInstantVectorOperator(expr parser.Expr) (types.InstantVectorOperator, error) {
	if expr.Type() != parser.ValueTypeVector {
		return nil, fmt.Errorf("cannot create instant vector operator for expression that produces a %s", parser.DocumentedType(expr.Type()))
	}

	switch e := expr.(type) {
	case *parser.VectorSelector:
		lookbackDelta := q.opts.LookbackDelta()
		if lookbackDelta == 0 {
			lookbackDelta = q.engine.lookbackDelta
		}

		if !q.engine.featureToggles.EnableOffsetModifier && (e.OriginalOffset != 0 || e.Offset != 0) {
			return nil, compat.NewNotSupportedError("instant vector selector with 'offset'")
		}

		return &operators.InstantVectorSelector{
			MemoryConsumptionTracker: q.memoryConsumptionTracker,
			Selector: &operators.Selector{
				Queryable:     q.queryable,
				Start:         q.startT,
				End:           q.endT,
				Timestamp:     e.Timestamp,
				Interval:      q.intervalMs,
				Offset:        e.OriginalOffset.Milliseconds(),
				LookbackDelta: lookbackDelta,
				Matchers:      e.LabelMatchers,

				ExpressionPosition: e.PositionRange(),
			},
		}, nil
	case *parser.AggregateExpr:
		if !q.engine.featureToggles.EnableAggregationOperations {
			return nil, compat.NewNotSupportedError("aggregation operations")
		}

		if e.Param != nil {
			return nil, compat.NewNotSupportedError(fmt.Sprintf("'%s' aggregation with parameter", e.Op))
		}

		inner, err := q.convertToInstantVectorOperator(e.Expr)
		if err != nil {
			return nil, err
		}

		return operators.NewAggregation(
			inner,
			q.startT,
			q.endT,
			q.intervalMs,
			e.Grouping,
			e.Without,
			e.Op,
			q.memoryConsumptionTracker,
			q.annotations,
			e.PosRange,
		)
	case *parser.Call:
		return q.convertFunctionCallToInstantVectorOperator(e)
	case *parser.BinaryExpr:
		if !q.engine.featureToggles.EnableBinaryOperations {
			return nil, compat.NewNotSupportedError("binary expressions")
		}

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
				scalar, err = q.convertToScalarOperator(e.LHS)
				if err != nil {
					return nil, err
				}

				vector, err = q.convertToInstantVectorOperator(e.RHS)
				if err != nil {
					return nil, err
				}
			} else {
				scalar, err = q.convertToScalarOperator(e.RHS)
				if err != nil {
					return nil, err
				}

				vector, err = q.convertToInstantVectorOperator(e.LHS)
				if err != nil {
					return nil, err
				}
			}

			scalarIsLeftSide := e.LHS.Type() == parser.ValueTypeScalar

			o, err := operators.NewVectorScalarBinaryOperation(scalar, vector, scalarIsLeftSide, e.Op, q.startT, q.endT, q.intervalMs, q.memoryConsumptionTracker, q.annotations, e.PositionRange())
			if err != nil {
				return nil, err
			}

			return operators.NewDeduplicateAndMerge(o, q.memoryConsumptionTracker), err
		}

		// Vectors on both sides.

		if e.VectorMatching.Card != parser.CardOneToOne {
			return nil, compat.NewNotSupportedError(fmt.Sprintf("binary expression with %v matching", e.VectorMatching.Card))
		}

		lhs, err := q.convertToInstantVectorOperator(e.LHS)
		if err != nil {
			return nil, err
		}

		rhs, err := q.convertToInstantVectorOperator(e.RHS)
		if err != nil {
			return nil, err
		}

		return operators.NewVectorVectorBinaryOperation(lhs, rhs, *e.VectorMatching, e.Op, q.memoryConsumptionTracker, q.annotations, e.PositionRange())
	case *parser.UnaryExpr:
		if e.Op != parser.SUB {
			return nil, compat.NewNotSupportedError(fmt.Sprintf("unary expression with '%s'", e.Op))
		}

		if !q.engine.featureToggles.EnableUnaryNegation {
			return nil, compat.NewNotSupportedError("unary negation of instant vectors")
		}

		inner, err := q.convertToInstantVectorOperator(e.Expr)
		if err != nil {
			return nil, err
		}

		return unaryNegationOfInstantVectorOperatorFactory(inner, q.memoryConsumptionTracker, e.PositionRange()), nil

	case *parser.StepInvariantExpr:
		// One day, we'll do something smarter here.
		return q.convertToInstantVectorOperator(e.Expr)
	case *parser.ParenExpr:
		return q.convertToInstantVectorOperator(e.Expr)
	default:
		return nil, compat.NewNotSupportedError(fmt.Sprintf("PromQL expression type %T for instant vectors", e))
	}
}

func (q *Query) convertFunctionCallToInstantVectorOperator(e *parser.Call) (types.InstantVectorOperator, error) {
	if !q.engine.featureToggles.EnableOverTimeFunctions && slices.Contains(overTimeFunctionNames, e.Func.Name) {
		return nil, compat.NewNotSupportedError(fmt.Sprintf("'%s' function", e.Func.Name))
	}

	factory, ok := instantVectorFunctionOperatorFactories[e.Func.Name]
	if !ok {
		return nil, compat.NewNotSupportedError(fmt.Sprintf("'%s' function", e.Func.Name))
	}

	args := make([]types.Operator, len(e.Args))
	for i := range e.Args {
		a, err := q.convertToOperator(e.Args[i])
		if err != nil {
			return nil, err
		}
		args[i] = a
	}

	return factory(args, q.memoryConsumptionTracker, q.annotations, e.PosRange)
}

func (q *Query) convertToRangeVectorOperator(expr parser.Expr) (types.RangeVectorOperator, error) {
	if expr.Type() != parser.ValueTypeMatrix {
		return nil, fmt.Errorf("cannot create range vector operator for expression that produces a %s", parser.DocumentedType(expr.Type()))
	}

	switch e := expr.(type) {
	case *parser.MatrixSelector:
		vectorSelector := e.VectorSelector.(*parser.VectorSelector)

		if !q.engine.featureToggles.EnableOffsetModifier && (vectorSelector.OriginalOffset != 0 || vectorSelector.Offset != 0) {
			return nil, compat.NewNotSupportedError("range vector selector with 'offset'")
		}

		return &operators.RangeVectorSelector{
			Selector: &operators.Selector{
				Queryable: q.queryable,
				Start:     q.startT,
				End:       q.endT,
				Timestamp: vectorSelector.Timestamp,
				Interval:  q.intervalMs,
				Offset:    vectorSelector.OriginalOffset.Milliseconds(),
				Range:     e.Range,
				Matchers:  vectorSelector.LabelMatchers,

				ExpressionPosition: e.PositionRange(),
			},
		}, nil
	case *parser.StepInvariantExpr:
		// One day, we'll do something smarter here.
		return q.convertToRangeVectorOperator(e.Expr)
	case *parser.ParenExpr:
		return q.convertToRangeVectorOperator(e.Expr)
	default:
		return nil, compat.NewNotSupportedError(fmt.Sprintf("PromQL expression type %T for range vectors", e))
	}
}

func (q *Query) convertToScalarOperator(expr parser.Expr) (types.ScalarOperator, error) {
	if expr.Type() != parser.ValueTypeScalar {
		return nil, fmt.Errorf("cannot create scalar operator for expression that produces a %s", parser.DocumentedType(expr.Type()))
	}

	if !q.engine.featureToggles.EnableScalars {
		return nil, compat.NewNotSupportedError("scalar values")
	}

	switch e := expr.(type) {
	case *parser.NumberLiteral:
		o := operators.NewScalarConstant(
			e.Val,
			q.startT,
			q.endT,
			q.intervalMs,
			q.memoryConsumptionTracker,
			e.PositionRange(),
		)

		return o, nil

	case *parser.Call:
		return q.convertFunctionCallToScalarOperator(e)

	case *parser.UnaryExpr:
		if e.Op != parser.SUB {
			return nil, compat.NewNotSupportedError(fmt.Sprintf("unary expression with '%s'", e.Op))
		}

		if !q.engine.featureToggles.EnableUnaryNegation {
			return nil, compat.NewNotSupportedError("unary negation of scalars")
		}

		inner, err := q.convertToScalarOperator(e.Expr)
		if err != nil {
			return nil, err
		}

		return operators.NewUnaryNegationOfScalar(inner, e.PositionRange()), nil

	case *parser.StepInvariantExpr:
		// One day, we'll do something smarter here.
		return q.convertToScalarOperator(e.Expr)
	case *parser.ParenExpr:
		return q.convertToScalarOperator(e.Expr)
	case *parser.BinaryExpr:
		if !q.engine.featureToggles.EnableBinaryOperations {
			return nil, compat.NewNotSupportedError("binary expressions")
		}

		lhs, err := q.convertToScalarOperator(e.LHS)
		if err != nil {
			return nil, err
		}

		rhs, err := q.convertToScalarOperator(e.RHS)
		if err != nil {
			return nil, err
		}

		return operators.NewScalarScalarBinaryOperation(lhs, rhs, e.Op, q.memoryConsumptionTracker, e.PositionRange())

	default:
		return nil, compat.NewNotSupportedError(fmt.Sprintf("PromQL expression type %T for scalars", e))
	}
}

func (q *Query) convertFunctionCallToScalarOperator(e *parser.Call) (types.ScalarOperator, error) {
	factory, ok := scalarFunctionOperatorFactories[e.Func.Name]
	if !ok {
		return nil, compat.NewNotSupportedError(fmt.Sprintf("'%s' function", e.Func.Name))
	}

	args := make([]types.Operator, len(e.Args))
	for i := range e.Args {
		a, err := q.convertToOperator(e.Args[i])
		if err != nil {
			return nil, err
		}
		args[i] = a
	}

	return factory(args, q.memoryConsumptionTracker, q.annotations, e.PosRange, q.startT, q.endT, q.intervalMs)
}

func (q *Query) IsInstant() bool {
	return q.statement.Start == q.statement.End && q.statement.Interval == 0
}

func (q *Query) Exec(ctx context.Context) *promql.Result {
	defer q.root.Close()

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

	if q.engine.activeQueryTracker != nil {
		queryID, err := q.engine.activeQueryTracker.Insert(ctx, q.qs)
		if err != nil {
			return &promql.Result{Err: err}
		}

		defer q.engine.activeQueryTracker.Delete(queryID)
	}

	defer func() {
		logger := spanlogger.FromContext(ctx, q.engine.logger)
		level.Info(logger).Log("msg", "query stats", "estimatedPeakMemoryConsumption", q.memoryConsumptionTracker.PeakEstimatedMemoryConsumptionBytes)
		q.engine.estimatedPeakMemoryConsumption.Observe(float64(q.memoryConsumptionTracker.PeakEstimatedMemoryConsumptionBytes))
	}()

	switch q.statement.Expr.Type() {
	case parser.ValueTypeMatrix:
		root := q.root.(types.RangeVectorOperator)
		series, err := root.SeriesMetadata(ctx)
		if err != nil {
			return &promql.Result{Err: err}
		}
		defer types.PutSeriesMetadataSlice(series)

		v, err := q.populateMatrixFromRangeVectorOperator(ctx, root, series)
		if err != nil {
			return &promql.Result{Err: err}
		}

		q.result = &promql.Result{Value: v}
	case parser.ValueTypeVector:
		root := q.root.(types.InstantVectorOperator)
		series, err := root.SeriesMetadata(ctx)
		if err != nil {
			return &promql.Result{Err: err}
		}
		defer types.PutSeriesMetadataSlice(series)

		if q.IsInstant() {
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
	case parser.ValueTypeScalar:
		root := q.root.(types.ScalarOperator)
		d, err := root.GetValues(ctx)
		if err != nil {
			return &promql.Result{Err: err}
		}

		if q.IsInstant() {
			q.result = &promql.Result{Value: q.populateScalarFromScalarOperator(d)}
		} else {
			q.result = &promql.Result{Value: q.populateMatrixFromScalarOperator(d)}
		}

	default:
		// This should be caught in newQuery above.
		return &promql.Result{Err: compat.NewNotSupportedError(fmt.Sprintf("unsupported result type %s", parser.DocumentedType(q.statement.Expr.Type())))}
	}

	q.result.Warnings = *q.annotations

	return q.result
}

func (q *Query) populateVectorFromInstantVectorOperator(ctx context.Context, o types.InstantVectorOperator, series []types.SeriesMetadata) (promql.Vector, error) {
	ts := timeMilliseconds(q.statement.Start)
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

	slices.SortFunc(m, func(a, b promql.Series) int {
		return labels.Compare(a.Metric, b.Metric)
	})

	return m, nil
}

func (q *Query) populateMatrixFromRangeVectorOperator(ctx context.Context, o types.RangeVectorOperator, series []types.SeriesMetadata) (promql.Matrix, error) {
	m := types.GetMatrix(len(series))
	floatBuffer := types.NewFPointRingBuffer(q.memoryConsumptionTracker)
	histogramBuffer := types.NewHPointRingBuffer(q.memoryConsumptionTracker)
	defer floatBuffer.Close()
	defer histogramBuffer.Close()

	for i, s := range series {
		err := o.NextSeries(ctx)
		if err != nil {
			if errors.Is(err, types.EOS) {
				return nil, fmt.Errorf("expected %v series, but only received %v", len(series), i)
			}

			return nil, err
		}

		floatBuffer.Reset()
		histogramBuffer.Reset()
		step, err := o.NextStepSamples(floatBuffer, histogramBuffer)
		if err != nil {
			return nil, err
		}

		floats, err := floatBuffer.CopyPoints(step.RangeEnd)
		if err != nil {
			return nil, err
		}

		histograms, err := histogramBuffer.CopyPoints(step.RangeEnd)
		if err != nil {
			return nil, err
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
	// Not yet supported.
	return nil
}

func (q *Query) Cancel() {
	if q.cancel != nil {
		q.cancel(errQueryCancelled)
	}
}

func (q *Query) String() string {
	return q.qs
}

func timeMilliseconds(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond/time.Nanosecond)
}
