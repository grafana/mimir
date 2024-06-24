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
	"github.com/prometheus/prometheus/util/stats"
	"golang.org/x/exp/slices"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/pooling"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

var errQueryCancelled = cancellation.NewErrorf("query execution cancelled")
var errQueryClosed = cancellation.NewErrorf("Query.Close() called")
var errQueryFinished = cancellation.NewErrorf("query execution finished")

type Query struct {
	queryable storage.Queryable
	opts      promql.QueryOpts
	statement *parser.EvalStmt
	root      types.Operator
	engine    *Engine
	qs        string
	cancel    context.CancelCauseFunc
	pool      *pooling.LimitingPool

	result *promql.Result
}

func newQuery(ctx context.Context, queryable storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration, engine *Engine) (*Query, error) {
	if opts == nil {
		opts = promql.NewPrometheusQueryOpts(false, 0)
	}

	maxInMemorySamples, err := engine.limitsProvider.GetMaxEstimatedMemoryConsumptionPerQuery(ctx)
	if err != nil {
		return nil, err
	}

	expr, err := parser.ParseExpr(qs)
	if err != nil {
		return nil, err
	}

	expr = promql.PreprocessExpr(expr, start, end)

	q := &Query{
		queryable: queryable,
		opts:      opts,
		engine:    engine,
		qs:        qs,
		pool:      pooling.NewLimitingPool(maxInMemorySamples, engine.queriesRejectedDueToPeakMemoryConsumption),
		statement: &parser.EvalStmt{
			Expr:          expr,
			Start:         start,
			End:           end,
			Interval:      interval,
			LookbackDelta: opts.LookbackDelta(),
		},
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
	default:
		return nil, compat.NewNotSupportedError(fmt.Sprintf("%s value as top-level expression", parser.DocumentedType(expr.Type())))
	}
}

func (q *Query) convertToInstantVectorOperator(expr parser.Expr) (types.InstantVectorOperator, error) {
	if expr.Type() != parser.ValueTypeVector {
		return nil, fmt.Errorf("cannot create instant vector operator for expression that produces a %s", parser.DocumentedType(expr.Type()))
	}

	interval := q.statement.Interval

	if q.IsInstant() {
		interval = time.Millisecond
	}

	switch e := expr.(type) {
	case *parser.VectorSelector:
		lookbackDelta := q.opts.LookbackDelta()
		if lookbackDelta == 0 {
			lookbackDelta = q.engine.lookbackDelta
		}

		if e.OriginalOffset != 0 || e.Offset != 0 {
			return nil, compat.NewNotSupportedError("instant vector selector with 'offset'")
		}

		return &operators.InstantVectorSelector{
			Pool: q.pool,
			Selector: &operators.Selector{
				Queryable:     q.queryable,
				Start:         timestamp.FromTime(q.statement.Start),
				End:           timestamp.FromTime(q.statement.End),
				Timestamp:     e.Timestamp,
				Interval:      interval.Milliseconds(),
				LookbackDelta: lookbackDelta,
				Matchers:      e.LabelMatchers,
			},
		}, nil
	case *parser.AggregateExpr:
		if e.Op != parser.SUM {
			return nil, compat.NewNotSupportedError(fmt.Sprintf("'%s' aggregation", e.Op))
		}

		if e.Param != nil {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("unexpected parameter for %s aggregation: %s", e.Op, e.Param)
		}

		if e.Without {
			return nil, compat.NewNotSupportedError("grouping with 'without'")
		}

		slices.Sort(e.Grouping)

		inner, err := q.convertToInstantVectorOperator(e.Expr)
		if err != nil {
			return nil, err
		}

		return &operators.Aggregation{
			Inner:    inner,
			Start:    q.statement.Start,
			End:      q.statement.End,
			Interval: interval,
			Grouping: e.Grouping,
			Pool:     q.pool,
		}, nil
	case *parser.Call:
		return q.convertFunctionCallToOperator(e)
	case *parser.BinaryExpr:
		if e.LHS.Type() != parser.ValueTypeVector || e.RHS.Type() != parser.ValueTypeVector {
			return nil, compat.NewNotSupportedError("binary expression with scalars")
		}

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

		return operators.NewBinaryOperation(lhs, rhs, *e.VectorMatching, e.Op, q.pool)
	case *parser.StepInvariantExpr:
		// One day, we'll do something smarter here.
		return q.convertToInstantVectorOperator(e.Expr)
	case *parser.ParenExpr:
		return q.convertToInstantVectorOperator(e.Expr)
	default:
		return nil, compat.NewNotSupportedError(fmt.Sprintf("PromQL expression type %T", e))
	}
}

func (q *Query) convertFunctionCallToOperator(e *parser.Call) (types.InstantVectorOperator, error) {
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

	return factory(args, q.pool)
}

func (q *Query) convertToRangeVectorOperator(expr parser.Expr) (types.RangeVectorOperator, error) {
	if expr.Type() != parser.ValueTypeMatrix {
		return nil, fmt.Errorf("cannot create range vector operator for expression that produces a %s", parser.DocumentedType(expr.Type()))
	}

	switch e := expr.(type) {
	case *parser.MatrixSelector:
		vectorSelector := e.VectorSelector.(*parser.VectorSelector)

		if vectorSelector.OriginalOffset != 0 || vectorSelector.Offset != 0 {
			return nil, compat.NewNotSupportedError("range vector selector with 'offset'")
		}

		interval := q.statement.Interval

		if q.IsInstant() {
			interval = time.Millisecond
		}

		return &operators.RangeVectorSelector{
			Selector: &operators.Selector{
				Queryable: q.queryable,
				Start:     timestamp.FromTime(q.statement.Start),
				End:       timestamp.FromTime(q.statement.End),
				Timestamp: vectorSelector.Timestamp,
				Interval:  interval.Milliseconds(),
				Range:     e.Range,
				Matchers:  vectorSelector.LabelMatchers,
			},
		}, nil
	case *parser.StepInvariantExpr:
		// One day, we'll do something smarter here.
		return q.convertToRangeVectorOperator(e.Expr)
	case *parser.ParenExpr:
		return q.convertToRangeVectorOperator(e.Expr)
	default:
		return nil, compat.NewNotSupportedError(fmt.Sprintf("PromQL expression type %T", e))
	}
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
		level.Info(logger).Log("msg", "query stats", "estimatedPeakMemoryConsumption", q.pool.PeakEstimatedMemoryConsumptionBytes)
		q.engine.estimatedPeakMemoryConsumption.Observe(float64(q.pool.PeakEstimatedMemoryConsumptionBytes))
	}()

	series, err := q.root.SeriesMetadata(ctx)
	if err != nil {
		return &promql.Result{Err: err}
	}
	defer pooling.PutSeriesMetadataSlice(series)

	switch q.statement.Expr.Type() {
	case parser.ValueTypeMatrix:
		v, err := q.populateMatrixFromRangeVectorOperator(ctx, q.root.(types.RangeVectorOperator), series)
		if err != nil {
			return &promql.Result{Err: err}
		}

		q.result = &promql.Result{Value: v}
	case parser.ValueTypeVector:
		if q.IsInstant() {
			v, err := q.populateVectorFromInstantVectorOperator(ctx, q.root.(types.InstantVectorOperator), series)
			if err != nil {
				return &promql.Result{Err: err}
			}

			q.result = &promql.Result{Value: v}
		} else {
			v, err := q.populateMatrixFromInstantVectorOperator(ctx, q.root.(types.InstantVectorOperator), series)
			if err != nil {
				return &promql.Result{Err: err}
			}

			q.result = &promql.Result{Value: v}
		}
	default:
		// This should be caught in newQuery above.
		return &promql.Result{Err: compat.NewNotSupportedError(fmt.Sprintf("unsupported result type %s", parser.DocumentedType(q.statement.Expr.Type())))}
	}

	return q.result
}

func (q *Query) populateVectorFromInstantVectorOperator(ctx context.Context, o types.InstantVectorOperator, series []types.SeriesMetadata) (promql.Vector, error) {
	ts := timeMilliseconds(q.statement.Start)
	v, err := q.pool.GetVector(len(series))
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
			q.pool.PutInstantVectorSeriesData(d)
			// A series may have no data points.
			if len(d.Floats) == 0 && len(d.Histograms) == 0 {
				continue
			}
			return nil, fmt.Errorf("expected exactly one sample for series %s, but got %v floats, %v histograms", s.Labels.String(), len(d.Floats), len(d.Histograms))
		}

		q.pool.PutInstantVectorSeriesData(d)
	}

	return v, nil
}

func (q *Query) populateMatrixFromInstantVectorOperator(ctx context.Context, o types.InstantVectorOperator, series []types.SeriesMetadata) (promql.Matrix, error) {
	m := pooling.GetMatrix(len(series))

	for i, s := range series {
		d, err := o.NextSeries(ctx)
		if err != nil {
			if errors.Is(err, types.EOS) {
				return nil, fmt.Errorf("expected %v series, but only received %v", len(series), i)
			}

			return nil, err
		}

		if len(d.Floats) == 0 && len(d.Histograms) == 0 {
			q.pool.PutInstantVectorSeriesData(d)
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
	m := pooling.GetMatrix(len(series))
	b := types.NewRingBuffer(q.pool)
	defer b.Close()

	for i, s := range series {
		err := o.NextSeries(ctx)
		if err != nil {
			if errors.Is(err, types.EOS) {
				return nil, fmt.Errorf("expected %v series, but only received %v", len(series), i)
			}

			return nil, err
		}

		b.Reset()
		step, err := o.NextStepSamples(b)
		if err != nil {
			return nil, err
		}

		floats, err := b.CopyPoints(step.RangeEnd)
		if err != nil {
			return nil, err
		}

		m = append(m, promql.Series{
			Metric: s.Labels,
			Floats: floats,
		})
	}

	slices.SortFunc(m, func(a, b promql.Series) int {
		return labels.Compare(a.Metric, b.Metric)
	})

	return m, nil
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
			q.pool.PutFPointSlice(s.Floats)
			q.pool.PutHPointSlice(s.Histograms)
		}

		pooling.PutMatrix(v)
	case promql.Vector:
		q.pool.PutVector(v)
	default:
		panic(fmt.Sprintf("unknown result value type %T", q.result.Value))
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
