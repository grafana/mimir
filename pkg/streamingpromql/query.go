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

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/stats"
	"golang.org/x/exp/slices"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/operator"
)

type Query struct {
	queryable storage.Queryable
	opts      promql.QueryOpts
	statement *parser.EvalStmt
	root      operator.Operator
	engine    *Engine
	qs        string

	result *promql.Result
}

func newQuery(queryable storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration, engine *Engine) (*Query, error) {
	if opts == nil {
		opts = promql.NewPrometheusQueryOpts(false, 0)
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

	switch expr.Type() {
	case parser.ValueTypeMatrix:
		q.root, err = q.convertToRangeVectorOperator(expr)
		if err != nil {
			return nil, err
		}
	case parser.ValueTypeVector:
		q.root, err = q.convertToInstantVectorOperator(expr)
		if err != nil {
			return nil, err
		}
	default:
		return nil, compat.NewNotSupportedError(fmt.Sprintf("%s value as top-level expression", parser.DocumentedType(expr.Type())))
	}

	return q, nil
}

func (q *Query) convertToInstantVectorOperator(expr parser.Expr) (operator.InstantVectorOperator, error) {
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

		return &operator.InstantVectorSelector{
			Selector: &operator.Selector{
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

		return &operator.Aggregation{
			Inner:    inner,
			Start:    q.statement.Start,
			End:      q.statement.End,
			Interval: interval,
			Grouping: e.Grouping,
		}, nil
	case *parser.Call:
		if e.Func.Name != "rate" {
			return nil, compat.NewNotSupportedError(fmt.Sprintf("'%s' function", e.Func.Name))
		}

		if len(e.Args) != 1 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected exactly one argument for rate, got %v", len(e.Args))
		}

		inner, err := q.convertToRangeVectorOperator(e.Args[0])
		if err != nil {
			return nil, err
		}

		return &operator.RangeVectorFunction{
			Inner: inner,
		}, nil
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

		return operator.NewBinaryOperation(lhs, rhs, *e.VectorMatching, e.Op)
	case *parser.StepInvariantExpr:
		// One day, we'll do something smarter here.
		return q.convertToInstantVectorOperator(e.Expr)
	case *parser.ParenExpr:
		return q.convertToInstantVectorOperator(e.Expr)
	default:
		return nil, compat.NewNotSupportedError(fmt.Sprintf("PromQL expression type %T", e))
	}
}

func (q *Query) convertToRangeVectorOperator(expr parser.Expr) (operator.RangeVectorOperator, error) {
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

		return &operator.RangeVectorSelector{
			Selector: &operator.Selector{
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

	series, err := q.root.SeriesMetadata(ctx)
	if err != nil {
		return &promql.Result{Err: err}
	}
	defer operator.PutSeriesMetadataSlice(series)

	switch q.statement.Expr.Type() {
	case parser.ValueTypeMatrix:
		v, err := q.populateMatrixFromRangeVectorOperator(ctx, q.root.(operator.RangeVectorOperator), series)
		if err != nil {
			return &promql.Result{Err: err}
		}

		q.result = &promql.Result{Value: v}
	case parser.ValueTypeVector:
		if q.IsInstant() {
			v, err := q.populateVectorFromInstantVectorOperator(ctx, q.root.(operator.InstantVectorOperator), series)
			if err != nil {
				return &promql.Result{Err: err}
			}

			q.result = &promql.Result{Value: v}
		} else {
			v, err := q.populateMatrixFromInstantVectorOperator(ctx, q.root.(operator.InstantVectorOperator), series)
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

func (q *Query) populateVectorFromInstantVectorOperator(ctx context.Context, o operator.InstantVectorOperator, series []operator.SeriesMetadata) (promql.Vector, error) {
	ts := timeMilliseconds(q.statement.Start)
	v := operator.GetVector(len(series))

	returnSlices := func(f []promql.FPoint, h []promql.HPoint) {
		operator.PutFPointSlice(f)
		operator.PutHPointSlice(h)
	}

	for i, s := range series {
		d, err := o.NextSeries(ctx)
		if err != nil {
			if errors.Is(err, operator.EOS) {
				return nil, fmt.Errorf("expected %v series, but only received %v", len(series), i)
			}

			return nil, err
		}

		// A series may have no data points.
		if len(d.Floats)+len(d.Histograms) == 0 {
			continue
		}

		if len(d.Floats)+len(d.Histograms) != 1 {
			returnSlices(d.Floats, d.Histograms)
			return nil, fmt.Errorf("expected exactly one sample for series %s, but got %v Floats, %v Histograms", s.Labels.String(), len(d.Floats), len(d.Histograms))
		}

		if len(d.Floats) == 1 {
			point := d.Floats[0]
			v = append(v, promql.Sample{
				Metric: s.Labels,
				T:      ts,
				F:      point.F,
			})
		} else if len(d.Histograms) == 1 {
			point := d.Histograms[0]
			v = append(v, promql.Sample{
				Metric: s.Labels,
				T:      ts,
				H:      point.H,
			})
		} else {
			returnSlices(d.Floats, d.Histograms)
			return nil, fmt.Errorf("expected exactly one sample for series %s, but got %v Floats, %v Histograms", s.Labels.String(), len(d.Floats), len(d.Histograms))
		}
		returnSlices(d.Floats, d.Histograms)
	}

	return v, nil
}

func (q *Query) populateMatrixFromInstantVectorOperator(ctx context.Context, o operator.InstantVectorOperator, series []operator.SeriesMetadata) (promql.Matrix, error) {
	m := operator.GetMatrix(len(series))

	for i, s := range series {
		d, err := o.NextSeries(ctx)
		if err != nil {
			if errors.Is(err, operator.EOS) {
				return nil, fmt.Errorf("expected %v series, but only received %v", len(series), i)
			}

			return nil, err
		}

		if len(d.Floats) == 0 && len(d.Histograms) == 0 {
			operator.PutFPointSlice(d.Floats)
			operator.PutHPointSlice(d.Histograms)
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

func (q *Query) populateMatrixFromRangeVectorOperator(ctx context.Context, o operator.RangeVectorOperator, series []operator.SeriesMetadata) (promql.Matrix, error) {
	m := operator.GetMatrix(len(series))
	b := &operator.RingBuffer{}
	defer b.Close()

	for i, s := range series {
		err := o.NextSeries(ctx)
		if err != nil {
			if errors.Is(err, operator.EOS) {
				return nil, fmt.Errorf("expected %v series, but only received %v", len(series), i)
			}

			return nil, err
		}

		b.Reset()
		step, err := o.NextStepSamples(b)
		if err != nil {
			return nil, err
		}

		m = append(m, promql.Series{
			Metric: s.Labels,
			Floats: b.CopyPoints(step.RangeEnd),
		})
	}

	slices.SortFunc(m, func(a, b promql.Series) int {
		return labels.Compare(a.Metric, b.Metric)
	})

	return m, nil
}

func (q *Query) Close() {
	if q.result == nil {
		return
	}

	switch v := q.result.Value.(type) {
	case promql.Matrix:
		for _, s := range v {
			operator.PutFPointSlice(s.Floats)
			operator.PutHPointSlice(s.Histograms)
		}

		operator.PutMatrix(v)
	case promql.Vector:
		operator.PutVector(v)
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
	// Not yet supported.
}

func (q *Query) String() string {
	return q.qs
}

func timeMilliseconds(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond/time.Nanosecond)
}
