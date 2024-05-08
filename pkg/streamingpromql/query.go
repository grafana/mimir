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

	"github.com/grafana/mimir/pkg/streamingpromql/operator"
)

type Query struct {
	queryable storage.Queryable
	opts      promql.QueryOpts
	statement *parser.EvalStmt
	root      operator.InstantVectorOperator
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

	q.root, err = q.convertToOperator(expr)
	if err != nil {
		return nil, err
	}

	return q, nil
}

func (q *Query) convertToOperator(expr parser.Expr) (operator.InstantVectorOperator, error) {
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
			return nil, NewNotSupportedError("instant vector selector with 'offset'")
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
			return nil, NewNotSupportedError(fmt.Sprintf("'%s' aggregation", e.Op))
		}

		if e.Param != nil {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("unexpected parameter for %s aggregation: %s", e.Op, e.Param)
		}

		if e.Without {
			return nil, NewNotSupportedError("grouping with 'without'")
		}

		slices.Sort(e.Grouping)

		inner, err := q.convertToOperator(e.Expr)
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
			return nil, NewNotSupportedError(fmt.Sprintf("'%s' function", e.Func.Name))
		}

		if len(e.Args) != 1 {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, fmt.Errorf("expected exactly one argument for rate, got %v", len(e.Args))
		}

		matrixSelector, ok := e.Args[0].(*parser.MatrixSelector)
		if !ok {
			// Should be caught by the PromQL parser, but we check here for safety.
			return nil, NewNotSupportedError(fmt.Sprintf("unsupported rate argument type %T", e.Args[0]))
		}

		vectorSelector := matrixSelector.VectorSelector.(*parser.VectorSelector)

		if vectorSelector.OriginalOffset != 0 || vectorSelector.Offset != 0 {
			return nil, NewNotSupportedError("range vector selector with 'offset'")
		}

		return &operator.RangeVectorSelectorWithTransformation{
			Selector: &operator.Selector{
				Queryable: q.queryable,
				Start:     timestamp.FromTime(q.statement.Start),
				End:       timestamp.FromTime(q.statement.End),
				Timestamp: vectorSelector.Timestamp,
				Interval:  interval.Milliseconds(),
				Range:     matrixSelector.Range,
				Matchers:  vectorSelector.LabelMatchers,
			},
		}, nil
	case *parser.BinaryExpr:
		if e.LHS.Type() != parser.ValueTypeVector || e.RHS.Type() != parser.ValueTypeVector {
			return nil, NewNotSupportedError("binary expression with scalars")
		}

		if e.VectorMatching.Card != parser.CardOneToOne {
			return nil, NewNotSupportedError(fmt.Sprintf("binary expression with %v matching", e.VectorMatching.Card))
		}

		if e.Op.IsComparisonOperator() || e.Op.IsSetOperator() {
			return nil, NewNotSupportedError(fmt.Sprintf("binary expression with '%s'", e.Op))
		}

		lhs, err := q.convertToOperator(e.LHS)
		if err != nil {
			return nil, err
		}

		rhs, err := q.convertToOperator(e.RHS)
		if err != nil {
			return nil, err
		}

		return &operator.BinaryOperation{
			Left:           lhs,
			Right:          rhs,
			VectorMatching: *e.VectorMatching,
			Op:             e.Op,
		}, nil
	case *parser.StepInvariantExpr:
		// One day, we'll do something smarter here.
		return q.convertToOperator(e.Expr)
	case *parser.ParenExpr:
		return q.convertToOperator(e.Expr)
	default:
		return nil, NewNotSupportedError(fmt.Sprintf("PromQL expression type %T", e))
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

	if q.IsInstant() {
		v, err := q.populateVector(ctx, series)
		if err != nil {
			return &promql.Result{Err: err}
		}

		q.result = &promql.Result{Value: v}
	} else {
		m, err := q.populateMatrix(ctx, series)
		if err != nil {
			return &promql.Result{Value: m}
		}

		q.result = &promql.Result{Value: m}
	}

	return q.result
}

func (q *Query) populateVector(ctx context.Context, series []operator.SeriesMetadata) (promql.Vector, error) {
	ts := timeMilliseconds(q.statement.Start)
	v := operator.GetVector(len(series))

	for i, s := range series {
		d, err := q.root.Next(ctx)
		if err != nil {
			if errors.Is(err, operator.EOS) {
				return nil, fmt.Errorf("expected %v series, but only received %v", len(series), i)
			}

			return nil, err
		}

		if len(d.Floats)+len(d.Histograms) != 1 {
			operator.PutFPointSlice(d.Floats)
			// TODO: put histogram point slice back in pool

			if len(d.Floats)+len(d.Histograms) == 0 {
				continue
			}

			return nil, fmt.Errorf("expected exactly one sample for series %s, but got %v", s.Labels.String(), len(d.Floats))
		}

		point := d.Floats[0]
		v = append(v, promql.Sample{
			Metric: s.Labels,
			T:      ts,
			F:      point.F,
		})

		operator.PutFPointSlice(d.Floats)
		// TODO: put histogram point slice back in pool
	}

	return v, nil
}

func (q *Query) populateMatrix(ctx context.Context, series []operator.SeriesMetadata) (promql.Matrix, error) {
	m := operator.GetMatrix(len(series))

	for i, s := range series {
		d, err := q.root.Next(ctx)
		if err != nil {
			if errors.Is(err, operator.EOS) {
				return nil, fmt.Errorf("expected %v series, but only received %v", len(series), i)
			}

			return nil, err
		}

		if len(d.Floats) == 0 && len(d.Histograms) == 0 {
			operator.PutFPointSlice(d.Floats)
			// TODO: put histogram point slice back in pool

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

func (q *Query) Close() {
	if q.result == nil {
		return
	}

	switch v := q.result.Value.(type) {
	case promql.Matrix:
		for _, s := range v {
			operator.PutFPointSlice(s.Floats)
			// TODO: put histogram point slice back in pool
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
