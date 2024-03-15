// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streaming

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/stats"
	"golang.org/x/exp/slices"

	"github.com/grafana/mimir/pkg/querier/engine/streaming/operator"
)

type Query struct {
	queryable storage.Queryable
	opts      promql.QueryOpts
	statement *parser.EvalStmt
	root      operator.InstantVectorOperator
	engine    *Engine

	result *promql.Result
}

func NewQuery(queryable storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration, engine *Engine) (*Query, error) {
	if opts == nil {
		opts = promql.NewPrometheusQueryOpts(false, 0)
	}

	expr, err := parser.ParseExpr(qs)
	if err != nil {
		return nil, err
	}

	q := &Query{
		queryable: queryable,
		opts:      opts,
		engine:    engine,
		statement: &parser.EvalStmt{
			Expr:          expr,
			Start:         start,
			End:           end,
			Interval:      interval,
			LookbackDelta: opts.LookbackDelta(),
		},
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

		if e.Timestamp != nil {
			return nil, NewNotSupportedError("instant vector selector with '@' modifier")
		}

		if e.StartOrEnd == parser.START {
			return nil, NewNotSupportedError("instant vector selector with '@ start()'")
		} else if e.StartOrEnd == parser.END {
			return nil, NewNotSupportedError("instant vector selector with '@ end()'")
		}

		return &operator.InstantVectorSelector{
			Queryable:     q.queryable,
			Start:         q.statement.Start,
			End:           q.statement.End,
			Interval:      interval,
			LookbackDelta: lookbackDelta,
			Matchers:      e.LabelMatchers,
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

		lookbackDelta := q.opts.LookbackDelta()
		if lookbackDelta == 0 {
			lookbackDelta = q.engine.lookbackDelta
		}

		vectorSelector := matrixSelector.VectorSelector.(*parser.VectorSelector)

		if vectorSelector.OriginalOffset != 0 || vectorSelector.Offset != 0 {
			return nil, NewNotSupportedError("range vector selector with 'offset'")
		}

		if vectorSelector.Timestamp != nil {
			return nil, NewNotSupportedError("range vector selector with '@' modifier")
		}

		if vectorSelector.StartOrEnd == parser.START {
			return nil, NewNotSupportedError("range vector selector with '@ start()'")
		} else if vectorSelector.StartOrEnd == parser.END {
			return nil, NewNotSupportedError("range vector selector with '@ end()'")
		}

		return &operator.RangeVectorSelectorWithTransformation{
			Queryable:     q.queryable,
			Start:         q.statement.Start,
			End:           q.statement.End,
			Interval:      interval,
			Range:         matrixSelector.Range,
			LookbackDelta: lookbackDelta,
			Matchers:      vectorSelector.LabelMatchers,
		}, nil
	default:
		return nil, NewNotSupportedError(fmt.Sprintf("PromQL expression type %T", e))
	}
}

func (q *Query) IsInstant() bool {
	return q.statement.Start == q.statement.End && q.statement.Interval == 0
}

func (q *Query) Exec(ctx context.Context) *promql.Result {
	series, err := q.root.Series(ctx)
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

		if len(d.Floats) != 1 {
			defer operator.PutFPointSlice(d.Floats)
			return nil, fmt.Errorf("expected exactly one sample for series %s, but got %v", s.Labels.String(), len(d.Floats))
		}

		point := d.Floats[0]
		v = append(v, promql.Sample{
			Metric: s.Labels,
			T:      ts,
			F:      point.F,
		})

		operator.PutFPointSlice(d.Floats)
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

		m = append(m, promql.Series{
			Metric:     s.Labels,
			Floats:     d.Floats,
			Histograms: d.Histograms,
		})
	}

	return m, nil
}

func (q *Query) Close() {
	q.root.Close() // TODO: should this go in Exec()?

	if q.result == nil {
		return
	}

	switch v := q.result.Value.(type) {
	case promql.Matrix:
		for _, s := range v {
			operator.PutFPointSlice(s.Floats)
			// TODO: histograms
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
	// TODO
	return nil
}

func (q *Query) Cancel() {
	// TODO
}

func (q *Query) String() string {
	//TODO
	return ""
}

func timeMilliseconds(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond/time.Nanosecond)
}
