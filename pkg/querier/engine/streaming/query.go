// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streaming

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/exp/slices"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/stats"

	"github.com/grafana/mimir/pkg/querier/engine/streaming/operator"
)

var globalPool = operator.NewPool()

type Query struct {
	queryable storage.Queryable
	opts      promql.QueryOpts
	qs        string
	statement parser.Statement
	root      operator.Operator
	start     time.Time
	end       time.Time
	interval  time.Duration
	pool      *operator.Pool
	engine    *Engine

	result *promql.Result
}

func NewQuery(queryable storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration, engine *Engine) (*Query, error) {
	if opts == nil {
		opts = promql.NewPrometheusQueryOpts(false, 0)
	}

	q := &Query{
		queryable: queryable,
		opts:      opts,
		qs:        qs,
		start:     start,
		end:       end,
		interval:  interval,
		pool:      globalPool,
		engine:    engine,
	}

	expr, err := parser.ParseExpr(qs)
	if err != nil {
		return nil, err
	}

	q.root, err = q.convertToOperator(expr)
	if err != nil {
		return nil, err
	}

	return q, nil
}

func (q *Query) convertToOperator(expr parser.Expr) (operator.Operator, error) {
	interval := q.interval

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
			return nil, NewNotSupportedError("vector selector with offset")
		}

		if e.Timestamp != nil {
			return nil, NewNotSupportedError("vector selector with timestamp")
		}

		// TODO: handle @, offset
		// (see getTimeRangesForSelector() in promql/engine.go)
		return &operator.VectorSelector{
			Queryable:     q.queryable,
			Start:         q.start,
			End:           q.end,
			Interval:      interval,
			LookbackDelta: lookbackDelta,
			Matchers:      e.LabelMatchers,
			Pool:          q.pool,
		}, nil
	case *parser.AggregateExpr:
		if e.Op != parser.SUM {
			return nil, NewNotSupportedError(fmt.Sprintf("unsupported aggregation operator %s", e.Op))
		}
		if e.Param != nil {
			return nil, fmt.Errorf("unexpected parameter for %s aggregation: %s", e.Op, e.Param)
		}
		if e.Without {
			return nil, NewNotSupportedError("grouping with 'without' not supported")
		}

		slices.Sort(e.Grouping)

		inner, err := q.convertToOperator(e.Expr)
		if err != nil {
			return nil, err
		}

		return &operator.Aggregation{
			Inner:    inner,
			Start:    q.start,
			End:      q.end,
			Interval: interval,
			Grouping: e.Grouping,
			Pool:     q.pool,
		}, nil
	case *parser.Call:
		if e.Func.Name != "rate" {
			return nil, NewNotSupportedError(fmt.Sprintf("unsupported function %s", e.Func.Name))
		}
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("expected exactly one argument for rate, got %v", len(e.Args))
		}

		matrixSelector, ok := e.Args[0].(*parser.MatrixSelector)
		if !ok {
			return nil, NewNotSupportedError(fmt.Sprintf("unsupported rate argument type %T", e.Args[0]))
		}

		lookbackDelta := q.opts.LookbackDelta()
		if lookbackDelta == 0 {
			lookbackDelta = q.engine.lookbackDelta
		}

		vectorSelector := matrixSelector.VectorSelector.(*parser.VectorSelector)

		if vectorSelector.OriginalOffset != 0 || vectorSelector.Offset != 0 {
			return nil, NewNotSupportedError("vector selector with offset")
		}

		if vectorSelector.Timestamp != nil {
			return nil, NewNotSupportedError("vector selector with timestamp")
		}

		// TODO: handle @, offset
		// (see getTimeRangesForSelector() in promql/engine.go)
		return &operator.MatrixSelectorWithTransformationOverRange{
			Queryable:     q.queryable,
			Start:         q.start,
			End:           q.end,
			Interval:      interval,
			Range:         matrixSelector.Range,
			LookbackDelta: lookbackDelta,
			Matchers:      vectorSelector.LabelMatchers,
			Pool:          q.pool,
		}, nil
	default:
		return nil, NewNotSupportedError(fmt.Sprintf("PromQL expression type %T", e))
	}
}

func (q *Query) IsInstant() bool {
	return q.start == q.end && q.interval == 0
}

func (q *Query) Exec(ctx context.Context) *promql.Result {
	series, err := q.root.Series(ctx)
	if err != nil {
		return &promql.Result{Err: err}
	}
	defer q.pool.PutSeriesMetadataSlice(series)

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
	ts := timeMilliseconds(q.start)
	v := q.pool.GetVector(len(series))

	for i, s := range series {
		ok, d, err := q.root.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("expected %v series, but only received %v", len(series), i)
		}
		if len(d.Floats) != 1 {
			defer q.pool.PutFPointSlice(d.Floats)
			return nil, fmt.Errorf("expected exactly one sample for series %s, but got %v", s.Labels.String(), len(d.Floats))
		}

		point := d.Floats[0]
		v = append(v, promql.Sample{
			Metric: s.Labels,
			T:      ts,
			F:      point.F,
		})

		q.pool.PutFPointSlice(d.Floats)
	}

	return v, nil
}

func (q *Query) populateMatrix(ctx context.Context, series []operator.SeriesMetadata) (promql.Matrix, error) {
	m := q.pool.GetMatrix(len(series))

	for i, s := range series {
		ok, d, err := q.root.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("expected %v series, but only received %v", len(series), i)
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
			q.pool.PutFPointSlice(s.Floats)
			// TODO: histograms
		}

		q.pool.PutMatrix(v)
	case promql.Vector:
		q.pool.PutVector(v)
	default:
		panic(fmt.Sprintf("unknown result value type %T", q.result.Value))
	}
}

func (q *Query) Statement() parser.Statement {
	//TODO implement me
	panic("implement me")
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
