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
	promstats "github.com/prometheus/prometheus/util/stats"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
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
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
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

func (e *Engine) newQuery(ctx context.Context, queryable storage.Queryable, opts promql.QueryOpts, timeRange types.QueryTimeRange, originalExpression string) (*Query, error) {
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

	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, maxEstimatedMemoryConsumptionPerQuery, e.queriesRejectedDueToPeakMemoryConsumption, originalExpression)
	stats, err := types.NewQueryStats(timeRange, e.enablePerStepStats && opts.EnablePerStepStats(), memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}
	q := &Query{
		queryable:                queryable,
		engine:                   e,
		memoryConsumptionTracker: memoryConsumptionTracker,
		annotations:              annotations.New(),
		stats:                    stats,
		topLevelQueryTimeRange:   timeRange,
		lookbackDelta:            lookbackDelta,
		originalExpression:       originalExpression,
	}

	return q, nil
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

	// Add the memory consumption tracker to the context of this query before executing it so
	// that we can pass it to the rest of the read path and keep track of memory used loading
	// chunks from store-gateways or ingesters.
	ctx = limiter.AddMemoryTrackerToContext(ctx, q.memoryConsumptionTracker)

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
			"estimatedPeakMemoryConsumption", int64(q.memoryConsumptionTracker.PeakEstimatedMemoryConsumptionBytes()),
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
		q.engine.estimatedPeakMemoryConsumption.Observe(float64(q.memoryConsumptionTracker.PeakEstimatedMemoryConsumptionBytes()))
	}()

	err = q.root.Prepare(ctx, &types.PrepareParams{
		QueryStats: q.stats,
	})
	if err != nil {
		return &promql.Result{Err: fmt.Errorf("failed to prepare query: %w", err)}
	}

	switch root := q.root.(type) {
	case types.RangeVectorOperator:
		series, err := root.SeriesMetadata(ctx)
		if err != nil {
			return &promql.Result{Err: err}
		}
		defer types.SeriesMetadataSlicePool.Put(&series, q.memoryConsumptionTracker)

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
		defer types.SeriesMetadataSlicePool.Put(&series, q.memoryConsumptionTracker)
		// TODO: place this as part of Put call above.
		defer func() {
			for _, s := range series {
				q.memoryConsumptionTracker.DecreaseMemoryConsumptionForLabels(s.Labels)
			}
		}()

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

		if len(d.Floats)+len(d.Histograms) > 1 {
			types.PutInstantVectorSeriesData(d, q.memoryConsumptionTracker)
			return nil, fmt.Errorf("expected exactly one sample for series %s, but got %v floats, %v histograms", s.Labels.String(), len(d.Floats), len(d.Histograms))
		}

		// In addition to the two cases below, a series may also have no data points, in which case we don't need to do anything.
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
			types.FPointSlicePool.Put(&floats, q.memoryConsumptionTracker)
			types.HPointSlicePool.Put(&histograms, q.memoryConsumptionTracker)
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
	defer types.FPointSlicePool.Put(&d.Samples, q.memoryConsumptionTracker)

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
	q.stats.Close()

	if q.result == nil {
		return
	}

	switch v := q.result.Value.(type) {
	case promql.Matrix:
		for _, s := range v {
			types.FPointSlicePool.Put(&s.Floats, q.memoryConsumptionTracker)
			types.HPointSlicePool.Put(&s.Histograms, q.memoryConsumptionTracker)
		}

		types.PutMatrix(v)
	case promql.Vector:
		types.VectorPool.Put(&v, q.memoryConsumptionTracker)
	case promql.Scalar:
		// Nothing to do, we already returned the slice in populateScalarFromScalarOperator.
	case promql.String:
		// Nothing to do as strings don't come from a pool
	case nil:
		// Nothing to do if there is no value.
	default:
		panic(fmt.Sprintf("unknown result value type %T", q.result.Value))
	}

	q.result.Value = nil

	if q.engine.pedantic && q.result.Err == nil {
		if bytesUsed := q.memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(); bytesUsed > 0 {
			panic(fmt.Sprintf("Memory consumption tracker still estimates %d bytes used for %q. This indicates something has not been returned to a pool.", bytesUsed, q.originalExpression))
		}
	}
}

func (q *Query) Statement() parser.Statement {
	return q.statement
}

func (q *Query) Stats() *promstats.Statistics {
	return &promstats.Statistics{
		Timers: promstats.NewQueryTimers(),
		Samples: &promstats.QuerySamples{
			TotalSamples:        q.stats.TotalSamples,
			TotalSamplesPerStep: q.stats.TotalSamplesPerStep,
			EnablePerStepStats:  q.stats.EnablePerStepStats,
			Interval:            q.topLevelQueryTimeRange.IntervalMilliseconds,
			StartTimestamp:      q.topLevelQueryTimeRange.StartT,
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
