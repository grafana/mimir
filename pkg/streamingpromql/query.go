// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streamingpromql

import (
	"context"
	"fmt"
	"slices"

	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"
	promstats "github.com/prometheus/prometheus/util/stats"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type Query struct {
	evaluator                *Evaluator
	statement                *parser.EvalStmt
	engine                   *Engine
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker

	originalExpression string

	// Time range of the top-level query.
	// Subqueries may use a different range.
	topLevelQueryTimeRange types.QueryTimeRange

	annotations    *annotations.Annotations
	seriesMetadata []types.SeriesMetadata
	matrix         promql.Matrix
	vector         promql.Vector
	string         *promql.String
	scalar         *promql.Scalar

	succeeded bool
}

func (q *Query) Exec(ctx context.Context) *promql.Result {
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

	if err := q.evaluator.Evaluate(ctx, q); err != nil {
		q.returnResultToPool()
		return &promql.Result{Err: err}
	}

	types.SeriesMetadataSlicePool.Put(&q.seriesMetadata, q.memoryConsumptionTracker)

	result := &promql.Result{}

	if q.annotations != nil {
		result.Warnings = *q.annotations
	}

	switch {
	case q.matrix != nil:
		slices.SortFunc(q.matrix, func(a, b promql.Series) int {
			return labels.Compare(a.Metric, b.Metric)
		})
		result.Value = q.matrix
	case q.vector != nil:
		result.Value = q.vector
	case q.string != nil:
		result.Value = q.string
	case q.scalar != nil:
		result.Value = q.scalar
	default:
		return &promql.Result{Err: fmt.Errorf("unknown result type for query %q over time range %v", q.originalExpression, q.topLevelQueryTimeRange)}
	}

	q.succeeded = true
	return result
}

func (q *Query) SeriesMetadataEvaluated(evaluator *Evaluator, series []types.SeriesMetadata) error {
	q.seriesMetadata = series
	return nil
}

func (q *Query) InstantVectorSeriesDataEvaluated(evaluator *Evaluator, seriesIndex int, seriesData types.InstantVectorSeriesData) error {
	series := q.seriesMetadata[seriesIndex]
	q.seriesMetadata[seriesIndex] = types.SeriesMetadata{} // Clear the original series metadata slice so we don't return the labels twice when the slice is returned later.

	if len(seriesData.Floats) == 0 && len(seriesData.Histograms) == 0 {
		types.PutInstantVectorSeriesData(seriesData, q.memoryConsumptionTracker)
		return nil
	}

	if q.topLevelQueryTimeRange.IsInstant {
		// Instant query: we'll return a vector.
		defer types.PutInstantVectorSeriesData(seriesData, q.memoryConsumptionTracker)
		if q.vector == nil {
			var err error
			q.vector, err = types.VectorPool.Get(len(q.seriesMetadata), q.memoryConsumptionTracker)
			if err != nil {
				return err
			}
		}

		if len(seriesData.Floats)+len(seriesData.Histograms) > 1 {
			return fmt.Errorf("expected exactly one sample for series %s, but got %v floats, %v histograms", series.Labels.String(), len(seriesData.Floats), len(seriesData.Histograms))
		}

		ts := q.topLevelQueryTimeRange.StartT

		// In addition to the two cases below, a series may also have no data points, in which case we don't need to do anything.
		if len(seriesData.Floats) == 1 && len(seriesData.Histograms) == 0 {
			point := seriesData.Floats[0]
			q.vector = append(q.vector, promql.Sample{
				Metric: series.Labels,
				T:      ts,
				F:      point.F,
			})
		} else if len(seriesData.Floats) == 0 && len(seriesData.Histograms) == 1 {
			point := seriesData.Histograms[0]
			q.vector = append(q.vector, promql.Sample{
				Metric: series.Labels,
				T:      ts,
				H:      point.H,
			})

			// Remove histogram from slice to ensure it's not mutated when the slice is reused.
			seriesData.Histograms[0].H = nil
		}

		return nil
	}

	// Range query: we'll return a matrix.
	if q.matrix == nil {
		q.matrix = types.GetMatrix(len(q.seriesMetadata))
	}

	q.matrix = append(q.matrix, promql.Series{
		Metric:     series.Labels,
		Floats:     seriesData.Floats,
		Histograms: seriesData.Histograms,
	})

	return nil
}

func (q *Query) RangeVectorStepSamplesEvaluated(evaluator *Evaluator, seriesIndex int, stepIndex int, stepData *types.RangeVectorStepData) error {
	if stepIndex != 0 {
		// Top-level range vector expressions should only ever have one step (ie. be an instant query).
		return fmt.Errorf("unexpected step index for range vector result: %d", stepIndex)
	}

	if q.matrix == nil {
		q.matrix = types.GetMatrix(len(q.seriesMetadata))
	}

	series := q.seriesMetadata[seriesIndex]
	q.seriesMetadata[seriesIndex] = types.SeriesMetadata{} // Clear the original series metadata slice so we don't return the labels twice when the slice is returned later.

	floats, err := stepData.Floats.CopyPoints()
	if err != nil {
		return err
	}

	histograms, err := stepData.Histograms.CopyPoints()
	if err != nil {
		return err
	}

	if len(floats) == 0 && len(histograms) == 0 {
		types.FPointSlicePool.Put(&floats, q.memoryConsumptionTracker)
		types.HPointSlicePool.Put(&histograms, q.memoryConsumptionTracker)
		return nil
	}

	q.matrix = append(q.matrix, promql.Series{
		Metric:     series.Labels,
		Floats:     floats,
		Histograms: histograms,
	})

	return nil
}

func (q *Query) ScalarEvaluated(evaluator *Evaluator, data types.ScalarData) error {
	if q.topLevelQueryTimeRange.IsInstant {
		defer types.FPointSlicePool.Put(&data.Samples, q.memoryConsumptionTracker)

		p := data.Samples[0]
		q.scalar = &promql.Scalar{
			T: p.T,
			V: p.F,
		}
	} else {
		q.matrix = promql.Matrix{
			{
				Metric: labels.EmptyLabels(),
				Floats: data.Samples,
			},
		}
	}

	return nil
}

func (q *Query) StringEvaluated(evaluator *Evaluator, data string) error {
	q.string = &promql.String{
		T: q.topLevelQueryTimeRange.StartT,
		V: data,
	}

	return nil
}

func (q *Query) EvaluationCompleted(evaluator *Evaluator, annotations *annotations.Annotations) error {
	q.annotations = annotations
	return nil
}

func (q *Query) Close() {
	q.evaluator.Close()
	q.returnResultToPool()

	if q.engine.pedantic && q.succeeded {
		// Only bother checking memory consumption if the query succeeded: it's not expected that all memory
		// will be returned if the query failed.
		if bytesUsed := q.memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(); bytesUsed > 0 {
			panic(fmt.Sprintf("Memory consumption tracker still estimates %d bytes used for %q. This indicates something has not been returned to a pool. Current memory consumption by type:\n%v", bytesUsed, q.originalExpression, q.memoryConsumptionTracker.DescribeCurrentMemoryConsumption()))
		}
	}
}

func (q *Query) returnResultToPool() {
	types.SeriesMetadataSlicePool.Put(&q.seriesMetadata, q.memoryConsumptionTracker)

	if q.matrix != nil {
		for _, s := range q.matrix {
			types.FPointSlicePool.Put(&s.Floats, q.memoryConsumptionTracker)
			types.HPointSlicePool.Put(&s.Histograms, q.memoryConsumptionTracker)
		}

		types.PutMatrix(q.matrix)

		q.matrix = nil
	}

	if q.vector != nil {
		types.VectorPool.Put(&q.vector, q.memoryConsumptionTracker)
	}

	// Nothing to do for scalars: we already returned the slice in populateScalarFromScalarOperator.
	q.scalar = nil

	// And nothing to do for strings: these don't come from a pool.
	q.string = nil
}

func (q *Query) Statement() parser.Statement {
	return q.statement
}

func (q *Query) Stats() *promstats.Statistics {
	return &promstats.Statistics{
		Timers: promstats.NewQueryTimers(),
		Samples: &promstats.QuerySamples{
			TotalSamples:        q.evaluator.stats.TotalSamples,
			TotalSamplesPerStep: q.evaluator.stats.TotalSamplesPerStep,
			EnablePerStepStats:  q.evaluator.stats.EnablePerStepStats,
			Interval:            q.topLevelQueryTimeRange.IntervalMilliseconds,
			StartTimestamp:      q.topLevelQueryTimeRange.StartT,
		},
	}
}

func (q *Query) Cancel() {
	q.evaluator.Cancel()
}

func (q *Query) String() string {
	return q.originalExpression
}
