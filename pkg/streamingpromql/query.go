// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streamingpromql

import (
	"context"
	"fmt"
	"slices"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"
	promstats "github.com/prometheus/prometheus/util/stats"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// Query represents a top-level query.
// It acts as a bridge from the querying interface Prometheus expects into how MQE operates.
type Query struct {
	evaluator                *Evaluator
	statement                *parser.EvalStmt
	engine                   *Engine
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker

	originalExpression string

	// Time range of the top-level query.
	// Subqueries may use a different range.
	topLevelQueryTimeRange types.QueryTimeRange

	seriesMetadata []types.SeriesMetadata
	matrix         promql.Matrix
	vector         promql.Vector
	string         *promql.String
	scalar         *promql.Scalar
	annotations    *annotations.Annotations
	stats          *types.QueryStats

	topLevelValueType parser.ValueType
	resultIsVector    bool // This is necessary as we need to know what kind of result to return (vector or matrix) if the result is empty.

	succeeded bool
}

func (q *Query) Exec(ctx context.Context) (res *promql.Result) {
	logger, ctx := spanlogger.New(ctx, q.engine.logger, tracer, "Query.Exec")
	defer logger.Finish()

	q.resultIsVector = q.topLevelQueryTimeRange.IsInstant && q.topLevelValueType == parser.ValueTypeVector

	if err := q.evaluator.Evaluate(ctx, q); err != nil {
		q.returnResultToPool()
		return &promql.Result{Err: err}
	}

	// Any labels that we need to return will have been removed from q.seriesMetadata and copied into the matrix or vector,
	// so we can safely return it now.
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
		result.Value = *q.string
	case q.scalar != nil:
		result.Value = *q.scalar
	case q.resultIsVector:
		// The result is an empty vector.
		var err error
		result.Value, err = types.VectorPool.Get(0, q.memoryConsumptionTracker)
		if err != nil {
			return &promql.Result{Err: err}
		}
	default:
		// The result is an empty matrix.
		result.Value = types.GetMatrix(0)
	}

	q.succeeded = true
	return result
}

// SeriesMetadataEvaluated implements the EvaluationObserver interface.
func (q *Query) SeriesMetadataEvaluated(_ context.Context, _ *Evaluator, _ planning.Node, series []types.SeriesMetadata) error {
	q.seriesMetadata = series
	return nil
}

// InstantVectorSeriesDataEvaluated implements the EvaluationObserver interface.
func (q *Query) InstantVectorSeriesDataEvaluated(_ context.Context, _ *Evaluator, _ planning.Node, seriesIndex int, _ int, seriesData types.InstantVectorSeriesData) error {
	if len(seriesData.Floats) == 0 && len(seriesData.Histograms) == 0 {
		// Nothing to do.
		types.PutInstantVectorSeriesData(seriesData, q.memoryConsumptionTracker)
		return nil
	}

	series := q.seriesMetadata[seriesIndex]
	q.seriesMetadata[seriesIndex] = types.SeriesMetadata{} // Clear the original series metadata slice so we don't return the labels twice when the slice is returned later.

	if q.resultIsVector {
		return q.appendSeriesToVector(series, seriesData)
	}

	q.appendSeriesToMatrix(series, seriesData)
	return nil
}

func (q *Query) appendSeriesToVector(series types.SeriesMetadata, seriesData types.InstantVectorSeriesData) error {
	defer types.PutInstantVectorSeriesData(seriesData, q.memoryConsumptionTracker)
	if q.vector == nil {
		var err error
		q.vector, err = types.VectorPool.Get(len(q.seriesMetadata), q.memoryConsumptionTracker)
		if err != nil {
			return err
		}
	}

	if len(seriesData.Floats)+len(seriesData.Histograms) != 1 {
		return fmt.Errorf("expected exactly one sample for series %s, but got %v floats, %v histograms", series.Labels.String(), len(seriesData.Floats), len(seriesData.Histograms))
	}

	if len(seriesData.Floats) == 1 {
		point := seriesData.Floats[0]
		q.vector = append(q.vector, promql.Sample{
			Metric: series.Labels,
			T:      point.T,
			F:      point.F,
		})
	} else {
		point := seriesData.Histograms[0]
		q.vector = append(q.vector, promql.Sample{
			Metric: series.Labels,
			T:      point.T,
			H:      point.H,
		})

		// Remove histogram from slice to ensure it's not mutated when the slice is reused.
		seriesData.Histograms[0].H = nil
	}

	return nil
}

func (q *Query) appendSeriesToMatrix(series types.SeriesMetadata, seriesData types.InstantVectorSeriesData) {
	if q.matrix == nil {
		q.matrix = types.GetMatrix(len(q.seriesMetadata))
	}

	q.matrix = append(q.matrix, promql.Series{
		Metric:     series.Labels,
		Floats:     seriesData.Floats,
		Histograms: seriesData.Histograms,
	})
}

// RangeVectorStepSamplesEvaluated implements the EvaluationObserver interface.
func (q *Query) RangeVectorStepSamplesEvaluated(_ context.Context, _ *Evaluator, _ planning.Node, seriesIndex int, stepIndex int, stepData *types.RangeVectorStepData) error {
	if stepIndex != 0 {
		// Top-level range vector expressions should only ever have one step (ie. be an instant query).
		return fmt.Errorf("unexpected step index for range vector result: %d", stepIndex)
	}

	if !stepData.Floats.Any() && !stepData.Histograms.Any() {
		// Nothing to do.
		return nil
	}

	if q.matrix == nil {
		q.matrix = types.GetMatrix(len(q.seriesMetadata))
	}

	floats, err := stepData.Floats.CopyPoints()
	if err != nil {
		return err
	}

	histograms, err := stepData.Histograms.CopyPoints()
	if err != nil {
		return err
	}

	series := q.seriesMetadata[seriesIndex]
	q.seriesMetadata[seriesIndex] = types.SeriesMetadata{} // Clear the original series metadata slice so we don't return the labels twice when the slice is returned later.

	q.matrix = append(q.matrix, promql.Series{
		Metric:     series.Labels,
		Floats:     floats,
		Histograms: histograms,
	})

	return nil
}

// ScalarEvaluated implements the EvaluationObserver interface.
func (q *Query) ScalarEvaluated(_ context.Context, _ *Evaluator, _ planning.Node, data types.ScalarData) error {
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

// StringEvaluated implements the EvaluationObserver interface.
func (q *Query) StringEvaluated(_ context.Context, _ *Evaluator, _ planning.Node, data string) error {
	q.string = &promql.String{
		T: q.topLevelQueryTimeRange.StartT,
		V: data,
	}

	return nil
}

// EvaluationCompleted implements the EvaluationObserver interface.
func (q *Query) EvaluationCompleted(_ context.Context, _ *Evaluator, annotations *annotations.Annotations, stats *types.QueryStats) error {
	q.annotations = annotations
	q.stats = stats
	return nil
}

func (q *Query) Close() {
	q.evaluator.Close()
	q.returnResultToPool()

	// If the query did not succeed (cancelled, error, etc.), reset memory tracking.
	// This prevents inaccurate memory accounting for unconsumed series.
	// See PR #14148 for details.
	if !q.succeeded {
		q.memoryConsumptionTracker.ResetMemoryConsumption()
	}

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
			q.memoryConsumptionTracker.DecreaseMemoryConsumptionForLabels(s.Metric)
		}

		types.PutMatrix(q.matrix)

		q.matrix = nil
	}

	if q.vector != nil {
		for _, s := range q.vector {
			q.memoryConsumptionTracker.DecreaseMemoryConsumptionForLabels(s.Metric)
		}

		types.VectorPool.Put(&q.vector, q.memoryConsumptionTracker)
	}

	// Nothing to do for scalars: we already returned the slice in ScalarEvaluated.
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
			TotalSamples:       q.stats.TotalSamples,
			EnablePerStepStats: false,
			Interval:           q.topLevelQueryTimeRange.IntervalMilliseconds,
			StartTimestamp:     q.topLevelQueryTimeRange.StartT,
		},
	}
}

func (q *Query) Cancel() {
	q.evaluator.Cancel()
}

func (q *Query) String() string {
	return q.originalExpression
}
