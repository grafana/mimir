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

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"
	promstats "github.com/prometheus/prometheus/util/stats"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

var errMatrixContainsMetricsWithSameLabelset = errors.New("vector cannot contain metrics with the same labelset")

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
	annotations    annotations.Annotations
	stats          *types.OperatorEvaluationStats
	finalizedStats *promstats.QuerySamples

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

	result := &promql.Result{
		Warnings: q.annotations,
	}

	switch {
	case q.matrix != nil:
		// A top-level range vector result (i.e. an instant query whose expression is a subquery) can
		// contain series that collide after name removal. Merge them, matching Prometheus.
		if q.topLevelQueryTimeRange.IsInstant && q.topLevelValueType == parser.ValueTypeMatrix {
			if err := q.mergeMatrixSeriesWithSameLabelset(); err != nil {
				q.returnResultToPool()
				return &promql.Result{Err: err}
			}
		}

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

	for _, pp := range q.engine.queryPostProcessors {
		if err := pp.PostProcess(ctx, q.evaluator.originalExpression); err != nil {
			return &promql.Result{Err: fmt.Errorf("post-processing query failed: %w", err)}
		}
	}

	q.succeeded = true
	return result
}

// mergeMatrixSeriesWithSameLabelset merges series in q.matrix that share a labelset, combining their
// samples, mirroring Prometheus under delayed name removal (cleanupMetricLabels ->
// mergeSeriesWithSameLabelset). Two samples for the same labelset at the same timestamp are an error.
func (q *Query) mergeMatrixSeriesWithSameLabelset() error {
	if len(q.matrix) <= 1 {
		return nil
	}

	// Group by labelset, keyed on the label bytes rather than a hash so distinct series that share a
	// hash are not merged.
	groups := make(map[string][]int, len(q.matrix))
	order := make([]string, 0, len(q.matrix))
	var keyBuf []byte
	haveDuplicates := false
	for i := range q.matrix {
		keyBuf = q.matrix[i].Metric.Bytes(keyBuf)
		indices, exists := groups[string(keyBuf)]
		if exists {
			haveDuplicates = true
		} else {
			order = append(order, string(keyBuf))
		}
		groups[string(keyBuf)] = append(indices, i)
	}

	if !haveDuplicates {
		return nil
	}

	// Move each processed series out of q.matrix (leaving an empty Series) into merged, so every sample
	// slice is owned in one place; returnResultToPool (called by Exec on error) can then safely free
	// whatever remains in q.matrix.
	original := q.matrix
	merged := types.GetMatrix(len(order))

	for _, key := range order {
		indices := groups[key]

		if len(indices) == 1 {
			merged = append(merged, original[indices[0]])
			original[indices[0]] = promql.Series{}
			continue
		}

		data := make([]types.InstantVectorSeriesData, len(indices))
		for i, idx := range indices {
			data[i] = types.InstantVectorSeriesData{Floats: original[idx].Floats, Histograms: original[idx].Histograms}
		}

		// MergeSeries takes ownership of (and frees) the sample slices in data regardless of the outcome,
		// so clear the corresponding series in original and release the labels of all but the first.
		outputMetric := original[indices[0]].Metric
		mergedData, conflict, err := operators.MergeSeries(data, slices.Clone(indices), q.memoryConsumptionTracker)
		for i, idx := range indices {
			if i != 0 {
				q.memoryConsumptionTracker.DecreaseMemoryConsumptionForLabels(original[idx].Metric)
			}
			original[idx] = promql.Series{}
		}

		// On error the query stops, so per this package's convention we do not return the merged-so-far
		// slices to the pool or adjust the memory estimate; returnResultToPool frees what remains in
		// q.matrix (the not-yet-processed series).
		if err != nil {
			return err
		}
		if conflict != nil {
			return errMatrixContainsMetricsWithSameLabelset
		}

		merged = append(merged, promql.Series{
			Metric:     outputMetric,
			Floats:     mergedData.Floats,
			Histograms: mergedData.Histograms,
		})
	}

	types.PutMatrix(original)
	q.matrix = merged
	return nil
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
func (q *Query) EvaluationCompleted(_ context.Context, _ *Evaluator, nodeInfo map[planning.Node]NodeCompletionInfo) error {
	if len(nodeInfo) != 1 {
		return fmt.Errorf("expected exactly one node completion info entry, but got %d", len(nodeInfo))
	}

	nodeCompletionInfo := nodeInfo[q.evaluator.nodeRequests[0].Node]
	q.stats = nodeCompletionInfo.Stats
	q.annotations = nodeCompletionInfo.Annotations

	var err error
	q.finalizedStats, err = q.stats.FinalizeAndComputePrometheusStats()
	if err != nil {
		return err
	}

	return nil
}

func (q *Query) Close() {
	q.evaluator.Close()
	q.returnResultToPool()

	if q.stats != nil {
		q.stats.Close()
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

	// Note this will also be called in the evaluator close but this is safe and help ensure this is always deregistered as quickly as possible.
	// This also avoids an issue where the Query (and underlying Evaluator) Close() may not be called on Query.Exec() error.
	q.engine.memoryConsumptionTrackerFactory.Deregister(q.evaluator.MemoryConsumptionTracker)
}

func (q *Query) Statement() parser.Statement {
	return q.statement
}

func (q *Query) Stats() *promstats.Statistics {
	return &promstats.Statistics{
		Timers:  promstats.NewQueryTimers(),
		Samples: q.finalizedStats,
	}
}

func (q *Query) Cancel() {
	q.evaluator.Cancel()
}

func (q *Query) String() string {
	return q.originalExpression
}

// QueryPostProcessor is invoked after a query has executed successfully.
//
// It can be used to observe the outcome of a query, for example to populate a cache from the query
// stats. Post-processors are not invoked if the query fails, and must not modify the query result.
type QueryPostProcessor interface {
	// PostProcess is called once, after the query has executed successfully. Implementations should
	// read whatever they need (for example the query stats) from ctx.
	PostProcess(ctx context.Context, originalExpression string) error
}
