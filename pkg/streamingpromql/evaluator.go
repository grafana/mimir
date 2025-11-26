// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"context"
	"fmt"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cancellation"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

var errQueryCancelled = cancellation.NewErrorf("query execution cancelled")
var errQueryClosed = cancellation.NewErrorf("Query.Close() called")
var errQueryFinished = cancellation.NewErrorf("query execution finished")

type Evaluator struct {
	root               types.Operator
	engine             *Engine
	originalExpression string
	timeRange          types.QueryTimeRange

	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	annotations *annotations.Annotations
	stats       *types.QueryStats
	cancel      context.CancelCauseFunc
}

func NewEvaluator(root types.Operator, params *planning.OperatorParameters, timeRange types.QueryTimeRange, engine *Engine, originalExpression string) (*Evaluator, error) {
	return &Evaluator{
		root:               root,
		engine:             engine,
		originalExpression: originalExpression,
		timeRange:          timeRange,

		MemoryConsumptionTracker: params.MemoryConsumptionTracker,
		annotations:              params.Annotations,
		stats:                    params.QueryStats,
	}, nil
}

// Evaluate evaluates the query.
//
// Evaluate will always call observer.EvaluationCompleted before returning nil.
// It may return a non-nil error after calling observer.EvaluationCompleted if observer.EvaluationCompleted returned a non-nil error.
func (e *Evaluator) Evaluate(ctx context.Context, observer EvaluationObserver) (err error) {
	logger, ctx := spanlogger.New(ctx, e.engine.logger, tracer, "Evaluator.Evaluate")
	defer logger.Finish()

	defer func() {
		msg := make([]interface{}, 0, 2*(3+4+2)) // 3 fields for all query types, plus worst case of 4 fields for range queries and 2 fields for a failed query

		msg = append(msg,
			"msg", "evaluation stats",
			"estimatedPeakMemoryConsumption", int64(e.MemoryConsumptionTracker.PeakEstimatedMemoryConsumptionBytes()),
			"originalExpression", e.originalExpression,
		)

		if e.timeRange.IsInstant {
			msg = append(msg,
				"timeRangeType", "instant",
				"time", e.timeRange.StartT,
			)
		} else {
			msg = append(msg,
				"timeRangeType", "range",
				"start", e.timeRange.StartT,
				"end", e.timeRange.EndT,
				"step", e.timeRange.IntervalMilliseconds,
			)
		}

		if err == nil {
			msg = append(msg, "status", "success")
		} else {
			msg = append(msg,
				"status", "failed",
				"err", err,
			)
		}

		level.Info(logger).Log(msg...)
		e.engine.estimatedPeakMemoryConsumption.Observe(float64(e.MemoryConsumptionTracker.PeakEstimatedMemoryConsumptionBytes()))
	}()

	// Add the memory consumption tracker to the context of this query before executing it so
	// that we can pass it to the rest of the read path and keep track of memory used loading
	// chunks from store-gateways or ingesters.
	ctx = limiter.AddMemoryTrackerToContext(ctx, e.MemoryConsumptionTracker)

	ctx, cancel := context.WithCancelCause(ctx)
	e.cancel = cancel

	if e.engine.timeout != 0 {
		var cancelTimeoutCtx context.CancelFunc
		ctx, cancelTimeoutCtx = context.WithTimeoutCause(ctx, e.engine.timeout, fmt.Errorf("%w: query timed out", context.DeadlineExceeded))

		defer cancelTimeoutCtx()
	}

	// The order of the deferred cancellations is important: we want to close all operators first, then
	// cancel with errQueryFinished and not a timeout, so we must defer this function last
	// (so that it runs before the cancellation of the context with timeout created above).
	defer func() {
		e.root.Close()
		cancel(errQueryFinished)
	}()

	if e.engine.pedantic {
		// Close the root operator a second time to ensure all operators behave correctly if Close is called multiple times.
		defer e.root.Close()
	}

	queryID, err := e.engine.activeQueryTracker.InsertWithDetails(ctx, e.originalExpression, "evaluation", e.timeRange)
	if err != nil {
		return err
	}

	defer e.engine.activeQueryTracker.Delete(queryID)

	if err := e.root.Prepare(ctx, &types.PrepareParams{}); err != nil {
		return fmt.Errorf("failed to prepare query: %w", err)
	}

	switch root := e.root.(type) {
	case types.InstantVectorOperator:
		if err := e.evaluateInstantVectorOperator(ctx, root, observer); err != nil {
			return err
		}

	case types.RangeVectorOperator:
		if err := e.evaluateRangeVectorOperator(ctx, root, observer); err != nil {
			return err
		}

	case types.ScalarOperator:
		if err := e.evaluateScalarOperator(ctx, root, observer); err != nil {
			return err
		}

	case types.StringOperator:
		if err := e.evaluateStringOperator(ctx, root, observer); err != nil {
			return err
		}

	default:
		return fmt.Errorf("operator type %T produces unknown result type", root)
	}

	if err := e.root.Finalize(ctx); err != nil {
		return err
	}

	if e.engine.pedantic {
		// Finalize the root operator a second time to ensure all operators behave correctly if Finalize is called multiple times.
		if err := e.root.Finalize(ctx); err != nil {
			return fmt.Errorf("pedantic mode: failed to finalize operator a second time after successfully finalizing the first time: %w", err)
		}
	}

	// To make comparing to Prometheus' engine easier, only return the annotations if there are some, otherwise, return nil.
	if len(*e.annotations) == 0 {
		e.annotations = nil
	}

	return observer.EvaluationCompleted(ctx, e, e.annotations, e.stats)
}

func (e *Evaluator) evaluateInstantVectorOperator(ctx context.Context, op types.InstantVectorOperator, observer EvaluationObserver) error {
	series, err := op.SeriesMetadata(ctx, nil)
	if err != nil {
		return err
	}

	seriesCount := len(series) // Take the length now, as the observer takes ownership of the slice when SeriesMetadataEvaluated is called.

	if err := observer.SeriesMetadataEvaluated(ctx, e, series); err != nil {
		return err
	}

	for seriesIdx := range seriesCount {
		d, err := op.NextSeries(ctx)
		if err != nil {
			if errors.Is(err, types.EOS) {
				return fmt.Errorf("expected %v series, but only received %v", seriesCount, seriesIdx)
			}

			return err
		}

		if err := observer.InstantVectorSeriesDataEvaluated(ctx, e, seriesIdx, d); err != nil {
			return err
		}
	}

	return nil
}

func (e *Evaluator) evaluateRangeVectorOperator(ctx context.Context, op types.RangeVectorOperator, observer EvaluationObserver) error {
	series, err := op.SeriesMetadata(ctx, nil)
	if err != nil {
		return err
	}

	seriesCount := len(series) // Take the length now, as the observer takes ownership of the slice when SeriesMetadataEvaluated is called.

	if err := observer.SeriesMetadataEvaluated(ctx, e, series); err != nil {
		return err
	}

	for seriesIdx := range seriesCount {
		err := op.NextSeries(ctx)
		if err != nil {
			if errors.Is(err, types.EOS) {
				return fmt.Errorf("expected %v series, but only received %v", seriesCount, seriesIdx)
			}

			return err
		}

		stepIdx := 0
		for {
			step, err := op.NextStepSamples(ctx)

			// nolint:errorlint // errors.Is introduces a performance overhead, and NextStepSamples is guaranteed to return exactly EOS, never a wrapped error.
			if err == types.EOS {
				break
			}

			if err != nil {
				return err
			}

			if err := observer.RangeVectorStepSamplesEvaluated(ctx, e, seriesIdx, stepIdx, step); err != nil {
				return err
			}

			stepIdx++
		}
	}

	return nil
}

func (e *Evaluator) evaluateScalarOperator(ctx context.Context, op types.ScalarOperator, observer EvaluationObserver) error {
	d, err := op.GetValues(ctx)
	if err != nil {
		return err
	}

	return observer.ScalarEvaluated(ctx, e, d)
}

func (e *Evaluator) evaluateStringOperator(ctx context.Context, op types.StringOperator, observer EvaluationObserver) error {
	v := op.GetValue()

	return observer.StringEvaluated(ctx, e, v)
}

func (e *Evaluator) GetQueryTimeRange() types.QueryTimeRange {
	return e.timeRange
}

func (e *Evaluator) Cancel() {
	if e.cancel != nil {
		e.cancel(errQueryCancelled)
	}
}

func (e *Evaluator) Close() {
	if e.cancel != nil {
		e.cancel(errQueryClosed)
	}
}

type EvaluationObserver interface {
	// SeriesMetadataEvaluated notifies this observer when series metadata has been evaluated.
	// Implementations of this method are responsible for returning the series slice to the pool when it is no longer needed.
	// Implementations of this method may mutate the series slice before returning it to the pool.
	SeriesMetadataEvaluated(ctx context.Context, evaluator *Evaluator, series []types.SeriesMetadata) error

	// InstantVectorSeriesDataEvaluated notifies this observer when samples for an instant vector series have been evaluated.
	// Implementations of this method are responsible for returning seriesData to the pool when it is no longer needed.
	// Implementations of this method may mutate seriesData before returning it to the pool.
	InstantVectorSeriesDataEvaluated(ctx context.Context, evaluator *Evaluator, seriesIndex int, seriesData types.InstantVectorSeriesData) error

	// RangeVectorStepSamplesEvaluated notifies this observer when samples for a range vector step have been evaluated.
	// Implementations of this method must not mutate stepData, and should copy any data they wish to retain from stepData before returning.
	RangeVectorStepSamplesEvaluated(ctx context.Context, evaluator *Evaluator, seriesIndex int, stepIndex int, stepData *types.RangeVectorStepData) error

	// ScalarEvaluated notifies this observer when a scalar has been evaluated.
	// Implementations of this method are responsible for returning data to the pool when it is no longer needed.
	// Implementations of this method may mutate data before returning it to the pool.
	ScalarEvaluated(ctx context.Context, evaluator *Evaluator, data types.ScalarData) error

	// StringEvaluated notifies this observer when a string has been evaluated.
	StringEvaluated(ctx context.Context, evaluator *Evaluator, data string) error

	// EvaluationCompleted notifies this observer when evaluation is complete.
	EvaluationCompleted(ctx context.Context, evaluator *Evaluator, annotations *annotations.Annotations, stats *types.QueryStats) error
}
