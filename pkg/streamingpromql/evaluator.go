// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streamingpromql

import (
	"context"
	"fmt"

	"github.com/grafana/dskit/cancellation"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

var errQueryCancelled = cancellation.NewErrorf("query execution cancelled")
var errQueryClosed = cancellation.NewErrorf("Query.Close() called")
var errQueryFinished = cancellation.NewErrorf("query execution finished")

type Evaluator struct {
	root                       types.Operator
	engine                     *Engine
	activityTrackerDescription string

	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	annotations              *annotations.Annotations
	stats                    *types.QueryStats
	cancel                   context.CancelCauseFunc
}

func NewEvaluator(root types.Operator, params *planning.OperatorParameters, timeRange types.QueryTimeRange, engine *Engine, opts promql.QueryOpts, originalExpression string) (*Evaluator, error) {
	stats, err := types.NewQueryStats(timeRange, engine.enablePerStepStats && opts.EnablePerStepStats(), params.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	return &Evaluator{
		root:                       root,
		engine:                     engine,
		activityTrackerDescription: originalExpression,

		memoryConsumptionTracker: params.MemoryConsumptionTracker,
		annotations:              params.Annotations,
		stats:                    stats,
	}, nil
}

func (e *Evaluator) Evaluate(ctx context.Context, observer EvaluationObserver) error {
	defer e.root.Close()

	if e.engine.pedantic {
		// Close the root operator a second time to ensure all operators behave correctly if Close is called multiple times.
		defer e.root.Close()
	}

	// Add the memory consumption tracker to the context of this query before executing it so
	// that we can pass it to the rest of the read path and keep track of memory used loading
	// chunks from store-gateways or ingesters.
	ctx = limiter.AddMemoryTrackerToContext(ctx, e.memoryConsumptionTracker)

	ctx, cancel := context.WithCancelCause(ctx)
	e.cancel = cancel

	if e.engine.timeout != 0 {
		var cancelTimeoutCtx context.CancelFunc
		ctx, cancelTimeoutCtx = context.WithTimeoutCause(ctx, e.engine.timeout, fmt.Errorf("%w: query timed out", context.DeadlineExceeded))

		defer cancelTimeoutCtx()
	}

	// The order of the deferred cancellations is important: we want to cancel with errQueryFinished first, so we must defer this cancellation last
	// (so that it runs before the cancellation of the context with timeout created above).
	defer cancel(errQueryFinished)

	queryID, err := e.engine.activeQueryTracker.Insert(ctx, e.activityTrackerDescription)
	if err != nil {
		return err
	}

	defer e.engine.activeQueryTracker.Delete(queryID)

	err = e.root.Prepare(ctx, &types.PrepareParams{QueryStats: e.stats})
	if err != nil {
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
		if err := e.evaluateStringOperator(root, observer); err != nil {
			return err
		}

	default:
		return fmt.Errorf("operator type %T produces unknown result type", root)
	}

	// To make comparing to Prometheus' engine easier, only return the annotations if there are some, otherwise, return nil.
	if len(*e.annotations) == 0 {
		e.annotations = nil
	}

	if err := observer.EvaluationCompleted(e, e.annotations, e.stats); err != nil {
		return err
	}

	return nil
}

func (e *Evaluator) evaluateInstantVectorOperator(ctx context.Context, o types.InstantVectorOperator, observer EvaluationObserver) error {
	series, err := o.SeriesMetadata(ctx)
	if err != nil {
		return err
	}

	seriesCount := len(series) // Take the length now, as the observer takes ownership of the slice when SeriesMetadataEvaluated is called.

	if err := observer.SeriesMetadataEvaluated(e, series); err != nil {
		return err
	}

	for seriesIdx := range seriesCount {
		d, err := o.NextSeries(ctx)
		if err != nil {
			if errors.Is(err, types.EOS) {
				return fmt.Errorf("expected %v series, but only received %v", seriesCount, seriesIdx)
			}

			return err
		}

		if err := observer.InstantVectorSeriesDataEvaluated(e, seriesIdx, d); err != nil {
			return err
		}
	}

	return nil
}

func (e *Evaluator) evaluateRangeVectorOperator(ctx context.Context, o types.RangeVectorOperator, observer EvaluationObserver) error {
	series, err := o.SeriesMetadata(ctx)
	if err != nil {
		return err
	}

	seriesCount := len(series) // Take the length now, as the observer takes ownership of the slice when SeriesMetadataEvaluated is called.

	if err := observer.SeriesMetadataEvaluated(e, series); err != nil {
		return err
	}

	for seriesIdx := range seriesCount {
		err := o.NextSeries(ctx)
		if err != nil {
			if errors.Is(err, types.EOS) {
				return fmt.Errorf("expected %v series, but only received %v", seriesCount, seriesIdx)
			}

			return err
		}

		// FIXME: add support for evaluating at multiple steps once we support returning this over the wire from queriers to query-frontends
		step, err := o.NextStepSamples()
		if err != nil {
			return err
		}

		if err := observer.RangeVectorStepSamplesEvaluated(e, seriesIdx, 0, step); err != nil {
			return err
		}
	}

	return nil
}

func (e *Evaluator) evaluateScalarOperator(ctx context.Context, o types.ScalarOperator, observer EvaluationObserver) error {
	d, err := o.GetValues(ctx)
	if err != nil {
		return err
	}

	if err := observer.ScalarEvaluated(e, d); err != nil {
		return err
	}

	return nil
}

func (e *Evaluator) evaluateStringOperator(o types.StringOperator, observer EvaluationObserver) error {
	v := o.GetValue()
	if err := observer.StringEvaluated(e, v); err != nil {
		return err
	}

	return nil
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

	if e.stats != nil {
		e.stats.Close()
	}
}

type EvaluationObserver interface {
	// SeriesMetadataEvaluated notifies this observer when series metadata has been evaluated.
	// Implementations of this method are responsible for returning the series slice to the pool when it is no longer needed.
	SeriesMetadataEvaluated(evaluator *Evaluator, series []types.SeriesMetadata) error

	// InstantVectorSeriesDataEvaluated notifies this observer when samples for an instant vector series have been evaluated.
	// Implementations of this method are responsible for returning seriesData to the pool when it is no longer needed.
	InstantVectorSeriesDataEvaluated(evaluator *Evaluator, seriesIndex int, seriesData types.InstantVectorSeriesData) error

	// RangeVectorStepSamplesEvaluated notifies this observer when samples for a range vector step have been evaluated.
	// Implementations of this method must not mutate stepData.
	RangeVectorStepSamplesEvaluated(evaluator *Evaluator, seriesIndex int, stepIndex int, stepData *types.RangeVectorStepData) error

	// ScalarEvaluated notifies this observer when a scalar has been evaluated.
	// Implementations of this method are responsible for returning data to the pool when it is no longer needed.
	ScalarEvaluated(evaluator *Evaluator, data types.ScalarData) error

	// StringEvaluated notifies this observer when a string has been evaluated.
	StringEvaluated(evaluator *Evaluator, data string) error

	// EvaluationCompleted notifies this observer when evaluation is complete.
	EvaluationCompleted(evaluator *Evaluator, annotations *annotations.Annotations, stats *types.QueryStats) error
}
