// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"context"
	"fmt"
	"runtime"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/tracing"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/histogram"
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
	nodeRequests       []NodeEvaluationRequest
	engine             *Engine
	originalExpression string

	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	cancel context.CancelCauseFunc
}

func NewEvaluator(nodeRequests []NodeEvaluationRequest, params *planning.OperatorParameters, engine *Engine, originalExpression string) (*Evaluator, error) {
	return &Evaluator{
		nodeRequests:       nodeRequests,
		engine:             engine,
		originalExpression: originalExpression,

		MemoryConsumptionTracker: params.MemoryConsumptionTracker,
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
		msg := make([]interface{}, 0, 2*(6+4+2)) // 3 fields for all query types, plus worst case of 4 fields for range queries and 2 fields for a failed query

		msg = append(msg,
			"msg", "evaluation stats",
			"estimatedPeakMemoryConsumption", int64(e.MemoryConsumptionTracker.PeakEstimatedMemoryConsumptionBytes()),
			"originalExpression", e.originalExpression,
			"nodeCount", len(e.nodeRequests),
		)

		if len(e.nodeRequests) == 1 {
			timeRange := e.nodeRequests[0].TimeRange

			if timeRange.IsInstant {
				msg = append(msg,
					"timeRangeType", "instant",
					"time", timeRange.StartT,
				)
			} else {
				msg = append(msg,
					"timeRangeType", "range",
					"start", timeRange.StartT,
					"end", timeRange.EndT,
					"step", timeRange.IntervalMilliseconds,
				)
			}
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

	// Recover from panics during evaluation.
	//
	// A genuine Go runtime error (nil dereference, index out of range, ...) indicates a bug in
	// the engine, so we re-panic to crash the process: engine bugs are typically reproducible in
	// test environments, and failing fast there surfaces them before they reach production.
	//
	// Any other panic is converted into a query error rather than crashing. In particular, the
	// Prometheus histogram library uses panic-with-an-error as an error channel when it encounters
	// invalid data during evaluation (for example a stored native histogram with a negative-offset
	// span, which older versions could write). Such data is only ever seen in production, so a
	// crash there cannot be caught earlier and would take down a querier or ruler shared by other
	// tenants. Converting it into a failed query matches how the Prometheus engine behaves.
	//
	// Either way the stats logging above reports the query as failed instead of successful. Recovered
	// panics are counted, labelled by the affected tenant and a coarse reason, so they can be alerted
	// on; re-panicked runtime errors are not, because the process exits before the counter could be
	// reliably scraped, and the crash is visible on its own.
	defer func() {
		r := recover()
		if r == nil {
			return
		}

		if _, isRuntimeErr := r.(runtime.Error); isRuntimeErr {
			level.Error(logger).Log("msg", "runtime error while evaluating query, re-panicking to crash", "err", r, "expr", e.originalExpression)
			if err == nil {
				err = fmt.Errorf("panic during query evaluation: %v", r)
			}
			panic(r)
		}

		userID := ""
		if tenantIDs, tenantErr := tenant.TenantIDs(ctx); tenantErr == nil {
			userID = tenant.JoinTenantIDs(tenantIDs)
		}

		// Classify the panic so data problems can be told apart from possible engine bugs without
		// reading logs. Validation errors raised by the histogram library indicate invalid stored
		// data; anything else may be a bug even though nothing crashed.
		reason := "other"
		rErr, isErr := r.(error)
		if isErr {
			var validationErr histogram.Error
			if errors.As(rErr, &validationErr) {
				reason = "invalid_data"
			}
		}

		e.engine.evaluationPanics.WithLabelValues(userID, reason).Inc()
		if reason == "invalid_data" {
			// The panic's origin is known and this can recur on every evaluation over the same
			// invalid series, so don't log a stack trace for it.
			level.Warn(logger).Log("msg", "recovered from panic while evaluating query, returning it as a query error", "err", r, "expr", e.originalExpression)
		} else {
			// A possible engine bug that no longer crashes the process: the stack trace in this log
			// is the only remaining pointer to the panic's origin, so capture it. Deferred functions
			// run before the stack unwinds, so this includes the frames down to the panic site.
			buf := make([]byte, 64<<10)
			buf = buf[:runtime.Stack(buf, false)]
			level.Error(logger).Log("msg", "recovered from panic while evaluating query, returning it as a query error", "err", r, "expr", e.originalExpression, "stacktrace", string(buf))
		}
		if err == nil {
			if isErr {
				err = rErr
			} else {
				err = fmt.Errorf("panic during query evaluation: %v", r)
			}
		}
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

	queryID, err := e.engine.activeQueryTracker.InsertWithDetails(ctx, e.originalExpression, "evaluation", len(e.nodeRequests) == 1, e.nodeRequests[0].TimeRange)
	if err != nil {
		return err
	}

	defer e.engine.activeQueryTracker.Delete(queryID)

	// The order of the deferred cancellations is important: we want to close all operators first, then
	// cancel with errQueryFinished and not a timeout, so we must defer this function last
	// (so that it runs before the cancellation of the context with timeout created above).
	defer func() {
		e.closeOperators()
		cancel(errQueryFinished)
	}()

	if e.engine.pedantic {
		// Close all operators a second time to ensure all operators behave correctly if Close is called multiple times.
		defer e.closeOperators()
	}

	if err := e.prepare(ctx); err != nil {
		return err
	}

	for _, req := range e.nodeRequests {
		if err := req.operator.AfterPrepare(ctx); err != nil {
			return err
		}
	}

	remainingOperatorEvaluators := make([]operatorEvaluator, 0, len(e.nodeRequests))

	for _, req := range e.nodeRequests {
		switch op := req.operator.(type) {
		case types.InstantVectorOperator:
			remainingOperatorEvaluators = append(remainingOperatorEvaluators, &instantVectorEvaluator{node: req.Node, operator: op})
		case types.RangeVectorOperator:
			remainingOperatorEvaluators = append(remainingOperatorEvaluators, &rangeVectorEvaluator{node: req.Node, operator: op})
		case types.ScalarOperator:
			remainingOperatorEvaluators = append(remainingOperatorEvaluators, &scalarEvaluator{node: req.Node, operator: op})
		case types.StringOperator:
			remainingOperatorEvaluators = append(remainingOperatorEvaluators, &stringEvaluator{node: req.Node, operator: op})
		default:
			return fmt.Errorf("operator type %T produces unknown result type", op)
		}
	}

	for len(remainingOperatorEvaluators) > 0 {
		for i := 0; i < len(remainingOperatorEvaluators); i++ {
			evaluator := remainingOperatorEvaluators[i]
			haveMoreWork, err := evaluator.performWork(ctx, e, observer)
			if err != nil {
				return err
			}

			if !haveMoreWork {
				if err := evaluator.finishedReading(ctx); err != nil {
					return err
				}

				remainingOperatorEvaluators = append(remainingOperatorEvaluators[:i], remainingOperatorEvaluators[i+1:]...)
				i--
			}
		}
	}

	if e.engine.pedantic {
		// Call FinishedReading on all operators a second time to ensure all operators behave correctly if FinishedReading is called multiple times.
		for _, req := range e.nodeRequests {
			if err := req.operator.FinishedReading(ctx); err != nil {
				return fmt.Errorf("pedantic mode: failed to call FinishedReading on the operator a second time after successfully finishedReading the first time: %w", err)
			}
		}
	}

	nodeInfo, err := e.finalize(ctx)
	if err != nil {
		return err
	}

	return observer.EvaluationCompleted(ctx, e, nodeInfo)
}

func (e *Evaluator) prepare(ctx context.Context) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "Evaluator.prepare")
	defer span.Finish()

	prepareParams := &types.PrepareParams{}

	for _, req := range e.nodeRequests {
		if err := req.operator.Prepare(ctx, prepareParams); err != nil {
			return fmt.Errorf("failed to prepare query: %w", err)
		}
	}
	return nil
}

func (e *Evaluator) finalize(ctx context.Context) (map[planning.Node]NodeCompletionInfo, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Evaluator.finalize")
	defer span.Finish()

	nodeInfo := make(map[planning.Node]NodeCompletionInfo, len(e.nodeRequests))
	for _, req := range e.nodeRequests {
		s, a, err := req.operator.Finalize(ctx)
		if err != nil {
			return nil, err
		}

		nodeInfo[req.Node] = NodeCompletionInfo{
			Annotations: a,
			Stats:       s,
		}
	}

	return nodeInfo, nil
}

func (e *Evaluator) closeOperators() {
	for _, req := range e.nodeRequests {
		req.operator.Close()
	}
}

type operatorEvaluator interface {
	// performWork performs the next piece of work for this operator.
	//
	// performWork returns true if there is more work remaining for this operator, or false otherwise.
	performWork(ctx context.Context, evaluator *Evaluator, observer EvaluationObserver) (bool, error)

	finishedReading(ctx context.Context) error
}

type instantVectorEvaluator struct {
	node            planning.Node
	operator        types.InstantVectorOperator
	seriesCount     int
	nextSeriesIndex int
}

func (e *instantVectorEvaluator) performWork(ctx context.Context, evaluator *Evaluator, observer EvaluationObserver) (bool, error) {
	if e.seriesCount == 0 {
		// First call: get series metadata.
		// If the operator returns no series, performWork won't be called a second time.

		series, err := e.operator.SeriesMetadata(ctx, nil)
		if err != nil {
			return false, err
		}

		e.seriesCount = len(series) // Take the length now, as the observer takes ownership of the slice when SeriesMetadataEvaluated is called.

		if err := observer.SeriesMetadataEvaluated(ctx, evaluator, e.node, series); err != nil {
			return false, err
		}

		return e.seriesCount > 0, nil
	}

	thisSeriesIndex := e.nextSeriesIndex

	d, err := e.operator.NextSeries(ctx)
	if err != nil {
		if errors.Is(err, types.EOS) {
			return false, fmt.Errorf("expected %v series, but only received %v", e.seriesCount, thisSeriesIndex)
		}

		return false, err
	}

	if err := observer.InstantVectorSeriesDataEvaluated(ctx, evaluator, e.node, thisSeriesIndex, e.seriesCount, d); err != nil {
		return false, err
	}

	e.nextSeriesIndex++
	haveMoreSeries := e.nextSeriesIndex < e.seriesCount

	return haveMoreSeries, nil
}

func (e *instantVectorEvaluator) finishedReading(ctx context.Context) error {
	return e.operator.FinishedReading(ctx)
}

type rangeVectorEvaluator struct {
	node            planning.Node
	operator        types.RangeVectorOperator
	seriesCount     int
	nextSeriesIndex int
}

func (e *rangeVectorEvaluator) performWork(ctx context.Context, evaluator *Evaluator, observer EvaluationObserver) (bool, error) {
	if e.seriesCount == 0 {
		// First call: get series metadata.
		// If the operator returns no series, performWork won't be called a second time.

		series, err := e.operator.SeriesMetadata(ctx, nil)
		if err != nil {
			return false, err
		}

		e.seriesCount = len(series) // Take the length now, as the observer takes ownership of the slice when SeriesMetadataEvaluated is called.

		if err := observer.SeriesMetadataEvaluated(ctx, evaluator, e.node, series); err != nil {
			return false, err
		}

		return e.seriesCount > 0, nil
	}

	thisSeriesIndex := e.nextSeriesIndex

	err := e.operator.NextSeries(ctx)
	if err != nil {
		if errors.Is(err, types.EOS) {
			return false, fmt.Errorf("expected %v series, but only received %v", e.seriesCount, thisSeriesIndex)
		}

		return false, err
	}

	stepIdx := 0
	for {
		step, err := e.operator.NextStepSamples(ctx)

		// nolint:errorlint // errors.Is introduces a performance overhead, and NextStepSamples is guaranteed to return exactly EOS, never a wrapped error.
		if err == types.EOS {
			break
		}

		if err != nil {
			return false, err
		}

		if err := observer.RangeVectorStepSamplesEvaluated(ctx, evaluator, e.node, thisSeriesIndex, stepIdx, step); err != nil {
			return false, err
		}

		stepIdx++
	}

	e.nextSeriesIndex++
	haveMoreSeries := e.nextSeriesIndex < e.seriesCount

	return haveMoreSeries, nil
}

func (e *rangeVectorEvaluator) finishedReading(ctx context.Context) error {
	return e.operator.FinishedReading(ctx)
}

type scalarEvaluator struct {
	node     planning.Node
	operator types.ScalarOperator
}

func (e *scalarEvaluator) performWork(ctx context.Context, evaluator *Evaluator, observer EvaluationObserver) (bool, error) {
	d, err := e.operator.GetValues(ctx)
	if err != nil {
		return false, err
	}

	return false, observer.ScalarEvaluated(ctx, evaluator, e.node, d)
}

func (e *scalarEvaluator) finishedReading(ctx context.Context) error {
	return e.operator.FinishedReading(ctx)
}

type stringEvaluator struct {
	node     planning.Node
	operator types.StringOperator
}

func (e *stringEvaluator) performWork(ctx context.Context, evaluator *Evaluator, observer EvaluationObserver) (bool, error) {
	v := e.operator.GetValue()

	return false, observer.StringEvaluated(ctx, evaluator, e.node, v)
}

func (e *stringEvaluator) finishedReading(ctx context.Context) error {
	return e.operator.FinishedReading(ctx)
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
	e.engine.memoryConsumptionTrackerFactory.Deregister(e.MemoryConsumptionTracker)
}

type EvaluationObserver interface {
	// SeriesMetadataEvaluated notifies this observer when series metadata has been evaluated.
	// Implementations of this method are responsible for returning the series slice to the pool when it is no longer needed.
	// Implementations of this method may mutate the series slice before returning it to the pool.
	SeriesMetadataEvaluated(ctx context.Context, evaluator *Evaluator, node planning.Node, series []types.SeriesMetadata) error

	// InstantVectorSeriesDataEvaluated notifies this observer when samples for an instant vector series have been evaluated.
	// Implementations of this method are responsible for returning seriesData to the pool when it is no longer needed.
	// Implementations of this method may mutate seriesData before returning it to the pool.
	InstantVectorSeriesDataEvaluated(ctx context.Context, evaluator *Evaluator, node planning.Node, seriesIndex int, seriesCount int, seriesData types.InstantVectorSeriesData) error

	// RangeVectorStepSamplesEvaluated notifies this observer when samples for a range vector step have been evaluated.
	// Implementations of this method must not mutate stepData, and should copy any data they wish to retain from stepData before returning.
	RangeVectorStepSamplesEvaluated(ctx context.Context, evaluator *Evaluator, node planning.Node, seriesIndex int, stepIndex int, stepData *types.RangeVectorStepData) error

	// ScalarEvaluated notifies this observer when a scalar has been evaluated.
	// Implementations of this method are responsible for returning data to the pool when it is no longer needed.
	// Implementations of this method may mutate data before returning it to the pool.
	ScalarEvaluated(ctx context.Context, evaluator *Evaluator, node planning.Node, data types.ScalarData) error

	// StringEvaluated notifies this observer when a string has been evaluated.
	StringEvaluated(ctx context.Context, evaluator *Evaluator, node planning.Node, data string) error

	// EvaluationCompleted notifies this observer when evaluation is complete.
	// Implementations of this method are responsible for closing the OperatorEvaluationStats instances when they are no longer needed.
	EvaluationCompleted(ctx context.Context, evaluator *Evaluator, nodeInfo map[planning.Node]NodeCompletionInfo) error
}

type NodeCompletionInfo struct {
	Annotations annotations.Annotations
	Stats       *types.OperatorEvaluationStats
}
