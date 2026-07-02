// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package selectors

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type RangeVectorSelector struct {
	Selector                 *Selector
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	rangeMilliseconds int64
	chunkIterator     chunkenc.Iterator
	matchesSubsets    []bool
	nextStepT         int64
	floats            *types.FPointRingBuffer
	histograms        *types.HPointRingBuffer
	stepData          *types.RangeVectorStepData // Retain the last step data instance we used to avoid allocating it for every step.
	evaluationStats   *types.OperatorEvaluationStats

	// Maintain metadata about modifications made to the floats buffer to support the smoothed/anchored extended range implementation.
	// A single instance is allocated (if required) and re-used between all steps and all series.
	extendedPointsState *RevertibleExtendedPointsState
}

var _ types.RangeVectorOperator = &RangeVectorSelector{}

func NewRangeVectorSelector(selector *Selector, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *RangeVectorSelector {
	rangeVectorSelector := RangeVectorSelector{
		Selector:                 selector,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		floats:                   types.NewFPointRingBuffer(memoryConsumptionTracker),
		histograms:               types.NewHPointRingBuffer(memoryConsumptionTracker),
		stepData:                 &types.RangeVectorStepData{Anchored: selector.Anchored, Smoothed: selector.Smoothed}, // Include the smoothed/anchored context to the step data as functions such as rate/increase require this
	}

	if selector.Anchored {
		rangeVectorSelector.extendedPointsState = NewRevertibleExtendedPointsState(rangeVectorSelector.floats, anchored)
	} else if selector.Smoothed {
		mode := smoothed
		if selector.CounterAware {
			mode = smoothedCounter
		}
		rangeVectorSelector.extendedPointsState = NewRevertibleExtendedPointsState(rangeVectorSelector.floats, mode)
	}

	return &rangeVectorSelector
}

func (m *RangeVectorSelector) ExpressionPosition() posrange.PositionRange {
	return m.Selector.ExpressionPosition
}

func (m *RangeVectorSelector) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	// Compute value we need on every call to NextSeries() once, here.
	m.rangeMilliseconds = m.Selector.Range.Milliseconds()
	return m.Selector.SeriesMetadata(ctx, matchers)
}

func (m *RangeVectorSelector) NextSeries(ctx context.Context) error {
	var err error
	m.chunkIterator, m.matchesSubsets, err = m.Selector.Next(ctx, m.chunkIterator)
	if err != nil {
		return err
	}
	m.nextStepT = m.Selector.TimeRange.StartT
	m.floats.Reset()
	m.histograms.Reset()
	if m.extendedPointsState != nil {
		// Any changes recorded will no longer be valid as the floats buffer has been reset
		m.extendedPointsState.Reset()
	}
	return nil
}

func (m *RangeVectorSelector) NextStepSamples(ctx context.Context) (*types.RangeVectorStepData, error) {
	if m.nextStepT > m.Selector.TimeRange.EndT {
		return nil, types.EOS
	}

	m.stepData.StepT = m.nextStepT
	rangeEnd := m.nextStepT
	m.nextStepT += m.Selector.TimeRange.IntervalMilliseconds

	if m.Selector.Timestamp != nil {
		// Timestamp from @ modifier takes precedence over query evaluation timestamp.
		rangeEnd = *m.Selector.Timestamp
	}

	// Apply offset after adjusting for timestamp from @ modifier.
	rangeEnd = rangeEnd - m.Selector.Offset
	rangeStart := rangeEnd - m.rangeMilliseconds

	// Take a copy of the original range since the smoothed/anchored modifiers will change these
	originalRangeStart := rangeStart
	originalRangeEnd := rangeEnd

	// When the smoothed/anchored modifiers are used, the selector (fillBuffer) will return a wider range of points.
	// Modify the range boundaries accordingly so that we do not discard these extended points.
	if m.Selector.Anchored {
		rangeStart -= m.Selector.LookbackDelta.Milliseconds()
	} else if m.Selector.Smoothed {
		rangeStart -= m.Selector.LookbackDelta.Milliseconds()
		rangeEnd += m.Selector.LookbackDelta.Milliseconds()
	}

	if m.Selector.Anchored || m.Selector.Smoothed {
		// Restore the buffer to its original points before the mutations were applied by the `ApplyBoundaryMutations` function.
		// The `ApplyBoundaryMutations` may have modified the points in the floats buffer - removing points and adding synthetic boundary points.
		// These changes must be reverted so the next step iteration does not use these values.
		// Note this must be done prior to calling the next fillBuffer
		if err := m.extendedPointsState.UndoChanges(); err != nil {
			return nil, err
		}
	}

	// Note that this rangeStart is the extended rangeStart when we are processing a smoothed/anchored request
	m.floats.DiscardPointsAtOrBefore(rangeStart)
	m.histograms.DiscardPointsAtOrBefore(rangeStart)

	fillBufferRequired := true

	// We may already have a point in the buffer after the range end. If we continue to
	// fill the float (or histogram) buffer we may pull in samples that are well outside the
	// current step's window.
	//
	// This can occur in a range query where there are missing samples or where a sparse
	// histogram series sits next to a dense float series: the last sample can be well past
	// rangeEnd and we do not need to fetch another sample until a future step's rangeEnd
	// moves beyond it.
	//
	// rangeEnd is the extended look-ahead end for smoothed (originalRangeEnd + lookback) and equals
	// originalRangeEnd for anchored and unmodified selectors. fillBuffer consumes a single,
	// time-ordered iterator, so the last sample pulled is max(floats.Last().T, histograms.Last().T).
	// If either is at or beyond rangeEnd then every sample before rangeEnd has already been pulled and
	// the window is fully covered, so we can skip the fill regardless of the modifier. For smoothed this
	// also ensures the look-ahead window is fully populated so that mixed float/histogram windows are
	// detected the same way Prometheus detects them across the full extended matrix.
	switch {
	case m.floats.Count() > 0 && m.floats.Last().T >= rangeEnd:
		fillBufferRequired = false
	case m.histograms.Count() > 0 && m.histograms.Last().T >= rangeEnd:
		fillBufferRequired = false
	}

	if fillBufferRequired {
		// Note - fillBuffer may result in the buffer having a point after the rangeEnd.
		if err := m.fillBuffer(m.floats, m.histograms, rangeStart, rangeEnd); err != nil {
			return nil, err
		}
	}

	// Update query stats before we perform any mutations for the anchored or smoothed modifier.
	m.evaluationStats.TrackSamplesForRangeVectorSelector(m.stepData.StepT, m.floats, m.histograms, originalRangeStart, originalRangeEnd, m.Selector.Timestamp != nil, m.matchesSubsets)

	// Pre-mutation snapshot of buffer counts. We use this to detect mixed-type ranges in the
	// extended look-back/look-ahead window for the anchored/smoothed paths, matching Prometheus's
	// extendedRate / extendedHistogramRate dispatch where mixed floats and histograms anywhere in
	// the extended window force the query to drop the point with MixedFloatsHistogramsWarning.
	//
	// The ring buffers can legitimately hold samples outside the current step's
	// (extendedRangeStart, extendedRangeEnd] window (a single trailing sample is retained after
	// fillBuffer stops, and gaps in the data can leave that sample far past extendedRangeEnd for
	// many subsequent steps). Restricting the count to the active window prevents an
	// out-of-window stray sample from spuriously flagging this step as mixed.
	m.stepData.MixedInExtendedRange = (m.Selector.Anchored || m.Selector.Smoothed) &&
		m.floats.CountBetween(rangeStart, rangeEnd) > 0 &&
		m.histograms.CountBetween(rangeStart, rangeEnd) > 0

	if m.Selector.AnchoredResetsChanges {
		// Anchored resets()/changes() select the anchor (the last sample at or before rangeStart) across both
		// floats and histograms and count transitions through the in-range samples. They do not need synthetic
		// float boundary values, so we leave the float buffer un-mutated and return the raw view spanning the
		// extended look-back window (which retains the pre-rangeStart anchor candidates). The anchor selection
		// itself happens in the resetsChanges step function.
		m.stepData.Floats = m.floats.ViewUntilSearchingBackwards(originalRangeEnd, m.stepData.Floats)
	} else if m.Selector.Anchored || m.Selector.Smoothed {
		if m.floats.Count() > 0 && m.floats.PointAt(0).T > originalRangeEnd {
			// This will be an empty set
			m.stepData.Floats = m.floats.ViewUntilSearchingBackwards(originalRangeEnd, m.stepData.Floats)
		} else {
			// Mutate the floats buffer to align and extend points to the original time boundaries.
			// The result is either an empty buffer or one containing only points within the
			// original time range, with points present at both boundaries.
			if err := m.extendedPointsState.ApplyBoundaryMutations(originalRangeStart, originalRangeEnd, rangeEnd); err != nil {
				return nil, err
			}

			// A ViewAll can be used since we know that this buffer only contains points within the requested range.
			m.stepData.Floats = m.floats.ViewAll(m.stepData.Floats)
		}
	} else {
		m.stepData.Floats = m.floats.ViewUntilSearchingBackwards(rangeEnd, m.stepData.Floats)
	}

	// For histograms we do not mutate the underlying ring buffer. The view spans the extended
	// look-back/look-ahead window (up to the extended rangeEnd).
	//
	// For the rate/increase/delta family this lets them pick or interpolate boundary values from points
	// outside the original range, mirroring Prometheus's extendedHistogramRate. For anchored resets()/changes()
	// the same raw view supplies the histogram anchor candidates and in-range histograms, which the resetsChanges
	// step function then anchors to match upstream Prometheus's pickFirstSampleIndices.
	m.stepData.Histograms = m.histograms.ViewUntilSearchingBackwards(rangeEnd, m.stepData.Histograms)
	m.stepData.RangeStart = originalRangeStart // important to return the original range start so that functions like rate() can determine the range duration regardless of smoothed / anchored
	m.stepData.RangeEnd = originalRangeEnd

	return m.stepData, nil
}

// fillBuffer will iterate through the chunkIterator and add points to the given ring buffers.
// points are accumulated into the buffer if they have a timestamp greater than rangeStart with the accumulation stopping
// once a point with a timestamp greater than or equal to rangeEnd has been accumulated.
// As such, no point is accumulated for rangeStart and there may be one point after rangeEnd in the buffer.
func (m *RangeVectorSelector) fillBuffer(floats *types.FPointRingBuffer, histograms *types.HPointRingBuffer, rangeStart, rangeEnd int64) error {
	// Keep filling the buffer until we reach the end of the range or the end of the iterator.

	for {
		valueType := m.chunkIterator.Next()

		switch valueType {
		case chunkenc.ValNone:
			// No more data. We are done.
			return m.chunkIterator.Err()
		case chunkenc.ValFloat:
			t, f := m.chunkIterator.At()
			if value.IsStaleNaN(f) || t <= rangeStart {
				// Range vectors ignore stale markers
				// https://github.com/prometheus/prometheus/issues/3746#issuecomment-361572859
				continue
			}

			// We might append a sample beyond the range end, but this is OK:
			// - callers of NextStepSamples are expected to pass the same RingBuffer to subsequent calls, so the point is not lost
			// - callers of NextStepSamples are expected to handle the case where the buffer contains points beyond the end of the range
			if _, err := floats.Append(promql.FPoint{T: t, F: f}); err != nil {
				return err
			}

			if t >= rangeEnd {
				return nil
			}
		case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
			t := m.chunkIterator.AtT()

			if t <= rangeStart {
				continue
			}

			hPoint, _, err := histograms.NextPoint()
			if err != nil {
				return err
			}
			hPoint.T, hPoint.H = m.chunkIterator.AtFloatHistogram(hPoint.H)
			if value.IsStaleNaN(hPoint.H.Sum) {
				// Range vectors ignore stale markers
				// https://github.com/prometheus/prometheus/issues/3746#issuecomment-361572859
				// We have to remove the last point since we didn't actually use it, and NextPoint already allocated it.
				histograms.RemoveLastPoint()
				continue
			}

			if t >= rangeEnd {
				return nil
			}
		default:
			return fmt.Errorf("unknown value type %s", valueType.String())
		}
	}
}

func (m *RangeVectorSelector) Prepare(ctx context.Context, params *types.PrepareParams) error {
	var err error
	m.evaluationStats, err = types.NewOperatorEvaluationStats(ctx, m.Selector.TimeRange, m.MemoryConsumptionTracker, len(m.Selector.Subsets))
	if err != nil {
		return err
	}
	return m.Selector.Prepare(ctx, params)
}

func (m *RangeVectorSelector) AfterPrepare(ctx context.Context) error {
	return nil
}

func (m *RangeVectorSelector) FinishedReading(ctx context.Context) error {
	m.floats.Close()
	m.histograms.Close()
	m.chunkIterator = nil
	m.Selector.Close()
	return nil
}

func (m *RangeVectorSelector) Finalize(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	stats := m.evaluationStats
	m.evaluationStats = nil
	return stats, nil, nil
}

func (m *RangeVectorSelector) Close() {
	// If the query fails, then FinishedReading above won't be called, so make sure to close the selector.
	m.Selector.Close()

	if m.evaluationStats != nil {
		m.evaluationStats.Close()
		m.evaluationStats = nil
	}
}
