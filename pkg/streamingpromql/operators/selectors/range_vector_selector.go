// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package selectors

import (
	"context"
	"errors"
	"fmt"

	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type RangeVectorSelector struct {
	Selector *Selector
	Stats    *types.QueryStats

	rangeMilliseconds int64
	chunkIterator     chunkenc.Iterator
	nextStepT         int64
	floats            *types.FPointRingBuffer
	histograms        *types.HPointRingBuffer
	stepData          *types.RangeVectorStepData // Retain the last step data instance we used to avoid allocating it for every step.

	// Maintain metadata about modifications made to the floats buffer to support the smoothed/anchored extended range implementation.
	// A single instance is allocated (if required) and re-used between all steps and all series.
	extendedPointsState *RevertibleExtendedPointsState
}

var _ types.RangeVectorOperator = &RangeVectorSelector{}

func NewRangeVectorSelector(selector *Selector, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, stats *types.QueryStats) *RangeVectorSelector {

	rangeVectorSelector := RangeVectorSelector{
		Selector:   selector,
		Stats:      stats,
		floats:     types.NewFPointRingBuffer(memoryConsumptionTracker),
		histograms: types.NewHPointRingBuffer(memoryConsumptionTracker),
		stepData:   &types.RangeVectorStepData{Anchored: selector.Anchored, Smoothed: selector.Smoothed}, // Include the smoothed/anchored context to the step data as functions such as rate/increase require this
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
	m.chunkIterator, err = m.Selector.Next(ctx, m.chunkIterator)
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
	// fill the float buffer we may have multiple points after the rangeEnd.
	//
	// This can occur in a range query where there are missing samples.
	// The last point can be well past the rangeEnd and we do not need to get another point until
	// the step iterations takes us to a new rangeEnd beyond this point.
	//
	// Note that we only do this for smoothed/anchored since it does not care about histograms.
	if (m.Selector.Anchored || m.Selector.Smoothed) && m.floats.Count() > 0 {
		last := m.floats.Last()
		if last.T >= originalRangeEnd {
			fillBufferRequired = false
		}
	}

	var err error
	histogramObserved := false
	if fillBufferRequired {
		// Note - fillBuffer may result in the buffer having a point after the rangeEnd.
		histogramObserved, err = m.fillBuffer(m.floats, m.histograms, rangeStart, originalRangeEnd, m.Selector.Anchored || m.Selector.Smoothed)
		if err != nil {
			return nil, err
		}
	}

	if m.Selector.Anchored || m.Selector.Smoothed {
		// Histograms are not supported for these modified range queries
		if histogramObserved {
			return nil, errors.New("smoothed and anchored modifiers do not work with native histograms")
		}

		// Mutate the floats buffer to align and extend points to the original time boundaries.
		// The result is either an empty buffer or one containing only points within the
		// original time range, with points present at both boundaries.
		err = m.extendedPointsState.ApplyBoundaryMutations(originalRangeStart, originalRangeEnd, rangeEnd)
		if err != nil {
			return nil, err
		}

		// A ViewAll can be used since we know that this buffer only contains points within the requested range.
		m.stepData.Floats = m.floats.ViewAll(m.stepData.Floats)
	} else {
		m.stepData.Floats = m.floats.ViewUntilSearchingBackwards(rangeEnd, m.stepData.Floats)
	}

	m.stepData.Histograms = m.histograms.ViewUntilSearchingBackwards(rangeEnd, m.stepData.Histograms)
	m.stepData.RangeStart = originalRangeStart // important to return the original range start so that functions like rate() can determine the range duration regardless of smoothed / anchored
	m.stepData.RangeEnd = originalRangeEnd

	m.Stats.IncrementSamples(int64(m.stepData.Floats.Count()) + m.stepData.Histograms.EquivalentFloatSampleCount())

	return m.stepData, nil
}

// fillBuffer will iterate through the chunkIterator and add points to the given ring buffers.
// points are accumulated into the buffer if they have a timestamp greater than rangeStart with the accumulation stopping
// once a point with a timestamp greater than or equal to rangeEnd has been accumulated.
// As such, no point is accumulated for rangeStart and there may be one point after rangeEnd in the buffer.
func (m *RangeVectorSelector) fillBuffer(floats *types.FPointRingBuffer, histograms *types.HPointRingBuffer, rangeStart, rangeEnd int64, smoothedOrAnchored bool) (bool, error) {
	// Keep filling the buffer until we reach the end of the range or the end of the iterator.
	histogramObserved := false

	for {
		valueType := m.chunkIterator.Next()

		switch valueType {
		case chunkenc.ValNone:
			// No more data. We are done.
			return histogramObserved, m.chunkIterator.Err()
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
			if err := floats.Append(promql.FPoint{T: t, F: f}); err != nil {
				return false, err
			}

			if t >= rangeEnd {
				return histogramObserved, nil
			}
		case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
			t := m.chunkIterator.AtT()

			if t <= rangeStart {
				continue
			}

			hPoint, err := histograms.NextPoint()
			if err != nil {
				return false, err
			}
			hPoint.T, hPoint.H = m.chunkIterator.AtFloatHistogram(hPoint.H)
			if value.IsStaleNaN(hPoint.H.Sum) {
				// Range vectors ignore stale markers
				// https://github.com/prometheus/prometheus/issues/3746#issuecomment-361572859
				// We have to remove the last point since we didn't actually use it, and NextPoint already allocated it.
				histograms.RemoveLastPoint()
				continue
			}

			histogramObserved = true

			if t >= rangeEnd {
				return histogramObserved, nil
			}
		default:
			return false, fmt.Errorf("unknown value type %s", valueType.String())
		}
	}
}

func (m *RangeVectorSelector) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return m.Selector.Prepare(ctx, params)
}

func (m *RangeVectorSelector) AfterPrepare(ctx context.Context) error {
	return nil
}

func (m *RangeVectorSelector) Finalize(ctx context.Context) error {
	// Nothing to do.
	return nil
}

func (m *RangeVectorSelector) Close() {
	m.Selector.Close()
	m.floats.Close()
	m.histograms.Close()
	m.chunkIterator = nil
}
