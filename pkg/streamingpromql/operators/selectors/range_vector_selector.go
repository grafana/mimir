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

	extendedRangeFloats *types.FPointRingBuffer // A buffer we use to create views for smoothed/anchored extended ranges which have added/modified points from the original floats buffer
	extendedRangeView   *types.FPointRingBufferView

	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	anchored                 bool // The anchored modifier has been used for this range query
	smoothed                 bool // The smoothed modifier has been used for this range query
}

var _ types.RangeVectorOperator = &RangeVectorSelector{}

func NewRangeVectorSelector(selector *Selector, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, stats *types.QueryStats, anchored bool, smoothed bool) *RangeVectorSelector {

	rangeVectorSelector := RangeVectorSelector{
		Selector:                 selector,
		Stats:                    stats,
		floats:                   types.NewFPointRingBuffer(memoryConsumptionTracker),
		histograms:               types.NewHPointRingBuffer(memoryConsumptionTracker),
		stepData:                 &types.RangeVectorStepData{Anchored: anchored, Smoothed: smoothed}, // Include the smoothed/anchored context to the step data as functions such as rate/increase require this
		anchored:                 anchored,
		smoothed:                 smoothed,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}

	if anchored || smoothed {
		rangeVectorSelector.extendedRangeFloats = types.NewFPointRingBuffer(memoryConsumptionTracker)
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
	if m.extendedRangeFloats != nil {
		m.extendedRangeFloats.Reset()
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

	// Take a copy of the original range - the smoothed/anchored modifiers will change these
	originalRangeStart := rangeStart
	originalRangeEnd := rangeEnd

	// When the smoothed/anchored modifiers are used, the selector (fillBuffer) will return a wider range of points.
	// Modify the range boundaries accordingly so that we do not discard these extended points.
	if m.anchored {
		rangeStart -= m.Selector.LookbackDelta.Milliseconds()
	} else if m.smoothed {
		rangeStart -= m.Selector.LookbackDelta.Milliseconds()
		rangeEnd += m.Selector.LookbackDelta.Milliseconds()
	}

	m.floats.DiscardPointsAtOrBefore(rangeStart)
	m.histograms.DiscardPointsAtOrBefore(rangeStart)
	m.stepData.SmoothedBasisPointsSetMask = 0

	// Fill the buffer with an extended range of points (smoothed/anchored) - these will be filtered out in the extendRangeVectorPoints() below
	histogramObserved, err := m.fillBuffer(m.floats, m.histograms, originalRangeStart, originalRangeEnd, rangeStart)
	if err != nil {
		return nil, err
	}

	if m.anchored || m.smoothed {
		m.extendedRangeFloats.Release()

		// Histograms are not supported for these modified range queries
		if histogramObserved {
			return nil, errors.New("smoothed and anchored modifiers do not work with native histograms")
		}

		// Note the extended range end is used since smoothed will have extended this
		m.extendedRangeView = m.floats.ViewUntilSearchingBackwards(rangeEnd, m.extendedRangeView)

		var buff []promql.FPoint
		if m.extendedRangeView.Any() {
			// ignore ok as we already tested that we have points
			lastInView, _ := m.extendedRangeView.Last()

			// No points were found within the original range.
			// If we only find points prior to the start of the original range then no points are returned.
			if lastInView.T > originalRangeStart {
				buff, m.stepData.SmoothedBasisPointsSetMask, err = extendRangeVectorPoints(m.extendedRangeView, originalRangeStart, originalRangeEnd, m.smoothed, &m.stepData.SmoothedBasisForHeadPoint, &m.stepData.SmoothedBasisForTailPoint, m.memoryConsumptionTracker)
				if err != nil {
					return nil, err
				}
			}
		}

		if buff != nil {
			err := m.extendedRangeFloats.Use(buff)
			if err != nil {
				return nil, err
			}
		}

		// Store the smoothed points in the range step data result so that consumers of this data can reference these values
		// without having to re-calculate off the original points. Re-use the view
		m.stepData.Floats = m.extendedRangeFloats.ViewAll(m.stepData.Floats)
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
// points are accumulated into the buffer if they are rangeStart < T <= rangeEnd.
// When extendedRangeStart != rangeStart, the last point which is extendedRangeStart < T <= rangeStart is accumulated into the buffer.
// When extendedRangeStart != rangeStart, the float buffer is automatically left-trimmed to ensure that there is at most 1 point <= rangeStart.
func (m *RangeVectorSelector) fillBuffer(floats *types.FPointRingBuffer, histograms *types.HPointRingBuffer, rangeStart, rangeEnd int64, extendedRangeStart int64) (bool, error) {
	// Keep filling the buffer until we reach the end of the range or the end of the iterator.

	if extendedRangeStart != rangeStart {
		defer func() {
			// This ensures that we have at most 1 point <= rangeStart
			// Note we only do this for floats, as this is only relevant to the smoothed/anchored implementation which does not consider histograms
			m.extendedRangeView = m.floats.ViewUntilSearchingForwards(rangeStart, m.extendedRangeView)
			last, ok := m.extendedRangeView.Last()
			if ok {
				m.floats.DiscardPointsAtOrBefore(last.T - 1)
			}
		}()
	}

	histogramObserved := false

	for {
		valueType := m.chunkIterator.Next()

		switch valueType {
		case chunkenc.ValNone:
			// No more data. We are done.
			return histogramObserved, m.chunkIterator.Err()
		case chunkenc.ValFloat:
			t, f := m.chunkIterator.At()
			if value.IsStaleNaN(f) || t <= extendedRangeStart {
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

			if t <= extendedRangeStart {
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

func (m *RangeVectorSelector) Finalize(ctx context.Context) error {
	// Nothing to do.
	return nil
}

func (m *RangeVectorSelector) Close() {
	m.Selector.Close()
	m.floats.Close()
	if m.extendedRangeFloats != nil {
		m.extendedRangeFloats.Close()
	}
	m.histograms.Close()
	m.chunkIterator = nil
}
