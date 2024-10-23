// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package selectors

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type RangeVectorSelector struct {
	Selector *Selector

	rangeMilliseconds int64
	chunkIterator     chunkenc.Iterator
	nextT             int64
	floats            *types.FPointRingBuffer
	histograms        *types.HPointRingBuffer
}

var _ types.RangeVectorOperator = &RangeVectorSelector{}

func NewRangeVectorSelector(selector *Selector, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) *RangeVectorSelector {
	return &RangeVectorSelector{
		Selector:   selector,
		floats:     types.NewFPointRingBuffer(memoryConsumptionTracker),
		histograms: types.NewHPointRingBuffer(memoryConsumptionTracker),
	}
}

func (m *RangeVectorSelector) ExpressionPosition() posrange.PositionRange {
	return m.Selector.ExpressionPosition
}

func (m *RangeVectorSelector) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	// Compute value we need on every call to NextSeries() once, here.
	m.rangeMilliseconds = m.Selector.Range.Milliseconds()

	return m.Selector.SeriesMetadata(ctx)
}

func (m *RangeVectorSelector) StepCount() int {
	return m.Selector.TimeRange.StepCount
}

func (m *RangeVectorSelector) Range() time.Duration {
	return m.Selector.Range
}

func (m *RangeVectorSelector) NextSeries(ctx context.Context) error {
	var err error
	m.chunkIterator, err = m.Selector.Next(ctx, m.chunkIterator)
	if err != nil {
		return err
	}

	m.nextT = m.Selector.TimeRange.StartT
	m.floats.Reset()
	m.histograms.Reset()
	return nil
}

func (m *RangeVectorSelector) NextStepSamples() (types.RangeVectorStepData, error) {
	if m.nextT > m.Selector.TimeRange.EndT {
		return types.RangeVectorStepData{}, types.EOS
	}

	stepT := m.nextT
	rangeEnd := stepT

	if m.Selector.Timestamp != nil {
		// Timestamp from @ modifier takes precedence over query evaluation timestamp.
		rangeEnd = *m.Selector.Timestamp
	}

	// Apply offset after adjusting for timestamp from @ modifier.
	rangeEnd = rangeEnd - m.Selector.Offset
	rangeStart := rangeEnd - m.rangeMilliseconds
	m.floats.DiscardPointsBefore(rangeStart)
	m.histograms.DiscardPointsBefore(rangeStart)

	if err := m.fillBuffer(m.floats, m.histograms, rangeStart, rangeEnd); err != nil {
		return types.RangeVectorStepData{}, err
	}

	m.nextT += m.Selector.TimeRange.IntervalMilliseconds

	return types.RangeVectorStepData{
		Floats:     m.floats.ViewUntil(rangeEnd, false),
		Histograms: m.histograms.ViewUntil(rangeEnd, false),
		StepT:      stepT,
		RangeStart: rangeStart,
		RangeEnd:   rangeEnd,
	}, nil
}

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
			if value.IsStaleNaN(f) || t < rangeStart {
				// Range vectors ignore stale markers
				// https://github.com/prometheus/prometheus/issues/3746#issuecomment-361572859
				continue
			}

			// We might append a sample beyond the range end, but this is OK:
			// - callers of NextStepSamples are expected to pass the same RingBuffer to subsequent calls, so the point is not lost
			// - callers of NextStepSamples are expected to handle the case where the buffer contains points beyond the end of the range
			if err := floats.Append(promql.FPoint{T: t, F: f}); err != nil {
				return err
			}

			if t >= rangeEnd {
				return nil
			}
		case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
			t := m.chunkIterator.AtT()
			if t < rangeStart {
				continue
			}
			hPoint, _ := histograms.NextPoint()
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

func (m *RangeVectorSelector) Close() {
	m.Selector.Close()
	m.floats.Close()
	m.histograms.Close()
}
