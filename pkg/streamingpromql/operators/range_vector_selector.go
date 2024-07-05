// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package operators

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type RangeVectorSelector struct {
	Selector *Selector

	rangeMilliseconds int64
	numSteps          int

	chunkIterator chunkenc.Iterator
	nextT         int64
}

var _ types.RangeVectorOperator = &RangeVectorSelector{}

func (m *RangeVectorSelector) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	// Compute value we need on every call to NextSeries() once, here.
	m.rangeMilliseconds = m.Selector.Range.Milliseconds()
	m.numSteps = stepCount(m.Selector.Start, m.Selector.End, m.Selector.Interval)

	return m.Selector.SeriesMetadata(ctx)
}

func (m *RangeVectorSelector) StepCount() int {
	return m.numSteps
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

	m.nextT = m.Selector.Start
	return nil
}

func (m *RangeVectorSelector) NextStepSamples(floats *types.FPointRingBuffer, histograms *types.HPointRingBuffer) (types.RangeVectorStepData, error) {
	if m.nextT > m.Selector.End {
		return types.RangeVectorStepData{}, types.EOS
	}

	stepT := m.nextT
	rangeEnd := stepT

	if m.Selector.Timestamp != nil {
		rangeEnd = *m.Selector.Timestamp
	}

	rangeStart := rangeEnd - m.rangeMilliseconds
	floats.DiscardPointsBefore(rangeStart)
	histograms.DiscardPointsBefore(rangeStart)

	if err := m.fillBuffer(floats, histograms, rangeStart, rangeEnd); err != nil {
		return types.RangeVectorStepData{}, err
	}

	m.nextT += m.Selector.Interval

	return types.RangeVectorStepData{
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
			if hPoint.H == nil {
				hPoint.H = &histogram.FloatHistogram{}
			}
			t, h := m.chunkIterator.AtFloatHistogram(hPoint.H)
			if value.IsStaleNaN(h.Sum) {
				// Range vectors ignore stale markers
				// https://github.com/prometheus/prometheus/issues/3746#issuecomment-361572859
				// We have to remove the last point since we didn't actually use it, and NextPoint already allocated it.
				histograms.RemoveLastPoint()
				continue
			}
			hPoint.T = t

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
}
