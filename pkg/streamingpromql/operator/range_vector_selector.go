// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package operator

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type RangeVectorSelector struct {
	Selector *Selector

	rangeMilliseconds int64
	numSteps          int

	chunkIterator chunkenc.Iterator
	nextT         int64
}

var _ RangeVectorOperator = &RangeVectorSelector{}

func (m *RangeVectorSelector) SeriesMetadata(ctx context.Context) ([]SeriesMetadata, error) {
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

func (m *RangeVectorSelector) NextSeries(_ context.Context) error {
	var err error
	m.chunkIterator, err = m.Selector.Next(m.chunkIterator)
	if err != nil {
		return err
	}

	m.nextT = m.Selector.Start
	return nil
}

func (m *RangeVectorSelector) NextStepSamples(floats *RingBuffer) (RangeVectorStepData, error) {
	if m.nextT > m.Selector.End {
		return RangeVectorStepData{}, EOS
	}

	stepT := m.nextT
	rangeEnd := stepT

	if m.Selector.Timestamp != nil {
		rangeEnd = *m.Selector.Timestamp
	}

	rangeStart := rangeEnd - m.rangeMilliseconds
	floats.DiscardPointsBefore(rangeStart)

	if err := m.fillBuffer(floats, rangeStart, rangeEnd); err != nil {
		return RangeVectorStepData{}, err
	}

	m.nextT += m.Selector.Interval

	return RangeVectorStepData{
		StepT:      stepT,
		RangeStart: rangeStart,
		RangeEnd:   rangeEnd,
	}, nil
}

func (m *RangeVectorSelector) fillBuffer(floats *RingBuffer, rangeStart, rangeEnd int64) error {
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
				continue
			}

			floats.Append(promql.FPoint{T: t, F: f})

			if t >= rangeEnd {
				return nil
			}
		default:
			// TODO: handle native histograms
			return fmt.Errorf("unknown value type %s", valueType.String())
		}
	}
}

func (m *RangeVectorSelector) Close() {
	if m.Selector != nil {
		m.Selector.Close()
	}
}
