// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type Subquery struct {
	Inner                types.InstantVectorOperator
	ParentQueryTimeRange types.QueryTimeRange

	SubqueryTimestamp *int64 // Milliseconds since Unix epoch, only set if selector uses @ modifier (eg. metric{...} @ 123)
	SubqueryOffset    int64  // In milliseconds
	SubqueryRange     time.Duration

	expressionPosition posrange.PositionRange

	nextStepT         int64
	rangeMilliseconds int64
	floats            *types.FPointRingBuffer
	histograms        *types.HPointRingBuffer
}

var _ types.RangeVectorOperator = &Subquery{}

func NewSubquery(
	inner types.InstantVectorOperator,
	parentQueryTimeRange types.QueryTimeRange,
	subqueryTimestamp *int64,
	subqueryOffset time.Duration,
	subqueryRange time.Duration,
	expressionPosition posrange.PositionRange,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
) *Subquery {
	return &Subquery{
		Inner:                inner,
		ParentQueryTimeRange: parentQueryTimeRange,
		SubqueryTimestamp:    subqueryTimestamp,
		SubqueryOffset:       subqueryOffset.Milliseconds(),
		SubqueryRange:        subqueryRange,
		expressionPosition:   expressionPosition,
		rangeMilliseconds:    subqueryRange.Milliseconds(),
		floats:               types.NewFPointRingBuffer(memoryConsumptionTracker),
		histograms:           types.NewHPointRingBuffer(memoryConsumptionTracker),
	}
}

func (s *Subquery) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	return s.Inner.SeriesMetadata(ctx)
}

func (s *Subquery) NextSeries(ctx context.Context) error {
	// Release the previous series' slices now, so we are likely to reuse the slices for the next series.
	s.floats.Release()
	s.histograms.Release()

	data, err := s.Inner.NextSeries(ctx)
	if err != nil {
		return err
	}

	s.nextStepT = s.ParentQueryTimeRange.StartT
	s.floats.Use(data.Floats)
	s.histograms.Use(data.Histograms)
	return nil
}

func (s *Subquery) NextStepSamples() (types.RangeVectorStepData, error) {
	if s.nextStepT > s.ParentQueryTimeRange.EndT {
		return types.RangeVectorStepData{}, types.EOS
	}

	stepT := s.nextStepT
	rangeEnd := stepT

	if s.SubqueryTimestamp != nil {
		// Timestamp from @ modifier takes precedence over query evaluation timestamp.
		rangeEnd = *s.SubqueryTimestamp
	}

	// Apply offset after adjusting for timestamp from @ modifier.
	rangeEnd = rangeEnd - s.SubqueryOffset
	rangeStart := rangeEnd - s.rangeMilliseconds
	s.floats.DiscardPointsBefore(rangeStart)
	s.histograms.DiscardPointsBefore(rangeStart)

	s.nextStepT += s.ParentQueryTimeRange.IntervalMilliseconds

	return types.RangeVectorStepData{
		Floats:     s.floats,
		Histograms: s.histograms,
		StepT:      stepT,
		RangeStart: rangeStart,
		RangeEnd:   rangeEnd,
	}, nil
}

func (s *Subquery) StepCount() int {
	return s.ParentQueryTimeRange.StepCount
}

func (s *Subquery) Range() time.Duration {
	return s.SubqueryRange
}

func (s *Subquery) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *Subquery) Close() {
	s.Inner.Close()
	s.histograms.Close()
	s.floats.Close()
}
