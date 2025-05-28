// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type Subquery struct {
	Inner                types.InstantVectorOperator
	ParentQueryTimeRange types.QueryTimeRange
	ChildQueryTimeRange  types.QueryTimeRange

	SubqueryTimestamp *int64 // Milliseconds since Unix epoch, only set if selector uses @ modifier (eg. metric{...} @ 123)
	SubqueryOffset    int64  // In milliseconds
	SubqueryRange     time.Duration

	expressionPosition posrange.PositionRange

	nextStepT         int64
	rangeMilliseconds int64
	floats            *types.FPointRingBuffer
	histograms        *types.HPointRingBuffer
	stepData          *types.RangeVectorStepData // Retain the last step data instance we used to avoid allocating it for every step.

	ParentQueryStats *types.QueryStats
	ChildQueryStats  *types.QueryStats
}

var _ types.RangeVectorOperator = &Subquery{}

func NewSubquery(
	inner types.InstantVectorOperator,
	parentQueryTimeRange types.QueryTimeRange,
	childQueryTimeRange types.QueryTimeRange,
	subqueryTimestamp *int64,
	subqueryOffset time.Duration,
	subqueryRange time.Duration,
	expressionPosition posrange.PositionRange,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	parentQueryStats *types.QueryStats,
	childQueryStats *types.QueryStats,
) *Subquery {
	return &Subquery{
		Inner:                inner,
		ParentQueryTimeRange: parentQueryTimeRange,
		ChildQueryTimeRange:  childQueryTimeRange,
		SubqueryTimestamp:    subqueryTimestamp,
		SubqueryOffset:       subqueryOffset.Milliseconds(),
		SubqueryRange:        subqueryRange,
		expressionPosition:   expressionPosition,
		rangeMilliseconds:    subqueryRange.Milliseconds(),
		floats:               types.NewFPointRingBuffer(memoryConsumptionTracker),
		histograms:           types.NewHPointRingBuffer(memoryConsumptionTracker),
		stepData:             &types.RangeVectorStepData{},
		ParentQueryStats:     parentQueryStats,
		ChildQueryStats:      childQueryStats,
	}
}

func (s *Subquery) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	return s.Inner.SeriesMetadata(ctx)
}

func (s *Subquery) NextSeries(ctx context.Context) error {
	// Release the previous series' slices now, so we are likely to reuse the slices for the next series.
	s.floats.Release()
	s.histograms.Release()
	s.ChildQueryStats.Release()

	data, err := s.Inner.NextSeries(ctx)
	if err != nil {
		return err
	}

	s.nextStepT = s.ParentQueryTimeRange.StartT

	if err := s.floats.Use(data.Floats); err != nil {
		return err
	}

	// Clear counter reset information to avoid under- or over-counting resets.
	// See https://github.com/prometheus/prometheus/pull/15987 for more explanation.
	for _, p := range data.Histograms {
		if p.H.CounterResetHint == histogram.NotCounterReset || p.H.CounterResetHint == histogram.CounterReset {
			p.H.CounterResetHint = histogram.UnknownCounterReset
		}
	}

	if err := s.histograms.Use(data.Histograms); err != nil {
		return err
	}

	return nil
}

func (s *Subquery) NextStepSamples() (*types.RangeVectorStepData, error) {
	if s.nextStepT > s.ParentQueryTimeRange.EndT {
		return nil, types.EOS
	}

	s.stepData.StepT = s.nextStepT
	rangeEnd := s.nextStepT
	s.nextStepT += s.ParentQueryTimeRange.IntervalMilliseconds

	if s.SubqueryTimestamp != nil {
		// Timestamp from @ modifier takes precedence over query evaluation timestamp.
		rangeEnd = *s.SubqueryTimestamp
	}

	// Apply offset after adjusting for timestamp from @ modifier.
	rangeEnd = rangeEnd - s.SubqueryOffset
	rangeStart := rangeEnd - s.rangeMilliseconds
	s.floats.DiscardPointsAtOrBefore(rangeStart)
	s.histograms.DiscardPointsAtOrBefore(rangeStart)

	s.stepData.Floats = s.floats.ViewUntilSearchingForwards(rangeEnd, s.stepData.Floats)
	s.stepData.Histograms = s.histograms.ViewUntilSearchingForwards(rangeEnd, s.stepData.Histograms)
	s.stepData.RangeStart = rangeStart
	s.stepData.RangeEnd = rangeEnd

	if s.ChildQueryStats != nil && s.ChildQueryStats.TotalSamplesPerStep != nil {
		temp := s.samplesProcessedInChildQuery(s.stepData.RangeStart, s.stepData.RangeEnd)
		s.ParentQueryStats.IncrementSamplesAtTimestamp(s.stepData.StepT, temp)
	}

	return s.stepData, nil
}

// samplesProcessedInChildQuery returns the number of samples processed in child query that corresponds to the current parent query step.
func (s *Subquery) samplesProcessedInChildQuery(start, end int64) int64 {
	if s.ChildQueryStats == nil || s.ChildQueryStats.TotalSamplesPerStep == nil {
		return 0
	}

	sum := int64(0)
	childStartT := s.ChildQueryTimeRange.StartT
	childInterval := s.ChildQueryTimeRange.IntervalMilliseconds
	totalChildSteps := len(s.ChildQueryStats.TotalSamplesPerStep)
	
	for i := 0; i < totalChildSteps; i++ {
		childTimestamp := childStartT + int64(i)*childInterval
		if childTimestamp > start && childTimestamp <= end {
			sum += s.ChildQueryStats.TotalSamplesPerStep[i]
		} else if childTimestamp > end {
			break
		}
	}
	return sum
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
