// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type Subquery struct {
	Inner                types.InstantVectorOperator
	ParentQueryTimeRange types.QueryTimeRange
	SubqueryTimeRange    types.QueryTimeRange

	SubqueryTimestamp *int64 // Milliseconds since Unix epoch, only set if selector uses @ modifier (eg. metric{...} @ 123)
	SubqueryOffset    int64  // In milliseconds
	SubqueryRange     time.Duration

	expressionPosition posrange.PositionRange

	nextStepT         int64
	rangeMilliseconds int64
	floats            *types.FPointRingBuffer
	histograms        *types.HPointRingBuffer
	stepData          *types.RangeVectorStepData // Retain the last step data instance we used to avoid allocating it for every step.

	parentQueryStats *types.QueryStats
	subqueryStats    *types.QueryStats

	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

var _ types.RangeVectorOperator = &Subquery{}

func NewSubquery(
	inner types.InstantVectorOperator,
	parentQueryTimeRange types.QueryTimeRange,
	subqueryTimeRange types.QueryTimeRange,
	subqueryTimestamp *int64,
	subqueryOffset time.Duration,
	subqueryRange time.Duration,
	expressionPosition posrange.PositionRange,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
) (*Subquery, error) {
	// Final aggregation into parent stats happens in samplesProcessedInSubquery().
	// Child samples always counted per step to count parent stats correctly.
	childStats, err := types.NewQueryStats(subqueryTimeRange, true, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}
	return &Subquery{
		Inner:                    inner,
		ParentQueryTimeRange:     parentQueryTimeRange,
		SubqueryTimeRange:        subqueryTimeRange,
		SubqueryTimestamp:        subqueryTimestamp,
		SubqueryOffset:           subqueryOffset.Milliseconds(),
		SubqueryRange:            subqueryRange,
		expressionPosition:       expressionPosition,
		rangeMilliseconds:        subqueryRange.Milliseconds(),
		floats:                   types.NewFPointRingBuffer(memoryConsumptionTracker),
		histograms:               types.NewHPointRingBuffer(memoryConsumptionTracker),
		stepData:                 &types.RangeVectorStepData{},
		memoryConsumptionTracker: memoryConsumptionTracker,
		subqueryStats:            childStats,
	}, nil
}

func (s *Subquery) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	if s.SubqueryTimeRange.StepCount == 0 {
		// There are no steps in the subquery time range.
		// This can happen with queries like "metric[7m:1h]" if the 7m range doesn't overlap with the beginning of an hour.
		return nil, nil
	}

	return s.Inner.SeriesMetadata(ctx)
}

func (s *Subquery) NextSeries(ctx context.Context) error {
	// Release the previous series' slices now, so we are likely to reuse the slices for the next series.
	s.floats.Release()
	s.histograms.Release()
	// Clear the subquery stats to not double count samples from previous series in a next series.
	s.subqueryStats.Clear()

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

func (s *Subquery) NextStepSamples(ctx context.Context) (*types.RangeVectorStepData, error) {
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

	s.parentQueryStats.IncrementSamplesAtTimestamp(
		s.stepData.StepT,
		s.samplesProcessedInSubqueryPerParentStep(s.stepData),
	)

	return s.stepData, nil
}

// samplesProcessedInSubqueryPerParentStep returns the number of samples processed by subquery
// within the parent query step range.
func (s *Subquery) samplesProcessedInSubqueryPerParentStep(step *types.RangeVectorStepData) int64 {
	sum := int64(0)
	startIndex := int64(0)
	// if step.RangeStart within SubqueryTimeRange - Calculate index of a first step with T > step.RangeStart
	// Otherwise, it's outside SubqueryTimeRange - first step after step.RangeStart is 0.
	if step.RangeStart >= s.SubqueryTimeRange.StartT {
		startIndex = (step.RangeStart-s.SubqueryTimeRange.StartT)/(s.SubqueryTimeRange.IntervalMilliseconds) + 1
	}

	endIndex := (step.RangeEnd - s.SubqueryTimeRange.StartT) / s.SubqueryTimeRange.IntervalMilliseconds

	for i := startIndex; i <= endIndex; i++ {
		sum += s.subqueryStats.TotalSamplesPerStep[i]
	}

	return sum
}

func (s *Subquery) Range() time.Duration {
	return s.SubqueryRange
}

func (s *Subquery) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *Subquery) Prepare(ctx context.Context, params *types.PrepareParams) error {
	s.parentQueryStats = params.QueryStats
	childParams := types.PrepareParams{
		QueryStats: s.subqueryStats,
	}
	return s.Inner.Prepare(ctx, &childParams)
}

func (s *Subquery) Finalize(ctx context.Context) error {
	return s.Inner.Finalize(ctx)
}

func (s *Subquery) Close() {
	s.Inner.Close()
	s.histograms.Close()
	s.floats.Close()
	s.subqueryStats.Close()
}
