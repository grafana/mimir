// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
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

	memoryConsumptionTracker *limiting.MemoryConsumptionTracker
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
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
) *Subquery {
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
	}
}

func (s *Subquery) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
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

	s.parentQueryStats.IncrementSamplesAtTimestamp(s.stepData.StepT, s.samplesProcessedInSubquery(s.stepData.RangeStart, s.stepData.RangeEnd))

	return s.stepData, nil
}

// samplesProcessedInSubquery returns the number of samples processed in a time range selected by parent query step from subquery results.
func (s *Subquery) samplesProcessedInSubquery(start, end int64) int64 {
	startIndex := 0
	if start >= s.SubqueryTimeRange.StartT {
		startIndex = int((start-s.SubqueryTimeRange.StartT)/s.SubqueryTimeRange.IntervalMilliseconds) + 1
	}

	endIndex := int((end - s.SubqueryTimeRange.StartT) / s.SubqueryTimeRange.IntervalMilliseconds)
	if endIndex >= len(s.subqueryStats.TotalSamplesPerStep) {
		endIndex = len(s.subqueryStats.TotalSamplesPerStep) - 1
	}

	sum := int64(0)
	for i := startIndex; i <= endIndex; i++ {
		sum += s.subqueryStats.TotalSamplesPerStep[i]
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

func (s *Subquery) Prepare(ctx context.Context, params *types.PrepareParams) error {
	s.parentQueryStats = params.QueryStats
	// Child Samples will be counted later when processing subquery results.
	childStats, err := types.NewQueryStats(s.SubqueryTimeRange, true, s.memoryConsumptionTracker)
	if err != nil {
		return fmt.Errorf("could not create stats for subquery: %w", err)
	}
	s.subqueryStats = childStats
	childParams := types.PrepareParams{
		QueryStats: childStats,
	}
	return s.Inner.Prepare(ctx, &childParams)
}

func (s *Subquery) Close() {
	s.Inner.Close()
	s.histograms.Close()
	s.floats.Close()
	s.subqueryStats.Close()
}
