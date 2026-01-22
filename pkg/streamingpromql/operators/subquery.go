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

	expressionPosition posrange.PositionRange

	nextStepT         int64
	rangeMilliseconds int64
	floats            *types.FPointRingBuffer
	histograms        *types.HPointRingBuffer
	stepData          *types.RangeVectorStepData // Retain the last step data instance we used to avoid allocating it for every step.

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
	return &Subquery{
		Inner:                    inner,
		ParentQueryTimeRange:     parentQueryTimeRange,
		SubqueryTimeRange:        subqueryTimeRange,
		SubqueryTimestamp:        subqueryTimestamp,
		SubqueryOffset:           subqueryOffset.Milliseconds(),
		expressionPosition:       expressionPosition,
		rangeMilliseconds:        subqueryRange.Milliseconds(),
		floats:                   types.NewFPointRingBuffer(memoryConsumptionTracker),
		histograms:               types.NewHPointRingBuffer(memoryConsumptionTracker),
		stepData:                 &types.RangeVectorStepData{},
		memoryConsumptionTracker: memoryConsumptionTracker,
	}, nil
}

func (s *Subquery) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	if s.SubqueryTimeRange.StepCount == 0 {
		// There are no steps in the subquery time range.
		// This can happen with queries like "metric[7m:1h]" if the 7m range doesn't overlap with the beginning of an hour.
		return nil, nil
	}

	return s.Inner.SeriesMetadata(ctx, matchers)
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

	return s.stepData, nil
}

func (s *Subquery) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *Subquery) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return s.Inner.Prepare(ctx, params)
}

func (s *Subquery) AfterPrepare(ctx context.Context) error {
	return s.Inner.AfterPrepare(ctx)
}

func (s *Subquery) Finalize(ctx context.Context) error {
	return s.Inner.Finalize(ctx)
}

func (s *Subquery) Close() {
	s.Inner.Close()
	s.histograms.Close()
	s.floats.Close()
}
