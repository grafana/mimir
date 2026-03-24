// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type StepInvariantInstantVectorOperator struct {
	inner                    types.InstantVectorOperator
	originalTimeRange        types.QueryTimeRange
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

func NewStepInvariantInstantVectorOperator(op types.InstantVectorOperator, originalTimeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *StepInvariantInstantVectorOperator {
	return &StepInvariantInstantVectorOperator{
		inner:                    op,
		originalTimeRange:        originalTimeRange,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
}

func (s *StepInvariantInstantVectorOperator) ExpressionPosition() posrange.PositionRange {
	return s.inner.ExpressionPosition()
}

func (s *StepInvariantInstantVectorOperator) Close() {
	s.inner.Close()
}

func (s *StepInvariantInstantVectorOperator) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return s.inner.Prepare(ctx, params)
}

func (s *StepInvariantInstantVectorOperator) AfterPrepare(ctx context.Context) error {
	return s.inner.AfterPrepare(ctx)
}

func (s *StepInvariantInstantVectorOperator) Finalize(ctx context.Context) error {
	return s.inner.Finalize(ctx)
}

func (s *StepInvariantInstantVectorOperator) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	return s.inner.SeriesMetadata(ctx, matchers)
}

func (s *StepInvariantInstantVectorOperator) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	data, err := s.inner.NextSeries(ctx)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if s.originalTimeRange.IsInstant || s.originalTimeRange.StepCount <= 1 {
		return data, nil
	}

	// The inner query should be pinned to a single point in time with a single step. We don't expect to have multiple values per series here
	if len(data.Floats) > 1 || len(data.Histograms) > 1 {
		return types.InstantVectorSeriesData{}, fmt.Errorf("expected a single value float or histogram series, but got %d floats and %d histograms", len(data.Floats), len(data.Histograms))
	}

	if len(data.Floats) == 1 {
		// Request a new slice based off the expected step count.
		// Although a new slice is retrieved from the pool and the old returned, this ensures we adhere to memory allocation checks within the pool.
		floats, err := types.FPointSlicePool.Get(s.originalTimeRange.StepCount, s.memoryConsumptionTracker)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		// Fill the expected steps with the same point.
		for ts := s.originalTimeRange.StartT; ts <= s.originalTimeRange.EndT; ts += s.originalTimeRange.IntervalMilliseconds {
			floats = append(floats, promql.FPoint{
				T: ts,
				F: data.Floats[0].F,
			})
		}

		// release the original slice memory
		types.FPointSlicePool.Put(&data.Floats, s.memoryConsumptionTracker)
		data.Floats = floats

	} else if len(data.Histograms) == 1 {
		// As per comment above for the FPointSlicePool.
		histograms, err := types.HPointSlicePool.Get(s.originalTimeRange.StepCount, s.memoryConsumptionTracker)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		histograms = append(histograms, promql.HPoint{T: data.Histograms[0].T, H: data.Histograms[0].H})
		// Note that we create a copy of the histogram for each step as we can not re-use the same *FloatHistogram in the slice from the pool.
		for ts := s.originalTimeRange.StartT + s.originalTimeRange.IntervalMilliseconds; ts <= s.originalTimeRange.EndT; ts += s.originalTimeRange.IntervalMilliseconds {
			histograms = append(histograms, promql.HPoint{
				T: ts,
				H: data.Histograms[0].H.Copy(),
			})
		}

		// Ensure that the histogram is not mangled when returned to the pool
		data.Histograms[0].H = nil
		types.HPointSlicePool.Put(&data.Histograms, s.memoryConsumptionTracker)
		data.Histograms = histograms
	}

	return data, err
}
