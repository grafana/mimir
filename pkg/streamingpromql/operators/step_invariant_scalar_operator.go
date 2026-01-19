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

type StepInvariantScalarOperator struct {
	inner                    types.ScalarOperator
	originalTimeRange        types.QueryTimeRange
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

func NewStepInvariantScalarOperator(op types.ScalarOperator, originalTimeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *StepInvariantScalarOperator {
	return &StepInvariantScalarOperator{
		inner:                    op,
		originalTimeRange:        originalTimeRange,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
}

func (s *StepInvariantScalarOperator) ExpressionPosition() posrange.PositionRange {
	return s.inner.ExpressionPosition()
}

func (s *StepInvariantScalarOperator) Close() {
	s.inner.Close()
}

func (s *StepInvariantScalarOperator) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return s.inner.Prepare(ctx, params)
}

func (s *StepInvariantScalarOperator) AfterPrepare(ctx context.Context) error {
	return s.inner.AfterPrepare(ctx)
}

func (s *StepInvariantScalarOperator) Finalize(ctx context.Context) error {
	return s.inner.Finalize(ctx)
}

func (s *StepInvariantScalarOperator) GetValues(ctx context.Context) (types.ScalarData, error) {
	data, err := s.inner.GetValues(ctx)
	if err != nil {
		return types.ScalarData{}, err
	}

	if s.originalTimeRange.IsInstant || s.originalTimeRange.StepCount <= 1 {
		return data, nil
	}

	// The inner query should be pinned to a single point in time with a single step. We don't expect to have multiple values per series here
	if len(data.Samples) > 1 {
		return types.ScalarData{}, fmt.Errorf("expected a single value series. samples=%d", len(data.Samples))
	}

	if len(data.Samples) == 1 {
		floats, err := types.FPointSlicePool.Get(s.originalTimeRange.StepCount, s.memoryConsumptionTracker)
		if err != nil {
			return types.ScalarData{}, err
		}

		floats = append(floats, data.Samples[0])

		for ts := s.originalTimeRange.StartT + s.originalTimeRange.IntervalMilliseconds; ts <= s.originalTimeRange.EndT; ts += s.originalTimeRange.IntervalMilliseconds {
			floats = append(floats, promql.FPoint{
				T: ts,
				F: data.Samples[0].F,
			})
		}

		types.FPointSlicePool.Put(&data.Samples, s.memoryConsumptionTracker)
		data.Samples = floats
	}

	return data, nil
}
