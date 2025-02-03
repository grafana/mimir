// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type Time struct {
	TimeRange                types.QueryTimeRange
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker
	expressionPosition       posrange.PositionRange
}

var _ types.ScalarOperator = &Time{}

func NewTime(
	timeRange types.QueryTimeRange,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
) *Time {
	return &Time{
		TimeRange:                timeRange,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
	}
}

func (s *Time) GetValues(_ context.Context) (types.ScalarData, error) {
	samples, err := types.FPointSlicePool.Get(s.TimeRange.StepCount, s.MemoryConsumptionTracker)

	if err != nil {
		return types.ScalarData{}, err
	}

	samples = samples[:s.TimeRange.StepCount]

	for step := range s.TimeRange.StepCount {
		t := s.TimeRange.IndexTime(step)
		samples[step].T = t
		samples[step].F = float64(t) / 1000
	}

	return types.ScalarData{Samples: samples}, nil
}

func (s *Time) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *Time) Close() {
	// Nothing to do.
}
