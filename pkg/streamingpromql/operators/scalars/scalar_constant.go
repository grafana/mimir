// SPDX-License-Identifier: AGPL-3.0-only

package scalars

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type ScalarConstant struct {
	Value                    float64
	TimeRange                types.QueryTimeRange
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	expressionPosition posrange.PositionRange
}

var _ types.ScalarOperator = &ScalarConstant{}

func NewScalarConstant(
	value float64,
	timeRange types.QueryTimeRange,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
) *ScalarConstant {
	return &ScalarConstant{
		Value:                    value,
		TimeRange:                timeRange,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
	}
}

func (s *ScalarConstant) GetValues(_ context.Context) (types.ScalarData, error) {
	samples, err := types.FPointSlicePool.Get(s.TimeRange.StepCount, s.MemoryConsumptionTracker)

	if err != nil {
		return types.ScalarData{}, err
	}

	samples = samples[:s.TimeRange.StepCount]

	for step := 0; step < s.TimeRange.StepCount; step++ {
		samples[step].T = s.TimeRange.StartT + int64(step)*s.TimeRange.IntervalMilliseconds
		samples[step].F = s.Value
	}

	return types.ScalarData{Samples: samples}, nil
}

func (s *ScalarConstant) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *ScalarConstant) Prepare(_ context.Context, _ *types.PrepareParams) error {
	// Nothing to do.
	return nil
}

func (s *ScalarConstant) AfterPrepare(_ context.Context) error {
	return nil
}

func (s *ScalarConstant) Finalize(_ context.Context) error {
	// Nothing to do.
	return nil
}

func (s *ScalarConstant) Close() {
	// Nothing to do.
}
