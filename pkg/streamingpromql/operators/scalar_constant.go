// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type ScalarConstant struct {
	Value                    float64
	Start                    int64 // Milliseconds since Unix epoch
	End                      int64 // Milliseconds since Unix epoch
	Interval                 int64 // In milliseconds
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker

	expressionPosition posrange.PositionRange
}

var _ types.ScalarOperator = &ScalarConstant{}

func NewScalarConstant(
	value float64,
	start int64,
	end int64,
	interval int64,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
) *ScalarConstant {
	return &ScalarConstant{
		Value:                    value,
		Start:                    start,
		End:                      end,
		Interval:                 interval,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
	}
}

func (s *ScalarConstant) GetValues(_ context.Context) (types.ScalarData, error) {
	numSteps := stepCount(s.Start, s.End, s.Interval)
	samples, err := types.FPointSlicePool.Get(numSteps, s.MemoryConsumptionTracker)

	if err != nil {
		return types.ScalarData{}, err
	}

	samples = samples[:numSteps]

	for step := 0; step < numSteps; step++ {
		samples[step].T = s.Start + int64(step)*s.Interval
		samples[step].F = s.Value
	}

	return types.ScalarData{Samples: samples}, nil
}

func (s *ScalarConstant) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *ScalarConstant) Close() {
	// Nothing to do.
}
