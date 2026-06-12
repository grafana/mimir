// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"fmt"
	"math"

	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type ScalarMinMax struct {
	cmp func(float64, float64) float64
	a   types.ScalarOperator
	b   types.ScalarOperator

	timeRange                types.QueryTimeRange
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	expressionPosition       posrange.PositionRange
}

var _ types.ScalarOperator = &ScalarMinMax{}

func NewMaxOf(
	a, b types.ScalarOperator,
	timeRange types.QueryTimeRange,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
) *ScalarMinMax {
	return &ScalarMinMax{
		cmp:                      math.Max,
		a:                        a,
		b:                        b,
		timeRange:                timeRange,
		memoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
	}
}

func NewMinOf(
	a, b types.ScalarOperator,
	timeRange types.QueryTimeRange,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
) *ScalarMinMax {
	return &ScalarMinMax{
		cmp:                      math.Min,
		a:                        a,
		b:                        b,
		timeRange:                timeRange,
		memoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
	}
}

func (m *ScalarMinMax) ExpressionPosition() posrange.PositionRange {
	return m.expressionPosition
}

func (m *ScalarMinMax) Close() {
	m.a.Close()
	m.b.Close()
}

func (m *ScalarMinMax) Prepare(ctx context.Context, params *types.PrepareParams) error {
	err := m.a.Prepare(ctx, params)
	if err != nil {
		return err
	}

	return m.b.Prepare(ctx, params)
}

func (m *ScalarMinMax) AfterPrepare(ctx context.Context) error {
	err := m.a.AfterPrepare(ctx)
	if err != nil {
		return err
	}

	return m.b.AfterPrepare(ctx)
}

func (m *ScalarMinMax) FinishedReading(ctx context.Context) error {
	err := m.a.FinishedReading(ctx)
	if err != nil {
		return err
	}

	return m.b.FinishedReading(ctx)
}

func (m *ScalarMinMax) Finalize(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	return types.FinalizeAndCombine(ctx, m.a, m.b)
}

func (m *ScalarMinMax) GetValues(ctx context.Context) (types.ScalarData, error) {
	val1, err := m.a.GetValues(ctx)
	if err != nil {
		return types.ScalarData{}, err
	}

	defer types.FPointSlicePool.Put(&val1.Samples, m.memoryConsumptionTracker)

	val2, err := m.b.GetValues(ctx)
	if err != nil {
		return types.ScalarData{}, err
	}

	// Note that we aren't cleaning up val2 since we use it for our return value.

	if len(val1.Samples) != m.timeRange.StepCount {
		return types.ScalarData{}, fmt.Errorf("unexpected number of samples in first argument. expected %d, got %d", m.timeRange.StepCount, len(val1.Samples))
	}

	if len(val2.Samples) != m.timeRange.StepCount {
		return types.ScalarData{}, fmt.Errorf("unexpected number of samples in second argument. expected %d, got %d", m.timeRange.StepCount, len(val2.Samples))
	}

	for step := 0; step < m.timeRange.StepCount; step++ {
		val2.Samples[step].F = m.cmp(val1.Samples[step].F, val2.Samples[step].F)
	}

	return val2, nil
}
