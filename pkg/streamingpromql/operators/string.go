// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type StringLiteral struct {
	Value string

	timeRange                types.QueryTimeRange
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	expressionPosition       posrange.PositionRange
}

var _ types.StringOperator = &StringLiteral{}

func NewStringLiteral(
	value string,
	timeRange types.QueryTimeRange,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
) *StringLiteral {
	return &StringLiteral{
		Value:                    value,
		timeRange:                timeRange,
		memoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
	}
}

func (s *StringLiteral) GetValue() string {
	return s.Value
}

func (s *StringLiteral) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *StringLiteral) Prepare(_ context.Context, _ *types.PrepareParams) error {
	// Nothing to do.
	return nil
}

func (s *StringLiteral) AfterPrepare(_ context.Context) error {
	return nil
}

func (s *StringLiteral) Finalize(_ context.Context) error {
	// Nothing to do.
	return nil
}

func (s *StringLiteral) Stats(_ context.Context) (*types.OperatorEvaluationStats, error) {
	return types.NewOperatorEvaluationStats(s.timeRange, s.memoryConsumptionTracker)
}

func (s *StringLiteral) Close() {
	// Nothing to do.
}
