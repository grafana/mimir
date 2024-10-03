// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// AndBinaryOperation represents a logical 'and' between two vectors.
type AndBinaryOperation struct {
	Left                     types.InstantVectorOperator
	Right                    types.InstantVectorOperator
	VectorMatching           parser.VectorMatching
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker

	expressionPosition posrange.PositionRange
}

var _ types.InstantVectorOperator = &AndBinaryOperation{}

func NewAndBinaryOperation(
	left types.InstantVectorOperator,
	right types.InstantVectorOperator,
	vectorMatching parser.VectorMatching,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
) *AndBinaryOperation {
	return &AndBinaryOperation{
		Left:                     left,
		Right:                    right,
		VectorMatching:           vectorMatching,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
	}
}

func (a *AndBinaryOperation) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	panic("implement me")
}

func (a *AndBinaryOperation) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	panic("implement me")
}

func (a *AndBinaryOperation) ExpressionPosition() posrange.PositionRange {
	return a.expressionPosition
}

func (a *AndBinaryOperation) Close() {
	a.Left.Close()
	a.Right.Close()
}
