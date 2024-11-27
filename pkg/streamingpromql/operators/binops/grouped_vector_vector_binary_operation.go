// SPDX-License-Identifier: AGPL-3.0-only

package binops

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// GroupedVectorVectorBinaryOperation represents a one-to-many or many-to-one binary operation between instant vectors such as "<expr> + group_left <expr>" or "<expr> - group_right <expr>".
// One-to-one binary operations between instant vectors are not supported.
type GroupedVectorVectorBinaryOperation struct {
	Left                     types.InstantVectorOperator
	Right                    types.InstantVectorOperator
	Op                       parser.ItemType
	ReturnBool               bool
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker

	VectorMatching parser.VectorMatching

	// We need to retain these so that NextSeries() can return an error message with the series labels when
	// multiple points match on a single side.
	// Note that we don't retain the output series metadata: if we need to return an error message, we can compute
	// the output series labels from these again.
	leftMetadata  []types.SeriesMetadata
	rightMetadata []types.SeriesMetadata

	opFunc binaryOperationFunc

	expressionPosition posrange.PositionRange
	annotations        *annotations.Annotations
}

var _ types.InstantVectorOperator = &GroupedVectorVectorBinaryOperation{}

func NewGroupedVectorVectorBinaryOperation(
	left types.InstantVectorOperator,
	right types.InstantVectorOperator,
	vectorMatching parser.VectorMatching,
	op parser.ItemType,
	returnBool bool,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
) (*GroupedVectorVectorBinaryOperation, error) {
	b := &GroupedVectorVectorBinaryOperation{
		Left:                     left,
		Right:                    right,
		VectorMatching:           vectorMatching,
		Op:                       op,
		ReturnBool:               returnBool,
		MemoryConsumptionTracker: memoryConsumptionTracker,

		expressionPosition: expressionPosition,
		annotations:        annotations,
	}

	if returnBool {
		b.opFunc = boolComparisonOperationFuncs[op]
	} else {
		b.opFunc = arithmeticAndComparisonOperationFuncs[op]
	}

	if b.opFunc == nil {
		return nil, compat.NewNotSupportedError(fmt.Sprintf("binary expression with '%s'", op))
	}

	return b, nil
}

func (g *GroupedVectorVectorBinaryOperation) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	return nil, nil
}

func (g *GroupedVectorVectorBinaryOperation) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	return types.InstantVectorSeriesData{}, nil
}

func (g *GroupedVectorVectorBinaryOperation) ExpressionPosition() posrange.PositionRange {
	return g.expressionPosition
}

func (g *GroupedVectorVectorBinaryOperation) Close() {
	g.Left.Close()
	g.Right.Close()

	if g.leftMetadata != nil {
		types.PutSeriesMetadataSlice(g.leftMetadata)
	}

	if g.rightMetadata != nil {
		types.PutSeriesMetadataSlice(g.rightMetadata)
	}

}
