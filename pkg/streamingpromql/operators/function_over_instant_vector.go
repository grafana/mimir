// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package operators

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// FunctionOverInstantVector performs a function over each series in an instant vector.
type FunctionOverInstantVector struct {
	// At the moment no instant-vector promql function takes more than one instant-vector
	// as an argument. We can assume this will always be the Inner operator and therefore
	// what we use for the SeriesMetadata.
	Inner types.InstantVectorOperator
	// Any scalar arguments will be read once and passed to Func.SeriesDataFunc.
	ScalarArgs               []types.ScalarOperator
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker
	Func                     functions.FunctionOverInstantVector

	// scalarArgsData stores the processed ScalarArgs during SeriesMetadata.
	// The order of scalarArgsData matches the order of ScalarArgs.
	// These are returned the pool at Close().
	scalarArgsData []types.ScalarData

	expressionPosition posrange.PositionRange
}

var _ types.InstantVectorOperator = &FunctionOverInstantVector{}

func NewFunctionOverInstantVector(
	inner types.InstantVectorOperator,
	scalarArgs []types.ScalarOperator,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	f functions.FunctionOverInstantVector,
	expressionPosition posrange.PositionRange,
) *FunctionOverInstantVector {
	return &FunctionOverInstantVector{
		Inner:                    inner,
		ScalarArgs:               scalarArgs,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		Func:                     f,

		expressionPosition: expressionPosition,
	}
}

func (m *FunctionOverInstantVector) ExpressionPosition() posrange.PositionRange {
	return m.expressionPosition
}

func (m *FunctionOverInstantVector) processScalarArgs(ctx context.Context) error {
	if len(m.ScalarArgs) == 0 {
		return nil
	}

	m.scalarArgsData = make([]types.ScalarData, 0, len(m.ScalarArgs))
	for _, so := range m.ScalarArgs {
		sd, err := so.GetValues(ctx)
		if err != nil {
			return err
		}
		m.scalarArgsData = append(m.scalarArgsData, sd)
	}

	return nil
}

func (m *FunctionOverInstantVector) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	// Pre-process any Scalar arguments
	err := m.processScalarArgs(ctx)
	if err != nil {
		return nil, err
	}

	metadata, err := m.Inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	if m.Func.SeriesMetadataFunction.Func != nil {
		return m.Func.SeriesMetadataFunction.Func(metadata, m.MemoryConsumptionTracker)
	}

	return metadata, nil
}

func (m *FunctionOverInstantVector) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	series, err := m.Inner.NextSeries(ctx)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	return m.Func.SeriesDataFunc(series, m.scalarArgsData, m.MemoryConsumptionTracker)
}

func (m *FunctionOverInstantVector) Close() {
	m.Inner.Close()
	for _, sd := range m.scalarArgsData {
		types.FPointSlicePool.Put(sd.Samples, m.MemoryConsumptionTracker)
	}
}
