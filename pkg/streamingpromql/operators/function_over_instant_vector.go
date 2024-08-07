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
	Inner                    types.InstantVectorOperator
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker

	SeriesMetadataFunc functions.SeriesMetadataFunction
	SeriesDataFunc     functions.InstantVectorFunction

	expressionPosition posrange.PositionRange
}

var _ types.InstantVectorOperator = &FunctionOverInstantVector{}

func NewFunctionOverInstantVector(
	inner types.InstantVectorOperator,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	metadataFunc functions.SeriesMetadataFunction,
	seriesDataFunc functions.InstantVectorFunction,
	expressionPosition posrange.PositionRange,
) *FunctionOverInstantVector {
	return &FunctionOverInstantVector{
		Inner:                    inner,
		MemoryConsumptionTracker: memoryConsumptionTracker,

		SeriesMetadataFunc: metadataFunc,
		SeriesDataFunc:     seriesDataFunc,

		expressionPosition: expressionPosition,
	}
}

func (m *FunctionOverInstantVector) ExpressionPosition() posrange.PositionRange {
	return m.expressionPosition
}

func (m *FunctionOverInstantVector) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	metadata, err := m.Inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	return m.SeriesMetadataFunc(metadata, m.MemoryConsumptionTracker)
}

func (m *FunctionOverInstantVector) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	series, err := m.Inner.NextSeries(ctx)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	return m.SeriesDataFunc(series, m.MemoryConsumptionTracker)
}

func (m *FunctionOverInstantVector) Close() {
	m.Inner.Close()
}
