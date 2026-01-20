// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package functions

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// FunctionOverInstantVector performs a function over each series in an instant vector.
type FunctionOverInstantVector struct {
	// At the moment no instant-vector promql function takes more than one instant-vector
	// as an argument. We can assume this will always be the Inner operator and therefore
	// what we use for the SeriesMetadata.
	Inner types.InstantVectorOperator
	// Any scalar arguments will be read once and passed to Func.SeriesDataFunc.
	ScalarArgs               []types.ScalarOperator
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
	Func                     FunctionOverInstantVectorDefinition

	// scalarArgsData stores the processed ScalarArgs during SeriesMetadata.
	// The order of scalarArgsData matches the order of ScalarArgs.
	// These are returned the pool at Close().
	scalarArgsData []types.ScalarData

	expressionPosition       posrange.PositionRange
	timeRange                types.QueryTimeRange
	enableDelayedNameRemoval bool
}

var _ types.InstantVectorOperator = &FunctionOverInstantVector{}

func NewFunctionOverInstantVector(
	inner types.InstantVectorOperator,
	scalarArgs []types.ScalarOperator,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	f FunctionOverInstantVectorDefinition,
	expressionPosition posrange.PositionRange,
	timeRange types.QueryTimeRange,
	enableDelayedNameRemoval bool,
) *FunctionOverInstantVector {
	return &FunctionOverInstantVector{
		Inner:                    inner,
		ScalarArgs:               scalarArgs,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		Func:                     f,

		expressionPosition:       expressionPosition,
		timeRange:                timeRange,
		enableDelayedNameRemoval: enableDelayedNameRemoval,
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

func (m *FunctionOverInstantVector) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	// Pre-process any Scalar arguments
	err := m.processScalarArgs(ctx)
	if err != nil {
		return nil, err
	}

	metadata, err := m.Inner.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	if m.Func.SeriesMetadataFunction.Func != nil {
		return m.Func.SeriesMetadataFunction.Func(metadata, m.MemoryConsumptionTracker, m.enableDelayedNameRemoval)
	}

	return metadata, nil
}

func (m *FunctionOverInstantVector) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	series, err := m.Inner.NextSeries(ctx)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	return m.Func.SeriesDataFunc(series, m.scalarArgsData, m.timeRange, m.MemoryConsumptionTracker)
}

func (m *FunctionOverInstantVector) Prepare(ctx context.Context, params *types.PrepareParams) error {
	if err := m.Inner.Prepare(ctx, params); err != nil {
		return err
	}
	for _, sa := range m.ScalarArgs {
		if err := sa.Prepare(ctx, params); err != nil {
			return err
		}
	}
	return nil
}

func (m *FunctionOverInstantVector) AfterPrepare(ctx context.Context) error {
	if err := m.Inner.AfterPrepare(ctx); err != nil {
		return err
	}
	for _, sa := range m.ScalarArgs {
		if err := sa.AfterPrepare(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (m *FunctionOverInstantVector) Finalize(ctx context.Context) error {
	if err := m.Inner.Finalize(ctx); err != nil {
		return err
	}

	for _, sa := range m.ScalarArgs {
		if err := sa.Finalize(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (m *FunctionOverInstantVector) Close() {
	m.Inner.Close()

	for _, sd := range m.scalarArgsData {
		types.FPointSlicePool.Put(&sd.Samples, m.MemoryConsumptionTracker)
	}

	m.scalarArgsData = nil
}
