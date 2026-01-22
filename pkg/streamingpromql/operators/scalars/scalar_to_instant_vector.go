// SPDX-License-Identifier: AGPL-3.0-only

package scalars

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// ScalarToInstantVector is an operator that implements the vector() function.
type ScalarToInstantVector struct {
	Scalar                   types.ScalarOperator
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	expressionPosition posrange.PositionRange
	consumed           bool
}

var _ types.InstantVectorOperator = &ScalarToInstantVector{}

func NewScalarToInstantVector(scalar types.ScalarOperator, expressionPosition posrange.PositionRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *ScalarToInstantVector {
	return &ScalarToInstantVector{
		Scalar:                   scalar,
		expressionPosition:       expressionPosition,
		MemoryConsumptionTracker: memoryConsumptionTracker,
	}
}

func (s *ScalarToInstantVector) SeriesMetadata(_ context.Context, _ types.Matchers) ([]types.SeriesMetadata, error) {
	metadata, err := types.SeriesMetadataSlicePool.Get(1, s.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	metadata, err = types.AppendSeriesMetadata(s.MemoryConsumptionTracker, metadata, types.SeriesMetadata{Labels: labels.EmptyLabels()})
	if err != nil {
		return nil, err
	}

	return metadata, nil
}

func (s *ScalarToInstantVector) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if s.consumed {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	scalarValue, err := s.Scalar.GetValues(ctx)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	s.consumed = true

	return types.InstantVectorSeriesData{
		Floats: scalarValue.Samples,
	}, nil
}

func (s *ScalarToInstantVector) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *ScalarToInstantVector) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return s.Scalar.Prepare(ctx, params)
}

func (s *ScalarToInstantVector) AfterPrepare(ctx context.Context) error {
	return s.Scalar.AfterPrepare(ctx)
}

func (s *ScalarToInstantVector) Finalize(ctx context.Context) error {
	return s.Scalar.Finalize(ctx)
}

func (s *ScalarToInstantVector) Close() {
	s.Scalar.Close()
}
