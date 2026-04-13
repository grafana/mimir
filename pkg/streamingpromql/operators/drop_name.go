// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type DropNameInstant struct {
	Inner                    types.InstantVectorOperator
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

var _ types.InstantVectorOperator = &DropNameInstant{}

func NewDropNameInstant(inner types.InstantVectorOperator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *DropNameInstant {
	return &DropNameInstant{Inner: inner, MemoryConsumptionTracker: memoryConsumptionTracker}
}

func (n *DropNameInstant) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	innerMetadata, err := n.Inner.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	if err = dropNames(innerMetadata, n.MemoryConsumptionTracker); err != nil {
		return nil, err
	}

	return innerMetadata, nil
}

func (n *DropNameInstant) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	return n.Inner.NextSeries(ctx)
}

func (n *DropNameInstant) ExpressionPosition() posrange.PositionRange {
	return n.Inner.ExpressionPosition()
}

func (n *DropNameInstant) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return n.Inner.Prepare(ctx, params)
}

func (n *DropNameInstant) AfterPrepare(ctx context.Context) error {
	return n.Inner.AfterPrepare(ctx)
}

func (n *DropNameInstant) Finalize(ctx context.Context) error {
	return n.Inner.Finalize(ctx)
}

func (n *DropNameInstant) Stats(ctx context.Context) (*types.OperatorEvaluationStats, error) {
	return n.Inner.Stats(ctx)
}

func (n *DropNameInstant) Close() {
	n.Inner.Close()
}

type DropNameRange struct {
	Inner                    types.RangeVectorOperator
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

var _ types.RangeVectorOperator = &DropNameRange{}

func NewDropNameRange(inner types.RangeVectorOperator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *DropNameRange {
	return &DropNameRange{Inner: inner, MemoryConsumptionTracker: memoryConsumptionTracker}
}

func (n *DropNameRange) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	innerMetadata, err := n.Inner.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	if err = dropNames(innerMetadata, n.MemoryConsumptionTracker); err != nil {
		return nil, err
	}

	return innerMetadata, nil
}

func (n *DropNameRange) NextSeries(ctx context.Context) error {
	return n.Inner.NextSeries(ctx)
}

func (n *DropNameRange) NextStepSamples(ctx context.Context) (*types.RangeVectorStepData, error) {
	return n.Inner.NextStepSamples(ctx)
}

func (n *DropNameRange) ExpressionPosition() posrange.PositionRange {
	return n.Inner.ExpressionPosition()
}

func (n *DropNameRange) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return n.Inner.Prepare(ctx, params)
}

func (n *DropNameRange) AfterPrepare(ctx context.Context) error {
	return n.Inner.AfterPrepare(ctx)
}

func (n *DropNameRange) Finalize(ctx context.Context) error {
	return n.Inner.Finalize(ctx)
}

func (n *DropNameRange) Stats(ctx context.Context) (*types.OperatorEvaluationStats, error) {
	return n.Inner.Stats(ctx)
}

func (n *DropNameRange) Close() {
	n.Inner.Close()
}

func dropNames(metadata []types.SeriesMetadata, mem *limiter.MemoryConsumptionTracker) error {
	for i := range metadata {
		if !metadata[i].DropName {
			continue
		}
		mem.DecreaseMemoryConsumptionForLabels(metadata[i].Labels)
		metadata[i].Labels = metadata[i].Labels.DropReserved(func(name string) bool {
			return name == model.MetricNameLabel
		})
		err := mem.IncreaseMemoryConsumptionForLabels(metadata[i].Labels)
		if err != nil {
			return err
		}
		metadata[i].DropName = false
	}

	return nil
}
