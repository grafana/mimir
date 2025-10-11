// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type NameDrop struct {
	Inner                    types.InstantVectorOperator
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

var _ types.InstantVectorOperator = &NameDrop{}

func NewNameDrop(inner types.InstantVectorOperator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *NameDrop {
	return &NameDrop{Inner: inner, MemoryConsumptionTracker: memoryConsumptionTracker}
}

func (n *NameDrop) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	innerMetadata, err := n.Inner.SeriesMetadata(ctx, matchers)

	if err != nil {
		return nil, err
	}

	// Drop metric names from series that have DropName flag set
	for i := range innerMetadata {
		if !innerMetadata[i].DropName {
			continue
		}
		n.MemoryConsumptionTracker.DecreaseMemoryConsumptionForLabels(innerMetadata[i].Labels)
		innerMetadata[i].Labels = innerMetadata[i].Labels.DropMetricName()
		err := n.MemoryConsumptionTracker.IncreaseMemoryConsumptionForLabels(innerMetadata[i].Labels)
		if err != nil {
			return nil, err
		}
		innerMetadata[i].DropName = false
	}

	return innerMetadata, nil
}

func (n *NameDrop) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	return n.Inner.NextSeries(ctx)
}

func (n *NameDrop) ExpressionPosition() posrange.PositionRange {
	return n.Inner.ExpressionPosition()
}

func (n *NameDrop) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return n.Inner.Prepare(ctx, params)
}

func (n *NameDrop) Finalize(ctx context.Context) error {
	return n.Inner.Finalize(ctx)
}

func (n *NameDrop) Close() {
	n.Inner.Close()
}
