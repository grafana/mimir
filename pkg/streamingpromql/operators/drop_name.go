// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type DropName struct {
	Inner                    types.InstantVectorOperator
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

var _ types.InstantVectorOperator = &DropName{}

func NewDropName(inner types.InstantVectorOperator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *DropName {
	return &DropName{Inner: inner, MemoryConsumptionTracker: memoryConsumptionTracker}
}

func (n *DropName) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
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
		//nolint:staticcheck // SA1019: DropMetricName is deprecated.
		innerMetadata[i].Labels = innerMetadata[i].Labels.DropMetricName()
		err := n.MemoryConsumptionTracker.IncreaseMemoryConsumptionForLabels(innerMetadata[i].Labels)
		if err != nil {
			return nil, err
		}
		innerMetadata[i].DropName = false
	}

	return innerMetadata, nil
}

func (n *DropName) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	return n.Inner.NextSeries(ctx)
}

func (n *DropName) ExpressionPosition() posrange.PositionRange {
	return n.Inner.ExpressionPosition()
}

func (n *DropName) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return n.Inner.Prepare(ctx, params)
}

func (n *DropName) AfterPrepare(ctx context.Context) error {
	return n.Inner.AfterPrepare(ctx)
}

func (n *DropName) Finalize(ctx context.Context) error {
	return n.Inner.Finalize(ctx)
}

func (n *DropName) Close() {
	n.Inner.Close()
}
