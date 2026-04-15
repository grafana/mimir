// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// NoOp is an instant vector operator that always returns an empty result.
// It is produced by optimization passes that detect subexpressions which can
// statically be determined to return no results.
type NoOp struct {
	timeRange                types.QueryTimeRange
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

var _ types.InstantVectorOperator = &NoOp{}

func NewNoOp(timeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *NoOp {
	return &NoOp{
		timeRange:                timeRange,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
}

func (n *NoOp) ExpressionPosition() posrange.PositionRange {
	return posrange.PositionRange{}
}

func (n *NoOp) Prepare(_ context.Context, _ *types.PrepareParams) error {
	return nil
}

func (n *NoOp) AfterPrepare(_ context.Context) error {
	return nil
}

func (n *NoOp) SeriesMetadata(_ context.Context, _ types.Matchers) ([]types.SeriesMetadata, error) {
	return nil, nil
}

func (n *NoOp) NextSeries(_ context.Context) (types.InstantVectorSeriesData, error) {
	return types.InstantVectorSeriesData{}, types.EOS
}

func (n *NoOp) Finalize(_ context.Context) error {
	return nil
}

func (n *NoOp) Stats(_ context.Context) (*types.OperatorEvaluationStats, error) {
	return types.NewOperatorEvaluationStats(n.timeRange, n.memoryConsumptionTracker, 0)
}

func (n *NoOp) Close() {
}
