// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// NoOpInstant is an instant vector operator that always returns an empty result.
// It is produced by optimization passes that detect subexpressions which can
// statically be determined to return no results.
type NoOpInstant struct {
	timeRange                types.QueryTimeRange
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

var _ types.InstantVectorOperator = &NoOpInstant{}

func NewNoOpInstant(timeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *NoOpInstant {
	return &NoOpInstant{
		timeRange:                timeRange,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
}

func (n *NoOpInstant) ExpressionPosition() posrange.PositionRange {
	return posrange.PositionRange{}
}

func (n *NoOpInstant) Prepare(_ context.Context, _ *types.PrepareParams) error {
	return nil
}

func (n *NoOpInstant) AfterPrepare(_ context.Context) error {
	return nil
}

func (n *NoOpInstant) SeriesMetadata(_ context.Context, _ types.Matchers) ([]types.SeriesMetadata, error) {
	return nil, nil
}

func (n *NoOpInstant) NextSeries(_ context.Context) (types.InstantVectorSeriesData, error) {
	return types.InstantVectorSeriesData{}, types.EOS
}

func (n *NoOpInstant) Finalize(_ context.Context) error {
	return nil
}

func (n *NoOpInstant) Stats(ctx context.Context) (*types.OperatorEvaluationStats, error) {
	return types.NewOperatorEvaluationStats(ctx, n.timeRange, n.memoryConsumptionTracker, 0)
}

func (n *NoOpInstant) Close() {
}

// NoOpRange is an range vector operator that always returns an empty result.
// It is produced by optimization passes that detect subexpressions which can
// statically be determined to return no results.
type NoOpRange struct {
	timeRange                types.QueryTimeRange
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

var _ types.RangeVectorOperator = &NoOpRange{}

func NewNoOpRange(timeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *NoOpRange {
	return &NoOpRange{
		timeRange:                timeRange,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
}

func (n *NoOpRange) ExpressionPosition() posrange.PositionRange {
	return posrange.PositionRange{}
}

func (n *NoOpRange) Prepare(_ context.Context, _ *types.PrepareParams) error {
	return nil
}

func (n *NoOpRange) AfterPrepare(_ context.Context) error {
	return nil
}

func (n *NoOpRange) Finalize(_ context.Context) error {
	return nil
}

func (n *NoOpRange) Stats(ctx context.Context) (*types.OperatorEvaluationStats, error) {
	return types.NewOperatorEvaluationStats(ctx, n.timeRange, n.memoryConsumptionTracker, 0)
}

func (n *NoOpRange) SeriesMetadata(_ context.Context, _ types.Matchers) ([]types.SeriesMetadata, error) {
	return nil, nil
}

func (n *NoOpRange) NextSeries(_ context.Context) error {
	return types.EOS
}

func (n *NoOpRange) NextStepSamples(_ context.Context) (*types.RangeVectorStepData, error) {
	return nil, types.EOS
}

func (n *NoOpRange) Close() {
}
