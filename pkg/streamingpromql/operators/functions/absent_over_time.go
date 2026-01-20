// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package functions

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// AbsentOverTime is an operator that implements the absent_over_time() function.
type AbsentOverTime struct {
	TimeRange                types.QueryTimeRange
	Labels                   labels.Labels
	Inner                    types.RangeVectorOperator
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	expressionPosition posrange.PositionRange
	presence           []bool
	exhausted          bool
}

var _ types.InstantVectorOperator = &AbsentOverTime{}

func NewAbsentOverTime(
	inner types.RangeVectorOperator,
	labels labels.Labels,
	timeRange types.QueryTimeRange,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
) *AbsentOverTime {
	return &AbsentOverTime{
		TimeRange:                timeRange,
		Inner:                    inner,
		Labels:                   labels,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
	}
}

func (a *AbsentOverTime) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	innerMetadata, err := a.Inner.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}
	defer types.SeriesMetadataSlicePool.Put(&innerMetadata, a.MemoryConsumptionTracker)

	a.presence, err = types.BoolSlicePool.Get(a.TimeRange.StepCount, a.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	// Initialize presence slice
	a.presence = a.presence[:a.TimeRange.StepCount]

	metadata, err := types.SeriesMetadataSlicePool.Get(1, a.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	metadata, err = types.AppendSeriesMetadata(a.MemoryConsumptionTracker, metadata, types.SeriesMetadata{Labels: a.Labels})
	if err != nil {
		return nil, err
	}

	for range innerMetadata {
		err := a.Inner.NextSeries(ctx)
		if err != nil {
			return nil, err
		}
		for stepIdx := range a.TimeRange.StepCount {
			step, err := a.Inner.NextStepSamples(ctx)
			if err != nil {
				return nil, err
			}
			if step.Floats.Any() || step.Histograms.Any() {
				a.presence[stepIdx] = true
			}
		}
	}
	return metadata, nil
}

func (a *AbsentOverTime) NextSeries(_ context.Context) (types.InstantVectorSeriesData, error) {
	output := types.InstantVectorSeriesData{}
	if a.exhausted {
		return output, types.EOS
	}
	a.exhausted = true

	var err error
	for step := range a.TimeRange.StepCount {
		if a.presence[step] {
			continue
		}

		if output.Floats == nil {
			output.Floats, err = types.FPointSlicePool.Get(a.TimeRange.StepCount, a.MemoryConsumptionTracker)
			if err != nil {
				return output, err
			}
		}

		t := a.TimeRange.IndexTime(int64(step))
		output.Floats = append(output.Floats, promql.FPoint{T: t, F: 1})
	}
	return output, nil
}

func (a *AbsentOverTime) ExpressionPosition() posrange.PositionRange {
	return a.expressionPosition
}

func (a *AbsentOverTime) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return a.Inner.Prepare(ctx, params)
}

func (a *AbsentOverTime) AfterPrepare(ctx context.Context) error {
	return a.Inner.AfterPrepare(ctx)
}

func (a *AbsentOverTime) Finalize(ctx context.Context) error {
	return a.Inner.Finalize(ctx)
}

func (a *AbsentOverTime) Close() {
	a.Inner.Close()

	types.BoolSlicePool.Put(&a.presence, a.MemoryConsumptionTracker)
}
