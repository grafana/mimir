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

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// AbsentOverTime is an operator that implements the absent_over_time() function.
type AbsentOverTime struct {
	TimeRange                types.QueryTimeRange
	Labels                   labels.Labels
	Inner                    types.RangeVectorOperator
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker

	expressionPosition posrange.PositionRange
	presence           []bool
	exhausted          bool
}

var _ types.InstantVectorOperator = &AbsentOverTime{}

func NewAbsentOverTime(
	inner types.RangeVectorOperator,
	labels labels.Labels,
	timeRange types.QueryTimeRange,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
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

func (a *AbsentOverTime) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	innerMetadata, err := a.Inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}
	defer types.PutSeriesMetadataSlice(innerMetadata)

	a.presence, err = types.BoolSlicePool.Get(a.TimeRange.StepCount, a.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	// Initialize presence slice
	a.presence = a.presence[:a.TimeRange.StepCount]

	metadata := types.GetSeriesMetadataSlice(1)
	metadata = append(metadata, types.SeriesMetadata{
		Labels: a.Labels,
	})

	for range innerMetadata {
		err := a.Inner.NextSeries(ctx)
		if err != nil {
			return nil, err
		}
		for stepIdx := range a.TimeRange.StepCount {
			step, err := a.Inner.NextStepSamples()
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

func (a *AbsentOverTime) Close() {
	a.Inner.Close()
	types.BoolSlicePool.Put(a.presence, a.MemoryConsumptionTracker)
}
