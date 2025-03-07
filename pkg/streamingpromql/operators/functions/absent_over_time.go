// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package functions

import (
	"context"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// AbsentOverTime performs a rate calculation over a range vector.
type AbsentOverTime struct {
	inner                    types.RangeVectorOperator
	ScalarArgs               []types.ScalarOperator
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker

	rangeSeconds float64

	expressionPosition posrange.PositionRange
	timeRange          types.QueryTimeRange
	presence           []bool
	argExpressions     parser.Expr
}

var _ types.InstantVectorOperator = &AbsentOverTime{}

func NewAbsentOverTime(
	inner types.RangeVectorOperator,
	argExpressions parser.Expr,
	timeRange types.QueryTimeRange,
	expressionPosition posrange.PositionRange,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
) *AbsentOverTime {
	return &AbsentOverTime{
		inner:                    inner,
		argExpressions:           argExpressions,
		timeRange:                timeRange,
		expressionPosition:       expressionPosition,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
}

func (a *AbsentOverTime) ExpressionPosition() posrange.PositionRange {
	return a.expressionPosition
}

func (a *AbsentOverTime) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	innerMetadata, err := a.inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}
	defer types.PutSeriesMetadataSlice(innerMetadata)

	a.rangeSeconds = a.inner.Range().Seconds()

	a.presence, err = types.BoolSlicePool.Get(a.timeRange.StepCount, a.memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	// Initialize presence slice
	a.presence = a.presence[:a.timeRange.StepCount]

	metadata := types.GetSeriesMetadataSlice(1)
	metadata = append(metadata, types.SeriesMetadata{
		Labels: createLabelsForAbsentFunction(a.argExpressions),
	})

	for range innerMetadata {
		err := a.inner.NextSeries(ctx)
		if err != nil {
			return nil, err
		}
		step, err := a.inner.NextStepSamples()
		if err != nil {
			return nil, err
		}
		if step.Floats.Any() || step.Histograms.Any() {
			a.presence[a.timeRange.PointIndex(step.StepT)] = true
		}

	}

	return metadata, nil
}

func (a *AbsentOverTime) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	output := types.InstantVectorSeriesData{}

	var err error
	for step := range a.timeRange.StepCount {
		t := a.timeRange.IndexTime(int64(step))
		if a.presence[step] {
			continue
		}

		if output.Floats == nil {
			output.Floats, err = types.FPointSlicePool.Get(a.timeRange.StepCount, a.memoryConsumptionTracker)
			if err != nil {
				return output, err
			}
		}
		output.Floats = append(output.Floats, promql.FPoint{T: t, F: 1})
	}
	return output, nil

}

func (a *AbsentOverTime) Close() {
	a.inner.Close()
	types.BoolSlicePool.Put(a.presence, a.memoryConsumptionTracker)
}
