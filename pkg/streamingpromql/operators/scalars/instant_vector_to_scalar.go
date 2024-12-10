// SPDX-License-Identifier: AGPL-3.0-only

package scalars

import (
	"context"
	"math"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// InstantVectorToScalar is an operator that implements the scalar() function.
type InstantVectorToScalar struct {
	Inner                    types.InstantVectorOperator
	TimeRange                types.QueryTimeRange
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker

	expressionPosition posrange.PositionRange
}

var _ types.ScalarOperator = &InstantVectorToScalar{}

func NewInstantVectorToScalar(
	inner types.InstantVectorOperator,
	timeRange types.QueryTimeRange,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
) *InstantVectorToScalar {
	return &InstantVectorToScalar{
		Inner:                    inner,
		TimeRange:                timeRange,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
	}
}

func (i *InstantVectorToScalar) GetValues(ctx context.Context) (types.ScalarData, error) {
	seriesCount, err := i.getInnerSeriesCount(ctx)
	if err != nil {
		return types.ScalarData{}, err
	}

	seenPoint, err := types.BoolSlicePool.Get(i.TimeRange.StepCount, i.MemoryConsumptionTracker)
	if err != nil {
		return types.ScalarData{}, err
	}

	defer types.BoolSlicePool.Put(seenPoint, i.MemoryConsumptionTracker)
	seenPoint = seenPoint[:i.TimeRange.StepCount]

	output, err := types.FPointSlicePool.Get(i.TimeRange.StepCount, i.MemoryConsumptionTracker)
	if err != nil {
		return types.ScalarData{}, err
	}

	for t := i.TimeRange.StartT; t <= i.TimeRange.EndT; t += i.TimeRange.IntervalMilliseconds {
		output = append(output, promql.FPoint{
			T: t,
			F: math.NaN(),
		})
	}

	for seriesIdx := 0; seriesIdx < seriesCount; seriesIdx++ {
		seriesData, err := i.Inner.NextSeries(ctx)
		if err != nil {
			return types.ScalarData{}, err
		}

		for _, p := range seriesData.Floats {
			sampleIdx := (p.T - i.TimeRange.StartT) / i.TimeRange.IntervalMilliseconds

			if seenPoint[sampleIdx] {
				// We've already seen another point at this timestamp, so return NaN at this timestamp.
				output[sampleIdx].F = math.NaN()
			} else {
				output[sampleIdx].F = p.F
				seenPoint[sampleIdx] = true
			}
		}

		types.PutInstantVectorSeriesData(seriesData, i.MemoryConsumptionTracker)
	}

	return types.ScalarData{
		Samples: output,
	}, nil
}

func (i *InstantVectorToScalar) getInnerSeriesCount(ctx context.Context) (int, error) {
	metadata, err := i.Inner.SeriesMetadata(ctx)
	if err != nil {
		return 0, err
	}

	defer types.PutSeriesMetadataSlice(metadata)

	seriesCount := len(metadata)

	return seriesCount, nil
}

func (i *InstantVectorToScalar) ExpressionPosition() posrange.PositionRange {
	return i.expressionPosition
}

func (i *InstantVectorToScalar) Close() {
	i.Inner.Close()
}
