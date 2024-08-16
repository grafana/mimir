// SPDX-License-Identifier: AGPL-3.0-only

package operators

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
	Start                    int64 // Milliseconds since Unix epoch
	End                      int64 // Milliseconds since Unix epoch
	Interval                 int64 // In milliseconds
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker

	expressionPosition posrange.PositionRange
}

var _ types.ScalarOperator = &InstantVectorToScalar{}

func NewInstantVectorToScalar(
	inner types.InstantVectorOperator,
	start int64,
	end int64,
	interval int64,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
) *InstantVectorToScalar {
	return &InstantVectorToScalar{
		Inner:                    inner,
		Start:                    start,
		End:                      end,
		Interval:                 interval,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
	}
}

func (i *InstantVectorToScalar) GetValues(ctx context.Context) (types.ScalarData, error) {
	seriesCount, err := i.getInnerSeriesCount(ctx)
	if err != nil {
		return types.ScalarData{}, err
	}

	stepCount := stepCount(i.Start, i.End, i.Interval)
	seenPoint, err := types.BoolSlicePool.Get(stepCount, i.MemoryConsumptionTracker)
	if err != nil {
		return types.ScalarData{}, err
	}

	defer types.BoolSlicePool.Put(seenPoint, i.MemoryConsumptionTracker)
	seenPoint = seenPoint[:stepCount]

	output, err := types.FPointSlicePool.Get(stepCount, i.MemoryConsumptionTracker)
	if err != nil {
		return types.ScalarData{}, err
	}

	for t := i.Start; t <= i.End; t += i.Interval {
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
			sampleIdx := (p.T - i.Start) / i.Interval

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
	// Nothing to do.
}
