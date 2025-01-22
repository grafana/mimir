// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package functions

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type Time struct {
	TimeRange                types.QueryTimeRange
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker
	expressionPosition       posrange.PositionRange
}

var _ types.ScalarOperator = &Time{}

func NewTime(
	timeRange types.QueryTimeRange,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
) *Time {
	return &Time{
		TimeRange:                timeRange,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
	}
}

func (s *Time) GetValues(_ context.Context) (types.ScalarData, error) {
	samples, err := types.FPointSlicePool.Get(s.TimeRange.StepCount, s.MemoryConsumptionTracker)

	if err != nil {
		return types.ScalarData{}, err
	}

	samples = samples[:s.TimeRange.StepCount]

	for step := 0; step < s.TimeRange.StepCount; step++ {
		t := s.TimeRange.StartT + int64(step)*s.TimeRange.IntervalMilliseconds
		samples[step].T = t
		samples[step].F = float64(t) / 1000
	}

	return types.ScalarData{Samples: samples}, nil
}

func (s *Time) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *Time) Close() {
	// Nothing to do.
}

var DaysInMonth = timeWrapperFunc(func(t time.Time) float64 {
	return float64(32 - time.Date(t.Year(), t.Month(), 32, 0, 0, 0, 0, time.UTC).Day())
})

var DayOfMonth = timeWrapperFunc(func(t time.Time) float64 {
	return float64(t.Day())
})

var DayOfWeek = timeWrapperFunc(func(t time.Time) float64 {
	return float64(t.Weekday())
})

var DayOfYear = timeWrapperFunc(func(t time.Time) float64 {
	return float64(t.YearDay())
})

var Hour = timeWrapperFunc(func(t time.Time) float64 {
	return float64(t.Hour())
})

var Minute = timeWrapperFunc(func(t time.Time) float64 {
	return float64(t.Minute())
})

var Month = timeWrapperFunc(func(t time.Time) float64 {
	return float64(t.Month())
})

var Year = timeWrapperFunc(func(t time.Time) float64 {
	return float64(t.Year())
})

func timeWrapperFunc(f func(t time.Time) float64) InstantVectorSeriesFunction {
	return func(seriesData types.InstantVectorSeriesData, _ []types.ScalarData, tr types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
		if len(seriesData.Floats)+len(seriesData.Histograms) == 0 {
			fp := promql.FPoint{
				F: f(time.Unix(tr.StartT, 0).UTC()),
			}
			seriesData.Floats = append(seriesData.Floats, fp)
			return seriesData, nil
		}

		if len(seriesData.Floats) > 0 {
			for i := range seriesData.Floats {
				t := time.Unix(int64(seriesData.Floats[i].F), 0).UTC()
				seriesData.Floats[i].F = f(t)
			}
			return seriesData, nil
		}

		// we don't do time based function on histograms
		types.HPointSlicePool.Put(seriesData.Histograms, memoryConsumptionTracker)
		seriesData.Histograms = nil
		return seriesData, nil
	}
}
