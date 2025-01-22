// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"time"

	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

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
