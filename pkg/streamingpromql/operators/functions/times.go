package functions

import (
	"time"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

var Time = timeFunc()

var DaysInMonth = dayWrapperFunc(func(t time.Time) float64 {
	return float64(32 - time.Date(t.Year(), t.Month(), 32, 0, 0, 0, 0, time.UTC).Day())
})

var DayOfMonth = dayWrapperFunc(func(t time.Time) float64 {
	return float64(t.Day())
})

var DayOfWeek = dayWrapperFunc(func(t time.Time) float64 {
	return float64(t.Weekday())
})

var DayOfYear = dayWrapperFunc(func(t time.Time) float64 {
	return float64(t.YearDay())
})

var Hour = dayWrapperFunc(func(t time.Time) float64 {
	return float64(t.Hour())
})

var Minute = dayWrapperFunc(func(t time.Time) float64 {
	return float64(t.Minute())
})

var Month = dayWrapperFunc(func(t time.Time) float64 {
	return float64(t.Minute())
})

var Year = dayWrapperFunc(func(t time.Time) float64 {
	return float64(t.Year())
})

func timeFunc() InstantVectorSeriesFunction {
	return func(seriesData types.InstantVectorSeriesData, _ []types.ScalarData, qtr types.QueryTimeRange, _ *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
		for i := range seriesData.Floats {
			seriesData.Floats[i].F = float64(qtr.StartT) / 1000
		}
		return seriesData, nil
	}
}

func dayWrapperFunc(f func(t time.Time) float64) InstantVectorSeriesFunction {
	return func(seriesData types.InstantVectorSeriesData, _ []types.ScalarData, _ types.QueryTimeRange, _ *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
		for i := range seriesData.Floats {
			t := time.Unix(int64(seriesData.Floats[i].F), 0).UTC()
			seriesData.Floats[i].F = f(t)
		}
		return seriesData, nil
	}
}
