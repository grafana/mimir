package functions

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type ScalarTime struct {
	Value                    float64
	ts                       time.Time
	TimeRange                types.QueryTimeRange
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker

	expressionPosition posrange.PositionRange
}

var _ types.ScalarOperator = &ScalarTime{}

func NewScalarTime(
	timeRange types.QueryTimeRange,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
) *ScalarTime {
	return &ScalarTime{
		TimeRange:                timeRange,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
	}
}

func (s *ScalarTime) GetValues(_ context.Context) (types.ScalarData, error) {
	samples, err := types.FPointSlicePool.Get(s.TimeRange.StepCount, s.MemoryConsumptionTracker)

	if err != nil {
		return types.ScalarData{}, err
	}

	samples = samples[:s.TimeRange.StepCount]

	for step := 0; step < s.TimeRange.StepCount; step++ {
		t := s.TimeRange.StartT + int64(step)*s.TimeRange.IntervalMilliseconds
		samples[step].T = t
		samples[step].F = float64(t)
	}

	return types.ScalarData{Samples: samples}, nil
}

func (s *ScalarTime) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *ScalarTime) Close() {
	// Nothing to do.
}

type WrapperTime struct {
	WrapperFunc              func(time time.Time) float64
	Value                    float64
	ts                       time.Time
	TimeRange                types.QueryTimeRange
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker

	expressionPosition posrange.PositionRange
}

func NewWrapperTime(
	wrapperFunc func(time time.Time) float64,
	timeRange types.QueryTimeRange,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
) *WrapperTime {
	return &WrapperTime{
		WrapperFunc:              wrapperFunc,
		TimeRange:                timeRange,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
	}
}

func (s *WrapperTime) GetValues(_ context.Context) (types.ScalarData, error) {
	samples, err := types.FPointSlicePool.Get(s.TimeRange.StepCount, s.MemoryConsumptionTracker)

	if err != nil {
		return types.ScalarData{}, err
	}

	samples = samples[:s.TimeRange.StepCount]

	for step := 0; step < s.TimeRange.StepCount; step++ {
		t := s.TimeRange.StartT + int64(step)*s.TimeRange.IntervalMilliseconds
		samples[step].T = t
		samples[step].F = s.WrapperFunc(time.Unix(int64(t), 0).UTC())
	}

	return types.ScalarData{Samples: samples}, nil
}

func (s *WrapperTime) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *WrapperTime) Close() {
	// Nothing to do.
}

var DaysInMonth = func(t time.Time) float64 {
	return float64(32 - time.Date(t.Year(), t.Month(), 32, 0, 0, 0, 0, time.UTC).Day())
}

var DayOfMonth = func(t time.Time) float64 {
	return float64(t.Day())
}

var DayOfWeek = func(t time.Time) float64 {
	return float64(t.Weekday())
}

var DayOfYear = func(t time.Time) float64 {
	return float64(t.YearDay())
}

var Hour = func(t time.Time) float64 {
	return float64(t.Hour())
}

var Minute = func(t time.Time) float64 {
	return float64(t.Minute())
}

var Month = func(t time.Time) float64 {
	return float64(t.Minute())
}

var Year = func(t time.Time) float64 {
	return float64(t.Year())
}
