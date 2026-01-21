// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestInstantVectorSeriesData_Clone(t *testing.T) {
	original := InstantVectorSeriesData{
		Floats: []promql.FPoint{
			{T: 0, F: 0},
			{T: 1, F: 1},
		},
		Histograms: []promql.HPoint{
			{T: 2, H: &histogram.FloatHistogram{Count: 2}},
			{T: 3, H: &histogram.FloatHistogram{Count: 3}},
		},
	}

	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
	cloned, err := original.Clone(memoryConsumptionTracker)

	require.NoError(t, err)
	require.Equal(t, original, cloned, "clone should have same data as original")
	require.NotSame(t, &original.Floats[0], &cloned.Floats[0], "clone should not share float slice with original")
	require.NotSame(t, &original.Histograms[0], &cloned.Histograms[0], "clone should not share histogram slice with original")
	require.NotSame(t, original.Histograms[0].H, cloned.Histograms[0].H, "clone should not share first histogram with original")
	require.NotSame(t, original.Histograms[1].H, cloned.Histograms[1].H, "clone should not share second histogram with original")
}

func TestInstantVectorSeriesDataIterator(t *testing.T) {
	type expected struct {
		T       int64
		F       float64
		H       *histogram.FloatHistogram
		HIndex  int
		HasNext bool
	}
	type testCase struct {
		name     string
		data     InstantVectorSeriesData
		expected []expected
	}

	testCases := []testCase{
		{
			name: "floats only",
			data: InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1000, F: 1.1},
					{T: 2000, F: 2.2},
					{T: 3000, F: 3.3},
				},
			},
			expected: []expected{
				{1000, 1.1, nil, -1, true},
				{2000, 2.2, nil, -1, true},
				{3000, 3.3, nil, -1, true},
				{0, 0, nil, -1, false},
			},
		},
		{
			name: "histograms only",
			data: InstantVectorSeriesData{
				Histograms: []promql.HPoint{
					{T: 1500, H: &histogram.FloatHistogram{Sum: 1500}},
					{T: 2500, H: &histogram.FloatHistogram{Sum: 2500}},
				},
			},
			expected: []expected{
				{1500, 0, &histogram.FloatHistogram{Sum: 1500}, 0, true},
				{2500, 0, &histogram.FloatHistogram{Sum: 2500}, 1, true},
				{0, 0, nil, -1, false},
			},
		},
		{
			name: "mixed data ends with float",
			data: InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1000, F: 1.1},
					{T: 2000, F: 2.2},
					{T: 3000, F: 3.3},
					{T: 4000, F: 4.4},
					{T: 5000, F: 5.5},
					{T: 6000, F: 6.5},
				},
				Histograms: []promql.HPoint{
					{T: 1500, H: &histogram.FloatHistogram{Sum: 1500}},
					{T: 2500, H: &histogram.FloatHistogram{Sum: 2500}},
					{T: 5500, H: &histogram.FloatHistogram{Sum: 5500}},
				},
			},
			expected: []expected{
				{1000, 1.1, nil, -1, true},
				{1500, 0, &histogram.FloatHistogram{Sum: 1500}, 0, true},
				{2000, 2.2, nil, -1, true},
				{2500, 0, &histogram.FloatHistogram{Sum: 2500}, 1, true},
				{3000, 3.3, nil, -1, true},
				{4000, 4.4, nil, -1, true},
				{5000, 5.5, nil, -1, true},
				{5500, 0, &histogram.FloatHistogram{Sum: 5500}, 2, true},
				{6000, 6.5, nil, -1, true},
				{0, 0, nil, -1, false},
			},
		},
		{
			name: "mixed data ends with histogram",
			data: InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1000, F: 1.1},
					{T: 2000, F: 2.2},
					{T: 3000, F: 3.3},
					{T: 4000, F: 4.4},
					{T: 5000, F: 5.5},
				},
				Histograms: []promql.HPoint{
					{T: 1500, H: &histogram.FloatHistogram{Sum: 1500}},
					{T: 2500, H: &histogram.FloatHistogram{Sum: 2500}},
					{T: 5500, H: &histogram.FloatHistogram{Sum: 5500}},
				},
			},
			expected: []expected{
				{1000, 1.1, nil, -1, true},
				{1500, 0, &histogram.FloatHistogram{Sum: 1500}, 0, true},
				{2000, 2.2, nil, -1, true},
				{2500, 0, &histogram.FloatHistogram{Sum: 2500}, 1, true},
				{3000, 3.3, nil, -1, true},
				{4000, 4.4, nil, -1, true},
				{5000, 5.5, nil, -1, true},
				{5500, 0, &histogram.FloatHistogram{Sum: 5500}, 2, true},
				{0, 0, nil, -1, false},
			},
		},
		{
			name: "empty data",
			data: InstantVectorSeriesData{},
			expected: []expected{
				{0, 0, nil, -1, false},
			},
		},
		{
			name: "multiple next calls after exhaustion",
			data: InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1000, F: 1.1},
				},
			},
			expected: []expected{
				{1000, 1.1, nil, -1, true},
				{0, 0, nil, -1, false},
				{0, 0, nil, -1, false},
				{0, 0, nil, -1, false},
			},
		},
	}

	iter := InstantVectorSeriesDataIterator{}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			iter.Reset(tc.data)

			for _, exp := range tc.expected {
				ts, f, h, hIndex, hasNext := iter.Next()
				require.Equal(t, exp.T, ts)
				require.Equal(t, exp.F, f)
				require.Equal(t, exp.H, h)
				require.Equal(t, exp.HIndex, hIndex)
				require.Equal(t, exp.HasNext, hasNext)
			}
		})
	}
}

func TestHasDuplicateSeries(t *testing.T) {
	testCases := map[string]struct {
		input        []SeriesMetadata
		hasDuplicate bool
	}{
		"no series": {
			input:        []SeriesMetadata{},
			hasDuplicate: false,
		},
		"one series": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo")},
			},
			hasDuplicate: false,
		},
		"two series, both different": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo")},
				{Labels: labels.FromStrings(model.MetricNameLabel, "bar")},
			},
			hasDuplicate: false,
		},
		"two series, some common labels but not completely the same": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "1")},
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "2")},
			},
			hasDuplicate: false,
		},
		"two series, both the same": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "1")},
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "1")},
			},
			hasDuplicate: true,
		},
		"three series, all different": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo")},
				{Labels: labels.FromStrings(model.MetricNameLabel, "bar")},
				{Labels: labels.FromStrings(model.MetricNameLabel, "baz")},
			},
			hasDuplicate: false,
		},
		"three series, some with common labels but none completely the same": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "1")},
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "2")},
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "3")},
			},
			hasDuplicate: false,
		},
		"three series, some the same": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "1")},
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "1")},
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "3")},
			},
			hasDuplicate: true,
		},
		"three series, all the same": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "1")},
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "1")},
				{Labels: labels.FromStrings(model.MetricNameLabel, "foo", "bar", "1")},
			},
			hasDuplicate: true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actualHasDuplicate := HasDuplicateSeries(testCase.input)

			require.Equal(t, testCase.hasDuplicate, actualHasDuplicate)
		})
	}
}

func TestQueryTimeRange(t *testing.T) {
	type testCase struct {
		start         time.Time
		end           time.Time
		interval      time.Duration
		expectedStart int64
		expectedEnd   int64
		expectedIntMs int64
		expectedSteps int
		testTimes     []time.Time
		expectedIdxs  []int64
	}

	startTime := time.Now()
	testCases := map[string]testCase{
		"Instant query": {
			start:         startTime,
			end:           startTime,
			interval:      0,
			expectedStart: timestamp.FromTime(startTime),
			expectedEnd:   timestamp.FromTime(startTime),
			expectedIntMs: 1,
			expectedSteps: 1,
			testTimes:     []time.Time{startTime},
			expectedIdxs:  []int64{0},
		},
		"Range query with 15-minute interval": {
			start:         startTime,
			end:           startTime.Add(time.Hour),
			interval:      time.Minute * 15,
			expectedStart: timestamp.FromTime(startTime),
			expectedEnd:   timestamp.FromTime(startTime.Add(time.Hour)),
			expectedIntMs: (time.Minute * 15).Milliseconds(),
			expectedSteps: 5,
			testTimes: []time.Time{
				startTime,
				startTime.Add(time.Minute * 15),
				startTime.Add(time.Minute * 30),
				startTime.Add(time.Minute * 45),
				startTime.Add(time.Hour),
			},
			expectedIdxs: []int64{0, 1, 2, 3, 4},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var qtr QueryTimeRange

			if tc.interval == 0 {
				qtr = NewInstantQueryTimeRange(tc.start)
				require.True(t, qtr.IsInstant)
			} else {
				qtr = NewRangeQueryTimeRange(tc.start, tc.end, tc.interval)
				require.False(t, qtr.IsInstant)
			}

			require.Equal(t, tc.expectedStart, qtr.StartT, "StartT matches")
			require.Equal(t, tc.expectedEnd, qtr.EndT, "EndT matches")
			require.Equal(t, tc.expectedIntMs, qtr.IntervalMilliseconds, "IntervalMs matches")
			require.Equal(t, tc.expectedSteps, qtr.StepCount, "StepCount matches")

			for i, tt := range tc.testTimes {
				ts := timestamp.FromTime(tt)
				pointIdx := qtr.PointIndex(ts)
				expectedIdx := tc.expectedIdxs[i]
				require.Equal(t, expectedIdx, pointIdx, "PointIdx matches for time %v", tt)
			}
		})
	}
}

func TestRangeVectorStepData_SubStep(t *testing.T) {
	memoryTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
	floats := NewFPointRingBuffer(memoryTracker)
	histograms := NewHPointRingBuffer(memoryTracker)

	// Add float points at T=110, 120, 130, 140, 150, 160, 170, 180, 190
	for i := int64(110); i <= 190; i += 10 {
		require.NoError(t, floats.Append(promql.FPoint{T: i, F: float64(i)}))
	}

	// Add histogram points at T=115, 135, 155, 175
	for i := int64(115); i <= 175; i += 20 {
		require.NoError(t, histograms.Append(promql.HPoint{T: i, H: &histogram.FloatHistogram{Count: float64(i)}}))
	}

	step := &RangeVectorStepData{
		StepT:      200,
		RangeStart: 100,
		RangeEnd:   200,
		Floats:     floats.ViewUntilSearchingBackwards(200, nil),
		Histograms: histograms.ViewUntilSearchingBackwards(200, nil),
	}

	t.Run("single substep", func(t *testing.T) {
		substep, err := step.SubStep(120, 160, nil)
		require.NoError(t, err)

		require.Equal(t, int64(200), substep.StepT)
		require.Equal(t, int64(120), substep.RangeStart)
		require.Equal(t, int64(160), substep.RangeEnd)

		// Verify floats: should have T=130, 140, 150, 160 (4 points)
		require.Equal(t, 4, substep.Floats.Count())
		require.Equal(t, int64(130), substep.Floats.First().T)
		last, hasLast := substep.Floats.Last()
		require.True(t, hasLast)
		require.Equal(t, int64(160), last.T)

		// Verify histograms: should have T=135, 155 (2 points)
		require.Equal(t, 2, substep.Histograms.Count())
		require.Equal(t, int64(135), substep.Histograms.First().T)
		lastH, hasLastH := substep.Histograms.Last()
		require.True(t, hasLastH)
		require.Equal(t, int64(155), lastH.T)
	})

	t.Run("substep with no matching points", func(t *testing.T) {
		substep, err := step.SubStep(195, 200, nil)
		require.NoError(t, err)

		require.Equal(t, int64(200), substep.StepT)
		require.Equal(t, int64(195), substep.RangeStart)
		require.Equal(t, int64(200), substep.RangeEnd)
		require.Equal(t, 0, substep.Floats.Count())
		require.Equal(t, 0, substep.Histograms.Count())
	})

	t.Run("substep spanning entire parent range", func(t *testing.T) {
		// Create substep for entire parent range (100, 200]
		substep, err := step.SubStep(100, 200, nil)
		require.NoError(t, err)

		// Should have all points
		require.Equal(t, 9, substep.Floats.Count())
		require.Equal(t, 4, substep.Histograms.Count())
		require.Equal(t, int64(110), substep.Floats.First().T)
		last, _ := substep.Floats.Last()
		require.Equal(t, int64(190), last.T)
	})
}

func TestRangeVectorStepData_SubStep_ErrorCases(t *testing.T) {
	testCases := []struct {
		name        string
		rangeStart  int64
		rangeEnd    int64
		smoothed    bool
		anchored    bool
		expectedErr string
	}{
		{
			name:        "start before parent start",
			rangeStart:  50,
			rangeEnd:    150,
			expectedErr: "substep start (50) is before parent step's start (100)",
		},
		{
			name:        "rangeEnd after parent end",
			rangeStart:  150,
			rangeEnd:    250,
			expectedErr: "substep end (250) is after parent step's end (200)",
		},
		{
			name:        "rangeStart equals end",
			rangeStart:  150,
			rangeEnd:    150,
			expectedErr: "substep start (150) must be less than end (150)",
		},
		{
			name:        "rangeStart greater than end",
			rangeStart:  180,
			rangeEnd:    170,
			expectedErr: "substep start (180) must be less than end (170)",
		},
		{
			name:        "start before parent and end after parent",
			rangeStart:  50,
			rangeEnd:    250,
			expectedErr: "substep start (50) is before parent step's start (100)",
		},
		{
			name:        "smoothed not supported",
			rangeStart:  120,
			rangeEnd:    150,
			smoothed:    true,
			expectedErr: "substep not supported for range vectors with anchored or smoothed modifiers",
		},
		{
			name:        "anchored not supported",
			rangeStart:  120,
			rangeEnd:    150,
			anchored:    true,
			expectedErr: "substep not supported for range vectors with anchored or smoothed modifiers",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			memoryTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
			floats := NewFPointRingBuffer(memoryTracker)
			histograms := NewHPointRingBuffer(memoryTracker)

			step := &RangeVectorStepData{
				StepT:      150,
				RangeStart: 100,
				RangeEnd:   200,
				Smoothed:   tc.smoothed,
				Anchored:   tc.anchored,
				Floats:     floats.ViewUntilSearchingBackwards(200, nil),
				Histograms: histograms.ViewUntilSearchingBackwards(200, nil),
			}

			_, err := step.SubStep(tc.rangeStart, tc.rangeEnd, nil)
			require.EqualError(t, err, tc.expectedErr)
		})
	}
}
