// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

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
				{Labels: labels.FromStrings(labels.MetricName, "foo")},
			},
			hasDuplicate: false,
		},
		"two series, both different": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(labels.MetricName, "foo")},
				{Labels: labels.FromStrings(labels.MetricName, "bar")},
			},
			hasDuplicate: false,
		},
		"two series, some common labels but not completely the same": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(labels.MetricName, "foo", "bar", "1")},
				{Labels: labels.FromStrings(labels.MetricName, "foo", "bar", "2")},
			},
			hasDuplicate: false,
		},
		"two series, both the same": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(labels.MetricName, "foo", "bar", "1")},
				{Labels: labels.FromStrings(labels.MetricName, "foo", "bar", "1")},
			},
			hasDuplicate: true,
		},
		"three series, all different": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(labels.MetricName, "foo")},
				{Labels: labels.FromStrings(labels.MetricName, "bar")},
				{Labels: labels.FromStrings(labels.MetricName, "baz")},
			},
			hasDuplicate: false,
		},
		"three series, some with common labels but none completely the same": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(labels.MetricName, "foo", "bar", "1")},
				{Labels: labels.FromStrings(labels.MetricName, "foo", "bar", "2")},
				{Labels: labels.FromStrings(labels.MetricName, "foo", "bar", "3")},
			},
			hasDuplicate: false,
		},
		"three series, some the same": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(labels.MetricName, "foo", "bar", "1")},
				{Labels: labels.FromStrings(labels.MetricName, "foo", "bar", "1")},
				{Labels: labels.FromStrings(labels.MetricName, "foo", "bar", "3")},
			},
			hasDuplicate: true,
		},
		"three series, all the same": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(labels.MetricName, "foo", "bar", "1")},
				{Labels: labels.FromStrings(labels.MetricName, "foo", "bar", "1")},
				{Labels: labels.FromStrings(labels.MetricName, "foo", "bar", "1")},
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
			} else {
				qtr = NewRangeQueryTimeRange(tc.start, tc.end, tc.interval)
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
