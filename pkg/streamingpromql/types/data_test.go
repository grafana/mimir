// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

func TestInstantVectorSeriesDataIterator(t *testing.T) {
	type expected struct {
		T       int64
		F       float64
		H       *histogram.FloatHistogram
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
				{1000, 1.1, nil, true},
				{2000, 2.2, nil, true},
				{3000, 3.3, nil, true},
				{0, 0, nil, false},
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
				{1500, 0, &histogram.FloatHistogram{Sum: 1500}, true},
				{2500, 0, &histogram.FloatHistogram{Sum: 2500}, true},
				{0, 0, nil, false},
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
				{1000, 1.1, nil, true},
				{1500, 0, &histogram.FloatHistogram{Sum: 1500}, true},
				{2000, 2.2, nil, true},
				{2500, 0, &histogram.FloatHistogram{Sum: 2500}, true},
				{3000, 3.3, nil, true},
				{4000, 4.4, nil, true},
				{5000, 5.5, nil, true},
				{5500, 0, &histogram.FloatHistogram{Sum: 5500}, true},
				{6000, 6.5, nil, true},
				{0, 0, nil, false},
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
				{1000, 1.1, nil, true},
				{1500, 0, &histogram.FloatHistogram{Sum: 1500}, true},
				{2000, 2.2, nil, true},
				{2500, 0, &histogram.FloatHistogram{Sum: 2500}, true},
				{3000, 3.3, nil, true},
				{4000, 4.4, nil, true},
				{5000, 5.5, nil, true},
				{5500, 0, &histogram.FloatHistogram{Sum: 5500}, true},
				{0, 0, nil, false},
			},
		},
		{
			name: "empty data",
			data: InstantVectorSeriesData{},
			expected: []expected{
				{0, 0, nil, false},
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
				{1000, 1.1, nil, true},
				{0, 0, nil, false},
				{0, 0, nil, false},
				{0, 0, nil, false},
			},
		},
	}

	iter := InstantVectorSeriesDataIterator{}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			iter.Reset(tc.data)

			for _, exp := range tc.expected {
				timestamp, floatVal, hist, hasNext := iter.Next()
				require.Equal(t, exp.T, timestamp)
				require.Equal(t, exp.F, floatVal)
				require.Equal(t, exp.H, hist)
				require.Equal(t, exp.HasNext, hasNext)
			}
		})
	}
}

func TestHasDuplicateSeries(t *testing.T) {
	testCases := map[string]struct {
		input           []SeriesMetadata
		hasDuplicate    bool
		expectedExample labels.Labels
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
			hasDuplicate:    true,
			expectedExample: labels.FromStrings(labels.MetricName, "foo", "bar", "1"),
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
			hasDuplicate:    true,
			expectedExample: labels.FromStrings(labels.MetricName, "foo", "bar", "1"),
		},
		"three series, all the same": {
			input: []SeriesMetadata{
				{Labels: labels.FromStrings(labels.MetricName, "foo", "bar", "1")},
				{Labels: labels.FromStrings(labels.MetricName, "foo", "bar", "1")},
				{Labels: labels.FromStrings(labels.MetricName, "foo", "bar", "1")},
			},
			hasDuplicate:    true,
			expectedExample: labels.FromStrings(labels.MetricName, "foo", "bar", "1"),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actualHasDuplicate, actualExample := HasDuplicateSeries(testCase.input)

			require.Equal(t, testCase.hasDuplicate, actualHasDuplicate)

			if testCase.hasDuplicate {
				require.Equal(t, testCase.expectedExample, actualExample)
			} else {
				require.Equal(t, labels.EmptyLabels(), actualExample)
			}
		})
	}
}
