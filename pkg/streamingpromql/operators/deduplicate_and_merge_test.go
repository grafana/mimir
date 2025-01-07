// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestDeduplicateAndMerge(t *testing.T) {
	// Most of the edge cases are already covered by the tests for MergeSeries and InstantVectorOperatorBuffer, so we focus
	// on the logic unique to DeduplicateAndMerge: handling conflicts and correctly sorting the output series to minimise
	// the number of buffered series.

	testCases := map[string]struct {
		inputSeries []labels.Labels
		inputData   []types.InstantVectorSeriesData

		expectConflict       bool
		expectedOutputSeries []labels.Labels
		expectedOutputData   []types.InstantVectorSeriesData
	}{
		"no series": {
			inputSeries: []labels.Labels{},
			inputData:   []types.InstantVectorSeriesData{},

			expectedOutputSeries: []labels.Labels{},
			expectedOutputData:   []types.InstantVectorSeriesData{},
		},

		"one series": {
			inputSeries: []labels.Labels{
				labels.FromStrings("foo", "1"),
			},
			inputData: []types.InstantVectorSeriesData{
				{Floats: []promql.FPoint{{T: 0, F: 0}, {T: 1, F: 1}}},
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("foo", "1"),
			},
			expectedOutputData: []types.InstantVectorSeriesData{
				{Floats: []promql.FPoint{{T: 0, F: 0}, {T: 1, F: 1}}},
			},
		},

		"many series, no duplicates": {
			inputSeries: []labels.Labels{
				labels.FromStrings("foo", "1"),
				labels.FromStrings("foo", "2"),
				labels.FromStrings("foo", "3"),
			},
			inputData: []types.InstantVectorSeriesData{
				{Floats: []promql.FPoint{{T: 0, F: 0}, {T: 1, F: 1}}},
				{Floats: []promql.FPoint{{T: 0, F: 10}, {T: 1, F: 11}}},
				{Floats: []promql.FPoint{{T: 0, F: 20}, {T: 1, F: 21}}},
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("foo", "1"),
				labels.FromStrings("foo", "2"),
				labels.FromStrings("foo", "3"),
			},
			expectedOutputData: []types.InstantVectorSeriesData{
				{Floats: []promql.FPoint{{T: 0, F: 0}, {T: 1, F: 1}}},
				{Floats: []promql.FPoint{{T: 0, F: 10}, {T: 1, F: 11}}},
				{Floats: []promql.FPoint{{T: 0, F: 20}, {T: 1, F: 21}}},
			},
		},

		"many series, has duplicates, no conflicts": {
			inputSeries: []labels.Labels{
				labels.FromStrings("foo", "1"),
				labels.FromStrings("foo", "2"),
				labels.FromStrings("foo", "3"),
				labels.FromStrings("foo", "1"),
				labels.FromStrings("foo", "4"),
				labels.FromStrings("foo", "3"),
			},
			inputData: []types.InstantVectorSeriesData{
				{Floats: []promql.FPoint{{T: 0, F: 10}}},
				{Floats: []promql.FPoint{{T: 0, F: 20}}},
				{Floats: []promql.FPoint{{T: 1, F: 30}}},
				{Floats: []promql.FPoint{{T: 1, F: 40}}},
				{Floats: []promql.FPoint{{T: 0, F: 50}}},
				{Floats: []promql.FPoint{{T: 0, F: 60}}},
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("foo", "2"),
				labels.FromStrings("foo", "1"),
				labels.FromStrings("foo", "4"),
				labels.FromStrings("foo", "3"),
			},
			expectedOutputData: []types.InstantVectorSeriesData{
				{Floats: []promql.FPoint{{T: 0, F: 20}}},
				{Floats: []promql.FPoint{{T: 0, F: 10}, {T: 1, F: 40}}},
				{Floats: []promql.FPoint{{T: 0, F: 50}}},
				{Floats: []promql.FPoint{{T: 0, F: 60}, {T: 1, F: 30}}},
			},
		},

		"many series, has duplicate, has conflicting floats at same timestamp": {
			inputSeries: []labels.Labels{
				labels.FromStrings("foo", "1"),
				labels.FromStrings("foo", "2"),
				labels.FromStrings("foo", "1"),
			},
			inputData: []types.InstantVectorSeriesData{
				{Floats: []promql.FPoint{{T: 0, F: 0}, {T: 2, F: 1}}},
				{Floats: []promql.FPoint{{T: 0, F: 10}, {T: 1, F: 11}}},
				{Floats: []promql.FPoint{{T: 1, F: 20}, {T: 2, F: 21}}},
			},

			expectConflict: true,
		},

		"many series, has duplicate, has conflicting histograms at same timestamp": {
			inputSeries: []labels.Labels{
				labels.FromStrings("foo", "1"),
				labels.FromStrings("foo", "2"),
				labels.FromStrings("foo", "1"),
			},
			inputData: []types.InstantVectorSeriesData{
				{Histograms: []promql.HPoint{{T: 0, H: &histogram.FloatHistogram{}}, {T: 2, H: &histogram.FloatHistogram{}}}},
				{Floats: []promql.FPoint{{T: 0, F: 10}, {T: 1, F: 11}}},
				{Histograms: []promql.HPoint{{T: 1, H: &histogram.FloatHistogram{}}, {T: 2, H: &histogram.FloatHistogram{}}}},
			},

			expectConflict: true,
		},

		"many series, has duplicate, has conflicting floats and histograms at same timestamp": {
			inputSeries: []labels.Labels{
				labels.FromStrings("foo", "1"),
				labels.FromStrings("foo", "2"),
				labels.FromStrings("foo", "1"),
			},
			inputData: []types.InstantVectorSeriesData{
				{Histograms: []promql.HPoint{{T: 0, H: &histogram.FloatHistogram{}}, {T: 2, H: &histogram.FloatHistogram{}}}},
				{Floats: []promql.FPoint{{T: 0, F: 10}, {T: 1, F: 11}}},
				{Floats: []promql.FPoint{{T: 1, F: 20}, {T: 2, F: 21}}},
			},

			expectConflict: true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			inner := &TestOperator{Series: testCase.inputSeries, Data: testCase.inputData}
			o := NewDeduplicateAndMerge(inner, limiting.NewMemoryConsumptionTracker(0, nil))

			outputSeriesMetadata, err := o.SeriesMetadata(context.Background())
			require.NoError(t, err)

			if !testCase.expectConflict {
				require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedOutputSeries), outputSeriesMetadata)
			}

			outputData := []types.InstantVectorSeriesData{}

			for {
				var nextSeries types.InstantVectorSeriesData
				nextSeries, err = o.NextSeries(context.Background())

				if err != nil {
					break
				}

				outputData = append(outputData, nextSeries)
			}

			if testCase.expectConflict {
				require.EqualError(t, err, "vector cannot contain metrics with the same labelset")
			} else {
				require.Equal(t, types.EOS, err)
				require.Equal(t, testCase.expectedOutputData, outputData)
			}
		})
	}
}
