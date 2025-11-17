// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestDropName(t *testing.T) {
	testCases := map[string]struct {
		inputSeries            []labels.Labels
		inputData              []types.InstantVectorSeriesData
		inputDropName          []bool
		expectedOutputSeries   []labels.Labels
		expectedOutputData     []types.InstantVectorSeriesData
		expectedOutputDropName []bool
	}{
		"with dropName set partially": {
			inputSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "metric", "foo", "1"),
				labels.FromStrings(model.MetricNameLabel, "metric", "foo", "2"),
				labels.FromStrings(model.MetricNameLabel, "metric", "foo", "3"),
			},
			inputData: []types.InstantVectorSeriesData{
				{Floats: []promql.FPoint{{T: 0, F: 0}, {T: 1, F: 1}}},
				{Floats: []promql.FPoint{{T: 0, F: 10}, {T: 1, F: 11}}},
				{Floats: []promql.FPoint{{T: 0, F: 20}, {T: 1, F: 21}}},
			},
			inputDropName: []bool{
				true,
				false,
				true,
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("foo", "1"),
				labels.FromStrings(model.MetricNameLabel, "metric", "foo", "2"),
				labels.FromStrings("foo", "3"),
			},
			expectedOutputData: []types.InstantVectorSeriesData{
				{Floats: []promql.FPoint{{T: 0, F: 0}, {T: 1, F: 1}}},
				{Floats: []promql.FPoint{{T: 0, F: 10}, {T: 1, F: 11}}},
				{Floats: []promql.FPoint{{T: 0, F: 20}, {T: 1, F: 21}}},
			},
			expectedOutputDropName: []bool{
				false,
				false,
				false,
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
			inner := &TestOperator{
				Series:                   testCase.inputSeries,
				Data:                     testCase.inputData,
				DropName:                 testCase.inputDropName,
				MemoryConsumptionTracker: memoryConsumptionTracker,
			}
			o := NewDropName(inner, memoryConsumptionTracker)

			outputSeriesMetadata, err := o.SeriesMetadata(ctx, nil)
			require.NoError(t, err)
			require.Equal(t, testutils.LabelsToSeriesMetadataWithDropName(testCase.expectedOutputSeries, testCase.expectedOutputDropName), outputSeriesMetadata)

			outputData := []types.InstantVectorSeriesData{}
			for {
				var nextSeries types.InstantVectorSeriesData
				nextSeries, err = o.NextSeries(ctx)

				if err != nil {
					break
				}

				outputData = append(outputData, nextSeries)
			}

			require.Equal(t, types.EOS, err)
			require.Equal(t, testCase.expectedOutputData, outputData)
		})
	}
}
