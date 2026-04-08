// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestDropName(t *testing.T) {
	inputSeries := []labels.Labels{
		labels.FromStrings(model.MetricNameLabel, "metric", "foo", "1"),
		labels.FromStrings(model.MetricNameLabel, "metric", "foo", "2"),
		labels.FromStrings(model.MetricNameLabel, "metric", "foo", "3"),
	}
	inputData := []types.InstantVectorSeriesData{
		{Floats: []promql.FPoint{{T: 0, F: 0}, {T: 1, F: 1}}},
		{Floats: []promql.FPoint{{T: 0, F: 10}, {T: 1, F: 11}}},
		{Floats: []promql.FPoint{{T: 0, F: 20}, {T: 1, F: 21}}},
	}
	inputDropName := []bool{
		true,
		false,
		true,
	}
	expectedOutputSeries := []labels.Labels{
		labels.FromStrings("foo", "1"),
		labels.FromStrings(model.MetricNameLabel, "metric", "foo", "2"),
		labels.FromStrings("foo", "3"),
	}
	expectedOutputData := []types.InstantVectorSeriesData{
		{Floats: []promql.FPoint{{T: 0, F: 0}, {T: 1, F: 1}}},
		{Floats: []promql.FPoint{{T: 0, F: 10}, {T: 1, F: 11}}},
		{Floats: []promql.FPoint{{T: 0, F: 20}, {T: 1, F: 21}}},
	}
	expectedOutputDropName := []bool{
		false,
		false,
		false,
	}

	t.Run("DropName instant vector", func(t *testing.T) {
		ctx := context.Background()
		memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
		inner := &TestOperator{
			Series:                   inputSeries,
			Data:                     inputData,
			DropName:                 inputDropName,
			MemoryConsumptionTracker: memoryConsumptionTracker,
		}
		o := NewDropNameInstant(inner, memoryConsumptionTracker)

		outputSeriesMetadata, err := o.SeriesMetadata(ctx, nil)
		require.NoError(t, err)
		require.Equal(t, testutils.LabelsToSeriesMetadataWithDropName(expectedOutputSeries, expectedOutputDropName), outputSeriesMetadata)

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
		require.Equal(t, expectedOutputData, outputData)
	})

	t.Run("DropName range vector", func(t *testing.T) {
		ctx := context.Background()
		memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

		inner := NewTestRangeOperator(
			inputSeries,
			inputDropName,
			inputData,
			time.Millisecond,
			memoryConsumptionTracker,
		)
		o := NewDropNameRange(inner, memoryConsumptionTracker)

		outputSeriesMetadata, err := o.SeriesMetadata(ctx, nil)
		require.NoError(t, err)
		require.Equal(t, testutils.LabelsToSeriesMetadataWithDropName(expectedOutputSeries, expectedOutputDropName), outputSeriesMetadata)

		var (
			outputData []types.InstantVectorSeriesData
			stepData   *types.RangeVectorStepData
			floats     []promql.FPoint
			histograms []promql.HPoint
		)
		for {
			err = o.NextSeries(ctx)
			if err != nil {
				break
			}

			for {
				// We iterate over each step but there's only a single step. This lets us
				// compare the same outputs as the instant vector operator.
				stepData, err = o.NextStepSamples(ctx)
				if err != nil {
					break
				}

				floats, err = stepData.Floats.CopyPoints()
				require.NoError(t, err)

				histograms, err = stepData.Histograms.CopyPoints()
				require.NoError(t, err)

				outputData = append(outputData, types.InstantVectorSeriesData{
					Floats:     floats,
					Histograms: histograms,
				})
			}

			require.Equal(t, types.EOS, err)
		}

		require.Equal(t, types.EOS, err)
		require.Equal(t, expectedOutputData, outputData)
	})
}
