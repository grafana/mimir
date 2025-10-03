// SPDX-License-Identifier: AGPL-3.0-only

package sharding

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestConcat(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")

	inner1 := &operators.TestOperator{
		Series: []labels.Labels{
			labels.FromStrings("operator", "1", "series", "1"),
			labels.FromStrings("operator", "1", "series", "2"),
		},
		Data: []types.InstantVectorSeriesData{
			createMockData(t, 0, memoryConsumptionTracker),
			createMockData(t, 1, memoryConsumptionTracker),
		},
		MemoryConsumptionTracker: memoryConsumptionTracker,
		Position:                 posrange.PositionRange{Start: 100, End: 200},
	}

	inner2 := &operators.TestOperator{
		Series: []labels.Labels{
			labels.FromStrings("operator", "2", "series", "1"),
			labels.FromStrings("operator", "2", "series", "2"),
		},
		Data: []types.InstantVectorSeriesData{
			createMockData(t, 2, memoryConsumptionTracker),
			createMockData(t, 3, memoryConsumptionTracker),
		},
		MemoryConsumptionTracker: memoryConsumptionTracker,
		Position:                 posrange.PositionRange{Start: 300, End: 400},
	}

	o, err := NewConcat([]types.InstantVectorOperator{inner1, inner2}, memoryConsumptionTracker)
	require.NoError(t, err)

	pos := o.ExpressionPosition()
	require.Equal(t, posrange.PositionRange{Start: 100, End: 200}, pos, "should take the position of the first inner operator")

	err = o.Prepare(ctx, nil)
	require.NoError(t, err)
	require.True(t, inner1.Prepared)
	require.True(t, inner2.Prepared)

	actualSeries, err := o.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	expectedSeries := testutils.LabelsToSeriesMetadata([]labels.Labels{
		labels.FromStrings("operator", "1", "series", "1"),
		labels.FromStrings("operator", "1", "series", "2"),
		labels.FromStrings("operator", "2", "series", "1"),
		labels.FromStrings("operator", "2", "series", "2"),
	})
	require.Equal(t, expectedSeries, actualSeries)
	types.SeriesMetadataSlicePool.Put(&actualSeries, memoryConsumptionTracker)

	expectedData := make([]types.InstantVectorSeriesData, 0, 4)
	actualData := make([]types.InstantVectorSeriesData, 0, 4)

	for idx := range 4 {
		d, err := o.NextSeries(ctx)
		require.NoError(t, err)
		actualData = append(actualData, d)

		expectedData = append(expectedData, createMockData(t, idx, memoryConsumptionTracker))
	}

	require.Equal(t, expectedData, actualData)
	putAllSeriesData(expectedData, memoryConsumptionTracker)
	putAllSeriesData(actualData, memoryConsumptionTracker)

	_, err = o.NextSeries(ctx)
	require.Equal(t, types.EOS, err)

	err = o.Finalize(ctx)
	require.NoError(t, err)
	require.True(t, inner1.Finalized)
	require.True(t, inner2.Finalized)

	o.Close()
	require.True(t, inner1.Closed)
	require.True(t, inner2.Closed)
	require.Zerof(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(), "expected all memory to be released, but still have:\n%v", memoryConsumptionTracker.DescribeCurrentMemoryConsumption())
}

func TestConcat_InnerOperatorsWithNoSeries(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")

	inner1 := &operators.TestOperator{MemoryConsumptionTracker: memoryConsumptionTracker}
	inner2 := &operators.TestOperator{MemoryConsumptionTracker: memoryConsumptionTracker}

	inner3 := &operators.TestOperator{
		Series: []labels.Labels{
			labels.FromStrings("operator", "3", "series", "1"),
			labels.FromStrings("operator", "3", "series", "2"),
		},
		Data: []types.InstantVectorSeriesData{
			createMockData(t, 0, memoryConsumptionTracker),
			createMockData(t, 1, memoryConsumptionTracker),
		},
		MemoryConsumptionTracker: memoryConsumptionTracker,
		Position:                 posrange.PositionRange{Start: 100, End: 200},
	}

	inner4 := &operators.TestOperator{MemoryConsumptionTracker: memoryConsumptionTracker}

	o, err := NewConcat([]types.InstantVectorOperator{inner1, inner2, inner3, inner4}, memoryConsumptionTracker)
	require.NoError(t, err)

	err = o.Prepare(ctx, nil)
	require.NoError(t, err)

	actualSeries, err := o.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	expectedSeries := testutils.LabelsToSeriesMetadata([]labels.Labels{
		labels.FromStrings("operator", "3", "series", "1"),
		labels.FromStrings("operator", "3", "series", "2"),
	})
	require.Equal(t, expectedSeries, actualSeries)
	types.SeriesMetadataSlicePool.Put(&actualSeries, memoryConsumptionTracker)

	expectedData := make([]types.InstantVectorSeriesData, 0, 2)
	actualData := make([]types.InstantVectorSeriesData, 0, 2)

	for idx := range 2 {
		d, err := o.NextSeries(ctx)
		require.NoError(t, err)
		actualData = append(actualData, d)

		expectedData = append(expectedData, createMockData(t, idx, memoryConsumptionTracker))
	}

	require.Equal(t, expectedData, actualData)
	putAllSeriesData(expectedData, memoryConsumptionTracker)
	putAllSeriesData(actualData, memoryConsumptionTracker)

	_, err = o.NextSeries(ctx)
	require.Equal(t, types.EOS, err)

	err = o.Finalize(ctx)
	require.NoError(t, err)
	require.True(t, inner1.Finalized)
	require.True(t, inner2.Finalized)
	require.True(t, inner3.Finalized)
	require.True(t, inner4.Finalized)

	o.Close()
	require.True(t, inner1.Closed)
	require.True(t, inner2.Closed)
	require.True(t, inner3.Closed)
	require.True(t, inner4.Closed)
	require.Zerof(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(), "expected all memory to be released, but still have:\n%v", memoryConsumptionTracker.DescribeCurrentMemoryConsumption())
}

func createMockData(t *testing.T, idx int, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) types.InstantVectorSeriesData {
	floats, err := types.FPointSlicePool.Get(2, memoryConsumptionTracker)
	require.NoError(t, err)

	floats = append(floats, promql.FPoint{T: 0, F: 100 + float64(idx)})
	floats = append(floats, promql.FPoint{T: 1000, F: 200 + float64(idx)})

	histograms, err := types.HPointSlicePool.Get(2, memoryConsumptionTracker)
	require.NoError(t, err)

	histograms = append(histograms, promql.HPoint{T: 2000, H: &histogram.FloatHistogram{Count: 300 + float64(idx)}})
	histograms = append(histograms, promql.HPoint{T: 3000, H: &histogram.FloatHistogram{Count: 400 + float64(idx)}})

	return types.InstantVectorSeriesData{
		Floats:     floats,
		Histograms: histograms,
	}
}

func putAllSeriesData(data []types.InstantVectorSeriesData, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	for _, d := range data {
		types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)
	}
}
