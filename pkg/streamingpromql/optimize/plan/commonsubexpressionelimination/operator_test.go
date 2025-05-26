// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"context"
	"errors"
	"strconv"
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

func TestOperator_Buffering(t *testing.T) {
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(0, nil)
	inner, expectedData := createTestOperator(t, 6, memoryConsumptionTracker)

	buffer := NewDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()
	ctx := context.Background()

	// Both consumers should get the same series metadata.
	metadata1, err := consumer1.SeriesMetadata(ctx)
	require.NoError(t, err)
	metadata2, err := consumer2.SeriesMetadata(ctx)
	require.NoError(t, err)
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.Series), metadata1, "first consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.Series), metadata2, "second consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(metadata1, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(metadata2, memoryConsumptionTracker)

	// Read some data from the first consumer and ensure that it was buffered for the second consumer.
	d, err := consumer1.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[0], d)
	require.Equal(t, 1, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	d, err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[1], d)
	require.Equal(t, 2, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	// Read the same data from the second consumer, and then keep reading data beyond what has already been buffered.
	d, err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[0], d)
	require.Equal(t, 1, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	d, err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[1], d)
	require.Equal(t, 0, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	d, err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[2], d)
	require.Equal(t, 1, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	d, err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[3], d)
	require.Equal(t, 2, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	d, err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[4], d)
	require.Equal(t, 3, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	// Read the buffered data in the first consumer.
	d, err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[2], d)
	require.Equal(t, 2, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	// Close the first consumer, check that the data that was being buffered for it is released.
	consumer1.Close()
	require.Equal(t, 0, buffer.buffer.Size())

	// Check that the second consumer can still read data and that we don't bother buffering it.
	d, err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[5], d)
	require.Equal(t, 0, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	// Close the second consumer, and check that the inner operator was closed.
	consumer2.Close()
	require.True(t, inner.Closed)
	require.Equal(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

	// Make sure it's safe to close either consumer a second time.
	consumer1.Close()
	consumer2.Close()
	require.Equal(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
}

func TestOperator_ClosedWithBufferedData(t *testing.T) {
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(0, nil)
	inner, expectedData := createTestOperator(t, 3, memoryConsumptionTracker)

	buffer := NewDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()
	ctx := context.Background()

	metadata1, err := consumer1.SeriesMetadata(ctx)
	require.NoError(t, err)
	metadata2, err := consumer2.SeriesMetadata(ctx)
	require.NoError(t, err)
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.Series), metadata1, "first consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.Series), metadata2, "second consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(metadata1, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(metadata2, memoryConsumptionTracker)

	// Read some data for the first consumer and ensure that it was buffered for the second consumer.
	d, err := consumer1.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[0], d)
	require.Equal(t, 1, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	d, err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[1], d)
	require.Equal(t, 2, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	d, err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[2], d)
	require.Equal(t, 3, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	// Close the first consumer, and check the data remains buffered for the second consumer.
	consumer1.Close()
	require.Equal(t, 3, buffer.buffer.Size())

	// Read some of the buffered data.
	d, err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[0], d)
	require.Equal(t, 2, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	// Close the second consumer, and check that the inner operator was closed and all buffered data was released.
	consumer2.Close()
	require.True(t, inner.Closed)
	require.Equal(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

	// Make sure it's safe to close either consumer a second time.
	consumer1.Close()
	consumer2.Close()
	require.Equal(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
}

func TestOperator_Cloning(t *testing.T) {
	series := types.InstantVectorSeriesData{
		Floats: []promql.FPoint{
			{T: 0, F: 0},
			{T: 1, F: 1},
		},
		Histograms: []promql.HPoint{
			{T: 2, H: &histogram.FloatHistogram{Count: 2}},
			{T: 3, H: &histogram.FloatHistogram{Count: 3}},
		},
	}

	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(0, nil)
	inner := &operators.TestOperator{
		Series:                   []labels.Labels{labels.FromStrings(labels.MetricName, "test_series")},
		Data:                     []types.InstantVectorSeriesData{series},
		MemoryConsumptionTracker: memoryConsumptionTracker,
	}

	buffer := NewDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()
	ctx := context.Background()

	// Both consumers should get the same series metadata, but not the same slice.
	metadata1, err := consumer1.SeriesMetadata(ctx)
	require.NoError(t, err)
	metadata2, err := consumer2.SeriesMetadata(ctx)
	require.NoError(t, err)
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.Series), metadata1, "first consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.Series), metadata2, "second consumer should get expected series metadata")
	require.NotSame(t, &metadata1[0], &metadata2[0], "consumers should not share series metadata slices")
	types.SeriesMetadataSlicePool.Put(metadata1, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(metadata2, memoryConsumptionTracker)

	// Both consumers should get the same data, but not the same slice, and not the same histogram instances.
	d1, err := consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d2, err := consumer2.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, series, d1, "first consumer should get expected series data")
	require.Equal(t, series, d2, "second consumer should get expected series data")
	require.NotSame(t, &d1.Floats[0], &d2.Floats[0], "consumers should not share float slices")
	require.NotSame(t, &d1.Histograms[0], &d2.Histograms[0], "consumers should not share histogram slices")
	require.NotSame(t, d1.Histograms[0].H, d2.Histograms[0].H, "consumers should not share first histogram")
	require.NotSame(t, d1.Histograms[1].H, d2.Histograms[1].H, "consumers should not share second histogram")
}

func createTestOperator(t *testing.T, seriesCount int, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (*operators.TestOperator, []types.InstantVectorSeriesData) {
	series := make([]labels.Labels, 0, seriesCount)
	operatorData := make([]types.InstantVectorSeriesData, 0, seriesCount)
	expectedData := make([]types.InstantVectorSeriesData, 0, seriesCount)

	for i := range seriesCount {
		series = append(series, labels.FromStrings("idx", strconv.Itoa(i)))

		f, err := types.FPointSlicePool.Get(1, memoryConsumptionTracker)
		require.NoError(t, err)

		f = append(f, promql.FPoint{
			T: 0,
			F: float64(i),
		})

		operatorData = append(operatorData, types.InstantVectorSeriesData{
			Floats: f,
		})

		// Create a second slice with the same data that does not use pooled slices, so we can check the returned data in the test.
		expectedData = append(expectedData, types.InstantVectorSeriesData{
			Floats: []promql.FPoint{
				f[0],
			},
		})
	}

	return &operators.TestOperator{
		Series:                   series,
		Data:                     operatorData,
		MemoryConsumptionTracker: memoryConsumptionTracker,
	}, expectedData
}

func TestOperator_ClosingAfterFirstReadFails(t *testing.T) {
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(0, nil)
	series, err := types.SeriesMetadataSlicePool.Get(1, memoryConsumptionTracker)
	require.NoError(t, err)

	series = append(series, types.SeriesMetadata{Labels: labels.FromStrings(labels.MetricName, "test_series")})

	buffer := NewDuplicationBuffer(&failingOperator{series: series}, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()
	ctx := context.Background()

	metadata1, err := consumer1.SeriesMetadata(ctx)
	require.NoError(t, err)
	require.Equal(t, series, metadata1, "first consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(metadata1, memoryConsumptionTracker)

	data, err := consumer1.NextSeries(ctx)
	require.EqualError(t, err, "something went wrong reading data")
	require.Equal(t, types.InstantVectorSeriesData{}, data)

	consumer2.Close()
	consumer1.Close()
	require.Equal(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
}

func TestOperator_ClosingAfterSubsequentReadFails(t *testing.T) {
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(0, nil)
	series, err := types.SeriesMetadataSlicePool.Get(2, memoryConsumptionTracker)
	require.NoError(t, err)

	series = append(series, types.SeriesMetadata{Labels: labels.FromStrings(labels.MetricName, "test_series_1")})
	series = append(series, types.SeriesMetadata{Labels: labels.FromStrings(labels.MetricName, "test_series_2")})

	buffer := NewDuplicationBuffer(&failingOperator{series: series, returnErrorAtSeriesIdx: 1}, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()
	ctx := context.Background()

	metadata1, err := consumer1.SeriesMetadata(ctx)
	require.NoError(t, err)
	require.Equal(t, series, metadata1, "first consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(metadata1, memoryConsumptionTracker)

	data, err := consumer1.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, types.InstantVectorSeriesData{}, data)

	data, err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, types.InstantVectorSeriesData{}, data)

	data, err = consumer1.NextSeries(ctx)
	require.EqualError(t, err, "something went wrong reading data")
	require.Equal(t, types.InstantVectorSeriesData{}, data)

	consumer2.Close()
	consumer1.Close()
	require.Equal(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
}

type failingOperator struct {
	series                 []types.SeriesMetadata
	returnErrorAtSeriesIdx int

	seriesRead int
}

func (o *failingOperator) SeriesMetadata(_ context.Context) ([]types.SeriesMetadata, error) {
	return o.series, nil
}

func (o *failingOperator) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if o.seriesRead >= o.returnErrorAtSeriesIdx {
		return types.InstantVectorSeriesData{}, errors.New("something went wrong reading data")
	}

	o.seriesRead++
	return types.InstantVectorSeriesData{}, nil
}

func (o *failingOperator) ExpressionPosition() posrange.PositionRange {
	return posrange.PositionRange{}
}

func (o *failingOperator) Close() {
	// Nothing to do.
}
