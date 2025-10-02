// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestRangeVectorOperator_Buffering(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	inner := createTestRangeVectorOperator(t, 6, memoryConsumptionTracker)
	expectedData := inner.data

	buffer := NewRangeVectorDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()

	// Both consumers should get the same series metadata.
	metadata1, err := consumer1.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	metadata2, err := consumer2.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.series), metadata1, "first consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.series), metadata2, "second consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata1, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&metadata2, memoryConsumptionTracker)

	// Read some data from the first consumer and ensure that it was buffered for the second consumer.
	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d, err := consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualData(t, expectedData[0], d, memoryConsumptionTracker)
	require.Equal(t, 1, buffer.buffer.Size())

	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualData(t, expectedData[1], d, memoryConsumptionTracker)
	require.Equal(t, 2, buffer.buffer.Size())

	// Read the same data from the second consumer, and then keep reading data beyond what has already been buffered.
	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualData(t, expectedData[0], d, memoryConsumptionTracker)
	require.Equal(t, 2, buffer.buffer.Size(), "buffered data should remain buffered until the next NextSeries or Close call")

	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualData(t, expectedData[1], d, memoryConsumptionTracker)
	require.Equal(t, 1, buffer.buffer.Size())

	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualData(t, expectedData[2], d, memoryConsumptionTracker)
	require.Equal(t, 1, buffer.buffer.Size())

	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualData(t, expectedData[3], d, memoryConsumptionTracker)
	require.Equal(t, 2, buffer.buffer.Size())

	// Read some of the buffered data in the first consumer.
	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualData(t, expectedData[2], d, memoryConsumptionTracker)
	require.Equal(t, 2, buffer.buffer.Size(), "buffered data should remain buffered until the next NextSeries or Close call")

	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualData(t, expectedData[4], d, memoryConsumptionTracker)
	require.Equal(t, 2, buffer.buffer.Size())

	// Read some more of the buffered data in the first consumer.
	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualData(t, expectedData[3], d, memoryConsumptionTracker)
	require.Equal(t, 2, buffer.buffer.Size(), "buffered data should remain buffered until the next NextSeries or Close call")

	// Close the first consumer, check that the data that was being buffered for it is released.
	consumer1.Close()
	require.Equal(t, 0, buffer.buffer.Size())

	// Check that the second consumer can still read data and that we don't bother buffering it.
	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualData(t, expectedData[5], d, memoryConsumptionTracker)
	require.Equal(t, 0, buffer.buffer.Size())

	// Check that the inner operator hasn't been closed or finalized yet.
	require.False(t, inner.finalized)
	require.False(t, inner.closed)

	// Finalize each consumer, and check that the inner operator was only finalized after the last consumer is finalized.
	require.NoError(t, consumer1.Finalize(ctx))
	require.False(t, inner.finalized)
	require.NoError(t, consumer2.Finalize(ctx))
	require.True(t, inner.finalized)
	require.NoError(t, consumer1.Finalize(ctx), "it should be safe to finalize either consumer a second time")
	require.NoError(t, consumer2.Finalize(ctx), "it should be safe to finalize either consumer a second time")

	// Close the second consumer, and check that the inner operator was closed.
	consumer2.Close()
	require.True(t, inner.closed)
	requireNoMemoryConsumption(t, memoryConsumptionTracker)

	// Make sure it's safe to close either consumer a second time.
	consumer1.Close()
	consumer2.Close()
	requireNoMemoryConsumption(t, memoryConsumptionTracker)
}

func requireNoMemoryConsumption(t *testing.T, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	require.Equalf(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(), "expected all instances to be returned to pool, current memory consumption is:\n%v", memoryConsumptionTracker.DescribeCurrentMemoryConsumption())
}

func TestRangeVectorOperator_ClosedWithBufferedData(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	inner := createTestRangeVectorOperator(t, 3, memoryConsumptionTracker)
	expectedData := inner.data

	buffer := NewRangeVectorDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()

	metadata1, err := consumer1.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	metadata2, err := consumer2.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.series), metadata1, "first consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.series), metadata2, "second consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata2, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&metadata1, memoryConsumptionTracker)

	// Read some data for the first consumer and ensure that it was buffered for the second consumer.
	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d, err := consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualData(t, expectedData[0], d, memoryConsumptionTracker)
	require.Equal(t, 1, buffer.buffer.Size())

	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualData(t, expectedData[1], d, memoryConsumptionTracker)
	require.Equal(t, 2, buffer.buffer.Size())

	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualData(t, expectedData[2], d, memoryConsumptionTracker)
	require.Equal(t, 3, buffer.buffer.Size())

	// Close the first consumer, and check the data remains buffered for the second consumer.
	consumer1.Close()
	require.Equal(t, 3, buffer.buffer.Size())

	// Read some of the buffered data.
	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualData(t, expectedData[0], d, memoryConsumptionTracker)
	require.Equal(t, 3, buffer.buffer.Size(), "buffered data should remain in the buffer until the next NextSeries or Close call")

	// Close the second consumer, and check that the inner operator was closed and all buffered data was released.
	consumer2.Close()
	require.True(t, inner.closed)
	requireNoMemoryConsumption(t, memoryConsumptionTracker)

	// Make sure it's safe to close either consumer a second time.
	consumer1.Close()
	consumer2.Close()
	requireNoMemoryConsumption(t, memoryConsumptionTracker)
}

func TestRangeVectorOperator_Cloning(t *testing.T) {
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

	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	inner := &testRangeVectorOperator{
		series:                   []labels.Labels{labels.FromStrings(labels.MetricName, "test_series")},
		data:                     []types.InstantVectorSeriesData{series},
		stepRange:                time.Minute,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}

	buffer := NewRangeVectorDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()

	// Both consumers should get the same series metadata, but not the same slice.
	metadata1, err := consumer1.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	metadata2, err := consumer2.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.series), metadata1, "first consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.series), metadata2, "second consumer should get expected series metadata")
	require.NotSame(t, &metadata1[0], &metadata2[0], "consumers should not share series metadata slices")
	types.SeriesMetadataSlicePool.Put(&metadata1, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&metadata2, memoryConsumptionTracker)

	// Both consumers should get the same ring buffer views.
	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d1, err := consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d2, err := consumer2.NextStepSamples(ctx)
	require.NoError(t, err)

	require.Equal(t, d1, d2, "both consumers should get same data")
	require.Same(t, d1.Floats, d2.Floats, "both consumers should get same float ring buffer view instance")
	require.Same(t, d1.Histograms, d2.Histograms, "both consumers should get same histogram ring buffer view instance")
	require.NotSame(t, inner.floatsView, d1.Floats, "both consumers should get a cloned view of the floats ring buffer")
	require.NotSame(t, inner.histogramsView, d1.Histograms, "both consumers should get a cloned view of the histograms ring buffer")

	requireEqualData(t, series, d1, memoryConsumptionTracker)
}

func requireEqualData(t *testing.T, expected types.InstantVectorSeriesData, actual *types.RangeVectorStepData, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	actualFloats, err := actual.Floats.CopyPoints()
	require.NoError(t, err)
	require.Equal(t, expected.Floats, actualFloats)
	types.FPointSlicePool.Put(&actualFloats, memoryConsumptionTracker)

	actualHistograms, err := actual.Histograms.CopyPoints()
	require.NoError(t, err)
	require.Equal(t, expected.Histograms, actualHistograms)
	types.HPointSlicePool.Put(&actualHistograms, memoryConsumptionTracker)
}

func createTestRangeVectorOperator(t *testing.T, seriesCount int, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *testRangeVectorOperator {
	series := make([]labels.Labels, 0, seriesCount)
	data := make([]types.InstantVectorSeriesData, 0, seriesCount)

	for i := range seriesCount {
		series = append(series, labels.FromStrings("idx", strconv.Itoa(i)))

		data = append(data, types.InstantVectorSeriesData{
			Floats: []promql.FPoint{
				{T: 0, F: float64(i)},
			},
		})
	}

	return &testRangeVectorOperator{
		series:                   series,
		data:                     data,
		stepRange:                time.Minute,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
}

func TestRangeVectorOperator_ClosingAfterFirstReadFails(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	series, err := types.SeriesMetadataSlicePool.Get(1, memoryConsumptionTracker)
	require.NoError(t, err)

	series, err = types.AppendSeriesMetadata(memoryConsumptionTracker, series, types.SeriesMetadata{Labels: labels.FromStrings(labels.MetricName, "test_series")})
	require.NoError(t, err)

	inner := &failingRangeVectorOperator{series: series, memoryConsumptionTracker: memoryConsumptionTracker}
	buffer := NewRangeVectorDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()

	metadata1, err := consumer1.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, series, metadata1, "first consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata1, memoryConsumptionTracker)

	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	data, err := consumer1.NextStepSamples(ctx)
	require.EqualError(t, err, "something went wrong reading data")
	require.Nil(t, data)

	consumer2.Close()
	consumer1.Close()
	requireNoMemoryConsumption(t, memoryConsumptionTracker)
}

func TestRangeVectorOperator_ClosingAfterSubsequentReadFails(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	series, err := types.SeriesMetadataSlicePool.Get(2, memoryConsumptionTracker)
	require.NoError(t, err)

	series, err = types.AppendSeriesMetadata(memoryConsumptionTracker, series, types.SeriesMetadata{Labels: labels.FromStrings(labels.MetricName, "test_series_1")})
	require.NoError(t, err)
	series, err = types.AppendSeriesMetadata(memoryConsumptionTracker, series, types.SeriesMetadata{Labels: labels.FromStrings(labels.MetricName, "test_series_2")})
	require.NoError(t, err)

	inner := &failingRangeVectorOperator{series: series, returnErrorAtSeriesIdx: 1, memoryConsumptionTracker: memoryConsumptionTracker}
	buffer := NewRangeVectorDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()

	metadata1, err := consumer1.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, series, metadata1, "first consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata1, memoryConsumptionTracker)

	// Read the data for the first series for both consumers.
	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	data, err := consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	require.Equal(t, data.Floats.First(), promql.FPoint{T: 0, F: 1234})
	require.Equal(t, data.Histograms.First(), promql.HPoint{T: 500, H: &histogram.FloatHistogram{Count: 100, Sum: 2}})

	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	data, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	require.Equal(t, data.Floats.First(), promql.FPoint{T: 0, F: 1234})
	require.Equal(t, data.Histograms.First(), promql.HPoint{T: 500, H: &histogram.FloatHistogram{Count: 100, Sum: 2}})

	// Try reading the next series, which should fail.
	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	data, err = consumer1.NextStepSamples(ctx)
	require.EqualError(t, err, "something went wrong reading data")
	require.Nil(t, data)

	// Close everything and confirm everything was returned to the pool correctly.
	consumer2.Close()
	consumer1.Close()
	requireNoMemoryConsumption(t, memoryConsumptionTracker)
}

type testRangeVectorOperator struct {
	series                     []labels.Labels
	data                       []types.InstantVectorSeriesData
	stepRange                  time.Duration
	haveReadCurrentStepSamples bool
	floats                     *types.FPointRingBuffer
	floatsView                 *types.FPointRingBufferView
	histograms                 *types.HPointRingBuffer
	histogramsView             *types.HPointRingBufferView

	finalized                bool
	closed                   bool
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

func (t *testRangeVectorOperator) SeriesMetadata(_ context.Context, _ types.Matchers) ([]types.SeriesMetadata, error) {
	if len(t.series) == 0 {
		return nil, nil
	}

	metadata, err := types.SeriesMetadataSlicePool.Get(len(t.series), t.memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	metadata = metadata[:len(t.series)]

	for i, l := range t.series {
		metadata[i].Labels = l
		err := t.memoryConsumptionTracker.IncreaseMemoryConsumptionForLabels(l)
		if err != nil {
			return nil, err
		}
	}

	return metadata, nil
}

func (t *testRangeVectorOperator) NextSeries(_ context.Context) error {
	if len(t.data) == 0 {
		return types.EOS
	}

	t.haveReadCurrentStepSamples = false

	return nil
}

func (t *testRangeVectorOperator) NextStepSamples(_ context.Context) (*types.RangeVectorStepData, error) {
	if t.haveReadCurrentStepSamples {
		return nil, types.EOS
	}

	t.haveReadCurrentStepSamples = true

	if t.floats == nil {
		t.floats = types.NewFPointRingBuffer(t.memoryConsumptionTracker)
	}

	if t.histograms == nil {
		t.histograms = types.NewHPointRingBuffer(t.memoryConsumptionTracker)
	}

	d := t.data[0]
	t.data = t.data[1:]
	endT := t.stepRange.Milliseconds()

	t.floats.Reset()
	for _, p := range d.Floats {
		if err := t.floats.Append(p); err != nil {
			return nil, err
		}
	}
	t.floatsView = t.floats.ViewUntilSearchingBackwards(endT, t.floatsView)

	t.histograms.Reset()
	for _, p := range d.Histograms {
		if err := t.histograms.Append(p); err != nil {
			return nil, err
		}
	}
	t.histogramsView = t.histograms.ViewUntilSearchingBackwards(endT, t.histogramsView)

	return &types.RangeVectorStepData{
		Floats:     t.floatsView,
		Histograms: t.histogramsView,
		StepT:      endT,
		RangeStart: 0,
		RangeEnd:   endT,
	}, nil
}

func (t *testRangeVectorOperator) ExpressionPosition() posrange.PositionRange {
	return posrange.PositionRange{}
}

func (t *testRangeVectorOperator) Prepare(_ context.Context, _ *types.PrepareParams) error {
	// Nothing to do.
	return nil
}

func (t *testRangeVectorOperator) Finalize(_ context.Context) error {
	t.finalized = true
	return nil
}

func (t *testRangeVectorOperator) Close() {
	t.closed = true

	if t.floats != nil {
		t.floats.Close()
	}

	if t.histograms != nil {
		t.histograms.Close()
	}

	t.floats = nil
	t.floatsView = nil
	t.histograms = nil
	t.histogramsView = nil
}

type failingRangeVectorOperator struct {
	series                   []types.SeriesMetadata
	returnErrorAtSeriesIdx   int
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker

	floats     *types.FPointRingBuffer
	floatsView *types.FPointRingBufferView

	histograms     *types.HPointRingBuffer
	histogramsView *types.HPointRingBufferView

	seriesRead int
}

func (o *failingRangeVectorOperator) SeriesMetadata(_ context.Context, _ types.Matchers) ([]types.SeriesMetadata, error) {
	return o.series, nil
}

func (o *failingRangeVectorOperator) NextSeries(_ context.Context) error {
	o.seriesRead++
	return nil
}

func (o *failingRangeVectorOperator) NextStepSamples(_ context.Context) (*types.RangeVectorStepData, error) {
	if o.seriesRead > o.returnErrorAtSeriesIdx {
		return nil, errors.New("something went wrong reading data")
	}

	if o.floats == nil {
		o.floats = types.NewFPointRingBuffer(o.memoryConsumptionTracker)
		if err := o.floats.Append(promql.FPoint{T: 0, F: 1234}); err != nil {
			return nil, err
		}

		o.floatsView = o.floats.ViewUntilSearchingBackwards(1000, o.floatsView)
	}

	if o.histograms == nil {
		o.histograms = types.NewHPointRingBuffer(o.memoryConsumptionTracker)
		if err := o.histograms.Append(promql.HPoint{T: 500, H: &histogram.FloatHistogram{Count: 100, Sum: 2}}); err != nil {
			return nil, err
		}

		o.histogramsView = o.histograms.ViewUntilSearchingBackwards(1000, o.histogramsView)
	}

	return &types.RangeVectorStepData{
		Floats:     o.floatsView,
		Histograms: o.histogramsView,
		StepT:      1000,
		RangeStart: 0,
		RangeEnd:   1000,
	}, nil
}

func (o *failingRangeVectorOperator) StepCount() int {
	return 0
}

func (o *failingRangeVectorOperator) Range() time.Duration {
	return time.Minute
}

func (o *failingRangeVectorOperator) ExpressionPosition() posrange.PositionRange {
	return posrange.PositionRange{}
}

func (o *failingRangeVectorOperator) Prepare(_ context.Context, _ *types.PrepareParams) error {
	// Nothing to do.
	return nil
}

func (o *failingRangeVectorOperator) Finalize(_ context.Context) error {
	return nil
}

func (o *failingRangeVectorOperator) Close() {
	if o.floats != nil {
		o.floats.Close()
	}

	o.floats = nil
	o.floatsView = nil

	if o.histograms != nil {
		o.histograms.Close()
	}

	o.histograms = nil
	o.histogramsView = nil
}
