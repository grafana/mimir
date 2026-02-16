// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/prometheus/common/model"
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

func TestInstantVectorOperator_Buffering_NoFiltering(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	inner, expectedData := createTestInstantVectorOperator(t, 6, memoryConsumptionTracker)

	buffer := NewInstantVectorDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()

	// Both consumers should get the same series metadata.
	metadata1, err := consumer1.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	metadata2, err := consumer2.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.Series), metadata1, "first consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.Series), metadata2, "second consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata1, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&metadata2, memoryConsumptionTracker)

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

	// Check that the inner operator hasn't been closed or finalized yet.
	require.False(t, inner.Finalized)
	require.False(t, inner.Closed)

	// Finalize each consumer, and check that the inner operator was only finalized after the last consumer is finalized.
	require.NoError(t, consumer1.Finalize(ctx))
	require.False(t, inner.Finalized)
	require.NoError(t, consumer2.Finalize(ctx))
	require.True(t, inner.Finalized)
	require.NoError(t, consumer1.Finalize(ctx), "it should be safe to finalize either consumer a second time")
	require.NoError(t, consumer2.Finalize(ctx), "it should be safe to finalize either consumer a second time")

	// Close the second consumer, and check that the inner operator was closed.
	consumer2.Close()
	require.True(t, inner.Closed)
	requireNoMemoryConsumption(t, memoryConsumptionTracker)

	// Make sure it's safe to close either consumer a second time.
	consumer1.Close()
	consumer2.Close()
	requireNoMemoryConsumption(t, memoryConsumptionTracker)
}

func TestInstantVectorOperator_Buffering_Filtering_AllConsumersOpen(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	inner, expectedData := createTestInstantVectorOperator(t, 6, memoryConsumptionTracker)

	buffer := NewInstantVectorDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()
	consumer2.SetFilters([]*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "idx", "1|2|5")})

	metadata1, err := consumer1.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	metadata2, err := consumer2.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.Series), metadata1, "first consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata([]labels.Labels{inner.Series[1], inner.Series[2], inner.Series[5]}), metadata2, "second consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata1, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&metadata2, memoryConsumptionTracker)

	// Read some data from the first consumer and ensure that it was buffered for the second consumer, if the second consumer needs that series.
	d, err := consumer1.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[0], d)
	require.Equal(t, 0, buffer.buffer.Size())
	require.Equal(t, 0, cap(buffer.buffer.elements), "should not temporarily buffer data that won't be read by another consumer")
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	d, err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[1], d)
	require.Equal(t, 1, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	// Read the same data from the second consumer, and then keep reading data beyond what has already been buffered.
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
	require.Equal(t, expectedData[5], d)
	require.Equal(t, 4, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	// Read the buffered data for the first consumer.
	d, err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[2], d)
	require.Equal(t, 3, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	// Close the first consumer, check that the data that was being buffered for it is released.
	consumer1.Close()
	require.Equal(t, 0, buffer.buffer.Size())

	// Check that the inner operator hasn't been closed or finalized yet.
	require.False(t, inner.Finalized)
	require.False(t, inner.Closed)

	// Finalize each consumer, and check that the inner operator was only finalized after the last consumer is finalized.
	require.NoError(t, consumer1.Finalize(ctx))
	require.False(t, inner.Finalized)
	require.NoError(t, consumer2.Finalize(ctx))
	require.True(t, inner.Finalized)
	require.NoError(t, consumer1.Finalize(ctx), "it should be safe to finalize either consumer a second time")
	require.NoError(t, consumer2.Finalize(ctx), "it should be safe to finalize either consumer a second time")

	// Close the second consumer, and check that the inner operator was closed.
	consumer2.Close()
	require.True(t, inner.Closed)
	requireNoMemoryConsumption(t, memoryConsumptionTracker)

	// Make sure it's safe to close either consumer a second time.
	consumer1.Close()
	consumer2.Close()
	requireNoMemoryConsumption(t, memoryConsumptionTracker)
}

func TestInstantVectorOperator_Buffering_Filtering_IteratingBeforeCallingSeriesMetadataOnAllConsumers(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	inner, expectedData := createTestInstantVectorOperator(t, 6, memoryConsumptionTracker)

	buffer := NewInstantVectorDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()
	consumer2.SetFilters([]*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "idx", "1|2|5")})

	metadata1, err := consumer1.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.Series), metadata1, "first consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata1, memoryConsumptionTracker)

	// Read some data from the first consumer and ensure that it was buffered for the second consumer, if the second consumer needs that series.
	d, err := consumer1.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[0], d)
	require.Equal(t, 0, buffer.buffer.Size())
	require.Equal(t, 0, cap(buffer.buffer.elements), "should not temporarily buffer data that won't be read by another consumer")
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	d, err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[1], d)
	require.Equal(t, 1, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	// Read the same data from the second consumer, and then keep reading data beyond what has already been buffered.
	metadata2, err := consumer2.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, testutils.LabelsToSeriesMetadata([]labels.Labels{inner.Series[1], inner.Series[2], inner.Series[5]}), metadata2, "second consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata2, memoryConsumptionTracker)

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
	require.Equal(t, expectedData[5], d)
	require.Equal(t, 4, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	// Close the first consumer, check that the data that was being buffered for it is released.
	consumer1.Close()
	require.Equal(t, 0, buffer.buffer.Size())

	// Check that the inner operator hasn't been closed or finalized yet.
	require.False(t, inner.Finalized)
	require.False(t, inner.Closed)

	// Finalize each consumer, and check that the inner operator was only finalized after the last consumer is finalized.
	require.NoError(t, consumer1.Finalize(ctx))
	require.False(t, inner.Finalized)
	require.NoError(t, consumer2.Finalize(ctx))
	require.True(t, inner.Finalized)
	require.NoError(t, consumer1.Finalize(ctx), "it should be safe to finalize either consumer a second time")
	require.NoError(t, consumer2.Finalize(ctx), "it should be safe to finalize either consumer a second time")

	// Close the second consumer, and check that the inner operator was closed.
	consumer2.Close()
	require.True(t, inner.Closed)
	requireNoMemoryConsumption(t, memoryConsumptionTracker)

	// Make sure it's safe to close either consumer a second time.
	consumer1.Close()
	consumer2.Close()
	requireNoMemoryConsumption(t, memoryConsumptionTracker)
}

func TestInstantVectorOperator_Buffering_Filtering_DoesNotBufferForClosedConsumer(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	inner, expectedData := createTestInstantVectorOperator(t, 6, memoryConsumptionTracker)

	buffer := NewInstantVectorDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()
	consumer2.SetFilters([]*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "idx", "1|2|5")})

	// Both consumers should get the same series metadata.
	metadata1, err := consumer1.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	metadata2, err := consumer2.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.Series), metadata1, "first consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata([]labels.Labels{inner.Series[1], inner.Series[2], inner.Series[5]}), metadata2, "second consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata1, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&metadata2, memoryConsumptionTracker)

	// Read some data from the second consumer, which will cause the first two series to be buffered for the first consumer.
	d, err := consumer2.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[1], d)
	require.Equal(t, 2, buffer.buffer.Size(), "the first and second series should be buffered for the first consumer")
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	// Finalize and close the first consumer, check that the data that was being buffered for it is released.
	require.NoError(t, consumer1.Finalize(ctx))
	consumer1.Close()
	require.Equal(t, 0, buffer.buffer.Size())

	// Check that the inner operator hasn't been closed or finalized yet.
	require.False(t, inner.Finalized)
	require.False(t, inner.Closed)

	// Keep reading data for the second consumer, confirm that no further data is buffered for the first consumer.
	d, err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[2], d)
	require.Equal(t, 0, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	d, err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[5], d)
	require.Equal(t, 0, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	// Finalize each consumer, and check that the inner operator was only finalized after the last consumer is finalized.
	require.NoError(t, consumer2.Finalize(ctx))
	require.True(t, inner.Finalized)
	require.NoError(t, consumer1.Finalize(ctx), "it should be safe to finalize either consumer a second time")
	require.NoError(t, consumer2.Finalize(ctx), "it should be safe to finalize either consumer a second time")

	// Close the second consumer, and check that the inner operator was closed.
	consumer2.Close()
	require.True(t, inner.Closed)
	requireNoMemoryConsumption(t, memoryConsumptionTracker)

	// Make sure it's safe to close either consumer a second time.
	consumer1.Close()
	consumer2.Close()
	requireNoMemoryConsumption(t, memoryConsumptionTracker)
}

func TestInstantVectorOperator_Buffering_Filtering_DoesNotBufferUnnecessarilyForLaggingConsumer(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	inner, expectedData := createTestInstantVectorOperator(t, 3, memoryConsumptionTracker)

	buffer := NewInstantVectorDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer1.SetFilters([]*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "idx", "0")})
	consumer2 := buffer.AddConsumer()
	consumer2.SetFilters([]*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "idx", "1|2")})

	metadata1, err := consumer1.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	metadata2, err := consumer2.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, testutils.LabelsToSeriesMetadata([]labels.Labels{inner.Series[0]}), metadata1, "first consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata([]labels.Labels{inner.Series[1], inner.Series[2]}), metadata2, "second consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata1, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&metadata2, memoryConsumptionTracker)

	// Read first series for the second consumer (the second underlying series).
	// The first series should be buffered for the first consumer, but not the second.
	d, err := consumer2.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[1], d)
	require.Equal(t, 1, buffer.buffer.Size(), "only the first series should be buffered for the first consumer")
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	d, err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[2], d)
	require.Equal(t, 1, buffer.buffer.Size(), "only the first series should be buffered for the first consumer")
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	// Finalize and close the first consumer, check that the data that was being buffered for it is released.
	require.NoError(t, consumer1.Finalize(ctx))
	consumer1.Close()
	require.Equal(t, 0, buffer.buffer.Size())

	// Check that the inner operator hasn't been closed or finalized yet.
	require.False(t, inner.Finalized)
	require.False(t, inner.Closed)

	// And the same for the second consumer.
	require.NoError(t, consumer2.Finalize(ctx))
	consumer2.Close()
	require.True(t, inner.Finalized)
	require.True(t, inner.Closed)
	requireNoMemoryConsumption(t, memoryConsumptionTracker)
}

func TestInstantVectorOperator_Buffering_NonContiguousSeries(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	inner, expectedData := createTestInstantVectorOperator(t, 4, memoryConsumptionTracker)

	buffer := NewInstantVectorDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()
	consumer2.SetFilters([]*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "idx", "0")})
	consumer3 := buffer.AddConsumer()
	consumer3.SetFilters([]*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "idx", "1|3")})

	metadata1, err := consumer1.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	metadata2, err := consumer2.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	metadata3, err := consumer3.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.Series), metadata1, "first consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata([]labels.Labels{inner.Series[0]}), metadata2, "second consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata([]labels.Labels{inner.Series[1], inner.Series[3]}), metadata3, "third consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata1, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&metadata2, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&metadata3, memoryConsumptionTracker)

	// Read all four series from the unfiltered consumer.
	// Only those series needed for the other consumers should be buffered.
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
	require.Equal(t, 2, buffer.buffer.Size(), "should not buffer series at index 2, as it's not needed by any other consumer")
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	d, err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[3], d)
	require.Equal(t, 3, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	require.NoError(t, consumer1.Finalize(ctx))
	consumer1.Close()
	require.False(t, inner.Finalized)
	require.False(t, inner.Closed)

	// Read all the data from consumer 3.
	d, err = consumer3.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[1], d)
	require.Equal(t, 3, buffer.buffer.Size(), "buffer size should be unchanged as series at index 1 should be tombstoned")
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	d, err = consumer3.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[3], d)
	require.Equal(t, 1, buffer.buffer.Size(), "should only be buffering series required by consumer 2")
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	require.NoError(t, consumer3.Finalize(ctx))
	consumer3.Close()
	require.False(t, inner.Finalized)
	require.False(t, inner.Closed)

	// Read all the data from consumer 2
	d, err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[0], d)
	require.Equal(t, 0, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	// Make sure everything is cleaned up properly.
	require.False(t, inner.Finalized)
	require.False(t, inner.Closed)
	require.NoError(t, consumer2.Finalize(ctx))
	require.True(t, inner.Finalized)
	consumer2.Close()
	require.True(t, inner.Closed)
	requireNoMemoryConsumption(t, memoryConsumptionTracker)
}

func TestInstantVectorOperator_ClosedWithBufferedData_NoFiltering(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	inner, expectedData := createTestInstantVectorOperator(t, 3, memoryConsumptionTracker)

	buffer := NewInstantVectorDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()

	metadata1, err := consumer1.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	metadata2, err := consumer2.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.Series), metadata1, "first consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.Series), metadata2, "second consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata2, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&metadata1, memoryConsumptionTracker)

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
	requireNoMemoryConsumption(t, memoryConsumptionTracker)

	// Make sure it's safe to close either consumer a second time.
	consumer1.Close()
	consumer2.Close()
	requireNoMemoryConsumption(t, memoryConsumptionTracker)
}

func TestInstantVectorOperator_ClosedWithBufferedData_Filtering(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	inner, expectedData := createTestInstantVectorOperator(t, 3, memoryConsumptionTracker)

	buffer := NewInstantVectorDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()
	consumer2.SetFilters([]*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "idx", "0|2")})
	consumer3 := buffer.AddConsumer()
	consumer3.SetFilters([]*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "idx", "1")})

	metadata1, err := consumer1.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	metadata2, err := consumer2.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	metadata3, err := consumer3.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.Series), metadata1, "first consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata([]labels.Labels{inner.Series[0], inner.Series[2]}), metadata2, "second consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata([]labels.Labels{inner.Series[1]}), metadata3, "third consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata1, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&metadata2, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&metadata3, memoryConsumptionTracker)

	// Read each series from the first consumer, filling the buffer for the other consumers.
	for idx := range 3 {
		d, err := consumer1.NextSeries(ctx)
		require.NoError(t, err)
		require.Equal(t, expectedData[idx], d)
		require.Equal(t, idx+1, buffer.buffer.Size())
		types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)
	}

	require.Equal(t, 3, buffer.buffer.Size(), "buffer should contain all three series for the remaining two consumers")
	consumer2.Close()
	require.Equal(t, 1, buffer.buffer.Size(), "buffer should only contain remaining series required by remaining consumer")

	d, err := consumer3.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData[1], d)
	require.Equal(t, 0, buffer.buffer.Size())
	types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

	consumer1.Close()
	consumer3.Close()
	requireNoMemoryConsumption(t, memoryConsumptionTracker)
}

func TestInstantVectorOperator_Cloning(t *testing.T) {
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
	inner := &operators.TestOperator{
		Series:                   []labels.Labels{labels.FromStrings(model.MetricNameLabel, "test_series")},
		Data:                     []types.InstantVectorSeriesData{series},
		MemoryConsumptionTracker: memoryConsumptionTracker,
	}

	buffer := NewInstantVectorDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()

	// Both consumers should get the same series metadata, but not the same slice.
	metadata1, err := consumer1.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	metadata2, err := consumer2.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.Series), metadata1, "first consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.Series), metadata2, "second consumer should get expected series metadata")
	require.NotSame(t, &metadata1[0], &metadata2[0], "consumers should not share series metadata slices")
	types.SeriesMetadataSlicePool.Put(&metadata1, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&metadata2, memoryConsumptionTracker)

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

func createTestInstantVectorOperator(t *testing.T, seriesCount int, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (*operators.TestOperator, []types.InstantVectorSeriesData) {
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

func TestInstantVectorOperator_ClosingAfterFirstReadFails(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	series, err := types.SeriesMetadataSlicePool.Get(1, memoryConsumptionTracker)
	require.NoError(t, err)

	series, err = types.AppendSeriesMetadata(memoryConsumptionTracker, series, types.SeriesMetadata{Labels: labels.FromStrings(model.MetricNameLabel, "test_series")})
	require.NoError(t, err)

	buffer := NewInstantVectorDuplicationBuffer(&failingInstantVectorOperator{series: series}, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()

	metadata1, err := consumer1.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, series, metadata1, "first consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata1, memoryConsumptionTracker)

	data, err := consumer1.NextSeries(ctx)
	require.EqualError(t, err, "something went wrong reading data")
	require.Equal(t, types.InstantVectorSeriesData{}, data)

	consumer2.Close()
	consumer1.Close()
	requireNoMemoryConsumption(t, memoryConsumptionTracker)
}

func TestInstantVectorOperator_ClosingAfterSubsequentReadFails(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	series, err := types.SeriesMetadataSlicePool.Get(2, memoryConsumptionTracker)
	require.NoError(t, err)

	series, err = types.AppendSeriesMetadata(memoryConsumptionTracker, series, types.SeriesMetadata{Labels: labels.FromStrings(model.MetricNameLabel, "test_series_1")})
	require.NoError(t, err)
	series, err = types.AppendSeriesMetadata(memoryConsumptionTracker, series, types.SeriesMetadata{Labels: labels.FromStrings(model.MetricNameLabel, "test_series_2")})
	require.NoError(t, err)

	buffer := NewInstantVectorDuplicationBuffer(&failingInstantVectorOperator{series: series, returnErrorAtSeriesIdx: 1}, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()

	metadata1, err := consumer1.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, series, metadata1, "first consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata1, memoryConsumptionTracker)

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
	requireNoMemoryConsumption(t, memoryConsumptionTracker)
}

type failingInstantVectorOperator struct {
	series                 []types.SeriesMetadata
	returnErrorAtSeriesIdx int

	seriesRead int
}

func (o *failingInstantVectorOperator) SeriesMetadata(_ context.Context, _ types.Matchers) ([]types.SeriesMetadata, error) {
	return o.series, nil
}

func (o *failingInstantVectorOperator) NextSeries(_ context.Context) (types.InstantVectorSeriesData, error) {
	if o.seriesRead >= o.returnErrorAtSeriesIdx {
		return types.InstantVectorSeriesData{}, errors.New("something went wrong reading data")
	}

	o.seriesRead++
	return types.InstantVectorSeriesData{}, nil
}

func (o *failingInstantVectorOperator) ExpressionPosition() posrange.PositionRange {
	return posrange.PositionRange{}
}

func (o *failingInstantVectorOperator) Prepare(_ context.Context, _ *types.PrepareParams) error {
	// Nothing to do.
	return nil
}

func (o *failingInstantVectorOperator) AfterPrepare(ctx context.Context) error {
	return nil
}

func (o *failingInstantVectorOperator) Finalize(_ context.Context) error {
	return nil
}

func (o *failingInstantVectorOperator) Close() {
	// Nothing to do.
}
