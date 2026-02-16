// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"context"
	"errors"
	"math/rand/v2"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestRangeVectorOperator_Buffering_NoFiltering(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	inner, expectedData := createTestRangeVectorOperator(t, 6, memoryConsumptionTracker)

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
	requireEqualDataAndReturnToPool(t, expectedData[0], d, memoryConsumptionTracker)
	require.Equal(t, 1, buffer.buffer.Size())

	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[1], d, memoryConsumptionTracker)
	require.Equal(t, 2, buffer.buffer.Size())

	// Read the same data from the second consumer, and then keep reading data beyond what has already been buffered.
	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[0], d, memoryConsumptionTracker)
	require.Equal(t, 2, buffer.buffer.Size(), "buffered data should remain buffered until the next NextSeries or Close call")

	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, buffer.buffer.Size())
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[1], d, memoryConsumptionTracker)
	require.Equal(t, 1, buffer.buffer.Size())

	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[2], d, memoryConsumptionTracker)
	require.Equal(t, 1, buffer.buffer.Size())

	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[3], d, memoryConsumptionTracker)
	require.Equal(t, 2, buffer.buffer.Size())

	// Read some of the buffered data in the first consumer.
	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[2], d, memoryConsumptionTracker)
	require.Equal(t, 2, buffer.buffer.Size(), "buffered data should remain buffered until the next NextSeries or Close call")

	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, buffer.buffer.Size())
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[4], d, memoryConsumptionTracker)
	require.Equal(t, 2, buffer.buffer.Size())

	// Read some more of the buffered data in the first consumer.
	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[3], d, memoryConsumptionTracker)
	require.Equal(t, 2, buffer.buffer.Size(), "buffered data should remain buffered until the next NextSeries or Close call")

	// Close the first consumer, check that the data that was being buffered for it is released.
	consumer1.Close()
	require.Equal(t, 0, buffer.buffer.Size())

	// Check that the second consumer can still read data and that we don't bother buffering it.
	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[5], d, memoryConsumptionTracker)
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

func TestRangeVectorOperator_Buffering_Filtering_AllConsumersOpen(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	inner, expectedData := createTestRangeVectorOperator(t, 6, memoryConsumptionTracker)

	buffer := NewRangeVectorDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()
	consumer2.SetFilters([]*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "idx", "1|2|5")})

	metadata1, err := consumer1.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	metadata2, err := consumer2.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.series), metadata1, "first consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata([]labels.Labels{inner.series[1], inner.series[2], inner.series[5]}), metadata2, "second consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata1, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&metadata2, memoryConsumptionTracker)

	// Read some data from the first consumer and ensure that it was buffered for the second consumer, if the second consumer needs that series.
	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d, err := consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[0], d, memoryConsumptionTracker)
	require.Equal(t, 0, buffer.buffer.Size())
	require.Equal(t, 0, cap(buffer.buffer.elements), "should not temporarily buffer data that won't be read by another consumer")

	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[1], d, memoryConsumptionTracker)
	require.Equal(t, 1, buffer.buffer.Size())

	// Read the same data from the second consumer, and then keep reading data beyond what has already been buffered.
	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[1], d, memoryConsumptionTracker)
	require.Equal(t, 1, buffer.buffer.Size(), "buffered data should remain buffered until the next NextSeries or Close call")

	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, buffer.buffer.Size())
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[2], d, memoryConsumptionTracker)
	require.Equal(t, 1, buffer.buffer.Size())

	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[5], d, memoryConsumptionTracker)
	require.Equal(t, 4, buffer.buffer.Size())

	// Read the buffered data for the first consumer.
	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[2], d, memoryConsumptionTracker)
	require.Equal(t, 4, buffer.buffer.Size(), "buffered data should remain buffered until the next NextSeries or Close call")

	// Close the first consumer, check that the data that was being buffered for it is released.
	consumer1.Close()
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

func TestRangeVectorOperator_Buffering_Filtering_IteratingBeforeCallingSeriesMetadataOnAllConsumers(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	inner, expectedData := createTestRangeVectorOperator(t, 6, memoryConsumptionTracker)

	buffer := NewRangeVectorDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()
	consumer2.SetFilters([]*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "idx", "1|2|5")})

	metadata1, err := consumer1.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.series), metadata1, "first consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata1, memoryConsumptionTracker)

	// Read some data from the first consumer and ensure that it was buffered for the second consumer, if the second consumer needs that series.
	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d, err := consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[0], d, memoryConsumptionTracker)
	require.Equal(t, 0, buffer.buffer.Size())
	require.Equal(t, 0, cap(buffer.buffer.elements), "should not temporarily buffer data that won't be read by another consumer")

	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[1], d, memoryConsumptionTracker)
	require.Equal(t, 1, buffer.buffer.Size())

	// Read the same data from the second consumer, and then keep reading data beyond what has already been buffered.
	metadata2, err := consumer2.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, testutils.LabelsToSeriesMetadata([]labels.Labels{inner.series[1], inner.series[2], inner.series[5]}), metadata2, "second consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata2, memoryConsumptionTracker)

	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[1], d, memoryConsumptionTracker)
	require.Equal(t, 1, buffer.buffer.Size(), "buffered data should remain buffered until the next NextSeries or Close call")

	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, buffer.buffer.Size())
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[2], d, memoryConsumptionTracker)
	require.Equal(t, 1, buffer.buffer.Size())

	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[5], d, memoryConsumptionTracker)
	require.Equal(t, 4, buffer.buffer.Size())

	// Read the buffered data for the first consumer.
	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[2], d, memoryConsumptionTracker)
	require.Equal(t, 4, buffer.buffer.Size(), "buffered data should remain buffered until the next NextSeries or Close call")

	// Close the first consumer, check that the data that was being buffered for it is released.
	consumer1.Close()
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

func TestRangeVectorOperator_Buffering_Filtering_DoesNotBufferForClosedConsumer(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	inner, expectedData := createTestRangeVectorOperator(t, 6, memoryConsumptionTracker)

	buffer := NewRangeVectorDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()
	consumer2.SetFilters([]*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "idx", "1|2|5")})

	// Both consumers should get the same series metadata.
	metadata1, err := consumer1.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	metadata2, err := consumer2.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.series), metadata1, "first consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata([]labels.Labels{inner.series[1], inner.series[2], inner.series[5]}), metadata2, "second consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata1, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&metadata2, memoryConsumptionTracker)

	// Read some data from the second consumer, which will cause the first two series to be buffered for the first consumer.
	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d, err := consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[1], d, memoryConsumptionTracker)
	require.Equal(t, 2, buffer.buffer.Size(), "the first and second series should be buffered for the first consumer")

	// Finalize and close the first consumer, check that the data that was being buffered for it is released.
	require.NoError(t, consumer1.Finalize(ctx))
	consumer1.Close()
	require.Equal(t, 0, buffer.buffer.Size())

	// Check that the inner operator hasn't been closed or finalized yet.
	require.False(t, inner.finalized)
	require.False(t, inner.closed)

	// Keep reading data for the second consumer, confirm that no further data is buffered for the first consumer.
	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[2], d, memoryConsumptionTracker)
	require.Equal(t, 0, buffer.buffer.Size())

	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[5], d, memoryConsumptionTracker)
	require.Equal(t, 0, buffer.buffer.Size())

	// Finalize each consumer, and check that the inner operator was only finalized after the last consumer is finalized.
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

func TestRangeVectorOperator_Buffering_Filtering_DoesNotBufferUnnecessarilyForLaggingConsumer(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	inner, expectedData := createTestRangeVectorOperator(t, 3, memoryConsumptionTracker)

	buffer := NewRangeVectorDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer1.SetFilters([]*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "idx", "0")})
	consumer2 := buffer.AddConsumer()
	consumer2.SetFilters([]*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "idx", "1|2")})

	metadata1, err := consumer1.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	metadata2, err := consumer2.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, testutils.LabelsToSeriesMetadata([]labels.Labels{inner.series[0]}), metadata1, "first consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata([]labels.Labels{inner.series[1], inner.series[2]}), metadata2, "second consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata1, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&metadata2, memoryConsumptionTracker)

	// Read first series for the second consumer (the second underlying series).
	// The first series should be buffered for the first consumer, but not the second.
	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d, err := consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[1], d, memoryConsumptionTracker)
	require.Equal(t, 1, buffer.buffer.Size(), "only the first series should be buffered for the first consumer")

	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[2], d, memoryConsumptionTracker)
	require.Equal(t, 1, buffer.buffer.Size(), "only the first series should be buffered for the first consumer")

	// Finalize and close the first consumer, check that the data that was being buffered for it is released.
	require.NoError(t, consumer1.Finalize(ctx))
	consumer1.Close()
	require.Equal(t, 0, buffer.buffer.Size())

	// Check that the inner operator hasn't been closed or finalized yet.
	require.False(t, inner.finalized)
	require.False(t, inner.closed)

	// And the same for the second consumer.
	require.NoError(t, consumer2.Finalize(ctx))
	consumer2.Close()
	require.True(t, inner.finalized)
	require.True(t, inner.closed)
	requireNoMemoryConsumption(t, memoryConsumptionTracker)
}

func TestRangeVectorOperator_Buffering_NonContiguousSeries(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	inner, expectedData := createTestRangeVectorOperator(t, 4, memoryConsumptionTracker)

	buffer := NewRangeVectorDuplicationBuffer(inner, memoryConsumptionTracker)
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
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.series), metadata1, "first consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata([]labels.Labels{inner.series[0]}), metadata2, "second consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata([]labels.Labels{inner.series[1], inner.series[3]}), metadata3, "third consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata1, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&metadata2, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&metadata3, memoryConsumptionTracker)

	// Read all four series from the unfiltered consumer.
	// Only those series needed for the other consumers should be buffered.
	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d, err := consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[0], d, memoryConsumptionTracker)
	require.Equal(t, 1, buffer.buffer.Size())

	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[1], d, memoryConsumptionTracker)
	require.Equal(t, 2, buffer.buffer.Size())

	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[2], d, memoryConsumptionTracker)
	require.Equal(t, 2, buffer.buffer.Size(), "should not buffer series at index 2, as it's not needed by any other consumer")

	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[3], d, memoryConsumptionTracker)
	require.Equal(t, 3, buffer.buffer.Size())

	require.NoError(t, consumer1.Finalize(ctx))
	consumer1.Close()
	require.False(t, inner.finalized)
	require.False(t, inner.closed)

	// Read all the data from consumer 3.
	err = consumer3.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer3.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[1], d, memoryConsumptionTracker)
	require.Equal(t, 3, buffer.buffer.Size(), "buffer size should be unchanged as series at index 0 is required for consumer 2, index 1 should be tombstoned, index 2 is required for consumer 3")

	err = consumer3.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer3.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[3], d, memoryConsumptionTracker)
	require.Equal(t, 3, buffer.buffer.Size(), "buffer size should be unchanged as series at index 0 is required for consumer 2, index 1 should be tombstoned, index 2 will be released on next interaction")

	require.NoError(t, consumer3.Finalize(ctx))
	consumer3.Close()
	require.Equal(t, 1, buffer.buffer.Size(), "should only be buffering series required by consumer 2 (series 0)")
	require.False(t, inner.finalized)
	require.False(t, inner.closed)

	// Read all the data from consumer 2
	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[0], d, memoryConsumptionTracker)
	require.Equal(t, 1, buffer.buffer.Size(), "buffered data should remain buffered until the next NextSeries or Close call")

	// Make sure everything is cleaned up properly.
	require.False(t, inner.finalized)
	require.False(t, inner.closed)
	require.NoError(t, consumer2.Finalize(ctx))
	require.True(t, inner.finalized)
	consumer2.Close()
	require.True(t, inner.closed)
	requireNoMemoryConsumption(t, memoryConsumptionTracker)
}

func TestRangeVectorOperator_ClosedWithBufferedData_NoFiltering(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	inner, expectedData := createTestRangeVectorOperator(t, 3, memoryConsumptionTracker)

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
	requireEqualDataAndReturnToPool(t, expectedData[0], d, memoryConsumptionTracker)
	require.Equal(t, 1, buffer.buffer.Size())

	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[1], d, memoryConsumptionTracker)
	require.Equal(t, 2, buffer.buffer.Size())

	err = consumer1.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer1.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[2], d, memoryConsumptionTracker)
	require.Equal(t, 3, buffer.buffer.Size())

	// Close the first consumer, and check the data remains buffered for the second consumer.
	consumer1.Close()
	require.Equal(t, 3, buffer.buffer.Size())

	// Read some of the buffered data.
	err = consumer2.NextSeries(ctx)
	require.NoError(t, err)
	d, err = consumer2.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[0], d, memoryConsumptionTracker)
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

func TestRangeVectorOperator_ClosedWithBufferedData_Filtering(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	inner, expectedData := createTestRangeVectorOperator(t, 3, memoryConsumptionTracker)

	buffer := NewRangeVectorDuplicationBuffer(inner, memoryConsumptionTracker)
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
	require.Equal(t, testutils.LabelsToSeriesMetadata(inner.series), metadata1, "first consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata([]labels.Labels{inner.series[0], inner.series[2]}), metadata2, "second consumer should get expected series metadata")
	require.Equal(t, testutils.LabelsToSeriesMetadata([]labels.Labels{inner.series[1]}), metadata3, "third consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata1, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&metadata2, memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&metadata3, memoryConsumptionTracker)

	// Read each series from the first consumer, filling the buffer for the other consumers.
	for idx := range 3 {
		err = consumer1.NextSeries(ctx)
		require.NoError(t, err)
		d, err := consumer1.NextStepSamples(ctx)
		require.NoError(t, err)
		requireEqualDataAndReturnToPool(t, expectedData[idx], d, memoryConsumptionTracker)
		require.Equal(t, idx+1, buffer.buffer.Size())
	}

	require.Equal(t, 3, buffer.buffer.Size(), "buffer should contain all three series for the remaining two consumers")
	consumer2.Close()
	require.Equal(t, 1, buffer.buffer.Size(), "buffer should only contain remaining series required by remaining consumer")

	err = consumer3.NextSeries(ctx)
	require.NoError(t, err)
	d, err := consumer3.NextStepSamples(ctx)
	require.NoError(t, err)
	requireEqualDataAndReturnToPool(t, expectedData[1], d, memoryConsumptionTracker)
	require.Equal(t, 1, buffer.buffer.Size(), "buffered data should remain in the buffer until the next NextSeries or Close call")

	consumer3.Close()
	require.Equal(t, 0, buffer.buffer.Size())
	consumer1.Close()
	requireNoMemoryConsumption(t, memoryConsumptionTracker)
}

// TestRangeVectorOperator_StepDataStructure is a test to validate that no additional fields have been added to RangeVectorStepData
// and not been considered in the cloneStepData().
// This test uses reflection to populate values into all fields, clones the record and asserts that the values match.
// The test will fail if the cloned record does not match, or there are fields found which this test does not consider.
// Should this test fail, add the necessary field handling to this test and update range_vector_operator.go cloneStepData().
func TestRangeVectorOperator_StepDataStructure(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")

	data := &types.RangeVectorStepData{}

	fpoints := types.NewFPointRingBuffer(memoryConsumptionTracker)
	require.NoError(t, fpoints.Use([]promql.FPoint{{T: 10, F: 30.3}}))

	hpoints := types.NewHPointRingBuffer(memoryConsumptionTracker)
	require.NoError(t, hpoints.Use([]promql.HPoint{{T: 0, H: &histogram.FloatHistogram{Count: 10, Sum: 1}}}))

	// set explicitly to avoid reflection complexity in handling these
	data.Floats = fpoints.ViewAll(nil)
	data.Histograms = hpoints.ViewUntilSearchingBackwards(0, nil)

	v := reflect.ValueOf(data).Elem() // reflect on struct value
	r := v.Type()                     // struct type

	for i := 0; i < v.NumField(); i++ {
		fieldVal := v.Field(i)
		fieldType := r.Field(i)

		// we have already poked in values for these
		if fieldType.Name == "Floats" || fieldType.Name == "Histograms" {
			continue
		}

		switch fieldVal.Kind() {
		case reflect.Int64:
			fieldVal.SetInt(rand.Int64())
		case reflect.Bool:
			fieldVal.SetBool(true)
		default:
			require.Fail(t, "unexpected field. field=%s, kind=%v", fieldType.Name, fieldVal.Kind())
		}
	}

	clonedStepData, err := cloneStepData(data)

	require.NoError(t, err)
	require.Equal(t, data, clonedStepData.stepData)
}

func TestRangeVectorOperator_Cloning_SmoothedAnchored(t *testing.T) {
	testCases := map[string]struct {
		stepData types.RangeVectorStepData
	}{
		"anchored": {
			stepData: types.RangeVectorStepData{
				Anchored: true,
			},
		},
		"smoothed": {
			stepData: types.RangeVectorStepData{
				Smoothed: true,
			},
		},
		"not anchored or smoothed": {
			stepData: types.RangeVectorStepData{
				Anchored: false,
				Smoothed: false,
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
			tc.stepData.Floats = types.NewFPointRingBuffer(memoryConsumptionTracker).ViewAll(nil)
			tc.stepData.Histograms = types.NewHPointRingBuffer(memoryConsumptionTracker).ViewUntilSearchingBackwards(0, nil)
			clonedStepData, err := cloneStepData(&tc.stepData)
			require.NoError(t, err)

			require.Equal(t, tc.stepData.Smoothed, clonedStepData.stepData.Smoothed)
			require.Equal(t, tc.stepData.Anchored, clonedStepData.stepData.Anchored)
		})
	}

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
	inner := newTestRangeVectorOperator(
		[]labels.Labels{labels.FromStrings(model.MetricNameLabel, "test_series")},
		[]types.InstantVectorSeriesData{series},
		time.Minute,
		memoryConsumptionTracker,
	)

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

	requireEqualDataAndReturnToPool(t, series, d1, memoryConsumptionTracker)
}

func requireEqualDataAndReturnToPool(t *testing.T, expected types.InstantVectorSeriesData, actual *types.RangeVectorStepData, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	actualFloats, err := actual.Floats.CopyPoints()
	require.NoError(t, err)
	require.Equal(t, expected.Floats, actualFloats)
	types.FPointSlicePool.Put(&actualFloats, memoryConsumptionTracker)

	actualHistograms, err := actual.Histograms.CopyPoints()
	require.NoError(t, err)
	require.Equal(t, expected.Histograms, actualHistograms)
	types.HPointSlicePool.Put(&actualHistograms, memoryConsumptionTracker)
}

func createTestRangeVectorOperator(t *testing.T, seriesCount int, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (*testRangeVectorOperator, []types.InstantVectorSeriesData) {
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

	return newTestRangeVectorOperator(
		series,
		data,
		time.Minute,
		memoryConsumptionTracker,
	), data
}

func TestRangeVectorOperator_ClosingAfterFirstReadFails(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	series, err := types.SeriesMetadataSlicePool.Get(1, memoryConsumptionTracker)
	require.NoError(t, err)

	series, err = types.AppendSeriesMetadata(memoryConsumptionTracker, series, types.SeriesMetadata{Labels: labels.FromStrings(model.MetricNameLabel, "test_series")})
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

	series, err = types.AppendSeriesMetadata(memoryConsumptionTracker, series, types.SeriesMetadata{Labels: labels.FromStrings(model.MetricNameLabel, "test_series_1")})
	require.NoError(t, err)
	series, err = types.AppendSeriesMetadata(memoryConsumptionTracker, series, types.SeriesMetadata{Labels: labels.FromStrings(model.MetricNameLabel, "test_series_2")})
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
	currentSeriesIndex         int
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

func newTestRangeVectorOperator(series []labels.Labels, data []types.InstantVectorSeriesData, stepRange time.Duration, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *testRangeVectorOperator {
	return &testRangeVectorOperator{
		series:                   series,
		currentSeriesIndex:       -1,
		data:                     data,
		stepRange:                stepRange,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
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
	if t.currentSeriesIndex >= len(t.series) {
		return types.EOS
	}

	t.haveReadCurrentStepSamples = false
	t.currentSeriesIndex++

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

	d := t.data[t.currentSeriesIndex]
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

func (t *testRangeVectorOperator) AfterPrepare(_ context.Context) error {
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

func (o *failingRangeVectorOperator) AfterPrepare(ctx context.Context) error {
	return nil
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

func requireNoMemoryConsumption(t *testing.T, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	require.Equalf(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(), "expected all instances to be returned to pool, current memory consumption is:\n%v", memoryConsumptionTracker.DescribeCurrentMemoryConsumption())
}
