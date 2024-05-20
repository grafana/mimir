// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"errors"
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func TestChunksStreamingCachingSeriesChunkRefsSetIterator_ExpectingSingleBatch_HappyPath(t *testing.T) {
	postings := []storage.SeriesRef{1, 2, 3}
	batchSize := 4
	factoryCalls := 0
	var factoryStrategy seriesIteratorStrategy

	firstBatchSeries := []seriesChunkRefs{
		{lset: labels.FromStrings("series", "1")},
		{lset: labels.FromStrings("series", "2")},
		{lset: labels.FromStrings("series", "3")},
	}

	iteratorFactory := func(strategy seriesIteratorStrategy, _ *postingsSetsIterator) iterator[seriesChunkRefsSet] {
		factoryCalls++
		factoryStrategy = strategy

		return newSliceSeriesChunkRefsSetIterator(
			nil,
			seriesChunkRefsSet{
				series:     firstBatchSeries,
				releasable: true,
			},
		)
	}

	it := newChunksStreamingCachingSeriesChunkRefsSetIterator(defaultStrategy, postings, batchSize, iteratorFactory)

	// Inner iterator should be created with chunk refs enabled.
	require.Equal(t, 1, factoryCalls)
	require.Equal(t, defaultStrategy.withChunkRefs(), factoryStrategy)
	require.NoError(t, it.Err())

	// During label sending phase, the single batch should be returned and not be releasable.
	batches := readAllSeriesChunkRefsSet(it)
	require.NoError(t, it.Err())

	unreleasableBatch := seriesChunkRefsSet{
		series:     firstBatchSeries,
		releasable: false,
	}
	require.Equal(t, []seriesChunkRefsSet{unreleasableBatch}, batches)

	// Prepare for chunks streaming phase. Inner iterator should not be recreated.
	it.PrepareForChunksStreamingPhase()
	require.Equal(t, 1, factoryCalls)
	require.NoError(t, it.Err())

	// During chunks streaming phase, the single batch should be returned but be releasable this time.
	batches = readAllSeriesChunkRefsSet(it)
	require.NoError(t, it.Err())

	releasableBatch := seriesChunkRefsSet{
		series:     firstBatchSeries,
		releasable: true,
	}
	require.Equal(t, []seriesChunkRefsSet{releasableBatch}, batches)
}

func TestChunksStreamingCachingSeriesChunkRefsSetIterator_ExpectingSingleBatch_InnerIteratorReturnsUnreleasableSet(t *testing.T) {
	postings := []storage.SeriesRef{1, 2, 3}
	batchSize := 4
	factoryCalls := 0

	unreleasableBatch := seriesChunkRefsSet{
		series: []seriesChunkRefs{
			{lset: labels.FromStrings("series", "1")},
			{lset: labels.FromStrings("series", "2")},
			{lset: labels.FromStrings("series", "3")},
		},
		releasable: false,
	}

	iteratorFactory := func(_ seriesIteratorStrategy, _ *postingsSetsIterator) iterator[seriesChunkRefsSet] {
		factoryCalls++
		return newSliceSeriesChunkRefsSetIterator(nil, unreleasableBatch)
	}

	it := newChunksStreamingCachingSeriesChunkRefsSetIterator(defaultStrategy, postings, batchSize, iteratorFactory)

	// During label sending phase, the single batch should be returned and not be releasable.
	batches := readAllSeriesChunkRefsSet(it)
	require.NoError(t, it.Err())
	require.Equal(t, []seriesChunkRefsSet{unreleasableBatch}, batches)

	// Prepare for chunks streaming phase. Inner iterator should not be recreated.
	it.PrepareForChunksStreamingPhase()
	require.Equal(t, 1, factoryCalls)
	require.NoError(t, it.Err())

	// During chunks streaming phase, the single batch should be returned and still not be releasable.
	batches = readAllSeriesChunkRefsSet(it)
	require.NoError(t, it.Err())
	require.Equal(t, []seriesChunkRefsSet{unreleasableBatch}, batches)
}

func TestChunksStreamingCachingSeriesChunkRefsSetIterator_ExpectingSingleBatch_AllBatchesFilteredOut(t *testing.T) {
	postings := []storage.SeriesRef{1, 2, 3}
	batchSize := 4
	factoryCalls := 0
	var factoryStrategy seriesIteratorStrategy

	iteratorFactory := func(strategy seriesIteratorStrategy, _ *postingsSetsIterator) iterator[seriesChunkRefsSet] {
		factoryCalls++
		factoryStrategy = strategy

		return newSliceSeriesChunkRefsSetIterator(
			nil,
			// No batches.
		)
	}

	it := newChunksStreamingCachingSeriesChunkRefsSetIterator(defaultStrategy, postings, batchSize, iteratorFactory)

	// Inner iterator should be created with chunk refs enabled.
	require.Equal(t, 1, factoryCalls)
	require.Equal(t, defaultStrategy.withChunkRefs(), factoryStrategy)
	require.NoError(t, it.Err())

	// Label sending phase.
	batches := readAllSeriesChunkRefsSet(it)
	require.NoError(t, it.Err())
	require.Empty(t, batches)

	// Prepare for chunks streaming phase. Inner iterator should not be recreated.
	it.PrepareForChunksStreamingPhase()
	require.Equal(t, 1, factoryCalls)
	require.NoError(t, it.Err())

	// Chunks streaming phase.
	batches = readAllSeriesChunkRefsSet(it)
	require.NoError(t, it.Err())
	require.Empty(t, batches)
}

func TestChunksStreamingCachingSeriesChunkRefsSetIterator_ExpectingSingleBatch_IteratorReturnsError(t *testing.T) {
	postings := []storage.SeriesRef{1, 2, 3}
	batchSize := 4
	factoryCalls := 0
	iteratorError := errors.New("something went wrong")

	iteratorFactory := func(_ seriesIteratorStrategy, _ *postingsSetsIterator) iterator[seriesChunkRefsSet] {
		factoryCalls++

		return newSliceSeriesChunkRefsSetIterator(
			iteratorError,
			seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("series", "1")},
					{lset: labels.FromStrings("series", "2")},
					{lset: labels.FromStrings("series", "3")},
				},
				releasable: true,
			},
		)
	}

	it := newChunksStreamingCachingSeriesChunkRefsSetIterator(defaultStrategy, postings, batchSize, iteratorFactory)

	// During label sending phase, the error should be returned.
	_ = readAllSeriesChunkRefsSet(it)
	require.Equal(t, iteratorError, it.Err())

	// Prepare for chunks streaming phase. Inner iterator should not be recreated.
	it.PrepareForChunksStreamingPhase()
	require.Equal(t, 1, factoryCalls)

	// During chunks streaming phase, the error should be returned.
	_ = readAllSeriesChunkRefsSet(it)
	require.Equal(t, iteratorError, it.Err())
}

func TestChunksStreamingCachingSeriesChunkRefsSetIterator_ExpectingMultipleBatches_HappyPath(t *testing.T) {
	postings := []storage.SeriesRef{1, 2, 3, 4, 5, 6}
	batchSize := 3
	factoryCalls := 0
	var factoryStrategy seriesIteratorStrategy

	firstBatchWithNoChunkRefs := seriesChunkRefsSet{
		series: []seriesChunkRefs{
			{lset: labels.FromStrings("series", "1", "have_chunk_refs", "no")},
			{lset: labels.FromStrings("series", "2", "have_chunk_refs", "no")},
			{lset: labels.FromStrings("series", "3", "have_chunk_refs", "no")},
		},
		releasable: true,
	}

	secondBatchWithNoChunkRefs := seriesChunkRefsSet{
		series: []seriesChunkRefs{
			{lset: labels.FromStrings("series", "4", "have_chunk_refs", "no")},
			{lset: labels.FromStrings("series", "5", "have_chunk_refs", "no")},
			{lset: labels.FromStrings("series", "6", "have_chunk_refs", "no")},
		},
		releasable: true,
	}

	firstBatchWithChunkRefs := seriesChunkRefsSet{
		series: []seriesChunkRefs{
			{lset: labels.FromStrings("series", "1", "have_chunk_refs", "yes")},
			{lset: labels.FromStrings("series", "2", "have_chunk_refs", "yes")},
			{lset: labels.FromStrings("series", "3", "have_chunk_refs", "yes")},
		},
		releasable: true,
	}

	secondBatchWithChunkRefs := seriesChunkRefsSet{
		series: []seriesChunkRefs{
			{lset: labels.FromStrings("series", "4", "have_chunk_refs", "yes")},
			{lset: labels.FromStrings("series", "5", "have_chunk_refs", "yes")},
			{lset: labels.FromStrings("series", "6", "have_chunk_refs", "yes")},
		},
		releasable: true,
	}

	iteratorFactory := func(strategy seriesIteratorStrategy, psi *postingsSetsIterator) iterator[seriesChunkRefsSet] {
		factoryCalls++
		factoryStrategy = strategy

		require.Equal(t, 0, psi.nextBatchPostingsOffset, "postings set iterator should be at beginning when creating iterator")

		if factoryCalls == 1 {
			// Simulate the underlying iterator advancing the postings set iterator to the end, to ensure we get a fresh iterator next time.
			for psi.Next() {
				// Nothing to do, we just want to advance.
			}

			return newSliceSeriesChunkRefsSetIterator(nil, firstBatchWithNoChunkRefs, secondBatchWithNoChunkRefs)
		}

		return newSliceSeriesChunkRefsSetIterator(nil, firstBatchWithChunkRefs, secondBatchWithChunkRefs)
	}

	it := newChunksStreamingCachingSeriesChunkRefsSetIterator(defaultStrategy, postings, batchSize, iteratorFactory)

	// Inner iterator should be created with chunk refs disabled.
	require.Equal(t, 1, factoryCalls)
	require.Equal(t, defaultStrategy.withNoChunkRefs(), factoryStrategy)
	require.NoError(t, it.Err())

	// During label sending phase, the batches should be returned as-is.
	batches := readAllSeriesChunkRefsSet(it)
	require.NoError(t, it.Err())
	require.Equal(t, []seriesChunkRefsSet{firstBatchWithNoChunkRefs, secondBatchWithNoChunkRefs}, batches)

	// Prepare for chunks streaming phase. Inner iterator should be recreated with chunk refs enabled.
	it.PrepareForChunksStreamingPhase()
	require.Equal(t, 2, factoryCalls)
	require.Equal(t, defaultStrategy.withChunkRefs(), factoryStrategy)
	require.NoError(t, it.Err())

	// During chunks streaming phase, the batches should be returned as-is from the new iterator.
	batches = readAllSeriesChunkRefsSet(it)
	require.NoError(t, it.Err())
	require.Equal(t, []seriesChunkRefsSet{firstBatchWithChunkRefs, secondBatchWithChunkRefs}, batches)
}

func TestChunksStreamingCachingSeriesChunkRefsSetIterator_ExpectingMultipleBatches_AllBatchesFilteredOut(t *testing.T) {
	postings := []storage.SeriesRef{1, 2, 3, 4, 5, 6}
	batchSize := 3
	factoryCalls := 0
	var factoryStrategy seriesIteratorStrategy

	iteratorFactory := func(strategy seriesIteratorStrategy, _ *postingsSetsIterator) iterator[seriesChunkRefsSet] {
		factoryCalls++
		factoryStrategy = strategy

		return newSliceSeriesChunkRefsSetIterator(
			nil,
			// No batches.
		)
	}

	it := newChunksStreamingCachingSeriesChunkRefsSetIterator(defaultStrategy, postings, batchSize, iteratorFactory)

	// Inner iterator should be created with chunk refs disabled.
	require.Equal(t, 1, factoryCalls)
	require.Equal(t, defaultStrategy.withNoChunkRefs(), factoryStrategy)
	require.NoError(t, it.Err())

	// Label sending phase.
	batches := readAllSeriesChunkRefsSet(it)
	require.NoError(t, it.Err())
	require.Empty(t, batches)

	// Prepare for chunks streaming phase. Inner iterator should be recreated with chunk refs enabled.
	it.PrepareForChunksStreamingPhase()
	require.Equal(t, 2, factoryCalls)
	require.Equal(t, defaultStrategy.withChunkRefs(), factoryStrategy)
	require.NoError(t, it.Err())

	// Chunks streaming phase.
	batches = readAllSeriesChunkRefsSet(it)
	require.NoError(t, it.Err())
	require.Empty(t, batches)
}

func TestChunksStreamingCachingSeriesChunkRefsSetIterator_ExpectingMultipleBatches_IteratorReturnsError(t *testing.T) {
	postings := []storage.SeriesRef{1, 2, 3, 4, 5, 6}
	batchSize := 3
	factoryCalls := 0

	iteratorFactory := func(_ seriesIteratorStrategy, _ *postingsSetsIterator) iterator[seriesChunkRefsSet] {
		factoryCalls++

		return newSliceSeriesChunkRefsSetIterator(
			fmt.Errorf("error #%v", factoryCalls),
			seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("series", "1")},
					{lset: labels.FromStrings("series", "2")},
					{lset: labels.FromStrings("series", "3")},
				},
				releasable: true,
			},
		)
	}

	it := newChunksStreamingCachingSeriesChunkRefsSetIterator(defaultStrategy, postings, batchSize, iteratorFactory)
	require.Equal(t, 1, factoryCalls)

	// During label sending phase, the error from the original iterator should be returned.
	_ = readAllSeriesChunkRefsSet(it)
	require.EqualError(t, it.Err(), "error #1")

	// Prepare for chunks streaming phase. Inner iterator should be recreated.
	it.PrepareForChunksStreamingPhase()
	require.Equal(t, 2, factoryCalls)

	// During chunks streaming phase, the error from the new iterator should be returned.
	_ = readAllSeriesChunkRefsSet(it)
	require.EqualError(t, it.Err(), "error #2")
}
