// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

type iteratorFactory func(strategy seriesIteratorStrategy) iterator[seriesChunkRefsSet]

// chunksStreamingCachingSeriesChunkRefsSetIterator is an iterator used while streaming chunks from store-gateways to queriers.
//
// It wraps another iterator that does the actual work. If that iterator is expected to produce only a single batch,
// this iterator caches that batch for the chunks streaming phase, to avoid repeating work done during the series label sending phase.
type chunksStreamingCachingSeriesChunkRefsSetIterator struct {
	strategy             seriesIteratorStrategy
	postingsSetsIterator *postingsSetsIterator
	factory              iteratorFactory
	it                   iterator[seriesChunkRefsSet]

	expectSingleBatch                    bool
	inChunksStreamingPhaseForSingleBatch bool
	haveCachedBatch                      bool // It's possible that we expected a single batch to be returned, but the batch was filtered out by the inner iterator.

	currentBatchIndex int // -1 after beginning chunks streaming phase, 0 when on first and only batch, 1 after first and only batch
	cachedBatch       seriesChunkRefsSet
}

func newChunksStreamingCachingSeriesChunkRefsSetIterator(strategy seriesIteratorStrategy, postingsSetsIterator *postingsSetsIterator, factory iteratorFactory) *chunksStreamingCachingSeriesChunkRefsSetIterator {
	expectSingleBatch := !postingsSetsIterator.HasMultipleBatches()
	var initialStrategy seriesIteratorStrategy

	if expectSingleBatch {
		initialStrategy = strategy.withChunkRefs()
	} else {
		// We'll load chunk refs during the chunks streaming phase.
		initialStrategy = strategy.withNoChunkRefs()
	}

	return &chunksStreamingCachingSeriesChunkRefsSetIterator{
		strategy:             strategy,
		postingsSetsIterator: postingsSetsIterator,
		factory:              factory,
		it:                   factory(initialStrategy),
		expectSingleBatch:    expectSingleBatch,
	}
}

func (i *chunksStreamingCachingSeriesChunkRefsSetIterator) Next() bool {
	if i.inChunksStreamingPhaseForSingleBatch && i.haveCachedBatch {
		i.currentBatchIndex++
		return i.currentBatchIndex < 1
	}

	return i.it.Next()
}

func (i *chunksStreamingCachingSeriesChunkRefsSetIterator) At() seriesChunkRefsSet {
	if i.inChunksStreamingPhaseForSingleBatch {
		if i.currentBatchIndex == 0 && i.haveCachedBatch {
			// Called Next() once. Return the cached batch.
			// If the original batch was releasable or unreleasable, retain that state here.
			return i.cachedBatch
		}

		// Haven't called Next() yet, or called Next() multiple times and we've advanced past the only batch.
		// At() should never be called in either case, so just return nothing if it is.
		return seriesChunkRefsSet{}
	}

	set := i.it.At()

	if i.expectSingleBatch {
		i.cachedBatch = set
		i.haveCachedBatch = true

		// Don't allow releasing this batch - we'll need it again later, so releasing it is not safe.
		return set.makeUnreleasable()
	}

	return set
}

func (i *chunksStreamingCachingSeriesChunkRefsSetIterator) Err() error {
	return i.it.Err()
}

func (i *chunksStreamingCachingSeriesChunkRefsSetIterator) PrepareForChunksStreamingPhase() {
	i.postingsSetsIterator.Reset()

	if i.expectSingleBatch {
		i.inChunksStreamingPhaseForSingleBatch = true
		i.currentBatchIndex = -1
	} else {
		i.it = i.factory(i.strategy.withChunkRefs())
	}
}
