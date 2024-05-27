// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"sync"

	"github.com/prometheus/prometheus/storage"
)

type seriesChunkRefsIteratorFactory func(strategy seriesIteratorStrategy, psi *postingsSetsIterator) iterator[seriesChunkRefsSet]

// chunksStreamingCachingSeriesChunkRefsSetIterator is an iterator used while streaming chunks from store-gateways to queriers.
//
// It wraps another iterator that does the actual work. If that iterator is expected to produce only a single batch,
// this iterator caches that batch for the chunks streaming phase, to avoid repeating work done during the series label sending phase.
type chunksStreamingCachingSeriesChunkRefsSetIterator struct {
	strategy        seriesIteratorStrategy
	iteratorFactory seriesChunkRefsIteratorFactory
	postings        []storage.SeriesRef
	batchSize       int

	it iterator[seriesChunkRefsSet]

	expectSingleBatch                    bool
	inChunksStreamingPhaseForSingleBatch bool
	haveCachedBatch                      bool // It's possible that we expected a single batch to be returned, but the batch was filtered out by the inner iterator.

	currentBatchIndex int // -1 after beginning chunks streaming phase, 0 when on first and only batch, 1 after first and only batch
	cachedBatch       seriesChunkRefsSet
}

func newChunksStreamingCachingSeriesChunkRefsSetIterator(strategy seriesIteratorStrategy, postings []storage.SeriesRef, batchSize int, iteratorFactory seriesChunkRefsIteratorFactory) *chunksStreamingCachingSeriesChunkRefsSetIterator {
	expectSingleBatch := len(postings) <= batchSize
	var initialStrategy seriesIteratorStrategy
	var psi *postingsSetsIterator

	if expectSingleBatch {
		initialStrategy = strategy.withChunkRefs()

		// No need to make a copy: we're only going to use the postings once.
		psi = newPostingsSetsIterator(postings, batchSize)
	} else {
		// We'll load chunk refs during the chunks streaming phase.
		initialStrategy = strategy.withNoChunkRefs()

		copiedPostings := make([]storage.SeriesRef, len(postings))
		copy(copiedPostings, postings)
		psi = newPostingsSetsIterator(copiedPostings, batchSize)
	}

	return &chunksStreamingCachingSeriesChunkRefsSetIterator{
		strategy:          strategy,
		postings:          postings,
		batchSize:         batchSize,
		iteratorFactory:   iteratorFactory,
		it:                iteratorFactory(initialStrategy, psi),
		expectSingleBatch: expectSingleBatch,
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
	if i.expectSingleBatch {
		i.inChunksStreamingPhaseForSingleBatch = true
		i.currentBatchIndex = -1
	} else {
		// No need to make a copy of postings here like we do in newChunksStreamingCachingSeriesChunkRefsSetIterator:
		// we're not expecting to use them again after this, so we don't care if they're modified.
		psi := newPostingsSetsIterator(i.postings, i.batchSize)
		i.it = i.iteratorFactory(i.strategy.withChunkRefs(), psi)
	}
}

// streamingSeriesIterators represents a collection of iterators that will be used to handle a
// Series() request that uses chunks streaming.
type streamingSeriesIterators struct {
	iterators []*chunksStreamingCachingSeriesChunkRefsSetIterator
	mtx       *sync.RWMutex
}

func newStreamingSeriesIterators() *streamingSeriesIterators {
	return &streamingSeriesIterators{
		mtx: &sync.RWMutex{},
	}
}

func (i *streamingSeriesIterators) wrapIterator(strategy seriesIteratorStrategy, ps []storage.SeriesRef, batchSize int, iteratorFactory seriesChunkRefsIteratorFactory) iterator[seriesChunkRefsSet] {
	it := newChunksStreamingCachingSeriesChunkRefsSetIterator(strategy, ps, batchSize, iteratorFactory)

	i.mtx.Lock()
	i.iterators = append(i.iterators, it)
	i.mtx.Unlock()

	return it
}

func (i *streamingSeriesIterators) prepareForChunksStreamingPhase() []iterator[seriesChunkRefsSet] {
	prepared := make([]iterator[seriesChunkRefsSet], 0, len(i.iterators))

	for _, it := range i.iterators {
		it.PrepareForChunksStreamingPhase()
		prepared = append(prepared, it)
	}

	return prepared
}
