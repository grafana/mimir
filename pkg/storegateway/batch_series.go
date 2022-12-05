// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"sync"
	"time"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storegateway/hintspb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/pool"
)

type seriesHasher interface {
	Hash(seriesID storage.SeriesRef, lset labels.Labels, stats *queryStats) uint64
}

type cachedSeriesHasher struct {
	cache *hashcache.BlockSeriesHashCache
}

func (b cachedSeriesHasher) Hash(id storage.SeriesRef, lset labels.Labels, stats *queryStats) uint64 {
	stats.seriesHashCacheRequests++

	hash, ok := b.cache.Fetch(id)
	if !ok {
		hash = lset.Hash()
		b.cache.Store(id, hash)
	} else {
		stats.seriesHashCacheHits++
	}
	return hash
}

func shardOwned(shard *sharding.ShardSelector, hasher seriesHasher, id storage.SeriesRef, lset labels.Labels, stats *queryStats) bool {
	if shard == nil {
		return true
	}
	hash := hasher.Hash(id, lset, stats)

	return hash%shard.ShardCount == shard.ShardIndex
}

func (s *BucketStore) batchSetsForBlocks(ctx context.Context, req *storepb.SeriesRequest, blocks []*bucketBlock, indexReaders map[ulid.ULID]*bucketIndexReader, chunkReaders *chunkReaders, chunksPool pool.Bytes, shardSelector *sharding.ShardSelector, matchers []*labels.Matcher, chunksLimiter ChunksLimiter, seriesLimiter SeriesLimiter, stats *safeQueryStats) (storepb.SeriesSet, *hintspb.SeriesResponseHints, func(), error) {
	resHints := &hintspb.SeriesResponseHints{}
	mtx := sync.Mutex{}
	batches := make([]seriesChunkRefsSetIterator, 0, len(blocks))
	g, _ := errgroup.WithContext(ctx)
	cleanups := make([]func(), 0, len(blocks))

	for _, b := range blocks {
		b := b

		// Keep track of queried blocks.
		resHints.AddQueriedBlock(b.meta.ULID)
		indexr := indexReaders[b.meta.ULID]

		// If query sharding is enabled we have to get the block-specific series hash cache
		// which is used by blockSeries().
		var blockSeriesHashCache *hashcache.BlockSeriesHashCache
		if shardSelector != nil {
			blockSeriesHashCache = s.seriesHashCache.GetBlockCache(b.meta.ULID.String())
		}
		g.Go(func() error {
			var (
				part seriesChunkRefsSetIterator
				err  error
			)

			part, err = openBlockSeriesChunkRefsSetsIterator(
				ctx, s.maxSeriesPerBatch, indexr, b.meta, matchers, shardSelector, blockSeriesHashCache, chunksLimiter, seriesLimiter, req.SkipChunks, req.MinTime, req.MaxTime, stats, s.logger)
			if err != nil {
				return errors.Wrapf(err, "fetch series for block %s", b.meta.ULID)
			}

			mtx.Lock()
			batches = append(batches, part)
			mtx.Unlock()

			return nil
		})
	}

	cleanup := func() {
		for _, c := range cleanups {
			c()
		}
	}

	begin := time.Now()
	err := g.Wait()
	if err != nil {
		return nil, nil, cleanup, err
	}

	getAllDuration := time.Since(begin)
	stats.update(func(stats *queryStats) {
		stats.blocksQueried = len(batches)
		stats.getAllDuration = getAllDuration
	})
	s.metrics.seriesGetAllDuration.Observe(getAllDuration.Seconds())
	s.metrics.seriesBlocksQueried.Observe(float64(len(batches)))

	mergedBatches := mergedSeriesChunkRefsSetIterators(s.maxSeriesPerBatch, batches...)
	var set storepb.SeriesSet
	if chunkReaders != nil {
		// We pass ctx and not gCtx because the gCtx will be canceled once the goroutines group is done.
		set = newSeriesSetWithChunks(ctx, *chunkReaders, chunksPool, mergedBatches, stats)
	} else {
		set = newSeriesSetWithoutChunks(mergedBatches)
	}
	return set, resHints, cleanup, nil
}

func newSeriesSetWithChunks(ctx context.Context, chunkReaders chunkReaders, chunksPool pool.Bytes, batches seriesChunkRefsSetIterator, stats *safeQueryStats) storepb.SeriesSet {
	return newSeriesChunksSeriesSet(newPreloadingSeriesChunkSetIterator(ctx, 1, newLoadingBatchSet(chunkReaders, chunksPool, batches, stats)))
}

type loadingBatchSet struct {
	chunkReaders chunkReaders
	from         seriesChunkRefsSetIterator
	chunksPool   pool.Bytes
	stats        *safeQueryStats

	current seriesChunksSet
	err     error
}

func newLoadingBatchSet(chunkReaders chunkReaders, chunksPool pool.Bytes, from seriesChunkRefsSetIterator, stats *safeQueryStats) *loadingBatchSet {
	return &loadingBatchSet{
		chunkReaders: chunkReaders,
		from:         from,
		chunksPool:   chunksPool,
		stats:        stats,
	}
}

func (c *loadingBatchSet) Next() bool {
	if c.err != nil {
		return false
	}

	if !c.from.Next() {
		c.err = c.from.Err()
		return false
	}

	nextUnloaded := c.from.At()
	entries := make([]seriesEntry, nextUnloaded.len())
	c.chunkReaders.reset()
	for i, s := range nextUnloaded.series {
		entries[i].lset = s.lset
		entries[i].chks = make([]storepb.AggrChunk, len(s.chunks))

		for j, chunk := range s.chunks {
			entries[i].chks[j].MinTime = chunk.minTime
			entries[i].chks[j].MaxTime = chunk.maxTime

			err := c.chunkReaders.addLoad(chunk.blockID, chunk.ref, i, j)
			if err != nil {
				c.err = errors.Wrap(err, "preloading chunks")
				return false
			}
		}
	}

	// Create a batched memory pool that can be released all at once.
	chunksPool := &pool.BatchBytes{Delegate: c.chunksPool}

	err := c.chunkReaders.load(entries, chunksPool, c.stats)
	if err != nil {
		c.err = errors.Wrap(err, "loading chunks")
		return false
	}
	nextLoaded := seriesChunksSet{
		series:         entries,
		chunksReleaser: chunksPool,
	}
	c.current = nextLoaded
	return true
}

func (c *loadingBatchSet) At() seriesChunksSet {
	return c.current
}

func (c *loadingBatchSet) Err() error {
	return c.err
}
