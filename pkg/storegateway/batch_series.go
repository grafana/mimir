// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storegateway/hintspb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/pool"
)

type blockSeriesChunkRefsSetsIterator struct {
	ctx      context.Context
	postings []storage.SeriesRef

	batchSize                  int
	currentBatchPostingsOffset int
	currentSet                 seriesChunkRefsSet
	err                        error

	blockID          ulid.ULID
	indexr           *bucketIndexReader      // Index reader for block.
	matchers         []*labels.Matcher       // Series matchers.
	shard            *sharding.ShardSelector // Shard selector.
	seriesHasher     seriesHasher            // Block-specific series hash cache (used only if shard selector is specified).
	chunksLimiter    ChunksLimiter           // Rate limiter for loading chunks.
	seriesLimiter    SeriesLimiter           // Rate limiter for loading series.
	skipChunks       bool                    // If true chunks are not loaded and minTime/maxTime are ignored.
	minTime, maxTime int64                   // Series must have data in this time range to be returned (ignored if skipChunks=true).
	stats            *safeQueryStats
	logger           log.Logger
}

func openBlockSeriesChunkRefsSetsIterator(
	ctx context.Context,
	batchSize int,
	indexr *bucketIndexReader, // Index reader for block.
	blockID ulid.ULID,
	matchers []*labels.Matcher, // Series matchers.
	shard *sharding.ShardSelector, // Shard selector.
	seriesHashCache *hashcache.BlockSeriesHashCache, // Block-specific series hash cache (used only if shard selector is specified).
	chunksLimiter ChunksLimiter, // Rate limiter for loading chunks.
	seriesLimiter SeriesLimiter, // Rate limiter for loading series.
	skipChunks bool, // If true chunks are not loaded and minTime/maxTime are ignored.
	minTime, maxTime int64, // Series must have data in this time range to be returned (ignored if skipChunks=true).
	stats *safeQueryStats,
	logger log.Logger,
) (seriesChunkRefsSetIterator, error) {
	if batchSize <= 0 {
		return nil, errors.New("set size must be a positive number")
	}

	ps, err := indexr.ExpandedPostings(ctx, matchers, stats)
	if err != nil {
		return nil, errors.Wrap(err, "expanded matching posting")
	}

	// We can't compute the series hash yet because we're still missing the series labels.
	// However, if the hash is already in the cache, then we can remove all postings for series
	// not belonging to the shard.
	if shard != nil {
		var unsafeStats queryStats
		ps, unsafeStats = filterPostingsByCachedShardHash(ps, shard, seriesHashCache)
		stats.merge(&unsafeStats)
	}

	return &blockSeriesChunkRefsSetsIterator{
		blockID:                    blockID,
		batchSize:                  batchSize,
		currentBatchPostingsOffset: -batchSize,
		ctx:                        ctx,
		postings:                   ps,
		indexr:                     indexr,
		matchers:                   matchers,
		shard:                      shard,
		seriesHasher:               cachedSeriesHasher{cache: seriesHashCache, stats: stats},
		chunksLimiter:              chunksLimiter,
		seriesLimiter:              seriesLimiter,
		skipChunks:                 skipChunks,
		minTime:                    minTime,
		maxTime:                    maxTime,
		stats:                      stats,
		logger:                     logger,
	}, nil
}

func (s *blockSeriesChunkRefsSetsIterator) Next() bool {
	if s.currentBatchPostingsOffset >= len(s.postings)-1 || s.err != nil {
		return false
	}
	return s.loadBatch()
}

func (s *blockSeriesChunkRefsSetsIterator) loadBatch() bool {
	s.currentBatchPostingsOffset += s.batchSize
	if s.currentBatchPostingsOffset > len(s.postings) {
		return false
	}

	end := s.currentBatchPostingsOffset + s.batchSize
	if end > len(s.postings) {
		end = len(s.postings)
	}
	s.currentSet = newSeriesChunkRefsSet(s.batchSize)
	nextPostings := s.postings[s.currentBatchPostingsOffset:end]

	loadedSeries, err := s.indexr.preloadSeries(s.ctx, nextPostings, s.stats)
	if err != nil {
		s.err = errors.Wrap(err, "preload series")
		return false
	}

	var (
		symbolizedLset []symbolizedLabel
		chks           []chunks.Meta
	)

	// Track the series loading statistics in a not synchronized data structure to avoid locking for each series
	// and then merge before returning from the function.
	loadStats := &queryStats{}
	defer s.stats.merge(loadStats)

	for _, id := range nextPostings {
		ok, err := loadedSeries.unsafeLoadSeriesForTime(id, &symbolizedLset, &chks, s.skipChunks, s.minTime, s.maxTime, loadStats)
		if err != nil {
			s.err = errors.Wrap(err, "read series")
			return false
		}
		if !ok {
			// No matching chunks for this time duration, skip series
			continue
		}

		lset, err := s.indexr.LookupLabelsSymbols(symbolizedLset)
		if err != nil {
			s.err = errors.Wrap(err, "lookup labels symbols")
			return false
		}

		if !shardOwned(s.shard, s.seriesHasher, id, lset) {
			continue
		}

		// Check series limit after filtering out series not belonging to the requested shard (if any).
		if err := s.seriesLimiter.Reserve(1); err != nil {
			s.err = errors.Wrap(err, "exceeded series limit")
			return false
		}

		entry := seriesChunkRefs{lset: lset}

		if !s.skipChunks {
			// Ensure sample limit through chunksLimiter if we return chunks.
			if err = s.chunksLimiter.Reserve(uint64(len(chks))); err != nil {
				s.err = errors.Wrap(err, "exceeded chunks limit")
				return false
			}
			entry.chunks = metasToChunks(s.blockID, chks)
		}

		s.currentSet.series = append(s.currentSet.series, entry)
	}

	if s.currentSet.len() == 0 {
		return s.loadBatch() // we didn't find any suitable series in this set, try with the next one
	}

	return true
}

func metasToChunks(blockID ulid.ULID, metas []chunks.Meta) []seriesChunkRef {
	chks := make([]seriesChunkRef, len(metas))
	for i, meta := range metas {
		chks[i] = seriesChunkRef{
			minTime: meta.MinTime,
			maxTime: meta.MaxTime,
			ref:     meta.Ref,
			blockID: blockID,
		}
	}
	return chks
}

func (s *blockSeriesChunkRefsSetsIterator) At() seriesChunkRefsSet {
	return s.currentSet
}

func (s *blockSeriesChunkRefsSetsIterator) Err() error {
	return s.err
}

type seriesHasher interface {
	Hash(seriesID storage.SeriesRef, lset labels.Labels) uint64
}

type cachedSeriesHasher struct {
	cache *hashcache.BlockSeriesHashCache
	stats *safeQueryStats
}

func (b cachedSeriesHasher) Hash(id storage.SeriesRef, lset labels.Labels) uint64 {
	hash, ok := b.cache.Fetch(id)
	b.stats.update(func(stats *queryStats) {
		stats.seriesHashCacheRequests++
	})

	if !ok {
		hash = lset.Hash()
		b.cache.Store(id, hash)
	} else {
		b.stats.update(func(stats *queryStats) {
			stats.seriesHashCacheHits++
		})
	}
	return hash
}

func shardOwned(shard *sharding.ShardSelector, hasher seriesHasher, id storage.SeriesRef, lset labels.Labels) bool {
	if shard == nil {
		return true
	}
	hash := hasher.Hash(id, lset)

	return hash%shard.ShardCount == shard.ShardIndex
}

func (s *BucketStore) batchSetsForBlocks(ctx context.Context, req *storepb.SeriesRequest, blocks []*bucketBlock, indexReaders map[ulid.ULID]*bucketIndexReader, chunkReaders *chunkReaders, chunksPool pool.Bytes, shardSelector *sharding.ShardSelector, matchers []*labels.Matcher, chunksLimiter ChunksLimiter, seriesLimiter SeriesLimiter, stats *safeQueryStats) (storepb.SeriesSet, *hintspb.SeriesResponseHints, func(), error) {
	resHints := &hintspb.SeriesResponseHints{}
	mtx := sync.Mutex{}
	batches := make([]seriesChunkRefsSetIterator, 0, len(blocks))
	g, gCtx := errgroup.WithContext(ctx)
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
				gCtx, s.maxSeriesPerBatch, indexr, b.meta.ULID, matchers, shardSelector, blockSeriesHashCache, chunksLimiter, seriesLimiter, req.SkipChunks, req.MinTime, req.MaxTime, stats, s.logger)
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
