// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/hashcache"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

type batchedSeriesSet struct {
	ctx      context.Context
	postings []storage.SeriesRef

	batchSize               int
	currBatchIdx            int
	currBatchPostingsOffset int
	preloaded               []seriesEntry
	stats                   *safeQueryStats
	err                     error

	indexr           *bucketIndexReader      // Index reader for block.
	chunkr           *bucketChunkReader      // Chunk reader for block.
	matchers         []*labels.Matcher       // Series matchers.
	shard            *sharding.ShardSelector // Shard selector.
	seriesHasher     seriesHasher            // Block-specific series hash cache (used only if shard selector is specified).
	chunksLimiter    ChunksLimiter           // Rate limiter for loading chunks.
	seriesLimiter    SeriesLimiter           // Rate limiter for loading series.
	skipChunks       bool                    // If true chunks are not loaded and minTime/maxTime are ignored.
	minTime, maxTime int64                   // Series must have data in this time range to be returned (ignored if skipChunks=true).
	loadAggregates   []storepb.Aggr          // List of aggregates to load when loading chunks.
	logger           log.Logger

	cleanupFuncs []func()
}

func batchedBlockSeries(
	ctx context.Context,
	batchSize int,
	indexr *bucketIndexReader, // Index reader for block.
	chunkr *bucketChunkReader, // Chunk reader for block.
	matchers []*labels.Matcher, // Series matchers.
	shard *sharding.ShardSelector, // Shard selector.
	seriesHashCache *hashcache.BlockSeriesHashCache, // Block-specific series hash cache (used only if shard selector is specified).
	chunksLimiter ChunksLimiter, // Rate limiter for loading chunks.
	seriesLimiter SeriesLimiter, // Rate limiter for loading series.
	skipChunks bool, // If true chunks are not loaded and minTime/maxTime are ignored.
	minTime, maxTime int64, // Series must have data in this time range to be returned (ignored if skipChunks=true).
	loadAggregates []storepb.Aggr, // List of aggregates to load when loading chunks.
	logger log.Logger,
) (storepb.SeriesSet, error) {
	if batchSize <= 0 {
		return nil, errors.New("batch size must be a positive number")
	}
	//if skipChunks {
	//	res, ok := fetchCachedSeries(ctx, indexr.block.userID, indexr.block.indexCache, indexr.block.meta.ULID, matchers, shard, logger)
	//	if ok {
	//		return newBucketSeriesSet(res), nil
	//	}
	//}
	stats := newSafeQueryStats()
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
		stats = stats.merge(&unsafeStats)
	}

	return &batchedSeriesSet{
		batchSize:               batchSize,
		preloaded:               make([]seriesEntry, 0, batchSize),
		stats:                   stats,
		currBatchPostingsOffset: -batchSize,
		ctx:                     ctx,
		postings:                ps,
		indexr:                  indexr,
		chunkr:                  chunkr,
		matchers:                matchers,
		shard:                   shard,
		seriesHasher:            cachedSeriesHasher{cache: seriesHashCache, stats: stats},
		chunksLimiter:           chunksLimiter,
		seriesLimiter:           seriesLimiter,
		skipChunks:              skipChunks,
		minTime:                 minTime,
		maxTime:                 maxTime,
		loadAggregates:          loadAggregates,
		logger:                  logger,
	}, nil
}

func (s *batchedSeriesSet) Next() bool {
	if s.currBatchPostingsOffset+s.currBatchIdx >= len(s.postings)-1 || s.err != nil {
		return false
	}
	s.currBatchIdx++
	if s.currBatchIdx >= len(s.preloaded) {
		return s.preload()
	}
	return true
}

func (s *batchedSeriesSet) preload() bool {
	s.resetPreloaded()
	s.currBatchPostingsOffset += s.batchSize
	if s.currBatchPostingsOffset > len(s.postings) {
		return false
	}

	end := s.currBatchPostingsOffset + s.batchSize
	if end > len(s.postings) {
		end = len(s.postings)
	}
	nextBatch := s.postings[s.currBatchPostingsOffset:end]

	loadedSeries, err := s.indexr.preloadSeries(s.ctx, nextBatch, s.stats)
	if err != nil {
		s.err = errors.Wrap(err, "preload series")
		return false
	}

	var (
		symbolizedLset []symbolizedLabel
		chks           []chunks.Meta
	)
	for _, id := range nextBatch {
		ok, err := loadedSeries.loadSeriesForTime(id, &symbolizedLset, &chks, s.skipChunks, s.minTime, s.maxTime, s.stats.unsafeStats)
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

		entry := seriesEntry{lset: lset}

		if !s.skipChunks {
			// Ensure sample limit through chunksLimiter if we return chunks.
			if err = s.chunksLimiter.Reserve(uint64(len(chks))); err != nil {
				s.err = errors.Wrap(err, "exceeded chunks limit")
				return false
			}
			// seriesEntry entry is appended to preloaded, but not at every outer loop iteration,
			// therefore len(preloaded) is the index we need here, not outer loop iteration number.
			entry.refs, entry.chks, s.err = s.scheduleChunksLoading(chks, len(s.preloaded))
			if s.err != nil {
				return false
			}
		}

		s.preloaded = append(s.preloaded, entry)
	}

	if len(s.preloaded) == 0 {
		return s.preload() // we didn't find any suitable series in this batch, try with the next one
	}

	if s.skipChunks {
		storeCachedSeries(s.ctx, s.indexr.block.indexCache, s.indexr.block.userID, s.indexr.block.meta.ULID, s.matchers, s.shard, s.preloaded, s.logger)
		return true
	}

	if !s.skipChunks {
		s.stats = s.stats.merge(s.chunkr.stats)
		if err := s.chunkr.load(s.preloaded, s.loadAggregates); err != nil {
			s.err = errors.Wrap(err, "load chunks")
			return false
		}
		return true
	}

	return true
}

func (s *batchedSeriesSet) scheduleChunksLoading(metas []chunks.Meta, seriesIdx int) ([]chunks.ChunkRef, []storepb.AggrChunk, error) {
	refs := make([]chunks.ChunkRef, 0, len(metas))
	chks := make([]storepb.AggrChunk, 0, len(metas))

	for j, meta := range metas {
		if err := s.chunkr.addLoad(meta.Ref, seriesIdx, j); err != nil {
			return nil, nil, errors.Wrap(err, "add chunk load")
		}
		chks = append(chks, storepb.AggrChunk{
			MinTime: meta.MinTime,
			MaxTime: meta.MaxTime,
		})
		refs = append(refs, meta.Ref)
	}

	return refs, chks, nil
}

func (s *batchedSeriesSet) resetPreloaded() {
	s.preloaded = s.preloaded[:0]
	s.loadAggregates = s.loadAggregates[:0]
	if s.chunkr != nil { // can be nil when the client didn't want to load chunks
		s.chunkr.reset()
		s.cleanupFuncs = append(s.cleanupFuncs, s.chunkr.unload)
	}
	s.currBatchIdx = 0
}

func (s *batchedSeriesSet) At() (labels.Labels, []storepb.AggrChunk) {
	if s.currBatchIdx >= len(s.preloaded) {
		return nil, nil
	}
	return s.preloaded[s.currBatchIdx].lset, s.preloaded[s.currBatchIdx].chks
}

func (s *batchedSeriesSet) Err() error {
	return s.err
}

func (s *batchedSeriesSet) CleanupFunc() func() {
	if len(s.cleanupFuncs) == 0 {
		return nil
	}
	cleanUps := s.cleanupFuncs[:]
	s.cleanupFuncs = s.cleanupFuncs[len(s.cleanupFuncs):]
	return func() {
		for _, cleanup := range cleanUps {
			cleanup()
		}
	}
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
