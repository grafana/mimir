// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"sync"
	"time"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storegateway/hintspb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/pool"
)

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
	return newSeriesChunksSeriesSet(newPreloadingSeriesChunkSetIterator(ctx, 1, newLoadingSeriesChunksSetIterator(chunkReaders, chunksPool, batches, stats)))
}
