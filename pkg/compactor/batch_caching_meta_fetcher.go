// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid/v2"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketcache"
)

// batchCachingMetaFetcher fetches block metadata from a cache with GetMulti
// before possibly falling back to object storage.
type batchCachingMetaFetcher struct {
	userBkt     objstore.BucketReader
	cache       cache.Cache
	logger      log.Logger
	tenant      string
	concurrency int
	contentTTL  time.Duration
}

func newBatchCachingMetaFetcher(
	userBkt objstore.BucketReader,
	cache cache.Cache,
	logger log.Logger,
	tenant string,
	concurrency int,
	contentTTL time.Duration,
) *batchCachingMetaFetcher {
	return &batchCachingMetaFetcher{
		userBkt:     userBkt,
		cache:       cache,
		logger:      logger,
		tenant:      tenant,
		concurrency: concurrency,
		contentTTL:  contentTTL,
	}
}

func (f *batchCachingMetaFetcher) metaCacheKey(blockID ulid.ULID) string {
	// hardcoded "" bucketID to match caching bucket
	return bucketcache.ContentKey("", path.Join(f.tenant, blockID.String(), block.MetaFilename))
}

// fetchCompactableMetasFromListing discovers blockIDs through an object storage listing,
// filters by ULID time and deletion markers, then batch fetches metadata from cache with fallback to object storage.
// The passed filters are then run with no-compact filtering at the end.
func (f *batchCachingMetaFetcher) fetchCompactableMetasFromListing(ctx context.Context, maxLookback time.Duration, filters []block.MetadataFilter, metrics *block.FetcherMetrics) (metas map[ulid.ULID]*block.Meta, err error) {
	start := time.Now()
	metrics.Syncs.Inc()
	metrics.ResetTx()
	defer func() {
		metrics.SyncDuration.Observe(time.Since(start).Seconds())
		if err != nil {
			metrics.SyncFailures.Inc()
		}
	}()

	blockIDs, noCompact, err := f.discoverBlocks(ctx, maxLookback, metrics.Synced)
	if err != nil {
		return nil, err
	}

	metas, stats, err := f.innerFetchMetas(ctx, blockIDs, false, true, metrics.Synced, filters)
	stats.updateMetrics(metrics)
	if err != nil {
		return nil, err
	}

	// Filter blocks marked as no-compact after innerFetchMetas to not get in the front of deduplication
	beforeLen := len(metas)
	filterMapIfMarked(metas, noCompact)
	metrics.Synced.WithLabelValues(block.MarkedForNoCompactionMeta).Set(float64(beforeLen - len(metas)))
	metrics.Synced.WithLabelValues(block.LoadedMeta).Set(float64(len(metas)))
	metrics.Submit()

	return metas, nil
}

// fetchMetasFromIDs fetches metadata for specific block IDs using the cache where possible.
// Unlike fetchCompactableMetasFromListing, this method returns an error if any block's meta.json is not
// found in storage or is corrupt.
func (f *batchCachingMetaFetcher) fetchMetasFromIDs(ctx context.Context, blockIDs []ulid.ULID, filters []block.MetadataFilter) (map[ulid.ULID]*block.Meta, error) {
	metas, _, err := f.innerFetchMetas(ctx, blockIDs, true, false, newNoopGaugeVec(), filters)
	return metas, err
}

// fetchStats holds counts from innerFetchMetas for callers to optionally record as metrics.
type fetchStats struct {
	totalLoads  int
	cachedLoads int
	noMeta      atomic.Int64
	corrupted   atomic.Int64
	failed      atomic.Int64
}

func (fs *fetchStats) updateMetrics(metrics *block.FetcherMetrics) {
	metrics.Loads.Add(float64(fs.totalLoads))
	metrics.CachedLoads.Add(float64(fs.cachedLoads))
	metrics.Synced.WithLabelValues(block.NoMeta).Set(float64(fs.noMeta.Load()))
	metrics.Synced.WithLabelValues(block.CorruptedMeta).Set(float64(fs.corrupted.Load()))
	metrics.Synced.WithLabelValues(block.FailedMeta).Set(float64(fs.failed.Load()))
}

// innerFetchMetas fetches metadata for the given block IDs from cache and/or storage
// with fast-fail behavior and applies filters afterwards.
// When failOnNotFoundOrCorrupt is true, a missing or corrupt meta.json in storage returns
// an error instead of being silently skipped.
// When cacheContent is true, cache misses that were later successfully loaded will
// be set in the cache.
func (f *batchCachingMetaFetcher) innerFetchMetas(ctx context.Context, blockIDs []ulid.ULID, failOnNotFoundOrCorrupt bool, cacheContent bool, synced block.GaugeVec, filters []block.MetadataFilter) (map[ulid.ULID]*block.Meta, *fetchStats, error) {
	stats := &fetchStats{}
	if len(blockIDs) == 0 {
		return map[ulid.ULID]*block.Meta{}, stats, nil
	}

	stats.totalLoads = len(blockIDs)
	metas, misses := f.fetchFromCache(ctx, blockIDs)
	stats.cachedLoads = len(metas)
	if len(misses) == 0 {
		remaining, err := f.runFilters(ctx, metas, synced, filters)
		return remaining, stats, err
	}

	var mtx sync.Mutex
	g, gCtx := errgroup.WithContext(ctx)
	if f.concurrency > 0 {
		g.SetLimit(f.concurrency)
	}

	shouldCache := cacheContent && f.cache != nil

	for blockID, cacheKey := range misses {
		g.Go(func() error {
			id := blockID.String()

			objectName := path.Join(id, block.MetaFilename)
			r, err := f.userBkt.Get(gCtx, objectName)
			if err != nil {
				if f.userBkt.IsObjNotFoundErr(err) {
					stats.noMeta.Inc()
					if failOnNotFoundOrCorrupt {
						return fmt.Errorf("block metadata not found in bucket for %s: %w", id, block.ErrorSyncMetaNotFound)
					}
					// Tolerate the missing block metadata
					return nil
				}
				stats.failed.Inc()
				return fmt.Errorf("failed to get block metadata from bucket for %s: %w", id, err)
			}
			defer runutil.CloseWithLogOnErr(f.logger, r, "close meta reader")

			data, err := io.ReadAll(r)
			if err != nil {
				stats.failed.Inc()
				return fmt.Errorf("failed block meta read for block %s: %w", id, err)
			}

			m := &block.Meta{}
			if err := json.Unmarshal(data, m); err != nil {
				stats.corrupted.Inc()
				if failOnNotFoundOrCorrupt {
					return fmt.Errorf("corrupted block metadata for %s: %w", id, block.ErrorSyncMetaCorrupted)
				}
				level.Warn(f.logger).Log("msg", "corrupted block meta, skipping", "block", id, "err", err)
				return nil
			}

			mtx.Lock()
			metas[blockID] = m
			mtx.Unlock()
			if shouldCache {
				f.cache.SetAsync(cacheKey, data, f.contentTTL)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, stats, err
	}

	metas, err := f.runFilters(ctx, metas, synced, filters)
	return metas, stats, err
}

// discoverBlocks lists blocks and block markers to discover which blocks are relevant for compaction planning.
// It sets discovery-related synced metrics on the provided synced gauge vec.
func (f *batchCachingMetaFetcher) discoverBlocks(ctx context.Context, maxLookback time.Duration, synced block.GaugeVec) ([]ulid.ULID, map[ulid.ULID]struct{}, error) {
	var lookbackExcluded, deletionMarked float64
	defer func() {
		synced.WithLabelValues(block.LookbackExcludedMeta).Set(lookbackExcluded)
		synced.WithLabelValues(block.MarkedForDeletionMeta).Set(deletionMarked)
	}()

	var minAllowedBlockID ulid.ULID
	if maxLookback > 0 {
		var err error
		minAllowedBlockID, err = ulid.New(ulid.Timestamp(time.Now().Add(-maxLookback)), nil)
		if err != nil {
			return nil, nil, err
		}
	}

	g, gCtx := errgroup.WithContext(ctx)

	var blockIDs []ulid.ULID
	g.Go(func() error {
		return f.userBkt.Iter(gCtx, "", func(name string) error {
			id, ok := block.IsBlockDir(name)
			if !ok {
				return nil
			}
			if maxLookback > 0 && id.Compare(minAllowedBlockID) == -1 {
				lookbackExcluded++
				return nil
			}
			blockIDs = append(blockIDs, id)
			return nil
		})
	})

	deletionMarks := map[ulid.ULID]struct{}{}
	noCompactMarks := map[ulid.ULID]struct{}{}
	g.Go(func() error {
		err := f.userBkt.Iter(gCtx, block.MarkersPathname+"/", func(name string) error {
			base := path.Base(name)
			if id, ok := block.IsDeletionMarkFilename(base); ok {
				deletionMarks[id] = struct{}{}
			} else if id, ok := block.IsNoCompactMarkFilename(base); ok {
				noCompactMarks[id] = struct{}{}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("list block markers: %w", err)
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}
	beforeLen := len(blockIDs)
	blockIDs = filterSliceIfMarked(blockIDs, deletionMarks)
	deletionMarked = float64(beforeLen - len(blockIDs))
	return blockIDs, noCompactMarks, nil
}

func filterSliceIfMarked(blockIDs []ulid.ULID, marks map[ulid.ULID]struct{}) []ulid.ULID {
	if len(marks) == 0 {
		return blockIDs
	}
	unmarked := blockIDs[:0]
	for _, id := range blockIDs {
		if _, marked := marks[id]; marked {
			continue
		}
		unmarked = append(unmarked, id)
	}
	return unmarked
}

func filterMapIfMarked(metas map[ulid.ULID]*block.Meta, marks map[ulid.ULID]struct{}) {
	for id := range marks {
		delete(metas, id)
	}
}

func (f *batchCachingMetaFetcher) runFilters(ctx context.Context, metas map[ulid.ULID]*block.Meta, synced block.GaugeVec, filters []block.MetadataFilter) (map[ulid.ULID]*block.Meta, error) {
	for _, filter := range filters {
		if err := filter.Filter(ctx, metas, synced); err != nil {
			return nil, fmt.Errorf("filter metas: %w", err)
		}
	}
	return metas, nil
}

// fetchFromCache attempts to retrieve block metadata from cache for the given block IDs.
// Returns the cached metas and the IDs not found in cache.
func (f *batchCachingMetaFetcher) fetchFromCache(ctx context.Context, blockIDs []ulid.ULID) (map[ulid.ULID]*block.Meta, map[ulid.ULID]string) {
	if f.cache == nil {
		// All misses, cache keys don't matter because they won't be used
		misses := make(map[ulid.ULID]string, len(blockIDs))
		for _, id := range blockIDs {
			misses[id] = ""
		}
		return make(map[ulid.ULID]*block.Meta), misses
	}

	keys := make([]string, len(blockIDs))
	for i, id := range blockIDs {
		keys[i] = f.metaCacheKey(id)
	}

	hits := f.cache.GetMulti(ctx, keys)

	metas := make(map[ulid.ULID]*block.Meta, len(blockIDs)) // not len(hits) since this will be written to later by callers
	misses := make(map[ulid.ULID]string, len(blockIDs))
	for i, blockID := range blockIDs {
		key := keys[i]
		data, ok := hits[key]
		if !ok {
			misses[blockID] = key
			continue
		}
		m := &block.Meta{}
		if err := json.Unmarshal(data, m); err != nil {
			level.Warn(f.logger).Log("msg", "corrupted cache entry for block meta, will fetch from storage", "block", blockID.String(), "err", err)
			misses[blockID] = key
			continue
		}
		// This is a sanity check against cache collisions, but it is entirely unexpected for such a collision to ever occur.
		if m.ULID.Compare(blockID) != 0 {
			level.Warn(f.logger).Log("msg", "ULID mismatch on cached metadata entry, skipping", "block", blockID.String())
			misses[blockID] = key
			continue
		}
		metas[blockID] = m
	}
	return metas, misses
}
