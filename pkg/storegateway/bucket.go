// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/types"
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/tracing"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
	"github.com/grafana/mimir/pkg/storegateway/hintspb"
	"github.com/grafana/mimir/pkg/storegateway/indexcache"
	"github.com/grafana/mimir/pkg/storegateway/indexheader"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/gate"
	"github.com/grafana/mimir/pkg/util/pool"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	// MaxSamplesPerChunk is approximately the max number of samples that we may have in any given chunk. This is needed
	// for precalculating the number of samples that we may have to retrieve and decode for any given query
	// without downloading them. Please take a look at https://github.com/prometheus/tsdb/pull/397 to know
	// where this number comes from. Long story short: TSDB is made in such a way, and it is made in such a way
	// because you barely get any improvements in compression when the number of samples is beyond this.
	// Take a look at Figure 6 in this whitepaper http://www.vldb.org/pvldb/vol8/p1816-teller.pdf.
	MaxSamplesPerChunk = 120
	maxSeriesSize      = 64 * 1024
	// Relatively large in order to reduce memory waste, yet small enough to avoid excessive allocations.
	chunkBytesPoolMinSize = 64 * 1024        // 64 KiB
	chunkBytesPoolMaxSize = 64 * 1024 * 1024 // 64 MiB

	// Labels for metrics.
	labelEncode = "encode"
	labelDecode = "decode"
)

type BucketStoreStats struct {
	// BlocksLoaded is the number of blocks currently loaded in the bucket store.
	BlocksLoaded int
}

// BucketStore implements the store API backed by a bucket. It loads all index
// files to local disk.
//
// NOTE: Bucket store reencodes postings using diff+varint+snappy when storing to cache.
// This makes them smaller, but takes extra CPU and memory.
// When used with in-memory cache, memory usage should decrease overall, thanks to postings being smaller.
type BucketStore struct {
	userID          string
	logger          log.Logger
	metrics         *BucketStoreMetrics
	bkt             objstore.InstrumentedBucketReader
	fetcher         block.MetadataFetcher
	dir             string
	indexCache      indexcache.IndexCache
	indexReaderPool *indexheader.ReaderPool
	chunkPool       pool.Bytes
	seriesHashCache *hashcache.SeriesHashCache

	// Sets of blocks that have the same labels. They are indexed by a hash over their label set.
	blocksMx sync.RWMutex
	blocks   map[ulid.ULID]*bucketBlock
	blockSet *bucketBlockSet

	// Verbose enabled additional logging.
	debugLogging bool
	// Number of goroutines to use when syncing blocks from object storage.
	blockSyncConcurrency int

	// Query gate which limits the maximum amount of concurrent queries.
	queryGate gate.Gate

	// chunksLimiterFactory creates a new limiter used to limit the number of chunks fetched by each Series() call.
	chunksLimiterFactory ChunksLimiterFactory
	// seriesLimiterFactory creates a new limiter used to limit the number of touched series by each Series() call,
	// or LabelName and LabelValues calls when used with matchers.
	seriesLimiterFactory SeriesLimiterFactory
	partitioner          Partitioner

	// Every how many posting offset entry we pool in heap memory. Default in Prometheus is 32.
	postingOffsetsInMemSampling int

	// Additional configuration for experimental indexheader.BinaryReader behaviour.
	indexHeaderCfg indexheader.BinaryReaderConfig
}

type noopCache struct{}

func (noopCache) StorePostings(context.Context, string, ulid.ULID, labels.Label, []byte) {}
func (noopCache) FetchMultiPostings(_ context.Context, _ string, _ ulid.ULID, keys []labels.Label) (map[labels.Label][]byte, []labels.Label) {
	return map[labels.Label][]byte{}, keys
}

func (noopCache) StoreSeriesForRef(context.Context, string, ulid.ULID, storage.SeriesRef, []byte) {}
func (noopCache) FetchMultiSeriesForRefs(_ context.Context, _ string, _ ulid.ULID, ids []storage.SeriesRef) (map[storage.SeriesRef][]byte, []storage.SeriesRef) {
	return map[storage.SeriesRef][]byte{}, ids
}

func (c noopCache) StoreExpandedPostings(_ context.Context, _ string, _ ulid.ULID, _ indexcache.LabelMatchersKey, _ []byte) {
}

func (c noopCache) FetchExpandedPostings(_ context.Context, _ string, _ ulid.ULID, _ indexcache.LabelMatchersKey) ([]byte, bool) {
	return nil, false
}

func (noopCache) StoreSeries(_ context.Context, _ string, _ ulid.ULID, _ indexcache.LabelMatchersKey, _ *sharding.ShardSelector, _ []byte) {
}
func (noopCache) FetchSeries(_ context.Context, _ string, _ ulid.ULID, _ indexcache.LabelMatchersKey, _ *sharding.ShardSelector) ([]byte, bool) {
	return nil, false
}

func (noopCache) StoreLabelNames(_ context.Context, _ string, _ ulid.ULID, _ indexcache.LabelMatchersKey, _ []byte) {
}
func (noopCache) FetchLabelNames(_ context.Context, _ string, _ ulid.ULID, _ indexcache.LabelMatchersKey) ([]byte, bool) {
	return nil, false
}

func (noopCache) StoreLabelValues(_ context.Context, _ string, _ ulid.ULID, _ string, _ indexcache.LabelMatchersKey, _ []byte) {
}
func (noopCache) FetchLabelValues(_ context.Context, _ string, _ ulid.ULID, _ string, _ indexcache.LabelMatchersKey) ([]byte, bool) {
	return nil, false
}

// BucketStoreOption are functions that configure BucketStore.
type BucketStoreOption func(s *BucketStore)

// WithLogger sets the BucketStore logger to the one you pass.
func WithLogger(logger log.Logger) BucketStoreOption {
	return func(s *BucketStore) {
		s.logger = logger
	}
}

// WithIndexCache sets a indexCache to use instead of a noopCache.
func WithIndexCache(cache indexcache.IndexCache) BucketStoreOption {
	return func(s *BucketStore) {
		s.indexCache = cache
	}
}

// WithQueryGate sets a queryGate to use instead of a noopGate.
func WithQueryGate(queryGate gate.Gate) BucketStoreOption {
	return func(s *BucketStore) {
		s.queryGate = queryGate
	}
}

// WithChunkPool sets a pool.Bytes to use for chunks.
func WithChunkPool(chunkPool pool.Bytes) BucketStoreOption {
	return func(s *BucketStore) {
		s.chunkPool = chunkPool
	}
}

// WithDebugLogging enables debug logging.
func WithDebugLogging() BucketStoreOption {
	return func(s *BucketStore) {
		s.debugLogging = true
	}
}

// NewBucketStore creates a new bucket backed store that implements the store API against
// an object store bucket. It is optimized to work against high latency backends.
func NewBucketStore(
	userID string,
	bkt objstore.InstrumentedBucketReader,
	fetcher block.MetadataFetcher,
	dir string,
	chunksLimiterFactory ChunksLimiterFactory,
	seriesLimiterFactory SeriesLimiterFactory,
	partitioner Partitioner,
	blockSyncConcurrency int,
	postingOffsetsInMemSampling int,
	indexHeaderCfg indexheader.BinaryReaderConfig,
	lazyIndexReaderEnabled bool,
	lazyIndexReaderIdleTimeout time.Duration,
	seriesHashCache *hashcache.SeriesHashCache,
	metrics *BucketStoreMetrics,
	options ...BucketStoreOption,
) (*BucketStore, error) {
	s := &BucketStore{
		logger:                      log.NewNopLogger(),
		bkt:                         bkt,
		fetcher:                     fetcher,
		dir:                         dir,
		indexCache:                  noopCache{},
		chunkPool:                   pool.NoopBytes{},
		blocks:                      map[ulid.ULID]*bucketBlock{},
		blockSet:                    newBucketBlockSet(),
		blockSyncConcurrency:        blockSyncConcurrency,
		queryGate:                   gate.NewNoop(),
		chunksLimiterFactory:        chunksLimiterFactory,
		seriesLimiterFactory:        seriesLimiterFactory,
		partitioner:                 partitioner,
		postingOffsetsInMemSampling: postingOffsetsInMemSampling,
		indexHeaderCfg:              indexHeaderCfg,
		seriesHashCache:             seriesHashCache,
		metrics:                     metrics,
		userID:                      userID,
	}

	for _, option := range options {
		option(s)
	}

	// Depend on the options
	s.indexReaderPool = indexheader.NewReaderPool(s.logger, lazyIndexReaderEnabled, lazyIndexReaderIdleTimeout, metrics.indexHeaderReaderMetrics)

	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, errors.Wrap(err, "create dir")
	}

	return s, nil
}

// RemoveBlocksAndClose remove all blocks from local disk and releases all resources associated with the BucketStore.
func (s *BucketStore) RemoveBlocksAndClose() error {
	err := s.removeAllBlocks()

	// Release other resources even if it failed to close some blocks.
	s.indexReaderPool.Close()

	return err
}

// Stats returns statistics about the BucketStore instance.
func (s *BucketStore) Stats() BucketStoreStats {
	stats := BucketStoreStats{}

	s.blocksMx.RLock()
	stats.BlocksLoaded = len(s.blocks)
	s.blocksMx.RUnlock()

	return stats
}

// SyncBlocks synchronizes the stores state with the Bucket bucket.
// It will reuse disk space as persistent cache based on s.dir param.
func (s *BucketStore) SyncBlocks(ctx context.Context) error {
	metas, _, metaFetchErr := s.fetcher.Fetch(ctx)
	// For partial view allow adding new blocks at least.
	if metaFetchErr != nil && metas == nil {
		return metaFetchErr
	}

	var wg sync.WaitGroup
	blockc := make(chan *metadata.Meta)

	for i := 0; i < s.blockSyncConcurrency; i++ {
		wg.Add(1)
		go func() {
			for meta := range blockc {
				if err := s.addBlock(ctx, meta); err != nil {
					continue
				}
			}
			wg.Done()
		}()
	}

	for id, meta := range metas {
		if b := s.getBlock(id); b != nil {
			continue
		}
		select {
		case <-ctx.Done():
		case blockc <- meta:
		}
	}

	close(blockc)
	wg.Wait()

	if metaFetchErr != nil {
		return metaFetchErr
	}

	// Drop all blocks that are no longer present in the bucket.
	for id := range s.blocks {
		if _, ok := metas[id]; ok {
			continue
		}
		if err := s.removeBlock(id); err != nil {
			level.Warn(s.logger).Log("msg", "drop of outdated block failed", "block", id, "err", err)
		}
		level.Info(s.logger).Log("msg", "dropped outdated block", "block", id)
	}

	return nil
}

// InitialSync perform blocking sync with extra step at the end to delete locally saved blocks that are no longer
// present in the bucket. The mismatch of these can only happen between restarts, so we can do that only once per startup.
func (s *BucketStore) InitialSync(ctx context.Context) error {
	if err := s.SyncBlocks(ctx); err != nil {
		return errors.Wrap(err, "sync block")
	}

	fis, err := os.ReadDir(s.dir)
	if err != nil {
		return errors.Wrap(err, "read dir")
	}
	names := make([]string, 0, len(fis))
	for _, fi := range fis {
		names = append(names, fi.Name())
	}
	for _, n := range names {
		id, ok := block.IsBlockDir(n)
		if !ok {
			continue
		}
		if b := s.getBlock(id); b != nil {
			continue
		}

		// No such block loaded, remove the local dir.
		if err := os.RemoveAll(path.Join(s.dir, id.String())); err != nil {
			level.Warn(s.logger).Log("msg", "failed to remove block which is not needed", "err", err)
		}
	}

	return nil
}

func (s *BucketStore) getBlock(id ulid.ULID) *bucketBlock {
	s.blocksMx.RLock()
	defer s.blocksMx.RUnlock()
	return s.blocks[id]
}

func (s *BucketStore) addBlock(ctx context.Context, meta *metadata.Meta) (err error) {
	dir := filepath.Join(s.dir, meta.ULID.String())
	start := time.Now()

	level.Debug(s.logger).Log("msg", "loading new block", "id", meta.ULID)
	defer func() {
		if err != nil {
			s.metrics.blockLoadFailures.Inc()
			if err2 := os.RemoveAll(dir); err2 != nil {
				level.Warn(s.logger).Log("msg", "failed to remove block we cannot load", "err", err2)
			}
			level.Warn(s.logger).Log("msg", "loading block failed", "elapsed", time.Since(start), "id", meta.ULID, "err", err)
		} else {
			level.Info(s.logger).Log("msg", "loaded new block", "elapsed", time.Since(start), "id", meta.ULID)
		}
	}()
	s.metrics.blockLoads.Inc()

	indexHeaderReader, err := s.indexReaderPool.NewBinaryReader(
		ctx,
		s.logger,
		s.bkt,
		s.dir,
		meta.ULID,
		s.postingOffsetsInMemSampling,
		s.indexHeaderCfg,
	)
	if err != nil {
		return errors.Wrap(err, "create index header reader")
	}

	defer func() {
		if err != nil {
			runutil.CloseWithErrCapture(&err, indexHeaderReader, "index-header")
		}
	}()

	b, err := newBucketBlock(
		ctx,
		s.userID,
		log.With(s.logger, "block", meta.ULID),
		s.metrics,
		meta,
		s.bkt,
		dir,
		s.indexCache,
		s.chunkPool,
		indexHeaderReader,
		s.partitioner,
	)
	if err != nil {
		return errors.Wrap(err, "new bucket block")
	}
	defer func() {
		if err != nil {
			runutil.CloseWithErrCapture(&err, b, "index-header")
		}
	}()

	s.blocksMx.Lock()
	defer s.blocksMx.Unlock()

	if err = s.blockSet.add(b); err != nil {
		return errors.Wrap(err, "add block to set")
	}
	s.blocks[b.meta.ULID] = b

	return nil
}

func (s *BucketStore) removeBlock(id ulid.ULID) (returnErr error) {
	defer func() {
		if returnErr != nil {
			s.metrics.blockDropFailures.Inc()
		}
	}()

	s.blocksMx.Lock()
	b, ok := s.blocks[id]
	if ok {
		s.blockSet.remove(id)
		delete(s.blocks, id)
	}
	s.blocksMx.Unlock()

	if !ok {
		return nil
	}

	// The block has already been removed from BucketStore, so we track it as removed
	// even if releasing its resources could fail below.
	s.metrics.blockDrops.Inc()

	if err := b.Close(); err != nil {
		return errors.Wrap(err, "close block")
	}
	if err := os.RemoveAll(b.dir); err != nil {
		return errors.Wrap(err, "delete block")
	}
	return nil
}

func (s *BucketStore) removeAllBlocks() error {
	// Build a list of blocks to remove.
	s.blocksMx.Lock()
	blockIDs := make([]ulid.ULID, 0, len(s.blocks))
	for id := range s.blocks {
		blockIDs = append(blockIDs, id)
	}
	s.blocksMx.Unlock()

	// Close all blocks.
	errs := multierror.New()

	for _, id := range blockIDs {
		if err := s.removeBlock(id); err != nil {
			errs.Add(errors.Wrap(err, fmt.Sprintf("block: %s", id.String())))
		}
	}

	return errs.Err()
}

// TimeRange returns the minimum and maximum timestamp of data available in the store.
func (s *BucketStore) TimeRange() (mint, maxt int64) {
	s.blocksMx.RLock()
	defer s.blocksMx.RUnlock()

	mint = math.MaxInt64
	maxt = math.MinInt64

	for _, b := range s.blocks {
		if b.meta.MinTime < mint {
			mint = b.meta.MinTime
		}
		if b.meta.MaxTime > maxt {
			maxt = b.meta.MaxTime
		}
	}

	return mint, maxt
}

type seriesEntry struct {
	lset labels.Labels
	refs []chunks.ChunkRef
	chks []storepb.AggrChunk
}

type bucketSeriesSet struct {
	set []seriesEntry
	i   int
	err error
}

func newBucketSeriesSet(set []seriesEntry) *bucketSeriesSet {
	return &bucketSeriesSet{
		set: set,
		i:   -1,
	}
}

func (s *bucketSeriesSet) Next() bool {
	if s.i >= len(s.set)-1 {
		return false
	}
	s.i++
	return true
}

func (s *bucketSeriesSet) At() (labels.Labels, []storepb.AggrChunk) {
	return s.set[s.i].lset, s.set[s.i].chks
}

func (s *bucketSeriesSet) Err() error {
	return s.err
}

// blockSeries returns series matching given matchers, that have some data in given time range.
// If skipChunks is provided, then provided minTime and maxTime are ignored and search is performed over the entire
// block to make the result cacheable.
func blockSeries(
	ctx context.Context,
	indexr *bucketIndexReader, // Index reader for block.
	chunkr *bucketChunkReader, // Chunk reader for block.
	chunksPool *pool.BatchBytes, // Pool used to get memory buffers to store chunks. Required only if !skipChunks.
	matchers []*labels.Matcher, // Series matchers.
	shard *sharding.ShardSelector, // Shard selector.
	seriesHashCache *hashcache.BlockSeriesHashCache, // Block-specific series hash cache (used only if shard selector is specified).
	chunksLimiter ChunksLimiter, // Rate limiter for loading chunks.
	seriesLimiter SeriesLimiter, // Rate limiter for loading series.
	skipChunks bool, // If true, chunks are not loaded and minTime/maxTime are ignored.
	minTime, maxTime int64, // Series must have data in this time range to be returned (ignored if skipChunks=true).
	logger log.Logger,
) (storepb.SeriesSet, *safeQueryStats, error) {
	span, ctx := tracing.StartSpan(ctx, "blockSeries()")
	span.LogKV(
		"block ID", indexr.block.meta.ULID.String(),
		"block min time", time.UnixMilli(indexr.block.meta.MinTime).UTC().Format(time.RFC3339Nano),
		"block max time", time.UnixMilli(indexr.block.meta.MinTime).UTC().Format(time.RFC3339Nano),
	)
	defer span.Finish()

	reqStats := newSafeQueryStats()

	if skipChunks {
		span.LogKV("msg", "manipulating mint/maxt to cover the entire block as skipChunks=true")
		minTime, maxTime = indexr.block.meta.MinTime, indexr.block.meta.MaxTime

		res, ok := fetchCachedSeries(ctx, indexr.block.userID, indexr.block.indexCache, indexr.block.meta.ULID, matchers, shard, logger)
		if ok {
			span.LogKV("msg", "using cached result", "len", len(res))
			return newBucketSeriesSet(res), reqStats, nil
		}
	}

	ps, err := indexr.ExpandedPostings(ctx, matchers, reqStats)
	if err != nil {
		return nil, nil, errors.Wrap(err, "expanded matching posting")
	}

	// We can't compute the series hash yet because we're still missing the series labels.
	// However, if the hash is already in the cache, then we can remove all postings for series
	// not belonging to the shard.
	var seriesCacheStats queryStats
	if shard != nil {
		ps, seriesCacheStats = filterPostingsByCachedShardHash(ps, shard, seriesHashCache)
	}

	if len(ps) == 0 {
		return storepb.EmptySeriesSet(), reqStats, nil
	}

	// Preload all series index data.
	// TODO(bwplotka): Consider not keeping all series in memory all the time.
	// TODO(bwplotka): Do lazy loading in one step as `ExpandingPostings` method.
	loadedSeries, err := indexr.preloadSeries(ctx, ps, reqStats)
	if err != nil {
		return nil, nil, errors.Wrap(err, "preload series")
	}

	// Transform all series into the response types and mark their relevant chunks
	// for preloading.
	var (
		res       []seriesEntry
		lookupErr error
	)

	tracing.DoWithSpan(ctx, "blockSeries() lookup series", func(ctx context.Context, span opentracing.Span) {
		var (
			symbolizedLset []symbolizedLabel
			chks           []chunks.Meta
			postingsStats  = &queryStats{}
		)

		// Keep track of postings lookup stats in a dedicated stats structure that doesn't require lock
		// and then merge it once done. We do it to avoid the lock overhead because unsafeLoadSeriesForTime()
		// may be called many times.
		defer func() {
			reqStats.merge(postingsStats)
		}()

		for _, id := range ps {
			ok, err := loadedSeries.unsafeLoadSeriesForTime(id, &symbolizedLset, &chks, skipChunks, minTime, maxTime, postingsStats)
			if err != nil {
				lookupErr = errors.Wrap(err, "read series")
				return
			}
			if !ok {
				// No matching chunks for this time duration, skip series.
				continue
			}

			lset, err := indexr.LookupLabelsSymbols(symbolizedLset)
			if err != nil {
				lookupErr = errors.Wrap(err, "lookup labels symbols")
				return
			}

			// Skip the series if it doesn't belong to the shard.
			if shard != nil {
				hash, ok := seriesHashCache.Fetch(id)
				seriesCacheStats.seriesHashCacheRequests++

				if !ok {
					hash = lset.Hash()
					seriesHashCache.Store(id, hash)
				} else {
					seriesCacheStats.seriesHashCacheHits++
				}

				if hash%shard.ShardCount != shard.ShardIndex {
					continue
				}
			}

			// Check series limit after filtering out series not belonging to the requested shard (if any).
			if err := seriesLimiter.Reserve(1); err != nil {
				lookupErr = errors.Wrap(err, "exceeded series limit")
				return
			}

			s := seriesEntry{lset: lset}

			if !skipChunks {
				// Schedule loading chunks.
				s.refs = make([]chunks.ChunkRef, 0, len(chks))
				s.chks = make([]storepb.AggrChunk, 0, len(chks))
				for j, meta := range chks {
					// seriesEntry s is appended to res, but not at every outer loop iteration,
					// therefore len(res) is the index we need here, not outer loop iteration number.
					if err := chunkr.addLoad(meta.Ref, len(res), j); err != nil {
						lookupErr = errors.Wrap(err, "add chunk load")
						return
					}
					s.chks = append(s.chks, storepb.AggrChunk{
						MinTime: meta.MinTime,
						MaxTime: meta.MaxTime,
					})
					s.refs = append(s.refs, meta.Ref)
				}

				// Ensure sample limit through chunksLimiter if we return chunks.
				if err := chunksLimiter.Reserve(uint64(len(s.chks))); err != nil {
					lookupErr = errors.Wrap(err, "exceeded chunks limit")
					return
				}
			}

			res = append(res, s)
		}
	})

	if lookupErr != nil {
		return nil, nil, lookupErr
	}

	if skipChunks {
		storeCachedSeries(ctx, indexr.block.indexCache, indexr.block.userID, indexr.block.meta.ULID, matchers, shard, res, logger)
		reqStats.merge(&seriesCacheStats)
		return newBucketSeriesSet(res), reqStats, nil
	}

	if err := chunkr.load(res, chunksPool, reqStats); err != nil {
		return nil, nil, errors.Wrap(err, "load chunks")
	}

	reqStats.merge(&seriesCacheStats)
	return newBucketSeriesSet(res), reqStats, nil
}

type seriesCacheEntry struct {
	LabelSets   []labels.Labels
	MatchersKey indexcache.LabelMatchersKey
	Shard       sharding.ShardSelector
}

func fetchCachedSeries(ctx context.Context, userID string, indexCache indexcache.IndexCache, blockID ulid.ULID, matchers []*labels.Matcher, shard *sharding.ShardSelector, logger log.Logger) ([]seriesEntry, bool) {
	matchersKey := indexcache.CanonicalLabelMatchersKey(matchers)
	data, ok := indexCache.FetchSeries(ctx, userID, blockID, matchersKey, shard)
	if !ok {
		return nil, false
	}
	var entry seriesCacheEntry
	if err := decodeSnappyGob(data, &entry); err != nil {
		level.Warn(spanlogger.FromContext(ctx, logger)).Log("msg", "can't decode series cache", "err", err)
		return nil, false
	}
	if entry.MatchersKey != matchersKey {
		level.Debug(spanlogger.FromContext(ctx, logger)).Log("msg", "cached series entry key doesn't match, possible collision", "cached_key", entry.MatchersKey, "requested_key", matchersKey)
		return nil, false
	}
	if entry.Shard != maybeNilShard(shard) {
		level.Debug(spanlogger.FromContext(ctx, logger)).Log("msg", "cached series shard doesn't match, possible collision", "cached_shard", entry.Shard, "requested_shard", maybeNilShard(shard))
		return nil, false
	}

	res := make([]seriesEntry, len(entry.LabelSets))
	for i, lset := range entry.LabelSets {
		res[i].lset = lset
	}
	return res, true
}

func storeCachedSeries(ctx context.Context, indexCache indexcache.IndexCache, userID string, blockID ulid.ULID, matchers []*labels.Matcher, shard *sharding.ShardSelector, series []seriesEntry, logger log.Logger) {
	entry := seriesCacheEntry{
		LabelSets:   make([]labels.Labels, len(series)),
		MatchersKey: indexcache.CanonicalLabelMatchersKey(matchers),
		Shard:       maybeNilShard(shard),
	}
	for i, s := range series {
		entry.LabelSets[i] = s.lset
	}
	data, err := encodeSnappyGob(entry)
	if err != nil {
		level.Error(spanlogger.FromContext(ctx, logger)).Log("msg", "can't encode series for caching", "err", err)
		return
	}
	indexCache.StoreSeries(ctx, userID, blockID, entry.MatchersKey, shard, data)
}

// debugFoundBlockSetOverview logs on debug level what exactly blocks we used for query in terms of
// labels and resolution. This is important because we allow mixed resolution results, so it is quite crucial
// to be aware what exactly resolution we see on query.
// TODO(bplotka): Consider adding resolution label to all results to propagate that info to UI and Query API.
func debugFoundBlockSetOverview(logger log.Logger, mint, maxt, maxResolutionMillis int64, bs []*bucketBlock) {
	if len(bs) == 0 {
		level.Debug(logger).Log("msg", "No block found", "mint", mint, "maxt", maxt)
		return
	}

	var (
		parts            []string
		currRes          = int64(-1)
		currMin, currMax int64
	)
	for _, b := range bs {
		if currRes == b.meta.Thanos.Downsample.Resolution {
			currMax = b.meta.MaxTime
			continue
		}

		if currRes != -1 {
			parts = append(parts, fmt.Sprintf("Range: %d-%d Resolution: %d", currMin, currMax, currRes))
		}

		currRes = b.meta.Thanos.Downsample.Resolution
		currMin = b.meta.MinTime
		currMax = b.meta.MaxTime
	}

	parts = append(parts, fmt.Sprintf("Range: %d-%d Resolution: %d", currMin, currMax, currRes))

	level.Debug(logger).Log("msg", "Blocks source resolutions", "blocks", len(bs), "Maximum Resolution", maxResolutionMillis, "mint", mint, "maxt", maxt, "spans", strings.Join(parts, "\n"))
}

// Series implements the storepb.StoreServer interface.
func (s *BucketStore) Series(req *storepb.SeriesRequest, srv storepb.Store_SeriesServer) (err error) {
	if s.queryGate != nil {
		tracing.DoWithSpan(srv.Context(), "store_query_gate_ismyturn", func(ctx context.Context, _ tracing.Span) {
			err = s.queryGate.Start(srv.Context())
		})
		if err != nil {
			return errors.Wrapf(err, "failed to wait for turn")
		}

		defer s.queryGate.Done()
	}

	matchers, err := storepb.MatchersToPromMatchers(req.Matchers...)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	// Check if matchers include the query shard selector.
	shardSelector, matchers, err := sharding.RemoveShardFromMatchers(matchers)
	if err != nil {
		return status.Error(codes.InvalidArgument, errors.Wrap(err, "parse query sharding label").Error())
	}

	spanLogger := spanlogger.FromContext(srv.Context(), s.logger)
	level.Debug(spanLogger).Log(
		"msg", "BucketStore.Series",
		"request min time", time.UnixMilli(req.MinTime).UTC().Format(time.RFC3339Nano),
		"request max time", time.UnixMilli(req.MaxTime).UTC().Format(time.RFC3339Nano),
		"request matchers", storepb.PromMatchersToString(matchers...),
		"request shard selector", maybeNilShard(shardSelector).LabelValue(),
	)

	var (
		ctx              = srv.Context()
		stats            = newSafeQueryStats()
		res              []storepb.SeriesSet
		resHints         = &hintspb.SeriesResponseHints{}
		reqBlockMatchers []*labels.Matcher
		chunksLimiter    = s.chunksLimiterFactory(s.metrics.queriesDropped.WithLabelValues("chunks"))
		seriesLimiter    = s.seriesLimiterFactory(s.metrics.queriesDropped.WithLabelValues("series"))
		chunksPool       = &pool.BatchBytes{Delegate: s.chunkPool}
	)
	defer chunksPool.Release()
	defer s.recordSeriesCallResult(stats)

	if req.Hints != nil {
		reqHints := &hintspb.SeriesRequestHints{}
		if err := types.UnmarshalAny(req.Hints, reqHints); err != nil {
			return status.Error(codes.InvalidArgument, errors.Wrap(err, "unmarshal series request hints").Error())
		}

		reqBlockMatchers, err = storepb.MatchersToPromMatchers(reqHints.BlockMatchers...)
		if err != nil {
			return status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request hints labels matchers").Error())
		}
	}

	span, ctx := tracing.StartSpan(ctx, "bucket_store_preload_all")

	blocks, indexReaders, chunkReaders := s.openBlocksForReading(ctx, req.SkipChunks, req.MinTime, req.MaxTime, req.MaxResolutionWindow, reqBlockMatchers)
	// We must keep the readers open until all their data has been sent.
	for _, r := range indexReaders {
		defer runutil.CloseWithLogOnErr(s.logger, r, "close block index reader")
	}
	for _, r := range chunkReaders {
		defer runutil.CloseWithLogOnErr(s.logger, r, "close block chunk reader")
	}

	res, cleanup, err := s.synchronousSeriesSet(ctx, req, stats, blocks, indexReaders, chunkReaders, chunksPool, resHints, shardSelector, matchers, chunksLimiter, seriesLimiter)
	if cleanup != nil {
		defer cleanup()
	}
	span.Finish()

	if err != nil {
		return err
	}

	// Merge the sub-results from each selected block.
	mergeStats := &queryStats{}
	tracing.DoWithSpan(ctx, "bucket_store_merge_all", func(ctx context.Context, _ tracing.Span) {
		begin := time.Now()

		// NOTE: We "carefully" assume series and chunks are sorted within each SeriesSet. This should be guaranteed by
		// blockSeries method. In worst case deduplication logic won't deduplicate correctly, which will be accounted later.
		set := storepb.MergeSeriesSets(res...)
		for set.Next() {
			var series storepb.Series

			mergeStats.mergedSeriesCount++

			var lset labels.Labels
			if req.SkipChunks {
				lset, _ = set.At()
			} else {
				lset, series.Chunks = set.At()
				mergeStats.mergedChunksCount += len(series.Chunks)
				s.metrics.chunkSizeBytes.Observe(float64(chunksSize(series.Chunks)))
			}
			series.Labels = mimirpb.FromLabelsToLabelAdapters(lset)
			if err = srv.Send(storepb.NewSeriesResponse(&series)); err != nil {
				err = status.Error(codes.Unknown, errors.Wrap(err, "send series response").Error())
				return
			}
		}
		if set.Err() != nil {
			err = status.Error(codes.Unknown, errors.Wrap(set.Err(), "expand series set").Error())
			return
		}
		mergeDuration := time.Since(begin)
		mergeStats.mergeDuration += mergeDuration
		s.metrics.seriesMergeDuration.Observe(mergeDuration.Seconds())

		err = nil
	})
	stats.merge(mergeStats)

	if err != nil {
		return
	}

	var anyHints *types.Any
	if anyHints, err = types.MarshalAny(resHints); err != nil {
		err = status.Error(codes.Unknown, errors.Wrap(err, "marshal series response hints").Error())
		return
	}

	if err = srv.Send(storepb.NewHintsSeriesResponse(anyHints)); err != nil {
		err = status.Error(codes.Unknown, errors.Wrap(err, "send series response hints").Error())
		return
	}

	unsafeStats := stats.export()
	if err = srv.Send(storepb.NewStatsResponse(unsafeStats.postingsFetchedSizeSum + unsafeStats.seriesFetchedSizeSum)); err != nil {
		err = status.Error(codes.Unknown, errors.Wrap(err, "sends series response stats").Error())
		return
	}

	return err
}

// synchronousSeriesSet returns seriesSet that contains the requested series. It returns a cleanup func. The cleanup func
// should be invoked always when non-nil; even when the returned error is non-nil.
func (s *BucketStore) synchronousSeriesSet(
	ctx context.Context,
	req *storepb.SeriesRequest,
	stats *safeQueryStats,
	blocks []*bucketBlock,
	indexReaders map[ulid.ULID]*bucketIndexReader,
	chunkReaders map[ulid.ULID]*bucketChunkReader,
	chunksPool *pool.BatchBytes,
	resHints *hintspb.SeriesResponseHints,
	shardSelector *sharding.ShardSelector,
	matchers []*labels.Matcher,
	chunksLimiter ChunksLimiter,
	seriesLimiter SeriesLimiter,
) ([]storepb.SeriesSet, func(), error) {
	var (
		resMtx   sync.Mutex
		res      []storepb.SeriesSet
		cleanups []func()
	)
	g, ctx := errgroup.WithContext(ctx)

	for _, b := range blocks {
		b := b

		// Keep track of queried blocks.
		resHints.AddQueriedBlock(b.meta.ULID)

		indexr := indexReaders[b.meta.ULID]
		chunkr := chunkReaders[b.meta.ULID]

		// If query sharding is enabled we have to get the block-specific series hash cache
		// which is used by blockSeries().
		var blockSeriesHashCache *hashcache.BlockSeriesHashCache
		if shardSelector != nil {
			blockSeriesHashCache = s.seriesHashCache.GetBlockCache(b.meta.ULID.String())
		}

		g.Go(func() error {
			part, pstats, err := blockSeries(
				ctx,
				indexr,
				chunkr,
				chunksPool,
				matchers,
				shardSelector,
				blockSeriesHashCache,
				chunksLimiter,
				seriesLimiter,
				req.SkipChunks,
				req.MinTime, req.MaxTime,
				s.logger,
			)
			if err != nil {
				return errors.Wrapf(err, "fetch series for block %s", b.meta.ULID)
			}

			stats.merge(pstats.export())
			resMtx.Lock()
			res = append(res, part)
			resMtx.Unlock()

			return nil
		})
	}

	cleanup := func() {
		// Iterate from last to first so that we mimic defer semantics
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}

	// Wait until data is fetched from all blocks
	begin := time.Now()
	err := g.Wait()
	if err != nil {
		code := codes.Aborted
		if s, ok := status.FromError(errors.Cause(err)); ok {
			code = s.Code()
		}
		return nil, cleanup, status.Error(code, err.Error())
	}

	getAllDuration := time.Since(begin)
	stats.update(func(stats *queryStats) {
		stats.blocksQueried = len(res)
		stats.getAllDuration = getAllDuration
	})
	s.metrics.seriesGetAllDuration.Observe(getAllDuration.Seconds())
	s.metrics.seriesBlocksQueried.Observe(float64(len(res)))

	return res, cleanup, err
}

func (s *BucketStore) recordSeriesCallResult(safeStats *safeQueryStats) {
	stats := safeStats.export()
	s.metrics.seriesDataTouched.WithLabelValues("postings").Observe(float64(stats.postingsTouched))
	s.metrics.seriesDataFetched.WithLabelValues("postings").Observe(float64(stats.postingsFetched))
	s.metrics.seriesDataSizeTouched.WithLabelValues("postings").Observe(float64(stats.postingsTouchedSizeSum))
	s.metrics.seriesDataSizeFetched.WithLabelValues("postings").Observe(float64(stats.postingsFetchedSizeSum))
	s.metrics.seriesDataTouched.WithLabelValues("series").Observe(float64(stats.seriesTouched))
	s.metrics.seriesDataFetched.WithLabelValues("series").Observe(float64(stats.seriesFetched))
	s.metrics.seriesDataSizeTouched.WithLabelValues("series").Observe(float64(stats.seriesTouchedSizeSum))
	s.metrics.seriesDataSizeFetched.WithLabelValues("series").Observe(float64(stats.seriesFetchedSizeSum))
	s.metrics.seriesDataTouched.WithLabelValues("chunks").Observe(float64(stats.chunksTouched))
	s.metrics.seriesDataFetched.WithLabelValues("chunks").Observe(float64(stats.chunksFetched))
	s.metrics.seriesDataSizeTouched.WithLabelValues("chunks").Observe(float64(stats.chunksTouchedSizeSum))
	s.metrics.seriesDataSizeFetched.WithLabelValues("chunks").Observe(float64(stats.chunksFetchedSizeSum))
	s.metrics.resultSeriesCount.Observe(float64(stats.mergedSeriesCount))
	s.metrics.cachedPostingsCompressions.WithLabelValues(labelEncode).Add(float64(stats.cachedPostingsCompressions))
	s.metrics.cachedPostingsCompressions.WithLabelValues(labelDecode).Add(float64(stats.cachedPostingsDecompressions))
	s.metrics.cachedPostingsCompressionErrors.WithLabelValues(labelEncode).Add(float64(stats.cachedPostingsCompressionErrors))
	s.metrics.cachedPostingsCompressionErrors.WithLabelValues(labelDecode).Add(float64(stats.cachedPostingsDecompressionErrors))
	s.metrics.cachedPostingsCompressionTimeSeconds.WithLabelValues(labelEncode).Add(stats.cachedPostingsCompressionTimeSum.Seconds())
	s.metrics.cachedPostingsCompressionTimeSeconds.WithLabelValues(labelDecode).Add(stats.cachedPostingsDecompressionTimeSum.Seconds())
	s.metrics.cachedPostingsOriginalSizeBytes.Add(float64(stats.cachedPostingsOriginalSizeSum))
	s.metrics.cachedPostingsCompressedSizeBytes.Add(float64(stats.cachedPostingsCompressedSizeSum))
	s.metrics.seriesHashCacheRequests.Add(float64(stats.seriesHashCacheRequests))
	s.metrics.seriesHashCacheHits.Add(float64(stats.seriesHashCacheHits))
}

func chunksSize(chks []storepb.AggrChunk) (size int) {
	for _, chk := range chks {
		size += chk.Size() // This gets the encoded proto size.
	}
	return size
}

func (s *BucketStore) openBlocksForReading(ctx context.Context, skipChunks bool, minT, maxT, maxResolutionMillis int64, blockMatchers []*labels.Matcher) ([]*bucketBlock, map[ulid.ULID]*bucketIndexReader, map[ulid.ULID]*bucketChunkReader) {
	s.blocksMx.RLock()
	defer s.blocksMx.RUnlock()

	// Find all blocks owned by this store-gateway instance and matching the request.
	blocks := s.blockSet.getFor(minT, maxT, maxResolutionMillis, blockMatchers)
	if s.debugLogging {
		debugFoundBlockSetOverview(s.logger, minT, maxT, maxResolutionMillis, blocks)
	}

	indexReaders := make(map[ulid.ULID]*bucketIndexReader, len(blocks))
	for _, b := range blocks {
		indexReaders[b.meta.ULID] = b.indexReader()
	}
	if skipChunks {
		return blocks, indexReaders, nil
	}

	chunkReaders := make(map[ulid.ULID]*bucketChunkReader, len(blocks))
	for _, b := range blocks {
		chunkReaders[b.meta.ULID] = b.chunkReader(ctx)
	}

	return blocks, indexReaders, chunkReaders
}

// LabelNames implements the storepb.StoreServer interface.
func (s *BucketStore) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	reqSeriesMatchers, err := storepb.MatchersToPromMatchers(req.Matchers...)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request labels matchers").Error())
	}

	resHints := &hintspb.LabelNamesResponseHints{}

	var reqBlockMatchers []*labels.Matcher
	if req.Hints != nil {
		reqHints := &hintspb.LabelNamesRequestHints{}
		err := types.UnmarshalAny(req.Hints, reqHints)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "unmarshal label names request hints").Error())
		}

		reqBlockMatchers, err = storepb.MatchersToPromMatchers(reqHints.BlockMatchers...)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request hints labels matchers").Error())
		}
	}

	g, gctx := errgroup.WithContext(ctx)

	s.blocksMx.RLock()

	var mtx sync.Mutex
	var sets [][]string
	seriesLimiter := s.seriesLimiterFactory(s.metrics.queriesDropped.WithLabelValues("series"))

	for _, b := range s.blocks {
		b := b
		if !b.overlapsClosedInterval(req.Start, req.End) {
			continue
		}
		if len(reqBlockMatchers) > 0 && !b.matchLabels(reqBlockMatchers) {
			continue
		}

		resHints.AddQueriedBlock(b.meta.ULID)

		indexr := b.indexReader()

		g.Go(func() error {
			defer runutil.CloseWithLogOnErr(s.logger, indexr, "label names")

			result, err := blockLabelNames(gctx, indexr, reqSeriesMatchers, seriesLimiter, s.logger)
			if err != nil {
				return errors.Wrapf(err, "block %s", b.meta.ULID)
			}

			if len(result) > 0 {
				mtx.Lock()
				sets = append(sets, result)
				mtx.Unlock()
			}

			return nil
		})
	}

	s.blocksMx.RUnlock()

	if err := g.Wait(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	anyHints, err := types.MarshalAny(resHints)
	if err != nil {
		return nil, status.Error(codes.Unknown, errors.Wrap(err, "marshal label names response hints").Error())
	}

	return &storepb.LabelNamesResponse{
		Names: util.MergeSlices(sets...),
		Hints: anyHints,
	}, nil
}

func blockLabelNames(ctx context.Context, indexr *bucketIndexReader, matchers []*labels.Matcher, seriesLimiter SeriesLimiter, logger log.Logger) ([]string, error) {
	names, ok := fetchCachedLabelNames(ctx, indexr.block.indexCache, indexr.block.userID, indexr.block.meta.ULID, matchers, logger)
	if ok {
		return names, nil
	}

	if len(matchers) == 0 {
		// Do it via index reader to have pending reader registered correctly.
		// LabelNames are already sorted.
		names, err := indexr.block.indexHeaderReader.LabelNames()
		if err != nil {
			return nil, errors.Wrap(err, "label names")
		}
		storeCachedLabelNames(ctx, indexr.block.indexCache, indexr.block.userID, indexr.block.meta.ULID, matchers, names, logger)
		return names, nil
	}

	// We ignore request's min/max time and query the entire block to make the result cacheable.
	minTime, maxTime := indexr.block.meta.MinTime, indexr.block.meta.MaxTime
	seriesSet, _, err := blockSeries(ctx, indexr, nil, nil, matchers, nil, nil, nil, seriesLimiter, true, minTime, maxTime, logger)
	if err != nil {
		return nil, errors.Wrap(err, "fetch series")
	}

	// Extract label names from all series. Many label names will be the same, so we need to deduplicate them.
	labelNames := map[string]struct{}{}
	for seriesSet.Next() {
		ls, _ := seriesSet.At()
		for _, l := range ls {
			labelNames[l.Name] = struct{}{}
		}
	}
	if seriesSet.Err() != nil {
		return nil, errors.Wrap(seriesSet.Err(), "iterate series")
	}

	names = make([]string, 0, len(labelNames))
	for n := range labelNames {
		names = append(names, n)
	}
	sort.Strings(names)

	storeCachedLabelNames(ctx, indexr.block.indexCache, indexr.block.userID, indexr.block.meta.ULID, matchers, names, logger)
	return names, nil
}

type labelNamesCacheEntry struct {
	Names       []string
	MatchersKey indexcache.LabelMatchersKey
}

func fetchCachedLabelNames(ctx context.Context, indexCache indexcache.IndexCache, userID string, blockID ulid.ULID, matchers []*labels.Matcher, logger log.Logger) ([]string, bool) {
	matchersKey := indexcache.CanonicalLabelMatchersKey(matchers)
	data, ok := indexCache.FetchLabelNames(ctx, userID, blockID, matchersKey)
	if !ok {
		return nil, false
	}
	var entry labelNamesCacheEntry
	if err := decodeSnappyGob(data, &entry); err != nil {
		level.Warn(spanlogger.FromContext(ctx, logger)).Log("msg", "can't decode label name cache", "err", err)
		return nil, false
	}
	if entry.MatchersKey != matchersKey {
		level.Debug(spanlogger.FromContext(ctx, logger)).Log("msg", "cached label names entry key doesn't match, possible collision", "cached_key", entry.MatchersKey, "requested_key", matchersKey)
		return nil, false
	}

	return entry.Names, true
}

func storeCachedLabelNames(ctx context.Context, indexCache indexcache.IndexCache, userID string, blockID ulid.ULID, matchers []*labels.Matcher, values []string, logger log.Logger) {
	entry := labelNamesCacheEntry{
		Names:       values,
		MatchersKey: indexcache.CanonicalLabelMatchersKey(matchers),
	}
	data, err := encodeSnappyGob(entry)
	if err != nil {
		level.Error(spanlogger.FromContext(ctx, logger)).Log("msg", "can't encode label names for caching", "err", err)
		return
	}
	indexCache.StoreLabelNames(ctx, userID, blockID, entry.MatchersKey, data)
}

// LabelValues implements the storepb.StoreServer interface.
func (s *BucketStore) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	reqSeriesMatchers, err := storepb.MatchersToPromMatchers(req.Matchers...)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request labels matchers").Error())
	}

	resHints := &hintspb.LabelValuesResponseHints{}

	g, gctx := errgroup.WithContext(ctx)

	var reqBlockMatchers []*labels.Matcher
	if req.Hints != nil {
		reqHints := &hintspb.LabelValuesRequestHints{}
		err := types.UnmarshalAny(req.Hints, reqHints)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "unmarshal label values request hints").Error())
		}

		reqBlockMatchers, err = storepb.MatchersToPromMatchers(reqHints.BlockMatchers...)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request hints labels matchers").Error())
		}
	}

	s.blocksMx.RLock()

	var mtx sync.Mutex
	var sets [][]string
	for _, b := range s.blocks {
		b := b

		if !b.overlapsClosedInterval(req.Start, req.End) {
			continue
		}
		if len(reqBlockMatchers) > 0 && !b.matchLabels(reqBlockMatchers) {
			continue
		}

		resHints.AddQueriedBlock(b.meta.ULID)

		indexr := b.indexReader()

		g.Go(func() error {
			defer runutil.CloseWithLogOnErr(s.logger, indexr, "label values")

			result, err := blockLabelValues(gctx, indexr, req.Label, reqSeriesMatchers, s.logger, newSafeQueryStats())
			if err != nil {
				return errors.Wrapf(err, "block %s", b.meta.ULID)
			}

			if len(result) > 0 {
				mtx.Lock()
				sets = append(sets, result)
				mtx.Unlock()
			}

			return nil
		})
	}

	s.blocksMx.RUnlock()

	if err := g.Wait(); err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}

	anyHints, err := types.MarshalAny(resHints)
	if err != nil {
		return nil, status.Error(codes.Unknown, errors.Wrap(err, "marshal label values response hints").Error())
	}

	return &storepb.LabelValuesResponse{
		Values: util.MergeSlices(sets...),
		Hints:  anyHints,
	}, nil
}

// blockLabelValues provides the values of the label with requested name,
// optionally restricting the search to the series that match the matchers provided.
// - First we fetch all possible values for this label from the index.
//   - If no matchers were provided, we just return those values.
//
// - Next we load the postings (references to series) for supplied matchers.
// - Then we load the postings for each label-value fetched in the first step.
// - Finally, we check if postings from each label-value intersect postings from matchers.
//   - A non empty intersection means that a matched series has that value, so we add it to the result.
//
// Notice that when no matchers are provided, the list of matched postings is AllPostings,
// so we could also intersect those with each label's postings being each one non empty and leading to the same result.
func blockLabelValues(ctx context.Context, indexr *bucketIndexReader, labelName string, matchers []*labels.Matcher, logger log.Logger, stats *safeQueryStats) ([]string, error) {
	values, ok := fetchCachedLabelValues(ctx, indexr.block.indexCache, indexr.block.userID, indexr.block.meta.ULID, labelName, matchers, logger)
	if ok {
		return values, nil
	}

	// TODO: if matchers contains labelName, we could use it to filter out label values here.
	allValues, err := indexr.block.indexHeaderReader.LabelValues(labelName, nil)
	if err != nil {
		return nil, errors.Wrap(err, "index header label values")
	}

	if len(matchers) == 0 {
		storeCachedLabelValues(ctx, indexr.block.indexCache, indexr.block.userID, indexr.block.meta.ULID, labelName, matchers, allValues, logger)
		return allValues, nil
	}

	p, err := indexr.ExpandedPostings(ctx, matchers, stats)
	if err != nil {
		return nil, errors.Wrap(err, "expanded postings")
	}

	keys := make([]labels.Label, len(allValues))
	for i, value := range allValues {
		keys[i] = labels.Label{Name: labelName, Value: value}
	}

	fetchedPostings, err := indexr.FetchPostings(ctx, keys, stats)
	if err != nil {
		return nil, errors.Wrap(err, "get postings")
	}

	matched := make([]string, 0, len(allValues))
	for i, value := range allValues {
		intersection := index.Intersect(index.NewListPostings(p), fetchedPostings[i])
		if intersection.Next() {
			matched = append(matched, value)
		}
		if err := intersection.Err(); err != nil {
			return nil, errors.Wrapf(err, "intersecting value %q postings", value)
		}
	}

	storeCachedLabelValues(ctx, indexr.block.indexCache, indexr.block.userID, indexr.block.meta.ULID, labelName, matchers, matched, logger)
	return matched, nil
}

type labelValuesCacheEntry struct {
	Values      []string
	LabelName   string
	MatchersKey indexcache.LabelMatchersKey
}

func fetchCachedLabelValues(ctx context.Context, indexCache indexcache.IndexCache, userID string, blockID ulid.ULID, labelName string, matchers []*labels.Matcher, logger log.Logger) ([]string, bool) {
	matchersKey := indexcache.CanonicalLabelMatchersKey(matchers)
	data, ok := indexCache.FetchLabelValues(ctx, userID, blockID, labelName, matchersKey)
	if !ok {
		return nil, false
	}
	var entry labelValuesCacheEntry
	if err := decodeSnappyGob(data, &entry); err != nil {
		level.Warn(spanlogger.FromContext(ctx, logger)).Log("msg", "can't decode label values cache", "err", err)
		return nil, false
	}
	if entry.LabelName != labelName {
		level.Debug(spanlogger.FromContext(ctx, logger)).Log("msg", "cached label values entry label name doesn't match, possible collision", "cached_label_name", entry.LabelName, "requested_label_name", labelName)
		return nil, false
	}
	if entry.MatchersKey != matchersKey {
		level.Debug(spanlogger.FromContext(ctx, logger)).Log("msg", "cached label values entry key doesn't match, possible collision", "cached_key", entry.MatchersKey, "requested_key", matchersKey)
		return nil, false
	}

	return entry.Values, true
}

func storeCachedLabelValues(ctx context.Context, indexCache indexcache.IndexCache, userID string, blockID ulid.ULID, labelName string, matchers []*labels.Matcher, values []string, logger log.Logger) {
	entry := labelValuesCacheEntry{
		Values:      values,
		LabelName:   labelName,
		MatchersKey: indexcache.CanonicalLabelMatchersKey(matchers),
	}
	data, err := encodeSnappyGob(entry)
	if err != nil {
		level.Error(spanlogger.FromContext(ctx, logger)).Log("msg", "can't encode label values for caching", "err", err)
		return
	}
	indexCache.StoreLabelValues(ctx, userID, blockID, labelName, entry.MatchersKey, data)
}

// bucketBlockSet holds all blocks of an equal label set. It internally splits
// them up by downsampling resolution and allows querying.
type bucketBlockSet struct {
	mtx         sync.RWMutex
	resolutions []int64          // Available resolution, high to low (in milliseconds).
	blocks      [][]*bucketBlock // Ordered buckets for the existing resolutions.
}

// newBucketBlockSet initializes a new set with the known downsampling windows hard-configured.
// (Mimir only supports no-downsampling)
// The set currently does not support arbitrary ranges.
func newBucketBlockSet() *bucketBlockSet {
	return &bucketBlockSet{
		resolutions: []int64{0},
		blocks:      make([][]*bucketBlock, 3),
	}
}

func (s *bucketBlockSet) add(b *bucketBlock) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	i := int64index(s.resolutions, b.meta.Thanos.Downsample.Resolution)
	if i < 0 {
		return errors.Errorf("unsupported downsampling resolution %d", b.meta.Thanos.Downsample.Resolution)
	}
	bs := append(s.blocks[i], b)
	s.blocks[i] = bs

	// Always sort blocks by min time, then max time.
	sort.Slice(bs, func(j, k int) bool {
		if bs[j].meta.MinTime == bs[k].meta.MinTime {
			return bs[j].meta.MaxTime < bs[k].meta.MaxTime
		}
		return bs[j].meta.MinTime < bs[k].meta.MinTime
	})
	return nil
}

func (s *bucketBlockSet) remove(id ulid.ULID) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for i, bs := range s.blocks {
		for j, b := range bs {
			if b.meta.ULID != id {
				continue
			}
			s.blocks[i] = append(bs[:j], bs[j+1:]...)
			return
		}
	}
}

func int64index(s []int64, x int64) int {
	for i, v := range s {
		if v == x {
			return i
		}
	}
	return -1
}

// getFor returns a time-ordered list of blocks that cover date between mint and maxt.
// Blocks with the biggest resolution possible but not bigger than the given max resolution are returned.
// It supports overlapping blocks.
//
// NOTE: s.blocks are expected to be sorted in minTime order.
func (s *bucketBlockSet) getFor(mint, maxt, maxResolutionMillis int64, blockMatchers []*labels.Matcher) (bs []*bucketBlock) {
	if mint > maxt {
		return nil
	}

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	// Find first matching resolution.
	i := 0
	for ; i < len(s.resolutions) && s.resolutions[i] > maxResolutionMillis; i++ {
	}

	// Fill the given interval with the blocks for the current resolution.
	// Our current resolution might not cover all data, so recursively fill the gaps with higher resolution blocks
	// if there is any.
	start := mint
	for _, b := range s.blocks[i] {
		if b.meta.MaxTime <= mint {
			continue
		}
		// NOTE: Block intervals are half-open: [b.MinTime, b.MaxTime).
		if b.meta.MinTime > maxt {
			break
		}

		if i+1 < len(s.resolutions) {
			bs = append(bs, s.getFor(start, b.meta.MinTime-1, s.resolutions[i+1], blockMatchers)...)
		}

		// Include the block in the list of matching ones only if there are no block-level matchers
		// or they actually match.
		if len(blockMatchers) == 0 || b.matchLabels(blockMatchers) {
			bs = append(bs, b)
		}

		start = b.meta.MaxTime
	}

	if i+1 < len(s.resolutions) {
		bs = append(bs, s.getFor(start, maxt, s.resolutions[i+1], blockMatchers)...)
	}
	return bs
}

// bucketBlock represents a block that is located in a bucket. It holds intermediate
// state for the block on local disk.
type bucketBlock struct {
	userID     string
	logger     log.Logger
	metrics    *BucketStoreMetrics
	bkt        objstore.BucketReader
	meta       *metadata.Meta
	dir        string
	indexCache indexcache.IndexCache
	chunkPool  pool.Bytes

	indexHeaderReader indexheader.Reader

	chunkObjs []string

	pendingReaders sync.WaitGroup

	partitioner Partitioner

	// Block's labels used by block-level matchers to filter blocks to query. These are used to select blocks using
	// request hints' BlockMatchers.
	blockLabels labels.Labels

	expandedPostingsPromises sync.Map
}

func newBucketBlock(
	ctx context.Context,
	userID string,
	logger log.Logger,
	metrics *BucketStoreMetrics,
	meta *metadata.Meta,
	bkt objstore.BucketReader,
	dir string,
	indexCache indexcache.IndexCache,
	chunkPool pool.Bytes,
	indexHeadReader indexheader.Reader,
	p Partitioner,
) (b *bucketBlock, err error) {
	b = &bucketBlock{
		userID:            userID,
		logger:            logger,
		metrics:           metrics,
		bkt:               bkt,
		indexCache:        indexCache,
		chunkPool:         chunkPool,
		dir:               dir,
		partitioner:       p,
		meta:              meta,
		indexHeaderReader: indexHeadReader,
		// Inject the block ID as a label to allow to match blocks by ID.
		blockLabels: labels.FromStrings(block.BlockIDLabel, meta.ULID.String()),
	}

	// Get object handles for all chunk files (segment files) from meta.json, if available.
	if len(meta.Thanos.SegmentFiles) > 0 {
		b.chunkObjs = make([]string, 0, len(meta.Thanos.SegmentFiles))

		for _, sf := range meta.Thanos.SegmentFiles {
			b.chunkObjs = append(b.chunkObjs, path.Join(meta.ULID.String(), block.ChunksDirname, sf))
		}
		return b, nil
	}

	// Get object handles for all chunk files from storage.
	if err = bkt.Iter(ctx, path.Join(meta.ULID.String(), block.ChunksDirname), func(n string) error {
		b.chunkObjs = append(b.chunkObjs, n)
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "list chunk files")
	}
	return b, nil
}

func (b *bucketBlock) indexFilename() string {
	return path.Join(b.meta.ULID.String(), block.IndexFilename)
}

func (b *bucketBlock) readIndexRange(ctx context.Context, off, length int64) ([]byte, error) {
	r, err := b.bkt.GetRange(ctx, b.indexFilename(), off, length)
	if err != nil {
		return nil, errors.Wrap(err, "get range reader")
	}
	defer runutil.CloseWithLogOnErr(b.logger, r, "readIndexRange close range reader")

	// Preallocate the buffer with the exact size so we don't waste allocations
	// while progressively growing an initial small buffer. The buffer capacity
	// is increased by MinRead to avoid extra allocations due to how ReadFrom()
	// internally works.
	buf := bytes.NewBuffer(make([]byte, 0, length+bytes.MinRead))
	if _, err := buf.ReadFrom(r); err != nil {
		return nil, errors.Wrap(err, "read range")
	}
	return buf.Bytes(), nil
}

func (b *bucketBlock) readChunkRange(ctx context.Context, seq int, off, length int64, chunkRanges byteRanges) (*[]byte, error) {
	if seq < 0 || seq >= len(b.chunkObjs) {
		return nil, errors.Errorf("unknown segment file for index %d", seq)
	}

	// Get a reader for the required range.
	reader, err := b.bkt.GetRange(ctx, b.chunkObjs[seq], off, length)
	if err != nil {
		return nil, errors.Wrap(err, "get range reader")
	}
	defer runutil.CloseWithLogOnErr(b.logger, reader, "readChunkRange close range reader")

	// Get a buffer from the pool.
	chunkBuffer, err := b.chunkPool.Get(chunkRanges.size())
	if err != nil {
		return nil, errors.Wrap(err, "allocate chunk bytes")
	}

	*chunkBuffer, err = readByteRanges(reader, *chunkBuffer, chunkRanges)
	if err != nil {
		return nil, err
	}

	return chunkBuffer, nil
}

func (b *bucketBlock) chunkRangeReader(ctx context.Context, seq int, off, length int64) (io.ReadCloser, error) {
	if seq < 0 || seq >= len(b.chunkObjs) {
		return nil, errors.Errorf("unknown segment file for index %d", seq)
	}

	return b.bkt.GetRange(ctx, b.chunkObjs[seq], off, length)
}

func (b *bucketBlock) indexReader() *bucketIndexReader {
	b.pendingReaders.Add(1)
	return newBucketIndexReader(b)
}

func (b *bucketBlock) chunkReader(ctx context.Context) *bucketChunkReader {
	b.pendingReaders.Add(1)
	return newBucketChunkReader(ctx, b)
}

// matchLabels verifies whether the block matches the given matchers.
func (b *bucketBlock) matchLabels(matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if !m.Matches(b.blockLabels.Get(m.Name)) {
			return false
		}
	}
	return true
}

// overlapsClosedInterval returns true if the block overlaps [mint, maxt).
func (b *bucketBlock) overlapsClosedInterval(mint, maxt int64) bool {
	// The block itself is a half-open interval
	// [b.meta.MinTime, b.meta.MaxTime).
	return b.meta.MinTime <= maxt && mint < b.meta.MaxTime
}

// Close waits for all pending readers to finish and then closes all underlying resources.
func (b *bucketBlock) Close() error {
	b.pendingReaders.Wait()
	return b.indexHeaderReader.Close()
}

type labelValuesReader interface {
	LabelValues(name string, filter func(string) bool) ([]string, error)
}

type Part struct {
	Start uint64
	End   uint64

	ElemRng [2]int
}

type Partitioner interface {
	// Partition partitions length entries into n <= length ranges that cover all
	// input ranges
	// It supports overlapping ranges.
	// NOTE: It expects range to be sorted by start time.
	Partition(length int, rng func(int) (uint64, uint64)) []Part
}

type symbolizedLabel struct {
	name, value uint32
}

// decodeSeriesForTime decodes a series entry from the given byte slice decoding only chunk metas that are within given min and max time.
// If skipChunks is specified decodeSeriesForTime does not return any chunks, but only labels and only if at least single chunk is within time range.
// decodeSeriesForTime returns false, when there are no series data for given time range.
func decodeSeriesForTime(b []byte, lset *[]symbolizedLabel, chks *[]chunks.Meta, skipChunks bool, selectMint, selectMaxt int64) (ok bool, err error) {
	*lset = (*lset)[:0]
	*chks = (*chks)[:0]

	d := encoding.Decbuf{B: b}

	// Read labels without looking up symbols.
	k := d.Uvarint()
	for i := 0; i < k; i++ {
		lno := uint32(d.Uvarint())
		lvo := uint32(d.Uvarint())
		*lset = append(*lset, symbolizedLabel{name: lno, value: lvo})
	}
	// Read the chunks meta data.
	k = d.Uvarint()
	if k == 0 {
		return false, d.Err()
	}

	// First t0 is absolute, rest is just diff so different type is used (Uvarint64).
	mint := d.Varint64()
	maxt := int64(d.Uvarint64()) + mint
	// Similar for first ref.
	ref := int64(d.Uvarint64())

	for i := 0; i < k; i++ {
		if i > 0 {
			mint += int64(d.Uvarint64())
			maxt = int64(d.Uvarint64()) + mint
			ref += d.Varint64()
		}

		if mint > selectMaxt {
			break
		}

		if maxt >= selectMint {
			// Found a chunk.
			if skipChunks {
				// We are not interested in chunks and we know there is at least one, that's enough to return series.
				return true, nil
			}

			*chks = append(*chks, chunks.Meta{
				Ref:     chunks.ChunkRef(ref),
				MinTime: mint,
				MaxTime: maxt,
			})
		}

		mint = maxt
	}
	return len(*chks) > 0, d.Err()
}

// NewDefaultChunkBytesPool returns a chunk bytes pool with default settings.
func NewDefaultChunkBytesPool(maxChunkPoolBytes uint64) (pool.Bytes, error) {
	return pool.NewBucketedBytes(chunkBytesPoolMinSize, chunkBytesPoolMaxSize, 2, maxChunkPoolBytes)
}

func maybeNilShard(shard *sharding.ShardSelector) sharding.ShardSelector {
	if shard == nil {
		return sharding.ShardSelector{}
	}
	return *shard
}
