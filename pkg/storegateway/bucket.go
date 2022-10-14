// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
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
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/pool"
	"github.com/thanos-io/thanos/pkg/strutil"
	"github.com/thanos-io/thanos/pkg/tracing"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storage/sharding"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storegateway/hintspb"
	"github.com/grafana/mimir/pkg/storegateway/indexcache"
	"github.com/grafana/mimir/pkg/storegateway/indexheader"
	"github.com/grafana/mimir/pkg/storegateway/labelpb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/gate"
	util_math "github.com/grafana/mimir/pkg/util/math"
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
	mtx      sync.RWMutex
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

	// Enables hints in the Series() response.
	enableSeriesResponseHints bool
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
	enableSeriesResponseHints bool, // TODO(pracucci) Thanos 0.12 and below doesn't gracefully handle new fields in SeriesResponse. Drop this flag and always enable hints once we can drop backward compatibility.
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
		enableSeriesResponseHints:   enableSeriesResponseHints,
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

	s.mtx.RLock()
	stats.BlocksLoaded = len(s.blocks)
	s.mtx.RUnlock()

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
	s.mtx.RLock()
	defer s.mtx.RUnlock()
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

	s.mtx.Lock()
	defer s.mtx.Unlock()

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

	s.mtx.Lock()
	b, ok := s.blocks[id]
	if ok {
		s.blockSet.remove(id)
		delete(s.blocks, id)
	}
	s.mtx.Unlock()

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
	s.mtx.Lock()
	blockIDs := make([]ulid.ULID, 0, len(s.blocks))
	for id := range s.blocks {
		blockIDs = append(blockIDs, id)
	}
	s.mtx.Unlock()

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
	s.mtx.RLock()
	defer s.mtx.RUnlock()

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
	matchers []*labels.Matcher, // Series matchers.
	shard *sharding.ShardSelector, // Shard selector.
	seriesHashCache *hashcache.BlockSeriesHashCache, // Block-specific series hash cache (used only if shard selector is specified).
	chunksLimiter ChunksLimiter, // Rate limiter for loading chunks.
	seriesLimiter SeriesLimiter, // Rate limiter for loading series.
	skipChunks bool, // If true, chunks are not loaded and minTime/maxTime are ignored.
	minTime, maxTime int64, // Series must have data in this time range to be returned (ignored if skipChunks=true).
	loadAggregates []storepb.Aggr, // List of aggregates to load when loading chunks.
	logger log.Logger,
) (storepb.SeriesSet, *queryStats, error) {
	span, ctx := tracing.StartSpan(ctx, "blockSeries()")
	span.LogKV(
		"block ID", indexr.block.meta.ULID.String(),
		"block min time", time.UnixMilli(indexr.block.meta.MinTime).UTC().Format(time.RFC3339Nano),
		"block max time", time.UnixMilli(indexr.block.meta.MinTime).UTC().Format(time.RFC3339Nano),
	)
	defer span.Finish()

	if skipChunks {
		span.LogKV("msg", "manipulating mint/maxt to cover the entire block as skipChunks=true")
		minTime, maxTime = indexr.block.meta.MinTime, indexr.block.meta.MaxTime

		res, ok := fetchCachedSeries(ctx, indexr.block.userID, indexr.block.indexCache, indexr.block.meta.ULID, matchers, shard, logger)
		if ok {
			span.LogKV("msg", "using cached result", "len", len(res))
			return newBucketSeriesSet(res), &queryStats{}, nil
		}
	}

	ps, err := indexr.ExpandedPostings(ctx, matchers)
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
		return storepb.EmptySeriesSet(), indexr.stats, nil
	}

	// Preload all series index data.
	// TODO(bwplotka): Consider not keeping all series in memory all the time.
	// TODO(bwplotka): Do lazy loading in one step as `ExpandingPostings` method.
	if err := indexr.PreloadSeries(ctx, ps); err != nil {
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
		)
		for _, id := range ps {
			ok, err := indexr.LoadSeriesForTime(id, &symbolizedLset, &chks, skipChunks, minTime, maxTime)
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
		return newBucketSeriesSet(res), indexr.stats.merge(&seriesCacheStats), nil
	}

	if err := chunkr.load(res, loadAggregates); err != nil {
		return nil, nil, errors.Wrap(err, "load chunks")
	}

	return newBucketSeriesSet(res), indexr.stats.merge(chunkr.stats).merge(&seriesCacheStats), nil
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

// filterPostingsByCachedShardHash filters the input postings by the provided shard. It filters only
// postings for which we have their series hash already in the cache; if a series is not in the cache,
// postings will be kept in the output.
func filterPostingsByCachedShardHash(ps []storage.SeriesRef, shard *sharding.ShardSelector, seriesHashCache *hashcache.BlockSeriesHashCache) (filteredPostings []storage.SeriesRef, stats queryStats) {
	writeIdx := 0
	stats.seriesHashCacheRequests = len(ps)

	for readIdx := 0; readIdx < len(ps); readIdx++ {
		seriesID := ps[readIdx]
		hash, ok := seriesHashCache.Fetch(seriesID)
		if ok {
			stats.seriesHashCacheHits++
		}

		// Keep the posting if it's not in the cache, or it's in the cache and belongs to our shard.
		if !ok || hash%uint64(shard.ShardCount) == uint64(shard.ShardIndex) {
			ps[writeIdx] = seriesID
			writeIdx++
			continue
		}

		// We can filter out the series because doesn't belong to the requested shard,
		// so we're not going to increase the writeIdx.
	}

	// Shrink the size.
	ps = ps[:writeIdx]

	return ps, stats
}

func populateChunk(out *storepb.AggrChunk, in chunkenc.Chunk, aggrs []storepb.Aggr, save func([]byte) ([]byte, error)) error {
	if in.Encoding() == chunkenc.EncXOR {
		b, err := save(in.Bytes())
		if err != nil {
			return err
		}
		out.Raw = &storepb.Chunk{Type: storepb.Chunk_XOR, Data: b}
		return nil
	}
	return errors.Errorf("unsupported chunk encoding %d", in.Encoding())
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

// Series implements the storegatewaypb.StoreServer interface.
func (s *BucketStore) Series(req *storepb.SeriesRequest, srv storepb.Store_SeriesServer) (err error) {
	if s.queryGate != nil {
		tracing.DoInSpan(srv.Context(), "store_query_gate_ismyturn", func(ctx context.Context) {
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

	var (
		ctx              = srv.Context()
		stats            = &queryStats{}
		res              []storepb.SeriesSet
		mtx              sync.Mutex
		g, gctx          = errgroup.WithContext(ctx)
		resHints         = &hintspb.SeriesResponseHints{}
		reqBlockMatchers []*labels.Matcher
		chunksLimiter    = s.chunksLimiterFactory(s.metrics.queriesDropped.WithLabelValues("chunks"))
		seriesLimiter    = s.seriesLimiterFactory(s.metrics.queriesDropped.WithLabelValues("series"))
	)

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

	gspan, gctx := tracing.StartSpan(gctx, "bucket_store_preload_all")

	s.mtx.RLock()

	blocks := s.blockSet.getFor(req.MinTime, req.MaxTime, req.MaxResolutionWindow, reqBlockMatchers)

	spanLogger := spanlogger.FromContext(srv.Context(), s.logger)
	level.Debug(spanLogger).Log(
		"msg", "BucketStore.Series",
		"request min time", time.UnixMilli(req.MinTime).UTC().Format(time.RFC3339Nano),
		"request max time", time.UnixMilli(req.MaxTime).UTC().Format(time.RFC3339Nano),
		"request matchers", storepb.PromMatchersToString(matchers...),
		"request shard selector", maybeNilShard(shardSelector).LabelValue(),
	)

	if s.debugLogging {
		debugFoundBlockSetOverview(s.logger, req.MinTime, req.MaxTime, req.MaxResolutionWindow, blocks)
	}

	for _, b := range blocks {
		b := b

		if s.enableSeriesResponseHints {
			// Keep track of queried blocks.
			resHints.AddQueriedBlock(b.meta.ULID)
		}

		var chunkr *bucketChunkReader
		// We must keep the readers open until all their data has been sent.
		indexr := b.indexReader()
		if !req.SkipChunks {
			chunkr = b.chunkReader(gctx)
			defer runutil.CloseWithLogOnErr(s.logger, chunkr, "series block")
		}

		// Defer all closes to the end of Series method.
		defer runutil.CloseWithLogOnErr(s.logger, indexr, "series block")

		// If query sharding is enabled we have to get the block-specific series hash cache
		// which is used by blockSeries().
		var blockSeriesHashCache *hashcache.BlockSeriesHashCache
		if shardSelector != nil {
			blockSeriesHashCache = s.seriesHashCache.GetBlockCache(b.meta.ULID.String())
		}

		g.Go(func() error {
			part, pstats, err := blockSeries(
				gctx,
				indexr,
				chunkr,
				matchers,
				shardSelector,
				blockSeriesHashCache,
				chunksLimiter,
				seriesLimiter,
				req.SkipChunks,
				req.MinTime, req.MaxTime,
				req.Aggregates,
				s.logger,
			)
			if err != nil {
				return errors.Wrapf(err, "fetch series for block %s", b.meta.ULID)
			}

			mtx.Lock()
			res = append(res, part)
			stats = stats.merge(pstats)
			mtx.Unlock()

			return nil
		})
	}

	s.mtx.RUnlock()

	defer func() {
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

		level.Debug(s.logger).Log("msg", "stats query processed",
			"stats", fmt.Sprintf("%+v", stats), "err", err)
	}()

	// Concurrently get data from all blocks.
	{
		begin := time.Now()
		err = g.Wait()
		gspan.Finish()
		if err != nil {
			code := codes.Aborted
			if s, ok := status.FromError(errors.Cause(err)); ok {
				code = s.Code()
			}
			return status.Error(code, err.Error())
		}
		stats.blocksQueried = len(res)
		stats.getAllDuration = time.Since(begin)
		s.metrics.seriesGetAllDuration.Observe(stats.getAllDuration.Seconds())
		s.metrics.seriesBlocksQueried.Observe(float64(stats.blocksQueried))
	}
	// Merge the sub-results from each selected block.
	tracing.DoInSpan(ctx, "bucket_store_merge_all", func(ctx context.Context) {
		begin := time.Now()

		// NOTE: We "carefully" assume series and chunks are sorted within each SeriesSet. This should be guaranteed by
		// blockSeries method. In worst case deduplication logic won't deduplicate correctly, which will be accounted later.
		set := storepb.MergeSeriesSets(res...)
		for set.Next() {
			var series storepb.Series

			stats.mergedSeriesCount++

			var lset labels.Labels
			if req.SkipChunks {
				lset, _ = set.At()
			} else {
				lset, series.Chunks = set.At()

				stats.mergedChunksCount += len(series.Chunks)
				s.metrics.chunkSizeBytes.Observe(float64(chunksSize(series.Chunks)))
			}
			series.Labels = labelpb.ZLabelsFromPromLabels(lset)
			if err = srv.Send(storepb.NewSeriesResponse(&series)); err != nil {
				err = status.Error(codes.Unknown, errors.Wrap(err, "send series response").Error())
				return
			}
		}
		if set.Err() != nil {
			err = status.Error(codes.Unknown, errors.Wrap(set.Err(), "expand series set").Error())
			return
		}
		stats.mergeDuration = time.Since(begin)
		s.metrics.seriesMergeDuration.Observe(stats.mergeDuration.Seconds())

		err = nil
	})

	if s.enableSeriesResponseHints {
		var anyHints *types.Any

		if anyHints, err = types.MarshalAny(resHints); err != nil {
			err = status.Error(codes.Unknown, errors.Wrap(err, "marshal series response hints").Error())
			return
		}

		if err = srv.Send(storepb.NewHintsSeriesResponse(anyHints)); err != nil {
			err = status.Error(codes.Unknown, errors.Wrap(err, "send series response hints").Error())
			return
		}
	}

	return err
}

func chunksSize(chks []storepb.AggrChunk) (size int) {
	for _, chk := range chks {
		size += chk.Size() // This gets the encoded proto size.
	}
	return size
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

	s.mtx.RLock()

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

	s.mtx.RUnlock()

	if err := g.Wait(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	anyHints, err := types.MarshalAny(resHints)
	if err != nil {
		return nil, status.Error(codes.Unknown, errors.Wrap(err, "marshal label names response hints").Error())
	}

	return &storepb.LabelNamesResponse{
		Names: strutil.MergeSlices(sets...),
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
	seriesSet, _, err := blockSeries(ctx, indexr, nil, matchers, nil, nil, nil, seriesLimiter, true, minTime, maxTime, nil, logger)
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

	s.mtx.RLock()

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

			result, err := blockLabelValues(gctx, indexr, req.Label, reqSeriesMatchers, s.logger)
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

	s.mtx.RUnlock()

	if err := g.Wait(); err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}

	anyHints, err := types.MarshalAny(resHints)
	if err != nil {
		return nil, status.Error(codes.Unknown, errors.Wrap(err, "marshal label values response hints").Error())
	}

	return &storepb.LabelValuesResponse{
		Values: strutil.MergeSlices(sets...),
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
func blockLabelValues(ctx context.Context, indexr *bucketIndexReader, labelName string, matchers []*labels.Matcher, logger log.Logger) ([]string, error) {
	values, ok := fetchCachedLabelValues(ctx, indexr.block.indexCache, indexr.block.userID, indexr.block.meta.ULID, labelName, matchers, logger)
	if ok {
		return values, nil
	}

	allValues, err := indexr.block.indexHeaderReader.LabelValues(labelName)
	if err != nil {
		return nil, errors.Wrap(err, "index header label values")
	}

	if len(matchers) == 0 {
		storeCachedLabelValues(ctx, indexr.block.indexCache, indexr.block.userID, indexr.block.meta.ULID, labelName, matchers, allValues, logger)
		return allValues, nil
	}

	p, err := indexr.ExpandedPostings(ctx, matchers)
	if err != nil {
		return nil, errors.Wrap(err, "expanded postings")
	}

	keys := make([]labels.Label, len(allValues))
	for i, value := range allValues {
		keys[i] = labels.Label{Name: labelName, Value: value}
	}

	fetchedPostings, err := indexr.FetchPostings(ctx, keys)
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
		blockLabels: labels.Labels{labels.Label{
			Name:  block.BlockIDLabel,
			Value: meta.ULID.String(),
		}},
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

// bucketIndexReader is a custom index reader (not conforming index.Reader interface) that reads index that is stored in
// object storage without having to fully download it.
type bucketIndexReader struct {
	block *bucketBlock
	dec   *index.Decoder
	stats *queryStats

	mtx          sync.Mutex
	loadedSeries map[storage.SeriesRef][]byte
}

func newBucketIndexReader(block *bucketBlock) *bucketIndexReader {
	r := &bucketIndexReader{
		block: block,
		dec: &index.Decoder{
			LookupSymbol: block.indexHeaderReader.LookupSymbol,
		},
		stats:        &queryStats{},
		loadedSeries: map[storage.SeriesRef][]byte{},
	}
	return r
}

// ExpandedPostings returns postings in expanded list instead of index.Postings.
// This is because we need to have them buffered anyway to perform efficient lookup
// on object storage.
// Found posting IDs (ps) are not strictly required to point to a valid Series, e.g. during
// background garbage collections.
//
// Reminder: A posting is a reference (represented as a uint64) to a series reference, which in turn points to the first
// chunk where the series contains the matching label-value pair for a given block of data. Postings can be fetched by
// single label name=value.
func (r *bucketIndexReader) ExpandedPostings(ctx context.Context, ms []*labels.Matcher) (returnRefs []storage.SeriesRef, returnErr error) {
	var (
		loaded bool
		cached bool
	)
	span, ctx := tracing.StartSpan(ctx, "ExpandedPostings()")
	defer func() {
		span.LogKV("returned postings", len(returnRefs), "cached", cached, "promise_loaded", loaded)
		if returnErr != nil {
			span.LogFields(otlog.Error(returnErr))
		}
		span.Finish()
	}()
	var promise expandedPostingsPromise
	promise, loaded = r.expandedPostingsPromise(ctx, ms)
	returnRefs, cached, returnErr = promise(ctx)
	return returnRefs, returnErr
}

// expandedPostingsPromise is the promise returned by bucketIndexReader.expandedPostingsPromise.
// The second return value indicates whether the returned data comes from the cache.
type expandedPostingsPromise func(ctx context.Context) ([]storage.SeriesRef, bool, error)

// expandedPostingsPromise provides a promise for the execution of expandedPostings method.
// First call to this method will be blocking until the expandedPostings are calculated.
// While first call is blocking, concurrent calls with same matchers will return a promise for the same results, without recalculating them.
// The second value returned by this function is set to true when this call just loaded a promise created by another goroutine.
// The promise returned by this function returns a bool value fromCache, set to true when data was loaded from cache.
// TODO: if promise creator's context is canceled, the entire promise will fail, even if there are more callers waiting for the results
// TODO: https://github.com/grafana/mimir/issues/331
func (r *bucketIndexReader) expandedPostingsPromise(ctx context.Context, ms []*labels.Matcher) (promise expandedPostingsPromise, loaded bool) {
	var (
		refs   []storage.SeriesRef
		err    error
		done   = make(chan struct{})
		cached bool
	)

	promise = func(ctx context.Context) ([]storage.SeriesRef, bool, error) {
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-done:
		}

		if err != nil {
			return nil, false, err
		}

		// We must make a copy of refs to return, because caller can modify the postings slice in place.
		refsCopy := make([]storage.SeriesRef, len(refs))
		copy(refsCopy, refs)

		return refsCopy, cached, nil
	}

	key := indexcache.CanonicalLabelMatchersKey(ms)

	var loadedPromise interface{}
	loadedPromise, loaded = r.block.expandedPostingsPromises.LoadOrStore(key, promise)
	if loaded {
		return loadedPromise.(expandedPostingsPromise), true
	}
	defer close(done)
	defer r.block.expandedPostingsPromises.Delete(key)

	refs, cached = r.fetchCachedExpandedPostings(ctx, r.block.userID, key)
	if cached {
		return promise, false
	}
	refs, err = r.expandedPostings(ctx, ms)
	if err != nil {
		return promise, false
	}
	r.cacheExpandedPostings(ctx, r.block.userID, key, refs)
	return promise, false
}

func (r *bucketIndexReader) cacheExpandedPostings(ctx context.Context, userID string, key indexcache.LabelMatchersKey, refs []storage.SeriesRef) {
	data, err := diffVarintSnappyEncode(index.NewListPostings(refs), len(refs))
	if err != nil {
		level.Warn(r.block.logger).Log("msg", "can't encode expanded postings cache", "err", err, "matchers_key", key, "block", r.block.meta.ULID)
		return
	}
	r.block.indexCache.StoreExpandedPostings(ctx, userID, r.block.meta.ULID, key, data)
}

func (r *bucketIndexReader) fetchCachedExpandedPostings(ctx context.Context, userID string, key indexcache.LabelMatchersKey) ([]storage.SeriesRef, bool) {
	data, ok := r.block.indexCache.FetchExpandedPostings(ctx, userID, r.block.meta.ULID, key)
	if !ok {
		return nil, false
	}

	p, err := r.decodePostings(data)
	if err != nil {
		level.Warn(r.block.logger).Log("msg", "can't decode expanded postings cache", "err", err, "matchers_key", key, "block", r.block.meta.ULID)
		return nil, false
	}

	refs, err := index.ExpandPostings(p)
	if err != nil {
		level.Warn(r.block.logger).Log("msg", "can't expand decoded expanded postings cache", "err", err, "matchers_key", key, "block", r.block.meta.ULID)
		return nil, false
	}
	return refs, true
}

// expandedPostings is the main logic of ExpandedPostings, without the promise wrapper.
func (r *bucketIndexReader) expandedPostings(ctx context.Context, ms []*labels.Matcher) (returnRefs []storage.SeriesRef, returnErr error) {
	var (
		postingGroups []*postingGroup
		allRequested  = false
		hasAdds       = false
		keys          []labels.Label
	)

	// NOTE: Derived from tsdb.PostingsForMatchers.
	for _, m := range ms {
		// Each group is separate to tell later what postings are intersecting with what.
		pg, err := toPostingGroup(r.block.indexHeaderReader.LabelValues, m)
		if err != nil {
			return nil, errors.Wrap(err, "toPostingGroup")
		}

		// If this groups adds nothing, it's an empty group. We can shortcut this, since intersection with empty
		// postings would return no postings anyway.
		// E.g. label="non-existing-value" returns empty group.
		if !pg.addAll && len(pg.addKeys) == 0 {
			return nil, nil
		}

		postingGroups = append(postingGroups, pg)
		allRequested = allRequested || pg.addAll
		hasAdds = hasAdds || len(pg.addKeys) > 0

		// Postings returned by fetchPostings will be in the same order as keys
		// so it's important that we iterate them in the same order later.
		// We don't have any other way of pairing keys and fetched postings.
		keys = append(keys, pg.addKeys...)
		keys = append(keys, pg.removeKeys...)
	}

	if len(postingGroups) == 0 {
		return nil, nil
	}

	// We only need special All postings if there are no other adds. If there are, we can skip fetching
	// special All postings completely.
	if allRequested && !hasAdds {
		// add group with label to fetch "special All postings".
		name, value := index.AllPostingsKey()
		allPostingsLabel := labels.Label{Name: name, Value: value}

		postingGroups = append(postingGroups, newPostingGroup(true, []labels.Label{allPostingsLabel}, nil))
		keys = append(keys, allPostingsLabel)
	}

	fetchedPostings, err := r.fetchPostings(ctx, keys)
	if err != nil {
		return nil, errors.Wrap(err, "get postings")
	}

	// Get "add" and "remove" postings from groups. We iterate over postingGroups and their keys
	// again, and this is exactly the same order as before (when building the groups), so we can simply
	// use one incrementing index to fetch postings from returned slice.
	postingIndex := 0

	var groupAdds, groupRemovals []index.Postings
	for _, g := range postingGroups {
		// We cannot add empty set to groupAdds, since they are intersected.
		if len(g.addKeys) > 0 {
			toMerge := make([]index.Postings, 0, len(g.addKeys))
			for _, l := range g.addKeys {
				toMerge = append(toMerge, checkNilPosting(l, fetchedPostings[postingIndex]))
				postingIndex++
			}

			groupAdds = append(groupAdds, index.Merge(toMerge...))
		}

		for _, l := range g.removeKeys {
			groupRemovals = append(groupRemovals, checkNilPosting(l, fetchedPostings[postingIndex]))
			postingIndex++
		}
	}

	result := index.Without(index.Intersect(groupAdds...), index.Merge(groupRemovals...))

	ps, err := index.ExpandPostings(result)
	if err != nil {
		return nil, errors.Wrap(err, "expand")
	}

	// As of version two all series entries are 16 byte padded. All references
	// we get have to account for that to get the correct offset.
	version, err := r.block.indexHeaderReader.IndexVersion()
	if err != nil {
		return nil, errors.Wrap(err, "get index version")
	}
	if version >= 2 {
		for i, id := range ps {
			ps[i] = id * 16
		}
	}

	return ps, nil
}

// postingGroup keeps posting keys for single matcher. Logical result of the group is:
// If addAll is set: special All postings minus postings for removeKeys labels. No need to merge postings for addKeys in this case.
// If addAll is not set: Merge of postings for "addKeys" labels minus postings for removeKeys labels
// This computation happens in ExpandedPostings.
type postingGroup struct {
	addAll     bool
	addKeys    []labels.Label
	removeKeys []labels.Label
}

func newPostingGroup(addAll bool, addKeys, removeKeys []labels.Label) *postingGroup {
	return &postingGroup{
		addAll:     addAll,
		addKeys:    addKeys,
		removeKeys: removeKeys,
	}
}

func checkNilPosting(l labels.Label, p index.Postings) index.Postings {
	if p == nil {
		// This should not happen. Debug for https://github.com/thanos-io/thanos/issues/874.
		return index.ErrPostings(errors.Errorf("postings is nil for %s. It was never fetched.", l))
	}
	return p
}

// NOTE: Derived from tsdb.postingsForMatcher. index.Merge is equivalent to map duplication.
func toPostingGroup(lvalsFn func(name string) ([]string, error), m *labels.Matcher) (*postingGroup, error) {
	if setMatches := m.SetMatches(); len(setMatches) > 0 && (m.Type == labels.MatchRegexp || m.Type == labels.MatchNotRegexp) {
		keys := make([]labels.Label, 0, len(setMatches))
		for _, val := range setMatches {
			keys = append(keys, labels.Label{Name: m.Name, Value: val})
		}
		if m.Type == labels.MatchNotRegexp {
			return newPostingGroup(true, nil, keys), nil
		}
		return newPostingGroup(false, keys, nil), nil
	}

	if m.Value != "" {
		// Fast-path for equal matching.
		// Works for every case except for `foo=""`, which is a special case, see below.
		if m.Type == labels.MatchEqual {
			return newPostingGroup(false, []labels.Label{{Name: m.Name, Value: m.Value}}, nil), nil
		}

		// If matcher is `label!="foo"`, we select an empty label value too,
		// i.e., series that don't have this label.
		// So this matcher selects all series in the storage,
		// except for the ones that do have `label="foo"`
		if m.Type == labels.MatchNotEqual {
			return newPostingGroup(true, nil, []labels.Label{{Name: m.Name, Value: m.Value}}), nil
		}
	}

	vals, err := lvalsFn(m.Name)
	if err != nil {
		return nil, err
	}

	// This is a more generic approach for the previous case.
	// Here we can enter with `label=""` or regexp matchers that match the empty value,
	// like `=~"|foo" or `error!~"5..".
	// Remember: if the matcher selects an empty value, it selects all the series which don't
	// have the label name set too. See: https://github.com/prometheus/prometheus/issues/3575
	// and https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555.
	if m.Matches("") {
		var toRemove []labels.Label
		for _, val := range vals {
			if !m.Matches(val) {
				toRemove = append(toRemove, labels.Label{Name: m.Name, Value: val})
			}
		}

		return newPostingGroup(true, nil, toRemove), nil
	}

	// Our matcher does not match the empty value, so we just need the postings that correspond
	// to label values matched by the matcher.
	var toAdd []labels.Label
	for _, val := range vals {
		if m.Matches(val) {
			toAdd = append(toAdd, labels.Label{Name: m.Name, Value: val})
		}
	}

	return newPostingGroup(false, toAdd, nil), nil
}

type postingPtr struct {
	keyID int
	ptr   index.Range
}

// FetchPostings fills postings requested by posting groups.
// It returns one postings for each key, in the same order.
// If postings for given key is not fetched, entry at given index will be an ErrPostings
func (r *bucketIndexReader) FetchPostings(ctx context.Context, keys []labels.Label) ([]index.Postings, error) {
	ps, err := r.fetchPostings(ctx, keys)
	if err != nil {
		return nil, err
	}

	// As of version two all series entries are 16 byte padded. All references
	// we get have to account for that to get the correct offset.
	version, err := r.block.indexHeaderReader.IndexVersion()
	if err != nil {
		return nil, errors.Wrap(err, "get index version")
	}
	for i := range ps {
		ps[i] = checkNilPosting(keys[i], ps[i])
		if version >= 2 {
			ps[i] = paddedPostings{ps[i]}
		}
	}
	return ps, nil
}

// fetchPostings is the version-unaware private implementation of FetchPostings.
// callers of this method may need to add padding to the results.
// If postings for given key is not fetched, entry at given index will be nil.
func (r *bucketIndexReader) fetchPostings(ctx context.Context, keys []labels.Label) ([]index.Postings, error) {
	timer := prometheus.NewTimer(r.block.metrics.postingsFetchDuration)
	defer timer.ObserveDuration()

	var ptrs []postingPtr

	output := make([]index.Postings, len(keys))

	// Fetch postings from the cache with a single call.
	fromCache, _ := r.block.indexCache.FetchMultiPostings(ctx, r.block.userID, r.block.meta.ULID, keys)

	// Iterate over all groups and fetch posting from cache.
	// If we have a miss, mark key to be fetched in `ptrs` slice.
	// Overlaps are well handled by partitioner, so we don't need to deduplicate keys.
	for ix, key := range keys {
		// Get postings for the given key from cache first.
		if b, ok := fromCache[key]; ok {
			r.stats.postingsTouched++
			r.stats.postingsTouchedSizeSum += len(b)

			l, err := r.decodePostings(b)
			if err == nil {
				output[ix] = l
				continue
			}

			level.Warn(r.block.logger).Log(
				"msg", "can't decode cached postings",
				"err", err,
				"key", fmt.Sprintf("%+v", key),
				"block", r.block.meta.ULID,
				"bytes_len", len(b),
				"bytes_head_hex", hex.EncodeToString(b[:util_math.Min(8, len(b))]),
			)
		}

		// Cache miss; save pointer for actual posting in index stored in object store.
		ptr, err := r.block.indexHeaderReader.PostingsOffset(key.Name, key.Value)
		if errors.Is(err, indexheader.NotFoundRangeErr) {
			// This block does not have any posting for given key.
			output[ix] = index.EmptyPostings()
			continue
		}

		if err != nil {
			return nil, errors.Wrap(err, "index header PostingsOffset")
		}

		r.stats.postingsToFetch++
		ptrs = append(ptrs, postingPtr{ptr: ptr, keyID: ix})
	}

	sort.Slice(ptrs, func(i, j int) bool {
		return ptrs[i].ptr.Start < ptrs[j].ptr.Start
	})

	// TODO(bwplotka): Asses how large in worst case scenario this can be. (e.g fetch for AllPostingsKeys)
	// Consider sub split if too big.
	parts := r.block.partitioner.Partition(len(ptrs), func(i int) (start, end uint64) {
		return uint64(ptrs[i].ptr.Start), uint64(ptrs[i].ptr.End)
	})

	g, ctx := errgroup.WithContext(ctx)
	for _, part := range parts {
		i, j := part.ElemRng[0], part.ElemRng[1]

		start := int64(part.Start)
		// We assume index does not have any ptrs that has 0 length.
		length := int64(part.End) - start

		// Fetch from object storage concurrently and update stats and posting list.
		g.Go(func() error {
			begin := time.Now()

			b, err := r.block.readIndexRange(ctx, start, length)
			if err != nil {
				return errors.Wrap(err, "read postings range")
			}
			fetchTime := time.Since(begin)

			r.mtx.Lock()
			r.stats.postingsFetchCount++
			r.stats.postingsFetched += j - i
			r.stats.postingsFetchDurationSum += fetchTime
			r.stats.postingsFetchedSizeSum += int(length)
			r.mtx.Unlock()

			for _, p := range ptrs[i:j] {
				// index-header can estimate endings, which means we need to resize the endings.
				pBytes, err := resizePostings(b[p.ptr.Start-start : p.ptr.End-start])
				if err != nil {
					return err
				}

				dataToCache := pBytes

				compressionTime := time.Duration(0)
				compressions, compressionErrors, compressedSize := 0, 0, 0

				// Reencode postings before storing to cache. If that fails, we store original bytes.
				// This can only fail, if postings data was somehow corrupted,
				// and there is nothing we can do about it.
				// Errors from corrupted postings will be reported when postings are used.
				compressions++
				s := time.Now()
				bep := newBigEndianPostings(pBytes[4:])
				data, err := diffVarintSnappyEncode(bep, bep.length())
				compressionTime = time.Since(s)
				if err == nil {
					dataToCache = data
					compressedSize = len(data)
				} else {
					compressionErrors = 1
				}

				r.mtx.Lock()
				// Return postings and fill LRU cache.
				// Truncate first 4 bytes which are length of posting.
				output[p.keyID] = newBigEndianPostings(pBytes[4:])

				r.block.indexCache.StorePostings(ctx, r.block.userID, r.block.meta.ULID, keys[p.keyID], dataToCache)

				// If we just fetched it we still have to update the stats for touched postings.
				r.stats.postingsTouched++
				r.stats.postingsTouchedSizeSum += len(pBytes)
				r.stats.cachedPostingsCompressions += compressions
				r.stats.cachedPostingsCompressionErrors += compressionErrors
				r.stats.cachedPostingsOriginalSizeSum += len(pBytes)
				r.stats.cachedPostingsCompressedSizeSum += compressedSize
				r.stats.cachedPostingsCompressionTimeSum += compressionTime
				r.mtx.Unlock()
			}
			return nil
		})
	}

	return output, g.Wait()
}

func (r *bucketIndexReader) decodePostings(b []byte) (index.Postings, error) {
	// Even if this instance is not using compression, there may be compressed
	// entries in the cache written by other stores.
	var (
		l   index.Postings
		err error
	)
	if isDiffVarintSnappyEncodedPostings(b) {
		s := time.Now()
		l, err = diffVarintSnappyDecode(b)
		r.stats.cachedPostingsDecompressions++
		r.stats.cachedPostingsDecompressionTimeSum += time.Since(s)
		if err != nil {
			r.stats.cachedPostingsDecompressionErrors++
		}
	} else {
		_, l, err = r.dec.Postings(b)
	}
	return l, err
}

func resizePostings(b []byte) ([]byte, error) {
	d := encoding.Decbuf{B: b}
	n := d.Be32int()
	if d.Err() != nil {
		return nil, errors.Wrap(d.Err(), "read postings list")
	}

	// 4 for postings number of entries, then 4, foreach each big endian posting.
	size := 4 + n*4
	if len(b) < size {
		return nil, encoding.ErrInvalidSize
	}
	return b[:size], nil
}

// bigEndianPostings implements the Postings interface over a byte stream of
// big endian numbers.
type bigEndianPostings struct {
	list []byte
	cur  storage.SeriesRef
}

// TODO(bwplotka): Expose those inside Prometheus.
func newBigEndianPostings(list []byte) *bigEndianPostings {
	return &bigEndianPostings{list: list}
}

func (it *bigEndianPostings) At() storage.SeriesRef {
	return it.cur
}

func (it *bigEndianPostings) Next() bool {
	if len(it.list) >= 4 {
		it.cur = storage.SeriesRef(binary.BigEndian.Uint32(it.list))
		it.list = it.list[4:]
		return true
	}
	return false
}

func (it *bigEndianPostings) Seek(x storage.SeriesRef) bool {
	if it.cur >= x {
		return true
	}

	num := len(it.list) / 4
	// Do binary search between current position and end.
	i := sort.Search(num, func(i int) bool {
		return binary.BigEndian.Uint32(it.list[i*4:]) >= uint32(x)
	})
	if i < num {
		j := i * 4
		it.cur = storage.SeriesRef(binary.BigEndian.Uint32(it.list[j:]))
		it.list = it.list[j+4:]
		return true
	}
	it.list = nil
	return false
}

func (it *bigEndianPostings) Err() error {
	return nil
}

// Returns number of remaining postings values.
func (it *bigEndianPostings) length() int {
	return len(it.list) / 4
}

func (r *bucketIndexReader) PreloadSeries(ctx context.Context, ids []storage.SeriesRef) error {
	span, ctx := tracing.StartSpan(ctx, "PreloadSeries()")
	defer span.Finish()

	timer := prometheus.NewTimer(r.block.metrics.seriesFetchDuration)
	defer timer.ObserveDuration()

	// Load series from cache, overwriting the list of ids to preload
	// with the missing ones.
	fromCache, ids := r.block.indexCache.FetchMultiSeriesForRefs(ctx, r.block.userID, r.block.meta.ULID, ids)
	for id, b := range fromCache {
		r.loadedSeries[id] = b
	}

	parts := r.block.partitioner.Partition(len(ids), func(i int) (start, end uint64) {
		return uint64(ids[i]), uint64(ids[i] + maxSeriesSize)
	})
	g, ctx := errgroup.WithContext(ctx)
	for _, p := range parts {
		s, e := p.Start, p.End
		i, j := p.ElemRng[0], p.ElemRng[1]

		g.Go(func() error {
			return r.loadSeries(ctx, ids[i:j], false, s, e)
		})
	}
	return g.Wait()
}

func (r *bucketIndexReader) loadSeries(ctx context.Context, ids []storage.SeriesRef, refetch bool, start, end uint64) error {
	begin := time.Now()

	b, err := r.block.readIndexRange(ctx, int64(start), int64(end-start))
	if err != nil {
		return errors.Wrap(err, "read series range")
	}

	r.mtx.Lock()
	r.stats.seriesFetchCount++
	r.stats.seriesFetched += len(ids)
	r.stats.seriesFetchDurationSum += time.Since(begin)
	r.stats.seriesFetchedSizeSum += int(end - start)
	r.mtx.Unlock()

	for i, id := range ids {
		c := b[uint64(id)-start:]

		l, n := binary.Uvarint(c)
		if n < 1 {
			return errors.New("reading series length failed")
		}
		if len(c) < n+int(l) {
			if i == 0 && refetch {
				return errors.Errorf("invalid remaining size, even after refetch, remaining: %d, expected %d", len(c), n+int(l))
			}

			// Inefficient, but should be rare.
			r.block.metrics.seriesRefetches.Inc()
			level.Warn(r.block.logger).Log("msg", "series size exceeded expected size; refetching", "id", id, "series length", n+int(l), "maxSeriesSize", maxSeriesSize)

			// Fetch plus to get the size of next one if exists.
			return r.loadSeries(ctx, ids[i:], true, uint64(id), uint64(id)+uint64(n+int(l)+1))
		}
		c = c[n : n+int(l)]
		r.mtx.Lock()
		r.loadedSeries[id] = c
		r.block.indexCache.StoreSeriesForRef(ctx, r.block.userID, r.block.meta.ULID, id, c)
		r.mtx.Unlock()
	}
	return nil
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

// LoadSeriesForTime populates the given symbolized labels for the series identified by the reference if at least one chunk is within
// time selection.
// LoadSeriesForTime also populates chunk metas slices if skipChunks if set to false. Chunks are also limited by the given time selection.
// LoadSeriesForTime returns false, when there are no series data for given time range.
//
// Error is returned on decoding error or if the reference does not resolve to a known series.
func (r *bucketIndexReader) LoadSeriesForTime(ref storage.SeriesRef, lset *[]symbolizedLabel, chks *[]chunks.Meta, skipChunks bool, mint, maxt int64) (ok bool, err error) {
	b, ok := r.loadedSeries[ref]
	if !ok {
		return false, errors.Errorf("series %d not found", ref)
	}

	r.stats.seriesTouched++
	r.stats.seriesTouchedSizeSum += len(b)
	return decodeSeriesForTime(b, lset, chks, skipChunks, mint, maxt)
}

// Close released the underlying resources of the reader.
func (r *bucketIndexReader) Close() error {
	r.block.pendingReaders.Done()
	return nil
}

// LookupLabelsSymbols populates label set strings from symbolized label set.
func (r *bucketIndexReader) LookupLabelsSymbols(symbolized []symbolizedLabel) (labels.Labels, error) {
	lbls := make(labels.Labels, len(symbolized))
	for ix, s := range symbolized {
		ln, err := r.dec.LookupSymbol(s.name)
		if err != nil {
			return nil, errors.Wrap(err, "lookup label name")
		}
		lv, err := r.dec.LookupSymbol(s.value)
		if err != nil {
			return nil, errors.Wrap(err, "lookup label value")
		}
		lbls[ix] = labels.Label{Name: ln, Value: lv}
	}
	return lbls, nil
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

type loadIdx struct {
	offset uint32
	// Indices, not actual entries and chunks.
	seriesEntry int
	chunk       int
}

type bucketChunkReader struct {
	ctx   context.Context
	block *bucketBlock

	toLoad [][]loadIdx

	// Mutex protects access to following fields, when updated from chunks-loading goroutines.
	// After chunks are loaded, mutex is no longer used.
	mtx        sync.Mutex
	stats      *queryStats
	chunkBytes []*[]byte // Byte slice to return to the chunk pool on close.
}

func newBucketChunkReader(ctx context.Context, block *bucketBlock) *bucketChunkReader {
	return &bucketChunkReader{
		ctx:    ctx,
		block:  block,
		stats:  &queryStats{},
		toLoad: make([][]loadIdx, len(block.chunkObjs)),
	}
}

func (r *bucketChunkReader) Close() error {
	r.block.pendingReaders.Done()

	for _, b := range r.chunkBytes {
		r.block.chunkPool.Put(b)
	}
	return nil
}

// addLoad adds the chunk with id to the data set to be fetched.
// Chunk will be fetched and saved to res[seriesEntry][chunk] upon r.load(res, <...>) call.
func (r *bucketChunkReader) addLoad(id chunks.ChunkRef, seriesEntry, chunk int) error {
	var (
		seq = int(id >> 32)
		off = uint32(id)
	)
	if seq >= len(r.toLoad) {
		return errors.Errorf("reference sequence %d out of range", seq)
	}
	r.toLoad[seq] = append(r.toLoad[seq], loadIdx{off, seriesEntry, chunk})
	return nil
}

// load loads all added chunks and saves resulting aggrs to res.
func (r *bucketChunkReader) load(res []seriesEntry, aggrs []storepb.Aggr) error {
	g, ctx := errgroup.WithContext(r.ctx)

	for seq, pIdxs := range r.toLoad {
		sort.Slice(pIdxs, func(i, j int) bool {
			return pIdxs[i].offset < pIdxs[j].offset
		})
		parts := r.block.partitioner.Partition(len(pIdxs), func(i int) (start, end uint64) {
			return uint64(pIdxs[i].offset), uint64(pIdxs[i].offset) + mimir_tsdb.EstimatedMaxChunkSize
		})

		for _, p := range parts {
			seq := seq
			p := p
			indices := pIdxs[p.ElemRng[0]:p.ElemRng[1]]
			g.Go(func() error {
				return r.loadChunks(ctx, res, aggrs, seq, p, indices)
			})
		}
	}
	return g.Wait()
}

// loadChunks will read range [start, end] from the segment file with sequence number seq.
// This data range covers chunks starting at supplied offsets.
func (r *bucketChunkReader) loadChunks(ctx context.Context, res []seriesEntry, aggrs []storepb.Aggr, seq int, part Part, pIdxs []loadIdx) error {
	fetchBegin := time.Now()

	// Get a reader for the required range.
	reader, err := r.block.chunkRangeReader(ctx, seq, int64(part.Start), int64(part.End-part.Start))
	if err != nil {
		return errors.Wrap(err, "get range reader")
	}
	defer runutil.CloseWithLogOnErr(r.block.logger, reader, "readChunkRange close range reader")
	bufReader := bufio.NewReaderSize(reader, mimir_tsdb.EstimatedMaxChunkSize)

	locked := true
	r.mtx.Lock()

	defer func() {
		if locked {
			r.mtx.Unlock()
		}
	}()

	r.stats.chunksFetchCount++
	r.stats.chunksFetched += len(pIdxs)
	r.stats.chunksFetchDurationSum += time.Since(fetchBegin)
	r.stats.chunksFetchedSizeSum += int(part.End - part.Start)

	var (
		buf        = make([]byte, mimir_tsdb.EstimatedMaxChunkSize)
		readOffset = int(pIdxs[0].offset)

		// Save a few allocations.
		written  int64
		diff     uint32
		chunkLen int
		n        int
	)

	for i, pIdx := range pIdxs {
		// Fast forward range reader to the next chunk start in case of sparse (for our purposes) byte range.
		for readOffset < int(pIdx.offset) {
			written, err = io.CopyN(io.Discard, bufReader, int64(pIdx.offset)-int64(readOffset))
			if err != nil {
				return errors.Wrap(err, "fast forward range reader")
			}
			readOffset += int(written)
		}
		// Presume chunk length to be reasonably large for common use cases.
		// However, declaration for EstimatedMaxChunkSize warns us some chunks could be larger in some rare cases.
		// This is handled further down below.
		chunkLen = mimir_tsdb.EstimatedMaxChunkSize
		if i+1 < len(pIdxs) {
			if diff = pIdxs[i+1].offset - pIdx.offset; int(diff) < chunkLen {
				chunkLen = int(diff)
			}
		}
		cb := buf[:chunkLen]
		n, err = io.ReadFull(bufReader, cb)
		readOffset += n
		// Unexpected EOF for last chunk could be a valid case. Any other errors are definitely real.
		if err != nil && !(errors.Is(err, io.ErrUnexpectedEOF) && i == len(pIdxs)-1) {
			return errors.Wrapf(err, "read range for seq %d offset %x", seq, pIdx.offset)
		}

		chunkDataLen, n := binary.Uvarint(cb)
		if n < 1 {
			return errors.New("reading chunk length failed")
		}

		// Chunk length is n (number of bytes used to encode chunk data), 1 for chunk encoding and chunkDataLen for actual chunk data.
		// There is also crc32 after the chunk, but we ignore that.
		chunkLen = n + 1 + int(chunkDataLen)
		if chunkLen <= len(cb) {
			err = populateChunk(&(res[pIdx.seriesEntry].chks[pIdx.chunk]), rawChunk(cb[n:chunkLen]), aggrs, r.save)
			if err != nil {
				return errors.Wrap(err, "populate chunk")
			}
			r.stats.chunksTouched++
			r.stats.chunksTouchedSizeSum += int(chunkDataLen)
			continue
		}

		// If we didn't fetch enough data for the chunk, fetch more.
		r.mtx.Unlock()
		locked = false

		fetchBegin = time.Now()

		// Read entire chunk into new buffer.
		// TODO: readChunkRange call could be avoided for any chunk but last in this particular part.
		nb, err := r.block.readChunkRange(ctx, seq, int64(pIdx.offset), int64(chunkLen), []byteRange{{offset: 0, length: chunkLen}})
		if err != nil {
			return errors.Wrapf(err, "preloaded chunk too small, expecting %d, and failed to fetch full chunk", chunkLen)
		}
		if len(*nb) != chunkLen {
			return errors.Errorf("preloaded chunk too small, expecting %d", chunkLen)
		}

		r.mtx.Lock()
		locked = true

		r.stats.chunksFetchCount++
		r.stats.chunksFetchDurationSum += time.Since(fetchBegin)
		r.stats.chunksFetchedSizeSum += len(*nb)
		err = populateChunk(&(res[pIdx.seriesEntry].chks[pIdx.chunk]), rawChunk((*nb)[n:]), aggrs, r.save)
		if err != nil {
			r.block.chunkPool.Put(nb)
			return errors.Wrap(err, "populate chunk")
		}
		r.stats.chunksTouched++
		r.stats.chunksTouchedSizeSum += int(chunkDataLen)

		r.block.chunkPool.Put(nb)
	}
	return nil
}

// save saves a copy of b's payload to a memory pool of its own and returns a new byte slice referencing said copy.
// Returned slice becomes invalid once r.block.chunkPool.Put() is called.
func (r *bucketChunkReader) save(b []byte) ([]byte, error) {
	// Ensure we never grow slab beyond original capacity.
	if len(r.chunkBytes) == 0 ||
		cap(*r.chunkBytes[len(r.chunkBytes)-1])-len(*r.chunkBytes[len(r.chunkBytes)-1]) < len(b) {
		s, err := r.block.chunkPool.Get(len(b))
		if err != nil {
			return nil, errors.Wrap(err, "allocate chunk bytes")
		}
		r.chunkBytes = append(r.chunkBytes, s)
	}
	slab := r.chunkBytes[len(r.chunkBytes)-1]
	*slab = append(*slab, b...)
	return (*slab)[len(*slab)-len(b):], nil
}

// rawChunk is a helper type that wraps a chunk's raw bytes and implements the chunkenc.Chunk
// interface over it.
// It is used to Store API responses which don't need to introspect and validate the chunk's contents.
type rawChunk []byte

func (b rawChunk) Encoding() chunkenc.Encoding {
	return chunkenc.Encoding(b[0])
}

func (b rawChunk) Bytes() []byte {
	return b[1:]
}
func (b rawChunk) Compact() {}

func (b rawChunk) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	panic("invalid call")
}

func (b rawChunk) Appender() (chunkenc.Appender, error) {
	panic("invalid call")
}

func (b rawChunk) NumSamples() int {
	panic("invalid call")
}

type queryStats struct {
	blocksQueried int

	postingsTouched          int
	postingsTouchedSizeSum   int
	postingsToFetch          int
	postingsFetched          int
	postingsFetchedSizeSum   int
	postingsFetchCount       int
	postingsFetchDurationSum time.Duration

	cachedPostingsCompressions         int
	cachedPostingsCompressionErrors    int
	cachedPostingsOriginalSizeSum      int
	cachedPostingsCompressedSizeSum    int
	cachedPostingsCompressionTimeSum   time.Duration
	cachedPostingsDecompressions       int
	cachedPostingsDecompressionErrors  int
	cachedPostingsDecompressionTimeSum time.Duration

	seriesTouched          int
	seriesTouchedSizeSum   int
	seriesFetched          int
	seriesFetchedSizeSum   int
	seriesFetchCount       int
	seriesFetchDurationSum time.Duration

	seriesHashCacheRequests int
	seriesHashCacheHits     int

	chunksTouched          int
	chunksTouchedSizeSum   int
	chunksFetched          int
	chunksFetchedSizeSum   int
	chunksFetchCount       int
	chunksFetchDurationSum time.Duration

	getAllDuration    time.Duration
	mergedSeriesCount int
	mergedChunksCount int
	mergeDuration     time.Duration
}

func (s queryStats) merge(o *queryStats) *queryStats {
	s.blocksQueried += o.blocksQueried

	s.postingsTouched += o.postingsTouched
	s.postingsTouchedSizeSum += o.postingsTouchedSizeSum
	s.postingsFetched += o.postingsFetched
	s.postingsFetchedSizeSum += o.postingsFetchedSizeSum
	s.postingsFetchCount += o.postingsFetchCount
	s.postingsFetchDurationSum += o.postingsFetchDurationSum

	s.cachedPostingsCompressions += o.cachedPostingsCompressions
	s.cachedPostingsCompressionErrors += o.cachedPostingsCompressionErrors
	s.cachedPostingsOriginalSizeSum += o.cachedPostingsOriginalSizeSum
	s.cachedPostingsCompressedSizeSum += o.cachedPostingsCompressedSizeSum
	s.cachedPostingsCompressionTimeSum += o.cachedPostingsCompressionTimeSum
	s.cachedPostingsDecompressions += o.cachedPostingsDecompressions
	s.cachedPostingsDecompressionErrors += o.cachedPostingsDecompressionErrors
	s.cachedPostingsDecompressionTimeSum += o.cachedPostingsDecompressionTimeSum

	s.seriesTouched += o.seriesTouched
	s.seriesTouchedSizeSum += o.seriesTouchedSizeSum
	s.seriesFetched += o.seriesFetched
	s.seriesFetchedSizeSum += o.seriesFetchedSizeSum
	s.seriesFetchCount += o.seriesFetchCount
	s.seriesFetchDurationSum += o.seriesFetchDurationSum

	s.seriesHashCacheRequests += o.seriesHashCacheRequests
	s.seriesHashCacheHits += o.seriesHashCacheHits

	s.chunksTouched += o.chunksTouched
	s.chunksTouchedSizeSum += o.chunksTouchedSizeSum
	s.chunksFetched += o.chunksFetched
	s.chunksFetchedSizeSum += o.chunksFetchedSizeSum
	s.chunksFetchCount += o.chunksFetchCount
	s.chunksFetchDurationSum += o.chunksFetchDurationSum

	s.getAllDuration += o.getAllDuration
	s.mergedSeriesCount += o.mergedSeriesCount
	s.mergedChunksCount += o.mergedChunksCount
	s.mergeDuration += o.mergeDuration

	return &s
}

// NewDefaultChunkBytesPool returns a chunk bytes pool with default settings.
func NewDefaultChunkBytesPool(maxChunkPoolBytes uint64) (pool.Bytes, error) {
	return pool.NewBucketedBytes(chunkBytesPoolMinSize, chunkBytesPoolMaxSize, 2, maxChunkPoolBytes)
}

// paddedPostings adds the v2 index padding to postings without expanding them
type paddedPostings struct {
	index.Postings
}

func (p paddedPostings) Seek(v storage.SeriesRef) bool {
	unpadded := v / 16
	if unpadded*16 != v {
		// if someone is looking for 17 (they shouldn't but who knows)
		// then we don't want stop seeking at 16 ((v/16) * 16), so we'll look for the next number
		// this is semantically correct
		unpadded++
	}
	return p.Postings.Seek(unpadded)
}

func (p paddedPostings) At() storage.SeriesRef {
	return p.Postings.At() * 16
}

func maybeNilShard(shard *sharding.ShardSelector) sharding.ShardSelector {
	if shard == nil {
		return sharding.ShardSelector{}
	}
	return *shard
}
