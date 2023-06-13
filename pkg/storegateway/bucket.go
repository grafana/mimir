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
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/types"
	"github.com/grafana/dskit/gate"
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/tracing"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketcache"
	"github.com/grafana/mimir/pkg/storegateway/chunkscache"
	"github.com/grafana/mimir/pkg/storegateway/hintspb"
	"github.com/grafana/mimir/pkg/storegateway/indexcache"
	"github.com/grafana/mimir/pkg/storegateway/indexheader"
	streamindex "github.com/grafana/mimir/pkg/storegateway/indexheader/index"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
	util_math "github.com/grafana/mimir/pkg/util/math"
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
	chunksCache     chunkscache.Cache
	indexReaderPool *indexheader.ReaderPool
	seriesHashCache *hashcache.SeriesHashCache

	// Sets of blocks that have the same labels. They are indexed by a hash over their label set.
	blocksMx sync.RWMutex
	blocks   map[ulid.ULID]*bucketBlock
	blockSet *bucketBlockSet

	// Number of goroutines to use when syncing blocks from object storage.
	blockSyncConcurrency int

	// maxSeriesPerBatch controls the batch size to use when fetching series.
	// This is not restricted to the Series() RPC.
	// This value must be greater than zero.
	maxSeriesPerBatch int

	// numChunksRangesPerSeries controls into how many ranges the chunks of each series from each block are split.
	// This value is effectively the number of chunks cache items per series per block.
	numChunksRangesPerSeries int

	// fineGrainedChunksCachingEnabled controls whether to use the per series chunks caching
	// or rely on the transparent caching bucket.
	fineGrainedChunksCachingEnabled bool

	// Query gate which limits the maximum amount of concurrent queries.
	queryGate gate.Gate

	// chunksLimiterFactory creates a new limiter used to limit the number of chunks fetched by each Series() call.
	chunksLimiterFactory ChunksLimiterFactory
	// seriesLimiterFactory creates a new limiter used to limit the number of touched series by each Series() call,
	// or LabelName and LabelValues calls when used with matchers.
	seriesLimiterFactory SeriesLimiterFactory
	partitioners         blockPartitioners

	// Every how many posting offset entry we pool in heap memory. Default in Prometheus is 32.
	postingOffsetsInMemSampling int

	// Additional configuration for experimental indexheader.BinaryReader behaviour.
	indexHeaderCfg indexheader.Config

	// postingsStrategy is a strategy shared among all tenants.
	postingsStrategy postingsSelectionStrategy
}

type noopCache struct{}

func (noopCache) StorePostings(string, ulid.ULID, labels.Label, []byte) {}
func (noopCache) FetchMultiPostings(_ context.Context, _ string, _ ulid.ULID, keys []labels.Label) indexcache.BytesResult {
	return &indexcache.MapIterator[labels.Label]{Keys: keys}
}

func (noopCache) StoreSeriesForRef(string, ulid.ULID, storage.SeriesRef, []byte) {}
func (noopCache) FetchMultiSeriesForRefs(_ context.Context, _ string, _ ulid.ULID, ids []storage.SeriesRef) (map[storage.SeriesRef][]byte, []storage.SeriesRef) {
	return map[storage.SeriesRef][]byte{}, ids
}

func (c noopCache) StoreExpandedPostings(_ string, _ ulid.ULID, _ indexcache.LabelMatchersKey, _ string, _ []byte) {
}

func (c noopCache) FetchExpandedPostings(_ context.Context, _ string, _ ulid.ULID, _ indexcache.LabelMatchersKey, _ string) ([]byte, bool) {
	return nil, false
}

func (noopCache) StoreSeriesForPostings(string, ulid.ULID, *sharding.ShardSelector, indexcache.PostingsKey, []byte) {
}
func (noopCache) FetchSeriesForPostings(context.Context, string, ulid.ULID, *sharding.ShardSelector, indexcache.PostingsKey) ([]byte, bool) {
	return nil, false
}

func (noopCache) StoreLabelNames(_ string, _ ulid.ULID, _ indexcache.LabelMatchersKey, _ []byte) {
}
func (noopCache) FetchLabelNames(_ context.Context, _ string, _ ulid.ULID, _ indexcache.LabelMatchersKey) ([]byte, bool) {
	return nil, false
}

func (noopCache) StoreLabelValues(_ string, _ ulid.ULID, _ string, _ indexcache.LabelMatchersKey, _ []byte) {
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

// WithChunksCache sets a chunksCache to use instead of a noopCache.
func WithChunksCache(cache chunkscache.Cache) BucketStoreOption {
	return func(s *BucketStore) {
		s.chunksCache = cache
	}
}

// WithQueryGate sets a queryGate to use instead of a noopGate.
func WithQueryGate(queryGate gate.Gate) BucketStoreOption {
	return func(s *BucketStore) {
		s.queryGate = queryGate
	}
}

func WithFineGrainedChunksCaching(enabled bool) BucketStoreOption {
	return func(s *BucketStore) {
		s.fineGrainedChunksCachingEnabled = enabled
	}
}

// NewBucketStore creates a new bucket backed store that implements the store API against
// an object store bucket. It is optimized to work against high latency backends.
func NewBucketStore(
	userID string,
	bkt objstore.InstrumentedBucketReader,
	fetcher block.MetadataFetcher,
	dir string,
	maxSeriesPerBatch int,
	numChunksRangesPerSeries int,
	postingsStrategy postingsSelectionStrategy,
	chunksLimiterFactory ChunksLimiterFactory,
	seriesLimiterFactory SeriesLimiterFactory,
	partitioners blockPartitioners,
	blockSyncConcurrency int,
	postingOffsetsInMemSampling int,
	indexHeaderCfg indexheader.Config,
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
		chunksCache:                 chunkscache.NoopCache{},
		blocks:                      map[ulid.ULID]*bucketBlock{},
		blockSet:                    newBucketBlockSet(),
		blockSyncConcurrency:        blockSyncConcurrency,
		queryGate:                   gate.NewNoop(),
		chunksLimiterFactory:        chunksLimiterFactory,
		seriesLimiterFactory:        seriesLimiterFactory,
		partitioners:                partitioners,
		postingOffsetsInMemSampling: postingOffsetsInMemSampling,
		indexHeaderCfg:              indexHeaderCfg,
		seriesHashCache:             seriesHashCache,
		metrics:                     metrics,
		userID:                      userID,
		maxSeriesPerBatch:           maxSeriesPerBatch,
		numChunksRangesPerSeries:    numChunksRangesPerSeries,
		postingsStrategy:            postingsStrategy,
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
	blockc := make(chan *block.Meta)

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

func (s *BucketStore) addBlock(ctx context.Context, meta *block.Meta) (err error) {
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
		indexHeaderReader,
		s.partitioners,
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

type seriesChunks struct {
	lset labels.Labels
	chks []storepb.AggrChunk
}

// Series implements the storepb.StoreServer interface.
func (s *BucketStore) Series(req *storepb.SeriesRequest, srv storepb.Store_SeriesServer) (err error) {
	if req.SkipChunks {
		req.StreamingChunksBatchSize = 0
	}
	defer func() {
		if err == nil {
			return
		}
		code := codes.Internal
		if st, ok := status.FromError(errors.Cause(err)); ok {
			code = st.Code()
		} else if errors.Is(err, context.Canceled) {
			code = codes.Canceled
		}
		err = status.Error(code, err.Error())
	}()

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
		reqBlockMatchers []*labels.Matcher
		chunksLimiter    = s.chunksLimiterFactory(s.metrics.queriesDropped.WithLabelValues("chunks"))
		seriesLimiter    = s.seriesLimiterFactory(s.metrics.queriesDropped.WithLabelValues("series"))
	)
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

	blocks, indexReaders, chunkReaders := s.openBlocksForReading(ctx, req.SkipChunks, req.MinTime, req.MaxTime, reqBlockMatchers, stats)
	// We must keep the readers open until all their data has been sent.
	for _, r := range indexReaders {
		defer runutil.CloseWithLogOnErr(s.logger, r, "close block index reader")
	}
	for _, r := range chunkReaders {
		defer runutil.CloseWithLogOnErr(s.logger, r, "close block chunk reader")
	}

	span.Finish()

	var readers *bucketChunkReaders
	if !req.SkipChunks {
		readers = newChunkReaders(chunkReaders)
	}

	// If we are streaming the series labels and chunks separately, we don't need to fetch the postings
	// twice. So we use these slices to re-use it.
	// Each reusePostings[i] and reusePendingMatchers[i] corresponds to a single block.
	var reusePostings [][]storage.SeriesRef
	var reusePendingMatchers [][]*labels.Matcher
	if req.StreamingChunksBatchSize > 0 && !req.SkipChunks {
		// The streaming feature is enabled where we stream the series labels first, followed
		// by the chunks later. Send only the labels here.
		req.SkipChunks = true

		reusePostings = make([][]storage.SeriesRef, len(blocks))
		reusePendingMatchers = make([][]*labels.Matcher, len(blocks))

		seriesSet, resHints, err := s.streamingSeriesSetForBlocks(ctx, req, blocks, indexReaders, nil, shardSelector, matchers, chunksLimiter, seriesLimiter, stats, reusePostings, reusePendingMatchers)
		if err != nil {
			return err
		}

		// This also sends the hints and the stats.
		err = s.sendStreamingSeriesLabelsHintsStats(req, srv, stats, seriesSet, resHints)
		if err != nil {
			return err
		}

		req.SkipChunks = false
	}

	// TODO: if streaming is enabled, we don't need to fetch the labels again; we just need to fetch the chunk references.
	// But we need labels to merge the series from blocks. Find other way of caching the resultant series refs (maybe final ordered
	// list of series IDs and block IDs).
	seriesSet, resHints, err := s.streamingSeriesSetForBlocks(ctx, req, blocks, indexReaders, readers, shardSelector, matchers, chunksLimiter, seriesLimiter, stats, reusePostings, reusePendingMatchers)
	if err != nil {
		return err
	}

	// Merge the sub-results from each selected block.
	tracing.DoWithSpan(ctx, "bucket_store_merge_all", func(ctx context.Context, _ tracing.Span) {
		err = s.sendSeriesChunks(req, srv, seriesSet, stats)
		if err != nil {
			return
		}
	})

	if err != nil {
		return
	}

	if req.StreamingChunksBatchSize == 0 || req.SkipChunks {
		// Hints and stats were not sent before, so send it now.
		return s.sendHintsAndStats(srv, resHints, stats)
	}

	return nil
}

// sendStreamingSeriesLabelsHintsStats sends the labels of the streaming series.
// Since hints and stats need to be sent before the "end of stream" streaming series message,
// this function also sends the hints and the stats.
func (s *BucketStore) sendStreamingSeriesLabelsHintsStats(
	req *storepb.SeriesRequest,
	srv storepb.Store_SeriesServer,
	stats *safeQueryStats,
	seriesSet storepb.SeriesSet,
	resHints *hintspb.SeriesResponseHints,
) error {
	// TODO: should we pool the seriesBuffer/seriesBatch?
	seriesBuffer := make([]*storepb.StreamingSeries, req.StreamingChunksBatchSize)
	for i := range seriesBuffer {
		seriesBuffer[i] = &storepb.StreamingSeries{}
	}
	seriesBatch := &storepb.StreamSeriesBatch{
		Series: seriesBuffer[:0],
	}
	// TODO: can we send this in parallel while we start fetching the chunks below?
	for seriesSet.Next() {
		var lset labels.Labels
		// IMPORTANT: do not retain the memory returned by seriesSet.At() beyond this loop cycle
		// because the subsequent call to seriesSet.Next() may release it.
		// TODO: check if it is safe to hold the lset.
		lset, _ = seriesSet.At()

		// We are re-using the slice for every batch this way.
		seriesBatch.Series = seriesBatch.Series[:len(seriesBatch.Series)+1]
		seriesBatch.Series[len(seriesBatch.Series)-1].Labels = mimirpb.FromLabelsToLabelAdapters(lset)

		// TODO: Add relevant trace spans and timers.

		if len(seriesBatch.Series) == int(req.StreamingChunksBatchSize) {
			msg := &grpc.PreparedMsg{}
			if err := msg.Encode(srv, storepb.NewStreamSeriesResponse(seriesBatch)); err != nil {
				return status.Error(codes.Internal, errors.Wrap(err, "encode streaming series response").Error())
			}

			// Send the message.
			if err := srv.SendMsg(msg); err != nil {
				return status.Error(codes.Unknown, errors.Wrap(err, "send streaming series response").Error())
			}

			seriesBatch.Series = seriesBatch.Series[:0]
		}
	}
	if seriesSet.Err() != nil {
		return errors.Wrap(seriesSet.Err(), "expand series set")
	}

	// We need to send hints and stats before sending the chunks.
	// Also, these need to be sent before we send IsEndOfSeriesStream=true.
	if err := s.sendHintsAndStats(srv, resHints, stats); err != nil {
		return err
	}

	// Send any remaining series and signal that there are no more series.
	msg := &grpc.PreparedMsg{}
	seriesBatch.IsEndOfSeriesStream = true
	if err := msg.Encode(srv, storepb.NewStreamSeriesResponse(seriesBatch)); err != nil {
		return status.Error(codes.Internal, errors.Wrap(err, "encode streaming series response").Error())
	}
	// Send the message.
	if err := srv.SendMsg(msg); err != nil {
		return status.Error(codes.Unknown, errors.Wrap(err, "send streaming series response").Error())
	}

	if seriesSet.Err() != nil {
		return errors.Wrap(seriesSet.Err(), "expand series set")
	}

	return nil
}

func (s *BucketStore) sendSeriesChunks(
	req *storepb.SeriesRequest,
	srv storepb.Store_SeriesServer,
	seriesSet storepb.SeriesSet,
	stats *safeQueryStats,
) (err error) {
	var (
		iterationBegin = time.Now()
		encodeDuration = time.Duration(0)
		sendDuration   = time.Duration(0)
		seriesCount    int
		chunksCount    int
	)

	// Once the iteration is done we will update the stats.
	defer stats.update(func(stats *queryStats) {
		stats.mergedSeriesCount += seriesCount
		stats.mergedChunksCount += chunksCount

		// The time spent iterating over the series set is the
		// actual time spent fetching series and chunks, encoding and sending them to the client.
		// We split the timings to have a better view over how time is spent.
		stats.streamingSeriesFetchSeriesAndChunksDuration += stats.streamingSeriesWaitBatchLoadedDuration
		stats.streamingSeriesEncodeResponseDuration += encodeDuration
		stats.streamingSeriesSendResponseDuration += sendDuration
		stats.streamingSeriesOtherDuration += time.Duration(util_math.Max(0, int64(time.Since(iterationBegin)-
			stats.streamingSeriesFetchSeriesAndChunksDuration-encodeDuration-sendDuration)))
	})

	for seriesSet.Next() {
		// IMPORTANT: do not retain the memory returned by seriesSet.At() beyond this loop cycle
		// because the subsequent call to seriesSet.Next() may release it.
		lset, chks := seriesSet.At()
		seriesCount++
		msg := &grpc.PreparedMsg{}
		if req.StreamingChunksBatchSize > 0 && !req.SkipChunks {
			// We only need to stream chunks here because the series labels have already
			// been sent above.
			// TODO: is the 'is end of stream' parameter required here?
			streamingChunks := storepb.StreamSeriesChunks{
				SeriesIndex: uint64(seriesCount - 1),
				Chunks:      chks,
			}

			// Encode the message. We encode it ourselves into a PreparedMsg in order to measure
			// the time it takes.
			encodeBegin := time.Now()
			if err := msg.Encode(srv, storepb.NewStreamSeriesChunksResponse(&streamingChunks)); err != nil {
				return status.Error(codes.Internal, errors.Wrap(err, "encode streaming chunks response").Error())
			}
			encodeDuration += time.Since(encodeBegin)
		} else {
			var series storepb.Series
			if !req.SkipChunks {
				series.Chunks = chks
			}
			series.Labels = mimirpb.FromLabelsToLabelAdapters(lset)

			// Encode the message. We encode it ourselves into a PreparedMsg in order to measure
			// the time it takes.
			encodeBegin := time.Now()
			if err := msg.Encode(srv, storepb.NewSeriesResponse(&series)); err != nil {
				return status.Error(codes.Internal, errors.Wrap(err, "encode series response").Error())
			}
			encodeDuration += time.Since(encodeBegin)
		}

		if !req.SkipChunks {
			chunksCount += len(chks)
			s.metrics.chunkSizeBytes.Observe(float64(chunksSize(chks)))
		}

		// Send the message.
		sendBegin := time.Now()
		if err := srv.SendMsg(msg); err != nil {
			// TODO: set the right error wrapper message.
			return status.Error(codes.Unknown, errors.Wrap(err, "send series response").Error())
		}
		sendDuration += time.Since(sendBegin)
	}
	if seriesSet.Err() != nil {
		return errors.Wrap(seriesSet.Err(), "expand series set")
	}

	return nil
}

func (s *BucketStore) sendHintsAndStats(srv storepb.Store_SeriesServer, resHints *hintspb.SeriesResponseHints, stats *safeQueryStats) error {
	var anyHints *types.Any
	var err error
	if anyHints, err = types.MarshalAny(resHints); err != nil {
		return status.Error(codes.Unknown, errors.Wrap(err, "marshal series response hints").Error())
	}

	if err := srv.Send(storepb.NewHintsSeriesResponse(anyHints)); err != nil {
		return status.Error(codes.Unknown, errors.Wrap(err, "send series response hints").Error())
	}

	unsafeStats := stats.export()
	if err := srv.Send(storepb.NewStatsResponse(unsafeStats.postingsTouchedSizeSum + unsafeStats.seriesProcessedSizeSum)); err != nil {
		return status.Error(codes.Unknown, errors.Wrap(err, "sends series response stats").Error())
	}

	return nil
}

func chunksSize(chks []storepb.AggrChunk) (size int) {
	for _, chk := range chks {
		size += chk.Size() // This gets the encoded proto size.
	}
	return size
}

func (s *BucketStore) streamingSeriesSetForBlocks(
	ctx context.Context,
	req *storepb.SeriesRequest,
	blocks []*bucketBlock,
	indexReaders map[ulid.ULID]*bucketIndexReader,
	chunkReaders *bucketChunkReaders,
	shardSelector *sharding.ShardSelector,
	matchers []*labels.Matcher,
	chunksLimiter ChunksLimiter, // Rate limiter for loading chunks.
	seriesLimiter SeriesLimiter, // Rate limiter for loading series.
	stats *safeQueryStats,
	reusePostings [][]storage.SeriesRef, // Used if not empty.
	reusePendingMatchers [][]*labels.Matcher, // Used if not empty.
) (storepb.SeriesSet, *hintspb.SeriesResponseHints, error) {
	var (
		resHints = &hintspb.SeriesResponseHints{}
		mtx      = sync.Mutex{}
		batches  = make([]seriesChunkRefsSetIterator, 0, len(blocks))
		g, _     = errgroup.WithContext(ctx)
		begin    = time.Now()
	)
	for i, b := range blocks {
		b := b
		i := i

		// Keep track of queried blocks.
		resHints.AddQueriedBlock(b.meta.ULID)
		indexr := indexReaders[b.meta.ULID]

		// If query sharding is enabled we have to get the block-specific series hash cache
		// which is used by blockSeriesSkippingChunks().
		var blockSeriesHashCache *hashcache.BlockSeriesHashCache
		if shardSelector != nil {
			blockSeriesHashCache = s.seriesHashCache.GetBlockCache(b.meta.ULID.String())
		}
		var ps []storage.SeriesRef
		var pendingMatchers []*labels.Matcher
		if len(reusePostings) > 0 {
			ps, pendingMatchers = reusePostings[i], reusePendingMatchers[i]
		}
		g.Go(func() error {
			part, newPs, newPendingMatchers, err := openBlockSeriesChunkRefsSetsIterator(
				ctx,
				s.maxSeriesPerBatch,
				s.userID,
				indexr,
				s.indexCache,
				b.meta,
				matchers,
				shardSelector,
				cachedSeriesHasher{blockSeriesHashCache},
				req.SkipChunks,
				req.StreamingChunksBatchSize > 0,
				req.MinTime, req.MaxTime,
				s.numChunksRangesPerSeries,
				stats,
				ps,
				pendingMatchers,
				s.logger,
			)
			if err != nil {
				return errors.Wrapf(err, "fetch series for block %s", b.meta.ULID)
			}

			if len(reusePostings) > 0 {
				reusePostings[i] = newPs
				reusePendingMatchers[i] = newPendingMatchers
			}

			mtx.Lock()
			batches = append(batches, part)
			mtx.Unlock()

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return nil, nil, err
	}

	stats.update(func(stats *queryStats) {
		stats.blocksQueried = len(batches)
		stats.streamingSeriesExpandPostingsDuration += time.Since(begin)
	})

	mergedIterator := mergedSeriesChunkRefsSetIterators(s.maxSeriesPerBatch, batches...)

	// Apply limits after the merging, so that if the same series is part of multiple blocks it just gets
	// counted once towards the limit.
	mergedIterator = newLimitingSeriesChunkRefsSetIterator(mergedIterator, chunksLimiter, seriesLimiter)

	var set storepb.SeriesSet
	if !req.SkipChunks {
		var cache chunkscache.Cache
		if s.fineGrainedChunksCachingEnabled {
			cache = s.chunksCache
		}
		set = newSeriesSetWithChunks(ctx, s.logger, s.userID, cache, *chunkReaders, mergedIterator, s.maxSeriesPerBatch, stats, req.MinTime, req.MaxTime)
	} else {
		set = newSeriesSetWithoutChunks(ctx, mergedIterator, stats)
	}
	return set, resHints, nil
}

func (s *BucketStore) recordSeriesCallResult(safeStats *safeQueryStats) {
	stats := safeStats.export()
	s.recordPostingsStats(stats)
	s.recordSeriesStats(stats)
	s.recordCachedPostingStats(stats)
	s.recordSeriesHashCacheStats(stats)
	s.recordStreamingSeriesStats(stats)

	s.metrics.streamingSeriesRequestDurationByStage.WithLabelValues("encode").Observe(stats.streamingSeriesEncodeResponseDuration.Seconds())
	s.metrics.streamingSeriesRequestDurationByStage.WithLabelValues("send").Observe(stats.streamingSeriesSendResponseDuration.Seconds())

	s.metrics.seriesDataFetched.WithLabelValues("chunks", "fetched").Observe(float64(stats.chunksFetched))
	s.metrics.seriesDataSizeFetched.WithLabelValues("chunks", "fetched").Observe(float64(stats.chunksFetchedSizeSum))

	s.metrics.seriesDataFetched.WithLabelValues("chunks", "refetched").Observe(float64(stats.chunksRefetched))
	s.metrics.seriesDataSizeFetched.WithLabelValues("chunks", "refetched").Observe(float64(stats.chunksRefetchedSizeSum))

	s.metrics.seriesBlocksQueried.Observe(float64(stats.blocksQueried))

	if s.fineGrainedChunksCachingEnabled {
		s.metrics.seriesDataTouched.WithLabelValues("chunks", "processed").Observe(float64(stats.chunksProcessed))
		s.metrics.seriesDataSizeTouched.WithLabelValues("chunks", "processed").Observe(float64(stats.chunksProcessedSizeSum))
		// With fine-grained caching we may have touched more chunks than we need because we had to fetch a
		// whole range of chunks, which includes chunks outside the request's minT/maxT.
		s.metrics.seriesDataTouched.WithLabelValues("chunks", "returned").Observe(float64(stats.chunksReturned))
		s.metrics.seriesDataSizeTouched.WithLabelValues("chunks", "returned").Observe(float64(stats.chunksReturnedSizeSum))
	} else {
		s.metrics.seriesDataTouched.WithLabelValues("chunks", "processed").Observe(float64(stats.chunksTouched))
		s.metrics.seriesDataSizeTouched.WithLabelValues("chunks", "processed").Observe(float64(stats.chunksTouchedSizeSum))
		// For the implementation which uses the caching bucket the bytes we touch are the bytes we return.
		s.metrics.seriesDataTouched.WithLabelValues("chunks", "returned").Observe(float64(stats.chunksTouched))
		s.metrics.seriesDataSizeTouched.WithLabelValues("chunks", "returned").Observe(float64(stats.chunksTouchedSizeSum))
	}

	s.metrics.resultSeriesCount.Observe(float64(stats.mergedSeriesCount))
}

func (s *BucketStore) recordLabelNamesCallResult(safeStats *safeQueryStats) {
	stats := safeStats.export()
	s.recordPostingsStats(stats)
	s.recordSeriesStats(stats)
	s.recordCachedPostingStats(stats)
	s.recordSeriesHashCacheStats(stats)
	s.recordStreamingSeriesStats(stats)

	s.metrics.seriesBlocksQueried.Observe(float64(stats.blocksQueried))
}

func (s *BucketStore) recordLabelValuesCallResult(safeStats *safeQueryStats) {
	stats := safeStats.export()
	s.recordPostingsStats(stats)
	s.recordSeriesStats(stats)
	s.recordStreamingSeriesStats(stats)
	s.recordCachedPostingStats(stats)
}

func (s *BucketStore) recordPostingsStats(stats *queryStats) {
	s.metrics.seriesDataTouched.WithLabelValues("postings", "").Observe(float64(stats.postingsTouched))
	s.metrics.seriesDataFetched.WithLabelValues("postings", "").Observe(float64(stats.postingsFetched))
	s.metrics.seriesDataSizeTouched.WithLabelValues("postings", "").Observe(float64(stats.postingsTouchedSizeSum))
	s.metrics.seriesDataSizeFetched.WithLabelValues("postings", "").Observe(float64(stats.postingsFetchedSizeSum))
}

func (s *BucketStore) recordSeriesStats(stats *queryStats) {
	s.metrics.seriesDataTouched.WithLabelValues("series", "processed").Observe(float64(stats.seriesProcessed))
	s.metrics.seriesDataTouched.WithLabelValues("series", "returned").Observe(float64(stats.seriesProcessed - stats.seriesOmitted))
	s.metrics.seriesDataFetched.WithLabelValues("series", "").Observe(float64(stats.seriesFetched))
	s.metrics.seriesDataSizeTouched.WithLabelValues("series", "").Observe(float64(stats.seriesProcessedSizeSum))
	s.metrics.seriesDataSizeFetched.WithLabelValues("series", "").Observe(float64(stats.seriesFetchedSizeSum))
	s.metrics.seriesRefetches.Add(float64(stats.seriesRefetches))
}

func (s *BucketStore) recordStreamingSeriesStats(stats *queryStats) {
	// Track the streaming store-gateway preloading effectiveness metrics only if the request had
	// more than 1 batch. If the request only had 1 batch, then preloading is not triggered at all.
	if stats.streamingSeriesBatchCount > 1 {
		s.metrics.streamingSeriesBatchPreloadingLoadDuration.Observe(stats.streamingSeriesBatchLoadDuration.Seconds())
		s.metrics.streamingSeriesBatchPreloadingWaitDuration.Observe(stats.streamingSeriesWaitBatchLoadedDuration.Seconds())
	}

	s.metrics.streamingSeriesRefsFetchDuration.Observe(stats.streamingSeriesFetchRefsDuration.Seconds())

	s.metrics.streamingSeriesRequestDurationByStage.WithLabelValues("expand_postings").Observe(stats.streamingSeriesExpandPostingsDuration.Seconds())
	s.metrics.streamingSeriesRequestDurationByStage.WithLabelValues("fetch_series_and_chunks").Observe(stats.streamingSeriesFetchSeriesAndChunksDuration.Seconds())
	s.metrics.streamingSeriesRequestDurationByStage.WithLabelValues("other").Observe(stats.streamingSeriesOtherDuration.Seconds())
	s.metrics.streamingSeriesRequestDurationByStage.WithLabelValues("load_index_header").Observe(stats.streamingSeriesIndexHeaderLoadDuration.Seconds())
}

func (s *BucketStore) recordCachedPostingStats(stats *queryStats) {
	s.metrics.cachedPostingsCompressions.WithLabelValues(labelEncode).Add(float64(stats.cachedPostingsCompressions))
	s.metrics.cachedPostingsCompressions.WithLabelValues(labelDecode).Add(float64(stats.cachedPostingsDecompressions))
	s.metrics.cachedPostingsCompressionErrors.WithLabelValues(labelEncode).Add(float64(stats.cachedPostingsCompressionErrors))
	s.metrics.cachedPostingsCompressionErrors.WithLabelValues(labelDecode).Add(float64(stats.cachedPostingsDecompressionErrors))
	s.metrics.cachedPostingsCompressionTimeSeconds.WithLabelValues(labelEncode).Add(stats.cachedPostingsCompressionTimeSum.Seconds())
	s.metrics.cachedPostingsCompressionTimeSeconds.WithLabelValues(labelDecode).Add(stats.cachedPostingsDecompressionTimeSum.Seconds())
	s.metrics.cachedPostingsOriginalSizeBytes.Add(float64(stats.cachedPostingsOriginalSizeSum))
	s.metrics.cachedPostingsCompressedSizeBytes.Add(float64(stats.cachedPostingsCompressedSizeSum))
}

func (s *BucketStore) recordSeriesHashCacheStats(stats *queryStats) {
	s.metrics.seriesHashCacheRequests.Add(float64(stats.seriesHashCacheRequests))
	s.metrics.seriesHashCacheHits.Add(float64(stats.seriesHashCacheHits))
}

func (s *BucketStore) openBlocksForReading(ctx context.Context, skipChunks bool, minT, maxT int64, blockMatchers []*labels.Matcher, stats *safeQueryStats) ([]*bucketBlock, map[ulid.ULID]*bucketIndexReader, map[ulid.ULID]chunkReader) {
	s.blocksMx.RLock()
	defer s.blocksMx.RUnlock()

	// Find all blocks owned by this store-gateway instance and matching the request.
	blocks := s.blockSet.getFor(minT, maxT, blockMatchers)

	indexReaders := make(map[ulid.ULID]*bucketIndexReader, len(blocks))
	for _, b := range blocks {
		indexReaders[b.meta.ULID] = b.loadedIndexReader(s.postingsStrategy, stats)
	}
	if skipChunks {
		return blocks, indexReaders, nil
	}

	chunkReaders := make(map[ulid.ULID]chunkReader, len(blocks))
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

	var (
		stats    = newSafeQueryStats()
		resHints = &hintspb.LabelNamesResponseHints{}
	)

	defer s.recordLabelNamesCallResult(stats)

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

		indexr := b.loadedIndexReader(s.postingsStrategy, stats)

		g.Go(func() error {
			defer runutil.CloseWithLogOnErr(s.logger, indexr, "label names")

			result, err := blockLabelNames(gctx, indexr, reqSeriesMatchers, seriesLimiter, s.maxSeriesPerBatch, s.logger, stats)
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
		if errors.Is(err, context.Canceled) {
			return nil, status.Error(codes.Canceled, err.Error())
		}

		return nil, status.Error(codes.Internal, err.Error())
	}

	stats.update(func(stats *queryStats) {
		stats.blocksQueried = len(sets)
	})

	anyHints, err := types.MarshalAny(resHints)
	if err != nil {
		return nil, status.Error(codes.Unknown, errors.Wrap(err, "marshal label names response hints").Error())
	}

	return &storepb.LabelNamesResponse{
		Names: util.MergeSlices(sets...),
		Hints: anyHints,
	}, nil
}

func blockLabelNames(ctx context.Context, indexr *bucketIndexReader, matchers []*labels.Matcher, seriesLimiter SeriesLimiter, seriesPerBatch int, logger log.Logger, stats *safeQueryStats) ([]string, error) {
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
	seriesSetsIterator, _, _, err := openBlockSeriesChunkRefsSetsIterator(
		ctx,
		seriesPerBatch,
		indexr.block.userID,
		indexr,
		indexr.block.indexCache,
		indexr.block.meta,
		matchers,
		nil,
		cachedSeriesHasher{nil},
		true, false,
		minTime, maxTime,
		1, // we skip chunks, so this doesn't make any difference
		stats,
		nil, nil,
		logger,
	)
	if err != nil {
		return nil, errors.Wrap(err, "fetch series")
	}
	seriesSetsIterator = newLimitingSeriesChunkRefsSetIterator(seriesSetsIterator, NewLimiter(0, nil), seriesLimiter)
	seriesSet := newSeriesChunkRefsSeriesSet(seriesSetsIterator)
	// Extract label names from all series. Many label names will be the same, so we need to deduplicate them.
	labelNames := map[string]struct{}{}
	for seriesSet.Next() {
		ls, _ := seriesSet.At()
		ls.Range(func(l labels.Label) {
			labelNames[l.Name] = struct{}{}
		})
	}
	if seriesSet.Err() != nil {
		return nil, errors.Wrap(seriesSet.Err(), "iterate series")
	}

	names = make([]string, 0, len(labelNames))
	for n := range labelNames {
		names = append(names, n)
	}
	slices.Sort(names)

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
	indexCache.StoreLabelNames(userID, blockID, entry.MatchersKey, data)
}

// LabelValues implements the storepb.StoreServer interface.
func (s *BucketStore) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	reqSeriesMatchers, err := storepb.MatchersToPromMatchers(req.Matchers...)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request labels matchers").Error())
	}

	stats := newSafeQueryStats()
	defer s.recordLabelValuesCallResult(stats)

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

		g.Go(func() error {
			result, err := blockLabelValues(gctx, b, s.postingsStrategy, s.maxSeriesPerBatch, req.Label, reqSeriesMatchers, s.logger, stats)
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
		if errors.Is(err, context.Canceled) {
			return nil, status.Error(codes.Canceled, err.Error())
		}

		return nil, status.Error(codes.Internal, err.Error())
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

// blockLabelValues returns sorted values of the label with requested name,
// optionally restricting the search to the series that match the matchers provided.
// - First we fetch all possible values for this label from the index.
//   - If no matchers were provided, we just return those values.
//
// - Next we load the postings (references to series) for supplied matchers.
// - Then we load the postings for each label-value fetched in the first step.
// - Finally, we check if postings from each label-value intersect postings from matchers.
//   - A non-empty intersection means that a matched series has that value, so we add it to the result.
//
// Notice that when no matchers are provided, the list of matched postings is AllPostings,
// so we could also intersect those with each label's postings being each one non-empty and leading to the same result.
func blockLabelValues(ctx context.Context, b *bucketBlock, postingsStrategy postingsSelectionStrategy, maxSeriesPerBatch int, labelName string, matchers []*labels.Matcher, logger log.Logger, stats *safeQueryStats) ([]string, error) {
	// This index reader shouldn't be used for ExpandedPostings, since it doesn't have the correct strategy.
	labelValuesReader := b.loadedIndexReader(selectAllStrategy{}, stats)
	defer runutil.CloseWithLogOnErr(b.logger, labelValuesReader, "close block index reader")

	values, ok := fetchCachedLabelValues(ctx, b.indexCache, b.userID, b.meta.ULID, labelName, matchers, logger)
	if ok {
		return values, nil
	}

	// TODO: if matchers contains labelName, we could use it to filter out label values here.
	allValuesPostingOffsets, err := b.indexHeaderReader.LabelValuesOffsets(labelName, "", nil)
	if err != nil {
		return nil, errors.Wrap(err, "index header label values")
	}

	if len(matchers) == 0 {
		values = extractLabelValues(allValuesPostingOffsets)
		storeCachedLabelValues(ctx, b.indexCache, b.userID, b.meta.ULID, labelName, matchers, values, logger)
		return values, nil
	}
	strategy := &labelValuesPostingsStrategy{
		matchersStrategy: postingsStrategy,
		allLabelValues:   allValuesPostingOffsets,
	}
	postingsAndSeriesReader := b.indexReader(strategy)
	defer runutil.CloseWithLogOnErr(b.logger, postingsAndSeriesReader, "close block index reader")

	matchersPostings, pendingMatchers, err := postingsAndSeriesReader.ExpandedPostings(ctx, matchers, stats)
	if err != nil {
		return nil, errors.Wrap(err, "expanded postings")
	}
	if len(pendingMatchers) > 0 || strategy.preferSeriesToPostings(matchersPostings) {
		values, err = labelValuesFromSeries(ctx, labelName, maxSeriesPerBatch, pendingMatchers, postingsAndSeriesReader, b, matchersPostings, stats)
	} else {
		values, err = labelValuesFromPostings(ctx, labelName, postingsAndSeriesReader, allValuesPostingOffsets, matchersPostings, stats)
	}
	if err != nil {
		return nil, err
	}

	storeCachedLabelValues(ctx, b.indexCache, b.userID, b.meta.ULID, labelName, matchers, values, logger)
	return values, nil
}

func labelValuesFromSeries(ctx context.Context, labelName string, seriesPerBatch int, pendingMatchers []*labels.Matcher, indexr *bucketIndexReader, b *bucketBlock, matchersPostings []storage.SeriesRef, stats *safeQueryStats) ([]string, error) {
	var iterator seriesChunkRefsSetIterator
	iterator = newLoadingSeriesChunkRefsSetIterator(
		ctx,
		newPostingsSetsIterator(matchersPostings, seriesPerBatch),
		indexr,
		b.indexCache,
		stats,
		b.meta,
		nil,
		nil,
		true, false,
		b.meta.MinTime,
		b.meta.MaxTime,
		b.userID,
		1,
		b.logger,
	)
	if len(pendingMatchers) > 0 {
		iterator = newFilteringSeriesChunkRefsSetIterator(pendingMatchers, iterator, stats)
	}
	iterator = seriesStreamingFetchRefsDurationIterator(iterator, stats)
	seriesSet := newSeriesSetWithoutChunks(ctx, iterator, stats)

	differentValues := make(map[string]struct{})
	for seriesSet.Next() {
		series, _ := seriesSet.At()
		lVal := series.Get(labelName)
		if lVal != "" {
			differentValues[lVal] = struct{}{}
		}
	}
	if seriesSet.Err() != nil {
		return nil, errors.Wrap(seriesSet.Err(), "iterating series for label values")
	}

	vals := make([]string, 0, len(differentValues))
	for val := range differentValues {
		vals = append(vals, val)
	}
	slices.Sort(vals)
	return vals, nil
}

func labelValuesFromPostings(ctx context.Context, labelName string, indexr *bucketIndexReader, allValues []streamindex.PostingListOffset, p []storage.SeriesRef, stats *safeQueryStats) ([]string, error) {
	keys := make([]labels.Label, len(allValues))
	for i, value := range allValues {
		keys[i] = labels.Label{Name: labelName, Value: value.LabelValue}
	}

	fetchedPostings, err := indexr.FetchPostings(ctx, keys, stats)
	if err != nil {
		return nil, errors.Wrap(err, "get postings")
	}

	matched := make([]string, 0, len(allValues))
	for i, value := range allValues {
		intersection := index.Intersect(index.NewListPostings(p), fetchedPostings[i])
		if intersection.Next() {
			matched = append(matched, value.LabelValue)
		}
		if err = intersection.Err(); err != nil {
			return nil, errors.Wrapf(err, "intersecting value %q postings", value.LabelValue)
		}
	}
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
	// This limit is a workaround for panics in decoding large responses. See https://github.com/golang/go/issues/59172
	const valuesLimit = 655360
	if len(values) > valuesLimit {
		level.Debug(spanlogger.FromContext(ctx, logger)).Log("msg", "skipping storing label values response to cache because it exceeds number of values limit", "limit", valuesLimit, "values_count", len(values))
		return
	}
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
	indexCache.StoreLabelValues(userID, blockID, labelName, entry.MatchersKey, data)
}

// bucketBlockSet holds all blocks.
type bucketBlockSet struct {
	mtx    sync.RWMutex
	blocks []*bucketBlock // Blocks sorted by mint, then maxt.
}

// newBucketBlockSet initializes a new set with the known downsampling windows hard-configured.
// (Mimir only supports no-downsampling)
// The set currently does not support arbitrary ranges.
func newBucketBlockSet() *bucketBlockSet {
	return &bucketBlockSet{}
}

func (s *bucketBlockSet) add(b *bucketBlock) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.blocks = append(s.blocks, b)

	// Always sort blocks by min time, then max time.
	sort.Slice(s.blocks, func(j, k int) bool {
		if s.blocks[j].meta.MinTime == s.blocks[k].meta.MinTime {
			return s.blocks[j].meta.MaxTime < s.blocks[k].meta.MaxTime
		}
		return s.blocks[j].meta.MinTime < s.blocks[k].meta.MinTime
	})
	return nil
}

func (s *bucketBlockSet) remove(id ulid.ULID) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for i, b := range s.blocks {
		if b.meta.ULID != id {
			continue
		}
		s.blocks = append(s.blocks[:i], s.blocks[i+1:]...)
		return
	}
}

// getFor returns a time-ordered list of blocks that cover date between mint and maxt.
// It supports overlapping blocks.
//
// NOTE: s.blocks are expected to be sorted in minTime order.
func (s *bucketBlockSet) getFor(mint, maxt int64, blockMatchers []*labels.Matcher) (bs []*bucketBlock) {
	if mint > maxt {
		return nil
	}

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	// Fill the given interval with the blocks within the request mint and maxt.
	for _, b := range s.blocks {
		if b.meta.MaxTime <= mint {
			continue
		}
		// NOTE: Block intervals are half-open: [b.MinTime, b.MaxTime).
		if b.meta.MinTime > maxt {
			break
		}

		// Include the block in the list of matching ones only if there are no block-level matchers
		// or they actually match.
		if len(blockMatchers) == 0 || b.matchLabels(blockMatchers) {
			bs = append(bs, b)
		}
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
	meta       *block.Meta
	dir        string
	indexCache indexcache.IndexCache

	indexHeaderReader indexheader.Reader

	chunkObjs []string

	pendingReaders sync.WaitGroup

	partitioners blockPartitioners

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
	meta *block.Meta,
	bkt objstore.BucketReader,
	dir string,
	indexCache indexcache.IndexCache,
	indexHeadReader indexheader.Reader,
	p blockPartitioners,
) (b *bucketBlock, err error) {
	b = &bucketBlock{
		userID:            userID,
		logger:            logger,
		metrics:           metrics,
		bkt:               bkt,
		indexCache:        indexCache,
		dir:               dir,
		partitioners:      p,
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

func (b *bucketBlock) indexRangeReader(ctx context.Context, off, length int64) (io.ReadCloser, error) {
	r, err := b.bkt.GetRange(ctx, b.indexFilename(), off, length)
	if err != nil {
		return nil, errors.Wrap(err, "get index range reader")
	}
	return r, nil
}

func (b *bucketBlock) readIndexRange(ctx context.Context, off, length int64) ([]byte, error) {
	r, err := b.indexRangeReader(ctx, off, length)
	if err != nil {
		return nil, err
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

func (b *bucketBlock) chunkRangeReader(ctx context.Context, seq int, off, length int64) (io.ReadCloser, error) {
	if seq < 0 || seq >= len(b.chunkObjs) {
		return nil, errors.Errorf("unknown segment file for index %d", seq)
	}

	ctx = bucketcache.WithMemoryPool(ctx, chunkBytesSlicePool, chunkBytesSlabSize)
	return b.bkt.GetRange(ctx, b.chunkObjs[seq], off, length)
}

func (b *bucketBlock) loadedIndexReader(postingsStrategy postingsSelectionStrategy, stats *safeQueryStats) *bucketIndexReader {
	loadStartTime := time.Now()
	// Call IndexVersion to lazy load the index header if it lazy-loaded.
	_, _ = b.indexHeaderReader.IndexVersion()
	stats.update(func(stats *queryStats) {
		stats.streamingSeriesIndexHeaderLoadDuration += time.Since(loadStartTime)
	})

	return b.indexReader(postingsStrategy)
}

func (b *bucketBlock) indexReader(postingsStrategy postingsSelectionStrategy) *bucketIndexReader {
	b.pendingReaders.Add(1)
	return newBucketIndexReader(b, postingsStrategy)
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

// decodeSeries decodes a series entry from the given byte slice decoding all chunk metas of the series.
// If skipChunks is specified decodeSeries does not return any chunks, but only labels and only if at least single chunk is within time range.
// decodeSeries returns false, when there are no series data for given time range.
func decodeSeries(b []byte, lsetPool *pool.SlabPool[symbolizedLabel], chks *[]chunks.Meta, resMint, resMaxt int64, skipChunks, streamingSeries bool) (ok bool, lset []symbolizedLabel, err error) {

	*chks = (*chks)[:0]

	d := encoding.Decbuf{B: b}

	// Read labels without looking up symbols.
	k := d.Uvarint()
	lset = lsetPool.Get(k)[:0]
	for i := 0; i < k; i++ {
		lno := uint32(d.Uvarint())
		lvo := uint32(d.Uvarint())
		lset = append(lset, symbolizedLabel{name: lno, value: lvo})
	}
	// Read the chunks meta data.
	k = d.Uvarint()
	if k == 0 {
		return false, nil, d.Err()
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

		// Found a chunk.
		if skipChunks {
			if streamingSeries {
				// We are not interested in chunks, but we want the series to overlap with the query mint-maxt.
				if maxt >= resMint && mint <= resMaxt {
					// Chunk overlaps.
					return true, lset, nil
				}
			} else {
				// We are not interested in chunks and we know there is at least one, that's enough to return series.
				return true, lset, nil
			}
		} else {
			*chks = append(*chks, chunks.Meta{
				Ref:     chunks.ChunkRef(ref),
				MinTime: mint,
				MaxTime: maxt,
			})
		}

		mint = maxt
	}
	return len(*chks) > 0, lset, d.Err()
}

func maybeNilShard(shard *sharding.ShardSelector) sharding.ShardSelector {
	if shard == nil {
		return sharding.ShardSelector{}
	}
	return *shard
}
