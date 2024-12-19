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
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/types"
	"github.com/grafana/dskit/gate"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/dskit/runutil"
	"github.com/grafana/dskit/services"
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
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketcache"
	"github.com/grafana/mimir/pkg/storegateway/hintspb"
	"github.com/grafana/mimir/pkg/storegateway/indexcache"
	"github.com/grafana/mimir/pkg/storegateway/indexheader"
	streamindex "github.com/grafana/mimir/pkg/storegateway/indexheader/index"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/pool"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	// Labels for metrics.
	labelEncode = "encode"
	labelDecode = "decode"

	targetQueryStreamBatchMessageSize = 1 * 1024 * 1024
)

type BucketStoreStats struct {
	// BlocksLoadedTotal is the total number of blocks currently loaded in the bucket store.
	BlocksLoadedTotal int
}

// BucketStore implements the store API backed by a bucket. It loads all index
// files to local disk.
//
// NOTE: Bucket store reencodes postings using diff+varint+snappy when storing to cache.
// This makes them smaller, but takes extra CPU and memory.
// When used with in-memory cache, memory usage should decrease overall, thanks to postings being smaller.
type BucketStore struct {
	services.Service

	userID          string
	logger          log.Logger
	metrics         *BucketStoreMetrics
	bkt             objstore.InstrumentedBucketReader
	fetcher         block.MetadataFetcher
	dir             string
	indexCache      indexcache.IndexCache
	indexReaderPool *indexheader.ReaderPool
	seriesHashCache *hashcache.SeriesHashCache

	snapshotter services.Service

	// Set of blocks that have the same labels
	blockSet *bucketBlockSet

	// Number of goroutines to use when syncing blocks from object storage.
	blockSyncConcurrency int

	// maxSeriesPerBatch controls the batch size to use when fetching series.
	// This is not restricted to the Series() RPC.
	// This value must be greater than zero.
	maxSeriesPerBatch int

	// Query gate which limits the maximum amount of concurrent queries.
	queryGate gate.Gate

	// Gate used to limit concurrency on loading index-headers across all tenants.
	lazyLoadingGate gate.Gate

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

func (noopCache) StorePostings(string, ulid.ULID, labels.Label, []byte, time.Duration) {}
func (noopCache) FetchMultiPostings(_ context.Context, _ string, _ ulid.ULID, keys []labels.Label) indexcache.BytesResult {
	return &indexcache.MapIterator[labels.Label]{Keys: keys}
}

func (noopCache) StoreSeriesForRef(string, ulid.ULID, storage.SeriesRef, []byte, time.Duration) {}
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

// WithQueryGate sets a queryGate to use instead of a gate.NewNoop().
func WithQueryGate(queryGate gate.Gate) BucketStoreOption {
	return func(s *BucketStore) {
		s.queryGate = queryGate
	}
}

// WithLazyLoadingGate sets a lazyLoadingGate to use instead of a gate.NewNoop().
func WithLazyLoadingGate(lazyLoadingGate gate.Gate) BucketStoreOption {
	return func(s *BucketStore) {
		s.lazyLoadingGate = lazyLoadingGate
	}
}

// NewBucketStore creates a new bucket backed store that implements the store API against
// an object store bucket. It is optimized to work against high latency backends.
func NewBucketStore(
	userID string,
	bkt objstore.InstrumentedBucketReader,
	fetcher block.MetadataFetcher,
	dir string,
	bucketStoreConfig tsdb.BucketStoreConfig,
	postingsStrategy postingsSelectionStrategy,
	chunksLimiterFactory ChunksLimiterFactory,
	seriesLimiterFactory SeriesLimiterFactory,
	partitioners blockPartitioners,
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
		blockSet:                    newBucketBlockSet(),
		blockSyncConcurrency:        bucketStoreConfig.BlockSyncConcurrency,
		queryGate:                   gate.NewNoop(),
		lazyLoadingGate:             gate.NewNoop(),
		chunksLimiterFactory:        chunksLimiterFactory,
		seriesLimiterFactory:        seriesLimiterFactory,
		partitioners:                partitioners,
		postingOffsetsInMemSampling: bucketStoreConfig.PostingOffsetsInMemSampling,
		indexHeaderCfg:              bucketStoreConfig.IndexHeader,
		seriesHashCache:             seriesHashCache,
		metrics:                     metrics,
		userID:                      userID,
		maxSeriesPerBatch:           bucketStoreConfig.StreamingBatchSize,
		postingsStrategy:            postingsStrategy,
	}

	for _, option := range options {
		option(s)
	}

	s.indexReaderPool = indexheader.NewReaderPool(s.logger, bucketStoreConfig.IndexHeader, s.lazyLoadingGate, metrics.indexHeaderReaderMetrics)

	if bucketStoreConfig.IndexHeader.EagerLoadingStartupEnabled {
		snapConfig := indexheader.SnapshotterConfig{
			Path:            dir,
			UserID:          userID,
			PersistInterval: bucketStoreConfig.IndexHeader.EagerLoadingPersistInterval,
		}
		s.snapshotter = indexheader.NewSnapshotter(s.logger, snapConfig, s.indexReaderPool)
	} else {
		s.snapshotter = services.NewIdleService(nil, nil)
	}

	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, errors.Wrap(err, "create dir")
	}

	s.Service = services.NewIdleService(s.start, s.stop)
	return s, nil
}

func (s *BucketStore) start(context.Context) error {
	// Use context.Background() so that we stop the index reader pool ourselves and do it after closing all blocks.
	return services.StartAndAwaitRunning(context.Background(), s.indexReaderPool)
}

func (s *BucketStore) stop(err error) error {
	errs := multierror.New(err)
	errs.Add(s.closeAllBlocks())
	// The snapshotter depends on the reader pool, so we close the snapshotter first.
	errs.Add(services.StopAndAwaitTerminated(context.Background(), s.snapshotter))
	errs.Add(services.StopAndAwaitTerminated(context.Background(), s.indexReaderPool))
	return errs.Err()
}

// RemoveBlocksAndClose remove all blocks from local disk and releases all resources associated with the BucketStore.
func (s *BucketStore) RemoveBlocksAndClose() error {
	errs := multierror.New()
	if err := services.StopAndAwaitTerminated(context.Background(), s); err != nil {
		errs.Add(fmt.Errorf("stopping subservices: %w", err))
	}
	// Remove the blocks even if the service didn't gracefully stop.
	// We want to free up disk resources given these blocks will likely not be queried again.
	if err := s.removeAllBlocks(); err != nil {
		errs.Add(fmt.Errorf("remove all blocks: %w", err))
	}

	return errs.Err()
}

// Stats returns statistics about the BucketStore instance.
func (s *BucketStore) Stats() BucketStoreStats {
	return BucketStoreStats{
		BlocksLoadedTotal: s.blockSet.len(),
	}
}

// SyncBlocks synchronizes the stores state with the Bucket bucket.
// It will reuse disk space as persistent cache based on s.dir param.
func (s *BucketStore) SyncBlocks(ctx context.Context) error {
	return s.syncBlocks(ctx)
}

func (s *BucketStore) syncBlocks(ctx context.Context) error {
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
		if s.blockSet.contains(id) {
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

	blockIDs := s.blockSet.openBlocksULIDs()
	for _, id := range blockIDs {
		if _, ok := metas[id]; ok {
			continue
		}
		if err := s.removeBlock(id); err != nil {
			level.Warn(s.logger).Log("msg", "drop of outdated block failed", "block", id, "err", err)
		}
		level.Info(s.logger).Log("msg", "dropped outdated block", "block", id)
	}

	// Start snapshotter in the end of the sync, but do that only once per BucketStore's lifetime.
	// We do that here, so the snapshotter watched after blocks from both initial sync and those discovered later.
	// If it's already started this will return an error. We ignore that because syncBlocks can run multiple times
	// We pass context.Background() because we want to stop it ourselves as opposed to stopping it as soon as the runtime context is cancelled..
	_ = s.snapshotter.StartAsync(context.Background())

	return nil
}

// InitialSync perform blocking sync with extra step at the end to delete locally saved blocks that are no longer
// present in the bucket. The mismatch of these can only happen between restarts, so we can do that only once per startup.
func (s *BucketStore) InitialSync(ctx context.Context) error {
	// Read the snapshot before running the sync. After we run a sync we'll start persisting the snapshots again,
	// so we need to read the pre-shutdown snapshot before the sync.
	previouslyLoadedBlocks := s.tryRestoreLoadedBlocksSet()

	if err := s.syncBlocks(ctx); err != nil {
		return errors.Wrap(err, "sync block")
	}
	if s.indexHeaderCfg.EagerLoadingStartupEnabled {
		s.loadBlocks(ctx, previouslyLoadedBlocks)
	}

	err := s.cleanUpUnownedBlocks()
	if err != nil {
		return err
	}

	return nil
}

func (s *BucketStore) tryRestoreLoadedBlocksSet() map[ulid.ULID]int64 {
	previouslyLoadedBlocks, err := indexheader.RestoreLoadedBlocks(s.dir)
	if err != nil {
		level.Warn(s.logger).Log(
			"msg", "loading the list of index-headers from snapshot file failed; not eagerly loading index-headers for tenant",
			"dir", s.dir,
			"err", err,
		)
		// Don't fail initialization. If eager loading doesn't happen, then we will load index-headers lazily.
		// Lazy loading which is slower, but not worth failing startup for.
	}
	return previouslyLoadedBlocks
}

func (s *BucketStore) loadBlocks(ctx context.Context, blocks map[ulid.ULID]int64) {
	// This is not happening during a request so we can ignore the stats.
	ignoredStats := newSafeQueryStats()
	// We ignore the time the block was used because it can only be in the map if it was still loaded before the shutdown
	s.blockSet.forEach(func(b *bucketBlock) {
		if _, ok := blocks[b.meta.ULID]; !ok {
			return
		}
		b.ensureIndexHeaderLoaded(ctx, ignoredStats)
	})
}

func (s *BucketStore) cleanUpUnownedBlocks() error {
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
		if s.blockSet.contains(id) {
			continue
		}

		// No such block loaded, remove the local dir.
		if err := os.RemoveAll(path.Join(s.dir, id.String())); err != nil {
			level.Warn(s.logger).Log("msg", "failed to remove block which is not needed", "err", err)
		}
	}

	return nil
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
			level.Error(s.logger).Log("msg", "loading block failed", "elapsed", time.Since(start), "id", meta.ULID, "err", err)
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

	if err = s.blockSet.add(b); err != nil {
		return errors.Wrap(err, "add block to set")
	}

	return nil
}

func (s *BucketStore) removeBlock(id ulid.ULID) (returnErr error) {
	defer func() {
		if returnErr != nil {
			s.metrics.blockDropFailures.Inc()
		}
	}()

	b := s.blockSet.remove(id)
	if b == nil {
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

func (s *BucketStore) closeAllBlocks() error {
	return s.blockSet.closeAll()
}

func (s *BucketStore) removeAllBlocks() error {
	blockIDs := s.blockSet.allBlockULIDs()

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
	return s.blockSet.timerange()
}

type seriesChunks struct {
	lset labels.Labels
	chks []storepb.AggrChunk
}

// Series implements the storegatewaypb.StoreGatewayServer interface.
func (s *BucketStore) Series(req *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer) (err error) {
	if req.SkipChunks {
		// We don't do the streaming call if we are not requesting the chunks.
		req.StreamingChunksBatchSize = 0
	}
	defer func() { err = mapSeriesError(err) }()

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
		spanLogger       = spanlogger.FromContext(srv.Context(), s.logger)
		ctx              = srv.Context()
		stats            = newSafeQueryStats()
		reqBlockMatchers []*labels.Matcher
	)
	defer s.recordSeriesCallResult(stats)
	defer s.recordRequestAmbientTime(stats, time.Now())

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

	logSeriesRequestToSpan(srv.Context(), s.logger, req.MinTime, req.MaxTime, matchers, reqBlockMatchers, shardSelector, req.StreamingChunksBatchSize)

	blocks, indexReaders, chunkReaders := s.openBlocksForReading(ctx, req.SkipChunks, req.MinTime, req.MaxTime, reqBlockMatchers, stats)
	// We must keep the readers open until all their data has been sent.
	for _, r := range indexReaders {
		defer runutil.CloseWithLogOnErr(s.logger, r, "close block index reader")
	}
	for _, r := range chunkReaders {
		defer runutil.CloseWithLogOnErr(s.logger, r, "close block chunk reader")
	}

	var readers *bucketChunkReaders
	if !req.SkipChunks {
		readers = newChunkReaders(chunkReaders)
	}

	// Wait for the query gate only after opening blocks. Opening blocks is usually fast (~1ms),
	// but sometimes it can take minutes if the block isn't loaded and there is a surge in queries for unloaded blocks.
	done, err := s.limitConcurrentQueries(ctx, stats)
	if err != nil {
		return err
	}
	defer done()

	var (
		streamingIterators *streamingSeriesIterators
		resHints           = &hintspb.SeriesResponseHints{}
	)
	for _, b := range blocks {
		resHints.AddQueriedBlock(b.meta.ULID)

		if b.meta.Compaction.Level == 1 && b.meta.Thanos.Source == block.ReceiveSource && !b.queried.Load() {
			level.Debug(s.logger).Log("msg", "queried non-compacted block", "blockId", b.meta.ULID, "ooo", b.meta.Compaction.FromOutOfOrder())
		}

		b.queried.Store(true)
	}
	if err := s.sendHints(srv, resHints); err != nil {
		return err
	}

	streamingSeriesCount := 0
	if req.StreamingChunksBatchSize > 0 {
		var (
			seriesSet       storepb.SeriesSet
			seriesLoadStart = time.Now()
			chunksLimiter   = s.chunksLimiterFactory(s.metrics.queriesDropped.WithLabelValues("chunks"))
			seriesLimiter   = s.seriesLimiterFactory(s.metrics.queriesDropped.WithLabelValues("series"))
		)

		seriesSet, streamingIterators, err = s.createIteratorForChunksStreamingLabelsPhase(ctx, req, blocks, indexReaders, shardSelector, matchers, chunksLimiter, seriesLimiter, stats)
		if err != nil {
			return err
		}

		streamingSeriesCount, err = s.sendStreamingSeriesLabelsAndStats(req, srv, stats, seriesSet)
		if err != nil {
			return err
		}
		spanLogger.DebugLog(
			"msg", "sent streaming series",
			"num_series", streamingSeriesCount,
			"duration", time.Since(seriesLoadStart),
		)

		if streamingSeriesCount == 0 {
			// There is no series to send chunks for.
			return nil
		}
	}

	// We create the limiter twice in the case of streaming so that we don't double count the series
	// and hit the limit prematurely.
	chunksLimiter := s.chunksLimiterFactory(s.metrics.queriesDropped.WithLabelValues("chunks"))
	seriesLimiter := s.seriesLimiterFactory(s.metrics.queriesDropped.WithLabelValues("series"))

	start := time.Now()
	if req.StreamingChunksBatchSize > 0 {
		seriesChunkIt := s.createIteratorForChunksStreamingChunksPhase(ctx, readers, stats, chunksLimiter, seriesLimiter, streamingIterators)
		err = s.sendStreamingChunks(req, srv, seriesChunkIt, stats, streamingSeriesCount)
	} else {
		var seriesSet storepb.SeriesSet
		seriesSet, err = s.createIteratorForNonChunksStreamingRequest(ctx, req, blocks, indexReaders, readers, shardSelector, matchers, chunksLimiter, seriesLimiter, stats)
		if err != nil {
			return err
		}
		err = s.sendSeriesChunks(req, srv, seriesSet, stats)
	}
	if err != nil {
		return
	}

	numSeries, numChunks := stats.seriesAndChunksCount()
	debugMessage := "sent series"
	if req.StreamingChunksBatchSize > 0 {
		debugMessage = "sent streaming chunks"
	}
	spanLogger.DebugLog(
		"msg", debugMessage,
		"num_series", numSeries,
		"num_chunks", numChunks,
		"duration", time.Since(start),
	)

	if req.StreamingChunksBatchSize == 0 {
		// Stats were not sent before, so send it now.
		return s.sendStats(srv, stats)
	}

	return nil
}

func mapSeriesError(err error) error {
	if err == nil {
		return err
	}

	var stGwErr storeGatewayError
	switch {
	case errors.As(err, &stGwErr):
		switch cause := stGwErr.errorCause(); cause {
		case mimirpb.INSTANCE_LIMIT:
			return globalerror.WrapErrorWithGRPCStatus(stGwErr, codes.Unavailable, &mimirpb.ErrorDetails{Cause: cause}).Err()
		default:
			return globalerror.WrapErrorWithGRPCStatus(stGwErr, codes.Internal, &mimirpb.ErrorDetails{Cause: cause}).Err()
		}
	default:
		code := codes.Internal
		if st, ok := grpcutil.ErrorToStatus(err); ok {
			code = st.Code()
		} else if errors.Is(err, context.Canceled) {
			code = codes.Canceled
		}
		return status.Error(code, err.Error())
	}
}

func (s *BucketStore) recordRequestAmbientTime(stats *safeQueryStats, requestStart time.Time) {
	stats.update(func(stats *queryStats) {
		stats.streamingSeriesAmbientTime += time.Since(requestStart)
	})
}

func (s *BucketStore) limitConcurrentQueries(ctx context.Context, stats *safeQueryStats) (done func(), err error) {
	waitStart := time.Now()
	err = s.queryGate.Start(ctx)
	waited := time.Since(waitStart)

	stats.update(func(stats *queryStats) { stats.streamingSeriesConcurrencyLimitWaitDuration = waited })
	level.Debug(spanlogger.FromContext(ctx, s.logger)).Log("msg", "waited for turn on query concurrency gate", "duration", waited)

	if err != nil {
		return nil, errors.Wrapf(err, "failed to wait for turn")
	}
	return s.queryGate.Done, nil
}

// sendStreamingSeriesLabelsAndStats sends the labels of the streaming series.
// Since hints and stats need to be sent before the "end of stream" streaming series message,
// this function also sends the hints and the stats.
func (s *BucketStore) sendStreamingSeriesLabelsAndStats(
	req *storepb.SeriesRequest,
	srv storegatewaypb.StoreGateway_SeriesServer,
	stats *safeQueryStats,
	seriesSet storepb.SeriesSet,
) (numSeries int, err error) {
	var (
		encodeDuration = time.Duration(0)
		sendDuration   = time.Duration(0)
	)
	defer stats.update(func(stats *queryStats) {
		stats.streamingSeriesEncodeResponseDuration += encodeDuration
		stats.streamingSeriesSendResponseDuration += sendDuration
	})

	seriesBuffer := make([]*storepb.StreamingSeries, req.StreamingChunksBatchSize)
	for i := range seriesBuffer {
		seriesBuffer[i] = &storepb.StreamingSeries{}
	}
	seriesBatch := &storepb.StreamingSeriesBatch{
		Series: seriesBuffer[:0],
	}
	// TODO: can we send this in parallel while we start fetching the chunks below?
	for seriesSet.Next() {
		numSeries++
		var lset labels.Labels
		// Although subsequent call to seriesSet.Next() may release the memory of this series object,
		// it is safe to hold onto the labels because they are not released.
		lset, _ = seriesSet.At()

		// We are re-using the slice for every batch this way.
		seriesBatch.Series = seriesBatch.Series[:len(seriesBatch.Series)+1]
		seriesBatch.Series[len(seriesBatch.Series)-1].Labels = mimirpb.FromLabelsToLabelAdapters(lset)

		if len(seriesBatch.Series) == int(req.StreamingChunksBatchSize) {
			err := s.sendMessage("streaming series", srv, storepb.NewStreamingSeriesResponse(seriesBatch), &encodeDuration, &sendDuration)
			if err != nil {
				return 0, err
			}
			seriesBatch.Series = seriesBatch.Series[:0]
		}
	}
	if seriesSet.Err() != nil {
		return 0, errors.Wrap(seriesSet.Err(), "expand series set")
	}

	// We need to send stats before sending IsEndOfSeriesStream=true.
	if err := s.sendStats(srv, stats); err != nil {
		return 0, err
	}

	// Send any remaining series and signal that there are no more series.
	seriesBatch.IsEndOfSeriesStream = true
	err = s.sendMessage("streaming series", srv, storepb.NewStreamingSeriesResponse(seriesBatch), &encodeDuration, &sendDuration)
	return numSeries, err
}

func (s *BucketStore) sendStreamingChunks(
	req *storepb.SeriesRequest,
	srv storegatewaypb.StoreGateway_SeriesServer,
	it iterator[seriesChunksSet],
	stats *safeQueryStats,
	totalSeriesCount int,
) error {
	var (
		encodeDuration           time.Duration
		sendDuration             time.Duration
		seriesCount, chunksCount int
	)

	defer stats.update(func(stats *queryStats) {
		stats.mergedSeriesCount += seriesCount
		stats.mergedChunksCount += chunksCount

		stats.streamingSeriesEncodeResponseDuration += encodeDuration
		stats.streamingSeriesSendResponseDuration += sendDuration
	})

	var (
		batchSizeBytes int
		chunksBuffer   = make([]*storepb.StreamingChunks, req.StreamingChunksBatchSize)
	)
	for i := range chunksBuffer {
		chunksBuffer[i] = &storepb.StreamingChunks{}
	}
	haveSentEstimatedChunks := false
	chunksBatch := &storepb.StreamingChunksBatch{Series: chunksBuffer[:0]}
	for it.Next() {
		set := it.At()

		if len(set.series) == 0 {
			set.release()
			continue
		}

		// We send the estimate before any chunks.
		if !haveSentEstimatedChunks {
			seriesInBatch := len(set.series)
			chunksInBatch := 0

			for _, sc := range set.series {
				chunksInBatch += len(sc.chks)
			}

			estimate := uint64(totalSeriesCount * chunksInBatch / seriesInBatch)
			err := s.sendMessage("streaming chunks estimate", srv, storepb.NewStreamingChunksEstimate(estimate), &encodeDuration, &sendDuration)
			if err != nil {
				return err
			}

			haveSentEstimatedChunks = true
		}

		for _, sc := range set.series {
			seriesCount++
			chunksBatch.Series = chunksBatch.Series[:len(chunksBatch.Series)+1]
			lastSeries := chunksBatch.Series[len(chunksBatch.Series)-1]
			lastSeries.Chunks = sc.chks
			lastSeries.SeriesIndex = uint64(seriesCount - 1)

			batchSizeBytes += lastSeries.Size()

			chunksCount += len(sc.chks)
			s.metrics.chunkSizeBytes.Observe(float64(chunksSize(sc.chks)))

			// We are not strictly required to be under targetQueryStreamBatchMessageSize.
			// The aim is to not hit gRPC and TCP limits, hence some overage is ok.
			if batchSizeBytes > targetQueryStreamBatchMessageSize || len(chunksBatch.Series) >= int(req.StreamingChunksBatchSize) {
				err := s.sendMessage("streaming chunks", srv, storepb.NewStreamingChunksResponse(chunksBatch), &encodeDuration, &sendDuration)
				if err != nil {
					return err
				}
				chunksBatch.Series = chunksBatch.Series[:0]
				batchSizeBytes = 0
			}
		}

		if len(chunksBatch.Series) > 0 {
			// Still some chunks left to send before we release the batch.
			err := s.sendMessage("streaming chunks", srv, storepb.NewStreamingChunksResponse(chunksBatch), &encodeDuration, &sendDuration)
			if err != nil {
				return err
			}
			chunksBatch.Series = chunksBatch.Series[:0]
			batchSizeBytes = 0
		}

		set.release()
	}

	if it.Err() != nil {
		return it.Err()
	}

	// If we never sent an estimate (because there were no batches, or no batch had any series), send it now.
	if !haveSentEstimatedChunks {
		err := s.sendMessage("streaming chunks estimate", srv, storepb.NewStreamingChunksEstimate(0), &encodeDuration, &sendDuration)
		if err != nil {
			return err
		}
	}

	return it.Err()
}

func (s *BucketStore) sendSeriesChunks(
	req *storepb.SeriesRequest,
	srv storegatewaypb.StoreGateway_SeriesServer,
	seriesSet storepb.SeriesSet,
	stats *safeQueryStats,
) error {
	var (
		encodeDuration           time.Duration
		sendDuration             time.Duration
		seriesCount, chunksCount int
	)

	defer stats.update(func(stats *queryStats) {
		stats.mergedSeriesCount += seriesCount
		stats.mergedChunksCount += chunksCount

		stats.streamingSeriesEncodeResponseDuration += encodeDuration
		stats.streamingSeriesSendResponseDuration += sendDuration
	})

	for seriesSet.Next() {
		seriesCount++
		// IMPORTANT: do not retain the memory returned by seriesSet.At() beyond this loop cycle
		// because the subsequent call to seriesSet.Next() may release it. But it is safe to hold
		// onto lset because the labels are not released.
		lset, chks := seriesSet.At()
		series := storepb.Series{
			Labels: mimirpb.FromLabelsToLabelAdapters(lset),
		}
		if !req.SkipChunks {
			series.Chunks = chks
			chunksCount += len(chks)
			s.metrics.chunkSizeBytes.Observe(float64(chunksSize(chks)))
		}

		err := s.sendMessage("series", srv, storepb.NewSeriesResponse(&series), &encodeDuration, &sendDuration)
		if err != nil {
			return err
		}
	}
	if seriesSet.Err() != nil {
		return errors.Wrap(seriesSet.Err(), "expand series set")
	}

	return nil
}

func (s *BucketStore) sendMessage(typ string, srv storegatewaypb.StoreGateway_SeriesServer, msg interface{}, encodeDuration, sendDuration *time.Duration) error {
	// We encode it ourselves into a PreparedMsg in order to measure the time it takes.
	encodeBegin := time.Now()
	pmsg := &grpc.PreparedMsg{}
	err := pmsg.Encode(srv, msg)
	*encodeDuration += time.Since(encodeBegin)
	if err != nil {
		return status.Error(codes.Internal, errors.Wrapf(err, "encode %s response", typ).Error())
	}

	sendBegin := time.Now()
	err = srv.SendMsg(pmsg)
	*sendDuration += time.Since(sendBegin)
	if err != nil {
		return status.Error(codes.Unknown, errors.Wrapf(err, "send %s response", typ).Error())
	}

	return nil
}

func (s *BucketStore) sendHints(srv storegatewaypb.StoreGateway_SeriesServer, resHints *hintspb.SeriesResponseHints) error {
	var anyHints *types.Any
	var err error
	if anyHints, err = types.MarshalAny(resHints); err != nil {
		return status.Error(codes.Internal, errors.Wrap(err, "marshal series response hints").Error())
	}

	if err := srv.Send(storepb.NewHintsSeriesResponse(anyHints)); err != nil {
		return status.Error(codes.Unknown, errors.Wrap(err, "send series response hints").Error())
	}

	return nil
}

func (s *BucketStore) sendStats(srv storegatewaypb.StoreGateway_SeriesServer, stats *safeQueryStats) error {
	var encodeDuration, sendDuration time.Duration
	defer stats.update(func(stats *queryStats) {
		stats.streamingSeriesSendResponseDuration += sendDuration
		stats.streamingSeriesEncodeResponseDuration += encodeDuration
	})
	unsafeStats := stats.export()
	if err := s.sendMessage("series response stats", srv, storepb.NewStatsResponse(unsafeStats.postingsTouchedSizeSum+unsafeStats.seriesProcessedSizeSum), &encodeDuration, &sendDuration); err != nil {
		return err
	}
	return nil
}

func logSeriesRequestToSpan(ctx context.Context, l log.Logger, minT, maxT int64, matchers, blockMatchers []*labels.Matcher, shardSelector *sharding.ShardSelector, streamingChunksBatchSize uint64) {
	spanLogger := spanlogger.FromContext(ctx, l)
	spanLogger.DebugLog(
		"msg", "BucketStore.Series",
		"request min time", time.UnixMilli(minT).UTC().Format(time.RFC3339Nano),
		"request max time", time.UnixMilli(maxT).UTC().Format(time.RFC3339Nano),
		"request matchers", util.MatchersStringer(matchers),
		"request block matchers", util.MatchersStringer(blockMatchers),
		"request shard selector", maybeNilShard(shardSelector).LabelValue(),
		"streaming chunks batch size", streamingChunksBatchSize,
	)
}

func chunksSize(chks []storepb.AggrChunk) (size int) {
	for _, chk := range chks {
		size += chk.Size() // This gets the encoded proto size.
	}
	return size
}

// createIteratorForNonChunksStreamingRequest is used when the streaming feature is not enabled.
func (s *BucketStore) createIteratorForNonChunksStreamingRequest(
	ctx context.Context,
	req *storepb.SeriesRequest,
	blocks []*bucketBlock,
	indexReaders map[ulid.ULID]*bucketIndexReader,
	chunkReaders *bucketChunkReaders,
	shardSelector *sharding.ShardSelector,
	matchers []*labels.Matcher,
	chunksLimiter ChunksLimiter,
	seriesLimiter SeriesLimiter,
	stats *safeQueryStats,
) (storepb.SeriesSet, error) {
	strategy := defaultStrategy
	if req.SkipChunks {
		strategy = noChunkRefs
	}
	it, err := s.getSeriesIteratorFromBlocks(ctx, req, blocks, indexReaders, shardSelector, matchers, chunksLimiter, seriesLimiter, stats, strategy, nil)
	if err != nil {
		return nil, err
	}

	var set storepb.SeriesSet
	if !req.SkipChunks {
		ss := newChunksPreloadingIterator(ctx, s.logger, s.userID, *chunkReaders, it, s.maxSeriesPerBatch, stats)
		set = newSeriesChunksSeriesSet(ss)
	} else {
		set = newSeriesSetWithoutChunks(ctx, it, stats)
	}
	return set, nil
}

// createIteratorForChunksStreamingLabelsPhase is used when streaming feature is enabled.
// It returns a series set that only contains the series labels without any chunks information.
// The streamingSeriesIterators should be re-used when getting chunks to save on computation.
func (s *BucketStore) createIteratorForChunksStreamingLabelsPhase(
	ctx context.Context,
	req *storepb.SeriesRequest,
	blocks []*bucketBlock,
	indexReaders map[ulid.ULID]*bucketIndexReader,
	shardSelector *sharding.ShardSelector,
	matchers []*labels.Matcher,
	chunksLimiter ChunksLimiter,
	seriesLimiter SeriesLimiter,
	stats *safeQueryStats,
) (storepb.SeriesSet, *streamingSeriesIterators, error) {
	streamingIterators := newStreamingSeriesIterators()
	it, err := s.getSeriesIteratorFromBlocks(ctx, req, blocks, indexReaders, shardSelector, matchers, chunksLimiter, seriesLimiter, stats, overlapMintMaxt, streamingIterators)
	if err != nil {
		return nil, nil, err
	}

	return newSeriesSetWithoutChunks(ctx, it, stats), streamingIterators, nil
}

// createIteratorForChunksStreamingChunksPhase is used when streaming feature is enabled.
// It returns an iterator to go over the chunks for the series returned in the createIteratorForChunksStreamingLabelsPhase call.
// It is required to pass the iterators returned by the createIteratorForChunksStreamingLabelsPhase call for reuse.
func (s *BucketStore) createIteratorForChunksStreamingChunksPhase(
	ctx context.Context,
	chunkReaders *bucketChunkReaders,
	stats *safeQueryStats,
	chunksLimiter ChunksLimiter,
	seriesLimiter SeriesLimiter,
	iterators *streamingSeriesIterators,
) iterator[seriesChunksSet] {
	preparedIterators := iterators.prepareForChunksStreamingPhase()
	it := s.getSeriesIteratorFromPerBlockIterators(preparedIterators, chunksLimiter, seriesLimiter)
	scsi := newChunksPreloadingIterator(ctx, s.logger, s.userID, *chunkReaders, it, s.maxSeriesPerBatch, stats)

	return scsi
}

func (s *BucketStore) getSeriesIteratorFromBlocks(
	ctx context.Context,
	req *storepb.SeriesRequest,
	blocks []*bucketBlock,
	indexReaders map[ulid.ULID]*bucketIndexReader,
	shardSelector *sharding.ShardSelector,
	matchers []*labels.Matcher,
	chunksLimiter ChunksLimiter, // Rate limiter for loading chunks.
	seriesLimiter SeriesLimiter, // Rate limiter for loading series.
	stats *safeQueryStats,
	strategy seriesIteratorStrategy,
	streamingIterators *streamingSeriesIterators,
) (iterator[seriesChunkRefsSet], error) {
	var (
		mtx                      = sync.Mutex{}
		batches                  = make([]iterator[seriesChunkRefsSet], 0, len(blocks))
		g, _                     = errgroup.WithContext(ctx)
		begin                    = time.Now()
		blocksQueriedByBlockMeta = make(map[blockQueriedMeta]int)
	)
	for _, b := range blocks {
		// Keep track of queried blocks.
		indexr := indexReaders[b.meta.ULID]

		// If query sharding is enabled we have to get the block-specific series hash cache
		// which is used by blockSeriesSkippingChunks().
		var blockSeriesHashCache *hashcache.BlockSeriesHashCache
		if shardSelector != nil {
			blockSeriesHashCache = s.seriesHashCache.GetBlockCache(b.meta.ULID.String())
		}
		g.Go(func() error {
			part, err := openBlockSeriesChunkRefsSetsIterator(
				ctx,
				s.maxSeriesPerBatch,
				s.userID,
				indexr,
				s.indexCache,
				b.meta,
				matchers,
				shardSelector,
				cachedSeriesHasher{blockSeriesHashCache},
				strategy,
				req.MinTime, req.MaxTime,
				stats,
				s.logger,
				streamingIterators,
			)
			if err != nil {
				return errors.Wrapf(err, "fetch series for block %s", b.meta.ULID)
			}

			mtx.Lock()
			batches = append(batches, part)
			mtx.Unlock()

			return nil
		})

		blocksQueriedByBlockMeta[newBlockQueriedMeta(b.meta)]++
	}

	err := g.Wait()
	if err != nil {
		return nil, err
	}

	stats.update(func(stats *queryStats) {
		stats.blocksQueried = len(batches)
		for sl, count := range blocksQueriedByBlockMeta {
			stats.blocksQueriedByBlockMeta[sl] = count
		}
		stats.streamingSeriesExpandPostingsDuration += time.Since(begin)
	})

	return s.getSeriesIteratorFromPerBlockIterators(batches, chunksLimiter, seriesLimiter), nil
}

func (s *BucketStore) getSeriesIteratorFromPerBlockIterators(perBlockIterators []iterator[seriesChunkRefsSet], chunksLimiter ChunksLimiter, seriesLimiter SeriesLimiter) iterator[seriesChunkRefsSet] {
	mergedIterator := mergedSeriesChunkRefsSetIterators(s.maxSeriesPerBatch, perBlockIterators...)

	// Apply limits after the merging, so that if the same series is part of multiple blocks it just gets
	// counted once towards the limit.
	mergedIterator = newLimitingSeriesChunkRefsSetIterator(mergedIterator, chunksLimiter, seriesLimiter)

	return mergedIterator
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
	s.metrics.streamingSeriesRequestDurationByStage.WithLabelValues("wait_max_concurrent").Observe(stats.streamingSeriesConcurrencyLimitWaitDuration.Seconds())

	s.metrics.seriesDataFetched.WithLabelValues("chunks", "fetched").Observe(float64(stats.chunksFetched))
	s.metrics.seriesDataSizeFetched.WithLabelValues("chunks", "fetched").Observe(float64(stats.chunksFetchedSizeSum))

	s.metrics.seriesDataFetched.WithLabelValues("chunks", "refetched").Observe(float64(stats.chunksRefetched))
	s.metrics.seriesDataSizeFetched.WithLabelValues("chunks", "refetched").Observe(float64(stats.chunksRefetchedSizeSum))

	for m, count := range stats.blocksQueriedByBlockMeta {
		s.metrics.seriesBlocksQueried.WithLabelValues(string(m.source), strconv.Itoa(m.level), strconv.FormatBool(m.outOfOrder)).Observe(float64(count))
	}

	s.metrics.seriesDataTouched.WithLabelValues("chunks", "processed").Observe(float64(stats.chunksTouched))
	s.metrics.seriesDataSizeTouched.WithLabelValues("chunks", "processed").Observe(float64(stats.chunksTouchedSizeSum))
	// For the implementation which uses the caching bucket the bytes we touch are the bytes we return.
	s.metrics.seriesDataTouched.WithLabelValues("chunks", "returned").Observe(float64(stats.chunksTouched))
	s.metrics.seriesDataSizeTouched.WithLabelValues("chunks", "returned").Observe(float64(stats.chunksTouchedSizeSum))

	s.metrics.resultSeriesCount.Observe(float64(stats.mergedSeriesCount))
}

func (s *BucketStore) recordLabelNamesCallResult(safeStats *safeQueryStats) {
	stats := safeStats.export()
	s.recordPostingsStats(stats)
	s.recordSeriesStats(stats)
	s.recordCachedPostingStats(stats)
	s.recordSeriesHashCacheStats(stats)
	s.recordStreamingSeriesStats(stats)

	for m, count := range stats.blocksQueriedByBlockMeta {
		s.metrics.seriesBlocksQueried.WithLabelValues(string(m.source), strconv.Itoa(m.level), strconv.FormatBool(m.outOfOrder)).Observe(float64(count))
	}
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

	s.metrics.streamingSeriesRequestDurationByStage.WithLabelValues("expand_postings").Observe(stats.streamingSeriesExpandPostingsDuration.Seconds())
	s.metrics.streamingSeriesRequestDurationByStage.WithLabelValues("fetch_series_and_chunks").Observe(stats.streamingSeriesBatchLoadDuration.Seconds())
	s.metrics.streamingSeriesRequestDurationByStage.WithLabelValues("load_index_header").Observe(stats.streamingSeriesIndexHeaderLoadDuration.Seconds())

	categorizedTime := stats.streamingSeriesExpandPostingsDuration +
		stats.streamingSeriesBatchLoadDuration +
		stats.streamingSeriesIndexHeaderLoadDuration +
		stats.streamingSeriesConcurrencyLimitWaitDuration +
		stats.streamingSeriesEncodeResponseDuration +
		stats.streamingSeriesSendResponseDuration

	// "other" time is any time we have spent according to the wall clock,
	// that hasn't been recorded in any of the known categories.
	s.metrics.streamingSeriesRequestDurationByStage.WithLabelValues("other").Observe((stats.streamingSeriesAmbientTime - categorizedTime).Seconds())
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
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "bucket_store_open_blocks_for_reading")
	defer span.Finish()

	var (
		blocks       []*bucketBlock
		indexReaders map[ulid.ULID]*bucketIndexReader
		chunkReaders map[ulid.ULID]chunkReader
	)

	// Find all blocks owned by this store-gateway instance and matching the request.
	s.blockSet.filter(minT, maxT, blockMatchers, func(b *bucketBlock) {
		blocks = append(blocks, b)

		// Unlike below, ensureIndexHeaderLoaded() does not retain the context after it returns.
		b.ensureIndexHeaderLoaded(spanCtx, stats)

		if indexReaders == nil {
			indexReaders = make(map[ulid.ULID]*bucketIndexReader)
		}
		indexReaders[b.meta.ULID] = b.indexReader(s.postingsStrategy)

		if skipChunks {
			return
		}

		if chunkReaders == nil {
			chunkReaders = make(map[ulid.ULID]chunkReader)
		}
		// Ignore the span context from this method; chunkReader() retains the context to add spans after openBlocksForReading() returns.
		chunkReaders[b.meta.ULID] = b.chunkReader(ctx)
	})

	return blocks, indexReaders, chunkReaders
}

// LabelNames implements the storegatewaypb.StoreGatewayServer interface.
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
	defer s.recordRequestAmbientTime(stats, time.Now())

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

	var setsMtx sync.Mutex
	var sets [][]string
	var blocksQueriedByBlockMeta = make(map[blockQueriedMeta]int)
	seriesLimiter := s.seriesLimiterFactory(s.metrics.queriesDropped.WithLabelValues("series"))

	s.blockSet.filter(req.Start, req.End, reqBlockMatchers, func(b *bucketBlock) {
		resHints.AddQueriedBlock(b.meta.ULID)
		blocksQueriedByBlockMeta[newBlockQueriedMeta(b.meta)]++

		// This indexReader is here to make sure its block is held open inside the goroutine below.
		indexr := b.indexReader(s.postingsStrategy)

		g.Go(func() error {
			defer runutil.CloseWithLogOnErr(s.logger, indexr, "label names")

			b.ensureIndexHeaderLoaded(gctx, stats)

			result, err := blockLabelNames(gctx, indexr, reqSeriesMatchers, seriesLimiter, s.maxSeriesPerBatch, s.logger, stats)
			if err != nil {
				return errors.Wrapf(err, "block %s", b.meta.ULID)
			}

			if len(result) > 0 {
				setsMtx.Lock()
				sets = append(sets, result)
				setsMtx.Unlock()
			}

			return nil
		})
	})

	if err := g.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, status.Error(codes.Canceled, err.Error())
		}

		return nil, status.Error(codes.Internal, err.Error())
	}

	stats.update(func(stats *queryStats) {
		stats.blocksQueried = len(sets)
		for sl, count := range blocksQueriedByBlockMeta {
			stats.blocksQueriedByBlockMeta[sl] = count
		}
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
		names, err := indexr.block.indexHeaderReader.LabelNames(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "label names")
		}
		storeCachedLabelNames(ctx, indexr.block.indexCache, indexr.block.userID, indexr.block.meta.ULID, matchers, names, logger)
		return names, nil
	}

	// We ignore request's min/max time and query the entire block to make the result cacheable.
	minTime, maxTime := indexr.block.meta.MinTime, indexr.block.meta.MaxTime
	seriesSetsIterator, err := openBlockSeriesChunkRefsSetsIterator(
		ctx,
		seriesPerBatch,
		indexr.block.userID,
		indexr,
		indexr.block.indexCache,
		indexr.block.meta,
		matchers,
		nil,
		cachedSeriesHasher{nil},
		noChunkRefs,
		minTime, maxTime,
		stats,
		logger,
		nil,
	)
	if err != nil {
		return nil, errors.Wrap(err, "fetch series")
	}
	seriesSetsIterator = newLimitingSeriesChunkRefsSetIterator(seriesSetsIterator, NewLimiter(0, nil, nil), seriesLimiter)
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
		spanlogger.FromContext(ctx, logger).DebugLog("msg", "cached label names entry key doesn't match, possible collision", "cached_key", entry.MatchersKey, "requested_key", matchersKey)
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

// LabelValues implements the storegatewaypb.StoreGatewayServer interface.
func (s *BucketStore) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	reqSeriesMatchers, err := storepb.MatchersToPromMatchers(req.Matchers...)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request labels matchers").Error())
	}

	stats := newSafeQueryStats()
	defer s.recordLabelValuesCallResult(stats)
	defer s.recordRequestAmbientTime(stats, time.Now())

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

	var setsMtx sync.Mutex
	var sets [][]string
	s.blockSet.filter(req.Start, req.End, reqBlockMatchers, func(b *bucketBlock) {
		resHints.AddQueriedBlock(b.meta.ULID)

		// This index reader shouldn't be used for ExpandedPostings, since it doesn't have the correct strategy.
		// It's here only to make sure the block is held open inside the goroutine below.
		indexr := b.indexReader(nil)

		g.Go(func() error {
			defer runutil.CloseWithLogOnErr(b.logger, indexr, "close block index reader")

			b.ensureIndexHeaderLoaded(ctx, stats)

			result, err := blockLabelValues(gctx, b, s.postingsStrategy, s.maxSeriesPerBatch, req.Label, reqSeriesMatchers, s.logger, stats)
			if err != nil {
				return errors.Wrapf(err, "block %s", b.meta.ULID)
			}

			if len(result) > 0 {
				setsMtx.Lock()
				sets = append(sets, result)
				setsMtx.Unlock()
			}

			return nil
		})
	})

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
	values, ok := fetchCachedLabelValues(ctx, b.indexCache, b.userID, b.meta.ULID, labelName, matchers, logger)
	if ok {
		return values, nil
	}

	// TODO: if matchers contains labelName, we could use it to filter out label values here.
	allValuesPostingOffsets, err := b.indexHeaderReader.LabelValuesOffsets(ctx, labelName, "", nil)
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
	var iterator iterator[seriesChunkRefsSet]
	iterator = newLoadingSeriesChunkRefsSetIterator(
		ctx,
		newPostingsSetsIterator(matchersPostings, seriesPerBatch),
		indexr,
		b.indexCache,
		stats,
		b.meta,
		nil,
		nil,
		noChunkRefs,
		b.meta.MinTime,
		b.meta.MaxTime,
		b.userID,
		b.logger,
	)
	if len(pendingMatchers) > 0 {
		iterator = newFilteringSeriesChunkRefsSetIterator(pendingMatchers, iterator, stats)
	}
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
		spanlogger.FromContext(ctx, logger).DebugLog("msg", "cached label values entry label name doesn't match, possible collision", "cached_label_name", entry.LabelName, "requested_label_name", labelName)
		return nil, false
	}
	if entry.MatchersKey != matchersKey {
		spanlogger.FromContext(ctx, logger).DebugLog("msg", "cached label values entry key doesn't match, possible collision", "cached_key", entry.MatchersKey, "requested_key", matchersKey)
		return nil, false
	}

	return entry.Values, true
}

func storeCachedLabelValues(ctx context.Context, indexCache indexcache.IndexCache, userID string, blockID ulid.ULID, labelName string, matchers []*labels.Matcher, values []string, logger log.Logger) {
	// This limit is a workaround for panics in decoding large responses. See https://github.com/golang/go/issues/59172
	const valuesLimit = 655360
	if len(values) > valuesLimit {
		spanlogger.FromContext(ctx, logger).DebugLog("msg", "skipping storing label values response to cache because it exceeds number of values limit", "limit", valuesLimit, "values_count", len(values))
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
	// mtx protects the below data strcutures, helping to keep them in sync.
	mtx      sync.RWMutex
	blockSet sync.Map       // Maps block's ulid.ULID to the *bucketBlock.
	blocks   []*bucketBlock // Blocks sorted by mint, then maxt.
}

// newBucketBlockSet initializes a new set with the known downsampling windows hard-configured.
// (Mimir only supports no-downsampling)
// The set currently does not support arbitrary ranges.
func newBucketBlockSet() *bucketBlockSet {
	return &bucketBlockSet{}
}

// add adds a block to the set.
func (s *bucketBlockSet) add(b *bucketBlock) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	// The LoadOrStore verifies the block with the same id never ended up in the set more than once.
	_, ok := s.blockSet.LoadOrStore(b.meta.ULID, b)
	if ok {
		// This should not ever happen.
		return fmt.Errorf("block %s already exists in the set", b.meta.ULID)
	}

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

// remove removes the block identified by id from the set. It returns the removed block if it was present in the set.
func (s *bucketBlockSet) remove(id ulid.ULID) *bucketBlock {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	val, ok := s.blockSet.LoadAndDelete(id)
	if !ok {
		return nil
	}

	for i, b := range s.blocks {
		if b.meta.ULID != id {
			continue
		}
		s.blocks = append(s.blocks[:i], s.blocks[i+1:]...)
		break
	}

	return val.(*bucketBlock)
}

func (s *bucketBlockSet) contains(id ulid.ULID) bool {
	_, ok := s.blockSet.Load(id)
	return ok
}

func (s *bucketBlockSet) len() int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return len(s.blocks)
}

// filter iterates over a time-ordered list of non-closed blocks that cover date between mint and maxt. It supports overlapping
// blocks. It only guaranties that a block is held open during the execution of fn.
func (s *bucketBlockSet) filter(mint, maxt int64, blockMatchers []*labels.Matcher, fn func(b *bucketBlock)) {
	if mint > maxt {
		return
	}

	s.mtx.RLock()

	// Fill the given interval with the blocks within the request mint and maxt.
	bs := make([]*bucketBlock, 0, len(s.blocks))
	for _, b := range s.blocks {
		// NOTE: s.blocks are expected to be sorted in minTime order, their intervals are half-open: [b.MinTime, b.MaxTime).
		if b.meta.MinTime > maxt {
			break
		}

		if b.overlapsClosedInterval(mint, maxt) {
			// Include the block in the list of matching ones only if there are no block-level matchers
			// or they actually match.
			if len(blockMatchers) == 0 || b.matchLabels(blockMatchers) {
				bs = append(bs, b)
			}
		}
	}

	s.mtx.RUnlock()

	step := func(b *bucketBlock) {
		b.closedMtx.RLock()
		defer b.closedMtx.RUnlock()
		if !b.closed {
			fn(b)
		}
	}

	for _, b := range bs {
		step(b)
	}
}

// forEach iterates over all non-closed blocks in the set. It only guaranties that a block is held open during the execution of fn.
func (s *bucketBlockSet) forEach(fn func(b *bucketBlock)) {
	s.blockSet.Range(func(_, val any) bool {
		b := val.(*bucketBlock)

		b.closedMtx.RLock()
		defer b.closedMtx.RUnlock()

		if !b.closed {
			fn(b)
		}
		return true
	})
}

// closeAll closes all blocks in the set and returns all encountered errors after trying all blocks.
func (s *bucketBlockSet) closeAll() error {
	errs := multierror.New()
	s.blockSet.Range(func(_, val any) bool {
		errs.Add(val.(*bucketBlock).Close())
		return true
	})
	return errs.Err()
}

// openBlocksULIDs returns the ULIDs of all blocks in the set which are not closed.
func (s *bucketBlockSet) openBlocksULIDs() []ulid.ULID {
	ulids := make([]ulid.ULID, 0, s.len())
	s.forEach(func(b *bucketBlock) {
		ulids = append(ulids, b.meta.ULID)
	})
	return ulids
}

// allBlockULIDs returns the ULIDs of all blocks in the set regardless whether they are closed or not.
func (s *bucketBlockSet) allBlockULIDs() []ulid.ULID {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	ulids := make([]ulid.ULID, 0, len(s.blocks))
	for _, b := range s.blocks {
		ulids = append(ulids, b.meta.ULID)
	}
	return ulids

}

// timerange returns the minimum and maximum timestamp available in the set.
func (s *bucketBlockSet) timerange() (mint, maxt int64) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	if len(s.blocks) == 0 {
		return math.MaxInt64, math.MinInt64
	}

	mint = math.MaxInt64
	maxt = math.MinInt64

	// NOTE: s.blocks are expected to be sorted in minTime order.
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
	pendingReaders    sync.WaitGroup
	closedMtx         sync.RWMutex
	closed            bool

	chunkObjs []string

	partitioners blockPartitioners

	// Block's labels used by block-level matchers to filter blocks to query. These are used to select blocks using
	// request hints' BlockMatchers.
	blockLabels labels.Labels

	expandedPostingsPromises sync.Map

	// Indicates whether the block was queried.
	queried atomic.Bool
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

// ensureIndexHeaderLoaded lazy-loads block's index header and record the loading time.
func (b *bucketBlock) ensureIndexHeaderLoaded(ctx context.Context, stats *safeQueryStats) {
	span, _ := opentracing.StartSpanFromContext(ctx, "bucketBlock.ensureIndexHeaderLoaded")
	defer span.Finish()
	span.SetTag("blockID", b.meta.ULID)

	loadStartTime := time.Now()
	// Call IndexVersion to lazy load the index header if it lazy-loaded.
	_, _ = b.indexHeaderReader.IndexVersion(ctx)
	stats.update(func(stats *queryStats) {
		stats.streamingSeriesIndexHeaderLoadDuration += time.Since(loadStartTime)
	})
}

// Close waits for all pending readers to finish and then closes all underlying resources.
func (b *bucketBlock) Close() error {
	b.closedMtx.Lock()
	b.closed = true
	b.closedMtx.Unlock()

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
// If skipChunks is specified decodeSeries does not return any chunks, but only labels and only if there is at least a single chunk.
// decodeSeries returns false, when there are no chunks for the series.
func decodeSeries(b []byte, lsetPool *pool.SlabPool[symbolizedLabel], chks *[]chunks.Meta, skipChunks bool) (ok bool, lset []symbolizedLabel, err error) {

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
			// We are not interested in chunks and we know there is at least one, that's enough to return series.
			return true, lset, nil
		}

		*chks = append(*chks, chunks.Meta{
			Ref:     chunks.ChunkRef(ref),
			MinTime: mint,
			MaxTime: maxt,
		})

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
