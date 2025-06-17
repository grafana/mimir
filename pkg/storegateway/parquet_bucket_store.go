// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/types"
	"github.com/grafana/dskit/gate"
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/dskit/runutil"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway/hintspb"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// ParquetBucketStore implements the store API backed by a bucket.
// It loads all Parquet block labels files to local disk.
type ParquetBucketStore struct {
	services.Service

	logger log.Logger

	userID        string
	localDir      string
	bucketMetrics *BucketStoreMetrics // TODO: Create ParquetBucketStoreMetrics
	bkt           objstore.InstrumentedBucketReader
	fetcher       block.MetadataFetcher

	// Set of blocks that have the same labels
	blockSet *parquetBlockSet

	// Number of goroutines to use when syncing blocks from object storage.
	blockSyncConcurrency int

	// Query gate which limits the maximum amount of concurrent queries.
	queryGate gate.Gate

	// Gate used to limit concurrency on loading index-headers across all tenants.
	lazyLoadingGate gate.Gate

	// chunksLimiterFactory creates a new limiter used to limit the number of chunks fetched by each Series() call.
	chunksLimiterFactory ChunksLimiterFactory
	// seriesLimiterFactory creates a new limiter used to limit the number of touched series by each Series() call,
	// or LabelName and LabelValues calls when used with matchers.
	seriesLimiterFactory SeriesLimiterFactory

	// maxSeriesPerBatch controls the batch size to use when fetching series.
	// This is not restricted to the Series() RPC.
	// This value must be greater than zero.
	maxSeriesPerBatch int
}

// NewParquetBucketStore creates a new bucket backed store that implements the store API against
// an object store bucket. It is optimized to work against high latency backends.
func NewParquetBucketStore(
	userID string,
	localDir string,
	bkt objstore.InstrumentedBucketReader,
	bucketStoreConfig tsdb.BucketStoreConfig,
	blockMetaFetcher block.MetadataFetcher,
	queryGate gate.Gate,
	lazyLoadingGate gate.Gate,
	chunksLimiterFactory ChunksLimiterFactory,
	seriesLimiterFactory SeriesLimiterFactory,
	metrics *BucketStoreMetrics,
	logger log.Logger,
) (*ParquetBucketStore, error) {
	s := &ParquetBucketStore{
		logger: logger,

		userID:   userID,
		localDir: localDir,

		bucketMetrics: metrics,
		bkt:           bkt,
		fetcher:       blockMetaFetcher,

		blockSet:             &parquetBlockSet{},
		blockSyncConcurrency: bucketStoreConfig.BlockSyncConcurrency,

		queryGate:       queryGate,
		lazyLoadingGate: lazyLoadingGate,

		chunksLimiterFactory: chunksLimiterFactory,
		seriesLimiterFactory: seriesLimiterFactory,
		maxSeriesPerBatch:    bucketStoreConfig.StreamingBatchSize,
	}

	if err := os.MkdirAll(localDir, 0750); err != nil {
		return nil, errors.Wrap(err, "create local localDir")
	}

	s.Service = services.NewIdleService(s.start, s.stop)
	return s, nil
}

func (s *ParquetBucketStore) start(_ context.Context) error {
	// Use context.Background() so that we stop the index reader pool ourselves and do it after closing all blocks.
	return services.StartAndAwaitRunning(context.Background(), nil)
}

func (s *ParquetBucketStore) stop(err error) error {
	errs := multierror.New(err)
	errs.Add(s.closeAllBlocks())
	// The snapshotter depends on the reader pool, so we close the snapshotter first.
	errs.Add(services.StopAndAwaitTerminated(context.Background(), nil)) // TODO insert snapshotter
	errs.Add(services.StopAndAwaitTerminated(context.Background(), nil)) // TODO insert index reader pool
	return errs.Err()
}

func (s *ParquetBucketStore) Series(req *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer) (err error) {
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

	shards := s.openParquetShardsForReading(ctx, req.SkipChunks, req.MinTime, req.MaxTime, reqBlockMatchers, stats)
	// We must keep the readers open until all their data has been sent.
	for _, shard := range shards {
		defer runutil.CloseWithLogOnErr(s.logger, shard, "close block shard")
	}

	// Wait for the query gate only after opening blocks. Opening blocks is usually fast (~1ms),
	// but sometimes it can take minutes if the block isn't loaded and there is a surge in queries for unloaded blocks.
	done, err := s.limitConcurrentQueries(ctx, stats)
	if err != nil {
		return err
	}
	defer done()

	var (
		resHints = &hintspb.SeriesResponseHints{}
	)
	for _, shard := range shards {
		resHints.AddQueriedBlock(shard.BlockID)
		shard.MarkQueried()
	}
	if err := s.sendHints(srv, resHints); err != nil {
		return err
	}

	streamingSeriesCount := 0
	if req.StreamingChunksBatchSize > 0 {
		var (
			seriesSet       storepb.SeriesSet
			seriesLoadStart = time.Now()
			chunksLimiter   = s.chunksLimiterFactory(s.bucketMetrics.queriesDropped.WithLabelValues("chunks"))
			seriesLimiter   = s.seriesLimiterFactory(s.bucketMetrics.queriesDropped.WithLabelValues("series"))
		)

		// Placeholder: Create series set for streaming labels from parquet shards
		seriesSet, err = s.createParquetSeriesSetForLabels(ctx, req, shards, shardSelector, matchers, chunksLimiter, seriesLimiter, stats)
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
	chunksLimiter := s.chunksLimiterFactory(s.bucketMetrics.queriesDropped.WithLabelValues("chunks"))
	seriesLimiter := s.seriesLimiterFactory(s.bucketMetrics.queriesDropped.WithLabelValues("series"))

	start := time.Now()
	if req.StreamingChunksBatchSize > 0 {
		seriesChunkIt := s.createParquetSeriesChunksSetIterator(ctx, req, shards, shardSelector, matchers, chunksLimiter, seriesLimiter, stats)
		err = s.sendStreamingChunks(req, srv, seriesChunkIt, stats, streamingSeriesCount)
	} else {
		var seriesSet storepb.SeriesSet
		seriesSet, err = s.createParquetSeriesSetWithChunks(ctx, req, shards, shardSelector, matchers, chunksLimiter, seriesLimiter, stats)
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

func (s *ParquetBucketStore) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	// TODO implement me
	panic("implement me")
}

func (s *ParquetBucketStore) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	// TODO implement me
	panic("implement me")
}

// Placeholder methods for parquet-specific functionality
func (s *ParquetBucketStore) openParquetShardsForReading(ctx context.Context, skipChunks bool, minTime, maxTime int64, reqBlockMatchers []*labels.Matcher, stats *safeQueryStats) []*parquetBucketBlock {
	// TODO: Implement parquet shard discovery and opening logic
	// This should:
	// 1. Discover parquet shards that intersect with the time range
	// 2. Use storage.ParquetShardOpener to open .labels.parquet and .chunks.parquet files
	// 3. Read parquet schemas and metadata for efficient querying using shard.TSDBSchema()
	// 4. Wrap opened ParquetShard with metadata (BlockID, queried status)
	panic("TODO: implement openParquetShardsForReading")
}

func (s *ParquetBucketStore) limitConcurrentQueries(ctx context.Context, stats *safeQueryStats) (func(), error) {
	// TODO: Can potentially reuse BucketStore.limitConcurrentQueries
	// or implement parquet-specific version if needed
	panic("TODO: implement limitConcurrentQueries")
}

func (s *ParquetBucketStore) sendHints(srv storegatewaypb.StoreGateway_SeriesServer, resHints *hintspb.SeriesResponseHints) error {
	// TODO: Implement hints sending for parquet stores
	panic("TODO: implement sendHints")
}

func (s *ParquetBucketStore) createParquetSeriesSetForLabels(ctx context.Context, req *storepb.SeriesRequest, shards []*parquetBucketBlock, shardSelector *sharding.ShardSelector, matchers []*labels.Matcher, chunksLimiter ChunksLimiter, seriesLimiter SeriesLimiter, stats *safeQueryStats) (storepb.SeriesSet, error) {
	// TODO: Implement parquet series set creation for labels phase
	// This should:
	// 1. "Stream read" .labels.parquet files from shards using shard.LabelsFile()
	// 2. Create and return storepb.SeriesSet that iterates over series labels without chunks
	// Please note that storepb.SeriesSet assumes series are ordered.
	panic("TODO: implement createParquetSeriesSetForLabels")
}

func (s *ParquetBucketStore) sendStreamingSeriesLabelsAndStats(req *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer, stats *safeQueryStats, seriesSet storepb.SeriesSet) (int, error) {
	// TODO: Can potentially reuse BucketStore.sendStreamingSeriesLabelsAndStats
	// or implement parquet-specific version if needed
	panic("TODO: implement sendStreamingSeriesLabelsAndStats")
}

func (s *ParquetBucketStore) sendStreamingChunks(req *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer, seriesChunkIt iterator[seriesChunksSet], stats *safeQueryStats, streamingSeriesCount int) error {
	// TODO: Can potentially reuse BucketStore.sendStreamingChunks
	// or implement parquet-specific version if needed
	panic("TODO: implement sendStreamingChunks")
}

func (s *ParquetBucketStore) createParquetSeriesChunksSetIterator(ctx context.Context, req *storepb.SeriesRequest, shards []*parquetBucketBlock, shardSelector *sharding.ShardSelector, matchers []*labels.Matcher, chunksLimiter ChunksLimiter, seriesLimiter SeriesLimiter, stats *safeQueryStats) iterator[seriesChunksSet] {
	// TODO: Implement parquet series chunks iterator creation
	// This should:
	// 1. Stream read .chunks.parquet files from shards using shard.ChunksFile()
	// 2. Return iterator[seriesChunksSet] / or the new iterator Nico is workisng on in his PR that streams chunks for the series discovered in labels phase
	panic("TODO: implement createParquetSeriesChunksSetIterator")
}

func (s *ParquetBucketStore) sendSeriesChunks(req *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer, seriesSet storepb.SeriesSet, stats *safeQueryStats) error {
	// TODO: Can potentially reuse BucketStore.sendSeriesChunks
	// or implement parquet-specific version if needed
	panic("TODO: implement sendSeriesChunks")
}

func (s *ParquetBucketStore) createParquetSeriesSetWithChunks(ctx context.Context, req *storepb.SeriesRequest, shards []*parquetBucketBlock, shardSelector *sharding.ShardSelector, matchers []*labels.Matcher, chunksLimiter ChunksLimiter, seriesLimiter SeriesLimiter, stats *safeQueryStats) (storepb.SeriesSet, error) {
	// TODO: Implement parquet series set creation for non-streaming request
	// I think this should create a series that includes the labels in one go and its typically called when skipchunks is true
	panic("TODO: implement createParquetSeriesSetWithChunks")
}

func (s *ParquetBucketStore) recordSeriesCallResult(stats *safeQueryStats) {
	// TODO: Implement series call result recording for parquet stores
	panic("TODO: implement recordSeriesCallResult")
}

func (s *ParquetBucketStore) recordRequestAmbientTime(stats *safeQueryStats, startTime time.Time) {
	// TODO: Implement request ambient time recording for parquet stores
	panic("TODO: implement recordRequestAmbientTime")
}

func (s *ParquetBucketStore) sendStats(srv storegatewaypb.StoreGateway_SeriesServer, stats *safeQueryStats) error {
	// TODO: Implement stats sending for parquet stores
	panic("TODO: implement sendStats")
}

func (s *ParquetBucketStore) SyncBlocks(ctx context.Context) error {
	return s.syncBlocks(ctx)
}

func (s *ParquetBucketStore) syncBlocks(ctx context.Context) error {
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
	//_ = s.snapshotter.StartAsync(context.Background())

	return nil
}

func (s *ParquetBucketStore) addBlock(ctx context.Context, meta *block.Meta) (err error) {
	dir := filepath.Join(s.localDir, meta.ULID.String())
	start := time.Now()

	level.Debug(s.logger).Log("msg", "loading new block", "id", meta.ULID)
	defer func() {
		if err != nil {
			s.bucketMetrics.blockLoadFailures.Inc()
			if err2 := os.RemoveAll(dir); err2 != nil {
				level.Warn(s.logger).Log("msg", "failed to remove block we cannot load", "err", err2)
			}
			level.Error(s.logger).Log("msg", "loading block failed", "elapsed", time.Since(start), "id", meta.ULID, "err", err)
		} else {
			level.Info(s.logger).Log("msg", "loaded new block", "elapsed", time.Since(start), "id", meta.ULID)
		}
	}()
	s.bucketMetrics.blockLoads.Inc()

	indexHeaderReader, err := s.indexReaderPool.NewBinaryReader(
		ctx,
		s.logger,
		s.bkt,
		s.localDir,
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
		s.bucketMetrics,
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

func (s *ParquetBucketStore) closeAllBlocks() error {
	return s.blockSet.closeAll()
}
