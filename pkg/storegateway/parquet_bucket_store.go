// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"context"
	"fmt"
	"os"
	"path"
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
	"github.com/oklog/ulid/v2"
	parquetGo "github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus-community/parquet-common/queryable"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/search"
	parquetStorage "github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/parquetconverter"
	"github.com/grafana/mimir/pkg/storage/parquet"
	parquetBlock "github.com/grafana/mimir/pkg/storage/parquet/block"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway/hintspb"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// ParquetBucketStore implements the store API backed by a bucket.
// It loads all Parquet block currLabels files to local disk.
type ParquetBucketStore struct {
	services.Service

	logger log.Logger

	userID string

	bkt        objstore.InstrumentedBucketReader
	fetcher    block.MetadataFetcher
	localDir   string
	readerPool *parquetBlock.ReaderPool

	// Metrics specific to bkt store operations
	metrics *ParquetBucketStoreMetrics

	// Set of blocks that have the same currLabels
	blockSet *parquetBlockSet

	// Number of goroutines to use when syncing blocks from object storage.
	blockSyncConcurrency int

	// Query gate which limits the maximum amount of concurrent queries.
	queryGate gate.Gate

	// Gate used to limit concurrency on loading index-headers across all tenants.
	lazyLoadingGate gate.Gate
	loadIndexToDisk bool
	fileOpts        []parquetStorage.FileOption

	// chunksLimiterFactory creates a new limiter used to limit the number of chunks fetched by each Series() call.
	chunksLimiterFactory ChunksLimiterFactory
	// seriesLimiterFactory creates a new limiter used to limit the number of touched series by each Series() call,
	// or LabelName and LabelValues calls when used with matchers.
	seriesLimiterFactory SeriesLimiterFactory

	// maxSeriesPerBatch controls the batch size to use when fetching series.
	// This is not restricted to the Series() RPC.
	// This value must be greater than zero.
	maxSeriesPerBatch int

	// querierOpts holds the options for the parquet querier. These opts are passed down to the Materializer which is
	// the one that has information about the row count, chunk size and data size limits.
	querierOpts []parquet.QuerierOpts
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
	loadIndexToDisk bool,
	fileOpts []parquetStorage.FileOption,
	chunksLimiterFactory ChunksLimiterFactory,
	seriesLimiterFactory SeriesLimiterFactory,
	metrics *ParquetBucketStoreMetrics,
	logger log.Logger,
) (*ParquetBucketStore, error) {
	s := &ParquetBucketStore{
		logger: logger,

		userID:   userID,
		localDir: localDir,

		metrics: metrics,
		bkt:     bkt,
		fetcher: blockMetaFetcher,

		blockSet:             &parquetBlockSet{},
		blockSyncConcurrency: bucketStoreConfig.BlockSyncConcurrency,

		queryGate:       queryGate,
		lazyLoadingGate: lazyLoadingGate,
		loadIndexToDisk: loadIndexToDisk,
		fileOpts: append(fileOpts,
			parquetStorage.WithFileOptions(parquetGo.SkipBloomFilters(false)),
		),

		chunksLimiterFactory: chunksLimiterFactory,
		seriesLimiterFactory: seriesLimiterFactory,
		maxSeriesPerBatch:    bucketStoreConfig.StreamingBatchSize,
	}

	var querierOpts []parquet.QuerierOpts
	if bucketStoreConfig.ParquetMaxRowCount > 0 {
		querierOpts = append(querierOpts, parquet.WithRowCountLimitFunc(func(ctx context.Context) int64 {
			return int64(bucketStoreConfig.ParquetMaxRowCount)
		}))
	}
	if bucketStoreConfig.ParquetMaxChunkSizeBytes > 0 {
		querierOpts = append(querierOpts, parquet.WithChunkBytesLimitFunc(func(ctx context.Context) int64 {
			return int64(bucketStoreConfig.ParquetMaxChunkSizeBytes)
		}))
	}
	if bucketStoreConfig.ParquetMaxDataSizeBytes > 0 {
		querierOpts = append(querierOpts, parquet.WithDataBytesLimitFunc(func(ctx context.Context) int64 {
			return int64(bucketStoreConfig.ParquetMaxDataSizeBytes)
		}))
	}
	s.querierOpts = querierOpts

	s.readerPool = parquetBlock.NewReaderPool(
		bucketStoreConfig.IndexHeader,
		s.lazyLoadingGate,
		logger,
		s.metrics.indexHeaderReaderMetrics,
	)

	if err := os.MkdirAll(localDir, 0750); err != nil {
		return nil, errors.Wrap(err, "create local localDir")
	}

	s.Service = services.NewIdleService(s.start, s.stop)
	return s, nil
}

func (s *ParquetBucketStore) start(_ context.Context) error {
	// Use context.Background() so that we stop the index reader pool ourselves and do it after closing all blocks.
	return services.StartAndAwaitRunning(context.Background(), s.readerPool)
}

func (s *ParquetBucketStore) stop(err error) error {
	errs := multierror.New(err)
	errs.Add(s.closeAllBlocks())
	// The snapshotter depends on the reader pool, so we close the snapshotter first.
	// errs.Add(services.StopAndAwaitTerminated(context.Background(), nil)) // TODO insert snapshotter
	errs.Add(services.StopAndAwaitTerminated(context.Background(), s.readerPool))
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
			return status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request hints currLabels matchers").Error())
		}
	}

	logSeriesRequestToSpan(srv.Context(), s.logger, req.MinTime, req.MaxTime, matchers, reqBlockMatchers, shardSelector, req.StreamingChunksBatchSize)

	bucketBlocks, shardReaders := s.openParquetBlocksForReading(ctx, req.SkipChunks, req.MinTime, req.MaxTime, reqBlockMatchers, stats)
	// We must keep the readers open until all their data has been sent.
	for _, shardReader := range shardReaders {
		defer runutil.CloseWithLogOnErr(s.logger, shardReader, "close parquet block shard reader")
	}

	spanLogger.DebugLog(
		"msg", "opened parquet shards for reading",
		"blocks", len(bucketBlocks),
		"num_shards", len(shardReaders),
	)

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
	for _, bucketBlock := range bucketBlocks {
		resHints.AddQueriedBlock(bucketBlock.meta.ULID)
		bucketBlock.MarkQueried()
	}
	if err := s.sendHints(srv, resHints); err != nil {
		return err
	}

	chunksLimiter := s.chunksLimiterFactory(s.metrics.queriesDropped.WithLabelValues("chunks"))
	seriesLimiter := s.seriesLimiterFactory(s.metrics.queriesDropped.WithLabelValues("series"))

	start := time.Now()

	labelsIt, chunksIt, err := s.createLabelsAndChunksIterators(ctx, req, bucketBlocks, shardReaders, shardSelector, matchers, chunksLimiter, seriesLimiter, stats)
	if err != nil {
		return err
	}

	// Send the series back to the querier (same series set for both streaming and non-streaming)
	if req.StreamingChunksBatchSize > 0 {
		seriesLoadStart := time.Now()
		var streamingSeriesCount int
		streamingSeriesCount, err = s.sendStreamingSeriesLabelsAndStats(req, srv, stats, labelsIt)
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
		err = s.sendStreamingChunks(req, srv, chunksIt, stats, streamingSeriesCount)
	} else {
		// Non-streaming mode, send all series and chunks in one go.
		err = s.sendSeriesChunks(req, srv, labelsIt, chunksIt, stats)
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

	s.blockSet.filter(req.Start, req.End, reqBlockMatchers, func(b *parquetBucketBlock) {
		resHints.AddQueriedBlock(b.meta.ULID)
		blocksQueriedByBlockMeta[newBlockQueriedMeta(b.meta)]++

		shard := b.ShardReader()
		shardsFinder := func(ctx context.Context, mint, maxt int64) ([]parquetStorage.ParquetShard, error) {
			return []parquetStorage.ParquetShard{shard}, nil
		}

		g.Go(func() error {
			defer runutil.CloseWithLogOnErr(s.logger, shard, "close shard")

			decoder := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
			parquetQueryable, err := queryable.NewParquetQueryable(decoder, shardsFinder)
			if err != nil {
				return errors.Wrap(err, "error creating parquet queryable")
			}
			q, err := parquetQueryable.Querier(req.Start, req.End)
			if err != nil {
				return errors.Wrap(err, "error creating parquet querier")
			}
			// TODO we already have the blockLabels, ideally we could use them to avoid querying the block again.
			result, _, err := q.LabelNames(gctx, nil, reqSeriesMatchers...)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}
				return errors.Wrap(err, "error querying label names")
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

	names := util.MergeSlices(sets...)
	if req.Limit > 0 && len(names) > int(req.Limit) {
		names = names[:req.Limit]
	}

	return &storepb.LabelNamesResponse{
		Names: names,
		Hints: anyHints,
	}, nil
}

func (s *ParquetBucketStore) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	reqSeriesMatchers, err := storepb.MatchersToPromMatchers(req.Matchers...)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request labels matchers").Error())
	}

	var (
		stats    = newSafeQueryStats()
		resHints = &hintspb.LabelValuesResponseHints{}
	)

	defer s.recordLabelValuesCallResult(stats)
	defer s.recordRequestAmbientTime(stats, time.Now())

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

	g, gctx := errgroup.WithContext(ctx)

	var setsMtx sync.Mutex
	var sets [][]string
	var blocksQueriedByBlockMeta = make(map[blockQueriedMeta]int)

	s.blockSet.filter(req.Start, req.End, reqBlockMatchers, func(b *parquetBucketBlock) {
		resHints.AddQueriedBlock(b.meta.ULID)
		blocksQueriedByBlockMeta[newBlockQueriedMeta(b.meta)]++

		shard := b.ShardReader()
		shardsFinder := func(ctx context.Context, mint, maxt int64) ([]parquetStorage.ParquetShard, error) {
			return []parquetStorage.ParquetShard{shard}, nil
		}

		g.Go(func() error {
			defer runutil.CloseWithLogOnErr(s.logger, shard, "close shard")

			decoder := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
			parquetQueryable, err := queryable.NewParquetQueryable(decoder, shardsFinder)
			if err != nil {
				return errors.Wrap(err, "error creating parquet queryable")
			}
			q, err := parquetQueryable.Querier(req.Start, req.End)
			if err != nil {
				return errors.Wrap(err, "error creating parquet querier")
			}
			result, _, err := q.LabelValues(gctx, req.Label, nil, reqSeriesMatchers...)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}
				return errors.Wrap(err, "error querying label values")
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
		return nil, status.Error(codes.Unknown, errors.Wrap(err, "marshal label values response hints").Error())
	}

	values := util.MergeSlices(sets...)
	if req.Limit > 0 && len(values) > int(req.Limit) {
		values = values[:req.Limit]
	}

	return &storepb.LabelValuesResponse{
		Values: values,
		Hints:  anyHints,
	}, nil
}

// Placeholder methods for parquet-specific functionality
func (s *ParquetBucketStore) openParquetBlocksForReading(ctx context.Context, _ bool, minTime, maxTime int64, reqBlockMatchers []*labels.Matcher, stats *safeQueryStats) ([]*parquetBucketBlock, map[ulid.ULID]ParquetShardReaderCloser) {
	_, span := tracer.Start(ctx, "parquet_bucket_store_open_blocks_for_reading")
	defer span.End()

	var blocks []*parquetBucketBlock
	shardReaders := make(map[ulid.ULID]ParquetShardReaderCloser)

	s.blockSet.filter(minTime, maxTime, reqBlockMatchers, func(b *parquetBucketBlock) {
		blocks = append(blocks, b)
		shardReaders[b.meta.ULID] = b.ShardReader()

	})
	return blocks, shardReaders
}

func (s *ParquetBucketStore) limitConcurrentQueries(ctx context.Context, stats *safeQueryStats) (done func(), err error) {
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

func (s *ParquetBucketStore) sendHints(srv storegatewaypb.StoreGateway_SeriesServer, resHints *hintspb.SeriesResponseHints) error {
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

// createLabelsAndChunksIterators creates iterators for labels and chunks from
// the parquet shards for the given query. If req.SkipChunks is true, it only
// returns an iterator for labels. Otherwise, both iterators are guaranteed
// to have equal length and order, modulo any errors that may occur during
// iteration.
func (s *ParquetBucketStore) createLabelsAndChunksIterators(
	ctx context.Context,
	req *storepb.SeriesRequest,
	parquetBlocks []*parquetBucketBlock,
	shardReaders map[ulid.ULID]ParquetShardReaderCloser,
	shardSelector *sharding.ShardSelector,
	matchers []*labels.Matcher,
	chunksLimiter ChunksLimiter,
	seriesLimiter SeriesLimiter,
	stats *safeQueryStats,
) (iterator[labels.Labels], iterator[[]storepb.AggrChunk], error) {
	spanLogger := spanlogger.FromContext(ctx, s.logger)

	shardsFinder := func(ctx context.Context, mint, maxt int64) ([]parquetStorage.ParquetShard, error) {
		var parquetShards []parquetStorage.ParquetShard
		for _, shardReader := range shardReaders {
			parquetShards = append(parquetShards, shardReader)
		}
		return parquetShards, nil
	}

	decoder := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
	opts := s.querierOpts
	if shardSelector != nil {
		shardFilter := func(_ context.Context, _ *storage.SelectHints) (search.MaterializedLabelsFilter, bool) {
			return materializedLabelsShardFilter{shardSelector: shardSelector}, true
		}
		opts = append(opts, parquet.WithMaterializedLabelsFilterCallback(shardFilter))
	}

	q, err := parquet.NewParquetChunkQuerier(decoder, shardsFinder, opts...)
	if err != nil {
		return nil, nil, errors.Wrap(err, "error creating parquet queryable")
	}
	defer q.Close()

	hints := &storage.SelectHints{
		Start: req.MinTime,
		End:   req.MaxTime,
	}

	// If we're skipping chunks, use the "series" function hint to only get currLabels
	if req.SkipChunks {
		hints.Func = "series"
	}

	chunkSeriesSet := q.Select(ctx, true, hints, matchers...)
	// NOTE: we want to be able to iterate labels and chunks separately, so we
	// load everything into slices and return iterators over those slices. This
	// does defeat the purpose of streaming, but: currently, the querier does
	// not support streaming results, the iterator we get from q.Select is
	// already backed by a slice. So we are not losing as much as it may seem.
	// We are planning to implement proper streaming.
	lbls, aggrChunks, err := toLabelsAndAggChunksSlice(chunkSeriesSet, req.SkipChunks)
	if err != nil {
		return nil, nil, errors.Wrap(err, "error converting parquet series set to labels and chunks slice")
	}

	spanLogger.DebugLog(
		"msg", "createLabelsAndChunksIterators",
		"blocks", len(parquetBlocks),
		"num_shards", len(shardReaders),
		"hints", hints,
		"matchers", matchers,
		"numLabels", len(lbls),
		"numAggrChks", len(aggrChunks),
		"skipChunks", req.SkipChunks,
	)

	labelsIt := newConcreteIterator(lbls)
	if req.SkipChunks {
		return labelsIt, nil, nil
	}

	return labelsIt, newConcreteIterator(aggrChunks), nil
}

type materializedLabelsShardFilter struct {
	shardSelector *sharding.ShardSelector
}

func (f materializedLabelsShardFilter) Filter(lbls labels.Labels) bool {
	return shardOwnedUncached(f.shardSelector, lbls)
}

func (f materializedLabelsShardFilter) Close() {
}

// shardOwnedUncached checks if the given labels belong to the shard specified
// by the shard selector. As opposed to shardOwned & friends from the
// non-Parquet path, this function does not cache hashes. This is because, at
// least yet, we don't have easy access to an identifier for the series in the
// block to use as a cache key.
func shardOwnedUncached(shard *sharding.ShardSelector, lset labels.Labels) bool {
	if shard == nil {
		return true
	}

	hash := labels.StableHash(lset)
	return hash%shard.ShardCount == shard.ShardIndex
}

func (s *ParquetBucketStore) sendStreamingSeriesLabelsAndStats(req *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer, stats *safeQueryStats, labelsIt iterator[labels.Labels]) (numSeries int, err error) {
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
	for labelsIt.Next() {
		numSeries++
		var lset labels.Labels
		lset = labelsIt.At()

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
	if labelsIt.Err() != nil {
		return 0, errors.Wrap(labelsIt.Err(), "expand series set")
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

func (s *ParquetBucketStore) sendStreamingChunks(
	req *storepb.SeriesRequest,
	srv storegatewaypb.StoreGateway_SeriesServer,
	chunksIt iterator[[]storepb.AggrChunk],
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
	for chunksIt.Next() {
		chunks := chunksIt.At()

		if !haveSentEstimatedChunks {
			// TODO(npazosmendez): we don't have series batches for now, how should we adapt this?
			seriesInBatch := 1
			chunksInBatch := len(chunks)

			estimate := uint64(totalSeriesCount * chunksInBatch / seriesInBatch)
			err := s.sendMessage("streaming chunks estimate", srv, storepb.NewStreamingChunksEstimate(estimate), &encodeDuration, &sendDuration)
			if err != nil {
				return err
			}

			haveSentEstimatedChunks = true
		}

		seriesCount++
		chunksBatch.Series = chunksBatch.Series[:len(chunksBatch.Series)+1]
		lastSeries := chunksBatch.Series[len(chunksBatch.Series)-1]
		lastSeries.Chunks = chunks
		lastSeries.SeriesIndex = uint64(seriesCount - 1)

		batchSizeBytes += lastSeries.Size()

		chunksCount += len(chunks)
		s.metrics.chunkSizeBytes.Observe(float64(chunksSize(chunks)))

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

		if len(chunksBatch.Series) > 0 {
			// Still some chunks left to send before we release the batch.
			err := s.sendMessage("streaming chunks", srv, storepb.NewStreamingChunksResponse(chunksBatch), &encodeDuration, &sendDuration)
			if err != nil {
				return err
			}
			chunksBatch.Series = chunksBatch.Series[:0]
			batchSizeBytes = 0
		}

	}

	if chunksIt.Err() != nil {
		return chunksIt.Err()
	}

	// If we never sent an estimate (because there were no batches, or no batch had any series), send it now.
	if !haveSentEstimatedChunks {
		err := s.sendMessage("streaming chunks estimate", srv, storepb.NewStreamingChunksEstimate(0), &encodeDuration, &sendDuration)
		if err != nil {
			return err
		}
	}

	return chunksIt.Err()
}

func (s *ParquetBucketStore) sendSeriesChunks(req *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer, labelsIt iterator[labels.Labels], chunksIt iterator[[]storepb.AggrChunk], stats *safeQueryStats) error {
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

	for labelsIt.Next() && (req.SkipChunks || chunksIt.Next()) {
		lset := labelsIt.At()
		seriesCount++
		series := storepb.Series{
			Labels: mimirpb.FromLabelsToLabelAdapters(lset),
		}
		if !req.SkipChunks {
			chks := chunksIt.At()
			series.Chunks = chks
			chunksCount += len(chks)
			s.metrics.chunkSizeBytes.Observe(float64(chunksSize(chks)))
		}

		err := s.sendMessage("series", srv, storepb.NewSeriesResponse(&series), &encodeDuration, &sendDuration)
		if err != nil {
			return err
		}
	}
	if labelsIt.Err() != nil {
		return errors.Wrap(labelsIt.Err(), "error iterating labels")
	}
	if !req.SkipChunks && chunksIt.Err() != nil {
		return errors.Wrap(chunksIt.Err(), "error iterating chunks")
	}

	return nil
}

func (s *ParquetBucketStore) recordSeriesCallResult(stats *safeQueryStats) {
	// TODO Implement stats reporting here
}

func (s *ParquetBucketStore) recordRequestAmbientTime(stats *safeQueryStats, startTime time.Time) {
	stats.update(func(stats *queryStats) {
		stats.streamingSeriesAmbientTime += time.Since(startTime)
	})
}

func (s *ParquetBucketStore) sendMessage(typ string, srv storegatewaypb.StoreGateway_SeriesServer, msg interface{}, encodeDuration, sendDuration *time.Duration) error {
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

func (s *ParquetBucketStore) sendStats(srv storegatewaypb.StoreGateway_SeriesServer, stats *safeQueryStats) error {
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

// Stats returns statistics about the BucketStore instance.
func (s *ParquetBucketStore) Stats() BucketStoreStats {
	return BucketStoreStats{
		BlocksLoadedTotal: s.blockSet.len(),
	}
}

// InitialSync perform blocking sync with extra step at the end to delete locally saved blocks that are no longer
// present in the bucket. The mismatch of these can only happen between restarts, so we can do that only once per startup.
func (s *ParquetBucketStore) InitialSync(ctx context.Context) error {
	// Read the snapshot before running the sync. After we run a sync we'll start persisting the snapshots again,
	// so we need to read the pre-shutdown snapshot before the sync.

	// TODO implement aspects that rely on the indexheader for parquet blocks

	// previouslyLoadedBlocks := s.tryRestoreLoadedBlocksSet()

	if err := s.syncBlocks(ctx); err != nil {
		return errors.Wrap(err, "sync block")
	}
	// if s.indexHeaderCfg.EagerLoadingStartupEnabled {
	//	s.loadBlocks(ctx, previouslyLoadedBlocks)
	// }

	err := s.cleanUpUnownedBlocks()
	if err != nil {
		return err
	}

	return nil
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
		level.Debug(s.logger).Log("msg", "syncing block", "id", id, "meta", meta)
		if s.blockSet.contains(id) {
			level.Debug(s.logger).Log("msg", "block already loaded, skipping", "id", id)
			continue
		}

		markPath := path.Join(id.String(), parquetconverter.ParquetConversionMarkFileName)
		exists, err := s.bkt.Exists(ctx, markPath)
		if err != nil {
			level.Debug(s.logger).Log("msg", "failed to check parquet conversion mark existence, skipping block", "block", id, "err", err)
			continue
		}

		if !exists {
			level.Debug(s.logger).Log("msg", "parquet conversion mark not found, block not converted, skipping", "block", id)
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
	// _ = s.snapshotter.StartAsync(context.Background())

	return nil
}

func (s *ParquetBucketStore) addBlock(ctx context.Context, meta *block.Meta) (err error) {
	blockLocalDir := filepath.Join(s.localDir, meta.ULID.String())
	start := time.Now()

	level.Debug(s.logger).Log("msg", "loading new block", "id", meta.ULID)
	defer func() {
		if err != nil {
			s.metrics.blockLoadFailures.Inc()
			if err2 := os.RemoveAll(blockLocalDir); err2 != nil {
				level.Warn(s.logger).Log("msg", "failed to remove block we cannot load", "err", err2)
			}
			level.Error(s.logger).Log("msg", "loading block failed", "elapsed", time.Since(start), "id", meta.ULID, "err", err)
		} else {
			level.Info(s.logger).Log("msg", "loaded new block", "elapsed", time.Since(start), "id", meta.ULID)
		}
	}()
	s.metrics.blockLoads.Inc()

	blockReader, err := s.readerPool.GetReader(
		ctx,
		meta.ULID,
		s.bkt,
		blockLocalDir,
		s.loadIndexToDisk,
		s.fileOpts,
		s.logger,
	)

	if err != nil {
		return errors.Wrap(err, "create parquet block reader")
	}

	defer func() {
		if err != nil {
			runutil.CloseWithErrCapture(&err, blockReader, "parquet block reader")
		}
	}()

	b := newParquetBucketBlock(
		meta,
		blockReader,
		blockLocalDir,
	)
	if err != nil {
		return errors.Wrap(err, "new parquet bucket block")
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

func (s *ParquetBucketStore) tryRestoreLoadedBlocksSet() map[ulid.ULID]struct{} { // nolint:unused
	// TODO implement for parquet blocks
	// previouslyLoadedBlocks, err := indexheader.RestoreLoadedBlocks(s.localDir)
	// if err != nil {
	//	level.Warn(s.logger).Log(
	//		"msg", "loading the list of index-headers from snapshot file failed; not eagerly loading index-headers for tenant",
	//		"dir", s.localDir,
	//		"err", err,
	//	)
	//	// Don't fail initialization. If eager loading doesn't happen, then we will load index-headers lazily.
	//	// Lazy loading which is slower, but not worth failing startup for.
	// }
	// return previouslyLoadedBlocks
	return nil
}

func (s *ParquetBucketStore) removeBlock(id ulid.ULID) (returnErr error) {
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
	if err := os.RemoveAll(b.localDir); err != nil {
		return errors.Wrap(err, "delete block")
	}
	return nil
}

// RemoveBlocksAndClose remove all blocks from local disk and releases all resources associated with the BucketStore.
func (s *ParquetBucketStore) RemoveBlocksAndClose() error {
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

func (s *ParquetBucketStore) removeAllBlocks() error {
	blockIDs := s.blockSet.allBlockULIDs()

	errs := multierror.New()
	for _, id := range blockIDs {
		if err := s.removeBlock(id); err != nil {
			errs.Add(errors.Wrap(err, fmt.Sprintf("block: %s", id.String())))
		}
	}

	return errs.Err()
}

func (s *ParquetBucketStore) closeAllBlocks() error {
	return s.blockSet.closeAll()
}

func (s *ParquetBucketStore) cleanUpUnownedBlocks() error {
	fis, err := os.ReadDir(s.localDir)
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
		if err := os.RemoveAll(path.Join(s.localDir, id.String())); err != nil {
			level.Warn(s.logger).Log("msg", "failed to remove block which is not needed", "err", err)
		}
	}

	return nil
}

// toLabelsAndAggChunksSlice pulls all labels and chunks from the
// storage.ChunkSeriesSet and returns them as slices, converting the chunks to
// storepb.AggrChunk format. If skipChunks is true, the chunks slice will be
// empty.
func toLabelsAndAggChunksSlice(chunkSeriesSet storage.ChunkSeriesSet, skipChunks bool) ([]labels.Labels, [][]storepb.AggrChunk, error) {
	var seriesLabels []labels.Labels
	var aggrChunks [][]storepb.AggrChunk

	for chunkSeriesSet.Next() {
		chunkSeries := chunkSeriesSet.At()
		lbls := chunkSeries.Labels()
		seriesLabels = append(seriesLabels, lbls)

		if skipChunks {
			continue
		}

		// Convert chunks to storepb.AggrChunk
		var aggrChunkList []storepb.AggrChunk
		it := chunkSeries.Iterator(nil)
		for it.Next() {
			meta := it.At()
			aggrChunkList = append(aggrChunkList, storepb.AggrChunk{
				MinTime: meta.MinTime,
				MaxTime: meta.MaxTime,
				Raw: storepb.Chunk{
					Type: prometheusChunkEncodingToStorePBChunkType(meta.Chunk.Encoding()),
					Data: meta.Chunk.Bytes(),
				},
			})
		}
		aggrChunks = append(aggrChunks, aggrChunkList)
	}

	return seriesLabels, aggrChunks, chunkSeriesSet.Err()
}

func prometheusChunkEncodingToStorePBChunkType(enc chunkenc.Encoding) storepb.Chunk_Encoding {
	switch enc {
	case chunkenc.EncXOR:
		return storepb.Chunk_XOR
	case chunkenc.EncHistogram:
		return storepb.Chunk_Histogram
	case chunkenc.EncFloatHistogram:
		return storepb.Chunk_FloatHistogram
	default:
		panic("unknown encoding")
	}
}

type concreteIterator[T any] struct {
	items []T
	curr  int
}

func newConcreteIterator[T any](items []T) *concreteIterator[T] {
	return &concreteIterator[T]{
		items: items,
		curr:  -1,
	}
}

func (it *concreteIterator[T]) Next() bool {
	it.curr++
	return it.curr < len(it.items)
}

func (a *concreteIterator[T]) Err() error {
	return nil
}

func (a *concreteIterator[T]) At() T {
	return a.items[a.curr]
}

// TimeRange returns the minimum and maximum timestamp of data available in the store.
func (s *ParquetBucketStore) TimeRange() (mint, maxt int64) {
	return s.blockSet.timerange()
}

func (s *ParquetBucketStore) recordLabelNamesCallResult(stats *safeQueryStats) {
	// TODO implement for proper stats reporting
}

func (s *ParquetBucketStore) recordLabelValuesCallResult(stats *safeQueryStats) {
	// TODO implement for proper stats reporting
}
