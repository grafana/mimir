// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/types"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storegateway/hintspb"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type ParquetBucketStores struct {
	services.Service

	logger log.Logger
	reg    prometheus.Registerer

	metrics              *BucketStoreMetrics // TODO: Create ParquetBucketStoreMetrics
	chunksLimiterFactory ChunksLimiterFactory
	seriesLimiterFactory SeriesLimiterFactory
}

// NewParquetBucketStores initializes a Parquet implementation of the Stores interface.
func NewParquetBucketStores(
	logger log.Logger,
	reg prometheus.Registerer,
) (*ParquetBucketStores, error) {

	stores := &ParquetBucketStores{
		logger: logger,
		reg:    reg,
	}
	stores.Service = services.NewIdleService(nil, nil)

	return stores, nil
}

func (ss ParquetBucketStores) Series(req *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer) (err error) {
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
		spanLogger       = spanlogger.FromContext(srv.Context(), ss.logger)
		ctx              = srv.Context()
		stats            = newSafeQueryStats()
		reqBlockMatchers []*labels.Matcher
	)
	defer ss.recordSeriesCallResult(stats)
	defer ss.recordRequestAmbientTime(stats, time.Now())

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

	logSeriesRequestToSpan(srv.Context(), ss.logger, req.MinTime, req.MaxTime, matchers, reqBlockMatchers, shardSelector, req.StreamingChunksBatchSize)

	shards := ss.openParquetShardsForReading(ctx, req.SkipChunks, req.MinTime, req.MaxTime, reqBlockMatchers, stats)
	defer ss.closeParquetShards(shards)

	// Wait for the query gate only after opening blocks. Opening blocks is usually fast (~1ms),
	// but sometimes it can take minutes if the block isn't loaded and there is a surge in queries for unloaded blocks.
	done, err := ss.limitConcurrentQueries(ctx, stats)
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
	if err := ss.sendHints(srv, resHints); err != nil {
		return err
	}

	streamingSeriesCount := 0
	if req.StreamingChunksBatchSize > 0 {
		var (
			seriesSet       storepb.SeriesSet
			seriesLoadStart = time.Now()
			chunksLimiter   = ss.chunksLimiterFactory(ss.metrics.queriesDropped.WithLabelValues("chunks"))
			seriesLimiter   = ss.seriesLimiterFactory(ss.metrics.queriesDropped.WithLabelValues("series"))
		)

		// Placeholder: Create series set for streaming labels from parquet shards
		seriesSet, err = ss.createParquetSeriesSetForLabels(ctx, req, shards, shardSelector, matchers, chunksLimiter, seriesLimiter, stats)
		if err != nil {
			return err
		}

		streamingSeriesCount, err = ss.sendStreamingSeriesLabelsAndStats(req, srv, stats, seriesSet)
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
	chunksLimiter := ss.chunksLimiterFactory(ss.metrics.queriesDropped.WithLabelValues("chunks"))
	seriesLimiter := ss.seriesLimiterFactory(ss.metrics.queriesDropped.WithLabelValues("series"))

	start := time.Now()
	if req.StreamingChunksBatchSize > 0 {
		seriesChunkIt := ss.createParquetSeriesChunksSetIterator(ctx, req, shards, shardSelector, matchers, chunksLimiter, seriesLimiter, stats)
		err = ss.sendStreamingChunks(req, srv, seriesChunkIt, stats, streamingSeriesCount)
	} else {
		var seriesSet storepb.SeriesSet
		seriesSet, err = ss.createParquetSeriesSetWithChunks(ctx, req, shards, shardSelector, matchers, chunksLimiter, seriesLimiter, stats)
		if err != nil {
			return err
		}
		err = ss.sendSeriesChunks(req, srv, seriesSet, stats)
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
		return ss.sendStats(srv, stats)
	}

	return nil
}

func (ss ParquetBucketStores) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	// TODO implement me
	panic("implement me")
}

func (ss ParquetBucketStores) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	// TODO implement me
	panic("implement me")
}

func (ss ParquetBucketStores) SyncBlocks(ctx context.Context) error {
	// TODO implement me
	panic("implement me")
}

func (ss ParquetBucketStores) scanUsers(ctx context.Context) ([]string, error) {
	// TODO implement me
	panic("implement me")
}

type parquetShardWithMetadata struct {
	storage.ParquetShardOpener
	BlockID ulid.ULID
	queried bool
}

func (ps *parquetShardWithMetadata) MarkQueried() {
	ps.queried = true
}

// Placeholder methods for parquet-specific functionality
func (ss *ParquetBucketStores) openParquetShardsForReading(ctx context.Context, skipChunks bool, minTime, maxTime int64, reqBlockMatchers []*labels.Matcher, stats *safeQueryStats) []*parquetShardWithMetadata {
	// TODO: Implement parquet shard discovery and opening logic
	// This should:
	// 1. Discover parquet shards that intersect with the time range
	// 2. Use storage.ParquetShardOpener to open .labels.parquet and .chunks.parquet files
	// 3. Read parquet schemas and metadata for efficient querying using shard.TSDBSchema()
	// 4. Wrap opened ParquetShard with metadata (BlockID, queried status)
	panic("TODO: implement openParquetShardsForReading")
}

func (ss *ParquetBucketStores) closeParquetShards(shards []*parquetShardWithMetadata) {
	for _, shard := range shards {
		if shard == nil {
			continue
		}
		if err := shard.Close(); err != nil {
			ss.logger.Log("msg", "failed to close parquet shard", "block_id", shard.BlockID, "err", err)
		}
	}
	// TODO: Implement parquet shard cleanup
	// Close any open parquet file handles and release resources
	panic("TODO: implement closeParquetShards")
}

func (ss *ParquetBucketStores) limitConcurrentQueries(ctx context.Context, stats *safeQueryStats) (func(), error) {
	// TODO: Can potentially reuse BucketStore.limitConcurrentQueries
	// or implement parquet-specific version if needed
	panic("TODO: implement limitConcurrentQueries")
}

func (ss *ParquetBucketStores) sendHints(srv storegatewaypb.StoreGateway_SeriesServer, resHints *hintspb.SeriesResponseHints) error {
	// TODO: Implement hints sending for parquet stores
	panic("TODO: implement sendHints")
}

func (ss *ParquetBucketStores) createParquetSeriesSetForLabels(ctx context.Context, req *storepb.SeriesRequest, shards []*parquetShardWithMetadata, shardSelector *sharding.ShardSelector, matchers []*labels.Matcher, chunksLimiter ChunksLimiter, seriesLimiter SeriesLimiter, stats *safeQueryStats) (storepb.SeriesSet, error) {
	// TODO: Implement parquet series set creation for labels phase
	// This should:
	// 1. "Stream read" .labels.parquet files from shards using shard.LabelsFile()
	// 2. Create and return storepb.SeriesSet that iterates over series labels without chunks
	// Please note that storepb.SeriesSet assumes series are ordered.
	panic("TODO: implement createParquetSeriesSetForLabels")
}

func (ss *ParquetBucketStores) sendStreamingSeriesLabelsAndStats(req *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer, stats *safeQueryStats, seriesSet storepb.SeriesSet) (int, error) {
	// TODO: Can potentially reuse BucketStore.sendStreamingSeriesLabelsAndStats
	// or implement parquet-specific version if needed
	panic("TODO: implement sendStreamingSeriesLabelsAndStats")
}

func (ss *ParquetBucketStores) createParquetSeriesChunksSetIterator(ctx context.Context, req *storepb.SeriesRequest, shards []*parquetShardWithMetadata, shardSelector *sharding.ShardSelector, matchers []*labels.Matcher, chunksLimiter ChunksLimiter, seriesLimiter SeriesLimiter, stats *safeQueryStats) iterator[seriesChunksSet] {
	// TODO: Implement parquet series chunks iterator creation
	// This should:
	// 1. Stream read .chunks.parquet files from shards using shard.ChunksFile()
	// 2. Return iterator[seriesChunksSet] / or the new iterator Nico is workisng on in his PR that streams chunks for the series discovered in labels phase
	panic("TODO: implement createParquetSeriesChunksSetIterator")
}

func (ss *ParquetBucketStores) sendStreamingChunks(req *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer, seriesChunkIt iterator[seriesChunksSet], stats *safeQueryStats, streamingSeriesCount int) error {
	// TODO: Can potentially reuse BucketStore.sendStreamingChunks
	// or implement parquet-specific version if needed
	panic("TODO: implement sendStreamingChunks")
}

func (ss *ParquetBucketStores) createParquetSeriesSetWithChunks(ctx context.Context, req *storepb.SeriesRequest, shards []*parquetShardWithMetadata, shardSelector *sharding.ShardSelector, matchers []*labels.Matcher, chunksLimiter ChunksLimiter, seriesLimiter SeriesLimiter, stats *safeQueryStats) (storepb.SeriesSet, error) {
	// TODO: Implement parquet series set creation for non-streaming request
	// I think this should create a series that includes the labels in one go and its typically called when skipchunks is true
	panic("TODO: implement createParquetSeriesSetWithChunks")
}

func (ss *ParquetBucketStores) sendSeriesChunks(req *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer, seriesSet storepb.SeriesSet, stats *safeQueryStats) error {
	// TODO: Can potentially reuse BucketStore.sendSeriesChunks
	// or implement parquet-specific version if needed
	panic("TODO: implement sendSeriesChunks")
}

func (ss *ParquetBucketStores) sendStats(srv storegatewaypb.StoreGateway_SeriesServer, stats *safeQueryStats) error {
	// TODO: Implement stats sending for parquet stores
	panic("TODO: implement sendStats")
}

func (ss *ParquetBucketStores) recordSeriesCallResult(stats *safeQueryStats) {
	// TODO: Implement series call result recording for parquet stores
	panic("TODO: implement recordSeriesCallResult")
}

func (ss *ParquetBucketStores) recordRequestAmbientTime(stats *safeQueryStats, startTime time.Time) {
	// TODO: Implement request ambient time recording for parquet stores
	panic("TODO: implement recordRequestAmbientTime")
}
