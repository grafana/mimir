// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/blocks_store_queryable.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/types"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/objstore"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
	grpc_metadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/storage/sharding"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/storegateway/hintspb"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/limiter"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	// The maximum number of times we attempt fetching missing blocks from different
	// store-gateways. If no more store-gateways are left (ie. due to lower replication
	// factor) than we'll end the retries earlier.
	maxFetchSeriesAttempts = 3
)

// BlocksStoreSet is the interface used to get the clients to query series on a set of blocks.
type BlocksStoreSet interface {
	services.Service

	// GetClientsFor returns the store gateway clients that should be used to
	// query the set of blocks in input. The exclude parameter is the map of
	// blocks -> store-gateway addresses that should be excluded.
	GetClientsFor(userID string, blockIDs []ulid.ULID, exclude map[ulid.ULID][]string) (map[BlocksStoreClient][]ulid.ULID, error)
}

// BlocksFinder is the interface used to find blocks for a given user and time range.
type BlocksFinder interface {
	services.Service

	// GetBlocks returns known blocks for userID containing samples within the range minT
	// and maxT (milliseconds, both included). Returned blocks are sorted by MaxTime descending.
	GetBlocks(ctx context.Context, userID string, minT, maxT int64) (bucketindex.Blocks, map[ulid.ULID]*bucketindex.BlockDeletionMark, error)
}

// BlocksStoreClient is the interface that should be implemented by any client used
// to query a backend store-gateway.
type BlocksStoreClient interface {
	storegatewaypb.StoreGatewayClient

	// RemoteAddress returns the address of the remote store-gateway and is used to uniquely
	// identify a store-gateway backend instance.
	RemoteAddress() string
}

// BlocksStoreLimits is the interface that should be implemented by the limits provider.
type BlocksStoreLimits interface {
	bucket.TenantConfigProvider

	MaxLabelsQueryLength(userID string) time.Duration
	MaxChunksPerQuery(userID string) int
	StoreGatewayTenantShardSize(userID string) int
}

type blocksStoreQueryableMetrics struct {
	storesHit prometheus.Histogram
	refetches prometheus.Histogram

	blocksFound                                       prometheus.Counter
	blocksQueried                                     prometheus.Counter
	blocksWithCompactorShardButIncompatibleQueryShard prometheus.Counter
}

func newBlocksStoreQueryableMetrics(reg prometheus.Registerer) *blocksStoreQueryableMetrics {
	return &blocksStoreQueryableMetrics{
		storesHit: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "querier_storegateway_instances_hit_per_query",
			Help:      "Number of store-gateway instances hit for a single query.",
			Buckets:   []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		}),
		refetches: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "querier_storegateway_refetches_per_query",
			Help:      "Number of re-fetches attempted while querying store-gateway instances due to missing blocks.",
			Buckets:   []float64{0, 1, 2},
		}),

		blocksFound: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_querier_blocks_found_total",
			Help: "Number of blocks found based on query time range.",
		}),
		blocksQueried: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_querier_blocks_queried_total",
			Help: "Number of blocks queried to satisfy query. Compared to blocks found, some blocks may have been filtered out thanks to query and compactor sharding.",
		}),
		blocksWithCompactorShardButIncompatibleQueryShard: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_querier_blocks_with_compactor_shard_but_incompatible_query_shard_total",
			Help: "Blocks that couldn't be checked for query and compactor sharding optimization due to incompatible shard counts.",
		}),
	}
}

// BlocksStoreQueryable is a queryable which queries blocks storage via
// the store-gateway.
type BlocksStoreQueryable struct {
	services.Service

	stores                   BlocksStoreSet
	finder                   BlocksFinder
	consistency              *BlocksConsistencyChecker
	logger                   log.Logger
	queryStoreAfter          time.Duration
	metrics                  *blocksStoreQueryableMetrics
	limits                   BlocksStoreLimits
	streamingChunksBatchSize uint64

	// Subservices manager.
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher
}

func NewBlocksStoreQueryable(
	stores BlocksStoreSet,
	finder BlocksFinder,
	consistency *BlocksConsistencyChecker,
	limits BlocksStoreLimits,
	queryStoreAfter time.Duration,
	streamingChunksBatchSize uint64,
	logger log.Logger,
	reg prometheus.Registerer,
) (*BlocksStoreQueryable, error) {
	manager, err := services.NewManager(stores, finder)
	if err != nil {
		return nil, errors.Wrap(err, "register blocks storage queryable subservices")
	}

	q := &BlocksStoreQueryable{
		stores:                   stores,
		finder:                   finder,
		consistency:              consistency,
		queryStoreAfter:          queryStoreAfter,
		logger:                   logger,
		subservices:              manager,
		subservicesWatcher:       services.NewFailureWatcher(),
		metrics:                  newBlocksStoreQueryableMetrics(reg),
		limits:                   limits,
		streamingChunksBatchSize: streamingChunksBatchSize,
	}

	q.Service = services.NewBasicService(q.starting, q.running, q.stopping)

	return q, nil
}

func NewBlocksStoreQueryableFromConfig(querierCfg Config, gatewayCfg storegateway.Config, storageCfg mimir_tsdb.BlocksStorageConfig, limits BlocksStoreLimits, logger log.Logger, reg prometheus.Registerer) (*BlocksStoreQueryable, error) {
	var (
		stores       BlocksStoreSet
		bucketClient objstore.Bucket
	)

	bucketClient, err := bucket.NewClient(context.Background(), storageCfg.Bucket, "querier", logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create bucket client")
	}

	// Blocks finder doesn't use chunks, but we pass config for consistency.
	cachingBucket, err := mimir_tsdb.CreateCachingBucket(nil, storageCfg.BucketStore.ChunksCache, storageCfg.BucketStore.MetadataCache, bucketClient, logger, prometheus.WrapRegistererWith(prometheus.Labels{"component": "querier"}, reg))
	if err != nil {
		return nil, errors.Wrap(err, "create caching bucket")
	}
	bucketClient = cachingBucket

	// Create the blocks finder.
	var finder BlocksFinder
	if storageCfg.BucketStore.BucketIndex.DeprecatedEnabled {
		finder = NewBucketIndexBlocksFinder(BucketIndexBlocksFinderConfig{
			IndexLoader: bucketindex.LoaderConfig{
				CheckInterval:         time.Minute,
				UpdateOnStaleInterval: storageCfg.BucketStore.SyncInterval,
				UpdateOnErrorInterval: storageCfg.BucketStore.BucketIndex.UpdateOnErrorInterval,
				IdleTimeout:           storageCfg.BucketStore.BucketIndex.IdleTimeout,
			},
			MaxStalePeriod:           storageCfg.BucketStore.BucketIndex.MaxStalePeriod,
			IgnoreDeletionMarksDelay: storageCfg.BucketStore.IgnoreDeletionMarksDelay,
		}, bucketClient, limits, logger, reg)
	} else {
		finder = NewBucketScanBlocksFinder(BucketScanBlocksFinderConfig{
			ScanInterval:             storageCfg.BucketStore.SyncInterval,
			TenantsConcurrency:       storageCfg.BucketStore.TenantSyncConcurrency,
			MetasConcurrency:         storageCfg.BucketStore.MetaSyncConcurrency,
			CacheDir:                 storageCfg.BucketStore.SyncDir,
			IgnoreDeletionMarksDelay: storageCfg.BucketStore.IgnoreDeletionMarksDelay,
		}, bucketClient, limits, logger, reg)
	}

	storesRingCfg := gatewayCfg.ShardingRing.ToRingConfig()
	storesRingBackend, err := kv.NewClient(
		storesRingCfg.KVStore,
		ring.GetCodec(),
		kv.RegistererWithKVName(prometheus.WrapRegistererWithPrefix("cortex_", reg), "querier-store-gateway"),
		logger,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create store-gateway ring backend")
	}

	storesRing, err := ring.NewWithStoreClientAndStrategy(storesRingCfg, storegateway.RingNameForClient, storegateway.RingKey, storesRingBackend, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), prometheus.WrapRegistererWithPrefix("cortex_", reg), logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create store-gateway ring client")
	}

	stores, err = newBlocksStoreReplicationSet(storesRing, randomLoadBalancing, limits, querierCfg.StoreGatewayClient, logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create store set")
	}

	consistency := NewBlocksConsistencyChecker(
		// Exclude blocks which have been recently uploaded, in order to give enough time to store-gateways
		// to discover and load them (3 times the sync interval).
		3*storageCfg.BucketStore.SyncInterval,
		// To avoid any false positive in the consistency check, we do exclude blocks which have been
		// recently marked for deletion, until the "ignore delay / 2". This means the consistency checker
		// exclude such blocks about 50% of the time before querier and store-gateway stops querying them.
		storageCfg.BucketStore.IgnoreDeletionMarksDelay/2,
		logger,
		reg,
	)

	streamingBufferSize := querierCfg.StreamingChunksPerStoreGatewaySeriesBufferSize
	if !querierCfg.PreferStreamingChunksFromStoreGateways {
		streamingBufferSize = 0
	}

	return NewBlocksStoreQueryable(stores, finder, consistency, limits, querierCfg.QueryStoreAfter, streamingBufferSize, logger, reg)
}

func (q *BlocksStoreQueryable) starting(ctx context.Context) error {
	q.subservicesWatcher.WatchManager(q.subservices)

	if err := services.StartManagerAndAwaitHealthy(ctx, q.subservices); err != nil {
		return errors.Wrap(err, "unable to start blocks storage queryable subservices")
	}

	return nil
}

func (q *BlocksStoreQueryable) running(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-q.subservicesWatcher.Chan():
			return errors.Wrap(err, "block storage queryable subservice failed")
		}
	}
}

func (q *BlocksStoreQueryable) stopping(_ error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), q.subservices)
}

// Querier returns a new Querier on the storage.
func (q *BlocksStoreQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	if s := q.State(); s != services.Running {
		return nil, errors.Errorf("BlocksStoreQueryable is not running: %v", s)
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	return &blocksStoreQuerier{
		ctx:                      ctx,
		minT:                     mint,
		maxT:                     maxt,
		userID:                   userID,
		finder:                   q.finder,
		stores:                   q.stores,
		metrics:                  q.metrics,
		limits:                   q.limits,
		streamingChunksBatchSize: q.streamingChunksBatchSize,
		consistency:              q.consistency,
		logger:                   q.logger,
		queryStoreAfter:          q.queryStoreAfter,
	}, nil
}

type blocksStoreQuerier struct {
	ctx                      context.Context
	minT, maxT               int64
	userID                   string
	finder                   BlocksFinder
	stores                   BlocksStoreSet
	metrics                  *blocksStoreQueryableMetrics
	consistency              *BlocksConsistencyChecker
	limits                   BlocksStoreLimits
	streamingChunksBatchSize uint64
	logger                   log.Logger

	// If set, the querier manipulates the max time to not be greater than
	// "now - queryStoreAfter" so that most recent blocks are not queried.
	queryStoreAfter time.Duration
}

// Select implements storage.Querier interface.
// The bool passed is ignored because the series is always sorted.
func (q *blocksStoreQuerier) Select(_ bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return q.selectSorted(sp, matchers...)
}

func (q *blocksStoreQuerier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	spanLog, ctx := spanlogger.NewWithLogger(q.ctx, q.logger, "blocksStoreQuerier.LabelNames")
	defer spanLog.Span.Finish()

	minT, maxT := q.minT, q.maxT

	level.Debug(spanLog).Log("start", util.TimeFromMillis(minT).UTC().String(), "end",
		util.TimeFromMillis(maxT).UTC().String(), "matchers", util.MatchersStringer(matchers))

	// Clamp minT; we cannot push this down into queryWithConsistencyCheck as not all its callers need to clamp minT
	maxQueryLength := q.limits.MaxLabelsQueryLength(q.userID)
	if maxQueryLength != 0 {
		minT = clampMinTime(spanLog, minT, maxT, -maxQueryLength, "max label query length")
	}

	var (
		resNameSets       = [][]string{}
		resWarnings       = storage.Warnings(nil)
		convertedMatchers = convertMatchersToLabelMatcher(matchers)
	)

	queryF := func(clients map[BlocksStoreClient][]ulid.ULID, minT, maxT int64) ([]ulid.ULID, error) {
		nameSets, warnings, queriedBlocks, err := q.fetchLabelNamesFromStore(ctx, clients, minT, maxT, convertedMatchers)
		if err != nil {
			return nil, err
		}

		resNameSets = append(resNameSets, nameSets...)
		resWarnings = append(resWarnings, warnings...)

		return queriedBlocks, nil
	}

	err := q.queryWithConsistencyCheck(ctx, spanLog, minT, maxT, nil, queryF)
	if err != nil {
		return nil, nil, err
	}

	return util.MergeSlices(resNameSets...), resWarnings, nil
}

func (q *blocksStoreQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	spanLog, ctx := spanlogger.NewWithLogger(q.ctx, q.logger, "blocksStoreQuerier.LabelValues")
	defer spanLog.Span.Finish()

	minT, maxT := q.minT, q.maxT

	level.Debug(spanLog).Log("start", util.TimeFromMillis(minT).UTC().String(), "end",
		util.TimeFromMillis(maxT).UTC().String(), "matchers", util.MatchersStringer(matchers))

	// Clamp minT; we cannot push this down into queryWithConsistencyCheck as not all its callers need to clamp minT
	maxQueryLength := q.limits.MaxLabelsQueryLength(q.userID)
	if maxQueryLength != 0 {
		minT = clampMinTime(spanLog, minT, maxT, -maxQueryLength, "max label query length")
	}

	var (
		resValueSets = [][]string{}
		resWarnings  = storage.Warnings(nil)
	)

	queryF := func(clients map[BlocksStoreClient][]ulid.ULID, minT, maxT int64) ([]ulid.ULID, error) {
		valueSets, warnings, queriedBlocks, err := q.fetchLabelValuesFromStore(ctx, name, clients, minT, maxT, matchers...)
		if err != nil {
			return nil, err
		}

		resValueSets = append(resValueSets, valueSets...)
		resWarnings = append(resWarnings, warnings...)

		return queriedBlocks, nil
	}

	err := q.queryWithConsistencyCheck(ctx, spanLog, minT, maxT, nil, queryF)
	if err != nil {
		return nil, nil, err
	}

	return util.MergeSlices(resValueSets...), resWarnings, nil
}

func (q *blocksStoreQuerier) Close() error {
	return nil
}

func (q *blocksStoreQuerier) selectSorted(sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	spanLog, ctx := spanlogger.NewWithLogger(q.ctx, q.logger, "blocksStoreQuerier.selectSorted")
	defer spanLog.Span.Finish()

	minT, maxT := sp.Start, sp.End

	var (
		convertedMatchers = convertMatchersToLabelMatcher(matchers)
		resSeriesSets     = []storage.SeriesSet(nil)
		resWarnings       = storage.Warnings(nil)
		streamStarters    []func()
		chunkEstimators   []func() int
		queryLimiter      = limiter.QueryLimiterFromContextWithFallback(ctx)
	)

	shard, _, err := sharding.ShardFromMatchers(matchers)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	queryF := func(clients map[BlocksStoreClient][]ulid.ULID, minT, maxT int64) ([]ulid.ULID, error) {
		seriesSets, queriedBlocks, warnings, startStreamingChunks, chunkEstimator, err := q.fetchSeriesFromStores(ctx, sp, clients, minT, maxT, convertedMatchers)
		if err != nil {
			return nil, err
		}

		resSeriesSets = append(resSeriesSets, seriesSets...)
		resWarnings = append(resWarnings, warnings...)
		streamStarters = append(streamStarters, startStreamingChunks)
		chunkEstimators = append(chunkEstimators, chunkEstimator)

		return queriedBlocks, nil
	}

	err = q.queryWithConsistencyCheck(ctx, spanLog, minT, maxT, shard, queryF)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	if len(streamStarters) > 0 {
		level.Debug(spanLog).Log("msg", "starting streaming")

		// If this was a streaming call, start fetching streaming chunks here.
		for _, ss := range streamStarters {
			ss()
		}

		level.Debug(spanLog).Log("msg", "streaming started, waiting for chunks estimates")

		chunksEstimate := 0
		for _, chunkEstimator := range chunkEstimators {
			chunksEstimate += chunkEstimator()
		}

		level.Debug(spanLog).Log("msg", "received chunks estimate from all store-gateways", "chunks_estimate", chunksEstimate)

		if err := queryLimiter.AddEstimatedChunks(chunksEstimate); err != nil {
			return storage.ErrSeriesSet(err)
		}
	}

	if len(resSeriesSets) == 0 {
		storage.EmptySeriesSet()
	}

	return series.NewSeriesSetWithWarnings(
		storage.NewMergeSeriesSet(resSeriesSets, storage.ChainedSeriesMerge),
		resWarnings)
}

type queryFunc func(clients map[BlocksStoreClient][]ulid.ULID, minT, maxT int64) ([]ulid.ULID, error)

func (q *blocksStoreQuerier) queryWithConsistencyCheck(
	ctx context.Context, logger log.Logger, minT, maxT int64, shard *sharding.ShardSelector, queryF queryFunc,
) error {
	now := time.Now()

	if !ShouldQueryBlockStore(q.queryStoreAfter, now, minT) {
		q.metrics.storesHit.Observe(0)
		level.Debug(logger).Log("msg", "not querying block store; query time range begins after the query-store-after limit")
		return nil
	}

	maxT = clampMaxTime(logger, maxT, now.UnixMilli(), -q.queryStoreAfter, "query store after")

	// Find the list of blocks we need to query given the time range.
	knownBlocks, knownDeletionMarks, err := q.finder.GetBlocks(ctx, q.userID, minT, maxT)
	if err != nil {
		return err
	}

	if len(knownBlocks) == 0 {
		q.metrics.storesHit.Observe(0)
		level.Debug(logger).Log("msg", "no blocks found")
		return nil
	}

	q.metrics.blocksFound.Add(float64(len(knownBlocks)))

	if shard != nil && shard.ShardCount > 0 {
		level.Debug(logger).Log("msg", "filtering blocks due to sharding", "blocksBeforeFiltering", knownBlocks.String(), "shardID", shard.LabelValue())

		result, incompatibleBlocks := filterBlocksByShard(knownBlocks, shard.ShardIndex, shard.ShardCount)

		level.Debug(logger).Log("msg", "result of filtering blocks", "before", len(knownBlocks), "after", len(result), "filtered", len(knownBlocks)-len(result), "incompatible", incompatibleBlocks)
		q.metrics.blocksWithCompactorShardButIncompatibleQueryShard.Add(float64(incompatibleBlocks))

		knownBlocks = result
	}

	q.metrics.blocksQueried.Add(float64(len(knownBlocks)))

	level.Debug(logger).Log("msg", "found blocks to query", "expected", knownBlocks.String())

	var (
		// At the beginning the list of blocks to query are all known blocks.
		remainingBlocks = knownBlocks.GetULIDs()
		attemptedBlocks = map[ulid.ULID][]string{}
		touchedStores   = map[string]struct{}{}

		resQueriedBlocks = []ulid.ULID(nil)
	)

	for attempt := 1; attempt <= maxFetchSeriesAttempts; attempt++ {
		// Find the set of store-gateway instances having the blocks. The exclude parameter is the
		// map of blocks queried so far, with the list of store-gateway addresses for each block.
		clients, err := q.stores.GetClientsFor(q.userID, remainingBlocks, attemptedBlocks)
		if err != nil {
			// If it's a retry and we get an error, it means there are no more store-gateways left
			// from which running another attempt, so we're just stopping retrying.
			if attempt > 1 {
				level.Warn(logger).Log("msg", "unable to get store-gateway clients while retrying to fetch missing blocks", "err", err)
				break
			}

			return err
		}
		level.Debug(logger).Log("msg", "found store-gateway instances to query", "num instances", len(clients), "attempt", attempt)

		// Fetch series from stores. If an error occur we do not retry because retries
		// are only meant to cover missing blocks.
		queriedBlocks, err := queryF(clients, minT, maxT)
		if err != nil {
			return err
		}
		level.Debug(logger).Log("msg", "received series from all store-gateways", "queried blocks", strings.Join(convertULIDsToString(queriedBlocks), " "))

		resQueriedBlocks = append(resQueriedBlocks, queriedBlocks...)

		// Update the map of blocks we attempted to query.
		for client, blockIDs := range clients {
			touchedStores[client.RemoteAddress()] = struct{}{}

			for _, blockID := range blockIDs {
				attemptedBlocks[blockID] = append(attemptedBlocks[blockID], client.RemoteAddress())
			}
		}

		// Ensure all expected blocks have been queried (during all tries done so far).
		missingBlocks := q.consistency.Check(knownBlocks, knownDeletionMarks, resQueriedBlocks)
		if len(missingBlocks) == 0 {
			q.metrics.storesHit.Observe(float64(len(touchedStores)))
			q.metrics.refetches.Observe(float64(attempt - 1))

			return nil
		}

		level.Debug(logger).Log("msg", "consistency check failed", "attempt", attempt, "missing blocks", strings.Join(convertULIDsToString(missingBlocks), " "))

		// The next attempt should just query the missing blocks.
		remainingBlocks = missingBlocks
	}

	// We've not been able to query all expected blocks after all retries.
	level.Warn(util_log.WithContext(ctx, logger)).Log("msg", "failed consistency check", "err", err)
	return newStoreConsistencyCheckFailedError(remainingBlocks)
}

func newStoreConsistencyCheckFailedError(remainingBlocks []ulid.ULID) error {
	return fmt.Errorf("%v. The failed blocks are: %s", globalerror.StoreConsistencyCheckFailed.Message("failed to fetch some blocks"), strings.Join(convertULIDsToString(remainingBlocks), " "))
}

// filterBlocksByShard removes blocks that can be safely ignored when using query sharding.
// We know that block can be safely ignored, if it was compacted using split-and-merge
// compactor, and it has a valid compactor shard ID. We exploit the fact that split-and-merge
// compactor and query-sharding use the same series-sharding algorithm.
//
// This function modifies input slice.
//
// This function also returns number of "incompatible" blocks -- blocks with compactor shard ID,
// but with compactor shard and query shard being incompatible for optimization.
func filterBlocksByShard(blocks bucketindex.Blocks, queryShardIndex, queryShardCount uint64) (_ bucketindex.Blocks, incompatibleBlocks int) {
	for ix := 0; ix < len(blocks); {
		b := blocks[ix]
		if b.CompactorShardID == "" {
			ix++
			continue
		}

		compactorShardIndex, compactorShardCount, err := sharding.ParseShardIDLabelValue(b.CompactorShardID)
		if err != nil {
			// Cannot parse compactor shardID, we must query this block.
			ix++
			continue
		}

		res, divisible := canBlockWithCompactorShardIndexContainQueryShard(queryShardIndex, queryShardCount, compactorShardIndex, compactorShardCount)
		if !divisible {
			incompatibleBlocks++
		}

		if res {
			ix++
			continue
		}

		// Series shard is NOT included in this block, we can remove this block.
		blocks = append(blocks[:ix], blocks[ix+1:]...)
	}

	return blocks, incompatibleBlocks
}

// canBlockWithCompactorShardIndexContainQueryShard returns false if block with
// given compactor shard ID can *definitely NOT* contain series for given query shard.
// Returns true otherwise (we don't know if block *does* contain such series,
// but we cannot rule it out).
//
// In other words, if this function returns false, block with given compactorShardID
// doesn't need to be searched for series from given query shard.
//
// In addition this function also returns whether query and compactor shard counts were
// divisible by each other (one way or the other).
func canBlockWithCompactorShardIndexContainQueryShard(queryShardIndex, queryShardCount, compactorShardIndex,
	compactorShardCount uint64) (result bool, divisibleShardCounts bool) {
	// If queryShardCount = compactorShardCount * K for integer K, then we know that series in queryShardIndex
	// can only be in the block for which (queryShardIndex % compactorShardCount == compactorShardIndex).
	//
	// For example if queryShardCount = 8 and compactorShardCount = 4, then series that should be returned
	// for queryShardIndex 5 can only be in block with compactorShardIndex = 1.
	if queryShardCount >= compactorShardCount && queryShardCount%compactorShardCount == 0 {
		wantedCompactorShardIndex := queryShardIndex % compactorShardCount

		return compactorShardIndex == wantedCompactorShardIndex, true
	}

	// If compactorShardCount = queryShardCount * K for some integer K, then series in queryShardIndex
	// can only be in K blocks for which queryShardIndex % compactorShardCount == compactorShardIndex.
	//
	// For example if queryShardCount = 4, and compactorShardCount = 8, then series that should be returned for
	// queryShardIndex 3 can only be in blocks with compactorShardIndex 3 and 7.
	if compactorShardCount >= queryShardCount && compactorShardCount%queryShardCount == 0 {
		wantedQueryShardIndex := compactorShardIndex % queryShardCount

		return queryShardIndex == wantedQueryShardIndex, true
	}

	return true, false
}

// fetchSeriesFromStores fetches series satisfying convertedMatchers and in the
// time range [minT, maxT) from all store-gateways in clients. Series are fetched
// from the given set of store-gateways concurrently. In successful case, i.e.,
// when all the concurrent fetches terminate with no exception, fetchSeriesFromStores returns:
//  1. a slice of fetched storage.SeriesSet
//  2. a slice of ulid.ULID corresponding to the queried blocks
//  3. storage.Warnings encountered during the operation
//
// In case of a serious error during any of the concurrent executions, the error is returned.
// Errors while creating storepb.SeriesRequest, context cancellation, and unprocessable
// requests to the store-gateways (e.g., if a chunk or series limit is hit) are
// considered serious errors. All other errors are not returned, but they give rise to fetch retrials.
//
// In case of a successful run, fetchSeriesFromStores returns a startStreamingChunks function to start streaming
// chunks for the fetched series iff it was a streaming call for series+chunks. startStreamingChunks must be called
// before iterating on the series.
func (q *blocksStoreQuerier) fetchSeriesFromStores(ctx context.Context, sp *storage.SelectHints, clients map[BlocksStoreClient][]ulid.ULID, minT int64, maxT int64, convertedMatchers []storepb.LabelMatcher) (_ []storage.SeriesSet, _ []ulid.ULID, _ storage.Warnings, startStreamingChunks func(), estimateChunks func() int, _ error) {
	var (
		reqCtx        = grpc_metadata.AppendToOutgoingContext(ctx, storegateway.GrpcContextMetadataTenantID, q.userID)
		g, gCtx       = errgroup.WithContext(reqCtx)
		mtx           = sync.Mutex{}
		seriesSets    = []storage.SeriesSet(nil)
		warnings      = storage.Warnings(nil)
		queriedBlocks = []ulid.ULID(nil)
		spanLog       = spanlogger.FromContext(ctx, q.logger)
		queryLimiter  = limiter.QueryLimiterFromContextWithFallback(ctx)
		reqStats      = stats.FromContext(ctx)
		streamReaders []*storeGatewayStreamReader
		streams       []storegatewaypb.StoreGateway_SeriesClient
	)

	// Concurrently fetch series from all clients.
	for c, blockIDs := range clients {
		// Change variables scope since it will be used in a goroutine.
		c := c
		blockIDs := blockIDs

		g.Go(func() error {
			log, reqCtx := spanlogger.NewWithLogger(reqCtx, spanLog, "blocksStoreQuerier.fetchSeriesFromStores")
			defer log.Span.Finish()
			log.Span.SetTag("store_gateway_address", c.RemoteAddress())

			// See: https://github.com/prometheus/prometheus/pull/8050
			// TODO(goutham): we should ideally be passing the hints down to the storage layer
			// and let the TSDB return us data with no chunks as in prometheus#8050.
			// But this is an acceptable workaround for now.
			skipChunks := sp != nil && sp.Func == "series"

			req, err := createSeriesRequest(minT, maxT, convertedMatchers, skipChunks, blockIDs, q.streamingChunksBatchSize)
			if err != nil {
				return errors.Wrapf(err, "failed to create series request")
			}

			stream, err := c.Series(reqCtx, req)
			if err == nil {
				mtx.Lock()
				streams = append(streams, stream)
				mtx.Unlock()
				err = gCtx.Err()
			}
			if err != nil {
				if shouldStopQueryFunc(err) {
					return err
				}

				level.Warn(log).Log("msg", "failed to fetch series", "remote", c.RemoteAddress(), "err", err)
				return nil
			}

			// A storegateway client will only fill either of mySeries or myStreamingSeries, and not both.
			mySeries := []*storepb.Series(nil)
			myStreamingSeries := []*storepb.StreamingSeries(nil)
			myWarnings := storage.Warnings(nil)
			myQueriedBlocks := []ulid.ULID(nil)
			indexBytesFetched := uint64(0)

			for {
				// Ensure the context hasn't been canceled in the meanwhile (eg. an error occurred
				// in another goroutine).
				if gCtx.Err() != nil {
					return gCtx.Err()
				}

				resp, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					if shouldStopQueryFunc(err) {
						return err
					}

					level.Warn(log).Log("msg", "failed to receive series", "remote", c.RemoteAddress(), "err", err)
					return nil
				}

				// Response may either contain series, streaming series, warning or hints.
				if s := resp.GetSeries(); s != nil {
					mySeries = append(mySeries, s)

					// Add series fingerprint to query limiter; will return error if we are over the limit
					if err := queryLimiter.AddSeries(s.Labels); err != nil {
						return err
					}

					chunksCount, chunksSize := countChunksAndBytes(s)
					if err := queryLimiter.AddChunkBytes(chunksSize); err != nil {
						return err
					}
					if err := queryLimiter.AddChunks(chunksCount); err != nil {
						return err
					}
					if err := queryLimiter.AddEstimatedChunks(chunksCount); err != nil {
						return err
					}
				}

				if w := resp.GetWarning(); w != "" {
					myWarnings = append(myWarnings, errors.New(w))
				}

				if h := resp.GetHints(); h != nil {
					hints := hintspb.SeriesResponseHints{}
					if err := types.UnmarshalAny(h, &hints); err != nil {
						return errors.Wrapf(err, "failed to unmarshal series hints from %s", c.RemoteAddress())
					}

					ids, err := convertBlockHintsToULIDs(hints.QueriedBlocks)
					if err != nil {
						return errors.Wrapf(err, "failed to parse queried block IDs from received hints")
					}

					myQueriedBlocks = append(myQueriedBlocks, ids...)
				}

				if s := resp.GetStats(); s != nil {
					indexBytesFetched += s.FetchedIndexBytes
				}

				if ss := resp.GetStreamingSeries(); ss != nil {
					for _, s := range ss.Series {
						// Add series fingerprint to query limiter; will return error if we are over the limit
						limitErr := queryLimiter.AddSeries(s.Labels)
						if limitErr != nil {
							return validation.LimitError(limitErr.Error())
						}
					}
					myStreamingSeries = append(myStreamingSeries, ss.Series...)
					if ss.IsEndOfSeriesStream {
						// We expect "end of stream" to be sent after the hints and the stats have been sent.
						break
					}
				}
			}

			reqStats.AddFetchedIndexBytes(indexBytesFetched)
			var streamReader *storeGatewayStreamReader
			if len(mySeries) > 0 {
				chunksFetched, chunkBytes := countChunksAndBytes(mySeries...)

				reqStats.AddFetchedSeries(uint64(len(mySeries)))
				reqStats.AddFetchedChunkBytes(uint64(chunkBytes))
				reqStats.AddFetchedChunks(uint64(chunksFetched))

				level.Debug(log).Log("msg", "received series from store-gateway",
					"instance", c.RemoteAddress(),
					"fetched series", len(mySeries),
					"fetched chunk bytes", chunkBytes,
					"fetched chunks", chunksFetched,
					"fetched index bytes", indexBytesFetched,
					"requested blocks", strings.Join(convertULIDsToString(blockIDs), " "),
					"queried blocks", strings.Join(convertULIDsToString(myQueriedBlocks), " "))
			} else if len(myStreamingSeries) > 0 {
				// FetchedChunks and FetchedChunkBytes are added by the SeriesChunksStreamReader.
				reqStats.AddFetchedSeries(uint64(len(myStreamingSeries)))
				streamReader = newStoreGatewayStreamReader(stream, len(myStreamingSeries), queryLimiter, reqStats, q.logger)
				level.Debug(log).Log("msg", "received streaming series from store-gateway",
					"instance", c.RemoteAddress(),
					"fetched series", len(myStreamingSeries),
					"fetched index bytes", indexBytesFetched,
					"requested blocks", strings.Join(convertULIDsToString(blockIDs), " "),
					"queried blocks", strings.Join(convertULIDsToString(myQueriedBlocks), " "))
			}

			// Store the result.
			mtx.Lock()
			if len(mySeries) > 0 {
				seriesSets = append(seriesSets, &blockQuerierSeriesSet{series: mySeries})
			} else if len(myStreamingSeries) > 0 {
				seriesSets = append(seriesSets, &blockStreamingQuerierSeriesSet{series: myStreamingSeries, streamReader: streamReader})
				streamReaders = append(streamReaders, streamReader)
			}
			warnings = append(warnings, myWarnings...)
			queriedBlocks = append(queriedBlocks, myQueriedBlocks...)
			mtx.Unlock()

			return nil
		})
	}

	// Wait until all client requests complete.
	if err := g.Wait(); err != nil {
		for _, stream := range streams {
			if err := stream.CloseSend(); err != nil {
				level.Warn(q.logger).Log("msg", "closing storegateway client stream failed", "err", err)
			}
		}
		return nil, nil, nil, nil, nil, err
	}

	startStreamingChunks = func() {
		for _, sr := range streamReaders {
			sr.StartBuffering()
		}
	}

	estimateChunks = func() int {
		totalChunks := 0

		for _, sr := range streamReaders {
			totalChunks += sr.EstimateChunkCount()
		}

		return totalChunks
	}

	return seriesSets, queriedBlocks, warnings, startStreamingChunks, estimateChunks, nil
}

func shouldStopQueryFunc(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	if st, ok := status.FromError(errors.Cause(err)); ok {
		if int(st.Code()) == http.StatusUnprocessableEntity {
			return true
		}
	}

	return false
}

func (q *blocksStoreQuerier) fetchLabelNamesFromStore(
	ctx context.Context,
	clients map[BlocksStoreClient][]ulid.ULID,
	minT int64,
	maxT int64,
	matchers []storepb.LabelMatcher,
) ([][]string, storage.Warnings, []ulid.ULID, error) {
	var (
		reqCtx        = grpc_metadata.AppendToOutgoingContext(ctx, storegateway.GrpcContextMetadataTenantID, q.userID)
		g, gCtx       = errgroup.WithContext(reqCtx)
		mtx           = sync.Mutex{}
		nameSets      = [][]string{}
		warnings      = storage.Warnings(nil)
		queriedBlocks = []ulid.ULID(nil)
		spanLog       = spanlogger.FromContext(ctx, q.logger)
	)

	// Concurrently fetch series from all clients.
	for c, blockIDs := range clients {
		// Change variables scope since it will be used in a goroutine.
		c := c
		blockIDs := blockIDs

		g.Go(func() error {
			req, err := createLabelNamesRequest(minT, maxT, blockIDs, matchers)
			if err != nil {
				return errors.Wrapf(err, "failed to create label names request")
			}

			namesResp, err := c.LabelNames(gCtx, req)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}

				level.Warn(spanLog).Log("msg", "failed to fetch label names", "remote", c.RemoteAddress(), "err", err)
				return nil
			}

			myQueriedBlocks := []ulid.ULID(nil)
			if namesResp.Hints != nil {
				hints := hintspb.LabelNamesResponseHints{}
				if err := types.UnmarshalAny(namesResp.Hints, &hints); err != nil {
					return errors.Wrapf(err, "failed to unmarshal label names hints from %s", c.RemoteAddress())
				}

				ids, err := convertBlockHintsToULIDs(hints.QueriedBlocks)
				if err != nil {
					return errors.Wrapf(err, "failed to parse queried block IDs from received hints")
				}

				myQueriedBlocks = ids
			}

			level.Debug(spanLog).Log("msg", "received label names from store-gateway",
				"instance", c,
				"num labels", len(namesResp.Names),
				"requested blocks", strings.Join(convertULIDsToString(blockIDs), " "),
				"queried blocks", strings.Join(convertULIDsToString(myQueriedBlocks), " "))

			// Store the result.
			mtx.Lock()
			nameSets = append(nameSets, namesResp.Names)
			for _, w := range namesResp.Warnings {
				warnings = append(warnings, errors.New(w))
			}
			queriedBlocks = append(queriedBlocks, myQueriedBlocks...)
			mtx.Unlock()

			return nil
		})
	}

	// Wait until all client requests complete.
	if err := g.Wait(); err != nil {
		return nil, nil, nil, err
	}

	return nameSets, warnings, queriedBlocks, nil
}

func (q *blocksStoreQuerier) fetchLabelValuesFromStore(
	ctx context.Context,
	name string,
	clients map[BlocksStoreClient][]ulid.ULID,
	minT int64,
	maxT int64,
	matchers ...*labels.Matcher,
) ([][]string, storage.Warnings, []ulid.ULID, error) {
	var (
		reqCtx        = grpc_metadata.AppendToOutgoingContext(ctx, storegateway.GrpcContextMetadataTenantID, q.userID)
		g, gCtx       = errgroup.WithContext(reqCtx)
		mtx           = sync.Mutex{}
		valueSets     = [][]string{}
		warnings      = storage.Warnings(nil)
		queriedBlocks = []ulid.ULID(nil)
		spanLog       = spanlogger.FromContext(ctx, q.logger)
	)

	// Concurrently fetch series from all clients.
	for c, blockIDs := range clients {
		// Change variables scope since it will be used in a goroutine.
		c := c
		blockIDs := blockIDs

		g.Go(func() error {
			req, err := createLabelValuesRequest(minT, maxT, name, blockIDs, matchers...)
			if err != nil {
				return errors.Wrapf(err, "failed to create label values request")
			}

			valuesResp, err := c.LabelValues(gCtx, req)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				level.Warn(spanLog).Log("msg", "failed to fetch label values", "remote", c.RemoteAddress(), "err", err)
				return nil
			}

			myQueriedBlocks := []ulid.ULID(nil)
			if valuesResp.Hints != nil {
				hints := hintspb.LabelValuesResponseHints{}
				if err := types.UnmarshalAny(valuesResp.Hints, &hints); err != nil {
					return errors.Wrapf(err, "failed to unmarshal label values hints from %s", c.RemoteAddress())
				}

				ids, err := convertBlockHintsToULIDs(hints.QueriedBlocks)
				if err != nil {
					return errors.Wrapf(err, "failed to parse queried block IDs from received hints")
				}

				myQueriedBlocks = ids
			}

			level.Debug(spanLog).Log("msg", "received label values from store-gateway",
				"instance", c.RemoteAddress(),
				"num values", len(valuesResp.Values),
				"requested blocks", strings.Join(convertULIDsToString(blockIDs), " "),
				"queried blocks", strings.Join(convertULIDsToString(myQueriedBlocks), " "))

			// Values returned need not be sorted, but we need them to be sorted so we can merge.
			slices.Sort(valuesResp.Values)

			// Store the result.
			mtx.Lock()
			valueSets = append(valueSets, valuesResp.Values)
			for _, w := range valuesResp.Warnings {
				warnings = append(warnings, errors.New(w))
			}
			queriedBlocks = append(queriedBlocks, myQueriedBlocks...)
			mtx.Unlock()

			return nil
		})
	}

	// Wait until all client requests complete.
	if err := g.Wait(); err != nil {
		return nil, nil, nil, err
	}

	return valueSets, warnings, queriedBlocks, nil
}

func createSeriesRequest(minT, maxT int64, matchers []storepb.LabelMatcher, skipChunks bool, blockIDs []ulid.ULID, streamingBatchSize uint64) (*storepb.SeriesRequest, error) {
	// Selectively query only specific blocks.
	hints := &hintspb.SeriesRequestHints{
		BlockMatchers: []storepb.LabelMatcher{
			{
				Type:  storepb.LabelMatcher_RE,
				Name:  block.BlockIDLabel,
				Value: strings.Join(convertULIDsToString(blockIDs), "|"),
			},
		},
	}

	anyHints, err := types.MarshalAny(hints)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal series request hints")
	}

	if skipChunks {
		// We don't do the streaming call if we are not requesting the chunks.
		streamingBatchSize = 0
	}
	return &storepb.SeriesRequest{
		MinTime:                  minT,
		MaxTime:                  maxT,
		Matchers:                 matchers,
		Hints:                    anyHints,
		SkipChunks:               skipChunks,
		StreamingChunksBatchSize: streamingBatchSize,
	}, nil
}

func createLabelNamesRequest(minT, maxT int64, blockIDs []ulid.ULID, matchers []storepb.LabelMatcher) (*storepb.LabelNamesRequest, error) {
	req := &storepb.LabelNamesRequest{
		Start:    minT,
		End:      maxT,
		Matchers: matchers,
	}

	// Selectively query only specific blocks.
	hints := &hintspb.LabelNamesRequestHints{
		BlockMatchers: []storepb.LabelMatcher{
			{
				Type:  storepb.LabelMatcher_RE,
				Name:  block.BlockIDLabel,
				Value: strings.Join(convertULIDsToString(blockIDs), "|"),
			},
		},
	}

	anyHints, err := types.MarshalAny(hints)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal label names request hints")
	}

	req.Hints = anyHints

	return req, nil
}

func createLabelValuesRequest(minT, maxT int64, label string, blockIDs []ulid.ULID, matchers ...*labels.Matcher) (*storepb.LabelValuesRequest, error) {
	req := &storepb.LabelValuesRequest{
		Start:    minT,
		End:      maxT,
		Label:    label,
		Matchers: convertMatchersToLabelMatcher(matchers),
	}

	// Selectively query only specific blocks.
	hints := &hintspb.LabelValuesRequestHints{
		BlockMatchers: []storepb.LabelMatcher{
			{
				Type:  storepb.LabelMatcher_RE,
				Name:  block.BlockIDLabel,
				Value: strings.Join(convertULIDsToString(blockIDs), "|"),
			},
		},
	}

	anyHints, err := types.MarshalAny(hints)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal label values request hints")
	}

	req.Hints = anyHints

	return req, nil
}

func convertULIDsToString(ids []ulid.ULID) []string {
	res := make([]string, len(ids))
	for idx, id := range ids {
		res[idx] = id.String()
	}
	return res
}

func convertBlockHintsToULIDs(hints []hintspb.Block) ([]ulid.ULID, error) {
	res := make([]ulid.ULID, len(hints))

	for idx, hint := range hints {
		blockID, err := ulid.Parse(hint.Id)
		if err != nil {
			return nil, err
		}

		res[idx] = blockID
	}

	return res, nil
}

// countChunksAndBytes returns the number of chunks and size of the chunks making up the provided series in bytes
func countChunksAndBytes(series ...*storepb.Series) (chunks, bytes int) {
	for _, s := range series {
		chunks += len(s.Chunks)
		for _, c := range s.Chunks {
			bytes += c.Size()
		}
	}

	return chunks, bytes
}
