// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storegateway/bucket_stores.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storegateway

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/pool"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/logging"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storegateway/indexcache"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/gate"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

// GrpcContextMetadataTenantID is a key for GRPC Metadata used to pass tenant ID to store-gateway process.
// (This is now separate from DeprecatedTenantIDExternalLabel to signify different use case.)
const GrpcContextMetadataTenantID = "__org_id__"

// BucketStores is a multi-tenant wrapper of Thanos BucketStore.
type BucketStores struct {
	logger             log.Logger
	cfg                tsdb.BlocksStorageConfig
	limits             *validation.Overrides
	bucket             objstore.Bucket
	logLevel           logging.Level
	bucketStoreMetrics *BucketStoreMetrics
	metaFetcherMetrics *MetadataFetcherMetrics
	shardingStrategy   ShardingStrategy
	syncBackoffConfig  backoff.Config

	// Index cache shared across all tenants.
	indexCache indexcache.IndexCache

	// Series hash cache shared across all tenants.
	seriesHashCache *hashcache.SeriesHashCache

	// Chunks bytes pool shared across all tenants.
	chunksPool pool.Bytes

	// Partitioner shared across all tenants.
	partitioner Partitioner

	// Gate used to limit query concurrency across all tenants.
	queryGate gate.Gate

	// Keeps a bucket store for each tenant.
	storesMu sync.RWMutex
	stores   map[string]*BucketStore

	// Metrics.
	syncTimes         prometheus.Histogram
	syncLastSuccess   prometheus.Gauge
	tenantsDiscovered prometheus.Gauge
	tenantsSynced     prometheus.Gauge
	blocksLoaded      prometheus.GaugeFunc
}

// NewBucketStores makes a new BucketStores.
func NewBucketStores(cfg tsdb.BlocksStorageConfig, shardingStrategy ShardingStrategy, bucketClient objstore.Bucket, limits *validation.Overrides, logLevel logging.Level, logger log.Logger, reg prometheus.Registerer) (*BucketStores, error) {
	cachingBucket, err := tsdb.CreateCachingBucket(cfg.BucketStore.ChunksCache, cfg.BucketStore.MetadataCache, bucketClient, logger, reg)
	if err != nil {
		return nil, errors.Wrapf(err, "create caching bucket")
	}

	// The number of concurrent queries against the tenants BucketStores are limited.
	queryGateReg := extprom.WrapRegistererWithPrefix("cortex_bucket_stores_", reg)
	var queryGate gate.Gate
	if cfg.BucketStore.MaxConcurrentRejectOverLimit {
		queryGate = gate.NewRejecting(cfg.BucketStore.MaxConcurrent)
	} else {
		queryGate = gate.NewBlocking(cfg.BucketStore.MaxConcurrent)
	}
	queryGate = gate.NewInstrumented(queryGateReg, cfg.BucketStore.MaxConcurrent, queryGate)

	u := &BucketStores{
		logger:             logger,
		cfg:                cfg,
		limits:             limits,
		bucket:             cachingBucket,
		shardingStrategy:   shardingStrategy,
		stores:             map[string]*BucketStore{},
		logLevel:           logLevel,
		bucketStoreMetrics: NewBucketStoreMetrics(reg),
		metaFetcherMetrics: NewMetadataFetcherMetrics(),
		queryGate:          queryGate,
		partitioner:        newGapBasedPartitioner(cfg.BucketStore.PartitionerMaxGapBytes, reg),
		seriesHashCache:    hashcache.NewSeriesHashCache(cfg.BucketStore.SeriesHashCacheMaxBytes),
		syncBackoffConfig: backoff.Config{
			MinBackoff: 1 * time.Second,
			MaxBackoff: 10 * time.Second,
			MaxRetries: 3,
		},
	}

	// Register metrics.
	u.syncTimes = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:    "cortex_bucket_stores_blocks_sync_seconds",
		Help:    "The total time it takes to perform a sync stores",
		Buckets: []float64{0.1, 1, 10, 30, 60, 120, 300, 600, 900},
	})
	u.syncLastSuccess = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_bucket_stores_blocks_last_successful_sync_timestamp_seconds",
		Help: "Unix timestamp of the last successful blocks sync.",
	})
	u.tenantsDiscovered = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_bucket_stores_tenants_discovered",
		Help: "Number of tenants discovered in the bucket.",
	})
	u.tenantsSynced = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_bucket_stores_tenants_synced",
		Help: "Number of tenants synced.",
	})
	u.blocksLoaded = promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_bucket_store_blocks_loaded",
		Help: "Number of currently loaded blocks.",
	}, u.getBlocksLoadedMetric)

	// Init the index cache.
	if u.indexCache, err = tsdb.NewIndexCache(cfg.BucketStore.IndexCache, logger, reg); err != nil {
		return nil, errors.Wrap(err, "create index cache")
	}

	// Init the chunks bytes pool.
	if u.chunksPool, err = newChunkBytesPool(cfg.BucketStore.ChunkPoolMinBucketSizeBytes, cfg.BucketStore.ChunkPoolMaxBucketSizeBytes, cfg.BucketStore.MaxChunkPoolBytes, reg); err != nil {
		return nil, errors.Wrap(err, "create chunks bytes pool")
	}

	if reg != nil {
		reg.MustRegister(u.metaFetcherMetrics)
	}

	return u, nil
}

// InitialSync does an initial synchronization of blocks for all users.
func (u *BucketStores) InitialSync(ctx context.Context) error {
	level.Info(u.logger).Log("msg", "synchronizing TSDB blocks for all users")

	if err := u.syncUsersBlocksWithRetries(ctx, func(ctx context.Context, s *BucketStore) error {
		return s.InitialSync(ctx)
	}); err != nil {
		level.Warn(u.logger).Log("msg", "failed to synchronize TSDB blocks", "err", err)
		return err
	}

	level.Info(u.logger).Log("msg", "successfully synchronized TSDB blocks for all users")
	return nil
}

// SyncBlocks synchronizes the stores state with the Bucket store for every user.
func (u *BucketStores) SyncBlocks(ctx context.Context) error {
	return u.syncUsersBlocksWithRetries(ctx, func(ctx context.Context, s *BucketStore) error {
		return s.SyncBlocks(ctx)
	})
}

func (u *BucketStores) syncUsersBlocksWithRetries(ctx context.Context, f func(context.Context, *BucketStore) error) error {
	retries := backoff.New(ctx, u.syncBackoffConfig)

	var lastErr error
	for retries.Ongoing() {
		lastErr = u.syncUsersBlocks(ctx, f)
		if lastErr == nil {
			return nil
		}

		retries.Wait()
	}

	if lastErr == nil {
		return retries.Err()
	}

	return lastErr
}

func (u *BucketStores) syncUsersBlocks(ctx context.Context, f func(context.Context, *BucketStore) error) (returnErr error) {
	defer func(start time.Time) {
		u.syncTimes.Observe(time.Since(start).Seconds())
		if returnErr == nil {
			u.syncLastSuccess.SetToCurrentTime()
		}
	}(time.Now())

	type job struct {
		userID string
		store  *BucketStore
	}

	wg := &sync.WaitGroup{}
	jobs := make(chan job)
	errs := tsdb_errors.NewMulti()
	errsMx := sync.Mutex{}

	// Scan users in the bucket. In case of error, it may return a subset of users. If we sync a subset of users
	// during a periodic sync, we may end up unloading blocks for users that still belong to this store-gateway
	// so we do prefer to not run the sync at all.
	userIDs, err := u.scanUsers(ctx)
	if err != nil {
		return err
	}

	ownedUserIDs, err := u.shardingStrategy.FilterUsers(ctx, userIDs)
	if err != nil {
		return errors.Wrap(err, "unable to check tenants owned by this store-gateway instance")
	}

	includeUserIDs := make(map[string]struct{}, len(ownedUserIDs))
	for _, userID := range ownedUserIDs {
		includeUserIDs[userID] = struct{}{}
	}

	u.tenantsDiscovered.Set(float64(len(userIDs)))
	u.tenantsSynced.Set(float64(len(includeUserIDs)))

	// Create a pool of workers which will synchronize blocks. The pool size
	// is limited in order to avoid to concurrently sync a lot of tenants in
	// a large cluster.
	for i := 0; i < u.cfg.BucketStore.TenantSyncConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for job := range jobs {
				if err := f(ctx, job.store); err != nil {
					errsMx.Lock()
					errs.Add(errors.Wrapf(err, "failed to synchronize TSDB blocks for user %s", job.userID))
					errsMx.Unlock()
				}
			}
		}()
	}

	// Lazily create a bucket store for each new user found
	// and submit a sync job for each user.
	for userID := range includeUserIDs {
		bs, err := u.getOrCreateStore(userID)
		if err != nil {
			errsMx.Lock()
			errs.Add(err)
			errsMx.Unlock()

			continue
		}

		select {
		case jobs <- job{userID: userID, store: bs}:
			// Nothing to do. Will loop to push more jobs.
		case <-ctx.Done():
			// Wait until all workers have done, so the goroutines leak detector doesn't
			// report any issue. This is expected to be quick, considering the done ctx
			// is used by the worker callback function too.
			close(jobs)
			wg.Wait()

			return ctx.Err()
		}
	}

	// Wait until all workers completed.
	close(jobs)
	wg.Wait()

	u.closeBucketStoreAndDeleteLocalFilesForExcludedTenants(includeUserIDs)

	return errs.Err()
}

// Series makes a series request to the underlying user bucket store.
func (u *BucketStores) Series(req *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	spanLog, spanCtx := spanlogger.NewWithLogger(srv.Context(), u.logger, "BucketStores.Series")
	defer spanLog.Span.Finish()

	userID := getUserIDFromGRPCContext(spanCtx)
	if userID == "" {
		return fmt.Errorf("no userID")
	}

	store := u.getStore(userID)
	if store == nil {
		return nil
	}

	return store.Series(req, spanSeriesServer{
		Store_SeriesServer: srv,
		ctx:                spanCtx,
	})
}

// LabelNames implements the Storegateway proto service.
func (u *BucketStores) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	spanLog, spanCtx := spanlogger.NewWithLogger(ctx, u.logger, "BucketStores.LabelNames")
	defer spanLog.Span.Finish()

	userID := getUserIDFromGRPCContext(spanCtx)
	if userID == "" {
		return nil, fmt.Errorf("no userID")
	}

	store := u.getStore(userID)
	if store == nil {
		return &storepb.LabelNamesResponse{}, nil
	}

	return store.LabelNames(ctx, req)
}

// LabelValues implements the Storegateway proto service.
func (u *BucketStores) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	spanLog, spanCtx := spanlogger.NewWithLogger(ctx, u.logger, "BucketStores.LabelValues")
	defer spanLog.Span.Finish()

	userID := getUserIDFromGRPCContext(spanCtx)
	if userID == "" {
		return nil, fmt.Errorf("no userID")
	}

	store := u.getStore(userID)
	if store == nil {
		return &storepb.LabelValuesResponse{}, nil
	}

	return store.LabelValues(ctx, req)
}

// scanUsers in the bucket and return the list of found users. If an error occurs while
// iterating the bucket, it may return both an error and a subset of the users in the bucket.
func (u *BucketStores) scanUsers(ctx context.Context) ([]string, error) {
	return tsdb.ListUsers(ctx, u.bucket)
}

func (u *BucketStores) getStore(userID string) *BucketStore {
	u.storesMu.RLock()
	defer u.storesMu.RUnlock()
	return u.stores[userID]
}

var (
	errBucketStoreNotFound = errors.New("bucket store not found")
)

// closeBucketStore closes bucket store for given user
// and removes it from bucket stores map and metrics.
// If bucket store doesn't exist, returns errBucketStoreNotFound.
// Otherwise returns error from closing the bucket store.
func (u *BucketStores) closeBucketStore(userID string) error {
	u.storesMu.Lock()
	unlockInDefer := true
	defer func() {
		if unlockInDefer {
			u.storesMu.Unlock()
		}
	}()

	bs := u.stores[userID]
	if bs == nil {
		return errBucketStoreNotFound
	}

	delete(u.stores, userID)
	unlockInDefer = false
	u.storesMu.Unlock()

	u.metaFetcherMetrics.RemoveUserRegistry(userID)
	return bs.RemoveBlocksAndClose()
}

func (u *BucketStores) syncDirForUser(userID string) string {
	return filepath.Join(u.cfg.BucketStore.SyncDir, userID)
}

func (u *BucketStores) getOrCreateStore(userID string) (*BucketStore, error) {
	// Check if the store already exists.
	bs := u.getStore(userID)
	if bs != nil {
		return bs, nil
	}

	u.storesMu.Lock()
	defer u.storesMu.Unlock()

	// Check again for the store in the event it was created in-between locks.
	bs = u.stores[userID]
	if bs != nil {
		return bs, nil
	}

	userLogger := util_log.WithUserID(userID, u.logger)

	level.Info(userLogger).Log("msg", "creating user bucket store")

	userBkt := bucket.NewUserBucketClient(userID, u.bucket, u.limits)
	fetcherReg := prometheus.NewRegistry()

	// The sharding strategy filter MUST be before the ones we create here (order matters).
	filters := []block.MetadataFilter{
		NewShardingMetadataFilterAdapter(userID, u.shardingStrategy),
		block.NewConsistencyDelayMetaFilter(userLogger, u.cfg.BucketStore.ConsistencyDelay, fetcherReg),
		newMinTimeMetaFilter(u.cfg.BucketStore.IgnoreBlocksWithin),
		// Use our own custom implementation.
		NewIgnoreDeletionMarkFilter(userLogger, userBkt, u.cfg.BucketStore.IgnoreDeletionMarksDelay, u.cfg.BucketStore.MetaSyncConcurrency),
		// The duplicate filter has been intentionally omitted because it could cause troubles with
		// the consistency check done on the querier. The duplicate filter removes redundant blocks
		// but if the store-gateway removes redundant blocks before the querier discovers them, the
		// consistency check on the querier will fail.
	}

	// Instantiate a different blocks metadata fetcher based on whether bucket index is enabled or not.
	var fetcher block.MetadataFetcher
	if u.cfg.BucketStore.BucketIndex.Enabled {
		fetcher = NewBucketIndexMetadataFetcher(
			userID,
			u.bucket,
			u.limits,
			u.logger,
			fetcherReg,
			filters,
		)
	} else {
		var err error
		fetcher, err = block.NewMetaFetcher(
			userLogger,
			u.cfg.BucketStore.MetaSyncConcurrency,
			userBkt,
			u.syncDirForUser(userID), // The fetcher stores cached metas in the "meta-syncer/" sub directory
			fetcherReg,
			filters,
		)
		if err != nil {
			return nil, err
		}
	}

	bucketStoreOpts := []BucketStoreOption{
		WithLogger(userLogger),
		WithIndexCache(u.indexCache),
		WithQueryGate(u.queryGate),
		WithChunkPool(u.chunksPool),
	}
	if u.logLevel.String() == "debug" {
		bucketStoreOpts = append(bucketStoreOpts, WithDebugLogging())
	}

	bs, err := NewBucketStore(
		userID,
		userBkt,
		fetcher,
		u.syncDirForUser(userID),
		newChunksLimiterFactory(u.limits, userID),
		NewSeriesLimiterFactory(0), // No series limiter.
		u.partitioner,
		u.cfg.BucketStore.BlockSyncConcurrency,
		u.cfg.BucketStore.PostingOffsetsInMemSampling,
		u.cfg.BucketStore.IndexHeader,
		true, // Enable series hints.
		u.cfg.BucketStore.IndexHeaderLazyLoadingEnabled,
		u.cfg.BucketStore.IndexHeaderLazyLoadingIdleTimeout,
		u.seriesHashCache,
		u.bucketStoreMetrics,
		bucketStoreOpts...,
	)
	if err != nil {
		return nil, err
	}

	u.stores[userID] = bs
	u.metaFetcherMetrics.AddUserRegistry(userID, fetcherReg)

	return bs, nil
}

// closeBucketStoreAndDeleteLocalFilesForExcludedTenants closes bucket store and removes local "sync" directories
// for tenants that are not included in the current shard.
func (u *BucketStores) closeBucketStoreAndDeleteLocalFilesForExcludedTenants(includeUserIDs map[string]struct{}) {
	files, err := os.ReadDir(u.cfg.BucketStore.SyncDir)
	if err != nil {
		return
	}

	for _, f := range files {
		if !f.IsDir() {
			continue
		}

		userID := f.Name()
		if _, included := includeUserIDs[userID]; included {
			// Preserve directory for users owned by this shard.
			continue
		}

		err := u.closeBucketStore(userID)
		switch {
		case errors.Is(err, errBucketStoreNotFound):
			// This is OK, nothing was closed.
		case err == nil:
			level.Info(u.logger).Log("msg", "closed bucket store for user", "user", userID)
		default:
			level.Warn(u.logger).Log("msg", "failed to close bucket store for user", "user", userID, "err", err)
		}

		userSyncDir := u.syncDirForUser(userID)
		err = os.RemoveAll(userSyncDir)
		if err == nil {
			level.Info(u.logger).Log("msg", "deleted user sync directory", "dir", userSyncDir)
		} else {
			level.Warn(u.logger).Log("msg", "failed to delete user sync directory", "dir", userSyncDir, "err", err)
		}
	}
}

// getBlocksLoadedMetric returns the number of blocks currently loaded across all bucket stores.
func (u *BucketStores) getBlocksLoadedMetric() float64 {
	count := 0

	u.storesMu.RLock()
	for _, store := range u.stores {
		count += store.Stats().BlocksLoaded
	}
	u.storesMu.RUnlock()

	return float64(count)
}

func getUserIDFromGRPCContext(ctx context.Context) string {
	meta, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	values := meta.Get(GrpcContextMetadataTenantID)
	if len(values) != 1 {
		return ""
	}

	return values[0]
}

type spanSeriesServer struct {
	storepb.Store_SeriesServer

	ctx context.Context
}

func (s spanSeriesServer) Context() context.Context {
	return s.ctx
}

type chunkLimiter struct {
	limiter *Limiter
}

func (c *chunkLimiter) Reserve(num uint64) error {
	err := c.limiter.Reserve(num)
	if err != nil {
		return httpgrpc.Errorf(http.StatusUnprocessableEntity, err.Error())
	}

	return nil
}

func newChunksLimiterFactory(limits *validation.Overrides, userID string) ChunksLimiterFactory {
	return func(failedCounter prometheus.Counter) ChunksLimiter {
		// Since limit overrides could be live reloaded, we have to get the current user's limit
		// each time a new limiter is instantiated.
		return &chunkLimiter{
			limiter: NewLimiter(uint64(limits.MaxChunksPerQuery(userID)), failedCounter),
		}
	}
}
