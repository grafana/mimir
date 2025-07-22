// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storegateway/bucket_stores.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storegateway

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/gate"
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

type ParquetBucketStores struct {
	services.Service

	cfg               tsdb.BlocksStorageConfig
	limits            *validation.Overrides // nolint:unused
	allowedTenants    *util.AllowList
	shardingStrategy  ShardingStrategy // nolint:unused
	syncBackoffConfig backoff.Config   // nolint:unused

	bkt objstore.Bucket
	// Metrics specific to bucket store operations
	bucketStoreMetrics *ParquetBucketStoreMetrics
	metaFetcherMetrics *MetadataFetcherMetrics // nolint:unused

	// Gate used to limit query concurrency across all tenants.
	queryGate gate.Gate

	// Gate used to limit concurrency on loading index-headers across all tenants.
	lazyLoadingGate gate.Gate // nolint:unused

	// Keeps a bucket store for each tenant.
	storesMu sync.RWMutex
	stores   map[string]*ParquetBucketStore

	// Block sync metrics.
	syncTimes         prometheus.Histogram // nolint:unused
	syncLastSuccess   prometheus.Gauge     // nolint:unused
	tenantsDiscovered prometheus.Gauge     // nolint:unused
	tenantsSynced     prometheus.Gauge     // nolint:unused
	blocksLoaded      *prometheus.Desc     // nolint:unused

	logger log.Logger
	reg    prometheus.Registerer
}

// NewParquetBucketStores initializes a Parquet implementation of the Stores interface.
func NewParquetBucketStores(
	cfg tsdb.BlocksStorageConfig,
	limits *validation.Overrides,
	allowedTenants *util.AllowList,
	shardingStrategy ShardingStrategy,
	bkt objstore.Bucket,
	logger log.Logger,
	reg prometheus.Registerer,
) (*ParquetBucketStores, error) {

	chunksCacheClient, err := cache.CreateClient("parquet-chunks-cache", cfg.BucketStore.ChunksCache.BackendConfig, logger, prometheus.WrapRegistererWithPrefix("thanos_", reg))
	if err != nil {
		return nil, errors.Wrapf(err, "parquet-chunks-cache")
	}

	cachingBucket, err := tsdb.CreateCachingBucket(chunksCacheClient, cfg.BucketStore.ChunksCache, cfg.BucketStore.MetadataCache, bkt, logger, reg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create caching bucket for parquet bucket stores")
	}

	gateReg := prometheus.WrapRegistererWithPrefix("cortex_bucket_stores_", reg)

	// The number of concurrent queries against the tenants BucketStores are limited.
	queryGateReg := prometheus.WrapRegistererWith(prometheus.Labels{"gate": "query"}, gateReg)
	queryGate := gate.NewBlocking(cfg.BucketStore.MaxConcurrent)
	queryGate = gate.NewInstrumented(queryGateReg, cfg.BucketStore.MaxConcurrent, queryGate)
	queryGate = timeoutGate{delegate: queryGate, timeout: cfg.BucketStore.MaxConcurrentQueueTimeout}

	// The number of concurrent index header loads from storegateway are limited.
	lazyLoadingGateReg := prometheus.WrapRegistererWith(prometheus.Labels{"gate": "index_header"}, gateReg)
	lazyLoadingGate := gate.NewNoop()
	lazyLoadingMax := cfg.BucketStore.IndexHeader.LazyLoadingConcurrency
	if lazyLoadingMax != 0 {
		lazyLoadingGate = gate.NewBlocking(cfg.BucketStore.IndexHeader.LazyLoadingConcurrency)
		lazyLoadingGate = gate.NewInstrumented(lazyLoadingGateReg, cfg.BucketStore.IndexHeader.LazyLoadingConcurrency, lazyLoadingGate)
		lazyLoadingGate = timeoutGate{delegate: lazyLoadingGate, timeout: cfg.BucketStore.IndexHeader.LazyLoadingConcurrencyQueueTimeout}
	}

	stores := &ParquetBucketStores{
		cfg:              cfg,
		limits:           limits,
		allowedTenants:   allowedTenants,
		shardingStrategy: shardingStrategy,
		syncBackoffConfig: backoff.Config{
			MinBackoff: 1 * time.Second,
			MaxBackoff: 10 * time.Second,
			MaxRetries: 3,
		},

		bkt:    cachingBucket,
		stores: map[string]*ParquetBucketStore{},

		bucketStoreMetrics: NewParquetBucketStoreMetrics(reg),
		metaFetcherMetrics: NewMetadataFetcherMetrics(logger),
		queryGate:          queryGate,
		lazyLoadingGate:    lazyLoadingGate,

		logger: logger,
		reg:    reg,
	}

	// Register metrics.
	stores.syncTimes = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:    "cortex_parquet_bucket_stores_blocks_sync_seconds",
		Help:    "The total time it takes to perform a sync stores",
		Buckets: []float64{0.1, 1, 10, 30, 60, 120, 300, 600, 900},
	})
	stores.syncLastSuccess = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_parquet_bucket_stores_blocks_last_successful_sync_timestamp_seconds",
		Help: "Unix timestamp of the last successful blocks sync.",
	})
	stores.tenantsDiscovered = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_parquet_bucket_stores_tenants_discovered",
		Help: "Number of tenants discovered in the bucket.",
	})
	stores.tenantsSynced = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_parquet_bucket_stores_tenants_synced",
		Help: "Number of tenants synced.",
	})
	stores.blocksLoaded = prometheus.NewDesc(
		"cortex_parquet_bucket_store_blocks_loaded",
		"Number of currently loaded blocks.",
		nil, nil,
	)

	if reg != nil {
		reg.MustRegister(stores.metaFetcherMetrics)
		reg.MustRegister(stores)
	}
	stores.Service = services.NewIdleService(stores.initialSync, stores.stopBucketStores)

	return stores, nil
}

// Series implements the storegatewaypb.StoreGatewayServer interface, making a series request to the underlying user bucket store.
func (ss *ParquetBucketStores) Series(req *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer) error {
	spanLog, spanCtx := spanlogger.New(srv.Context(), ss.logger, tracer, "ParquetBucketStores.Series")
	defer spanLog.Finish()

	userID := getUserIDFromGRPCContext(spanCtx)
	if userID == "" {
		return fmt.Errorf("no userID")
	}

	store := ss.getStore(userID)
	if store == nil {
		return nil
	}

	return store.Series(req, spanSeriesServer{
		StoreGateway_SeriesServer: srv,
		ctx:                       spanCtx,
	})
}

// LabelNames implements the storegatewaypb.StoreGatewayServer interface.
func (ss *ParquetBucketStores) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	spanLog, spanCtx := spanlogger.New(ctx, ss.logger, tracer, "ParquetBucketStores.LabelNames")
	defer spanLog.Finish()

	userID := getUserIDFromGRPCContext(spanCtx)
	if userID == "" {
		return nil, fmt.Errorf("no userID")
	}

	store := ss.getStore(userID)
	if store == nil {
		return &storepb.LabelNamesResponse{}, nil
	}

	return store.LabelNames(ctx, req)
}

// LabelValues implements the storegatewaypb.StoreGatewayServer interface.
func (ss *ParquetBucketStores) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	spanLog, spanCtx := spanlogger.New(ctx, ss.logger, tracer, "ParquetBucketStores.LabelValues")
	defer spanLog.Finish()

	userID := getUserIDFromGRPCContext(spanCtx)
	if userID == "" {
		return nil, fmt.Errorf("no userID")
	}

	store := ss.getStore(userID)
	if store == nil {
		return &storepb.LabelValuesResponse{}, nil
	}

	return store.LabelValues(ctx, req)
}

func (ss *ParquetBucketStores) stopBucketStores(error) error {
	ss.storesMu.Lock()
	defer ss.storesMu.Unlock()
	errs := multierror.New()
	for userID, bs := range ss.stores {
		err := services.StopAndAwaitTerminated(context.Background(), bs)
		if err != nil {
			errs.Add(fmt.Errorf("closing bucket store for user %s: %w", userID, err))
		}
	}
	return errs.Err()
}

// initialSync does an initial synchronization of blocks for all users.
func (ss *ParquetBucketStores) initialSync(ctx context.Context) error {
	level.Info(ss.logger).Log("msg", "synchronizing TSDB blocks for all users")

	if err := ss.syncUsersBlocksWithRetries(ctx, func(ctx context.Context, store *ParquetBucketStore) error {
		return store.InitialSync(ctx)
	}); err != nil {
		level.Warn(ss.logger).Log("msg", "failed to synchronize TSDB blocks", "err", err)
		return fmt.Errorf("initial synchronisation with bucket: %w", err)
	}

	level.Info(ss.logger).Log("msg", "successfully synchronized TSDB blocks for all users")
	return nil
}

// SyncBlocks synchronizes the stores state with the Bucket store for every user.
func (ss *ParquetBucketStores) SyncBlocks(ctx context.Context) error {
	return ss.syncUsersBlocksWithRetries(ctx, func(ctx context.Context, store *ParquetBucketStore) error {
		return store.SyncBlocks(ctx)
	})
}

func (ss *ParquetBucketStores) syncUsersBlocksWithRetries(ctx context.Context, f func(context.Context, *ParquetBucketStore) error) error {
	retries := backoff.New(ctx, ss.syncBackoffConfig)

	var lastErr error
	for retries.Ongoing() {
		userIDs, err := ss.ownedUsers(ctx)
		if err != nil {
			retries.Wait()
			continue
		}
		lastErr = ss.syncUsersBlocks(ctx, userIDs, f)
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

func (ss *ParquetBucketStores) ownedUsers(ctx context.Context) ([]string, error) {
	userIDs, err := ss.scanUsers(ctx)
	if err != nil {
		return nil, err
	}
	ss.tenantsDiscovered.Set(float64(len(userIDs)))

	ownedUserIDs, err := ss.shardingStrategy.FilterUsers(ctx, userIDs)
	if err != nil {
		return nil, errors.Wrap(err, "unable to check tenants owned by this store-gateway instance")
	}

	return ownedUserIDs, nil
}

func (ss *ParquetBucketStores) syncUsersBlocks(ctx context.Context, includeUserIDs []string, f func(context.Context, *ParquetBucketStore) error) (returnErr error) {
	defer func(start time.Time) {
		ss.syncTimes.Observe(time.Since(start).Seconds())
		if returnErr == nil {
			ss.syncLastSuccess.SetToCurrentTime()
		}
	}(time.Now())

	type job struct {
		userID string
		store  *ParquetBucketStore
	}

	wg := &sync.WaitGroup{}
	jobs := make(chan job)
	errs := tsdb_errors.NewMulti()
	errsMx := sync.Mutex{}

	ss.tenantsSynced.Set(float64(len(includeUserIDs)))

	// Create a pool of workers which will synchronize blocks. The pool size
	// is limited in order to avoid to concurrently sync a lot of tenants in
	// a large cluster.
	for i := 0; i < ss.cfg.BucketStore.TenantSyncConcurrency; i++ {
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
	for _, userID := range includeUserIDs {
		bs, err := ss.getOrCreateStore(ctx, userID)
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

	ss.closeBucketStoreAndDeleteLocalFilesForExcludedTenants(includeUserIDs)

	return errs.Err()
}

// scanUsers in the bucket and return the list of found users, respecting any specifically
// enabled or disabled users.
func (ss *ParquetBucketStores) scanUsers(ctx context.Context) ([]string, error) {
	users, err := tsdb.ListUsers(ctx, ss.bkt)
	if err != nil {
		return nil, err
	}

	filtered := make([]string, 0, len(users))
	for _, user := range users {
		if ss.allowedTenants.IsAllowed(user) {
			filtered = append(filtered, user)
		}
	}

	return filtered, nil
}

func (ss *ParquetBucketStores) getStore(userID string) *ParquetBucketStore {
	ss.storesMu.RLock()
	defer ss.storesMu.RUnlock()
	return ss.stores[userID]
}

func (ss *ParquetBucketStores) getOrCreateStore(ctx context.Context, userID string) (*ParquetBucketStore, error) {
	// Check if the store already exists.
	bs := ss.getStore(userID)
	if bs != nil {
		return bs, nil
	}

	ss.storesMu.Lock()
	defer ss.storesMu.Unlock()

	// Check again for the store in the event it was created in-between locks.
	bs = ss.stores[userID]
	if bs != nil {
		return bs, nil
	}

	userLogger := util_log.WithUserID(userID, ss.logger)

	level.Info(userLogger).Log("msg", "creating user bucket store")

	userBkt := bucket.NewUserBucketClient(userID, ss.bkt, ss.limits)
	fetcherReg := prometheus.NewRegistry()

	// The sharding strategy filter MUST be before the ones we create here (order matters).
	filters := []block.MetadataFilter{
		NewShardingMetadataFilterAdapter(userID, ss.shardingStrategy),
		newMinTimeMetaFilter(ss.cfg.BucketStore.IgnoreBlocksWithin),
		// Use our own custom implementation.
		NewIgnoreDeletionMarkFilter(userLogger, userBkt, ss.cfg.BucketStore.IgnoreDeletionMarksInStoreGatewayDelay, ss.cfg.BucketStore.MetaSyncConcurrency),
		// The duplicate filter has been intentionally omitted because it could cause troubles with
		// the consistency check done on the querier. The duplicate filter removes redundant blocks
		// but if the store-gateway removes redundant blocks before the querier discovers them, the
		// consistency check on the querier will fail.
	}
	fetcher := NewBucketIndexMetadataFetcher(
		userID,
		ss.bkt,
		ss.limits,
		ss.logger,
		fetcherReg,
		filters,
	)

	bs, err := NewParquetBucketStore(
		userID,
		ss.syncDirForUser(userID),
		userBkt,
		ss.cfg.BucketStore,
		fetcher,
		ss.queryGate,
		ss.lazyLoadingGate,
		ss.cfg.BucketStore.ParquetLoadIndexToDisk,
		nil,
		NewChunksLimiterFactory(func() uint64 {
			return uint64(ss.limits.MaxChunksPerQuery(userID))
		}),
		NewSeriesLimiterFactory(func() uint64 {
			return uint64(ss.limits.MaxFetchedSeriesPerQuery(userID))
		}),
		ss.bucketStoreMetrics,
		userLogger,
	)

	if err != nil {
		return nil, err
	}
	if err = services.StartAndAwaitRunning(ctx, bs); err != nil {
		return nil, fmt.Errorf("starting bucket store for tenant %s: %w", userID, err)
	}

	ss.stores[userID] = bs
	ss.metaFetcherMetrics.AddUserRegistry(userID, fetcherReg)

	return bs, nil
}

// closeBucketStoreAndDeleteLocalFilesForExcludedTenants closes bucket store and removes local "sync" directories
// for tenants that are not included in the current shard.
func (ss *ParquetBucketStores) closeBucketStoreAndDeleteLocalFilesForExcludedTenants(includedUserIDs []string) {
	files, err := os.ReadDir(ss.cfg.BucketStore.SyncDir)
	if err != nil {
		return
	}

	includedUserIDsMap := util.StringsMap(includedUserIDs)
	for _, f := range files {
		if !f.IsDir() {
			continue
		}

		userID := f.Name()
		if includedUserIDsMap[userID] {
			// Preserve directory for users owned by this shard.
			continue
		}

		err := ss.closeBucketStore(userID)
		switch {
		case errors.Is(err, errBucketStoreNotFound):
			// This is OK, nothing was closed.
		case err == nil:
			level.Info(ss.logger).Log("msg", "closed bucket store for user", "user", userID)
		default:
			level.Warn(ss.logger).Log("msg", "failed to close bucket store for user", "user", userID, "err", err)
		}

		userSyncDir := ss.syncDirForUser(userID)
		err = os.RemoveAll(userSyncDir)
		if err == nil {
			level.Info(ss.logger).Log("msg", "deleted user sync directory", "dir", userSyncDir)
		} else {
			level.Warn(ss.logger).Log("msg", "failed to delete user sync directory", "dir", userSyncDir, "err", err)
		}
	}
}

// closeBucketStore closes bucket store for given user
// and removes it from bucket stores map and metrics.
// If bucket store doesn't exist, returns errBucketStoreNotFound.
// Otherwise returns error from closing the bucket store.
func (ss *ParquetBucketStores) closeBucketStore(userID string) error {
	ss.storesMu.Lock()
	unlockInDefer := true
	defer func() {
		if unlockInDefer {
			ss.storesMu.Unlock()
		}
	}()

	bs := ss.stores[userID]
	if bs == nil {
		return errBucketStoreNotFound
	}

	delete(ss.stores, userID)
	unlockInDefer = false
	ss.storesMu.Unlock()

	ss.metaFetcherMetrics.RemoveUserRegistry(userID)
	return bs.RemoveBlocksAndClose()
}

// countBlocksLoaded returns the total number of blocks loaded, summed for all users.
func (ss *ParquetBucketStores) countBlocksLoaded() int {
	total := 0

	ss.storesMu.RLock()
	defer ss.storesMu.RUnlock()

	for _, store := range ss.stores {
		stats := store.Stats()
		total += stats.BlocksLoadedTotal
	}

	return total
}

func (ss *ParquetBucketStores) Describe(descs chan<- *prometheus.Desc) {
	descs <- ss.blocksLoaded
}

func (ss *ParquetBucketStores) Collect(metrics chan<- prometheus.Metric) {
	total := ss.countBlocksLoaded()
	metrics <- prometheus.MustNewConstMetric(ss.blocksLoaded, prometheus.GaugeValue, float64(total))
}

func (ss *ParquetBucketStores) syncDirForUser(userID string) string {
	return filepath.Join(ss.cfg.BucketStore.SyncDir, userID)
}
