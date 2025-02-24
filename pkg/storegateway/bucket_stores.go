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
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"github.com/thanos-io/objstore"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway/indexcache"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

// GrpcContextMetadataTenantID is a key for GRPC Metadata used to pass tenant ID to store-gateway process.
// (This is now separate from DeprecatedTenantIDExternalLabel to signify different use case.)
const GrpcContextMetadataTenantID = "__org_id__"

// BucketStores is a multi-tenant wrapper of Thanos BucketStore.
type BucketStores struct {
	services.Service

	logger             log.Logger
	cfg                tsdb.BlocksStorageConfig
	limits             *validation.Overrides
	bucket             objstore.Bucket
	bucketStoreMetrics *BucketStoreMetrics
	metaFetcherMetrics *MetadataFetcherMetrics
	shardingStrategy   ShardingStrategy
	syncBackoffConfig  backoff.Config

	// Index cache shared across all tenants.
	indexCache indexcache.IndexCache

	// Series hash cache shared across all tenants.
	seriesHashCache *hashcache.SeriesHashCache

	// partitioners shared across all tenants.
	partitioners blockPartitioners

	// Gate used to limit query concurrency across all tenants.
	queryGate gate.Gate

	// Gate used to limit concurrency on loading index-headers across all tenants.
	lazyLoadingGate gate.Gate

	// Keeps a bucket store for each tenant.
	storesMu sync.RWMutex
	stores   map[string]*BucketStore

	// Tenants that are specifically enabled or disabled via configuration
	allowedTenants *util.AllowedTenants

	// Metrics.
	syncTimes         prometheus.Histogram
	syncLastSuccess   prometheus.Gauge
	tenantsDiscovered prometheus.Gauge
	tenantsSynced     prometheus.Gauge
	blocksLoaded      *prometheus.Desc
}

// NewBucketStores makes a new BucketStores. After starting the returned BucketStores
func NewBucketStores(cfg tsdb.BlocksStorageConfig, shardingStrategy ShardingStrategy, bucketClient objstore.Bucket, allowedTenants *util.AllowedTenants, limits *validation.Overrides, logger log.Logger, reg prometheus.Registerer) (*BucketStores, error) {
	chunksCacheClient, err := cache.CreateClient("chunks-cache", cfg.BucketStore.ChunksCache.BackendConfig, logger, prometheus.WrapRegistererWithPrefix("thanos_", reg))
	if err != nil {
		return nil, errors.Wrapf(err, "chunks-cache")
	}

	cachingBucket, err := tsdb.CreateCachingBucket(chunksCacheClient, cfg.BucketStore.ChunksCache, cfg.BucketStore.MetadataCache, bucketClient, logger, reg)
	if err != nil {
		return nil, errors.Wrapf(err, "create caching bucket")
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

	u := &BucketStores{
		logger:             logger,
		cfg:                cfg,
		limits:             limits,
		bucket:             cachingBucket,
		shardingStrategy:   shardingStrategy,
		allowedTenants:     allowedTenants,
		stores:             map[string]*BucketStore{},
		bucketStoreMetrics: NewBucketStoreMetrics(reg),
		metaFetcherMetrics: NewMetadataFetcherMetrics(logger),
		queryGate:          queryGate,
		lazyLoadingGate:    lazyLoadingGate,
		partitioners:       newGapBasedPartitioners(cfg.BucketStore.PartitionerMaxGapBytes, reg),
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
	u.blocksLoaded = prometheus.NewDesc(
		"cortex_bucket_store_blocks_loaded",
		"Number of currently loaded blocks.",
		nil, nil,
	)

	// Init the index cache.
	if u.indexCache, err = tsdb.NewIndexCache(cfg.BucketStore.IndexCache, logger, reg); err != nil {
		return nil, errors.Wrap(err, "create index cache")
	}

	if reg != nil {
		reg.MustRegister(u.metaFetcherMetrics)
		reg.MustRegister(u)
	}
	u.Service = services.NewIdleService(u.initialSync, u.stopBucketStores)

	return u, nil
}

func (u *BucketStores) stopBucketStores(error) error {
	u.storesMu.Lock()
	defer u.storesMu.Unlock()
	errs := multierror.New()
	for userID, bs := range u.stores {
		err := services.StopAndAwaitTerminated(context.Background(), bs)
		if err != nil {
			errs.Add(fmt.Errorf("closing bucket store for user %s: %w", userID, err))
		}
	}
	return errs.Err()
}

// initialSync does an initial synchronization of blocks for all users.
func (u *BucketStores) initialSync(ctx context.Context) error {
	level.Info(u.logger).Log("msg", "synchronizing TSDB blocks for all users")

	if err := u.syncUsersBlocksWithRetries(ctx, func(ctx context.Context, store *BucketStore) error {
		return store.InitialSync(ctx)
	}); err != nil {
		level.Warn(u.logger).Log("msg", "failed to synchronize TSDB blocks", "err", err)
		return fmt.Errorf("initial synchronisation with bucket: %w", err)
	}

	level.Info(u.logger).Log("msg", "successfully synchronized TSDB blocks for all users")
	return nil
}

// SyncBlocks synchronizes the stores state with the Bucket store for every user.
func (u *BucketStores) SyncBlocks(ctx context.Context) error {
	return u.syncUsersBlocksWithRetries(ctx, func(ctx context.Context, store *BucketStore) error {
		return store.SyncBlocks(ctx)
	})
}

func (u *BucketStores) syncUsersBlocksWithRetries(ctx context.Context, f func(context.Context, *BucketStore) error) error {
	retries := backoff.New(ctx, u.syncBackoffConfig)

	var lastErr error
	for retries.Ongoing() {
		userIDs, err := u.ownedUsers(ctx)
		if err != nil {
			retries.Wait()
			continue
		}
		lastErr = u.syncUsersBlocks(ctx, userIDs, f)
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

func (u *BucketStores) ownedUsers(ctx context.Context) ([]string, error) {
	userIDs, err := u.scanUsers(ctx)
	if err != nil {
		return nil, err
	}
	u.tenantsDiscovered.Set(float64(len(userIDs)))

	ownedUserIDs, err := u.shardingStrategy.FilterUsers(ctx, userIDs)
	if err != nil {
		return nil, errors.Wrap(err, "unable to check tenants owned by this store-gateway instance")
	}

	return ownedUserIDs, nil
}

func (u *BucketStores) syncUsersBlocks(ctx context.Context, includeUserIDs []string, f func(context.Context, *BucketStore) error) (returnErr error) {
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
	for _, userID := range includeUserIDs {
		bs, err := u.getOrCreateStore(ctx, userID)
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

// Series implements the storegatewaypb.StoreGatewayServer interface, making a series request to the underlying user bucket store.
func (u *BucketStores) Series(req *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer) error {
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
		StoreGateway_SeriesServer: srv,
		ctx:                       spanCtx,
	})
}

// LabelNames implements the storegatewaypb.StoreGatewayServer interface.
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

// LabelValues implements the storegatewaypb.StoreGatewayServer interface.
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

// scanUsers in the bucket and return the list of found users, respecting any specifically
// enabled or disabled users.
func (u *BucketStores) scanUsers(ctx context.Context) ([]string, error) {
	users, err := tsdb.ListUsers(ctx, u.bucket)
	if err != nil {
		return nil, err
	}

	filtered := make([]string, 0, len(users))
	for _, user := range users {
		if u.allowedTenants.IsAllowed(user) {
			filtered = append(filtered, user)
		}
	}

	return filtered, nil
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

// timeoutGate returns errGateTimeout when the timeout is reached while still waiting for the delegate gate.
// timeoutGate belongs better in dskit. However, at the time of writing dskit supports go 1.20.
// go 1.20 doesn't have context.WithTimeoutCause yet,
// so we choose to implement timeoutGate here instead of implementing context.WithTimeoutCause ourselves in dskit.
// It also allows to keep the span logger in timeoutGate as opposed to in the bucket store.
type timeoutGate struct {
	delegate gate.Gate
	timeout  time.Duration
}

var errGateTimeout = staticError{cause: mimirpb.INSTANCE_LIMIT, msg: "timeout waiting for concurrency gate"}

func (t timeoutGate) Start(ctx context.Context) error {
	if t.timeout == 0 {
		return t.delegate.Start(ctx)
	}

	// Inject our own error so that we can differentiate between a timeout caused by this gate
	// or a timeout in the original request timeout.
	ctx, cancel := context.WithTimeoutCause(ctx, t.timeout, errGateTimeout)
	defer cancel()

	err := t.delegate.Start(ctx)
	// Note that we only return an error for a timeout when the delegate has also returned an
	// error. This ensures that when we get a slot in the delegate, our caller will call Done()
	// and release the slot.
	if err != nil && errors.Is(context.Cause(ctx), errGateTimeout) {
		_ = spanlogger.FromContext(ctx, log.NewNopLogger()).Error(err)
		err = errGateTimeout
	}
	return err
}

func (t timeoutGate) Done() {
	t.delegate.Done()
}

func (u *BucketStores) getOrCreateStore(ctx context.Context, userID string) (*BucketStore, error) {
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
		newMinTimeMetaFilter(u.cfg.BucketStore.IgnoreBlocksWithin),
		// Use our own custom implementation.
		NewIgnoreDeletionMarkFilter(userLogger, userBkt, u.cfg.BucketStore.IgnoreDeletionMarksInStoreGatewayDelay, u.cfg.BucketStore.MetaSyncConcurrency),
		// The duplicate filter has been intentionally omitted because it could cause troubles with
		// the consistency check done on the querier. The duplicate filter removes redundant blocks
		// but if the store-gateway removes redundant blocks before the querier discovers them, the
		// consistency check on the querier will fail.
	}
	fetcher := NewBucketIndexMetadataFetcher(
		userID,
		u.bucket,
		u.limits,
		u.logger,
		fetcherReg,
		filters,
	)
	bucketStoreOpts := []BucketStoreOption{
		WithLogger(userLogger),
		WithIndexCache(u.indexCache),
		WithQueryGate(u.queryGate),
		WithLazyLoadingGate(u.lazyLoadingGate),
	}

	bs, err := NewBucketStore(
		userID,
		userBkt,
		fetcher,
		u.syncDirForUser(userID),
		u.cfg.BucketStore,
		worstCaseFetchedDataStrategy{postingListActualSizeFactor: u.cfg.BucketStore.SeriesFetchPreference},
		NewChunksLimiterFactory(func() uint64 {
			return uint64(u.limits.MaxChunksPerQuery(userID))
		}),
		NewSeriesLimiterFactory(func() uint64 {
			return uint64(u.limits.MaxFetchedSeriesPerQuery(userID))
		}),
		u.partitioners,
		u.seriesHashCache,
		u.bucketStoreMetrics,
		bucketStoreOpts...,
	)
	if err != nil {
		return nil, err
	}
	if err = services.StartAndAwaitRunning(ctx, bs); err != nil {
		return nil, fmt.Errorf("starting bucket store for tenant %s: %w", userID, err)
	}

	u.stores[userID] = bs
	u.metaFetcherMetrics.AddUserRegistry(userID, fetcherReg)

	return bs, nil
}

// closeBucketStoreAndDeleteLocalFilesForExcludedTenants closes bucket store and removes local "sync" directories
// for tenants that are not included in the current shard.
func (u *BucketStores) closeBucketStoreAndDeleteLocalFilesForExcludedTenants(includedUserIDs []string) {
	files, err := os.ReadDir(u.cfg.BucketStore.SyncDir)
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

// countBlocksLoaded returns the total number of blocks loaded, summed for all users.
func (u *BucketStores) countBlocksLoaded() int {
	total := 0

	u.storesMu.RLock()
	defer u.storesMu.RUnlock()

	for _, store := range u.stores {
		stats := store.Stats()
		total += stats.BlocksLoadedTotal
	}

	return total
}

func (u *BucketStores) Describe(descs chan<- *prometheus.Desc) {
	descs <- u.blocksLoaded
}

func (u *BucketStores) Collect(metrics chan<- prometheus.Metric) {
	total := u.countBlocksLoaded()
	metrics <- prometheus.MustNewConstMetric(u.blocksLoaded, prometheus.GaugeValue, float64(total))
}

func getUserIDFromGRPCContext(ctx context.Context) string {
	values := metadata.ValueFromIncomingContext(ctx, GrpcContextMetadataTenantID)
	if len(values) != 1 {
		return ""
	}

	return values[0]
}

type spanSeriesServer struct {
	storegatewaypb.StoreGateway_SeriesServer

	ctx context.Context
}

func (s spanSeriesServer) Context() context.Context {
	return s.ctx
}
