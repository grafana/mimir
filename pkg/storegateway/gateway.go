// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storegateway/gateway.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storegateway

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tracing"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/activitytracker"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	syncReasonInitial    = "initial"
	syncReasonPeriodic   = "periodic"
	syncReasonRingChange = "ring-change"

	// ringAutoForgetUnhealthyPeriods is how many consecutive timeout periods an unhealthy instance
	// in the ring will be automatically removed.
	ringAutoForgetUnhealthyPeriods = 10

	// ringNumTokensDefault is the number of tokens registered in the ring by each store-gateway
	// instance for testing purposes.
	ringNumTokensDefault = 512
)

var (
	// Validation errors.
	errInvalidTenantShardSize = errors.New("invalid tenant shard size, the value must be greater or equal to 0")
)

// Config holds the store gateway config.
type Config struct {
	ShardingRing       RingConfig               `yaml:"sharding_ring" doc:"description=The hash ring configuration."`
	DynamicReplication DynamicReplicationConfig `yaml:"dynamic_replication" doc:"description=Experimental dynamic replication configuration." category:"experimental"`

	EnabledTenants  flagext.StringSliceCSV `yaml:"enabled_tenants" category:"advanced"`
	DisabledTenants flagext.StringSliceCSV `yaml:"disabled_tenants" category:"advanced"`
}

// RegisterFlags registers the Config flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	cfg.ShardingRing.RegisterFlags(f, logger)
	cfg.DynamicReplication.RegisterFlagsWithPrefix(f, "store-gateway.")

	f.Var(&cfg.EnabledTenants, "store-gateway.enabled-tenants", "Comma separated list of tenants that can be loaded by the store-gateway. If specified, only blocks for these tenants will be loaded by the store-gateway, otherwise all tenants can be loaded. Subject to sharding.")
	f.Var(&cfg.DisabledTenants, "store-gateway.disabled-tenants", "Comma separated list of tenants that cannot be loaded by the store-gateway. If specified, and the store-gateway would normally load a given tenant for (via -store-gateway.enabled-tenants or sharding), it will be ignored instead.")
}

// Validate the Config.
func (cfg *Config) Validate(limits validation.Limits) error {
	if limits.StoreGatewayTenantShardSize < 0 {
		return errInvalidTenantShardSize
	}

	if err := cfg.DynamicReplication.Validate(); err != nil {
		return err
	}

	return nil
}

// StoreGateway is the Mimir service responsible to expose an API over the bucket
// where blocks are stored, supporting blocks sharding and replication across a pool
// of store gateway instances (optional).
type StoreGateway struct {
	services.Service

	gatewayCfg Config
	storageCfg mimir_tsdb.BlocksStorageConfig
	logger     log.Logger
	stores     *BucketStores
	tracker    *activitytracker.ActivityTracker

	// Ring used for sharding blocks.
	ringLifecycler *ring.BasicLifecycler
	ring           *ring.Ring

	// Subservices manager (ring, lifecycler)
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher

	bucketSync *prometheus.CounterVec
	// Shutdown marker for store-gateway scale down
	shutdownMarker prometheus.Gauge
}

func NewStoreGateway(gatewayCfg Config, storageCfg mimir_tsdb.BlocksStorageConfig, limits *validation.Overrides, logger log.Logger, reg prometheus.Registerer, tracker *activitytracker.ActivityTracker) (*StoreGateway, error) {
	var ringStore kv.Client

	bucketClient, err := createBucketClient(storageCfg, logger, reg)
	if err != nil {
		return nil, err
	}

	ringStore, err = kv.NewClient(
		gatewayCfg.ShardingRing.KVStore,
		ring.GetCodec(),
		kv.RegistererWithKVName(prometheus.WrapRegistererWithPrefix("cortex_", reg), "store-gateway"),
		logger,
	)
	if err != nil {
		return nil, errors.Wrap(err, "create KV store client")
	}

	return newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, limits, logger, reg, tracker)
}

func newStoreGateway(gatewayCfg Config, storageCfg mimir_tsdb.BlocksStorageConfig, bucketClient objstore.Bucket, ringStore kv.Client, limits *validation.Overrides, logger log.Logger, reg prometheus.Registerer, tracker *activitytracker.ActivityTracker) (*StoreGateway, error) {
	var err error

	g := &StoreGateway{
		gatewayCfg: gatewayCfg,
		storageCfg: storageCfg,
		logger:     logger,
		tracker:    tracker,
		bucketSync: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_storegateway_bucket_sync_total",
			Help: "Total number of times the bucket sync operation triggered.",
		}, []string{"reason"}),
		shutdownMarker: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_storegateway_prepare_shutdown_requested",
			Help: "If the store-gateway has been requested to prepare for shutdown via endpoint or marker file.",
		}),
	}

	// Init metrics.
	g.bucketSync.WithLabelValues(syncReasonInitial)
	g.bucketSync.WithLabelValues(syncReasonPeriodic)
	g.bucketSync.WithLabelValues(syncReasonRingChange)

	// Init sharding strategy.
	var shardingStrategy ShardingStrategy

	lifecyclerCfg, err := gatewayCfg.ShardingRing.ToLifecyclerConfig(logger)
	if err != nil {
		return nil, errors.Wrap(err, "invalid ring lifecycler config")
	}

	// Define lifecycler delegates in reverse order (last to be called defined first because they're
	// chained via "next delegate").
	delegate := ring.BasicLifecyclerDelegate(ring.NewInstanceRegisterDelegate(ring.JOINING, lifecyclerCfg.NumTokens))
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, logger)
	delegate = ring.NewTokensPersistencyDelegate(gatewayCfg.ShardingRing.TokensFilePath, ring.JOINING, delegate, logger)
	if gatewayCfg.ShardingRing.AutoForgetEnabled {
		delegate = ring.NewAutoForgetDelegate(ringAutoForgetUnhealthyPeriods*gatewayCfg.ShardingRing.HeartbeatTimeout, delegate, logger)
	}

	g.ringLifecycler, err = ring.NewBasicLifecycler(lifecyclerCfg, RingNameForServer, RingKey, ringStore, delegate, logger, prometheus.WrapRegistererWithPrefix("cortex_", reg))
	if err != nil {
		return nil, errors.Wrap(err, "create ring lifecycler")
	}

	ringCfg := gatewayCfg.ShardingRing.ToRingConfig()
	g.ring, err = ring.NewWithStoreClientAndStrategy(ringCfg, RingNameForServer, RingKey, ringStore, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), prometheus.WrapRegistererWithPrefix("cortex_", reg), logger)
	if err != nil {
		return nil, errors.Wrap(err, "create ring client")
	}

	var dynamicReplication DynamicReplication = NewNopDynamicReplication()
	if gatewayCfg.DynamicReplication.Enabled {
		dynamicReplication = NewMaxTimeDynamicReplication(
			gatewayCfg.DynamicReplication.MaxTimeThreshold,
			// Exclude blocks which have recently become eligible for expanded replication, in order to give
			// enough time to store-gateways to discover and load them (3 times the sync interval)
			mimir_tsdb.NewBlockDiscoveryDelayMultiplier*storageCfg.BucketStore.SyncInterval,
		)
	}

	shardingStrategy = NewShuffleShardingStrategy(g.ring, lifecyclerCfg.ID, lifecyclerCfg.Addr, dynamicReplication, limits, logger)

	allowedTenants := util.NewAllowedTenants(gatewayCfg.EnabledTenants, gatewayCfg.DisabledTenants)
	if len(gatewayCfg.EnabledTenants) > 0 {
		level.Info(logger).Log("msg", "store-gateway using enabled users", "enabled", gatewayCfg.EnabledTenants)
	}
	if len(gatewayCfg.DisabledTenants) > 0 {
		level.Info(logger).Log("msg", "store-gateway using disabled users", "disabled", gatewayCfg.DisabledTenants)
	}

	g.stores, err = NewBucketStores(storageCfg, shardingStrategy, bucketClient, allowedTenants, limits, logger, prometheus.WrapRegistererWith(prometheus.Labels{"component": "store-gateway"}, reg))
	if err != nil {
		return nil, errors.Wrap(err, "create bucket stores")
	}

	g.Service = services.NewBasicService(g.starting, g.running, g.stopping)

	return g, nil
}

func (g *StoreGateway) starting(ctx context.Context) (err error) {
	// In case this function will return error we want to unregister the instance
	// from the ring. We do it ensuring dependencies are gracefully stopped if they
	// were already started.
	defer func() {
		if err == nil || g.subservices == nil {
			return
		}

		if stopErr := services.StopManagerAndAwaitStopped(context.Background(), g.subservices); stopErr != nil {
			level.Error(g.logger).Log("msg", "failed to gracefully stop store-gateway dependencies", "err", stopErr)
		}
	}()

	err = g.setPrepareShutdownFromShutdownMarker()
	if err != nil {
		return err
	}

	// First of all we register the instance in the ring and wait
	// until the lifecycler successfully started.
	if g.subservices, err = services.NewManager(g.ringLifecycler, g.ring); err != nil {
		return errors.Wrap(err, "unable to start store-gateway dependencies")
	}

	g.subservicesWatcher = services.NewFailureWatcher()
	g.subservicesWatcher.WatchManager(g.subservices)

	if err = services.StartManagerAndAwaitHealthy(ctx, g.subservices); err != nil {
		return errors.Wrap(err, "unable to start store-gateway dependencies")
	}

	// Wait until the ring client detected this instance in the JOINING state to
	// make sure that when we'll run the initial sync we already know  the tokens
	// assigned to this instance.
	level.Info(g.logger).Log("msg", "waiting until store-gateway is JOINING in the ring")
	if err := ring.WaitInstanceState(ctx, g.ring, g.ringLifecycler.GetInstanceID(), ring.JOINING); err != nil {
		return err
	}
	level.Info(g.logger).Log("msg", "store-gateway is JOINING in the ring")

	// In the event of a cluster cold start or scale up of 2+ store-gateway instances at the same
	// time, we may end up in a situation where each new store-gateway instance starts at a slightly
	// different time and thus each one starts with a different state of the ring. It's better
	// to just wait a short time for ring stability.
	if g.gatewayCfg.ShardingRing.WaitStabilityMinDuration > 0 {
		minWaiting := g.gatewayCfg.ShardingRing.WaitStabilityMinDuration
		maxWaiting := g.gatewayCfg.ShardingRing.WaitStabilityMaxDuration

		level.Info(g.logger).Log("msg", "waiting until store-gateway ring topology is stable", "min_waiting", minWaiting.String(), "max_waiting", maxWaiting.String())
		if err := ring.WaitRingTokensStability(ctx, g.ring, BlocksOwnerSync, minWaiting, maxWaiting); err != nil {
			level.Warn(g.logger).Log("msg", "store-gateway ring topology is not stable after the max waiting time, proceeding anyway")
		} else {
			level.Info(g.logger).Log("msg", "store-gateway ring topology is stable")
		}
	}

	// At this point, the instance is registered with some tokens
	// and we can start the bucket stores.
	g.bucketSync.WithLabelValues(syncReasonInitial).Inc()
	// We pass context.Background() because we want to control the shutdown of stores ourselves instead of stopping it immediately after when ctx is cancelled.
	if err = services.StartAndAwaitRunning(context.Background(), g.stores); err != nil {
		return errors.Wrap(err, "starting bucket stores")
	}

	// After starting the store, we should have loaded all blocks
	// assigned to our shard, so we can switch to ACTIVE and start serving
	// requests.
	if err = g.ringLifecycler.ChangeState(ctx, ring.ACTIVE); err != nil {
		return errors.Wrapf(err, "switch instance to %s in the ring", ring.ACTIVE)
	}

	// Wait until the ring client detected this instance in the ACTIVE state to
	// make sure that when we'll run the loop it won't be detected as a ring
	// topology change.
	level.Info(g.logger).Log("msg", "waiting until store-gateway is ACTIVE in the ring")
	if err := ring.WaitInstanceState(ctx, g.ring, g.ringLifecycler.GetInstanceID(), ring.ACTIVE); err != nil {
		return err
	}
	level.Info(g.logger).Log("msg", "store-gateway is ACTIVE in the ring")

	return nil
}

func (g *StoreGateway) running(ctx context.Context) error {
	// Apply a jitter to the sync frequency in order to increase the probability
	// of hitting the shared cache (if any).
	syncTicker := time.NewTicker(util.DurationWithJitter(g.storageCfg.BucketStore.SyncInterval, 0.2))
	defer syncTicker.Stop()

	ringLastState, _ := g.ring.GetAllHealthy(BlocksOwnerSync) // nolint:errcheck
	ringTicker := time.NewTicker(util.DurationWithJitter(g.gatewayCfg.ShardingRing.RingCheckPeriod, 0.2))
	defer ringTicker.Stop()

	for {
		select {
		case <-syncTicker.C:
			g.syncStores(ctx, syncReasonPeriodic)
		case <-ringTicker.C:
			// We ignore the error because in case of error it will return an empty
			// replication set which we use to compare with the previous state.
			currRingState, _ := g.ring.GetAllHealthy(BlocksOwnerSync) // nolint:errcheck

			if ring.HasReplicationSetChanged(ringLastState, currRingState) {
				ringLastState = currRingState
				g.syncStores(ctx, syncReasonRingChange)
			}
		case <-ctx.Done():
			return nil
		case err := <-g.subservicesWatcher.Chan():
			return errors.Wrap(err, "store gateway subservice failed")
		}
	}
}

func (g *StoreGateway) stopping(_ error) error {
	if g.subservices != nil {
		if err := services.StopManagerAndAwaitStopped(context.Background(), g.subservices); err != nil {
			level.Warn(g.logger).Log("msg", "failed to stop store-gateway subservices", "err", err)
		}
	}

	g.unsetPrepareShutdownMarker()

	if err := services.StopAndAwaitTerminated(context.Background(), g.stores); err != nil {
		level.Warn(g.logger).Log("msg", "failed to stop store-gateway stores", "err", err)
	}
	return nil
}

func (g *StoreGateway) syncStores(ctx context.Context, reason string) {
	level.Info(g.logger).Log("msg", "synchronizing TSDB blocks for all users", "reason", reason)
	g.bucketSync.WithLabelValues(reason).Inc()

	if err := g.stores.SyncBlocks(ctx); err != nil {
		level.Warn(g.logger).Log("msg", "failed to synchronize TSDB blocks", "reason", reason, "err", err)
	} else {
		level.Info(g.logger).Log("msg", "successfully synchronized TSDB blocks for all users", "reason", reason)
	}
}

// Series implements the storegatewaypb.StoreGatewayServer interface.
func (g *StoreGateway) Series(req *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer) error {
	ix := g.tracker.Insert(func() string {
		return requestActivity(srv.Context(), "StoreGateway/Series", req)
	})
	defer g.tracker.Delete(ix)

	return g.stores.Series(req, srv)
}

// LabelNames implements the storegatewaypb.StoreGatewayServer interface.
func (g *StoreGateway) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	ix := g.tracker.Insert(func() string {
		return requestActivity(ctx, "StoreGateway/LabelNames", req)
	})
	defer g.tracker.Delete(ix)

	return g.stores.LabelNames(ctx, req)
}

// LabelValues implements the storegatewaypb.StoreGatewayServer interface.
func (g *StoreGateway) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	ix := g.tracker.Insert(func() string {
		return requestActivity(ctx, "StoreGateway/LabelValues", req)
	})
	defer g.tracker.Delete(ix)

	return g.stores.LabelValues(ctx, req)
}

func requestActivity(ctx context.Context, name string, req interface{}) string {
	user := getUserIDFromGRPCContext(ctx)
	traceID, _ := tracing.ExtractSampledTraceID(ctx)
	return fmt.Sprintf("%s: user=%q trace=%q request=%v", name, user, traceID, req)
}

func createBucketClient(cfg mimir_tsdb.BlocksStorageConfig, logger log.Logger, reg prometheus.Registerer) (objstore.Bucket, error) {
	bucketClient, err := bucket.NewClient(context.Background(), cfg.Bucket, "store-gateway", logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "create bucket client")
	}

	return bucketClient, nil
}
