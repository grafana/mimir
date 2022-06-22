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
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/common/tracing"

	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/threadpool"
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
)

var (
	// Validation errors.
	errInvalidTenantShardSize = errors.New("invalid tenant shard size, the value must be greater or equal to 0")
)

// Config holds the store gateway config.
type Config struct {
	ShardingRing RingConfig `yaml:"sharding_ring" doc:"description=The hash ring configuration."`
	// ThreadPoolSize controls the number of OS threads that are dedicated for handling requests
	ThreadPoolSize uint `yaml:"thread_pool_size" category:"experimental"`
}

// RegisterFlags registers the Config flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	cfg.ShardingRing.RegisterFlags(f, logger)

	f.UintVar(&cfg.ThreadPoolSize, "store-gateway.thread-pool-size", uint(0), "Number of OS threads that are dedicated for handling requests. Set to 0 to disable use of dedicated OS threads for handling requests.")
}

// Validate the Config.
func (cfg *Config) Validate(limits validation.Limits) error {
	if limits.StoreGatewayTenantShardSize < 0 {
		return errInvalidTenantShardSize
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
	threadpool *threadpool.Threadpool

	// Ring used for sharding blocks.
	ringLifecycler *ring.BasicLifecycler
	ring           *ring.Ring

	// Subservices manager (ring, lifecycler)
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher

	bucketSync *prometheus.CounterVec
}

func NewStoreGateway(gatewayCfg Config, storageCfg mimir_tsdb.BlocksStorageConfig, limits *validation.Overrides, logLevel logging.Level, logger log.Logger, reg prometheus.Registerer, tracker *activitytracker.ActivityTracker) (*StoreGateway, error) {
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

	return newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, limits, logLevel, logger, reg, tracker)
}

func newStoreGateway(gatewayCfg Config, storageCfg mimir_tsdb.BlocksStorageConfig, bucketClient objstore.Bucket, ringStore kv.Client, limits *validation.Overrides, logLevel logging.Level, logger log.Logger, reg prometheus.Registerer, tracker *activitytracker.ActivityTracker) (*StoreGateway, error) {
	var err error

	g := &StoreGateway{
		gatewayCfg: gatewayCfg,
		storageCfg: storageCfg,
		logger:     logger,
		tracker:    tracker,
		threadpool: threadpool.NewThreadpool(gatewayCfg.ThreadPoolSize, reg),
		bucketSync: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_storegateway_bucket_sync_total",
			Help: "Total number of times the bucket sync operation triggered.",
		}, []string{"reason"}),
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
	delegate := ring.BasicLifecyclerDelegate(ring.NewInstanceRegisterDelegate(ring.JOINING, RingNumTokens))
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, logger)
	delegate = ring.NewTokensPersistencyDelegate(gatewayCfg.ShardingRing.TokensFilePath, ring.JOINING, delegate, logger)
	delegate = ring.NewAutoForgetDelegate(ringAutoForgetUnhealthyPeriods*gatewayCfg.ShardingRing.HeartbeatTimeout, delegate, logger)

	g.ringLifecycler, err = ring.NewBasicLifecycler(lifecyclerCfg, RingNameForServer, RingKey, ringStore, delegate, logger, prometheus.WrapRegistererWithPrefix("cortex_", reg))
	if err != nil {
		return nil, errors.Wrap(err, "create ring lifecycler")
	}

	ringCfg := gatewayCfg.ShardingRing.ToRingConfig()
	g.ring, err = ring.NewWithStoreClientAndStrategy(ringCfg, RingNameForServer, RingKey, ringStore, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), prometheus.WrapRegistererWithPrefix("cortex_", reg), logger)
	if err != nil {
		return nil, errors.Wrap(err, "create ring client")
	}

	shardingStrategy = NewShuffleShardingStrategy(g.ring, lifecyclerCfg.ID, lifecyclerCfg.Addr, limits, logger)

	g.stores, err = NewBucketStores(storageCfg, shardingStrategy, bucketClient, limits, logLevel, logger, extprom.WrapRegistererWith(prometheus.Labels{"component": "store-gateway"}, reg))
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

	// First of all we register the instance in the ring and wait
	// until the lifecycler successfully started.
	if g.subservices, err = services.NewManager(g.ringLifecycler, g.ring, g.threadpool); err != nil {
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

	// At this point, if sharding is enabled, the instance is registered with some tokens
	// and we can run the initial synchronization.
	g.bucketSync.WithLabelValues(syncReasonInitial).Inc()
	if err = g.stores.InitialSync(ctx); err != nil {
		return errors.Wrap(err, "initial blocks synchronization")
	}

	// Now that the initial sync is done, we should have loaded all blocks
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
		return services.StopManagerAndAwaitStopped(context.Background(), g.subservices)
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

func (g *StoreGateway) Series(req *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer) error {
	ix := g.tracker.Insert(func() string {
		return requestActivity(srv.Context(), "StoreGateway/Series", req)
	})
	defer g.tracker.Delete(ix)

	_, err := g.threadpool.Execute(func() (interface{}, error) {
		return nil, g.stores.Series(req, srv)
	})

	return err
}

// LabelNames implements the Storegateway proto service.
func (g *StoreGateway) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	ix := g.tracker.Insert(func() string {
		return requestActivity(ctx, "StoreGateway/LabelNames", req)
	})
	defer g.tracker.Delete(ix)

	res, err := g.threadpool.Execute(func() (interface{}, error) {
		return g.stores.LabelNames(ctx, req)
	})

	if err != nil {
		return nil, err
	}

	return res.(*storepb.LabelNamesResponse), err
}

// LabelValues implements the Storegateway proto service.
func (g *StoreGateway) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	ix := g.tracker.Insert(func() string {
		return requestActivity(ctx, "StoreGateway/LabelValues", req)
	})
	defer g.tracker.Delete(ix)

	res, err := g.threadpool.Execute(func() (interface{}, error) {
		return g.stores.LabelValues(ctx, req)
	})

	if err != nil {
		return nil, err
	}

	return res.(*storepb.LabelValuesResponse), err
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
