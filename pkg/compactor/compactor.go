// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/compactor/compactor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package compactor

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"

	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

const (
	// CompactorRingKey is the key under which we store the compactors ring in the KVStore.
	CompactorRingKey = "compactor"
)

const (
	blocksMarkedForDeletionName = "cortex_compactor_blocks_marked_for_deletion_total"
	blocksMarkedForDeletionHelp = "Total number of blocks marked for deletion in compactor."
)

var (
	errInvalidBlockRanges                 = "compactor block range periods should be divisible by the previous one, but %s is not divisible by %s"
	errInvalidCompactionOrder             = fmt.Errorf("unsupported compaction order (supported values: %s)", strings.Join(CompactionOrders, ", "))
	errInvalidMaxOpeningBlocksConcurrency = fmt.Errorf("invalid max-opening-blocks-concurrency value, must be positive")
	errInvalidMaxClosingBlocksConcurrency = fmt.Errorf("invalid max-closing-blocks-concurrency value, must be positive")
	errInvalidSymbolFlushersConcurrency   = fmt.Errorf("invalid symbols-flushers-concurrency value, must be positive")
	RingOp                                = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, nil)
)

// BlocksGrouperFactory builds and returns the grouper to use to compact a tenant's blocks.
type BlocksGrouperFactory func(
	ctx context.Context,
	cfg Config,
	cfgProvider ConfigProvider,
	userID string,
	logger log.Logger,
	reg prometheus.Registerer,
) Grouper

// BlocksCompactorFactory builds and returns the compactor and planner to use to compact a tenant's blocks.
type BlocksCompactorFactory func(
	ctx context.Context,
	cfg Config,
	logger log.Logger,
	reg prometheus.Registerer,
) (Compactor, Planner, error)

// Config holds the MultitenantCompactor config.
type Config struct {
	BlockRanges           mimir_tsdb.DurationList `yaml:"block_ranges" category:"advanced"`
	BlockSyncConcurrency  int                     `yaml:"block_sync_concurrency" category:"advanced"`
	MetaSyncConcurrency   int                     `yaml:"meta_sync_concurrency" category:"advanced"`
	ConsistencyDelay      time.Duration           `yaml:"consistency_delay" category:"advanced"`
	DataDir               string                  `yaml:"data_dir"`
	CompactionInterval    time.Duration           `yaml:"compaction_interval" category:"advanced"`
	CompactionRetries     int                     `yaml:"compaction_retries" category:"advanced"`
	CompactionConcurrency int                     `yaml:"compaction_concurrency" category:"advanced"`
	CleanupInterval       time.Duration           `yaml:"cleanup_interval" category:"advanced"`
	CleanupConcurrency    int                     `yaml:"cleanup_concurrency" category:"advanced"`
	DeletionDelay         time.Duration           `yaml:"deletion_delay" category:"advanced"`
	TenantCleanupDelay    time.Duration           `yaml:"tenant_cleanup_delay" category:"advanced"`
	MaxCompactionTime     time.Duration           `yaml:"max_compaction_time" category:"advanced"`

	// Compactor concurrency options
	MaxOpeningBlocksConcurrency int `yaml:"max_opening_blocks_concurrency" category:"advanced"` // Number of goroutines opening blocks before compaction.
	MaxClosingBlocksConcurrency int `yaml:"max_closing_blocks_concurrency" category:"advanced"` // Max number of blocks that can be closed concurrently during split compaction. Note that closing of newly compacted block uses a lot of memory for writing index.
	SymbolsFlushersConcurrency  int `yaml:"symbols_flushers_concurrency" category:"advanced"`   // Number of symbols flushers used when doing split compaction.

	EnabledTenants  flagext.StringSliceCSV `yaml:"enabled_tenants" category:"advanced"`
	DisabledTenants flagext.StringSliceCSV `yaml:"disabled_tenants" category:"advanced"`

	// Compactors sharding.
	ShardingRing RingConfig `yaml:"sharding_ring"`

	CompactionJobsOrder string `yaml:"compaction_jobs_order" category:"advanced"`

	// No need to add options to customize the retry backoff,
	// given the defaults should be fine, but allow to override
	// it in tests.
	retryMinBackoff time.Duration `yaml:"-"`
	retryMaxBackoff time.Duration `yaml:"-"`

	// Allow downstream projects to customise the blocks compactor.
	BlocksGrouperFactory   BlocksGrouperFactory   `yaml:"-"`
	BlocksCompactorFactory BlocksCompactorFactory `yaml:"-"`
}

// RegisterFlags registers the MultitenantCompactor flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	cfg.ShardingRing.RegisterFlags(f, logger)

	cfg.BlockRanges = mimir_tsdb.DurationList{2 * time.Hour, 12 * time.Hour, 24 * time.Hour}
	cfg.retryMinBackoff = 10 * time.Second
	cfg.retryMaxBackoff = time.Minute

	f.Var(&cfg.BlockRanges, "compactor.block-ranges", "List of compaction time ranges.")
	f.DurationVar(&cfg.ConsistencyDelay, "compactor.consistency-delay", 0, "Minimum age of fresh (non-compacted) blocks before they are being processed.")
	f.IntVar(&cfg.BlockSyncConcurrency, "compactor.block-sync-concurrency", 8, "Number of Go routines to use when downloading blocks for compaction and uploading resulting blocks.")
	f.IntVar(&cfg.MetaSyncConcurrency, "compactor.meta-sync-concurrency", 20, "Number of Go routines to use when syncing block meta files from the long term storage.")
	f.StringVar(&cfg.DataDir, "compactor.data-dir", "./data-compactor/", "Directory to temporarily store blocks during compaction. This directory is not required to be persisted between restarts.")
	f.DurationVar(&cfg.CompactionInterval, "compactor.compaction-interval", time.Hour, "The frequency at which the compaction runs")
	f.DurationVar(&cfg.MaxCompactionTime, "compactor.max-compaction-time", time.Hour, "Max time for starting compactions for a single tenant. After this time no new compactions for the tenant are started before next compaction cycle. This can help in multi-tenant environments to avoid single tenant using all compaction time, but also in single-tenant environments to force new discovery of blocks more often. 0 = disabled.")
	f.IntVar(&cfg.CompactionRetries, "compactor.compaction-retries", 3, "How many times to retry a failed compaction within a single compaction run.")
	f.IntVar(&cfg.CompactionConcurrency, "compactor.compaction-concurrency", 1, "Max number of concurrent compactions running.")
	f.DurationVar(&cfg.CleanupInterval, "compactor.cleanup-interval", 15*time.Minute, "How frequently compactor should run blocks cleanup and maintenance, as well as update the bucket index.")
	f.IntVar(&cfg.CleanupConcurrency, "compactor.cleanup-concurrency", 20, "Max number of tenants for which blocks cleanup and maintenance should run concurrently.")
	f.StringVar(&cfg.CompactionJobsOrder, "compactor.compaction-jobs-order", CompactionOrderOldestFirst, fmt.Sprintf("The sorting to use when deciding which compaction jobs should run first for a given tenant. Supported values are: %s.", strings.Join(CompactionOrders, ", ")))
	f.DurationVar(&cfg.DeletionDelay, "compactor.deletion-delay", 12*time.Hour, "Time before a block marked for deletion is deleted from bucket. "+
		"If not 0, blocks will be marked for deletion and compactor component will permanently delete blocks marked for deletion from the bucket. "+
		"If 0, blocks will be deleted straight away. Note that deleting blocks immediately can cause query failures.")
	f.DurationVar(&cfg.TenantCleanupDelay, "compactor.tenant-cleanup-delay", 6*time.Hour, "For tenants marked for deletion, this is time between deleting of last block, and doing final cleanup (marker files, debug files) of the tenant.")
	// compactor concurrency options
	f.IntVar(&cfg.MaxOpeningBlocksConcurrency, "compactor.max-opening-blocks-concurrency", 1, "Number of goroutines opening blocks before compaction.")
	f.IntVar(&cfg.MaxClosingBlocksConcurrency, "compactor.max-closing-blocks-concurrency", 1, "Max number of blocks that can be closed concurrently during split compaction. Note that closing of newly compacted block uses a lot of memory for writing index.")
	f.IntVar(&cfg.SymbolsFlushersConcurrency, "compactor.symbols-flushers-concurrency", 1, "Number of symbols flushers used when doing split compaction.")

	f.Var(&cfg.EnabledTenants, "compactor.enabled-tenants", "Comma separated list of tenants that can be compacted. If specified, only these tenants will be compacted by compactor, otherwise all tenants can be compacted. Subject to sharding.")
	f.Var(&cfg.DisabledTenants, "compactor.disabled-tenants", "Comma separated list of tenants that cannot be compacted by this compactor. If specified, and compactor would normally pick given tenant for compaction (via -compactor.enabled-tenants or sharding), it will be ignored instead.")
}

func (cfg *Config) Validate() error {
	// Each block range period should be divisible by the previous one.
	for i := 1; i < len(cfg.BlockRanges); i++ {
		if cfg.BlockRanges[i]%cfg.BlockRanges[i-1] != 0 {
			return errors.Errorf(errInvalidBlockRanges, cfg.BlockRanges[i].String(), cfg.BlockRanges[i-1].String())
		}
	}

	if cfg.MaxOpeningBlocksConcurrency < 1 {
		return errInvalidMaxOpeningBlocksConcurrency
	}
	if cfg.MaxClosingBlocksConcurrency < 1 {
		return errInvalidMaxClosingBlocksConcurrency
	}
	if cfg.SymbolsFlushersConcurrency < 1 {
		return errInvalidSymbolFlushersConcurrency
	}

	if !util.StringsContain(CompactionOrders, cfg.CompactionJobsOrder) {
		return errInvalidCompactionOrder
	}

	return nil
}

// ConfigProvider defines the per-tenant config provider for the MultitenantCompactor.
type ConfigProvider interface {
	bucket.TenantConfigProvider

	// CompactorBlocksRetentionPeriod returns the retention period for a given user.
	CompactorBlocksRetentionPeriod(user string) time.Duration

	// CompactorSplitAndMergeShards returns the number of shards to use when splitting blocks.
	CompactorSplitAndMergeShards(userID string) int

	// CompactorSplitGroups returns the number of groups that blocks used for splitting should
	// be grouped into. Different groups are then split by different jobs.
	CompactorSplitGroups(userID string) int

	// CompactorTenantShardSize returns number of compactors that this user can use. 0 = all compactors.
	CompactorTenantShardSize(userID string) int

	// CompactorPartialBlockDeletionDelay returns the partial block delay time period for a given user,
	// and whether the configured value was valid. If the value wasn't valid, the returned delay is the default one
	// and the caller is responsible to warn the Mimir operator about it.
	CompactorPartialBlockDeletionDelay(userID string) (delay time.Duration, valid bool)

	// CompactorBlockUploadEnabled returns whether block upload is enabled for a given tenant.
	CompactorBlockUploadEnabled(tenantID string) bool
}

// MultitenantCompactor is a multi-tenant TSDB blocks compactor based on Thanos.
type MultitenantCompactor struct {
	services.Service

	compactorCfg Config
	storageCfg   mimir_tsdb.BlocksStorageConfig
	cfgProvider  ConfigProvider
	logger       log.Logger
	parentLogger log.Logger
	registerer   prometheus.Registerer

	// Functions that creates bucket client, grouper, planner and compactor using the context.
	// Useful for injecting mock objects from tests.
	bucketClientFactory    func(ctx context.Context) (objstore.Bucket, error)
	blocksGrouperFactory   BlocksGrouperFactory
	blocksCompactorFactory BlocksCompactorFactory

	// Blocks cleaner is responsible to hard delete blocks marked for deletion.
	blocksCleaner *BlocksCleaner

	// Underlying compactor and planner used to compact TSDB blocks.
	blocksCompactor Compactor
	blocksPlanner   Planner

	// Client used to run operations on the bucket storing blocks.
	bucketClient objstore.Bucket

	// Ring used for sharding compactions.
	ringLifecycler         *ring.Lifecycler
	ring                   *ring.Ring
	ringSubservices        *services.Manager
	ringSubservicesWatcher *services.FailureWatcher

	shardingStrategy shardingStrategy
	jobsOrder        JobsOrderFunc

	// Metrics.
	compactionRunsStarted          prometheus.Counter
	compactionRunsCompleted        prometheus.Counter
	compactionRunsFailed           prometheus.Counter
	compactionRunsLastSuccess      prometheus.Gauge
	compactionRunDiscoveredTenants prometheus.Gauge
	compactionRunSkippedTenants    prometheus.Gauge
	compactionRunSucceededTenants  prometheus.Gauge
	compactionRunFailedTenants     prometheus.Gauge
	compactionRunInterval          prometheus.Gauge
	blocksMarkedForDeletion        prometheus.Counter

	// Metrics shared across all BucketCompactor instances.
	bucketCompactorMetrics *BucketCompactorMetrics

	// TSDB syncer metrics
	syncerMetrics *aggregatedSyncerMetrics
}

// NewMultitenantCompactor makes a new MultitenantCompactor.
func NewMultitenantCompactor(compactorCfg Config, storageCfg mimir_tsdb.BlocksStorageConfig, cfgProvider ConfigProvider, logger log.Logger, registerer prometheus.Registerer) (*MultitenantCompactor, error) {
	bucketClientFactory := func(ctx context.Context) (objstore.Bucket, error) {
		return bucket.NewClient(ctx, storageCfg.Bucket, "compactor", logger, registerer)
	}

	// Configure the compactor and grouper factories.
	if compactorCfg.BlocksGrouperFactory != nil && compactorCfg.BlocksCompactorFactory != nil {
		// Nothing to do because it was already set by a downstream project.
	} else {
		configureSplitAndMergeCompactor(&compactorCfg)
	}

	blocksGrouperFactory := compactorCfg.BlocksGrouperFactory
	blocksCompactorFactory := compactorCfg.BlocksCompactorFactory

	mimirCompactor, err := newMultitenantCompactor(compactorCfg, storageCfg, cfgProvider, logger, registerer, bucketClientFactory, blocksGrouperFactory, blocksCompactorFactory)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create blocks compactor")
	}

	return mimirCompactor, nil
}

func newMultitenantCompactor(
	compactorCfg Config,
	storageCfg mimir_tsdb.BlocksStorageConfig,
	cfgProvider ConfigProvider,
	logger log.Logger,
	registerer prometheus.Registerer,
	bucketClientFactory func(ctx context.Context) (objstore.Bucket, error),
	blocksGrouperFactory BlocksGrouperFactory,
	blocksCompactorFactory BlocksCompactorFactory,
) (*MultitenantCompactor, error) {
	c := &MultitenantCompactor{
		compactorCfg:           compactorCfg,
		storageCfg:             storageCfg,
		cfgProvider:            cfgProvider,
		parentLogger:           logger,
		logger:                 log.With(logger, "component", "compactor"),
		registerer:             registerer,
		syncerMetrics:          newAggregatedSyncerMetrics(registerer),
		bucketClientFactory:    bucketClientFactory,
		blocksGrouperFactory:   blocksGrouperFactory,
		blocksCompactorFactory: blocksCompactorFactory,

		compactionRunsStarted: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_runs_started_total",
			Help: "Total number of compaction runs started.",
		}),
		compactionRunsCompleted: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_runs_completed_total",
			Help: "Total number of compaction runs successfully completed.",
		}),
		compactionRunsFailed: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_runs_failed_total",
			Help: "Total number of compaction runs failed.",
		}),
		compactionRunsLastSuccess: promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_compactor_last_successful_run_timestamp_seconds",
			Help: "Unix timestamp of the last successful compaction run.",
		}),
		compactionRunDiscoveredTenants: promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_compactor_tenants_discovered",
			Help: "Number of tenants discovered during the current compaction run. Reset to 0 when compactor is idle.",
		}),
		compactionRunSkippedTenants: promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_compactor_tenants_skipped",
			Help: "Number of tenants skipped during the current compaction run. Reset to 0 when compactor is idle.",
		}),
		compactionRunSucceededTenants: promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_compactor_tenants_processing_succeeded",
			Help: "Number of tenants successfully processed during the current compaction run. Reset to 0 when compactor is idle.",
		}),
		compactionRunFailedTenants: promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_compactor_tenants_processing_failed",
			Help: "Number of tenants failed processing during the current compaction run. Reset to 0 when compactor is idle.",
		}),
		compactionRunInterval: promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_compactor_compaction_interval_seconds",
			Help: "The configured interval on which compaction is run in seconds. Useful when compared to the last successful run metric to accurately detect multiple failed compaction runs.",
		}),
		blocksMarkedForDeletion: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name:        blocksMarkedForDeletionName,
			Help:        blocksMarkedForDeletionHelp,
			ConstLabels: prometheus.Labels{"reason": "compaction"},
		}),
	}

	c.bucketCompactorMetrics = NewBucketCompactorMetrics(c.blocksMarkedForDeletion, registerer)

	if len(compactorCfg.EnabledTenants) > 0 {
		level.Info(c.logger).Log("msg", "compactor using enabled users", "enabled", strings.Join(compactorCfg.EnabledTenants, ", "))
	}
	if len(compactorCfg.DisabledTenants) > 0 {
		level.Info(c.logger).Log("msg", "compactor using disabled users", "disabled", strings.Join(compactorCfg.DisabledTenants, ", "))
	}

	c.jobsOrder = GetJobsOrderFunction(compactorCfg.CompactionJobsOrder)
	if c.jobsOrder == nil {
		return nil, errInvalidCompactionOrder
	}

	c.Service = services.NewBasicService(c.starting, c.running, c.stopping)

	// The last successful compaction run metric is exposed as seconds since epoch, so we need to use seconds for this metric.
	c.compactionRunInterval.Set(c.compactorCfg.CompactionInterval.Seconds())

	return c, nil
}

// Start the compactor.
func (c *MultitenantCompactor) starting(ctx context.Context) error {
	var err error

	// Create bucket client.
	c.bucketClient, err = c.bucketClientFactory(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create bucket client")
	}

	// Create blocks compactor dependencies.
	c.blocksCompactor, c.blocksPlanner, err = c.blocksCompactorFactory(ctx, c.compactorCfg, c.logger, c.registerer)
	if err != nil {
		return errors.Wrap(err, "failed to initialize compactor dependencies")
	}

	// Wrap the bucket client to write block deletion marks in the global location too.
	c.bucketClient = bucketindex.BucketWithGlobalMarkers(c.bucketClient)

	// Initialize the compactors ring if sharding is enabled.
	lifecyclerCfg := c.compactorCfg.ShardingRing.ToLifecyclerConfig()
	c.ringLifecycler, err = ring.NewLifecycler(lifecyclerCfg, ring.NewNoopFlushTransferer(), "compactor", CompactorRingKey, false, c.logger, prometheus.WrapRegistererWithPrefix("cortex_", c.registerer))
	if err != nil {
		return errors.Wrap(err, "unable to initialize compactor ring lifecycler")
	}

	c.ring, err = ring.New(lifecyclerCfg.RingConfig, "compactor", CompactorRingKey, c.logger, prometheus.WrapRegistererWithPrefix("cortex_", c.registerer))
	if err != nil {
		return errors.Wrap(err, "unable to initialize compactor ring")
	}

	c.ringSubservices, err = services.NewManager(c.ringLifecycler, c.ring)
	if err == nil {
		c.ringSubservicesWatcher = services.NewFailureWatcher()
		c.ringSubservicesWatcher.WatchManager(c.ringSubservices)

		err = services.StartManagerAndAwaitHealthy(ctx, c.ringSubservices)
	}

	if err != nil {
		return errors.Wrap(err, "unable to start compactor ring dependencies")
	}

	// If sharding is enabled we should wait until this instance is
	// ACTIVE within the ring. This MUST be done before starting the
	// any other component depending on the users scanner, because the
	// users scanner depends on the ring (to check whether an user belongs
	// to this shard or not).
	level.Info(c.logger).Log("msg", "waiting until compactor is ACTIVE in the ring")

	ctxWithTimeout, cancel := context.WithTimeout(ctx, c.compactorCfg.ShardingRing.WaitActiveInstanceTimeout)
	defer cancel()
	if err := ring.WaitInstanceState(ctxWithTimeout, c.ring, c.ringLifecycler.ID, ring.ACTIVE); err != nil {
		level.Error(c.logger).Log("msg", "compactor failed to become ACTIVE in the ring", "err", err)
		return err
	}
	level.Info(c.logger).Log("msg", "compactor is ACTIVE in the ring")

	// In the event of a cluster cold start or scale up of 2+ compactor instances at the same
	// time, we may end up in a situation where each new compactor instance starts at a slightly
	// different time and thus each one starts with a different state of the ring. It's better
	// to just wait a short time for ring stability.
	if c.compactorCfg.ShardingRing.WaitStabilityMinDuration > 0 {
		minWaiting := c.compactorCfg.ShardingRing.WaitStabilityMinDuration
		maxWaiting := c.compactorCfg.ShardingRing.WaitStabilityMaxDuration

		level.Info(c.logger).Log("msg", "waiting until compactor ring topology is stable", "min_waiting", minWaiting.String(), "max_waiting", maxWaiting.String())
		if err := ring.WaitRingStability(ctx, c.ring, RingOp, minWaiting, maxWaiting); err != nil {
			level.Warn(c.logger).Log("msg", "compactor ring topology is not stable after the max waiting time, proceeding anyway")
		} else {
			level.Info(c.logger).Log("msg", "compactor ring topology is stable")
		}
	}

	allowedTenants := util.NewAllowedTenants(c.compactorCfg.EnabledTenants, c.compactorCfg.DisabledTenants)
	c.shardingStrategy = newSplitAndMergeShardingStrategy(allowedTenants, c.ring, c.ringLifecycler, c.cfgProvider)

	// Create the blocks cleaner (service).
	c.blocksCleaner = NewBlocksCleaner(BlocksCleanerConfig{
		DeletionDelay:           c.compactorCfg.DeletionDelay,
		CleanupInterval:         util.DurationWithJitter(c.compactorCfg.CleanupInterval, 0.1),
		CleanupConcurrency:      c.compactorCfg.CleanupConcurrency,
		TenantCleanupDelay:      c.compactorCfg.TenantCleanupDelay,
		DeleteBlocksConcurrency: defaultDeleteBlocksConcurrency,
	}, c.bucketClient, c.shardingStrategy.blocksCleanerOwnUser, c.cfgProvider, c.parentLogger, c.registerer)

	// Start blocks cleaner asynchronously, don't wait until initial cleanup is finished.
	if err := c.blocksCleaner.StartAsync(ctx); err != nil {
		c.ringSubservices.StopAsync()
		return errors.Wrap(err, "failed to start the blocks cleaner")
	}

	return nil
}

func (c *MultitenantCompactor) stopping(_ error) error {
	ctx := context.Background()

	services.StopAndAwaitTerminated(ctx, c.blocksCleaner) //nolint:errcheck
	if c.ringSubservices != nil {
		return services.StopManagerAndAwaitStopped(ctx, c.ringSubservices)
	}
	return nil
}

func (c *MultitenantCompactor) running(ctx context.Context) error {
	// Run an initial compaction before starting the interval.
	c.compactUsers(ctx)

	ticker := time.NewTicker(util.DurationWithJitter(c.compactorCfg.CompactionInterval, 0.05))
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.compactUsers(ctx)
		case <-ctx.Done():
			return nil
		case err := <-c.ringSubservicesWatcher.Chan():
			return errors.Wrap(err, "compactor subservice failed")
		}
	}
}

func (c *MultitenantCompactor) compactUsers(ctx context.Context) {
	succeeded := false
	compactionErrorCount := 0

	c.compactionRunsStarted.Inc()

	defer func() {
		if succeeded && compactionErrorCount == 0 {
			c.compactionRunsCompleted.Inc()
			c.compactionRunsLastSuccess.SetToCurrentTime()
		} else {
			c.compactionRunsFailed.Inc()
		}

		// Reset progress metrics once done.
		c.compactionRunDiscoveredTenants.Set(0)
		c.compactionRunSkippedTenants.Set(0)
		c.compactionRunSucceededTenants.Set(0)
		c.compactionRunFailedTenants.Set(0)
	}()

	level.Info(c.logger).Log("msg", "discovering users from bucket")
	users, err := c.discoverUsersWithRetries(ctx)
	if err != nil {
		level.Error(c.logger).Log("msg", "failed to discover users from bucket", "err", err)
		return
	}

	level.Info(c.logger).Log("msg", "discovered users from bucket", "users", len(users))
	c.compactionRunDiscoveredTenants.Set(float64(len(users)))

	// When starting multiple compactor replicas nearly at the same time, running in a cluster with
	// a large number of tenants, we may end up in a situation where the 1st user is compacted by
	// multiple replicas at the same time. Shuffling users helps reduce the likelihood this will happen.
	rand.Shuffle(len(users), func(i, j int) {
		users[i], users[j] = users[j], users[i]
	})

	// Keep track of users owned by this shard, so that we can delete the local files for all other users.
	ownedUsers := map[string]struct{}{}
	for _, userID := range users {
		// Ensure the context has not been canceled (ie. compactor shutdown has been triggered).
		if ctx.Err() != nil {
			level.Info(c.logger).Log("msg", "interrupting compaction of user blocks", "err", err)
			return
		}

		// Ensure the user ID belongs to our shard.
		if owned, err := c.shardingStrategy.compactorOwnUser(userID); err != nil {
			c.compactionRunSkippedTenants.Inc()
			level.Warn(c.logger).Log("msg", "unable to check if user is owned by this shard", "user", userID, "err", err)
			continue
		} else if !owned {
			c.compactionRunSkippedTenants.Inc()
			level.Debug(c.logger).Log("msg", "skipping user because it is not owned by this shard", "user", userID)
			continue
		}

		ownedUsers[userID] = struct{}{}

		if markedForDeletion, err := mimir_tsdb.TenantDeletionMarkExists(ctx, c.bucketClient, userID); err != nil {
			c.compactionRunSkippedTenants.Inc()
			level.Warn(c.logger).Log("msg", "unable to check if user is marked for deletion", "user", userID, "err", err)
			continue
		} else if markedForDeletion {
			c.compactionRunSkippedTenants.Inc()
			level.Debug(c.logger).Log("msg", "skipping user because it is marked for deletion", "user", userID)
			continue
		}

		level.Info(c.logger).Log("msg", "starting compaction of user blocks", "user", userID)

		if err = c.compactUserWithRetries(ctx, userID); err != nil {
			c.compactionRunFailedTenants.Inc()
			compactionErrorCount++
			level.Error(c.logger).Log("msg", "failed to compact user blocks", "user", userID, "err", err)
			continue
		}

		c.compactionRunSucceededTenants.Inc()
		level.Info(c.logger).Log("msg", "successfully compacted user blocks", "user", userID)
	}

	// Delete local files for unowned tenants, if there are any. This cleans up
	// leftover local files for tenants that belong to different compactors now,
	// or have been deleted completely.
	for userID := range c.listTenantsWithMetaSyncDirectories() {
		if _, owned := ownedUsers[userID]; owned {
			continue
		}

		dir := c.metaSyncDirForUser(userID)
		s, err := os.Stat(dir)
		if err != nil {
			if !os.IsNotExist(err) {
				level.Warn(c.logger).Log("msg", "failed to stat local directory with user data", "dir", dir, "err", err)
			}
			continue
		}

		if s.IsDir() {
			err := os.RemoveAll(dir)
			if err == nil {
				level.Info(c.logger).Log("msg", "deleted directory for user not owned by this shard", "dir", dir)
			} else {
				level.Warn(c.logger).Log("msg", "failed to delete directory for user not owned by this shard", "dir", dir, "err", err)
			}
		}
	}

	succeeded = true
}

func (c *MultitenantCompactor) compactUserWithRetries(ctx context.Context, userID string) error {
	var lastErr error

	retries := backoff.New(ctx, backoff.Config{
		MinBackoff: c.compactorCfg.retryMinBackoff,
		MaxBackoff: c.compactorCfg.retryMaxBackoff,
		MaxRetries: c.compactorCfg.CompactionRetries,
	})

	for retries.Ongoing() {
		lastErr = c.compactUser(ctx, userID)
		if lastErr == nil {
			return nil
		}

		retries.Wait()
	}

	return lastErr
}

func (c *MultitenantCompactor) compactUser(ctx context.Context, userID string) error {
	bucket := bucket.NewUserBucketClient(userID, c.bucketClient, c.cfgProvider)
	reg := prometheus.NewRegistry()
	defer c.syncerMetrics.gatherThanosSyncerMetrics(reg)

	ulogger := util_log.WithUserID(userID, c.logger)

	// While fetching blocks, we filter out blocks that were marked for deletion by using ExcludeMarkedForDeletionFilter.
	// No delay is used -- all blocks with deletion marker are ignored, and not considered for compaction.
	excludeMarkedForDeletionFilter := NewExcludeMarkedForDeletionFilter(bucket)
	// Filters out duplicate blocks that can be formed from two or more overlapping
	// blocks that fully submatches the source blocks of the older blocks.
	deduplicateBlocksFilter := NewShardAwareDeduplicateFilter()

	// List of filters to apply (order matters).
	fetcherFilters := []block.MetadataFilter{
		// Remove the ingester ID because we don't shard blocks anymore, while still
		// honoring the shard ID if sharding was done in the past.
		// Remove TenantID external label to make sure that we compact blocks with and without the label
		// together.
		NewLabelRemoverFilter([]string{
			mimir_tsdb.DeprecatedTenantIDExternalLabel,
			mimir_tsdb.DeprecatedIngesterIDExternalLabel,
		}),
		block.NewConsistencyDelayMetaFilter(ulogger, c.compactorCfg.ConsistencyDelay, reg),
		excludeMarkedForDeletionFilter,
		deduplicateBlocksFilter,
		// removes blocks that should not be compacted due to being marked so.
		NewNoCompactionMarkFilter(bucket, true),
	}

	fetcher, err := block.NewMetaFetcher(
		ulogger,
		c.compactorCfg.MetaSyncConcurrency,
		bucket,
		c.metaSyncDirForUser(userID),
		reg,
		fetcherFilters,
	)
	if err != nil {
		return err
	}

	syncer, err := NewMetaSyncer(
		ulogger,
		reg,
		bucket,
		fetcher,
		deduplicateBlocksFilter,
		excludeMarkedForDeletionFilter,
		c.blocksMarkedForDeletion,
	)
	if err != nil {
		return errors.Wrap(err, "failed to create syncer")
	}

	compactor, err := NewBucketCompactor(
		ulogger,
		syncer,
		c.blocksGrouperFactory(ctx, c.compactorCfg, c.cfgProvider, userID, ulogger, reg),
		c.blocksPlanner,
		c.blocksCompactor,
		path.Join(c.compactorCfg.DataDir, "compact"),
		bucket,
		c.compactorCfg.CompactionConcurrency,
		true, // Skip blocks with out of order chunks, and mark them for no-compaction.
		c.shardingStrategy.ownJob,
		c.jobsOrder,
		c.compactorCfg.BlockSyncConcurrency,
		c.bucketCompactorMetrics,
	)
	if err != nil {
		return errors.Wrap(err, "failed to create bucket compactor")
	}

	if err := compactor.Compact(ctx, c.compactorCfg.MaxCompactionTime); err != nil {
		return errors.Wrap(err, "compaction")
	}

	return nil
}

func (c *MultitenantCompactor) discoverUsersWithRetries(ctx context.Context) ([]string, error) {
	var lastErr error

	retries := backoff.New(ctx, backoff.Config{
		MinBackoff: c.compactorCfg.retryMinBackoff,
		MaxBackoff: c.compactorCfg.retryMaxBackoff,
		MaxRetries: c.compactorCfg.CompactionRetries,
	})

	for retries.Ongoing() {
		var users []string

		users, lastErr = c.discoverUsers(ctx)
		if lastErr == nil {
			return users, nil
		}

		retries.Wait()
	}

	return nil, lastErr
}

func (c *MultitenantCompactor) discoverUsers(ctx context.Context) ([]string, error) {
	return mimir_tsdb.ListUsers(ctx, c.bucketClient)
}

// shardingStrategy describes whether compactor "owns" given user or job.
type shardingStrategy interface {
	compactorOwnUser(userID string) (bool, error)
	blocksCleanerOwnUser(userID string) (bool, error)
	ownJob(job *Job) (bool, error)
}

// splitAndMergeShardingStrategy is used by split-and-merge compactor when configured with sharding.
// All compactors from user's shard own the user for compaction purposes, and plan jobs.
// Each job is only owned and executed by single compactor.
// Only one of compactors from user's shard will do cleanup.
type splitAndMergeShardingStrategy struct {
	allowedTenants *util.AllowedTenants
	ring           *ring.Ring
	ringLifecycler *ring.Lifecycler
	configProvider ConfigProvider
}

func newSplitAndMergeShardingStrategy(allowedTenants *util.AllowedTenants, ring *ring.Ring, ringLifecycler *ring.Lifecycler, configProvider ConfigProvider) *splitAndMergeShardingStrategy {
	return &splitAndMergeShardingStrategy{
		allowedTenants: allowedTenants,
		ring:           ring,
		ringLifecycler: ringLifecycler,
		configProvider: configProvider,
	}
}

// Only single instance in the subring can run blocks cleaner for given user.
func (s *splitAndMergeShardingStrategy) blocksCleanerOwnUser(userID string) (bool, error) {
	if !s.allowedTenants.IsAllowed(userID) {
		return false, nil
	}

	r := s.ring.ShuffleShard(userID, s.configProvider.CompactorTenantShardSize(userID))

	return instanceOwnsTokenInRing(r, s.ringLifecycler.Addr, userID)
}

// ALL compactors should plan jobs for all users.
func (s *splitAndMergeShardingStrategy) compactorOwnUser(userID string) (bool, error) {
	if !s.allowedTenants.IsAllowed(userID) {
		return false, nil
	}

	r := s.ring.ShuffleShard(userID, s.configProvider.CompactorTenantShardSize(userID))

	return r.HasInstance(s.ringLifecycler.ID), nil
}

// Only single compactor should execute the job.
func (s *splitAndMergeShardingStrategy) ownJob(job *Job) (bool, error) {
	ok, err := s.compactorOwnUser(job.UserID())
	if err != nil || !ok {
		return ok, err
	}

	r := s.ring.ShuffleShard(job.UserID(), s.configProvider.CompactorTenantShardSize(job.UserID()))

	return instanceOwnsTokenInRing(r, s.ringLifecycler.Addr, job.ShardingKey())
}

func instanceOwnsTokenInRing(r ring.ReadRing, instanceAddr string, key string) (bool, error) {
	// Hash the key.
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	hash := hasher.Sum32()

	// Check whether this compactor instance owns the token.
	rs, err := r.Get(hash, RingOp, nil, nil, nil)
	if err != nil {
		return false, err
	}

	if len(rs.Instances) != 1 {
		return false, fmt.Errorf("unexpected number of compactors in the shard (expected 1, got %d)", len(rs.Instances))
	}

	return rs.Instances[0].Addr == instanceAddr, nil
}

const compactorMetaPrefix = "compactor-meta-"

// metaSyncDirForUser returns directory to store cached meta files.
// The fetcher stores cached metas in the "meta-syncer/" sub directory,
// but we prefix it with "compactor-meta-" in order to guarantee no clashing with
// the directory used by the Thanos Syncer, whatever is the user ID.
func (c *MultitenantCompactor) metaSyncDirForUser(userID string) string {
	return filepath.Join(c.compactorCfg.DataDir, compactorMetaPrefix+userID)
}

// This function returns tenants with meta sync directories found on local disk. On error, it returns nil map.
func (c *MultitenantCompactor) listTenantsWithMetaSyncDirectories() map[string]struct{} {
	result := map[string]struct{}{}

	files, err := os.ReadDir(c.compactorCfg.DataDir)
	if err != nil {
		return nil
	}

	for _, f := range files {
		if !f.IsDir() {
			continue
		}

		if !strings.HasPrefix(f.Name(), compactorMetaPrefix) {
			continue
		}

		result[f.Name()[len(compactorMetaPrefix):]] = struct{}{}
	}

	return result
}
