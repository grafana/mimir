// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester_v2.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"
	"expvar"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/status"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	promcfg "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/util/zeropool"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/ingester/activeseries"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/sharding"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/usagestats"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/limiter"
	util_log "github.com/grafana/mimir/pkg/util/log"
	util_math "github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/push"
	"github.com/grafana/mimir/pkg/util/shutdownmarker"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	// Number of timeseries to return in each batch of a QueryStream.
	queryStreamBatchSize = 128

	// Discarded Metadata metric labels.
	perUserMetadataLimit   = "per_user_metadata_limit"
	perMetricMetadataLimit = "per_metric_metadata_limit"

	// Period at which to attempt purging metadata from memory.
	metadataPurgePeriod = 5 * time.Minute

	// How frequently update the usage statistics.
	usageStatsUpdateInterval = usagestats.DefaultReportSendInterval / 10

	// IngesterRingKey is the key under which we store the ingesters ring in the KVStore.
	IngesterRingKey = "ring"

	errTSDBCreateIncompatibleState = "cannot create a new TSDB while the ingester is not in active state (current state: %s)"

	// Jitter applied to the idle timeout to prevent compaction in all ingesters concurrently.
	compactionIdleTimeoutJitter = 0.25

	instanceIngestionRateTickInterval = time.Second

	// Reasons for discarding samples
	reasonSampleOutOfOrder     = "sample-out-of-order"
	reasonSampleTooOld         = "sample-too-old"
	reasonSampleTooFarInFuture = "sample-too-far-in-future"
	reasonNewValueForTimestamp = "new-value-for-timestamp"
	reasonSampleOutOfBounds    = "sample-out-of-bounds"
	reasonPerUserSeriesLimit   = "per_user_series_limit"
	reasonPerMetricSeriesLimit = "per_metric_series_limit"

	replicationFactorStatsName             = "ingester_replication_factor"
	ringStoreStatsName                     = "ingester_ring_store"
	memorySeriesStatsName                  = "ingester_inmemory_series"
	memoryTenantsStatsName                 = "ingester_inmemory_tenants"
	appendedSamplesStatsName               = "ingester_appended_samples"
	appendedExemplarsStatsName             = "ingester_appended_exemplars"
	tenantsWithOutOfOrderEnabledStatName   = "ingester_ooo_enabled_tenants"
	minOutOfOrderTimeWindowSecondsStatName = "ingester_ooo_min_window"
	maxOutOfOrderTimeWindowSecondsStatName = "ingester_ooo_max_window"

	// Value used to track the limit between sequential and concurrent TSDB opernings.
	// Below this value, TSDBs of different tenants are opened sequentially, otherwise concurrently.
	maxTSDBOpenWithoutConcurrency = 10
)

var (
	reasonIngesterMaxIngestionRate        = globalerror.IngesterMaxIngestionRate.LabelValue()
	reasonIngesterMaxTenants              = globalerror.IngesterMaxTenants.LabelValue()
	reasonIngesterMaxInMemorySeries       = globalerror.IngesterMaxInMemorySeries.LabelValue()
	reasonIngesterMaxInflightPushRequests = globalerror.IngesterMaxInflightPushRequests.LabelValue()
)

// BlocksUploader interface is used to have an easy way to mock it in tests.
type BlocksUploader interface {
	Sync(ctx context.Context) (uploaded int, err error)
}

// QueryStreamType defines type of function to use when doing query-stream operation.
type QueryStreamType int

const (
	QueryStreamDefault QueryStreamType = iota // Use default configured value.
	QueryStreamSamples                        // Stream individual samples.
	QueryStreamChunks                         // Stream entire chunks.
)

type requestWithUsersAndCallback struct {
	users    *util.AllowedTenants // if nil, all tenants are allowed.
	callback chan<- struct{}      // when compaction/shipping is finished, this channel is closed
}

// Config for an Ingester.
type Config struct {
	IngesterRing RingConfig `yaml:"ring"`

	// Config for metadata purging.
	MetadataRetainPeriod time.Duration `yaml:"metadata_retain_period" category:"advanced"`

	RateUpdatePeriod time.Duration `yaml:"rate_update_period" category:"advanced"`

	ActiveSeriesMetrics activeseries.Config `yaml:",inline"`

	TSDBConfigUpdatePeriod time.Duration `yaml:"tsdb_config_update_period" category:"experimental"`

	BlocksStorageConfig         mimir_tsdb.BlocksStorageConfig `yaml:"-"`
	StreamChunksWhenUsingBlocks bool                           `yaml:"-" category:"advanced"`
	// Runtime-override for type of streaming query to use (chunks or samples).
	StreamTypeFn func() QueryStreamType `yaml:"-"`

	DefaultLimits    InstanceLimits         `yaml:"instance_limits"`
	InstanceLimitsFn func() *InstanceLimits `yaml:"-"`

	IgnoreSeriesLimitForMetricNames string `yaml:"ignore_series_limit_for_metric_names" category:"advanced"`

	ReadPathCPUUtilizationLimit          float64 `yaml:"read_path_cpu_utilization_limit" category:"experimental"`
	ReadPathMemoryUtilizationLimit       uint64  `yaml:"read_path_memory_utilization_limit" category:"experimental"`
	LogUtilizationBasedLimiterCPUSamples bool    `yaml:"log_utilization_based_limiter_cpu_samples" category:"experimental"`

	LimitInflightRequestsUsingGrpcTapHandle bool `yaml:"limit_inflight_requests_using_grpc_tap_handle" category:"experimental"`

	ErrorSampleRate int64 `yaml:"error_sample_rate" json:"error_sample_rate" category:"experimental"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	cfg.IngesterRing.RegisterFlags(f, logger)
	cfg.DefaultLimits.RegisterFlags(f)
	cfg.ActiveSeriesMetrics.RegisterFlags(f)

	f.DurationVar(&cfg.MetadataRetainPeriod, "ingester.metadata-retain-period", 10*time.Minute, "Period at which metadata we have not seen will remain in memory before being deleted.")
	f.DurationVar(&cfg.RateUpdatePeriod, "ingester.rate-update-period", 15*time.Second, "Period with which to update the per-tenant ingestion rates.")
	f.BoolVar(&cfg.StreamChunksWhenUsingBlocks, "ingester.stream-chunks-when-using-blocks", true, "Stream chunks from ingesters to queriers.")
	f.DurationVar(&cfg.TSDBConfigUpdatePeriod, "ingester.tsdb-config-update-period", 15*time.Second, "Period with which to update the per-tenant TSDB configuration.")
	f.StringVar(&cfg.IgnoreSeriesLimitForMetricNames, "ingester.ignore-series-limit-for-metric-names", "", "Comma-separated list of metric names, for which the -ingester.max-global-series-per-metric limit will be ignored. Does not affect the -ingester.max-global-series-per-user limit.")
	f.Float64Var(&cfg.ReadPathCPUUtilizationLimit, "ingester.read-path-cpu-utilization-limit", 0, "CPU utilization limit, as CPU cores, for CPU/memory utilization based read request limiting. Use 0 to disable it.")
	f.Uint64Var(&cfg.ReadPathMemoryUtilizationLimit, "ingester.read-path-memory-utilization-limit", 0, "Memory limit, in bytes, for CPU/memory utilization based read request limiting. Use 0 to disable it.")
	f.BoolVar(&cfg.LogUtilizationBasedLimiterCPUSamples, "ingester.log-utilization-based-limiter-cpu-samples", false, "Enable logging of utilization based limiter CPU samples.")
	f.BoolVar(&cfg.LimitInflightRequestsUsingGrpcTapHandle, "ingester.limit-inflight-requests-using-grpc-handlers", false, "Use experimental method of limiting push requests")
	f.Int64Var(&cfg.ErrorSampleRate, "ingester.error-sample-rate", 0, "Each error will be logged once in this many times. Use 0 to log all of them.")
}

func (cfg *Config) Validate() error {
	if cfg.ErrorSampleRate < 0 {
		return fmt.Errorf("error sample rate cannot be a negative number")
	}

	return cfg.IngesterRing.Validate()
}

func (cfg *Config) getIgnoreSeriesLimitForMetricNamesMap() map[string]struct{} {
	if cfg.IgnoreSeriesLimitForMetricNames == "" {
		return nil
	}

	result := map[string]struct{}{}

	for _, s := range strings.Split(cfg.IgnoreSeriesLimitForMetricNames, ",") {
		tr := strings.TrimSpace(s)
		if tr != "" {
			result[tr] = struct{}{}
		}
	}

	if len(result) == 0 {
		return nil
	}

	return result
}

// Ingester deals with "in flight" chunks.  Based on Prometheus 1.x
// MemorySeriesStorage.
type Ingester struct {
	*services.BasicService

	cfg Config

	metrics *ingesterMetrics
	logger  log.Logger

	lifecycler         *ring.Lifecycler
	limits             *validation.Overrides
	limiter            *Limiter
	subservicesWatcher *services.FailureWatcher

	// Mimir blocks storage.
	tsdbsMtx sync.RWMutex
	tsdbs    map[string]*userTSDB // tsdb sharded by userID

	bucket objstore.Bucket

	// Value used by shipper as external label.
	shipperIngesterID string

	// Metrics shared across all per-tenant shippers.
	shipperMetrics *shipperMetrics

	subservices  *services.Manager
	activeGroups *util.ActiveGroupsCleanupService

	tsdbMetrics *tsdbMetrics

	forceCompactTrigger chan requestWithUsersAndCallback
	shipTrigger         chan requestWithUsersAndCallback

	// Maps the per-block series ID with its labels hash.
	seriesHashCache *hashcache.SeriesHashCache

	// Timeout chosen for idle compactions.
	compactionIdleTimeout time.Duration

	// Number of series in memory, across all tenants.
	seriesCount atomic.Int64

	// For storing metadata ingested.
	usersMetadataMtx sync.RWMutex
	usersMetadata    map[string]*userMetricsMetadata

	// Rate of pushed samples. Used to limit global samples push rate.
	ingestionRate        *util_math.EwmaRate
	inflightPushRequests atomic.Int64

	// Anonymous usage statistics tracked by ingester.
	memorySeriesStats                  *expvar.Int
	memoryTenantsStats                 *expvar.Int
	appendedSamplesStats               *usagestats.Counter
	appendedExemplarsStats             *usagestats.Counter
	tenantsWithOutOfOrderEnabledStat   *expvar.Int
	minOutOfOrderTimeWindowSecondsStat *expvar.Int
	maxOutOfOrderTimeWindowSecondsStat *expvar.Int

	utilizationBasedLimiter utilizationBasedLimiter

	errorSamplers ingesterErrSamplers
}

func newIngester(cfg Config, limits *validation.Overrides, registerer prometheus.Registerer, logger log.Logger) (*Ingester, error) {
	if cfg.BlocksStorageConfig.Bucket.Backend == bucket.Filesystem {
		level.Warn(logger).Log("msg", "-blocks-storage.backend=filesystem is for development and testing only; you should switch to an external object store for production use or use a shared filesystem")
	}

	bucketClient, err := bucket.NewClient(context.Background(), cfg.BlocksStorageConfig.Bucket, "ingester", logger, registerer)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create the bucket client")
	}

	// Track constant usage stats.
	usagestats.GetInt(replicationFactorStatsName).Set(int64(cfg.IngesterRing.ReplicationFactor))
	usagestats.GetString(ringStoreStatsName).Set(cfg.IngesterRing.KVStore.Store)

	return &Ingester{
		cfg:    cfg,
		limits: limits,
		logger: logger,

		tsdbs:               make(map[string]*userTSDB),
		usersMetadata:       make(map[string]*userMetricsMetadata),
		bucket:              bucketClient,
		tsdbMetrics:         newTSDBMetrics(registerer, logger),
		shipperMetrics:      newShipperMetrics(registerer),
		forceCompactTrigger: make(chan requestWithUsersAndCallback),
		shipTrigger:         make(chan requestWithUsersAndCallback),
		seriesHashCache:     hashcache.NewSeriesHashCache(cfg.BlocksStorageConfig.TSDB.SeriesHashCacheMaxBytes),

		memorySeriesStats:                  usagestats.GetAndResetInt(memorySeriesStatsName),
		memoryTenantsStats:                 usagestats.GetAndResetInt(memoryTenantsStatsName),
		appendedSamplesStats:               usagestats.GetAndResetCounter(appendedSamplesStatsName),
		appendedExemplarsStats:             usagestats.GetAndResetCounter(appendedExemplarsStatsName),
		tenantsWithOutOfOrderEnabledStat:   usagestats.GetAndResetInt(tenantsWithOutOfOrderEnabledStatName),
		minOutOfOrderTimeWindowSecondsStat: usagestats.GetAndResetInt(minOutOfOrderTimeWindowSecondsStatName),
		maxOutOfOrderTimeWindowSecondsStat: usagestats.GetAndResetInt(maxOutOfOrderTimeWindowSecondsStatName),

		errorSamplers: newIngesterErrSamplers(cfg.ErrorSampleRate),
	}, nil
}

// New returns an Ingester that uses Mimir block storage.
func New(cfg Config, limits *validation.Overrides, activeGroupsCleanupService *util.ActiveGroupsCleanupService, registerer prometheus.Registerer, logger log.Logger) (*Ingester, error) {
	i, err := newIngester(cfg, limits, registerer, logger)
	if err != nil {
		return nil, err
	}
	i.ingestionRate = util_math.NewEWMARate(0.2, instanceIngestionRateTickInterval)
	i.metrics = newIngesterMetrics(registerer, cfg.ActiveSeriesMetrics.Enabled, i.getInstanceLimits, i.ingestionRate, &i.inflightPushRequests)
	i.activeGroups = activeGroupsCleanupService

	if registerer != nil {
		promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
			Name: "cortex_ingester_oldest_unshipped_block_timestamp_seconds",
			Help: "Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.",
		}, i.getOldestUnshippedBlockMetric)

		promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
			Name: "cortex_ingester_tsdb_head_min_timestamp_seconds",
			Help: "Minimum timestamp of the head block across all tenants.",
		}, i.minTsdbHeadTimestamp)

		promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
			Name: "cortex_ingester_tsdb_head_max_timestamp_seconds",
			Help: "Maximum timestamp of the head block across all tenants.",
		}, i.maxTsdbHeadTimestamp)
	}

	i.lifecycler, err = ring.NewLifecycler(cfg.IngesterRing.ToLifecyclerConfig(logger), i, "ingester", IngesterRingKey, cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown, logger, prometheus.WrapRegistererWithPrefix("cortex_", registerer))
	if err != nil {
		return nil, err
	}
	i.subservicesWatcher = services.NewFailureWatcher()
	i.subservicesWatcher.WatchService(i.lifecycler)

	// Init the limter and instantiate the user states which depend on it
	i.limiter = NewLimiter(
		limits,
		i.lifecycler,
		cfg.IngesterRing.ReplicationFactor,
		cfg.IngesterRing.ZoneAwarenessEnabled,
	)

	if cfg.ReadPathCPUUtilizationLimit > 0 || cfg.ReadPathMemoryUtilizationLimit > 0 {
		i.utilizationBasedLimiter = limiter.NewUtilizationBasedLimiter(cfg.ReadPathCPUUtilizationLimit,
			cfg.ReadPathMemoryUtilizationLimit, cfg.LogUtilizationBasedLimiterCPUSamples,
			log.WithPrefix(logger, "context", "read path"),
			prometheus.WrapRegistererWithPrefix("cortex_ingester_", registerer))
	}

	i.shipperIngesterID = i.lifecycler.ID

	// Apply positive jitter only to ensure that the minimum timeout is adhered to.
	i.compactionIdleTimeout = util.DurationWithPositiveJitter(i.cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout, compactionIdleTimeoutJitter)
	level.Info(i.logger).Log("msg", "TSDB idle compaction timeout set", "timeout", i.compactionIdleTimeout)

	i.BasicService = services.NewBasicService(i.starting, i.updateLoop, i.stopping)
	return i, nil
}

// NewForFlusher is a special version of ingester used by Flusher. This
// ingester is not ingesting anything, its only purpose is to react on Flush
// method and flush all openened TSDBs when called.
func NewForFlusher(cfg Config, limits *validation.Overrides, registerer prometheus.Registerer, logger log.Logger) (*Ingester, error) {
	i, err := newIngester(cfg, limits, registerer, logger)
	if err != nil {
		return nil, err
	}
	i.metrics = newIngesterMetrics(registerer, false, i.getInstanceLimits, nil, &i.inflightPushRequests)

	i.shipperIngesterID = "flusher"

	// This ingester will not start any subservices (lifecycler, compaction, shipping),
	// and will only open TSDBs, wait for Flush to be called, and then close TSDBs again.
	i.BasicService = services.NewIdleService(i.startingForFlusher, i.stoppingForFlusher)
	return i, nil
}

func (i *Ingester) startingForFlusher(ctx context.Context) error {
	if err := i.openExistingTSDB(ctx); err != nil {
		// Try to rollback and close opened TSDBs before halting the ingester.
		i.closeAllTSDB()

		return errors.Wrap(err, "opening existing TSDBs")
	}

	// Don't start any sub-services (lifecycler, compaction, shipper) at all.
	return nil
}

func (i *Ingester) starting(ctx context.Context) error {
	if err := i.openExistingTSDB(ctx); err != nil {
		// Try to rollback and close opened TSDBs before halting the ingester.
		i.closeAllTSDB()

		return errors.Wrap(err, "opening existing TSDBs")
	}

	// Important: we want to keep lifecycler running until we ask it to stop, so we need to give it independent context
	if err := i.lifecycler.StartAsync(context.Background()); err != nil {
		return errors.Wrap(err, "failed to start lifecycler")
	}
	if err := i.lifecycler.AwaitRunning(ctx); err != nil {
		return errors.Wrap(err, "failed to start lifecycler")
	}

	// let's start the rest of subservices via manager
	servs := []services.Service(nil)

	compactionService := services.NewBasicService(nil, i.compactionLoop, nil)
	servs = append(servs, compactionService)

	if i.cfg.BlocksStorageConfig.TSDB.IsBlocksShippingEnabled() {
		shippingService := services.NewBasicService(nil, i.shipBlocksLoop, nil)
		servs = append(servs, shippingService)
	}

	if i.cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout > 0 {
		interval := i.cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBInterval
		if interval == 0 {
			interval = mimir_tsdb.DefaultCloseIdleTSDBInterval
		}
		closeIdleService := services.NewTimerService(interval, nil, i.closeAndDeleteIdleUserTSDBs, nil)
		servs = append(servs, closeIdleService)
	}

	if i.utilizationBasedLimiter != nil {
		servs = append(servs, i.utilizationBasedLimiter)
	}

	shutdownMarkerPath := shutdownmarker.GetPath(i.cfg.BlocksStorageConfig.TSDB.Dir)
	shutdownMarkerFound, err := shutdownmarker.Exists(shutdownMarkerPath)
	if err != nil {
		return errors.Wrap(err, "failed to check ingester shutdown marker")
	}

	if shutdownMarkerFound {
		level.Info(i.logger).Log("msg", "detected existing shutdown marker, setting unregister and flush on shutdown", "path", shutdownMarkerPath)
		i.setPrepareShutdown()
	}

	i.subservices, err = services.NewManager(servs...)
	if err == nil {
		err = services.StartManagerAndAwaitHealthy(ctx, i.subservices)
	}
	return errors.Wrap(err, "failed to start ingester components")
}

func (i *Ingester) stoppingForFlusher(_ error) error {
	if !i.cfg.BlocksStorageConfig.TSDB.KeepUserTSDBOpenOnShutdown {
		i.closeAllTSDB()
	}
	return nil
}

func (i *Ingester) stopping(_ error) error {
	// It's important to wait until shipper is finished,
	// because the blocks transfer should start only once it's guaranteed
	// there's no shipping on-going.

	if err := services.StopManagerAndAwaitStopped(context.Background(), i.subservices); err != nil {
		level.Warn(i.logger).Log("msg", "failed to stop ingester subservices", "err", err)
	}

	// Next initiate our graceful exit from the ring.
	if err := services.StopAndAwaitTerminated(context.Background(), i.lifecycler); err != nil {
		level.Warn(i.logger).Log("msg", "failed to stop ingester lifecycler", "err", err)
	}

	// Remove the shutdown marker if it exists since we are shutting down
	shutdownMarkerPath := shutdownmarker.GetPath(i.cfg.BlocksStorageConfig.TSDB.Dir)
	if err := shutdownmarker.Remove(shutdownMarkerPath); err != nil {
		level.Warn(i.logger).Log("msg", "failed to remove shutdown marker", "path", shutdownMarkerPath, "err", err)
	}

	if !i.cfg.BlocksStorageConfig.TSDB.KeepUserTSDBOpenOnShutdown {
		i.closeAllTSDB()
	}
	return nil
}

func (i *Ingester) updateLoop(ctx context.Context) error {
	rateUpdateTicker := time.NewTicker(i.cfg.RateUpdatePeriod)
	defer rateUpdateTicker.Stop()

	ingestionRateTicker := time.NewTicker(instanceIngestionRateTickInterval)
	defer ingestionRateTicker.Stop()

	tsdbUpdateTicker := time.NewTicker(i.cfg.TSDBConfigUpdatePeriod)
	defer tsdbUpdateTicker.Stop()

	var activeSeriesTickerChan <-chan time.Time
	if i.cfg.ActiveSeriesMetrics.Enabled {
		t := time.NewTicker(i.cfg.ActiveSeriesMetrics.UpdatePeriod)
		activeSeriesTickerChan = t.C
		defer t.Stop()
	}

	// Similarly to the above, this is a hardcoded value.
	metadataPurgeTicker := time.NewTicker(metadataPurgePeriod)
	defer metadataPurgeTicker.Stop()

	usageStatsUpdateTicker := time.NewTicker(usageStatsUpdateInterval)
	defer usageStatsUpdateTicker.Stop()

	for {
		select {
		case <-metadataPurgeTicker.C:
			i.purgeUserMetricsMetadata()
		case <-ingestionRateTicker.C:
			i.ingestionRate.Tick()
		case <-rateUpdateTicker.C:
			i.tsdbsMtx.RLock()
			for _, db := range i.tsdbs {
				db.ingestedAPISamples.Tick()
				db.ingestedRuleSamples.Tick()
			}
			i.tsdbsMtx.RUnlock()

		case <-tsdbUpdateTicker.C:
			i.applyTSDBSettings()

		case <-activeSeriesTickerChan:
			i.updateActiveSeries(time.Now())

		case <-usageStatsUpdateTicker.C:
			i.updateUsageStats()

		case <-ctx.Done():
			return nil
		case err := <-i.subservicesWatcher.Chan():
			return errors.Wrap(err, "ingester subservice failed")
		}
	}
}

func (i *Ingester) replaceMatchers(asm *activeseries.Matchers, userDB *userTSDB, now time.Time) {
	i.metrics.deletePerUserCustomTrackerMetrics(userDB.userID, userDB.activeSeries.CurrentMatcherNames())
	userDB.activeSeries.ReloadMatchers(asm, now)
}

func (i *Ingester) updateActiveSeries(now time.Time) {
	for _, userID := range i.getTSDBUsers() {
		userDB := i.getTSDB(userID)
		if userDB == nil {
			continue
		}

		newMatchersConfig := i.limits.ActiveSeriesCustomTrackersConfig(userID)
		if newMatchersConfig.String() != userDB.activeSeries.CurrentConfig().String() {
			i.replaceMatchers(activeseries.NewMatchers(newMatchersConfig), userDB, now)
		}
		valid := userDB.activeSeries.Purge(now)
		if !valid {
			// Active series config has been reloaded, exposing loading metric until MetricsIdleTimeout passes.
			i.metrics.activeSeriesLoading.WithLabelValues(userID).Set(1)
		} else {
			allActive, activeMatching, allActiveHistograms, activeMatchingHistograms, allActiveBuckets, activeMatchingBuckets := userDB.activeSeries.ActiveWithMatchers()
			i.metrics.activeSeriesLoading.DeleteLabelValues(userID)
			if allActive > 0 {
				i.metrics.activeSeriesPerUser.WithLabelValues(userID).Set(float64(allActive))
			} else {
				i.metrics.activeSeriesPerUser.DeleteLabelValues(userID)
			}
			if allActiveHistograms > 0 {
				i.metrics.activeSeriesPerUserNativeHistograms.WithLabelValues(userID).Set(float64(allActiveHistograms))
			} else {
				i.metrics.activeSeriesPerUserNativeHistograms.DeleteLabelValues(userID)
			}
			if allActiveBuckets > 0 {
				i.metrics.activeNativeHistogramBucketsPerUser.WithLabelValues(userID).Set(float64(allActiveBuckets))
			} else {
				i.metrics.activeNativeHistogramBucketsPerUser.DeleteLabelValues(userID)
			}

			for idx, name := range userDB.activeSeries.CurrentMatcherNames() {
				// We only set the metrics for matchers that actually exist, to avoid increasing cardinality with zero valued metrics.
				if activeMatching[idx] > 0 {
					i.metrics.activeSeriesCustomTrackersPerUser.WithLabelValues(userID, name).Set(float64(activeMatching[idx]))
				} else {
					i.metrics.activeSeriesCustomTrackersPerUser.DeleteLabelValues(userID, name)
				}
				if activeMatchingHistograms[idx] > 0 {
					i.metrics.activeSeriesCustomTrackersPerUserNativeHistograms.WithLabelValues(userID, name).Set(float64(activeMatchingHistograms[idx]))
				} else {
					i.metrics.activeSeriesCustomTrackersPerUserNativeHistograms.DeleteLabelValues(userID, name)
				}
				if activeMatchingBuckets[idx] > 0 {
					i.metrics.activeNativeHistogramBucketsCustomTrackersPerUser.WithLabelValues(userID, name).Set(float64(activeMatchingBuckets[idx]))
				} else {
					i.metrics.activeNativeHistogramBucketsCustomTrackersPerUser.DeleteLabelValues(userID, name)
				}
			}
		}
	}
}

// updateUsageStats updated some anonymous usage statistics tracked by the ingester.
// This function is expected to be called periodically.
func (i *Ingester) updateUsageStats() {
	memoryUsersCount := int64(0)
	memorySeriesCount := int64(0)
	tenantsWithOutOfOrderEnabledCount := int64(0)
	minOutOfOrderTimeWindow := time.Duration(0)
	maxOutOfOrderTimeWindow := time.Duration(0)

	for _, userID := range i.getTSDBUsers() {
		userDB := i.getTSDB(userID)
		if userDB == nil {
			continue
		}

		// Track only tenants with at least 1 series.
		numSeries := userDB.Head().NumSeries()
		if numSeries == 0 {
			continue
		}

		memoryUsersCount++
		memorySeriesCount += int64(numSeries)

		oooWindow := i.limits.OutOfOrderTimeWindow(userID)
		if oooWindow > 0 {
			tenantsWithOutOfOrderEnabledCount++

			if minOutOfOrderTimeWindow == 0 || oooWindow < minOutOfOrderTimeWindow {
				minOutOfOrderTimeWindow = oooWindow
			}
			if oooWindow > maxOutOfOrderTimeWindow {
				maxOutOfOrderTimeWindow = oooWindow
			}
		}
	}

	// Track anonymous usage stats.
	i.memorySeriesStats.Set(memorySeriesCount)
	i.memoryTenantsStats.Set(memoryUsersCount)
	i.tenantsWithOutOfOrderEnabledStat.Set(tenantsWithOutOfOrderEnabledCount)
	i.minOutOfOrderTimeWindowSecondsStat.Set(int64(minOutOfOrderTimeWindow.Seconds()))
	i.maxOutOfOrderTimeWindowSecondsStat.Set(int64(maxOutOfOrderTimeWindow.Seconds()))
}

// applyTSDBSettings goes through all tenants and applies
// * The current max-exemplars setting. If it changed, tsdb will resize the buffer; if it didn't change tsdb will return quickly.
// * The current out-of-order time window. If it changes from 0 to >0, then a new Write-Behind-Log gets created for that tenant.
func (i *Ingester) applyTSDBSettings() {
	for _, userID := range i.getTSDBUsers() {
		globalValue := i.limits.MaxGlobalExemplarsPerUser(userID)
		localValue := i.limiter.convertGlobalToLocalLimit(userID, globalValue)

		oooTW := i.limits.OutOfOrderTimeWindow(userID)
		if oooTW < 0 {
			oooTW = 0
		}

		// We populate a Config struct with just TSDB related config, which is OK
		// because DB.ApplyConfig only looks at the specified config.
		// The other fields in Config are things like Rules, Scrape
		// settings, which don't apply to Head.
		cfg := promcfg.Config{
			StorageConfig: promcfg.StorageConfig{
				ExemplarsConfig: &promcfg.ExemplarsConfig{
					MaxExemplars: int64(localValue),
				},
				TSDBConfig: &promcfg.TSDBConfig{
					OutOfOrderTimeWindow: oooTW.Milliseconds(),
				},
			},
		}
		db := i.getTSDB(userID)
		if db == nil {
			continue
		}
		if err := db.db.ApplyConfig(&cfg); err != nil {
			level.Error(i.logger).Log("msg", "failed to apply config to TSDB", "user", userID, "err", err)
		}
		if i.limits.NativeHistogramsIngestionEnabled(userID) {
			// there is not much overhead involved, so don't keep previous state, just overwrite the current setting
			db.db.EnableNativeHistograms()
		} else {
			db.db.DisableNativeHistograms()
		}
	}
}

// GetRef() is an extra method added to TSDB to let Mimir check before calling Add()
type extendedAppender interface {
	storage.Appender
	storage.GetRef
}

type pushStats struct {
	succeededSamplesCount     int
	failedSamplesCount        int
	succeededExemplarsCount   int
	failedExemplarsCount      int
	sampleOutOfBoundsCount    int
	sampleOutOfOrderCount     int
	sampleTooOldCount         int
	sampleTooFarInFutureCount int
	newValueForTimestampCount int
	perUserSeriesLimitCount   int
	perMetricSeriesLimitCount int
}

// StartPushRequest checks if ingester can start push request, and increments relevant counters.
// If new push request cannot be started, errors converible to gRPC status code are returned, and metrics are updated.
func (i *Ingester) StartPushRequest() error {
	if err := i.checkRunning(); err != nil {
		return err
	}

	inflight := i.inflightPushRequests.Inc()
	decreaseInflightInDefer := true
	defer func() {
		if decreaseInflightInDefer {
			i.inflightPushRequests.Dec()
		}
	}()

	il := i.getInstanceLimits()
	if il != nil && il.MaxInflightPushRequests > 0 {
		if inflight > il.MaxInflightPushRequests {
			i.metrics.rejected.WithLabelValues(reasonIngesterMaxInflightPushRequests).Inc()
			return errMaxInflightRequestsReached
		}
	}

	if il != nil && il.MaxIngestionRate > 0 {
		if rate := i.ingestionRate.Rate(); rate >= il.MaxIngestionRate {
			i.metrics.rejected.WithLabelValues(reasonIngesterMaxIngestionRate).Inc()
			return errMaxIngestionRateReached
		}
	}

	decreaseInflightInDefer = false
	return nil
}

func (i *Ingester) FinishPushRequest() {
	i.inflightPushRequests.Dec()
}

// PushWithCleanup is the Push() implementation for blocks storage and takes a WriteRequest and adds it to the TSDB head.
func (i *Ingester) PushWithCleanup(ctx context.Context, pushReq *push.Request) (*mimirpb.WriteResponse, error) {
	// NOTE: because we use `unsafe` in deserialisation, we must not
	// retain anything from `req` past the exit from this function.
	defer pushReq.CleanUp()

	// If we're using grpc handlers, we don't need to start/finish request here.
	if !i.cfg.LimitInflightRequestsUsingGrpcTapHandle {
		if err := i.StartPushRequest(); err != nil {
			return nil, util_log.DoNotLogError{Err: err}
		}
		defer i.FinishPushRequest()
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	req, err := pushReq.WriteRequest()
	if err != nil {
		return nil, err
	}

	// Given metadata is a best-effort approach, and we don't halt on errors
	// process it before samples. Otherwise, we risk returning an error before ingestion.
	if ingestedMetadata := i.pushMetadata(ctx, userID, req.GetMetadata()); ingestedMetadata > 0 {
		// Distributor counts both samples and metadata, so for consistency ingester does the same.
		i.ingestionRate.Add(int64(ingestedMetadata))
	}

	// Early exit if no timeseries in request - don't create a TSDB or an appender.
	if len(req.Timeseries) == 0 {
		return &mimirpb.WriteResponse{}, nil
	}

	db, err := i.getOrCreateTSDB(userID, false)
	if err != nil {
		return nil, wrapOrAnnotateWithUser(err, userID)
	}

	lockState, err := db.acquireAppendLock(req.MinTimestamp())
	if err != nil {
		return &mimirpb.WriteResponse{}, newErrorWithHTTPStatus(wrapOrAnnotateWithUser(err, userID), http.StatusServiceUnavailable)
	}
	defer db.releaseAppendLock(lockState)

	// Note that we don't .Finish() the span in this method on purpose
	spanlog := spanlogger.FromContext(ctx, i.logger)
	spanlog.DebugLog("event", "acquired append lock")

	var (
		startAppend = time.Now()

		// Keep track of some stats which are tracked only if the samples will be
		// successfully committed
		stats pushStats

		firstPartialErr    error
		updateFirstPartial = func(errFn func() error) {
			if firstPartialErr == nil {
				firstPartialErr = errFn()
			}
		}
	)

	// Walk the samples, appending them to the users database
	app := db.Appender(ctx).(extendedAppender)
	spanlog.DebugLog("event", "got appender for timeseries", "series", len(req.Timeseries))

	var activeSeries *activeseries.ActiveSeries
	if i.cfg.ActiveSeriesMetrics.Enabled {
		activeSeries = db.activeSeries
	}

	minAppendTime, minAppendTimeAvailable := db.Head().AppendableMinValidTime()

	err = i.pushSamplesToAppender(userID, req.Timeseries, app, startAppend, &stats, updateFirstPartial, activeSeries, i.limits.OutOfOrderTimeWindow(userID), minAppendTimeAvailable, minAppendTime)
	if err != nil {
		if err := app.Rollback(); err != nil {
			level.Warn(i.logger).Log("msg", "failed to rollback appender on error", "user", userID, "err", err)
		}

		return nil, wrapOrAnnotateWithUser(err, userID)
	}

	// At this point all samples have been added to the appender, so we can track the time it took.
	i.metrics.appenderAddDuration.Observe(time.Since(startAppend).Seconds())

	spanlog.DebugLog(
		"event", "start commit",
		"succeededSamplesCount", stats.succeededSamplesCount,
		"failedSamplesCount", stats.failedSamplesCount,
		"succeededExemplarsCount", stats.succeededExemplarsCount,
		"failedExemplarsCount", stats.failedExemplarsCount,
	)

	startCommit := time.Now()
	if err := app.Commit(); err != nil {
		return nil, wrapOrAnnotateWithUser(err, userID)
	}

	commitDuration := time.Since(startCommit)
	i.metrics.appenderCommitDuration.Observe(commitDuration.Seconds())
	spanlog.DebugLog("event", "complete commit", "commitDuration", commitDuration.String())

	// If only invalid samples are pushed, don't change "last update", as TSDB was not modified.
	if stats.succeededSamplesCount > 0 {
		db.setLastUpdate(time.Now())
	}

	// Increment metrics only if the samples have been successfully committed.
	// If the code didn't reach this point, it means that we returned an error
	// which will be converted into an HTTP 5xx and the client should/will retry.
	i.metrics.ingestedSamples.WithLabelValues(userID).Add(float64(stats.succeededSamplesCount))
	i.metrics.ingestedSamplesFail.WithLabelValues(userID).Add(float64(stats.failedSamplesCount))
	i.metrics.ingestedExemplars.Add(float64(stats.succeededExemplarsCount))
	i.metrics.ingestedExemplarsFail.Add(float64(stats.failedExemplarsCount))
	i.appendedSamplesStats.Inc(int64(stats.succeededSamplesCount))
	i.appendedExemplarsStats.Inc(int64(stats.succeededExemplarsCount))

	group := i.activeGroups.UpdateActiveGroupTimestamp(userID, validation.GroupLabel(i.limits, userID, req.Timeseries), startAppend)

	i.updateMetricsFromPushStats(userID, group, &stats, req.Source, db, i.metrics.discarded)

	if firstPartialErr != nil {
		code := http.StatusBadRequest
		errWithUser := wrapOrAnnotateWithUser(firstPartialErr, userID)
		return &mimirpb.WriteResponse{}, newErrorWithHTTPStatus(errWithUser, code)
	}

	return &mimirpb.WriteResponse{}, nil
}

func (i *Ingester) updateMetricsFromPushStats(userID string, group string, stats *pushStats, samplesSource mimirpb.WriteRequest_SourceEnum, db *userTSDB, discarded *discardedMetrics) {
	if stats.sampleOutOfBoundsCount > 0 {
		discarded.sampleOutOfBounds.WithLabelValues(userID, group).Add(float64(stats.sampleOutOfBoundsCount))
	}
	if stats.sampleOutOfOrderCount > 0 {
		discarded.sampleOutOfOrder.WithLabelValues(userID, group).Add(float64(stats.sampleOutOfOrderCount))
	}
	if stats.sampleTooOldCount > 0 {
		discarded.sampleTooOld.WithLabelValues(userID, group).Add(float64(stats.sampleTooOldCount))
	}
	if stats.sampleTooFarInFutureCount > 0 {
		discarded.sampleTooFarInFuture.WithLabelValues(userID, group).Add(float64(stats.sampleTooFarInFutureCount))
	}
	if stats.newValueForTimestampCount > 0 {
		discarded.newValueForTimestamp.WithLabelValues(userID, group).Add(float64(stats.newValueForTimestampCount))
	}
	if stats.perUserSeriesLimitCount > 0 {
		discarded.perUserSeriesLimit.WithLabelValues(userID, group).Add(float64(stats.perUserSeriesLimitCount))
	}
	if stats.perMetricSeriesLimitCount > 0 {
		discarded.perMetricSeriesLimit.WithLabelValues(userID, group).Add(float64(stats.perMetricSeriesLimitCount))
	}
	if stats.succeededSamplesCount > 0 {
		i.ingestionRate.Add(int64(stats.succeededSamplesCount))

		if samplesSource == mimirpb.RULE {
			db.ingestedRuleSamples.Add(int64(stats.succeededSamplesCount))
		} else {
			db.ingestedAPISamples.Add(int64(stats.succeededSamplesCount))
		}
	}
}

// pushSamplesToAppender appends samples and exemplars to the appender. Most errors are handled via updateFirstPartial function,
// but in case of unhandled errors, appender is rolled back and such error is returned.
func (i *Ingester) pushSamplesToAppender(userID string, timeseries []mimirpb.PreallocTimeseries, app extendedAppender, startAppend time.Time,
	stats *pushStats, updateFirstPartial func(errFn func() error), activeSeries *activeseries.ActiveSeries,
	outOfOrderWindow time.Duration, minAppendTimeAvailable bool, minAppendTime int64) error {

	// Return true if handled as soft error, and we can ingest more series.
	handleAppendError := func(err error, timestamp int64, labels []mimirpb.LabelAdapter) bool {
		stats.failedSamplesCount++

		// Check if the error is a soft error we can proceed on. If so, we keep track
		// of it, so that we can return it back to the distributor, which will return a
		// 400 error to the client. The client (Prometheus) will not retry on 400, and
		// we actually ingested all samples which haven't failed.
		//nolint:errorlint // We don't expect the cause error to be wrapped.
		switch cause := errors.Cause(err); cause {
		case storage.ErrOutOfBounds:
			stats.sampleOutOfBoundsCount++
			updateFirstPartial(func() error {
				return i.errorSamplers.sampleTimestampTooOld.WrapError(newIngestErrSampleTimestampTooOld(model.Time(timestamp), labels))
			})
			return true

		case storage.ErrOutOfOrderSample:
			stats.sampleOutOfOrderCount++
			updateFirstPartial(func() error {
				return i.errorSamplers.sampleOutOfOrder.WrapError(newIngestErrSampleOutOfOrder(model.Time(timestamp), labels))
			})
			return true

		case storage.ErrTooOldSample:
			stats.sampleTooOldCount++
			updateFirstPartial(func() error {
				return i.errorSamplers.sampleTimestampTooOldOOOEnabled.WrapError(newIngestErrSampleTimestampTooOldOOOEnabled(model.Time(timestamp), labels, outOfOrderWindow))
			})
			return true

		case globalerror.SampleTooFarInFuture:
			stats.sampleTooFarInFutureCount++
			updateFirstPartial(func() error {
				return i.errorSamplers.sampleTimestampTooFarInFuture.WrapError(newIngestErrSampleTimestampTooFarInFuture(model.Time(timestamp), labels))
			})
			return true

		case storage.ErrDuplicateSampleForTimestamp:
			stats.newValueForTimestampCount++
			updateFirstPartial(func() error {
				return i.errorSamplers.sampleDuplicateTimestamp.WrapError(newIngestErrSampleDuplicateTimestamp(model.Time(timestamp), labels))
			})
			return true

		case errMaxSeriesPerUserLimitExceeded:
			stats.perUserSeriesLimitCount++
			updateFirstPartial(func() error {
				return i.errorSamplers.maxSeriesPerUserLimitExceeded.WrapError(formatMaxSeriesPerUserError(i.limiter.limits, userID))
			})
			return true

		case errMaxSeriesPerMetricLimitExceeded:
			stats.perMetricSeriesLimitCount++
			updateFirstPartial(func() error {
				return i.errorSamplers.maxSeriesPerMetricLimitExceeded.WrapError(formatMaxSeriesPerMetricError(i.limiter.limits, mimirpb.FromLabelAdaptersToLabelsWithCopy(labels), userID))
			})
			return true
		}
		return false
	}

	// Fetch limits once per push request both to avoid processing half the request differently.
	var (
		nativeHistogramsIngestionEnabled = i.limits.NativeHistogramsIngestionEnabled(userID)
		maxTimestampMs                   = startAppend.Add(i.limits.CreationGracePeriod(userID)).UnixMilli()
	)

	var builder labels.ScratchBuilder
	var nonCopiedLabels labels.Labels
	for _, ts := range timeseries {
		// The labels must be sorted (in our case, it's guaranteed a write request
		// has sorted labels once hit the ingester).

		// Fast path in case we only have samples and they are all out of bound
		// and out-of-order support is not enabled.
		// TODO(jesus.vazquez) If we had too many old samples we might want to
		// extend the fast path to fail early.
		if nativeHistogramsIngestionEnabled {
			if outOfOrderWindow <= 0 && minAppendTimeAvailable && len(ts.Exemplars) == 0 &&
				(len(ts.Samples) > 0 || len(ts.Histograms) > 0) &&
				allOutOfBoundsFloats(ts.Samples, minAppendTime) &&
				allOutOfBoundsHistograms(ts.Histograms, minAppendTime) {

				stats.failedSamplesCount += len(ts.Samples) + len(ts.Histograms)
				stats.sampleOutOfBoundsCount += len(ts.Samples) + len(ts.Histograms)

				var firstTimestamp int64
				if len(ts.Samples) > 0 {
					firstTimestamp = ts.Samples[0].TimestampMs
				}
				if len(ts.Histograms) > 0 && ts.Histograms[0].Timestamp < firstTimestamp {
					firstTimestamp = ts.Histograms[0].Timestamp
				}

				updateFirstPartial(func() error {
					return i.errorSamplers.sampleTimestampTooOld.WrapError(newIngestErrSampleTimestampTooOld(model.Time(firstTimestamp), ts.Labels))
				})
				continue
			}
		} else {
			// ignore native histograms in the condition and statitics as well
			if outOfOrderWindow <= 0 && minAppendTimeAvailable && len(ts.Exemplars) == 0 &&
				len(ts.Samples) > 0 && allOutOfBoundsFloats(ts.Samples, minAppendTime) {

				stats.failedSamplesCount += len(ts.Samples)
				stats.sampleOutOfBoundsCount += len(ts.Samples)

				firstTimestamp := ts.Samples[0].TimestampMs

				updateFirstPartial(func() error {
					return i.errorSamplers.sampleTimestampTooOld.WrapError(newIngestErrSampleTimestampTooOld(model.Time(firstTimestamp), ts.Labels))
				})
				continue
			}
		}

		// MUST BE COPIED before being retained.
		mimirpb.FromLabelAdaptersOverwriteLabels(&builder, ts.Labels, &nonCopiedLabels)
		hash := nonCopiedLabels.Hash()
		// Look up a reference for this series. The hash passed should be the output of Labels.Hash()
		// and NOT the stable hashing because we use the stable hashing in ingesters only for query sharding.
		ref, copiedLabels := app.GetRef(nonCopiedLabels, hash)

		// To find out if any sample was added to this series, we keep old value.
		oldSucceededSamplesCount := stats.succeededSamplesCount

		for _, s := range ts.Samples {
			var err error

			// Ensure the sample is not too far in the future.
			if s.TimestampMs > maxTimestampMs {
				handleAppendError(globalerror.SampleTooFarInFuture, s.TimestampMs, ts.Labels)
				continue
			}

			// If the cached reference exists, we try to use it.
			if ref != 0 {
				if _, err = app.Append(ref, copiedLabels, s.TimestampMs, s.Value); err == nil {
					stats.succeededSamplesCount++
					continue
				}
			} else {
				// Copy the label set because both TSDB and the active series tracker may retain it.
				copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)

				// Retain the reference in case there are multiple samples for the series.
				if ref, err = app.Append(0, copiedLabels, s.TimestampMs, s.Value); err == nil {
					stats.succeededSamplesCount++
					continue
				}
			}

			// If it's a soft error it will be returned back to the distributor later as a 400.
			if handleAppendError(err, s.TimestampMs, ts.Labels) {
				continue
			}

			// Otherwise, return a 500.
			return err
		}

		numNativeHistogramBuckets := -1
		if nativeHistogramsIngestionEnabled {
			for _, h := range ts.Histograms {
				var (
					err error
					ih  *histogram.Histogram
					fh  *histogram.FloatHistogram
				)

				if h.Timestamp > maxTimestampMs {
					handleAppendError(globalerror.SampleTooFarInFuture, h.Timestamp, ts.Labels)
					continue
				}

				if h.IsFloatHistogram() {
					fh = mimirpb.FromFloatHistogramProtoToFloatHistogram(&h)
				} else {
					ih = mimirpb.FromHistogramProtoToHistogram(&h)
				}

				// If the cached reference exists, we try to use it.
				if ref != 0 {
					if _, err = app.AppendHistogram(ref, copiedLabels, h.Timestamp, ih, fh); err == nil {
						stats.succeededSamplesCount++
						continue
					}
				} else {
					// Copy the label set because both TSDB and the active series tracker may retain it.
					copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)

					// Retain the reference in case there are multiple samples for the series.
					if ref, err = app.AppendHistogram(0, copiedLabels, h.Timestamp, ih, fh); err == nil {
						stats.succeededSamplesCount++
						continue
					}
				}

				if handleAppendError(err, h.Timestamp, ts.Labels) {
					continue
				}

				return err
			}
			numNativeHistograms := len(ts.Histograms)
			if numNativeHistograms > 0 {
				lastNativeHistogram := ts.Histograms[numNativeHistograms-1]
				numFloats := len(ts.Samples)
				if numFloats == 0 || ts.Samples[numFloats-1].TimestampMs < lastNativeHistogram.Timestamp {
					numNativeHistogramBuckets = 0
					for _, span := range lastNativeHistogram.PositiveSpans {
						numNativeHistogramBuckets += int(span.Length)
					}
					for _, span := range lastNativeHistogram.NegativeSpans {
						numNativeHistogramBuckets += int(span.Length)
					}
				}
			}
		}

		if activeSeries != nil && stats.succeededSamplesCount > oldSucceededSamplesCount {
			activeSeries.UpdateSeries(nonCopiedLabels, ref, startAppend, numNativeHistogramBuckets)
		}

		if len(ts.Exemplars) > 0 && i.limits.MaxGlobalExemplarsPerUser(userID) > 0 {
			// app.AppendExemplar currently doesn't create the series, it must
			// already exist.  If it does not then drop.
			if ref == 0 {
				updateFirstPartial(func() error {
					return newIngestErrExemplarMissingSeries(model.Time(ts.Exemplars[0].TimestampMs), ts.Labels, ts.Exemplars[0].Labels)
				})
				stats.failedExemplarsCount += len(ts.Exemplars)
			} else { // Note that else is explicit, rather than a continue in the above if, in case of additional logic post exemplar processing.
				for _, ex := range ts.Exemplars {
					if ex.TimestampMs > maxTimestampMs {
						stats.failedExemplarsCount++
						updateFirstPartial(func() error {
							return newIngestErrExemplarTimestampTooFarInFuture(model.Time(ex.TimestampMs), ts.Labels, ex.Labels)
						})
						continue
					}

					e := exemplar.Exemplar{
						Value:  ex.Value,
						Ts:     ex.TimestampMs,
						HasTs:  true,
						Labels: mimirpb.FromLabelAdaptersToLabelsWithCopy(ex.Labels),
					}

					var err error
					if _, err = app.AppendExemplar(ref, labels.EmptyLabels(), e); err == nil {
						stats.succeededExemplarsCount++
						continue
					}

					// Error adding exemplar
					updateFirstPartial(func() error {
						return wrappedTSDBIngestExemplarOtherErr(err, model.Time(ex.TimestampMs), ts.Labels, ex.Labels)
					})
					stats.failedExemplarsCount++
				}
			}
		}
	}
	return nil
}

func (i *Ingester) QueryExemplars(ctx context.Context, req *client.ExemplarQueryRequest) (*client.ExemplarQueryResponse, error) {
	if err := i.checkRunning(); err != nil {
		return nil, err
	}
	if err := i.checkReadOverloaded(); err != nil {
		return nil, err
	}

	spanlog, ctx := spanlogger.NewWithLogger(ctx, i.logger, "Ingester.QueryExemplars")
	defer spanlog.Finish()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	from, through, matchers, err := client.FromExemplarQueryRequest(req)
	if err != nil {
		return nil, err
	}

	i.metrics.queries.Inc()

	db := i.getTSDB(userID)
	if db == nil {
		return &client.ExemplarQueryResponse{}, nil
	}

	q, err := db.ExemplarQuerier(ctx)
	if err != nil {
		return nil, err
	}

	// It's not required to sort series from a single ingester because series are sorted by the Exemplar Storage before returning from Select.
	res, err := q.Select(from, through, matchers...)
	if err != nil {
		return nil, err
	}

	numExemplars := 0

	result := &client.ExemplarQueryResponse{}
	for _, es := range res {
		ts := mimirpb.TimeSeries{
			Labels:    mimirpb.FromLabelsToLabelAdapters(es.SeriesLabels),
			Exemplars: mimirpb.FromExemplarsToExemplarProtos(es.Exemplars),
		}

		numExemplars += len(ts.Exemplars)
		result.Timeseries = append(result.Timeseries, ts)
	}

	i.metrics.queriedExemplars.Observe(float64(numExemplars))

	return result, nil
}

func (i *Ingester) LabelValues(ctx context.Context, req *client.LabelValuesRequest) (*client.LabelValuesResponse, error) {
	if err := i.checkRunning(); err != nil {
		return nil, err
	}
	if err := i.checkReadOverloaded(); err != nil {
		return nil, err
	}

	labelName, startTimestampMs, endTimestampMs, matchers, err := client.FromLabelValuesRequest(req)
	if err != nil {
		return nil, err
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return &client.LabelValuesResponse{}, nil
	}

	q, err := db.Querier(ctx, startTimestampMs, endTimestampMs)
	if err != nil {
		return nil, err
	}
	defer q.Close()

	vals, _, err := q.LabelValues(labelName, matchers...)
	if err != nil {
		return nil, err
	}

	return &client.LabelValuesResponse{
		LabelValues: vals,
	}, nil
}

func (i *Ingester) LabelNames(ctx context.Context, req *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
	if err := i.checkRunning(); err != nil {
		return nil, err
	}
	if err := i.checkReadOverloaded(); err != nil {
		return nil, err
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return &client.LabelNamesResponse{}, nil
	}

	mint, maxt, matchers, err := client.FromLabelNamesRequest(req)
	if err != nil {
		return nil, err
	}

	q, err := db.Querier(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}
	defer q.Close()

	names, _, err := q.LabelNames(matchers...)
	if err != nil {
		return nil, err
	}

	return &client.LabelNamesResponse{
		LabelNames: names,
	}, nil
}

// MetricsForLabelMatchers implements IngesterServer.
func (i *Ingester) MetricsForLabelMatchers(ctx context.Context, req *client.MetricsForLabelMatchersRequest) (*client.MetricsForLabelMatchersResponse, error) {
	if err := i.checkRunning(); err != nil {
		return nil, err
	}
	if err := i.checkReadOverloaded(); err != nil {
		return nil, err
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return &client.MetricsForLabelMatchersResponse{}, nil
	}

	// Parse the request
	matchersSet, err := client.FromMetricsForLabelMatchersRequest(req)
	if err != nil {
		return nil, err
	}

	mint, maxt := req.StartTimestampMs, req.EndTimestampMs
	q, err := db.Querier(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}
	defer q.Close()

	// Run a query for each matchers set and collect all the results.
	var sets []storage.SeriesSet

	for _, matchers := range matchersSet {
		// Interrupt if the context has been canceled.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		hints := &storage.SelectHints{
			Start: mint,
			End:   maxt,
			Func:  "series", // There is no series function, this token is used for lookups that don't need samples.
		}

		seriesSet := q.Select(true, hints, matchers...)
		sets = append(sets, seriesSet)
	}

	// Generate the response merging all series sets.
	result := &client.MetricsForLabelMatchersResponse{
		Metric: make([]*mimirpb.Metric, 0),
	}

	mergedSet := storage.NewMergeSeriesSet(sets, storage.ChainedSeriesMerge)
	for mergedSet.Next() {
		// Interrupt if the context has been canceled.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		result.Metric = append(result.Metric, &mimirpb.Metric{
			Labels: mimirpb.FromLabelsToLabelAdapters(mergedSet.At().Labels()),
		})
	}

	return result, nil
}

func (i *Ingester) UserStats(ctx context.Context, req *client.UserStatsRequest) (*client.UserStatsResponse, error) {
	if err := i.checkRunning(); err != nil {
		return nil, err
	}
	if err := i.checkReadOverloaded(); err != nil {
		return nil, err
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return &client.UserStatsResponse{}, nil
	}

	return createUserStats(db, req)
}

func (i *Ingester) AllUserStats(_ context.Context, req *client.UserStatsRequest) (*client.UsersStatsResponse, error) {
	if err := i.checkRunning(); err != nil {
		return nil, err
	}

	i.tsdbsMtx.RLock()
	defer i.tsdbsMtx.RUnlock()

	users := i.tsdbs

	response := &client.UsersStatsResponse{
		Stats: make([]*client.UserIDStatsResponse, 0, len(users)),
	}
	for userID, db := range users {
		userStats, err := createUserStats(db, req)
		if err != nil {
			return nil, err
		}
		response.Stats = append(response.Stats, &client.UserIDStatsResponse{
			UserId: userID,
			Data:   userStats,
		})
	}
	return response, nil
}

// we defined to use the limit of 1 MB because we have default limit for the GRPC message that is 4 MB.
// So, 1 MB limit will prevent reaching the limit and won't affect performance significantly.
const labelNamesAndValuesTargetSizeBytes = 1 * 1024 * 1024

func (i *Ingester) LabelNamesAndValues(request *client.LabelNamesAndValuesRequest, server client.Ingester_LabelNamesAndValuesServer) error {
	if err := i.checkRunning(); err != nil {
		return err
	}
	if err := i.checkReadOverloaded(); err != nil {
		return err
	}

	userID, err := tenant.TenantID(server.Context())
	if err != nil {
		return err
	}
	db := i.getTSDB(userID)
	if db == nil {
		return nil
	}
	index, err := db.Head().Index()
	if err != nil {
		return err
	}
	defer index.Close()
	matchers, err := client.FromLabelMatchers(request.GetMatchers())
	if err != nil {
		return err
	}
	return labelNamesAndValues(index, matchers, labelNamesAndValuesTargetSizeBytes, server)
}

// labelValuesCardinalityTargetSizeBytes is the maximum allowed size in bytes for label cardinality response.
// We arbitrarily set it to 1mb to avoid reaching the actual gRPC default limit (4mb).
const labelValuesCardinalityTargetSizeBytes = 1 * 1024 * 1024

func (i *Ingester) LabelValuesCardinality(req *client.LabelValuesCardinalityRequest, srv client.Ingester_LabelValuesCardinalityServer) error {
	if err := i.checkRunning(); err != nil {
		return err
	}
	if err := i.checkReadOverloaded(); err != nil {
		return err
	}

	userID, err := tenant.TenantID(srv.Context())
	if err != nil {
		return err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return nil
	}
	idx, err := db.Head().Index()
	if err != nil {
		return err
	}
	defer idx.Close()

	matchers, err := client.FromLabelMatchers(req.GetMatchers())
	if err != nil {
		return err
	}

	var postingsForMatchersFn func(ix tsdb.IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error)
	switch req.GetCountMethod() {
	case client.IN_MEMORY:
		postingsForMatchersFn = tsdb.PostingsForMatchers
	case client.ACTIVE:
		postingsForMatchersFn = func(ix tsdb.IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error) {
			postings, err := tsdb.PostingsForMatchers(ix, ms...)
			if err != nil {
				return nil, err
			}
			return activeseries.NewPostings(db.activeSeries, postings), nil
		}
	default:
		return fmt.Errorf("unknown count method %q", req.GetCountMethod())
	}

	return labelValuesCardinality(
		req.GetLabelNames(),
		matchers,
		idx,
		postingsForMatchersFn,
		labelValuesCardinalityTargetSizeBytes,
		srv,
	)
}

func createUserStats(db *userTSDB, req *client.UserStatsRequest) (*client.UserStatsResponse, error) {
	apiRate := db.ingestedAPISamples.Rate()
	ruleRate := db.ingestedRuleSamples.Rate()

	var series uint64
	switch req.GetCountMethod() {
	case client.IN_MEMORY:
		series = db.Head().NumSeries()
	case client.ACTIVE:
		activeSeries, _, _ := db.activeSeries.Active()
		series = uint64(activeSeries)
	default:
		return nil, fmt.Errorf("unknown count method %q", req.GetCountMethod())
	}

	return &client.UserStatsResponse{
		IngestionRate:     apiRate + ruleRate,
		ApiIngestionRate:  apiRate,
		RuleIngestionRate: ruleRate,
		NumSeries:         series,
	}, nil
}

const queryStreamBatchMessageSize = 1 * 1024 * 1024

// QueryStream streams metrics from a TSDB. This implements the client.IngesterServer interface
func (i *Ingester) QueryStream(req *client.QueryRequest, stream client.Ingester_QueryStreamServer) error {
	if err := i.checkRunning(); err != nil {
		return err
	}
	if err := i.checkReadOverloaded(); err != nil {
		return err
	}

	spanlog, ctx := spanlogger.NewWithLogger(stream.Context(), i.logger, "Ingester.QueryStream")
	defer spanlog.Finish()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}

	from, through, matchers, err := client.FromQueryRequest(req)
	if err != nil {
		return err
	}

	// Check if query sharding is enabled for this query. If so, we need to remove the
	// query sharding label from matchers and pass the shard info down the query execution path.
	shard, matchers, err := sharding.RemoveShardFromMatchers(matchers)
	if err != nil {
		return err
	}

	i.metrics.queries.Inc()

	db := i.getTSDB(userID)
	if db == nil {
		return nil
	}

	numSamples := 0
	numSeries := 0

	streamType := QueryStreamSamples
	if i.cfg.StreamChunksWhenUsingBlocks {
		streamType = QueryStreamChunks
	}

	if i.cfg.StreamTypeFn != nil {
		runtimeType := i.cfg.StreamTypeFn()
		switch runtimeType {
		case QueryStreamChunks:
			streamType = QueryStreamChunks
		case QueryStreamSamples:
			streamType = QueryStreamSamples
		default:
			// no change from config value.
		}
	}

	if streamType == QueryStreamChunks {
		if req.StreamingChunksBatchSize > 0 {
			level.Debug(spanlog).Log("msg", "using executeStreamingQuery")
			numSeries, numSamples, err = i.executeStreamingQuery(ctx, db, int64(from), int64(through), matchers, shard, stream, req.StreamingChunksBatchSize, spanlog)
		} else {
			level.Debug(spanlog).Log("msg", "using executeChunksQuery")
			numSeries, numSamples, err = i.executeChunksQuery(ctx, db, int64(from), int64(through), matchers, shard, stream)
		}
	} else {
		level.Debug(spanlog).Log("msg", "using executeSamplesQuery")
		numSeries, numSamples, err = i.executeSamplesQuery(ctx, db, int64(from), int64(through), matchers, shard, stream)
	}
	if err != nil {
		return err
	}

	i.metrics.queriedSeries.Observe(float64(numSeries))
	i.metrics.queriedSamples.Observe(float64(numSamples))
	level.Debug(spanlog).Log("series", numSeries, "samples", numSamples)
	return nil
}

func (i *Ingester) executeSamplesQuery(ctx context.Context, db *userTSDB, from, through int64, matchers []*labels.Matcher, shard *sharding.ShardSelector, stream client.Ingester_QueryStreamServer) (numSeries, numSamples int, _ error) {
	q, err := db.Querier(ctx, from, through)
	if err != nil {
		return 0, 0, err
	}
	defer q.Close()

	var hints *storage.SelectHints
	if shard != nil {
		hints = configSelectHintsWithShard(initSelectHints(from, through), shard)
	}

	// It's not required to return sorted series because series are sorted by the Mimir querier.
	ss := q.Select(false, hints, matchers...)
	if ss.Err() != nil {
		return 0, 0, ss.Err()
	}

	timeseries := make([]mimirpb.TimeSeries, 0, queryStreamBatchSize)
	batchSizeBytes := 0
	var it chunkenc.Iterator
	for ss.Next() {
		series := ss.At()

		// convert labels to LabelAdapter
		ts := mimirpb.TimeSeries{
			Labels: mimirpb.FromLabelsToLabelAdapters(series.Labels()),
		}

		it = series.Iterator(it)
		for valType := it.Next(); valType != chunkenc.ValNone; valType = it.Next() {
			switch valType {
			case chunkenc.ValFloat:
				t, v := it.At()
				ts.Samples = append(ts.Samples, mimirpb.Sample{Value: v, TimestampMs: t})
			case chunkenc.ValHistogram:
				t, v := it.AtHistogram()
				ts.Histograms = append(ts.Histograms, mimirpb.FromHistogramToHistogramProto(t, v))
			case chunkenc.ValFloatHistogram:
				t, v := it.AtFloatHistogram()
				ts.Histograms = append(ts.Histograms, mimirpb.FromFloatHistogramToHistogramProto(t, v))
			default:
				return 0, 0, fmt.Errorf("unsupported value type: %v", valType)
			}
		}
		numSamples += len(ts.Samples) + len(ts.Histograms)
		numSeries++
		tsSize := ts.Size()

		if (batchSizeBytes > 0 && batchSizeBytes+tsSize > queryStreamBatchMessageSize) || len(timeseries) >= queryStreamBatchSize {
			// Adding this series to the batch would make it too big,
			// flush the data and add it to new batch instead.
			err = client.SendQueryStream(stream, &client.QueryStreamResponse{
				Timeseries: timeseries,
			})
			if err != nil {
				return 0, 0, err
			}

			batchSizeBytes = 0
			timeseries = timeseries[:0]
		}

		timeseries = append(timeseries, ts)
		batchSizeBytes += tsSize
	}

	// Ensure no error occurred while iterating the series set.
	if err := ss.Err(); err != nil {
		return 0, 0, err
	}

	// Finally flush any existing metrics
	if batchSizeBytes != 0 {
		err = client.SendQueryStream(stream, &client.QueryStreamResponse{
			Timeseries: timeseries,
		})
		if err != nil {
			return 0, 0, err
		}
	}

	return numSeries, numSamples, nil
}

// executeChunksQuery streams metrics from a TSDB. This implements the client.IngesterServer interface
func (i *Ingester) executeChunksQuery(ctx context.Context, db *userTSDB, from, through int64, matchers []*labels.Matcher, shard *sharding.ShardSelector, stream client.Ingester_QueryStreamServer) (numSeries, numSamples int, _ error) {
	var q storage.ChunkQuerier
	var err error
	if i.limits.OutOfOrderTimeWindow(db.userID) > 0 {
		q, err = db.UnorderedChunkQuerier(ctx, from, through)
	} else {
		q, err = db.ChunkQuerier(ctx, from, through)
	}
	if err != nil {
		return 0, 0, err
	}
	defer q.Close()

	// Disable chunks trimming, so that we don't have to rewrite chunks which have samples outside
	// the requested from/through range. PromQL engine can handle it.
	hints := initSelectHints(from, through)
	hints = configSelectHintsWithShard(hints, shard)
	hints = configSelectHintsWithDisabledTrimming(hints)

	// It's not required to return sorted series because series are sorted by the Mimir querier.
	ss := q.Select(false, hints, matchers...)
	if ss.Err() != nil {
		return 0, 0, ss.Err()
	}

	chunkSeries := make([]client.TimeSeriesChunk, 0, queryStreamBatchSize)
	batchSizeBytes := 0
	var it chunks.Iterator
	for ss.Next() {
		series := ss.At()

		// convert labels to LabelAdapter
		ts := client.TimeSeriesChunk{
			Labels: mimirpb.FromLabelsToLabelAdapters(series.Labels()),
		}

		it = series.Iterator(it)
		for it.Next() {
			// Chunks are ordered by min time.
			meta := it.At()

			// It is not guaranteed that chunk returned by iterator is populated.
			// For now just return error. We could also try to figure out how to read the chunk.
			if meta.Chunk == nil {
				return 0, 0, errors.Errorf("unfilled chunk returned from TSDB chunk querier")
			}

			ch, err := client.ChunkFromMeta(meta)
			if err != nil {
				return 0, 0, err
			}

			ts.Chunks = append(ts.Chunks, ch)
			numSamples += meta.Chunk.NumSamples()
		}
		numSeries++
		tsSize := ts.Size()

		if (batchSizeBytes > 0 && batchSizeBytes+tsSize > queryStreamBatchMessageSize) || len(chunkSeries) >= queryStreamBatchSize {
			// Adding this series to the batch would make it too big,
			// flush the data and add it to new batch instead.
			err = client.SendQueryStream(stream, &client.QueryStreamResponse{
				Chunkseries: chunkSeries,
			})
			if err != nil {
				return 0, 0, err
			}

			batchSizeBytes = 0
			chunkSeries = chunkSeries[:0]
		}

		chunkSeries = append(chunkSeries, ts)
		batchSizeBytes += tsSize
	}

	// Ensure no error occurred while iterating the series set.
	if err := ss.Err(); err != nil {
		return 0, 0, err
	}

	// Final flush any existing metrics
	if batchSizeBytes != 0 {
		err = client.SendQueryStream(stream, &client.QueryStreamResponse{
			Chunkseries: chunkSeries,
		})
		if err != nil {
			return 0, 0, err
		}
	}

	return numSeries, numSamples, nil
}

func (i *Ingester) executeStreamingQuery(ctx context.Context, db *userTSDB, from, through int64, matchers []*labels.Matcher, shard *sharding.ShardSelector, stream client.Ingester_QueryStreamServer, batchSize uint64, spanlog *spanlogger.SpanLogger) (numSeries, numSamples int, _ error) {
	var q storage.ChunkQuerier
	var err error
	if i.limits.OutOfOrderTimeWindow(db.userID) > 0 {
		q, err = db.UnorderedChunkQuerier(ctx, from, through)
	} else {
		q, err = db.ChunkQuerier(ctx, from, through)
	}
	if err != nil {
		return 0, 0, err
	}

	// The querier must remain open until we've finished streaming chunks.
	defer q.Close()

	allSeries, numSeries, err := i.sendStreamingQuerySeries(q, from, through, matchers, shard, stream)
	if err != nil {
		return 0, 0, err
	}

	level.Debug(spanlog).Log("msg", "finished sending series", "series", numSeries)

	numSamples, numChunks, numBatches, err := i.sendStreamingQueryChunks(allSeries, stream, batchSize)
	if err != nil {
		return 0, 0, err
	}

	level.Debug(spanlog).Log("msg", "finished sending chunks", "chunks", numChunks, "batches", numBatches)

	return numSeries, numSamples, nil
}

// chunkSeriesNode is used a build a linked list of slices of series.
// This is useful to avoid lots of allocation when you don't know the
// total number of series upfront.
//
// NOTE: Do not use this struct directly. Get it from getChunkSeriesNode()
// and put it back using putChunkSeriesNode() when you are done using it.
type chunkSeriesNode struct {
	series []storage.ChunkSeries
	next   *chunkSeriesNode
}

// Capacity of the slice in chunkSeriesNode.
const chunkSeriesNodeSize = 1024

// chunkSeriesNodePool is used during streaming queries.
var chunkSeriesNodePool zeropool.Pool[*chunkSeriesNode]

func getChunkSeriesNode() *chunkSeriesNode {
	sn := chunkSeriesNodePool.Get()
	if sn == nil {
		sn = &chunkSeriesNode{
			series: make([]storage.ChunkSeries, 0, chunkSeriesNodeSize),
		}
	}
	return sn
}

func putChunkSeriesNode(sn *chunkSeriesNode) {
	sn.series = sn.series[:0]
	sn.next = nil
	chunkSeriesNodePool.Put(sn)
}

func (i *Ingester) sendStreamingQuerySeries(q storage.ChunkQuerier, from, through int64, matchers []*labels.Matcher, shard *sharding.ShardSelector, stream client.Ingester_QueryStreamServer) (*chunkSeriesNode, int, error) {
	// Disable chunks trimming, so that we don't have to rewrite chunks which have samples outside
	// the requested from/through range. PromQL engine can handle it.
	hints := initSelectHints(from, through)
	hints = configSelectHintsWithShard(hints, shard)
	hints = configSelectHintsWithDisabledTrimming(hints)

	// Series must be sorted so that they can be read by the querier in the order the PromQL engine expects.
	ss := q.Select(true, hints, matchers...)
	if ss.Err() != nil {
		return nil, 0, ss.Err()
	}

	seriesInBatch := make([]client.QueryStreamSeries, 0, queryStreamBatchSize)

	// Why retain the storage.ChunkSeries instead of their chunks.Iterator? If we get the iterators here,
	// we can't re-use them. Re-using iterators has a bigger impact on allocations/memory than trying to
	// avoid holding labels reference in ChunkSeries.
	//
	// It is non-trivial to know the total number of series here, so we use a linked list of slices
	// of series and re-use them for future use as well. Even if we did know total number of series
	// here, maybe re-use of slices is a good idea.
	allSeriesList := getChunkSeriesNode()
	lastSeriesNode := allSeriesList
	seriesCount := 0

	for ss.Next() {
		series := ss.At()

		if len(lastSeriesNode.series) == chunkSeriesNodeSize {
			newNode := getChunkSeriesNode()
			lastSeriesNode.next = newNode
			lastSeriesNode = newNode
		}
		lastSeriesNode.series = append(lastSeriesNode.series, series)
		seriesCount++

		chunkCount, err := series.ChunkCount()
		if err != nil {
			return nil, 0, err
		}

		seriesInBatch = append(seriesInBatch, client.QueryStreamSeries{
			Labels:     mimirpb.FromLabelsToLabelAdapters(series.Labels()),
			ChunkCount: int64(chunkCount),
		})

		if len(seriesInBatch) >= queryStreamBatchSize {
			err := client.SendQueryStream(stream, &client.QueryStreamResponse{
				StreamingSeries: seriesInBatch,
			})
			if err != nil {
				return nil, 0, err
			}

			seriesInBatch = seriesInBatch[:0]
		}
	}

	// Send any remaining series, and signal that there are no more.
	err := client.SendQueryStream(stream, &client.QueryStreamResponse{
		StreamingSeries:     seriesInBatch,
		IsEndOfSeriesStream: true,
	})
	if err != nil {
		return nil, 0, err
	}

	// Ensure no error occurred while iterating the series set.
	if err := ss.Err(); err != nil {
		return nil, 0, err
	}

	return allSeriesList, seriesCount, nil
}

func (i *Ingester) sendStreamingQueryChunks(allSeries *chunkSeriesNode, stream client.Ingester_QueryStreamServer, batchSize uint64) (int, int, int, error) {
	var (
		it             chunks.Iterator
		seriesIdx      = -1
		currNode       = allSeries
		numSamples     = 0
		numChunks      = 0
		numBatches     = 0
		seriesInBatch  = make([]client.QueryStreamSeriesChunks, 0, batchSize)
		batchSizeBytes = 0
	)

	for currNode != nil {
		for _, series := range currNode.series {
			seriesIdx++
			seriesChunks := client.QueryStreamSeriesChunks{
				SeriesIndex: uint64(seriesIdx),
			}

			it = series.Iterator(it)

			for it.Next() {
				meta := it.At()

				// It is not guaranteed that chunk returned by iterator is populated.
				// For now just return error. We could also try to figure out how to read the chunk.
				if meta.Chunk == nil {
					return 0, 0, 0, errors.Errorf("unfilled chunk returned from TSDB chunk querier")
				}

				ch, err := client.ChunkFromMeta(meta)
				if err != nil {
					return 0, 0, 0, err
				}

				seriesChunks.Chunks = append(seriesChunks.Chunks, ch)
				numSamples += meta.Chunk.NumSamples()
			}

			numChunks += len(seriesChunks.Chunks)
			msgSize := seriesChunks.Size()

			if (batchSizeBytes > 0 && batchSizeBytes+msgSize > queryStreamBatchMessageSize) || len(seriesInBatch) >= int(batchSize) {
				// Adding this series to the batch would make it too big, flush the data and add it to new batch instead.
				err := client.SendQueryStream(stream, &client.QueryStreamResponse{
					StreamingSeriesChunks: seriesInBatch,
				})
				if err != nil {
					return 0, 0, 0, err
				}

				seriesInBatch = seriesInBatch[:0]
				batchSizeBytes = 0
				numBatches++
			}

			seriesInBatch = append(seriesInBatch, seriesChunks)
			batchSizeBytes += msgSize
		}

		toBePutInPool := currNode
		currNode = currNode.next
		putChunkSeriesNode(toBePutInPool)
	}

	// Send any remaining series.
	if batchSizeBytes != 0 {
		err := client.SendQueryStream(stream, &client.QueryStreamResponse{
			StreamingSeriesChunks: seriesInBatch,
		})
		if err != nil {
			return 0, 0, 0, err
		}
		numBatches++
	}

	return numSamples, numChunks, numBatches, nil
}

func (i *Ingester) getTSDB(userID string) *userTSDB {
	i.tsdbsMtx.RLock()
	defer i.tsdbsMtx.RUnlock()
	db := i.tsdbs[userID]
	return db
}

// List all users for which we have a TSDB. We do it here in order
// to keep the mutex locked for the shortest time possible.
func (i *Ingester) getTSDBUsers() []string {
	i.tsdbsMtx.RLock()
	defer i.tsdbsMtx.RUnlock()

	ids := make([]string, 0, len(i.tsdbs))
	for userID := range i.tsdbs {
		ids = append(ids, userID)
	}

	return ids
}

func (i *Ingester) getOrCreateTSDB(userID string, force bool) (*userTSDB, error) {
	db := i.getTSDB(userID)
	if db != nil {
		return db, nil
	}

	i.tsdbsMtx.Lock()
	defer i.tsdbsMtx.Unlock()

	// Check again for DB in the event it was created in-between locks
	var ok bool
	db, ok = i.tsdbs[userID]
	if ok {
		return db, nil
	}

	// We're ready to create the TSDB, however we must be sure that the ingester
	// is in the ACTIVE state, otherwise it may conflict with the transfer in/out.
	// The TSDB is created when the first series is pushed and this shouldn't happen
	// to a non-ACTIVE ingester, however we want to protect from any bug, cause we
	// may have data loss or TSDB WAL corruption if the TSDB is created before/during
	// a transfer in occurs.
	if ingesterState := i.lifecycler.GetState(); !force && ingesterState != ring.ACTIVE {
		return nil, fmt.Errorf(errTSDBCreateIncompatibleState, ingesterState)
	}

	gl := i.getInstanceLimits()
	if gl != nil && gl.MaxInMemoryTenants > 0 {
		if users := int64(len(i.tsdbs)); users >= gl.MaxInMemoryTenants {
			i.metrics.rejected.WithLabelValues(reasonIngesterMaxTenants).Inc()
			return nil, errMaxTenantsReached
		}
	}

	// Create the database and a shipper for a user
	db, err := i.createTSDB(userID, 0)
	if err != nil {
		return nil, err
	}

	// Add the db to list of user databases
	i.tsdbs[userID] = db
	i.metrics.memUsers.Inc()

	return db, nil
}

// createTSDB creates a TSDB for a given userID, and returns the created db.
func (i *Ingester) createTSDB(userID string, walReplayConcurrency int) (*userTSDB, error) {
	tsdbPromReg := prometheus.NewRegistry()
	udir := i.cfg.BlocksStorageConfig.TSDB.BlocksDir(userID)
	userLogger := util_log.WithUserID(userID, i.logger)

	blockRanges := i.cfg.BlocksStorageConfig.TSDB.BlockRanges.ToMilliseconds()
	matchersConfig := i.limits.ActiveSeriesCustomTrackersConfig(userID)

	userDB := &userTSDB{
		userID:              userID,
		activeSeries:        activeseries.NewActiveSeries(activeseries.NewMatchers(matchersConfig), i.cfg.ActiveSeriesMetrics.IdleTimeout),
		seriesInMetric:      newMetricCounter(i.limiter, i.cfg.getIgnoreSeriesLimitForMetricNamesMap()),
		ingestedAPISamples:  util_math.NewEWMARate(0.2, i.cfg.RateUpdatePeriod),
		ingestedRuleSamples: util_math.NewEWMARate(0.2, i.cfg.RateUpdatePeriod),
		instanceLimitsFn:    i.getInstanceLimits,
		instanceSeriesCount: &i.seriesCount,
		instanceErrors:      i.metrics.rejected,
		blockMinRetention:   i.cfg.BlocksStorageConfig.TSDB.Retention,
	}

	maxExemplars := i.limiter.convertGlobalToLocalLimit(userID, i.limits.MaxGlobalExemplarsPerUser(userID))
	oooTW := i.limits.OutOfOrderTimeWindow(userID)
	// Create a new user database
	db, err := tsdb.Open(udir, userLogger, tsdbPromReg, &tsdb.Options{
		RetentionDuration:                  i.cfg.BlocksStorageConfig.TSDB.Retention.Milliseconds(),
		MinBlockDuration:                   blockRanges[0],
		MaxBlockDuration:                   blockRanges[len(blockRanges)-1],
		NoLockfile:                         true,
		StripeSize:                         i.cfg.BlocksStorageConfig.TSDB.StripeSize,
		HeadChunksWriteBufferSize:          i.cfg.BlocksStorageConfig.TSDB.HeadChunksWriteBufferSize,
		HeadChunksEndTimeVariance:          i.cfg.BlocksStorageConfig.TSDB.HeadChunksEndTimeVariance,
		WALCompression:                     i.cfg.BlocksStorageConfig.TSDB.WALCompressionType(),
		WALSegmentSize:                     i.cfg.BlocksStorageConfig.TSDB.WALSegmentSizeBytes,
		WALReplayConcurrency:               walReplayConcurrency,
		SeriesLifecycleCallback:            userDB,
		BlocksToDelete:                     userDB.blocksToDelete,
		EnableExemplarStorage:              true, // enable for everyone so we can raise the limit later
		MaxExemplars:                       int64(maxExemplars),
		SeriesHashCache:                    i.seriesHashCache,
		EnableMemorySnapshotOnShutdown:     i.cfg.BlocksStorageConfig.TSDB.MemorySnapshotOnShutdown,
		IsolationDisabled:                  true,
		HeadChunksWriteQueueSize:           i.cfg.BlocksStorageConfig.TSDB.HeadChunksWriteQueueSize,
		AllowOverlappingCompaction:         false,                // always false since Mimir only uploads lvl 1 compacted blocks
		OutOfOrderTimeWindow:               oooTW.Milliseconds(), // The unit must be same as our timestamps.
		OutOfOrderCapMax:                   int64(i.cfg.BlocksStorageConfig.TSDB.OutOfOrderCapacityMax),
		HeadPostingsForMatchersCacheTTL:    i.cfg.BlocksStorageConfig.TSDB.HeadPostingsForMatchersCacheTTL,
		HeadPostingsForMatchersCacheSize:   i.cfg.BlocksStorageConfig.TSDB.HeadPostingsForMatchersCacheSize,
		HeadPostingsForMatchersCacheForce:  i.cfg.BlocksStorageConfig.TSDB.HeadPostingsForMatchersCacheForce,
		BlockPostingsForMatchersCacheTTL:   i.cfg.BlocksStorageConfig.TSDB.BlockPostingsForMatchersCacheTTL,
		BlockPostingsForMatchersCacheSize:  i.cfg.BlocksStorageConfig.TSDB.BlockPostingsForMatchersCacheSize,
		BlockPostingsForMatchersCacheForce: i.cfg.BlocksStorageConfig.TSDB.BlockPostingsForMatchersCacheForce,
		EnableNativeHistograms:             i.limits.NativeHistogramsIngestionEnabled(userID),
	}, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open TSDB: %s", udir)
	}
	db.DisableCompactions() // we will compact on our own schedule

	// Run compaction before using this TSDB. If there is data in head that needs to be put into blocks,
	// this will actually create the blocks. If there is no data (empty TSDB), this is a no-op, although
	// local blocks compaction may still take place if configured.
	level.Info(userLogger).Log("msg", "Running compaction after WAL replay")
	err = db.Compact()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to compact TSDB: %s", udir)
	}

	userDB.db = db
	// We set the limiter here because we don't want to limit
	// series during WAL replay.
	userDB.limiter = i.limiter

	// If head is empty (eg. new TSDB), don't close it right after.
	lastUpdateTime := time.Now()
	if db.Head().NumSeries() > 0 {
		// If there are series in the head, use max time from head. If this time is too old,
		// TSDB will be eligible for flushing and closing sooner, unless more data is pushed to it quickly.
		//
		// If TSDB's maxTime is in the future, ignore it. If we set "lastUpdate" to very distant future, it would prevent
		// us from detecting TSDB as idle for a very long time.
		headMaxTime := time.UnixMilli(db.Head().MaxTime())
		if headMaxTime.Before(lastUpdateTime) {
			lastUpdateTime = headMaxTime
		}
	}
	userDB.setLastUpdate(lastUpdateTime)

	// Create a new shipper for this database
	if i.cfg.BlocksStorageConfig.TSDB.IsBlocksShippingEnabled() {
		userDB.shipper = newShipper(
			userLogger,
			i.limits,
			userID,
			i.shipperMetrics,
			udir,
			bucket.NewUserBucketClient(userID, i.bucket, i.limits),
			block.ReceiveSource,
		)

		// Initialise the shipper blocks cache.
		if err := userDB.updateCachedShippedBlocks(); err != nil {
			level.Error(userLogger).Log("msg", "failed to update cached shipped blocks after shipper initialisation", "err", err)
		}
	}

	i.tsdbMetrics.setRegistryForUser(userID, tsdbPromReg)
	return userDB, nil
}

func (i *Ingester) closeAllTSDB() {
	i.tsdbsMtx.Lock()

	wg := &sync.WaitGroup{}
	wg.Add(len(i.tsdbs))

	// Concurrently close all users TSDB
	for userID, userDB := range i.tsdbs {
		userID := userID

		go func(db *userTSDB) {
			defer wg.Done()

			if err := db.Close(); err != nil {
				level.Warn(i.logger).Log("msg", "unable to close TSDB", "err", err, "user", userID)
				return
			}

			// Now that the TSDB has been closed, we should remove it from the
			// set of open ones. This lock acquisition doesn't deadlock with the
			// outer one, because the outer one is released as soon as all go
			// routines are started.
			i.tsdbsMtx.Lock()
			delete(i.tsdbs, userID)
			i.tsdbsMtx.Unlock()

			i.metrics.memUsers.Dec()
			i.metrics.deletePerUserCustomTrackerMetrics(userID, db.activeSeries.CurrentMatcherNames())
		}(userDB)
	}

	// Wait until all Close() completed
	i.tsdbsMtx.Unlock()
	wg.Wait()
}

// openExistingTSDB walks the user tsdb dir, and opens a tsdb for each user. This may start a WAL replay, so we limit the number of
// concurrently opening TSDB.
func (i *Ingester) openExistingTSDB(ctx context.Context) error {
	level.Info(i.logger).Log("msg", "opening existing TSDBs")
	startTime := time.Now()

	queue := make(chan string)
	group, groupCtx := errgroup.WithContext(ctx)

	userIDs, err := i.findUserIDsWithTSDBOnFilesystem()
	if err != nil {
		level.Error(i.logger).Log("msg", "error while finding existing TSDBs", "err", err)
		return err
	}

	if len(userIDs) == 0 {
		return nil
	}

	tsdbOpenConcurrency, tsdbWALReplayConcurrency := getOpenTSDBsConcurrencyConfig(i.cfg.BlocksStorageConfig.TSDB, len(userIDs))

	// Create a pool of workers which will open existing TSDBs.
	for n := 0; n < tsdbOpenConcurrency; n++ {
		group.Go(func() error {
			for userID := range queue {
				db, err := i.createTSDB(userID, tsdbWALReplayConcurrency)
				if err != nil {
					level.Error(i.logger).Log("msg", "unable to open TSDB", "err", err, "user", userID)
					return errors.Wrapf(err, "unable to open TSDB for user %s", userID)
				}

				// Add the database to the map of user databases
				i.tsdbsMtx.Lock()
				i.tsdbs[userID] = db
				i.tsdbsMtx.Unlock()
				i.metrics.memUsers.Inc()
			}

			return nil
		})
	}

	// Spawn a goroutine to place all users with a TSDB found on the filesystem in the queue.
	group.Go(func() error {
		defer close(queue)

		for _, userID := range userIDs {
			// Enqueue the user to be processed.
			select {
			case queue <- userID:
				// Nothing to do.
			case <-groupCtx.Done():
				// Interrupt in case a failure occurred in another goroutine.
				return nil
			}
		}
		return nil
	})

	// Wait for all workers to complete.
	err = group.Wait()
	if err != nil {
		level.Error(i.logger).Log("msg", "error while opening existing TSDBs", "err", err)
		return err
	}

	// Update the usage statistics once all TSDBs have been opened.
	i.updateUsageStats()

	i.metrics.openExistingTSDB.Add(time.Since(startTime).Seconds())
	level.Info(i.logger).Log("msg", "successfully opened existing TSDBs")
	return nil
}

func getOpenTSDBsConcurrencyConfig(tsdbConfig mimir_tsdb.TSDBConfig, userCount int) (tsdbOpenConcurrency, tsdbWALReplayConcurrency int) {
	tsdbOpenConcurrency = mimir_tsdb.DefaultMaxTSDBOpeningConcurrencyOnStartup
	tsdbWALReplayConcurrency = 0
	// When WALReplayConcurrency is enabled, we want to ensure the WAL replay at ingester startup
	// doesn't use more than the configured number of CPU cores. In order to optimize performance
	// both on single tenant and multi tenant Mimir clusters, we use a heuristic to decide whether
	// it's better to parallelize the WAL replay of each single TSDB (low number of tenants) or
	// the WAL replay of multiple TSDBs at the same time (high number of tenants).
	if tsdbConfig.WALReplayConcurrency > 0 {
		if userCount <= maxTSDBOpenWithoutConcurrency {
			tsdbOpenConcurrency = 1
			tsdbWALReplayConcurrency = tsdbConfig.WALReplayConcurrency
		} else {
			tsdbOpenConcurrency = tsdbConfig.WALReplayConcurrency
			tsdbWALReplayConcurrency = 1
		}
	}
	return
}

// findUserIDsWithTSDBOnFilesystem finds all users with a TSDB on the filesystem.
func (i *Ingester) findUserIDsWithTSDBOnFilesystem() ([]string, error) {
	var userIDs []string
	walkErr := filepath.Walk(i.cfg.BlocksStorageConfig.TSDB.Dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// If the root directory doesn't exist, we're OK (not needed to be created upfront).
			if os.IsNotExist(err) && path == i.cfg.BlocksStorageConfig.TSDB.Dir {
				return filepath.SkipDir
			}

			level.Error(i.logger).Log("msg", "an error occurred while iterating the filesystem storing TSDBs", "path", path, "err", err)
			return errors.Wrapf(err, "an error occurred while iterating the filesystem storing TSDBs at %s", path)
		}

		// Skip root dir and all other files
		if path == i.cfg.BlocksStorageConfig.TSDB.Dir || !info.IsDir() {
			return nil
		}

		// Top level directories are assumed to be user TSDBs
		userID := info.Name()
		f, err := os.Open(path)
		if err != nil {
			level.Error(i.logger).Log("msg", "unable to open TSDB dir", "err", err, "user", userID, "path", path)
			return errors.Wrapf(err, "unable to open TSDB dir %s for user %s", path, userID)
		}
		defer f.Close()

		// If the dir is empty skip it
		if _, err := f.Readdirnames(1); err != nil {
			if errors.Is(err, io.EOF) {
				return filepath.SkipDir
			}

			level.Error(i.logger).Log("msg", "unable to read TSDB dir", "err", err, "user", userID, "path", path)
			return errors.Wrapf(err, "unable to read TSDB dir %s for user %s", path, userID)
		}

		// Save userId.
		userIDs = append(userIDs, userID)

		// Don't descend into subdirectories.
		return filepath.SkipDir
	})

	return userIDs, errors.Wrapf(walkErr, "unable to walk directory %s containing existing TSDBs", i.cfg.BlocksStorageConfig.TSDB.Dir)
}

// getOldestUnshippedBlockMetric returns the unix timestamp of the oldest unshipped block or
// 0 if all blocks have been shipped.
func (i *Ingester) getOldestUnshippedBlockMetric() float64 {
	i.tsdbsMtx.RLock()
	defer i.tsdbsMtx.RUnlock()

	oldest := uint64(0)
	for _, db := range i.tsdbs {
		if ts := db.getOldestUnshippedBlockTime(); oldest == 0 || ts < oldest {
			oldest = ts
		}
	}

	return float64(oldest / 1000)
}

func (i *Ingester) minTsdbHeadTimestamp() float64 {
	i.tsdbsMtx.RLock()
	defer i.tsdbsMtx.RUnlock()

	minTime := int64(math.MaxInt64)
	for _, db := range i.tsdbs {
		minTime = util_math.Min(minTime, db.db.Head().MinTime())
	}

	if minTime == math.MaxInt64 {
		return 0
	}
	// convert to seconds
	return float64(minTime) / 1000
}

func (i *Ingester) maxTsdbHeadTimestamp() float64 {
	i.tsdbsMtx.RLock()
	defer i.tsdbsMtx.RUnlock()

	maxTime := int64(math.MinInt64)
	for _, db := range i.tsdbs {
		maxTime = util_math.Max(maxTime, db.db.Head().MaxTime())
	}

	if maxTime == math.MinInt64 {
		return 0
	}
	// convert to seconds
	return float64(maxTime) / 1000
}

func (i *Ingester) shipBlocksLoop(ctx context.Context) error {
	// We add a slight jitter to make sure that if the head compaction interval and ship interval are set to the same
	// value they don't clash (if they both continuously run at the same exact time, the head compaction may not run
	// because can't successfully change the state).
	shipTicker := time.NewTicker(util.DurationWithJitter(i.cfg.BlocksStorageConfig.TSDB.ShipInterval, 0.01))
	defer shipTicker.Stop()

	for {
		select {
		case <-shipTicker.C:
			i.shipBlocks(ctx, nil)

		case req := <-i.shipTrigger:
			i.shipBlocks(ctx, req.users)
			close(req.callback) // Notify back.

		case <-ctx.Done():
			return nil
		}
	}
}

// shipBlocks runs shipping for all users.
func (i *Ingester) shipBlocks(ctx context.Context, allowed *util.AllowedTenants) {
	// Do not ship blocks if the ingester is PENDING or JOINING. It's
	// particularly important for the JOINING state because there could
	// be a blocks transfer in progress (from another ingester) and if we
	// run the shipper in such state we could end up with race conditions.
	if i.lifecycler != nil {
		if ingesterState := i.lifecycler.GetState(); ingesterState == ring.PENDING || ingesterState == ring.JOINING {
			level.Info(i.logger).Log("msg", "TSDB blocks shipping has been skipped because of the current ingester state", "state", ingesterState)
			return
		}
	}

	// Number of concurrent workers is limited in order to avoid to concurrently sync a lot
	// of tenants in a large cluster.
	_ = concurrency.ForEachUser(ctx, i.getTSDBUsers(), i.cfg.BlocksStorageConfig.TSDB.ShipConcurrency, func(ctx context.Context, userID string) error {
		if !allowed.IsAllowed(userID) {
			return nil
		}

		// Get the user's DB. If the user doesn't exist, we skip it.
		userDB := i.getTSDB(userID)
		if userDB == nil || userDB.shipper == nil {
			return nil
		}

		if userDB.deletionMarkFound.Load() {
			return nil
		}

		if time.Since(time.Unix(userDB.lastDeletionMarkCheck.Load(), 0)) > mimir_tsdb.DeletionMarkCheckInterval {
			// Even if check fails with error, we don't want to repeat it too often.
			userDB.lastDeletionMarkCheck.Store(time.Now().Unix())

			deletionMarkExists, err := mimir_tsdb.TenantDeletionMarkExists(ctx, i.bucket, userID)
			if err != nil {
				// If we cannot check for deletion mark, we continue anyway, even though in production shipper will likely fail too.
				// This however simplifies unit tests, where tenant deletion check is enabled by default, but tests don't setup bucket.
				level.Warn(i.logger).Log("msg", "failed to check for tenant deletion mark before shipping blocks", "user", userID, "err", err)
			} else if deletionMarkExists {
				userDB.deletionMarkFound.Store(true)

				level.Info(i.logger).Log("msg", "tenant deletion mark exists, not shipping blocks", "user", userID)
				return nil
			}
		}

		// Run the shipper's Sync() to upload unshipped blocks. Make sure the TSDB state is active, in order to
		// avoid any race condition with closing idle TSDBs.
		if ok, s := userDB.changeState(active, activeShipping); !ok {
			level.Info(i.logger).Log("msg", "shipper skipped because the TSDB is not active", "user", userID, "state", s.String())
			return nil
		}
		defer userDB.changeState(activeShipping, active)

		uploaded, err := userDB.shipper.Sync(ctx)
		if err != nil {
			level.Warn(i.logger).Log("msg", "shipper failed to synchronize TSDB blocks with the storage", "user", userID, "uploaded", uploaded, "err", err)
		} else {
			level.Debug(i.logger).Log("msg", "shipper successfully synchronized TSDB blocks with storage", "user", userID, "uploaded", uploaded)
		}

		// The shipper meta file could be updated even if the Sync() returned an error,
		// so it's safer to update it each time at least a block has been uploaded.
		// Moreover, the shipper meta file could be updated even if no blocks are uploaded
		// (eg. blocks removed due to retention) but doesn't cause any harm not updating
		// the cached list of blocks in such case, so we're not handling it.
		if uploaded > 0 {
			if err := userDB.updateCachedShippedBlocks(); err != nil {
				level.Error(i.logger).Log("msg", "failed to update cached shipped blocks after shipper synchronisation", "user", userID, "err", err)
			}
		}

		return nil
	})
}

func (i *Ingester) compactionLoop(ctx context.Context) error {
	// At ingester startup, spread the first compaction over the configured compaction
	// interval. Then, the next compactions will happen at a regular interval. This logic
	// helps to have different ingesters running the compaction at a different time,
	// effectively spreading the compactions over the configured interval.
	firstHeadCompactionInterval := i.cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval
	if i.cfg.BlocksStorageConfig.TSDB.HeadCompactionIntervalJitterEnabled {
		firstHeadCompactionInterval = util.DurationWithNegativeJitter(i.cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval, 1)
	}

	ticker := time.NewTicker(firstHeadCompactionInterval)
	defer ticker.Stop()
	tickerRunOnce := false

	for ctx.Err() == nil {
		select {
		case <-ticker.C:
			// The forcedCompactionMaxTime has no meaning because force=false.
			i.compactBlocks(ctx, false, 0, nil)

			// Check if any TSDB Head should be compacted to reduce the number of in-memory series.
			i.compactBlocksToReduceInMemorySeries(ctx, time.Now())

			// Run it at a regular (configured) interval after the first compaction.
			if !tickerRunOnce {
				ticker.Reset(i.cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval)
				tickerRunOnce = true
			}

		case req := <-i.forceCompactTrigger:
			// Always pass math.MaxInt64 as forcedCompactionMaxTime because we want to compact the whole TSDB head.
			i.compactBlocks(ctx, true, math.MaxInt64, req.users)
			close(req.callback) // Notify back.

		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

// Compacts all compactable blocks. Force flag will force compaction even if head is not compactable yet.
func (i *Ingester) compactBlocks(ctx context.Context, force bool, forcedCompactionMaxTime int64, allowed *util.AllowedTenants) {
	// Don't compact TSDB blocks while JOINING as there may be ongoing blocks transfers.
	// Compaction loop is not running in LEAVING state, so if we get here in LEAVING state, we're flushing blocks.
	if i.lifecycler != nil {
		if ingesterState := i.lifecycler.GetState(); ingesterState == ring.JOINING {
			level.Info(i.logger).Log("msg", "TSDB blocks compaction has been skipped because of the current ingester state", "state", ingesterState)
			return
		}
	}

	_ = concurrency.ForEachUser(ctx, i.getTSDBUsers(), i.cfg.BlocksStorageConfig.TSDB.HeadCompactionConcurrency, func(ctx context.Context, userID string) error {
		if !allowed.IsAllowed(userID) {
			return nil
		}

		userDB := i.getTSDB(userID)
		if userDB == nil {
			return nil
		}

		// Don't do anything, if there is nothing to compact.
		h := userDB.Head()
		if h.NumSeries() == 0 {
			return nil
		}

		var err error

		i.metrics.compactionsTriggered.Inc()

		reason := ""
		switch {
		case force:
			reason = "forced"
			err = userDB.compactHead(i.cfg.BlocksStorageConfig.TSDB.BlockRanges[0].Milliseconds(), forcedCompactionMaxTime)

		case i.compactionIdleTimeout > 0 && userDB.isIdle(time.Now(), i.compactionIdleTimeout):
			reason = "idle"
			level.Info(i.logger).Log("msg", "TSDB is idle, forcing compaction", "user", userID)

			// Always pass math.MaxInt64 as forcedCompactionMaxTime because we want to compact the whole TSDB head.
			err = userDB.compactHead(i.cfg.BlocksStorageConfig.TSDB.BlockRanges[0].Milliseconds(), math.MaxInt64)

		default:
			reason = "regular"
			err = userDB.Compact()
		}

		if err != nil {
			i.metrics.compactionsFailed.Inc()
			level.Warn(i.logger).Log("msg", "TSDB blocks compaction for user has failed", "user", userID, "err", err, "compactReason", reason)
		} else {
			level.Debug(i.logger).Log("msg", "TSDB blocks compaction completed successfully", "user", userID, "compactReason", reason)
		}

		return nil
	})
}

// compactBlocksToReduceInMemorySeries compacts the TSDB Head of the elegible tenants in order to reduce the in-memory series.
func (i *Ingester) compactBlocksToReduceInMemorySeries(ctx context.Context, now time.Time) {
	// Skip if disabled.
	if i.cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinInMemorySeries <= 0 || !i.cfg.ActiveSeriesMetrics.Enabled {
		return
	}

	// No need to prematurely compact TSDB heads if the number of in-memory series is below a critical threshold.
	totalMemorySeries := i.seriesCount.Load()
	if totalMemorySeries < i.cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinInMemorySeries {
		return
	}

	level.Info(i.logger).Log("msg", "the number of in-memory series is higher than the configured early compaction threshold", "in_memory_series", totalMemorySeries, "early_compaction_threshold", i.cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinInMemorySeries)

	// Estimates the series reduction opportunity for each tenant.
	var (
		userIDs     = i.getTSDBUsers()
		estimations = make([]seriesReductionEstimation, 0, len(userIDs))
	)

	for _, userID := range userIDs {
		db := i.getTSDB(userID)
		if db == nil {
			continue
		}

		userMemorySeries := db.Head().NumSeries()
		if userMemorySeries == 0 {
			continue
		}

		// Purge the active series so that the next call to Active() will return the up-to-date count.
		db.activeSeries.Purge(now)

		// Estimate the number of series that would be dropped from the TSDB Head if we would
		// compact the head up until "now - active series idle timeout".
		totalActiveSeries, _, _ := db.activeSeries.Active()
		estimatedSeriesReduction := util_math.Max(0, int64(userMemorySeries)-int64(totalActiveSeries))
		estimations = append(estimations, seriesReductionEstimation{
			userID:              userID,
			estimatedCount:      estimatedSeriesReduction,
			estimatedPercentage: int((uint64(estimatedSeriesReduction) * 100) / userMemorySeries),
		})
	}

	usersToCompact := filterUsersToCompactToReduceInMemorySeries(totalMemorySeries, i.cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinInMemorySeries, i.cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinEstimatedSeriesReductionPercentage, estimations)
	if len(usersToCompact) == 0 {
		level.Info(i.logger).Log("msg", "no viable per-tenant TSDB found to early compact in order to reduce in-memory series")
		return
	}

	level.Info(i.logger).Log("msg", "running TSDB head compaction to reduce the number of in-memory series", "users", strings.Join(usersToCompact, " "))
	forcedCompactionMaxTime := now.Add(-i.cfg.ActiveSeriesMetrics.IdleTimeout).UnixMilli()
	i.compactBlocks(ctx, true, forcedCompactionMaxTime, util.NewAllowedTenants(usersToCompact, nil))
	level.Info(i.logger).Log("msg", "run TSDB head compaction to reduce the number of in-memory series", "before_in_memory_series", totalMemorySeries, "after_in_memory_series", i.seriesCount.Load())
}

type seriesReductionEstimation struct {
	userID              string
	estimatedCount      int64
	estimatedPercentage int
}

func filterUsersToCompactToReduceInMemorySeries(numMemorySeries, earlyCompactionMinSeries int64, earlyCompactionMinPercentage int, estimations []seriesReductionEstimation) []string {
	var (
		usersToCompact        []string
		seriesReductionSum    = int64(0)
		seriesReductionTarget = numMemorySeries - earlyCompactionMinSeries
	)

	// Skip if the estimated series reduction is too low (there would be no big benefit).
	totalEstimatedSeriesReduction := int64(0)
	for _, entry := range estimations {
		totalEstimatedSeriesReduction += entry.estimatedCount
	}

	if (totalEstimatedSeriesReduction*100)/numMemorySeries < int64(earlyCompactionMinPercentage) {
		return nil
	}

	// Compact all TSDBs blocks required to get the number of in-memory series below the threshold and, in addition,
	// all TSDBs where the estimated series reduction is greater than the minimum reduction percentage.
	slices.SortFunc(estimations, func(a, b seriesReductionEstimation) bool {
		return a.estimatedCount > b.estimatedCount
	})

	for _, entry := range estimations {
		if seriesReductionSum < seriesReductionTarget || entry.estimatedPercentage >= earlyCompactionMinPercentage {
			usersToCompact = append(usersToCompact, entry.userID)
			seriesReductionSum += entry.estimatedCount
		}
	}

	return usersToCompact
}

func (i *Ingester) closeAndDeleteIdleUserTSDBs(ctx context.Context) error {
	for _, userID := range i.getTSDBUsers() {
		if ctx.Err() != nil {
			return nil
		}

		result := i.closeAndDeleteUserTSDBIfIdle(userID)

		i.metrics.idleTsdbChecks.WithLabelValues(string(result)).Inc()
	}

	return nil
}

func (i *Ingester) closeAndDeleteUserTSDBIfIdle(userID string) tsdbCloseCheckResult {
	userDB := i.getTSDB(userID)
	if userDB == nil || userDB.shipper == nil {
		// We will not delete local data when not using shipping to storage.
		return tsdbShippingDisabled
	}

	if result := userDB.shouldCloseTSDB(i.cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout); !result.shouldClose() {
		return result
	}

	// This disables pushes and force-compactions. Not allowed to close while shipping is in progress.
	if ok, _ := userDB.changeState(active, closing); !ok {
		return tsdbNotActive
	}

	// If TSDB is fully closed, we will set state to 'closed', which will prevent this defered closing -> active transition.
	defer userDB.changeState(closing, active)

	// Make sure we don't ignore any possible inflight pushes.
	userDB.inFlightAppends.Wait()

	// Verify again, things may have changed during the checks and pushes.
	tenantDeleted := false
	if result := userDB.shouldCloseTSDB(i.cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout); !result.shouldClose() {
		// This will also change TSDB state back to active (via defer above).
		return result
	} else if result == tsdbTenantMarkedForDeletion {
		tenantDeleted = true
	}

	// At this point there are no more pushes to TSDB, and no possible compaction. Normally TSDB is empty,
	// but if we're closing TSDB because of tenant deletion mark, then it may still contain some series.
	// We need to remove these series from series count.
	i.seriesCount.Sub(int64(userDB.Head().NumSeries()))

	dir := userDB.db.Dir()

	if err := userDB.Close(); err != nil {
		level.Error(i.logger).Log("msg", "failed to close idle TSDB", "user", userID, "err", err)
		return tsdbCloseFailed
	}

	level.Info(i.logger).Log("msg", "closed idle TSDB", "user", userID)

	// This will prevent going back to "active" state in deferred statement.
	userDB.changeState(closing, closed)

	// Only remove user from TSDBState when everything is cleaned up
	// This will prevent concurrency problems when cortex are trying to open new TSDB - Ie: New request for a given tenant
	// came in - while closing the tsdb for the same tenant.
	// If this happens now, the request will get reject as the push will not be able to acquire the lock as the tsdb will be
	// in closed state
	defer func() {
		i.tsdbsMtx.Lock()
		delete(i.tsdbs, userID)
		i.tsdbsMtx.Unlock()
	}()

	i.metrics.memUsers.Dec()
	i.tsdbMetrics.removeRegistryForUser(userID)

	i.deleteUserMetadata(userID)
	i.metrics.deletePerUserMetrics(userID)
	i.metrics.deletePerUserCustomTrackerMetrics(userID, userDB.activeSeries.CurrentMatcherNames())

	// And delete local data.
	if err := os.RemoveAll(dir); err != nil {
		level.Error(i.logger).Log("msg", "failed to delete local TSDB", "user", userID, "err", err)
		return tsdbDataRemovalFailed
	}

	if tenantDeleted {
		level.Info(i.logger).Log("msg", "deleted local TSDB, user marked for deletion", "user", userID, "dir", dir)
		return tsdbTenantMarkedForDeletion
	}

	level.Info(i.logger).Log("msg", "deleted local TSDB, due to being idle", "user", userID, "dir", dir)
	return tsdbIdleClosed
}

func (i *Ingester) RemoveGroupMetricsForUser(userID, group string) {
	i.metrics.deletePerGroupMetricsForUser(userID, group)
}

// TransferOut implements ring.FlushTransferer.
func (i *Ingester) TransferOut(_ context.Context) error {
	return ring.ErrTransferDisabled
}

// Flush will flush all data. It is called as part of Lifecycler's shutdown (if flush on shutdown is configured), or from the flusher.
//
// When called as during Lifecycler shutdown, this happens as part of normal Ingester shutdown (see stopping method).
// Samples are not received at this stage. Compaction and Shipping loops have already been stopped as well.
//
// When used from flusher, ingester is constructed in a way that compaction, shipping and receiving of samples is never started.
func (i *Ingester) Flush() {
	level.Info(i.logger).Log("msg", "starting to flush and ship TSDB blocks")

	ctx := context.Background()

	// Always pass math.MaxInt64 as forcedCompactionMaxTime because we want to compact the whole TSDB head.
	i.compactBlocks(ctx, true, math.MaxInt64, nil)
	if i.cfg.BlocksStorageConfig.TSDB.IsBlocksShippingEnabled() {
		i.shipBlocks(ctx, nil)
	}

	level.Info(i.logger).Log("msg", "finished flushing and shipping TSDB blocks")
}

const (
	tenantParam = "tenant"
	waitParam   = "wait"
)

// Blocks version of Flush handler. It force-compacts blocks, and triggers shipping.
func (i *Ingester) FlushHandler(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		level.Warn(i.logger).Log("msg", "failed to parse HTTP request in flush handler", "err", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	tenants := r.Form[tenantParam]

	allowedUsers := util.NewAllowedTenants(tenants, nil)
	run := func() {
		ingCtx := i.BasicService.ServiceContext()
		if ingCtx == nil || ingCtx.Err() != nil {
			level.Info(i.logger).Log("msg", "flushing TSDB blocks: ingester not running, ignoring flush request")
			return
		}

		compactionCallbackCh := make(chan struct{})

		level.Info(i.logger).Log("msg", "flushing TSDB blocks: triggering compaction")
		select {
		case i.forceCompactTrigger <- requestWithUsersAndCallback{users: allowedUsers, callback: compactionCallbackCh}:
			// Compacting now.
		case <-ingCtx.Done():
			level.Warn(i.logger).Log("msg", "failed to compact TSDB blocks, ingester not running anymore")
			return
		}

		// Wait until notified about compaction being finished.
		select {
		case <-compactionCallbackCh:
			level.Info(i.logger).Log("msg", "finished compacting TSDB blocks")
		case <-ingCtx.Done():
			level.Warn(i.logger).Log("msg", "failed to compact TSDB blocks, ingester not running anymore")
			return
		}

		if i.cfg.BlocksStorageConfig.TSDB.IsBlocksShippingEnabled() {
			shippingCallbackCh := make(chan struct{}) // must be new channel, as compactionCallbackCh is closed now.

			level.Info(i.logger).Log("msg", "flushing TSDB blocks: triggering shipping")

			select {
			case i.shipTrigger <- requestWithUsersAndCallback{users: allowedUsers, callback: shippingCallbackCh}:
				// shipping now
			case <-ingCtx.Done():
				level.Warn(i.logger).Log("msg", "failed to ship TSDB blocks, ingester not running anymore")
				return
			}

			// Wait until shipping finished.
			select {
			case <-shippingCallbackCh:
				level.Info(i.logger).Log("msg", "shipping of TSDB blocks finished")
			case <-ingCtx.Done():
				level.Warn(i.logger).Log("msg", "failed to ship TSDB blocks, ingester not running anymore")
				return
			}
		}

		level.Info(i.logger).Log("msg", "flushing TSDB blocks: finished")
	}

	if len(r.Form[waitParam]) > 0 && r.Form[waitParam][0] == "true" {
		// Run synchronously. This simplifies and speeds up tests.
		run()
	} else {
		go run()
	}

	w.WriteHeader(http.StatusNoContent)
}

func wrappedTSDBIngestExemplarOtherErr(ingestErr error, timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) error {
	if ingestErr == nil {
		return nil
	}

	return fmt.Errorf("err: %v. timestamp=%s, series=%s, exemplar=%s", ingestErr, timestamp.Time().UTC().Format(time.RFC3339Nano),
		mimirpb.FromLabelAdaptersToLabels(seriesLabels).String(),
		mimirpb.FromLabelAdaptersToLabels(exemplarLabels).String(),
	)
}

func (i *Ingester) getInstanceLimits() *InstanceLimits {
	// Don't apply any limits while starting. We especially don't want to apply series in memory limit while replaying WAL.
	if i.State() == services.Starting {
		return nil
	}

	if i.cfg.InstanceLimitsFn == nil {
		return &i.cfg.DefaultLimits
	}

	l := i.cfg.InstanceLimitsFn()
	if l == nil {
		return &i.cfg.DefaultLimits
	}

	return l
}

// PrepareShutdownHandler inspects or changes the configuration of the ingester such that when
// it is stopped, it will:
//   - Change the state of ring to stop accepting writes.
//   - Flush all the chunks to long-term storage.
//
// It also creates a file on disk which is used to re-apply the configuration if the
// ingester crashes and restarts before being permanently shutdown.
//
// * `GET` shows the status of this configuration
// * `POST` enables this configuration
// * `DELETE` disables this configuration
func (i *Ingester) PrepareShutdownHandler(w http.ResponseWriter, r *http.Request) {
	// Don't allow callers to change the shutdown configuration while we're in the middle
	// of starting or shutting down.
	if i.State() != services.Running {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	shutdownMarkerPath := shutdownmarker.GetPath(i.cfg.BlocksStorageConfig.TSDB.Dir)
	switch r.Method {
	case http.MethodGet:
		exists, err := shutdownmarker.Exists(shutdownMarkerPath)
		if err != nil {
			level.Error(i.logger).Log("msg", "unable to check for prepare-shutdown marker file", "path", shutdownMarkerPath, "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if exists {
			util.WriteTextResponse(w, "set\n")
		} else {
			util.WriteTextResponse(w, "unset\n")
		}
	case http.MethodPost:
		if err := shutdownmarker.Create(shutdownMarkerPath); err != nil {
			level.Error(i.logger).Log("msg", "unable to create prepare-shutdown marker file", "path", shutdownMarkerPath, "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		i.setPrepareShutdown()
		level.Info(i.logger).Log("msg", "created prepare-shutdown marker file", "path", shutdownMarkerPath)

		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		if err := shutdownmarker.Remove(shutdownMarkerPath); err != nil {
			level.Error(i.logger).Log("msg", "unable to remove prepare-shutdown marker file", "path", shutdownMarkerPath, "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		i.unsetPrepareShutdown()
		level.Info(i.logger).Log("msg", "removed prepare-shutdown marker file", "path", shutdownMarkerPath)

		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// setPrepareShutdown toggles ingester lifecycler config to prepare for shutdown
func (i *Ingester) setPrepareShutdown() {
	i.lifecycler.SetUnregisterOnShutdown(true)
	i.lifecycler.SetFlushOnShutdown(true)
	i.metrics.shutdownMarker.Set(1)
}

func (i *Ingester) unsetPrepareShutdown() {
	i.lifecycler.SetUnregisterOnShutdown(i.cfg.IngesterRing.UnregisterOnShutdown)
	i.lifecycler.SetFlushOnShutdown(i.cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown)
	i.metrics.shutdownMarker.Set(0)
}

// ShutdownHandler triggers the following set of operations in order:
//   - Change the state of ring to stop accepting writes.
//   - Flush all the chunks.
func (i *Ingester) ShutdownHandler(w http.ResponseWriter, _ *http.Request) {
	originalFlush := i.lifecycler.FlushOnShutdown()
	// We want to flush the chunks if transfer fails irrespective of original flag.
	i.lifecycler.SetFlushOnShutdown(true)

	// In the case of an HTTP shutdown, we want to unregister no matter what.
	originalUnregister := i.lifecycler.ShouldUnregisterOnShutdown()
	i.lifecycler.SetUnregisterOnShutdown(true)

	_ = services.StopAndAwaitTerminated(context.Background(), i)
	// Set state back to original.
	i.lifecycler.SetFlushOnShutdown(originalFlush)
	i.lifecycler.SetUnregisterOnShutdown(originalUnregister)

	w.WriteHeader(http.StatusNoContent)
}

// Using block store, the ingester is only available when it is in a Running state. The ingester is not available
// when stopping to prevent any read or writes to the TSDB after the ingester has closed them.
func (i *Ingester) checkRunning() error {
	s := i.State()
	if s == services.Running {
		return nil
	}
	return status.Error(codes.Unavailable, s.String())
}

// Push implements client.IngesterServer
func (i *Ingester) Push(ctx context.Context, req *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	pushReq := push.NewParsedRequest(req)
	pushReq.AddCleanup(func() {
		mimirpb.ReuseSlice(req.Timeseries)
	})
	return i.PushWithCleanup(ctx, pushReq)
}

// pushMetadata returns number of ingested metadata.
func (i *Ingester) pushMetadata(ctx context.Context, userID string, metadata []*mimirpb.MetricMetadata) int {
	ingestedMetadata := 0
	failedMetadata := 0

	userMetadata := i.getOrCreateUserMetadata(userID)
	var firstMetadataErr error
	for _, m := range metadata {
		if m == nil {
			continue
		}
		err := userMetadata.add(m.MetricFamilyName, m)
		if err == nil {
			ingestedMetadata++
			continue
		}

		failedMetadata++
		if firstMetadataErr == nil {
			firstMetadataErr = err
		}
	}

	i.metrics.ingestedMetadata.Add(float64(ingestedMetadata))
	i.metrics.ingestedMetadataFail.Add(float64(failedMetadata))

	// If we have any error with regard to metadata we just log and no-op.
	// We consider metadata a best effort approach, errors here should not stop processing.
	if firstMetadataErr != nil {
		logger := util_log.WithContext(ctx, i.logger)
		level.Warn(logger).Log("msg", "failed to ingest some metadata", "err", firstMetadataErr)
	}

	return ingestedMetadata
}

func (i *Ingester) getOrCreateUserMetadata(userID string) *userMetricsMetadata {
	userMetadata := i.getUserMetadata(userID)
	if userMetadata != nil {
		return userMetadata
	}

	i.usersMetadataMtx.Lock()
	defer i.usersMetadataMtx.Unlock()

	// Ensure it was not created between switching locks.
	userMetadata, ok := i.usersMetadata[userID]
	if !ok {
		userMetadata = newMetadataMap(i.limiter, i.metrics, i.errorSamplers, userID)
		i.usersMetadata[userID] = userMetadata
	}
	return userMetadata
}

func (i *Ingester) getUserMetadata(userID string) *userMetricsMetadata {
	i.usersMetadataMtx.RLock()
	defer i.usersMetadataMtx.RUnlock()
	return i.usersMetadata[userID]
}

func (i *Ingester) deleteUserMetadata(userID string) {
	i.usersMetadataMtx.Lock()
	um := i.usersMetadata[userID]
	delete(i.usersMetadata, userID)
	i.usersMetadataMtx.Unlock()

	if um != nil {
		// We need call purge to update i.metrics.memMetadata correctly (it counts number of metrics with metadata in memory).
		// Passing zero time means purge everything.
		um.purge(time.Time{})
	}
}
func (i *Ingester) getUsersWithMetadata() []string {
	i.usersMetadataMtx.RLock()
	defer i.usersMetadataMtx.RUnlock()

	userIDs := make([]string, 0, len(i.usersMetadata))
	for userID := range i.usersMetadata {
		userIDs = append(userIDs, userID)
	}

	return userIDs
}

func (i *Ingester) purgeUserMetricsMetadata() {
	deadline := time.Now().Add(-i.cfg.MetadataRetainPeriod)

	for _, userID := range i.getUsersWithMetadata() {
		metadata := i.getUserMetadata(userID)
		if metadata == nil {
			continue
		}

		// Remove all metadata that we no longer need to retain.
		metadata.purge(deadline)
	}
}

// MetricsMetadata returns all the metrics metadata of a user.
func (i *Ingester) MetricsMetadata(ctx context.Context, req *client.MetricsMetadataRequest) (*client.MetricsMetadataResponse, error) {
	if err := i.checkRunning(); err != nil {
		return nil, err
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	userMetadata := i.getUserMetadata(userID)

	if userMetadata == nil {
		return &client.MetricsMetadataResponse{}, nil
	}

	return &client.MetricsMetadataResponse{Metadata: userMetadata.toClientMetadata(req)}, nil
}

// CheckReady is the readiness handler used to indicate to k8s when the ingesters
// are ready for the addition or removal of another ingester.
func (i *Ingester) CheckReady(ctx context.Context) error {
	if err := i.checkRunning(); err != nil {
		return fmt.Errorf("ingester not ready: %v", err)
	}
	return i.lifecycler.CheckReady(ctx)
}

func (i *Ingester) RingHandler() http.Handler {
	return i.lifecycler
}

func initSelectHints(start, end int64) *storage.SelectHints {
	return &storage.SelectHints{
		Start: start,
		End:   end,
	}
}

func configSelectHintsWithShard(hints *storage.SelectHints, shard *sharding.ShardSelector) *storage.SelectHints {
	if shard != nil {
		// If query sharding is enabled, we need to pass it along with hints.
		hints.ShardIndex = shard.ShardIndex
		hints.ShardCount = shard.ShardCount
	}
	return hints
}

func configSelectHintsWithDisabledTrimming(hints *storage.SelectHints) *storage.SelectHints {
	hints.DisableTrimming = true
	return hints
}

// allOutOfBounds returns whether all the provided (float) samples are out of bounds.
func allOutOfBoundsFloats(samples []mimirpb.Sample, minValidTime int64) bool {
	for _, s := range samples {
		if s.TimestampMs >= minValidTime {
			return false
		}
	}
	return true
}

// allOutOfBoundsHistograms returns whether all the provided histograms are out of bounds.
func allOutOfBoundsHistograms(histograms []mimirpb.Histogram, minValidTime int64) bool {
	for _, s := range histograms {
		if s.Timestamp >= minValidTime {
			return false
		}
	}
	return true
}

func (i *Ingester) UserRegistryHandler(w http.ResponseWriter, r *http.Request) {
	userID, err := tenant.TenantID(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	reg := i.tsdbMetrics.regs.GetRegistryForTenant(userID)
	if reg == nil {
		http.Error(w, "user registry not found", http.StatusNotFound)
		return
	}

	promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		DisableCompression: true,
		ErrorHandling:      promhttp.HTTPErrorOnError,
		Timeout:            10 * time.Second,
	}).ServeHTTP(w, r)
}

// checkReadOverloaded checks whether the ingester read path is overloaded wrt. CPU and/or memory.
func (i *Ingester) checkReadOverloaded() error {
	if i.utilizationBasedLimiter == nil {
		return nil
	}

	reason := i.utilizationBasedLimiter.LimitingReason()
	if reason == "" {
		return nil
	}

	i.metrics.utilizationLimitedRequests.WithLabelValues(reason).Inc()
	return tooBusyError
}

type utilizationBasedLimiter interface {
	services.Service

	LimitingReason() string
}
