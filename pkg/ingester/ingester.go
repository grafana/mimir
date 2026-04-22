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
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"github.com/thanos-io/objstore"
	"go.opentelemetry.io/otel"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/costattribution"
	"github.com/grafana/mimir/pkg/ingester/activeseries"
	"github.com/grafana/mimir/pkg/ingester/lookupplan"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/ingest"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/usagestats"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/limiter"
	util_math "github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/reactivelimiter"
	"github.com/grafana/mimir/pkg/util/shutdownmarker"
	"github.com/grafana/mimir/pkg/util/validation"
)

var tracer = otel.Tracer("pkg/ingester")

const (
	// Number of timeseries to return in each batch of a QueryStream.
	queryStreamBatchSize = 128

	// Number of chunks to return in each batch of a QueryStream if not set in the request.
	fallbackChunkStreamBatchSize = 256

	// Discarded Metadata metric labels.
	perUserMetadataLimit   = "per_user_metadata_limit"
	perMetricMetadataLimit = "per_metric_metadata_limit"

	// Period at which to attempt purging metadata from memory.
	metadataPurgePeriod = 5 * time.Minute

	// How frequently update the usage statistics.
	usageStatsUpdateInterval = usagestats.DefaultReportSendInterval / 10

	// IngesterRingKey is the key under which we store the ingesters ring in the KVStore.
	IngesterRingKey  = "ring"
	IngesterRingName = "ingester"

	// PartitionRingKey is the key under which we store the partitions ring used by the "ingest storage".
	PartitionRingKey  = "ingester-partitions"
	PartitionRingName = "ingester-partitions"

	// Jitter applied to the idle timeout to prevent compaction in all ingesters concurrently.
	compactionIdleTimeoutJitter = 0.25

	instanceIngestionRateTickInterval = time.Second

	// Reasons for discarding samples
	reasonSampleOutOfOrder       = "sample-out-of-order"
	reasonSampleTooOld           = "sample-too-old"
	reasonSampleTooFarInFuture   = "sample-too-far-in-future"
	reasonNewValueForTimestamp   = "new-value-for-timestamp"
	reasonSampleTimestampTooOld  = "sample-timestamp-too-old"
	reasonPerUserSeriesLimit     = "per_user_series_limit"
	reasonPerMetricSeriesLimit   = "per_metric_series_limit"
	reasonInvalidNativeHistogram = "invalid-native-histogram"
	reasonLabelsNotSorted        = "labels-not-sorted"

	replicationFactorStatsName             = "ingester_replication_factor"
	ringStoreStatsName                     = "ingester_ring_store"
	memorySeriesStatsName                  = "ingester_inmemory_series"
	activeSeriesStatsName                  = "ingester_active_series"
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
	reasonIngesterMaxIngestionRate             = globalerror.IngesterMaxIngestionRate.LabelValue()
	reasonIngesterMaxTenants                   = globalerror.IngesterMaxTenants.LabelValue()
	reasonIngesterMaxInMemorySeries            = globalerror.IngesterMaxInMemorySeries.LabelValue()
	reasonIngesterMaxInflightPushRequests      = globalerror.IngesterMaxInflightPushRequests.LabelValue()
	reasonIngesterMaxInflightPushRequestsBytes = globalerror.IngesterMaxInflightPushRequestsBytes.LabelValue()
	reasonIngesterMaxInflightReadRequests      = globalerror.IngesterMaxInflightReadRequests.LabelValue()
)

// Usage-stats expvars. Initialized as package-global in order to avoid race conditions and panics
// when initializing expvars per-ingester in multiple parallel tests at once.
var (
	// updated in Ingester.updateUsageStats.
	memorySeriesStats                  = usagestats.GetAndResetInt(memorySeriesStatsName)
	memoryTenantsStats                 = usagestats.GetAndResetInt(memoryTenantsStatsName)
	activeSeriesStats                  = usagestats.GetAndResetInt(activeSeriesStatsName)
	tenantsWithOutOfOrderEnabledStat   = usagestats.GetAndResetInt(tenantsWithOutOfOrderEnabledStatName)
	minOutOfOrderTimeWindowSecondsStat = usagestats.GetAndResetInt(minOutOfOrderTimeWindowSecondsStatName)
	maxOutOfOrderTimeWindowSecondsStat = usagestats.GetAndResetInt(maxOutOfOrderTimeWindowSecondsStatName)

	// updated in Ingester.PushWithCleanup.
	appendedSamplesStats   = usagestats.GetAndResetCounter(appendedSamplesStatsName)
	appendedExemplarsStats = usagestats.GetAndResetCounter(appendedExemplarsStatsName)

	// Set in newIngester.
	replicationFactor = usagestats.GetInt(replicationFactorStatsName)
	ringStoreName     = usagestats.GetString(ringStoreStatsName)
)

// BlocksUploader interface is used to have an easy way to mock it in tests.
type BlocksUploader interface {
	Sync(ctx context.Context) (uploaded int, err error)
}

type requestWithUsersAndCallback struct {
	users    *util.AllowList // if nil, all tenants are allowed.
	callback chan<- struct{} // when compaction/shipping is finished, this channel is closed
}

// Config for an Ingester.
type Config struct {
	IngesterRing          RingConfig          `yaml:"ring"`
	IngesterPartitionRing PartitionRingConfig `yaml:"partition_ring" category:"experimental"`

	// Config for metadata purging.
	MetadataRetainPeriod time.Duration `yaml:"metadata_retain_period" category:"advanced"`

	RateUpdatePeriod time.Duration `yaml:"rate_update_period" category:"advanced"`

	ActiveSeriesMetrics activeseries.Config `yaml:",inline"`

	TSDBConfigUpdatePeriod time.Duration `yaml:"tsdb_config_update_period" category:"experimental"`

	BlocksStorageConfig mimir_tsdb.BlocksStorageConfig `yaml:"-"`

	DefaultLimits    InstanceLimits         `yaml:"instance_limits"`
	InstanceLimitsFn func() *InstanceLimits `yaml:"-"`

	IgnoreSeriesLimitForMetricNames string `yaml:"ignore_series_limit_for_metric_names" category:"advanced"`

	ReadPathCPUUtilizationLimit    float64 `yaml:"read_path_cpu_utilization_limit" category:"advanced"`
	ReadPathMemoryUtilizationLimit uint64  `yaml:"read_path_memory_utilization_limit" category:"advanced"`

	ErrorSampleRate int64 `yaml:"error_sample_rate" json:"error_sample_rate" category:"advanced"`

	// UseIngesterOwnedSeriesForLimits was added in 2.12, but we keep it experimental until we decide, what is the correct behaviour
	// when the replication factor and the number of zones don't match. Refer to notes in https://github.com/grafana/mimir/pull/8695 and https://github.com/grafana/mimir/pull/9496
	UseIngesterOwnedSeriesForLimits bool          `yaml:"use_ingester_owned_series_for_limits" category:"experimental"`
	UpdateIngesterOwnedSeries       bool          `yaml:"track_ingester_owned_series" category:"experimental"`
	OwnedSeriesUpdateInterval       time.Duration `yaml:"owned_series_update_interval" category:"experimental"`

	PushCircuitBreaker   CircuitBreakerConfig              `yaml:"push_circuit_breaker"`
	ReadCircuitBreaker   CircuitBreakerConfig              `yaml:"read_circuit_breaker"`
	RejectionPrioritizer reactivelimiter.PrioritizerConfig `yaml:"rejection_prioritizer"`
	PushReactiveLimiter  reactivelimiter.Config            `yaml:"push_reactive_limiter"`
	ReadReactiveLimiter  reactivelimiter.Config            `yaml:"read_reactive_limiter"`

	PushGrpcMethodEnabled bool `yaml:"push_grpc_method_enabled" category:"experimental" doc:"hidden"`

	// This config is dynamically injected because defined outside the ingester config.
	IngestStorageConfig ingest.Config `yaml:"-"`

	// This config can be overridden in tests.
	limitMetricsUpdatePeriod time.Duration `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	cfg.IngesterRing.RegisterFlags(f, logger)
	cfg.IngesterPartitionRing.RegisterFlags(f)
	cfg.DefaultLimits.RegisterFlags(f)
	cfg.ActiveSeriesMetrics.RegisterFlags(f)
	cfg.PushCircuitBreaker.RegisterFlagsWithPrefix("ingester.push-circuit-breaker.", f, circuitBreakerDefaultPushTimeout)
	cfg.ReadCircuitBreaker.RegisterFlagsWithPrefix("ingester.read-circuit-breaker.", f, circuitBreakerDefaultReadTimeout)
	cfg.RejectionPrioritizer.RegisterFlagsWithPrefix("ingester.rejection-prioritizer.", f)
	cfg.PushReactiveLimiter.RegisterFlagsWithPrefix("ingester.push-reactive-limiter.", f)
	cfg.ReadReactiveLimiter.RegisterFlagsWithPrefix("ingester.read-reactive-limiter.", f)

	f.DurationVar(&cfg.MetadataRetainPeriod, "ingester.metadata-retain-period", 10*time.Minute, "Period at which metadata we have not seen will remain in memory before being deleted.")
	f.DurationVar(&cfg.RateUpdatePeriod, "ingester.rate-update-period", 15*time.Second, "Period with which to update the per-tenant ingestion rates.")
	f.DurationVar(&cfg.TSDBConfigUpdatePeriod, "ingester.tsdb-config-update-period", 15*time.Second, "Period with which to update the per-tenant TSDB configuration.")
	f.StringVar(&cfg.IgnoreSeriesLimitForMetricNames, "ingester.ignore-series-limit-for-metric-names", "", "Comma-separated list of metric names, for which the -ingester.max-global-series-per-metric limit will be ignored. Does not affect the -ingester.max-global-series-per-user limit.")
	f.Float64Var(&cfg.ReadPathCPUUtilizationLimit, "ingester.read-path-cpu-utilization-limit", 0, "CPU utilization limit, as CPU cores, for CPU/memory utilization based read request limiting. Use 0 to disable it.")
	f.Uint64Var(&cfg.ReadPathMemoryUtilizationLimit, "ingester.read-path-memory-utilization-limit", 0, "Memory limit, in bytes, for CPU/memory utilization based read request limiting. Use 0 to disable it.")
	f.Int64Var(&cfg.ErrorSampleRate, "ingester.error-sample-rate", 10, "Each error will be logged once in this many times. Use 0 to log all of them.")
	f.BoolVar(&cfg.UseIngesterOwnedSeriesForLimits, "ingester.use-ingester-owned-series-for-limits", false, "When enabled, only series currently owned by ingester according to the ring are used when checking user per-tenant series limit.")
	f.BoolVar(&cfg.UpdateIngesterOwnedSeries, "ingester.track-ingester-owned-series", false, "This option enables tracking of ingester-owned series based on ring state, even if -ingester.use-ingester-owned-series-for-limits is disabled.")
	f.DurationVar(&cfg.OwnedSeriesUpdateInterval, "ingester.owned-series-update-interval", 15*time.Second, "How often to check for ring changes and possibly recompute owned series as a result of detected change.")
	f.BoolVar(&cfg.PushGrpcMethodEnabled, "ingester.push-grpc-method-enabled", true, "Enables Push gRPC method on ingester. Can be only disabled when using ingest-storage to make sure ingesters only receive data from Kafka.")

	// Hardcoded config (can only be overridden in tests).
	cfg.limitMetricsUpdatePeriod = time.Second * 15
}

func (cfg *Config) Validate(log.Logger) error {
	if cfg.ErrorSampleRate < 0 {
		return fmt.Errorf("error sample rate cannot be a negative number")
	}

	// Tokenless mode requires gRPC push to be disabled.
	if cfg.IngesterRing.NumTokens == 0 && cfg.PushGrpcMethodEnabled {
		return fmt.Errorf("ring tokens can only be disabled when gRPC push is disabled")
	}

	if err := cfg.PushReactiveLimiter.Validate(); err != nil {
		return err
	}
	if err := cfg.ReadReactiveLimiter.Validate(); err != nil {
		return err
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

	instanceRing          ring.ReadRing
	lifecycler            ingesterLifecycler
	limits                *validation.Overrides
	limiter               *Limiter
	subservicesWatcher    *services.FailureWatcher
	ownedSeriesService    *ownedSeriesService
	compactionService     services.Service
	shippingService       services.Service
	metricsUpdaterService services.Service
	metadataPurgerService services.Service
	statisticsService     services.Service

	// Index lookup planning
	lookupPlanMetrics lookupplan.Metrics

	// Mimir blocks storage.
	tsdbsMtx sync.RWMutex
	tsdbs    map[string]*userTSDB // tsdb sharded by userID

	bucket objstore.Bucket

	// Ingester ID, used by shipper as external label.
	ingesterID string

	// Metrics shared across all per-tenant shippers.
	shipperMetrics *shipperMetrics

	// Metrics shared across all per-tenant offset catalogues.
	offsetCatalogueMetrics *offsetCatalogueMetrics

	subservicesForPartitionReplay          *services.Manager
	subservicesAfterIngesterRingLifecycler *services.Manager

	activeGroups *util.ActiveGroupsCleanupService

	costAttributionMgr *costattribution.Manager

	tsdbMetrics *mimir_tsdb.TSDBMetrics

	forceCompactTrigger chan requestWithUsersAndCallback
	shipTrigger         chan requestWithUsersAndCallback

	// Maps the per-block series ID with its labels hash.
	seriesHashCache *hashcache.SeriesHashCache

	// Timeout chosen for idle compactions.
	compactionIdleTimeout time.Duration

	// Number of series in memory, across all tenants.
	seriesCount atomic.Int64

	// Tracks the number of compactions in progress.
	numCompactionsInProgress atomic.Uint32

	// For storing metadata ingested.
	usersMetadataMtx sync.RWMutex
	usersMetadata    map[string]*userMetricsMetadata

	// For producing postings caches
	headPostingsForMatchersCacheFactory  tsdb.PostingsForMatchersCacheFactory
	blockPostingsForMatchersCacheFactory tsdb.PostingsForMatchersCacheFactory

	// Rate of pushed samples. Used to limit global samples push rate.
	ingestionRate             *util_math.EwmaRate
	inflightPushRequests      atomic.Int64
	inflightPushRequestsBytes atomic.Int64

	utilizationBasedLimiter utilizationBasedLimiter

	errorSamplers ingesterErrSamplers

	// The following is used by ingest storage (when enabled).
	ingestReader              *ingest.PartitionReader
	ingestPartitionID         int32
	ingestPartitionLifecycler *ring.PartitionInstanceLifecycler

	circuitBreaker  ingesterCircuitBreaker
	reactiveLimiter *ingesterReactiveLimiter
}

func newIngester(cfg Config, limits *validation.Overrides, ingestersRing ring.ReadRing, registerer prometheus.Registerer, logger log.Logger) (*Ingester, error) {
	if cfg.BlocksStorageConfig.Bucket.Backend == bucket.Filesystem {
		level.Warn(logger).Log("msg", "-blocks-storage.backend=filesystem is for development and testing only; you should switch to an external object store for production use or use a shared filesystem")
	}

	bucketClient, err := bucket.NewClient(context.Background(), cfg.BlocksStorageConfig.Bucket, "ingester", logger, registerer)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create the bucket client")
	}

	// Track constant usage stats.
	replicationFactor.Set(int64(cfg.IngesterRing.ReplicationFactor))
	ringStoreName.Set(cfg.IngesterRing.KVStore.Store)

	metrics := mimir_tsdb.NewTSDBMetrics(prometheus.WrapRegistererWithPrefix("cortex_ingester_", registerer), logger)

	return &Ingester{
		cfg:    cfg,
		limits: limits,
		logger: logger,

		instanceRing: ingestersRing,

		tsdbs:                  make(map[string]*userTSDB),
		usersMetadata:          make(map[string]*userMetricsMetadata),
		bucket:                 bucketClient,
		tsdbMetrics:            metrics,
		shipperMetrics:         newShipperMetrics(registerer),
		offsetCatalogueMetrics: newOffsetCatalogueMetrics(registerer),
		forceCompactTrigger:    make(chan requestWithUsersAndCallback),
		shipTrigger:            make(chan requestWithUsersAndCallback),
		seriesHashCache:        hashcache.NewSeriesHashCache(cfg.BlocksStorageConfig.TSDB.SeriesHashCacheMaxBytes),
		headPostingsForMatchersCacheFactory: tsdb.NewPostingsForMatchersCacheFactory(
			tsdb.PostingsForMatchersCacheConfig{
				Shared:                cfg.BlocksStorageConfig.TSDB.SharedPostingsForMatchersCache,
				KeyFunc:               tenant.TenantID,
				Invalidation:          cfg.BlocksStorageConfig.TSDB.HeadPostingsForMatchersCacheInvalidation,
				CacheVersions:         cfg.BlocksStorageConfig.TSDB.HeadPostingsForMatchersCacheVersions,
				TTL:                   cfg.BlocksStorageConfig.TSDB.HeadPostingsForMatchersCacheTTL,
				MaxItems:              cfg.BlocksStorageConfig.TSDB.HeadPostingsForMatchersCacheMaxItems,
				MaxBytes:              cfg.BlocksStorageConfig.TSDB.HeadPostingsForMatchersCacheMaxBytes,
				Force:                 cfg.BlocksStorageConfig.TSDB.HeadPostingsForMatchersCacheForce,
				Metrics:               tsdb.NewPostingsForMatchersCacheMetrics(prometheus.WrapRegistererWithPrefix("cortex_ingester_tsdb_head_", registerer)),
				PostingsClonerFactory: lookupplan.ActualSelectedPostingsClonerFactory{},
			},
		),
		blockPostingsForMatchersCacheFactory: tsdb.NewPostingsForMatchersCacheFactory(
			tsdb.PostingsForMatchersCacheConfig{
				Shared:                cfg.BlocksStorageConfig.TSDB.SharedPostingsForMatchersCache,
				KeyFunc:               tenant.TenantID,
				Invalidation:          false,
				CacheVersions:         0,
				TTL:                   cfg.BlocksStorageConfig.TSDB.BlockPostingsForMatchersCacheTTL,
				MaxItems:              cfg.BlocksStorageConfig.TSDB.BlockPostingsForMatchersCacheMaxItems,
				MaxBytes:              cfg.BlocksStorageConfig.TSDB.BlockPostingsForMatchersCacheMaxBytes,
				Force:                 cfg.BlocksStorageConfig.TSDB.BlockPostingsForMatchersCacheForce,
				Metrics:               tsdb.NewPostingsForMatchersCacheMetrics(prometheus.WrapRegistererWithPrefix("cortex_ingester_tsdb_block_", registerer)),
				PostingsClonerFactory: lookupplan.ActualSelectedPostingsClonerFactory{},
			},
		),
		errorSamplers: newIngesterErrSamplers(cfg.ErrorSampleRate),
	}, nil
}

// New returns an Ingester that uses Mimir block storage.
func New(cfg Config, limits *validation.Overrides, ingestersRing ring.ReadRing, partitionRingWatcher *ring.PartitionRingWatcher, activeGroupsCleanupService *util.ActiveGroupsCleanupService, costAttributionMgr *costattribution.Manager, registerer prometheus.Registerer, logger log.Logger) (*Ingester, error) {
	i, err := newIngester(cfg, limits, ingestersRing, registerer, logger)
	if err != nil {
		return nil, err
	}
	i.ingestionRate = util_math.NewEWMARate(0.2, instanceIngestionRateTickInterval)
	i.metrics = newIngesterMetrics(registerer, cfg.ActiveSeriesMetrics.Enabled, i.getInstanceLimits, i.ingestionRate, &i.inflightPushRequests, &i.inflightPushRequestsBytes)
	i.activeGroups = activeGroupsCleanupService

	i.costAttributionMgr = costAttributionMgr
	// We create a circuit breaker, which will be activated on a successful completion of starting.
	i.circuitBreaker = newIngesterCircuitBreaker(i.cfg.PushCircuitBreaker, i.cfg.ReadCircuitBreaker, logger, registerer)

	i.reactiveLimiter = newIngesterReactiveLimiter(&i.cfg.RejectionPrioritizer, &i.cfg.PushReactiveLimiter, &i.cfg.ReadReactiveLimiter, logger, registerer)

	i.lookupPlanMetrics = lookupplan.NewMetrics(registerer)

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

	// Create a Prometheus registerer where metrics are prefixed by "cortex_".
	cortexPrefixedRegisterer := prometheus.WrapRegistererWithPrefix("cortex_", registerer)

	// Create the lifecycler. In tokenless mode, we use a BasicLifecycler
	// configured to not register tokens. Otherwise, use classic Lifecycler.
	var ingesterID string
	if cfg.IngesterRing.NumTokens == 0 {
		// Tokenless mode requires ingest storage to be enabled.
		// This check is here instead of Config.Validate() because Config.IngestStorageConfig is injected after validation.
		if !cfg.IngestStorageConfig.Enabled {
			return nil, fmt.Errorf("ring tokens can only be disabled when ingest storage is enabled")
		}

		// Create KV store for the ring.
		ringKV, err := kv.NewClient(cfg.IngesterRing.KVStore, ring.GetCodec(), kv.RegistererWithKVName(cortexPrefixedRegisterer, IngesterRingName+"-lifecycler"), logger)
		if err != nil {
			return nil, errors.Wrap(err, "creating KV store for ingester ring in tokenless mode")
		}

		// Create BasicLifecycler config.
		lifecyclerCfg, err := cfg.IngesterRing.ToTokenlessBasicLifecyclerConfig(logger)
		if err != nil {
			return nil, errors.Wrap(err, "creating basic lifecycler config for ingester ring in tokenless mode")
		}

		// Create tokenless lifecycler.
		lifecycler, err := newTokenlessLifecycler(lifecyclerCfg, IngesterRingName, IngesterRingKey, ringKV, cfg.IngesterRing.MinReadyDuration, cfg.IngesterRing.FinalSleep, cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown, i.Flush, logger, cortexPrefixedRegisterer)
		if err != nil {
			return nil, errors.Wrap(err, "creating lifecycler for ingester ring in tokenless mode")
		}

		i.lifecycler = lifecycler
		ingesterID = lifecycler.GetInstanceID()
	} else {
		// Classic Lifecycler for token-based mode.
		lifecycler, err := ring.NewLifecycler(cfg.IngesterRing.ToLifecyclerConfig(), i, IngesterRingName, IngesterRingKey, cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown, logger, cortexPrefixedRegisterer)
		if err != nil {
			return nil, err
		}
		i.lifecycler = lifecycler
		ingesterID = lifecycler.ID
	}

	i.subservicesWatcher = services.NewFailureWatcher()
	i.subservicesWatcher.WatchService(i.lifecycler)

	if cfg.ReadPathCPUUtilizationLimit > 0 || cfg.ReadPathMemoryUtilizationLimit > 0 {
		i.utilizationBasedLimiter = limiter.NewUtilizationBasedLimiter(cfg.ReadPathCPUUtilizationLimit,
			cfg.ReadPathMemoryUtilizationLimit, true,
			log.WithPrefix(logger, "context", "read path"),
			prometheus.WrapRegistererWithPrefix("cortex_ingester_", registerer))
	}

	i.ingesterID = ingesterID

	// Apply positive jitter only to ensure that the minimum timeout is adhered to.
	i.compactionIdleTimeout = util.DurationWithPositiveJitter(i.cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout, compactionIdleTimeoutJitter)
	level.Info(i.logger).Log("msg", "TSDB idle compaction timeout set", "timeout", i.compactionIdleTimeout)

	var limiterStrategy limiterRingStrategy
	var ownedSeriesStrategy ownedSeriesRingStrategy

	if ingestCfg := cfg.IngestStorageConfig; ingestCfg.Enabled {
		kafkaCfg := ingestCfg.KafkaConfig

		i.ingestPartitionID, err = ingest.IngesterPartitionID(cfg.IngesterRing.InstanceID)
		if err != nil {
			return nil, errors.Wrap(err, "calculating ingester partition ID")
		}

		// We use the ingester instance ID as consumer group. This means that we have N consumer groups
		// where N is the total number of ingesters. Each ingester is part of their own consumer group
		// so that they all replay the owned partition with no gaps.
		kafkaCfg.FallbackClientErrorSampleRate = cfg.ErrorSampleRate
		kafkaCfg.MaxReplayPeriod = cfg.BlocksStorageConfig.TSDB.Retention

		// This is injected already higher up for methods invoked via the network.
		// Here we use it so that pushes from kafka also get a tenant assigned since the PartitionReader invokes the ingester.
		profilingIngester := NewIngesterProfilingWrapper(i)

		// The offset file is always stored in the TSDB directory alongside the ingester's data.
		offsetFilePath := filepath.Join(cfg.BlocksStorageConfig.TSDB.Dir, "kafka-offset.json")

		i.ingestReader, err = ingest.NewPartitionReaderForPusher(kafkaCfg, i.ingestPartitionID, cfg.IngesterRing.InstanceID, offsetFilePath, profilingIngester, log.With(logger, "component", "ingest_reader"), registerer)
		if err != nil {
			return nil, errors.Wrap(err, "creating ingest storage reader")
		}

		partitionRingKV := cfg.IngesterPartitionRing.KVStore.Mock
		if partitionRingKV == nil {
			partitionRingKV, err = kv.NewClient(cfg.IngesterPartitionRing.KVStore, ring.GetPartitionRingCodec(), kv.RegistererWithKVName(registerer, PartitionRingName+"-lifecycler"), logger)
			if err != nil {
				return nil, errors.Wrap(err, "creating KV store for ingester partition ring")
			}
		}

		i.ingestPartitionLifecycler = ring.NewPartitionInstanceLifecycler(
			i.cfg.IngesterPartitionRing.ToLifecyclerConfig(i.ingestPartitionID, cfg.IngesterRing.InstanceID),
			PartitionRingName,
			PartitionRingKey,
			partitionRingKV,
			logger,
			prometheus.WrapRegistererWithPrefix("cortex_", registerer))
		i.ingestPartitionLifecycler.BasicService = i.ingestPartitionLifecycler.WithName("partition-instance-lifecycler")

		limiterStrategy = newPartitionRingLimiterStrategy(partitionRingWatcher, i.limits.EffectiveIngestionPartitionsTenantWriteShardSize)
		ownedSeriesStrategy = newOwnedSeriesPartitionRingStrategy(i.ingestPartitionID, partitionRingWatcher, i.limits.EffectiveIngestionPartitionsTenantWriteShardSize)
	} else {
		limiterStrategy = newIngesterRingLimiterStrategy(ingestersRing, cfg.IngesterRing.ReplicationFactor, cfg.IngesterRing.ZoneAwarenessEnabled, cfg.IngesterRing.InstanceZone, i.limits.IngestionTenantShardSize)
		ownedSeriesStrategy = newOwnedSeriesIngesterRingStrategy(ingesterID, ingestersRing, i.limits.IngestionTenantShardSize)
	}

	i.limiter = NewLimiter(limits, limiterStrategy)

	if cfg.UseIngesterOwnedSeriesForLimits || cfg.UpdateIngesterOwnedSeries {
		i.ownedSeriesService = newOwnedSeriesService(i.cfg.OwnedSeriesUpdateInterval, ownedSeriesStrategy, log.With(i.logger, "component", "owned series"), registerer, i.limiter.maxSeriesPerUser, i.getTSDBUsers, i.getTSDB)

		// We add owned series service explicitly, because ingester doesn't start it using i.subservices.
		i.subservicesWatcher.WatchService(i.ownedSeriesService)
	}

	// Init compaction service, responsible to periodically run TSDB head compactions.
	i.compactionService = services.NewBasicService(nil, i.compactionServiceRunning, nil).WithName("ingester-compaction")
	i.subservicesWatcher.WatchService(i.compactionService)

	// Init shipping service, responsible to periodically ship TSDB blocks to long-term storage.
	if cfg.BlocksStorageConfig.TSDB.IsBlocksShippingEnabled() {
		i.shippingService = services.NewBasicService(nil, i.shipBlocksLoop, nil).WithName("ingester-shipping")
	}

	// Init metrics updater service, responsible to periodically update ingester metrics and stats.
	i.metricsUpdaterService = services.NewBasicService(nil, i.metricsUpdaterServiceRunning, nil).WithName("ingester-metrics-updater")
	i.subservicesWatcher.WatchService(i.metricsUpdaterService)

	// Init metadata purger service, responsible to periodically delete metrics metadata past their retention period.
	i.metadataPurgerService = services.NewTimerService(metadataPurgePeriod, nil, func(context.Context) error {
		i.purgeUserMetricsMetadata()
		return nil
	}, nil).WithName("ingester-metadata-purger")
	i.subservicesWatcher.WatchService(i.metadataPurgerService)

	// Init head statistics generation service if enabled
	if cfg.BlocksStorageConfig.TSDB.IndexLookupPlanning.Enabled {
		i.statisticsService = services.NewTimerService(cfg.BlocksStorageConfig.TSDB.IndexLookupPlanning.StatisticsCollectionFrequency, nil, i.generateHeadStatisticsForAllUsers, nil)
		i.subservicesWatcher.WatchService(i.statisticsService)
	}

	// Verify and init kafka offset catalogue.
	if cfg.BlocksStorageConfig.TSDB.OffsetCatalogue.Enabled {
		// This check is here instead of Config.Validate() because Config.IngestStorageConfig is injected after validation.
		if !cfg.IngestStorageConfig.Enabled {
			return nil, fmt.Errorf("kafka offset catalogue can only be enabled when ingest storage is enabled")
		}
	}

	i.BasicService = services.NewBasicService(i.starting, i.ingesterRunning, i.stopping).WithName("ingester")
	return i, nil
}

// generateHeadStatisticsForAllUsers iterates over all user TSDBs and generates head statistics.
func (i *Ingester) generateHeadStatisticsForAllUsers(context.Context) error {
	for _, userID := range i.getTSDBUsers() {
		userDB := i.getTSDB(userID)
		if userDB == nil {
			// A race with a user TSDB being removed, just skip it.
			continue
		}
		err := userDB.generateHeadStatistics()
		if err != nil {
			level.Warn(i.logger).Log("msg", "failed to generate head statistics; previous statistics will be used for queries if have been computed since startup", "user", userID, "err", err)
			continue
		}
	}
	return nil
}

func (i *Ingester) starting(ctx context.Context) (err error) {

	defer func() {
		if err != nil {
			// If starting() fails for any reason (e.g., context canceled), lifecycler must be stopped.
			shutdownTimeout := 1 * time.Minute
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer shutdownCancel()

			_ = services.StopAndAwaitTerminated(shutdownCtx, i.lifecycler)
		}
	}()

	// Ensure the TSDB directory exists before checking for markers.
	// This is required for ingesters starting with empty disks to support operations
	// like scale down that need to write markers even when no TSDBs have been created yet.
	if err := os.MkdirAll(i.cfg.BlocksStorageConfig.TSDB.Dir, os.ModePerm); err != nil {
		return errors.Wrap(err, "failed to create TSDB directory")
	}

	// First of all we have to check if the shutdown marker is set. This needs to be done
	// as first thing because, if found, it may change the behaviour of the ingester startup.
	if exists, err := shutdownmarker.Exists(shutdownmarker.GetPath(i.cfg.BlocksStorageConfig.TSDB.Dir)); err != nil {
		return errors.Wrap(err, "failed to check ingester shutdown marker")
	} else if exists {
		level.Info(i.logger).Log("msg", "detected existing shutdown marker, setting unregister and flush on shutdown", "path", shutdownmarker.GetPath(i.cfg.BlocksStorageConfig.TSDB.Dir))
		i.setPrepareShutdown()
	}

	if err := i.openExistingTSDB(ctx); err != nil {
		// Try to rollback and close opened TSDBs before halting the ingester.
		i.closeAllTSDB()

		return errors.Wrap(err, "opening existing TSDBs")
	}

	if i.ownedSeriesService != nil {
		// We need to perform the initial computation of owned series after the TSDBs are opened but before the ingester becomes
		// ACTIVE in the ring and starts to accept requests. However, because the ingester still uses the Lifecycler (rather
		// than BasicLifecycler) there is no deterministic way to delay the ACTIVE state until we finish the calculations.
		//
		// Start owned series service before starting lifecyclers. We wait for ownedSeriesService
		// to enter Running state here, that is ownedSeriesService computes owned series if ring is not empty.
		// If ring is empty, ownedSeriesService doesn't do anything.
		// If ring is not empty, but instance is not in the ring yet, ownedSeriesService will compute 0 owned series.
		//
		// We pass ingester's service context to ownedSeriesService, to make ownedSeriesService stop when ingester's
		// context is done (i.e. when ingester fails in Starting state, or when ingester exits Running state).
		if err := services.StartAndAwaitRunning(ctx, i.ownedSeriesService); err != nil {
			return errors.Wrap(err, "failed to start owned series service")
		}
	}

	// Start the following services before starting the ingest storage reader, in order to have them
	// running while replaying the partition (if ingest storage is enabled).
	i.subservicesForPartitionReplay, err = createManagerThenStartAndAwaitHealthy(ctx, i.compactionService, i.metricsUpdaterService, i.metadataPurgerService)
	if err != nil {
		return errors.Wrap(err, "failed to start ingester subservices before partition reader")
	}

	// When ingest storage is enabled, we have to make sure that reader catches up replaying the partition
	// BEFORE the ingester ring lifecycler is started, because once the ingester ring lifecycler will start
	// it will switch the ingester state in the ring to ACTIVE.
	if i.ingestReader != nil {
		if err := services.StartAndAwaitRunning(ctx, i.ingestReader); err != nil {
			return errors.Wrap(err, "failed to start partition reader")
		}
	}

	// Important: we want to keep lifecycler running until we ask it to stop, so we need to give it independent context
	if err := i.lifecycler.StartAsync(context.Background()); err != nil {
		return errors.Wrap(err, "failed to start lifecycler")
	}
	if err := i.lifecycler.AwaitRunning(ctx); err != nil {
		return errors.Wrap(err, "failed to start lifecycler")
	}
	waitInstanceStateTimeOut := 7 * time.Minute // autoJoin timeout is 5 minutes
	waitInstanceStateCtx, waitInstanceStateCancel := context.WithTimeout(ctx, waitInstanceStateTimeOut)
	defer waitInstanceStateCancel()
	if err = ring.WaitInstanceState(waitInstanceStateCtx, i.instanceRing, i.cfg.IngesterRing.InstanceID, ring.ACTIVE); err != nil {
		return errors.Wrap(err, "failed to wait for instance to be active in ring")
	}

	// Finally we start all services that should run after the ingester ring lifecycler.
	var servs []services.Service

	if i.shippingService != nil {
		servs = append(servs, i.shippingService)
	}

	if i.cfg.BlocksStorageConfig.TSDB.IndexLookupPlanning.Enabled {
		servs = append(servs, i.statisticsService)
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

	if i.reactiveLimiter.service != nil {
		servs = append(servs, i.reactiveLimiter.service)
	}

	if i.ingestPartitionLifecycler != nil {
		servs = append(servs, i.ingestPartitionLifecycler)
	}

	// Since subservices are conditional, We add an idle service if there are no subservices to
	// guarantee there's at least 1 service to run otherwise the service manager fails to start.
	if len(servs) == 0 {
		servs = append(servs, services.NewIdleService(nil, nil))
	}

	i.subservicesAfterIngesterRingLifecycler, err = createManagerThenStartAndAwaitHealthy(ctx, servs...)
	if err != nil {
		return errors.Wrap(err, "failed to start ingester subservices after ingester ring lifecycler")
	}

	i.circuitBreaker.read.activate()
	if ro, _ := i.lifecycler.GetReadOnlyState(); !ro {
		// If the ingester is not read-only, activate the push circuit breaker.
		i.circuitBreaker.push.activate()
	}

	return err
}

func (i *Ingester) stopping(_ error) error {
	if i.ingestReader != nil {
		if err := services.StopAndAwaitTerminated(context.Background(), i.ingestReader); err != nil {
			level.Warn(i.logger).Log("msg", "failed to stop partition reader", "err", err)
		}
	}

	if i.ownedSeriesService != nil {
		err := services.StopAndAwaitTerminated(context.Background(), i.ownedSeriesService)
		if err != nil {
			// This service can't really fail.
			level.Warn(i.logger).Log("msg", "error encountered while stopping owned series service", "err", err)
		}
	}

	// Stop subservices.
	i.subservicesForPartitionReplay.StopAsync()
	i.subservicesAfterIngesterRingLifecycler.StopAsync()

	if err := i.subservicesForPartitionReplay.AwaitStopped(context.Background()); err != nil {
		level.Warn(i.logger).Log("msg", "failed to stop ingester subservices", "err", err)
	}

	if err := i.subservicesAfterIngesterRingLifecycler.AwaitStopped(context.Background()); err != nil {
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

func (i *Ingester) ingesterRunning(ctx context.Context) error {
	tsdbUpdateTicker := time.NewTicker(i.cfg.TSDBConfigUpdatePeriod)
	defer tsdbUpdateTicker.Stop()

	for {
		select {
		case <-tsdbUpdateTicker.C:
			i.applyTSDBSettings()
		case <-ctx.Done():
			return nil
		case err := <-i.subservicesWatcher.Chan():
			return errors.Wrap(err, "ingester subservice failed")
		}
	}
}

// OnPartitionRingChanged resets the read reactive limiter when the ingester's partition transitions to active.
func (i *Ingester) OnPartitionRingChanged(oldRing, newRing *ring.PartitionRingDesc) {
	if i.reactiveLimiter.read != nil {
		oldPartition, ok1 := oldRing.Partitions[i.ingestPartitionID]
		newPartition, ok2 := newRing.Partitions[i.ingestPartitionID]
		if ok1 && ok2 && oldPartition.State != newPartition.State && newPartition.State == ring.PartitionActive {
			i.reactiveLimiter.read.Reset()
		}
	}
}

// checkAvailableForRead checks whether the ingester is available for read requests,
// and if it is not the case returns an unavailableError error.
func (i *Ingester) checkAvailableForRead() error {
	s := i.State()

	// The ingester is not available when starting / stopping to prevent any read to
	// TSDB when its closed.
	if s == services.Running {
		return nil
	}
	return newUnavailableError(s)
}

// checkAvailableForPush checks whether the ingester is available for push requests,
// and if it is not the case returns an unavailableError error.
func (i *Ingester) checkAvailableForPush() error {
	ingesterState := i.State()

	// The ingester is not available when stopping to prevent any push to
	// TSDB when its closed.
	if ingesterState == services.Running {
		return nil
	}

	// If ingest storage is enabled we also allow push requests when the ingester is starting
	// as far as the ingest reader is either in starting or running state. This is required to
	// let the ingest reader push data while replaying a partition at ingester startup.
	if ingesterState == services.Starting && i.ingestReader != nil {
		if readerState := i.ingestReader.State(); readerState == services.Starting || readerState == services.Running {
			return nil
		}
	}

	return newUnavailableError(ingesterState)
}

// CheckReady is the readiness handler used to indicate to k8s when the ingesters
// are ready for the addition or removal of another ingester.
func (i *Ingester) CheckReady(ctx context.Context) error {
	if err := i.checkAvailableForPush(); err != nil {
		return fmt.Errorf("ingester not ready for pushes: %v", err)
	}
	if err := i.checkAvailableForRead(); err != nil {
		return fmt.Errorf("ingester not ready for reads: %v", err)
	}
	return i.lifecycler.CheckReady(ctx)
}

func (i *Ingester) RingHandler() http.Handler {
	return i.lifecycler
}

type utilizationBasedLimiter interface {
	services.Service

	LimitingReason() string
}

func createManagerThenStartAndAwaitHealthy(ctx context.Context, srvs ...services.Service) (*services.Manager, error) {
	manager, err := services.NewManager(srvs...)
	if err != nil {
		return nil, err
	}

	if err := services.StartManagerAndAwaitHealthy(ctx, manager); err != nil {
		return nil, err
	}

	return manager, nil
}
