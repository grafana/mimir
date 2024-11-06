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
	"encoding/json"
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
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/oklog/ulid"
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

	"github.com/grafana/mimir/pkg/ingester/activeseries"
	asmodel "github.com/grafana/mimir/pkg/ingester/activeseries/model"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/api"
	mimir_storage "github.com/grafana/mimir/pkg/storage"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/sharding"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/usagestats"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/limiter"
	util_log "github.com/grafana/mimir/pkg/util/log"
	util_math "github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/shutdownmarker"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/tracing"
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
	reasonSampleOutOfBounds      = "sample-out-of-bounds"
	reasonPerUserSeriesLimit     = "per_user_series_limit"
	reasonPerMetricSeriesLimit   = "per_metric_series_limit"
	reasonInvalidNativeHistogram = "invalid-native-histogram"

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
	IngesterRing          RingConfig          `yaml:"ring"`
	IngesterPartitionRing PartitionRingConfig `yaml:"partition_ring" category:"experimental"`

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

	ErrorSampleRate int64 `yaml:"error_sample_rate" json:"error_sample_rate" category:"advanced"`

	// UseIngesterOwnedSeriesForLimits was added in 2.12, but we keep it experimental until we decide, what is the correct behaviour
	// when the replication factor and the number of zones don't match. Refer to notes in https://github.com/grafana/mimir/pull/8695 and https://github.com/grafana/mimir/pull/9496
	UseIngesterOwnedSeriesForLimits bool          `yaml:"use_ingester_owned_series_for_limits" category:"experimental"`
	UpdateIngesterOwnedSeries       bool          `yaml:"track_ingester_owned_series" category:"experimental"`
	OwnedSeriesUpdateInterval       time.Duration `yaml:"owned_series_update_interval" category:"experimental"`

	PushCircuitBreaker CircuitBreakerConfig `yaml:"push_circuit_breaker"`
	ReadCircuitBreaker CircuitBreakerConfig `yaml:"read_circuit_breaker"`

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

	f.DurationVar(&cfg.MetadataRetainPeriod, "ingester.metadata-retain-period", 10*time.Minute, "Period at which metadata we have not seen will remain in memory before being deleted.")
	f.DurationVar(&cfg.RateUpdatePeriod, "ingester.rate-update-period", 15*time.Second, "Period with which to update the per-tenant ingestion rates.")
	f.BoolVar(&cfg.StreamChunksWhenUsingBlocks, "ingester.stream-chunks-when-using-blocks", true, "Stream chunks from ingesters to queriers.")
	f.DurationVar(&cfg.TSDBConfigUpdatePeriod, "ingester.tsdb-config-update-period", 15*time.Second, "Period with which to update the per-tenant TSDB configuration.")
	f.StringVar(&cfg.IgnoreSeriesLimitForMetricNames, "ingester.ignore-series-limit-for-metric-names", "", "Comma-separated list of metric names, for which the -ingester.max-global-series-per-metric limit will be ignored. Does not affect the -ingester.max-global-series-per-user limit.")
	f.Float64Var(&cfg.ReadPathCPUUtilizationLimit, "ingester.read-path-cpu-utilization-limit", 0, "CPU utilization limit, as CPU cores, for CPU/memory utilization based read request limiting. Use 0 to disable it.")
	f.Uint64Var(&cfg.ReadPathMemoryUtilizationLimit, "ingester.read-path-memory-utilization-limit", 0, "Memory limit, in bytes, for CPU/memory utilization based read request limiting. Use 0 to disable it.")
	f.BoolVar(&cfg.LogUtilizationBasedLimiterCPUSamples, "ingester.log-utilization-based-limiter-cpu-samples", false, "Enable logging of utilization based limiter CPU samples.")
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

	lifecycler            *ring.Lifecycler
	limits                *validation.Overrides
	limiter               *Limiter
	subservicesWatcher    *services.FailureWatcher
	ownedSeriesService    *ownedSeriesService
	compactionService     services.Service
	metricsUpdaterService services.Service
	metadataPurgerService services.Service

	// Mimir blocks storage.
	tsdbsMtx sync.RWMutex
	tsdbs    map[string]*userTSDB // tsdb sharded by userID

	bucket objstore.Bucket

	// Value used by shipper as external label.
	shipperIngesterID string

	// Metrics shared across all per-tenant shippers.
	shipperMetrics *shipperMetrics

	subservicesForPartitionReplay          *services.Manager
	subservicesAfterIngesterRingLifecycler *services.Manager

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
	ingestionRate             *util_math.EwmaRate
	inflightPushRequests      atomic.Int64
	inflightPushRequestsBytes atomic.Int64

	utilizationBasedLimiter utilizationBasedLimiter

	errorSamplers ingesterErrSamplers

	// The following is used by ingest storage (when enabled).
	ingestReader              *ingest.PartitionReader
	ingestPartitionID         int32
	ingestPartitionLifecycler *ring.PartitionInstanceLifecycler

	circuitBreaker ingesterCircuitBreaker
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
	replicationFactor.Set(int64(cfg.IngesterRing.ReplicationFactor))
	ringStoreName.Set(cfg.IngesterRing.KVStore.Store)

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

		errorSamplers: newIngesterErrSamplers(cfg.ErrorSampleRate),
	}, nil
}

// New returns an Ingester that uses Mimir block storage.
func New(cfg Config, limits *validation.Overrides, ingestersRing ring.ReadRing, partitionRingWatcher *ring.PartitionRingWatcher, activeGroupsCleanupService *util.ActiveGroupsCleanupService, registerer prometheus.Registerer, logger log.Logger) (*Ingester, error) {
	i, err := newIngester(cfg, limits, registerer, logger)
	if err != nil {
		return nil, err
	}
	i.ingestionRate = util_math.NewEWMARate(0.2, instanceIngestionRateTickInterval)
	i.metrics = newIngesterMetrics(registerer, cfg.ActiveSeriesMetrics.Enabled, i.getInstanceLimits, i.ingestionRate, &i.inflightPushRequests, &i.inflightPushRequestsBytes)
	i.activeGroups = activeGroupsCleanupService

	// We create a circuit breaker, which will be activated on a successful completion of starting.
	i.circuitBreaker = newIngesterCircuitBreaker(i.cfg.PushCircuitBreaker, i.cfg.ReadCircuitBreaker, logger, registerer)

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

	i.lifecycler, err = ring.NewLifecycler(cfg.IngesterRing.ToLifecyclerConfig(), i, "ingester", IngesterRingKey, cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown, logger, prometheus.WrapRegistererWithPrefix("cortex_", registerer))
	if err != nil {
		return nil, err
	}
	i.subservicesWatcher = services.NewFailureWatcher()
	i.subservicesWatcher.WatchService(i.lifecycler)

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
		i.ingestReader, err = ingest.NewPartitionReaderForPusher(kafkaCfg, i.ingestPartitionID, cfg.IngesterRing.InstanceID, i, log.With(logger, "component", "ingest_reader"), registerer)
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

		limiterStrategy = newPartitionRingLimiterStrategy(partitionRingWatcher, i.limits.IngestionPartitionsTenantShardSize)
		ownedSeriesStrategy = newOwnedSeriesPartitionRingStrategy(i.ingestPartitionID, partitionRingWatcher, i.limits.IngestionPartitionsTenantShardSize)
	} else {
		limiterStrategy = newIngesterRingLimiterStrategy(ingestersRing, cfg.IngesterRing.ReplicationFactor, cfg.IngesterRing.ZoneAwarenessEnabled, cfg.IngesterRing.InstanceZone, i.limits.IngestionTenantShardSize)
		ownedSeriesStrategy = newOwnedSeriesIngesterRingStrategy(i.lifecycler.ID, ingestersRing, i.limits.IngestionTenantShardSize)
	}

	i.limiter = NewLimiter(limits, limiterStrategy)

	if cfg.UseIngesterOwnedSeriesForLimits || cfg.UpdateIngesterOwnedSeries {
		i.ownedSeriesService = newOwnedSeriesService(i.cfg.OwnedSeriesUpdateInterval, ownedSeriesStrategy, log.With(i.logger, "component", "owned series"), registerer, i.limiter.maxSeriesPerUser, i.getTSDBUsers, i.getTSDB)

		// We add owned series service explicitly, because ingester doesn't start it using i.subservices.
		i.subservicesWatcher.WatchService(i.ownedSeriesService)
	}

	// Init compaction service, responsible to periodically run TSDB head compactions.
	i.compactionService = services.NewBasicService(nil, i.compactionServiceRunning, nil)
	i.subservicesWatcher.WatchService(i.compactionService)

	// Init metrics updater service, responsible to periodically update ingester metrics and stats.
	i.metricsUpdaterService = services.NewBasicService(nil, i.metricsUpdaterServiceRunning, nil)
	i.subservicesWatcher.WatchService(i.metricsUpdaterService)

	// Init metadata purger service, responsible to periodically delete metrics metadata past their retention period.
	i.metadataPurgerService = services.NewTimerService(metadataPurgePeriod, nil, func(context.Context) error {
		i.purgeUserMetricsMetadata()
		return nil
	}, nil)
	i.subservicesWatcher.WatchService(i.metadataPurgerService)

	i.BasicService = services.NewBasicService(i.starting, i.ingesterRunning, i.stopping)
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
	i.metrics = newIngesterMetrics(registerer, false, i.getInstanceLimits, nil, &i.inflightPushRequests, &i.inflightPushRequestsBytes)

	i.shipperIngesterID = "flusher"
	i.limiter = NewLimiter(limits, flusherLimiterStrategy{})

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

func (i *Ingester) starting(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			// if starting() fails for any reason (e.g., context canceled),
			// the lifecycler must be stopped.
			_ = services.StopAndAwaitTerminated(context.Background(), i.lifecycler)
		}
	}()

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

	// Finally we start all services that should run after the ingester ring lifecycler.
	var servs []services.Service

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
	return nil
}

func (i *Ingester) stoppingForFlusher(_ error) error {
	if !i.cfg.BlocksStorageConfig.TSDB.KeepUserTSDBOpenOnShutdown {
		i.closeAllTSDB()
	}
	return nil
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

// metricsUpdaterServiceRunning is the running function for the internal metrics updater service.
func (i *Ingester) metricsUpdaterServiceRunning(ctx context.Context) error {
	// Launch a dedicated goroutine for inflightRequestsTicker
	// to ensure it operates independently, unaffected by delays from other logics in this function.
	go func() {
		inflightRequestsTicker := time.NewTicker(250 * time.Millisecond)
		defer inflightRequestsTicker.Stop()

		for {
			select {
			case <-inflightRequestsTicker.C:
				i.metrics.inflightRequestsSummary.Observe(float64(i.inflightPushRequests.Load()))
			case <-ctx.Done():
				return
			}
		}
	}()

	rateUpdateTicker := time.NewTicker(i.cfg.RateUpdatePeriod)
	defer rateUpdateTicker.Stop()

	ingestionRateTicker := time.NewTicker(instanceIngestionRateTickInterval)
	defer ingestionRateTicker.Stop()

	var activeSeriesTickerChan <-chan time.Time
	if i.cfg.ActiveSeriesMetrics.Enabled {
		t := time.NewTicker(i.cfg.ActiveSeriesMetrics.UpdatePeriod)
		activeSeriesTickerChan = t.C
		defer t.Stop()
	}

	usageStatsUpdateTicker := time.NewTicker(usageStatsUpdateInterval)
	defer usageStatsUpdateTicker.Stop()

	limitMetricsUpdateTicker := time.NewTicker(i.cfg.limitMetricsUpdatePeriod)
	defer limitMetricsUpdateTicker.Stop()

	for {
		select {
		case <-ingestionRateTicker.C:
			i.ingestionRate.Tick()
		case <-rateUpdateTicker.C:
			i.tsdbsMtx.RLock()
			for _, db := range i.tsdbs {
				db.ingestedAPISamples.Tick()
				db.ingestedRuleSamples.Tick()
			}
			i.tsdbsMtx.RUnlock()
		case <-activeSeriesTickerChan:
			i.updateActiveSeries(time.Now())
		case <-usageStatsUpdateTicker.C:
			i.updateUsageStats()
		case <-limitMetricsUpdateTicker.C:
			i.updateLimitMetrics()
		case <-ctx.Done():
			return nil
		}
	}
}

func (i *Ingester) replaceMatchers(asm *asmodel.Matchers, userDB *userTSDB, now time.Time) {
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
			i.replaceMatchers(asmodel.NewMatchers(newMatchersConfig), userDB, now)
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
	activeSeriesCount := int64(0)
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

		activeSeries, _, _ := userDB.activeSeries.Active()
		activeSeriesCount += int64(activeSeries)

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
	memorySeriesStats.Set(memorySeriesCount)
	activeSeriesStats.Set(activeSeriesCount)
	memoryTenantsStats.Set(memoryUsersCount)
	tenantsWithOutOfOrderEnabledStat.Set(tenantsWithOutOfOrderEnabledCount)
	minOutOfOrderTimeWindowSecondsStat.Set(int64(minOutOfOrderTimeWindow.Seconds()))
	maxOutOfOrderTimeWindowSecondsStat.Set(int64(maxOutOfOrderTimeWindow.Seconds()))
}

// applyTSDBSettings goes through all tenants and applies
// * The current max-exemplars setting. If it changed, tsdb will resize the buffer; if it didn't change tsdb will return quickly.
// * The current out-of-order time window. If it changes from 0 to >0, then a new Write-Behind-Log gets created for that tenant.
func (i *Ingester) applyTSDBSettings() {
	for _, userID := range i.getTSDBUsers() {
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
					MaxExemplars: int64(i.limiter.maxExemplarsPerUser(userID)),
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
		if i.limits.OOONativeHistogramsIngestionEnabled(userID) {
			db.db.EnableOOONativeHistograms()
		} else {
			db.db.DisableOOONativeHistograms()
		}
	}
}

func (i *Ingester) updateLimitMetrics() {
	for _, userID := range i.getTSDBUsers() {
		db := i.getTSDB(userID)
		if db == nil {
			continue
		}

		minLocalSeriesLimit := 0
		if i.cfg.UseIngesterOwnedSeriesForLimits || i.cfg.UpdateIngesterOwnedSeries {
			os := db.ownedSeriesState()
			i.metrics.ownedSeriesPerUser.WithLabelValues(userID).Set(float64(os.ownedSeriesCount))

			if i.cfg.UseIngesterOwnedSeriesForLimits {
				minLocalSeriesLimit = os.localSeriesLimit
			}
		}

		localLimit := i.limiter.maxSeriesPerUser(userID, minLocalSeriesLimit)
		i.metrics.maxLocalSeriesPerUser.WithLabelValues(userID).Set(float64(localLimit))
	}
}

// GetRef() is an extra method added to TSDB to let Mimir check before calling Add()
type extendedAppender interface {
	storage.Appender
	storage.GetRef
}

type pushStats struct {
	succeededSamplesCount       int
	failedSamplesCount          int
	succeededExemplarsCount     int
	failedExemplarsCount        int
	sampleOutOfBoundsCount      int
	sampleOutOfOrderCount       int
	sampleTooOldCount           int
	sampleTooFarInFutureCount   int
	newValueForTimestampCount   int
	perUserSeriesLimitCount     int
	perMetricSeriesLimitCount   int
	invalidNativeHistogramCount int
}

type ctxKey int

var pushReqCtxKey ctxKey = 1

type pushRequestState struct {
	requestSize     int64
	requestDuration time.Duration
	requestFinish   func(time.Duration, error)
	pushErr         error
}

func getPushRequestState(ctx context.Context) *pushRequestState {
	if st, ok := ctx.Value(pushReqCtxKey).(*pushRequestState); ok {
		return st
	}
	return nil
}

// StartPushRequest checks if ingester can start push request, and increments relevant counters.
// If new push request cannot be started, errors convertible to gRPC status code are returned, and metrics are updated.
func (i *Ingester) StartPushRequest(ctx context.Context, reqSize int64) (context.Context, error) {
	ctx, _, err := i.startPushRequest(ctx, reqSize)
	return ctx, err
}

func (i *Ingester) FinishPushRequest(ctx context.Context) {
	st := getPushRequestState(ctx)
	if st == nil {
		return
	}
	i.inflightPushRequests.Dec()
	if st.requestSize > 0 {
		i.inflightPushRequestsBytes.Sub(st.requestSize)
	}
	st.requestFinish(st.requestDuration, st.pushErr)
}

// This method can be called in two ways: 1. Ingester.PushWithCleanup, or 2. Ingester.StartPushRequest via gRPC server's method limiter.
//
// In the first case, returned errors can be inspected/logged by middleware. Ingester.PushWithCleanup will wrap the error in util_log.DoNotLogError wrapper.
// In the second case, returned errors will not be logged, because request will not reach any middleware.
//
// If startPushRequest ends with no error, the resulting context includes a *pushRequestState object
// containing relevant information about the push request started by this method.
// The resulting boolean flag tells if the caller must call finish on this request. If not, there is already someone in the call stack who will do that.
func (i *Ingester) startPushRequest(ctx context.Context, reqSize int64) (context.Context, bool, error) {
	if err := i.checkAvailableForPush(); err != nil {
		return nil, false, err
	}

	if st := getPushRequestState(ctx); st != nil {
		// If state is already in context, this means we already passed through StartPushRequest for this request.
		return ctx, false, nil
	}

	// We try to acquire a push permit from the circuit breaker.
	// If it is not possible, it is because the circuit breaker is open, and a circuitBreakerOpenError is returned.
	// If it is possible, a permit has to be released by recording either a success or a failure with the circuit
	// breaker. This is done by FinishPushRequest().
	finish, err := i.circuitBreaker.tryAcquirePushPermit()
	if err != nil {
		return nil, false, err
	}
	st := &pushRequestState{
		requestSize:   reqSize,
		requestFinish: finish,
	}
	ctx = context.WithValue(ctx, pushReqCtxKey, st)

	inflight := i.inflightPushRequests.Inc()
	inflightBytes := int64(0)
	rejectEqualInflightBytes := false
	if reqSize > 0 {
		inflightBytes = i.inflightPushRequestsBytes.Add(reqSize)
	} else {
		inflightBytes = i.inflightPushRequestsBytes.Load()
		rejectEqualInflightBytes = true // if inflightBytes == limit, reject new request
	}

	instanceLimitsErr := i.checkInstanceLimits(inflight, inflightBytes, rejectEqualInflightBytes)
	if instanceLimitsErr == nil {
		// In this case a pull request has been successfully started, and we return
		// the context enriched with the corresponding pushRequestState object.
		return ctx, true, nil
	}

	// In this case a per-instance limit has been hit, and the corresponding error has to be passed
	// to FinishPushRequest, which finishes the push request, records the error with the circuit breaker,
	// and gives it a possibly acquired permit back.
	st.pushErr = instanceLimitsErr
	i.FinishPushRequest(ctx)
	return nil, false, instanceLimitsErr
}

func (i *Ingester) checkInstanceLimits(inflight int64, inflightBytes int64, rejectEqualInflightBytes bool) error {
	il := i.getInstanceLimits()
	if il == nil {
		return nil
	}
	if il.MaxInflightPushRequests > 0 && inflight > il.MaxInflightPushRequests {
		i.metrics.rejected.WithLabelValues(reasonIngesterMaxInflightPushRequests).Inc()
		return errMaxInflightRequestsReached
	}

	if il.MaxInflightPushRequestsBytes > 0 {
		if (rejectEqualInflightBytes && inflightBytes >= il.MaxInflightPushRequestsBytes) || inflightBytes > il.MaxInflightPushRequestsBytes {
			i.metrics.rejected.WithLabelValues(reasonIngesterMaxInflightPushRequestsBytes).Inc()
			return errMaxInflightRequestsBytesReached
		}
	}

	if il.MaxIngestionRate > 0 {
		if rate := i.ingestionRate.Rate(); rate >= il.MaxIngestionRate {
			i.metrics.rejected.WithLabelValues(reasonIngesterMaxIngestionRate).Inc()
			return errMaxIngestionRateReached
		}
	}
	return nil
}

// PushWithCleanup is the Push() implementation for blocks storage and takes a WriteRequest and adds it to the TSDB head.
func (i *Ingester) PushWithCleanup(ctx context.Context, req *mimirpb.WriteRequest, cleanUp func()) (returnErr error) {
	// NOTE: because we use `unsafe` in deserialisation, we must not
	// retain anything from `req` past the exit from this function.
	defer cleanUp()

	start := time.Now()
	// Only start/finish request here when the request comes NOT from grpc handlers (i.e., from ingest.Store).
	// NOTE: request coming from grpc handler may end up calling start multiple times during its lifetime (e.g., when migrating to ingest storage).
	// startPushRequest handles this.
	if i.cfg.IngestStorageConfig.Enabled {
		reqSize := int64(req.Size())
		var (
			shouldFinish bool
			startPushErr error
		)
		// We need to replace the original context with the context returned by startPushRequest,
		// because the latter might store a new pushRequestState in the context.
		ctx, shouldFinish, startPushErr = i.startPushRequest(ctx, reqSize)
		if startPushErr != nil {
			return middleware.DoNotLogError{Err: startPushErr}
		}
		if shouldFinish {
			defer func() {
				i.FinishPushRequest(ctx)
			}()
		}
	}

	defer func() {
		// We enrich the pushRequestState contained in the context with this PushWithCleanUp()
		// call duration, and a possible error it returns. These data are needed during a
		// successive call to FinishPushRequest().
		if st := getPushRequestState(ctx); st != nil {
			st.requestDuration = time.Since(start)
			st.pushErr = returnErr
		}
	}()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}

	// Given metadata is a best-effort approach, and we don't halt on errors
	// process it before samples. Otherwise, we risk returning an error before ingestion.
	if ingestedMetadata := i.pushMetadata(ctx, userID, req.GetMetadata()); ingestedMetadata > 0 {
		// Distributor counts both samples and metadata, so for consistency ingester does the same.
		i.ingestionRate.Add(int64(ingestedMetadata))
	}

	// Early exit if no timeseries in request - don't create a TSDB or an appender.
	if len(req.Timeseries) == 0 {
		return nil
	}

	db, err := i.getOrCreateTSDB(userID)
	if err != nil {
		return wrapOrAnnotateWithUser(err, userID)
	}

	lockState, err := db.acquireAppendLock(req.MinTimestamp())
	if err != nil {
		return wrapOrAnnotateWithUser(err, userID)
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

		firstPartialErr error
		// updateFirstPartial is a function that, in case of a softError, stores that error
		// in firstPartialError, and makes PushWithCleanup proceed. This way all the valid
		// samples and exemplars will be we actually ingested, and the first softError that
		// was encountered will be returned. If a sampler is specified, the softError gets
		// wrapped by that sampler.
		updateFirstPartial = func(sampler *util_log.Sampler, errFn softErrorFunction) {
			if firstPartialErr == nil {
				firstPartialErr = errFn()
				if sampler != nil {
					firstPartialErr = sampler.WrapError(firstPartialErr)
				}
			}
		}

		outOfOrderWindow = i.limits.OutOfOrderTimeWindow(userID)

		errProcessor = mimir_storage.NewSoftAppendErrorProcessor(
			func() {
				stats.failedSamplesCount++
			},
			func(timestamp int64, labels []mimirpb.LabelAdapter) {
				stats.sampleOutOfBoundsCount++
				updateFirstPartial(i.errorSamplers.sampleTimestampTooOld, func() softError {
					return newSampleTimestampTooOldError(model.Time(timestamp), labels)
				})
			},
			func(timestamp int64, labels []mimirpb.LabelAdapter) {
				stats.sampleOutOfOrderCount++
				updateFirstPartial(i.errorSamplers.sampleOutOfOrder, func() softError {
					return newSampleOutOfOrderError(model.Time(timestamp), labels)
				})
			},
			func(timestamp int64, labels []mimirpb.LabelAdapter) {
				stats.sampleTooOldCount++
				updateFirstPartial(i.errorSamplers.sampleTimestampTooOldOOOEnabled, func() softError {
					return newSampleTimestampTooOldOOOEnabledError(model.Time(timestamp), labels, outOfOrderWindow)
				})
			},
			func(timestamp int64, labels []mimirpb.LabelAdapter) {
				stats.sampleTooFarInFutureCount++
				updateFirstPartial(i.errorSamplers.sampleTimestampTooFarInFuture, func() softError {
					return newSampleTimestampTooFarInFutureError(model.Time(timestamp), labels)
				})
			},
			func(timestamp int64, labels []mimirpb.LabelAdapter) {
				stats.newValueForTimestampCount++
				updateFirstPartial(i.errorSamplers.sampleDuplicateTimestamp, func() softError {
					return newSampleDuplicateTimestampError(model.Time(timestamp), labels)
				})
			},
			func() {
				stats.perUserSeriesLimitCount++
				updateFirstPartial(i.errorSamplers.maxSeriesPerUserLimitExceeded, func() softError {
					return newPerUserSeriesLimitReachedError(i.limiter.limits.MaxGlobalSeriesPerUser(userID))
				})
			},
			func(labels []mimirpb.LabelAdapter) {
				stats.perMetricSeriesLimitCount++
				updateFirstPartial(i.errorSamplers.maxSeriesPerMetricLimitExceeded, func() softError {
					return newPerMetricSeriesLimitReachedError(i.limiter.limits.MaxGlobalSeriesPerMetric(userID), labels)
				})
			},
			func(err error, timestamp int64, labels []mimirpb.LabelAdapter) {
				stats.sampleOutOfOrderCount++
				updateFirstPartial(i.errorSamplers.nativeHistogramValidationError, func() softError {
					e := newNativeHistogramValidationError(globalerror.NativeHistogramOOODisabled, err, model.Time(timestamp), labels)
					return e
				})
			},
			func(err error, timestamp int64, labels []mimirpb.LabelAdapter) {
				stats.invalidNativeHistogramCount++
				updateFirstPartial(i.errorSamplers.nativeHistogramValidationError, func() softError {
					return newNativeHistogramValidationError(globalerror.NativeHistogramCountMismatch, err, model.Time(timestamp), labels)
				})
			},
			func(err error, timestamp int64, labels []mimirpb.LabelAdapter) {
				stats.invalidNativeHistogramCount++
				updateFirstPartial(i.errorSamplers.nativeHistogramValidationError, func() softError {
					return newNativeHistogramValidationError(globalerror.NativeHistogramCountNotBigEnough, err, model.Time(timestamp), labels)
				})
			},
			func(err error, timestamp int64, labels []mimirpb.LabelAdapter) {
				stats.invalidNativeHistogramCount++
				updateFirstPartial(i.errorSamplers.nativeHistogramValidationError, func() softError {
					return newNativeHistogramValidationError(globalerror.NativeHistogramNegativeBucketCount, err, model.Time(timestamp), labels)
				})
			},
			func(err error, timestamp int64, labels []mimirpb.LabelAdapter) {
				stats.invalidNativeHistogramCount++
				updateFirstPartial(i.errorSamplers.nativeHistogramValidationError, func() softError {
					return newNativeHistogramValidationError(globalerror.NativeHistogramSpanNegativeOffset, err, model.Time(timestamp), labels)
				})
			},
			func(err error, timestamp int64, labels []mimirpb.LabelAdapter) {
				stats.invalidNativeHistogramCount++
				updateFirstPartial(i.errorSamplers.nativeHistogramValidationError, func() softError {
					return newNativeHistogramValidationError(globalerror.NativeHistogramSpansBucketsMismatch, err, model.Time(timestamp), labels)
				})
			},
		)
	)

	// Walk the samples, appending them to the users database
	app := db.Appender(ctx).(extendedAppender)
	spanlog.DebugLog("event", "got appender for timeseries", "series", len(req.Timeseries))

	var activeSeries *activeseries.ActiveSeries
	if i.cfg.ActiveSeriesMetrics.Enabled {
		activeSeries = db.activeSeries
	}

	minAppendTime, minAppendTimeAvailable := db.Head().AppendableMinValidTime()

	if pushSamplesToAppenderErr := i.pushSamplesToAppender(userID, req.Timeseries, app, startAppend, &stats, &errProcessor, updateFirstPartial, activeSeries, i.limits.OutOfOrderTimeWindow(userID), minAppendTimeAvailable, minAppendTime); pushSamplesToAppenderErr != nil {
		if err := app.Rollback(); err != nil {
			level.Warn(i.logger).Log("msg", "failed to rollback appender on error", "user", userID, "err", err)
		}

		return wrapOrAnnotateWithUser(pushSamplesToAppenderErr, userID)
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
		return wrapOrAnnotateWithUser(err, userID)
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
	appendedSamplesStats.Inc(int64(stats.succeededSamplesCount))
	appendedExemplarsStats.Inc(int64(stats.succeededExemplarsCount))

	group := i.activeGroups.UpdateActiveGroupTimestamp(userID, validation.GroupLabel(i.limits, userID, req.Timeseries), startAppend)

	i.updateMetricsFromPushStats(userID, group, &stats, req.Source, db, i.metrics.discarded)

	if firstPartialErr != nil {
		return wrapOrAnnotateWithUser(firstPartialErr, userID)
	}

	return nil
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
	if stats.invalidNativeHistogramCount > 0 {
		discarded.invalidNativeHistogram.WithLabelValues(userID, group).Add(float64(stats.invalidNativeHistogramCount))
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
// but in case of unhandled errors, appender is rolled back and such error is returned. Errors handled by updateFirstPartial
// must be of type softError.
func (i *Ingester) pushSamplesToAppender(userID string, timeseries []mimirpb.PreallocTimeseries, app extendedAppender, startAppend time.Time,
	stats *pushStats, errProcessor *mimir_storage.SoftAppendErrorProcessor, updateFirstPartial func(sampler *util_log.Sampler, errFn softErrorFunction), activeSeries *activeseries.ActiveSeries,
	outOfOrderWindow time.Duration, minAppendTimeAvailable bool, minAppendTime int64) error {

	// Fetch limits once per push request both to avoid processing half the request differently.
	var (
		nativeHistogramsIngestionEnabled = i.limits.NativeHistogramsIngestionEnabled(userID)
		maxTimestampMs                   = startAppend.Add(i.limits.CreationGracePeriod(userID)).UnixMilli()
		minTimestampMs                   = int64(math.MinInt64)
	)
	if i.limits.PastGracePeriod(userID) > 0 {
		minTimestampMs = startAppend.Add(-i.limits.PastGracePeriod(userID)).Add(-i.limits.OutOfOrderTimeWindow(userID)).UnixMilli()
	}

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
				if len(ts.Histograms) > 0 && (firstTimestamp == 0 || ts.Histograms[0].Timestamp < firstTimestamp) {
					firstTimestamp = ts.Histograms[0].Timestamp
				}

				updateFirstPartial(i.errorSamplers.sampleTimestampTooOld, func() softError {
					return newSampleTimestampTooOldError(model.Time(firstTimestamp), ts.Labels)
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

				updateFirstPartial(i.errorSamplers.sampleTimestampTooOld, func() softError {
					return newSampleTimestampTooOldError(model.Time(firstTimestamp), ts.Labels)
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
				errProcessor.ProcessErr(globalerror.SampleTooFarInFuture, s.TimestampMs, ts.Labels)
				continue
			} else if s.TimestampMs < minTimestampMs {
				errProcessor.ProcessErr(globalerror.SampleTooFarInPast, s.TimestampMs, ts.Labels)
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
			if errProcessor.ProcessErr(err, s.TimestampMs, ts.Labels) {
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
					errProcessor.ProcessErr(globalerror.SampleTooFarInFuture, h.Timestamp, ts.Labels)
					continue
				} else if h.Timestamp < minTimestampMs {
					errProcessor.ProcessErr(globalerror.SampleTooFarInPast, h.Timestamp, ts.Labels)
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

				if errProcessor.ProcessErr(err, h.Timestamp, ts.Labels) {
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
				updateFirstPartial(nil, func() softError {
					return newExemplarMissingSeriesError(model.Time(ts.Exemplars[0].TimestampMs), ts.Labels, ts.Exemplars[0].Labels)
				})
				stats.failedExemplarsCount += len(ts.Exemplars)
			} else { // Note that else is explicit, rather than a continue in the above if, in case of additional logic post exemplar processing.
				outOfOrderExemplars := 0
				for _, ex := range ts.Exemplars {
					if ex.TimestampMs > maxTimestampMs {
						stats.failedExemplarsCount++
						updateFirstPartial(nil, func() softError {
							return newExemplarTimestampTooFarInFutureError(model.Time(ex.TimestampMs), ts.Labels, ex.Labels)
						})
						continue
					} else if ex.TimestampMs < minTimestampMs {
						stats.failedExemplarsCount++
						updateFirstPartial(nil, func() softError {
							return newExemplarTimestampTooFarInPastError(model.Time(ex.TimestampMs), ts.Labels, ex.Labels)
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

					// We track the failed exemplars ingestion, whatever is the reason. This way, the sum of successfully
					// and failed ingested exemplars is equal to the total number of processed ones.
					stats.failedExemplarsCount++

					isOOOExemplar := errors.Is(err, storage.ErrOutOfOrderExemplar)
					if isOOOExemplar {
						outOfOrderExemplars++
						// Only report out of order exemplars if all are out of order, otherwise this was a partial update
						// to some existing set of exemplars.
						if outOfOrderExemplars < len(ts.Exemplars) {
							continue
						}
					}

					// Error adding exemplar. Do not report to client if the error was out of order and we ignore such error.
					if !isOOOExemplar || !i.limits.IgnoreOOOExemplars(userID) {
						updateFirstPartial(nil, func() softError {
							return newTSDBIngestExemplarErr(err, model.Time(ex.TimestampMs), ts.Labels, ex.Labels)
						})
					}
				}
			}
		}
	}
	return nil
}

func (i *Ingester) QueryExemplars(ctx context.Context, req *client.ExemplarQueryRequest) (resp *client.ExemplarQueryResponse, err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()
	finishReadRequest, err := i.startReadRequest()
	if err != nil {
		return nil, err
	}
	defer func() { finishReadRequest(err) }()

	spanlog, ctx := spanlogger.NewWithLogger(ctx, i.logger, "Ingester.QueryExemplars")
	defer spanlog.Finish()
	ctx = tracing.BridgeOpenTracingToOtel(ctx)

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	from, through, matchers, err := client.FromExemplarQueryRequest(req)
	if err != nil {
		return nil, err
	}

	i.metrics.queries.Inc()

	// Enforce read consistency before getting TSDB (covers the case the tenant's data has not been ingested
	// in this ingester yet, but there's some to ingest in the backlog).
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return nil, err
	}

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

func (i *Ingester) LabelValues(ctx context.Context, req *client.LabelValuesRequest) (resp *client.LabelValuesResponse, err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()
	finishReadRequest, err := i.startReadRequest()
	if err != nil {
		return nil, err
	}
	defer func() { finishReadRequest(err) }()

	labelName, startTimestampMs, endTimestampMs, matchers, err := client.FromLabelValuesRequest(req)
	if err != nil {
		return nil, err
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	// Enforce read consistency before getting TSDB (covers the case the tenant's data has not been ingested
	// in this ingester yet, but there's some to ingest in the backlog).
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return nil, err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return &client.LabelValuesResponse{}, nil
	}

	q, err := db.Querier(startTimestampMs, endTimestampMs)
	if err != nil {
		return nil, err
	}
	defer q.Close()

	hints := &storage.LabelHints{}
	vals, _, err := q.LabelValues(ctx, labelName, hints, matchers...)
	if err != nil {
		return nil, err
	}

	// The label value strings are sometimes pointing to memory mapped file
	// regions that may become unmapped anytime after Querier.Close is called.
	// So we copy those strings.
	for i, s := range vals {
		vals[i] = strings.Clone(s)
	}

	return &client.LabelValuesResponse{
		LabelValues: vals,
	}, nil
}

func (i *Ingester) LabelNames(ctx context.Context, req *client.LabelNamesRequest) (resp *client.LabelNamesResponse, err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()
	finishReadRequest, err := i.startReadRequest()
	if err != nil {
		return nil, err
	}
	defer func() { finishReadRequest(err) }()

	spanlog, ctx := spanlogger.NewWithLogger(ctx, i.logger, "Ingester.LabelNames")
	defer spanlog.Finish()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	// Enforce read consistency before getting TSDB (covers the case the tenant's data has not been ingested
	// in this ingester yet, but there's some to ingest in the backlog).
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
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

	q, err := db.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	defer q.Close()

	// Log the actual matchers passed down to TSDB. This can be useful for troubleshooting purposes.
	spanlog.DebugLog("num_matchers", len(matchers), "matchers", util.LabelMatchersToString(matchers))

	hints := &storage.LabelHints{}
	names, _, err := q.LabelNames(ctx, hints, matchers...)
	if err != nil {
		return nil, err
	}

	return &client.LabelNamesResponse{
		LabelNames: names,
	}, nil
}

// MetricsForLabelMatchers implements IngesterServer.
func (i *Ingester) MetricsForLabelMatchers(ctx context.Context, req *client.MetricsForLabelMatchersRequest) (resp *client.MetricsForLabelMatchersResponse, err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()
	finishReadRequest, err := i.startReadRequest()
	if err != nil {
		return nil, err
	}
	defer func() { finishReadRequest(err) }()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	// Enforce read consistency before getting TSDB (covers the case the tenant's data has not been ingested
	// in this ingester yet, but there's some to ingest in the backlog).
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
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
	q, err := db.Querier(mint, maxt)
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

		seriesSet := q.Select(ctx, true, hints, matchers...)
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

func (i *Ingester) UserStats(ctx context.Context, req *client.UserStatsRequest) (resp *client.UserStatsResponse, err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()
	finishReadRequest, err := i.startReadRequest()
	if err != nil {
		return nil, err
	}
	defer func() { finishReadRequest(err) }()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	// Enforce read consistency before getting TSDB (covers the case the tenant's data has not been ingested
	// in this ingester yet, but there's some to ingest in the backlog).
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return nil, err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return &client.UserStatsResponse{}, nil
	}

	return createUserStats(db, req)
}

// AllUserStats returns some per-tenant statistics about the data ingested in this ingester.
//
// When using the experimental ingest storage, this function doesn't support the read consistency setting
// because the purpose of this function is to show a snapshot of the live ingester's state.
func (i *Ingester) AllUserStats(_ context.Context, req *client.UserStatsRequest) (resp *client.UsersStatsResponse, err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()
	finishReadRequest, err := i.startReadRequest()
	if err != nil {
		return nil, err
	}
	defer func() { finishReadRequest(err) }()

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

func (i *Ingester) LabelNamesAndValues(request *client.LabelNamesAndValuesRequest, stream client.Ingester_LabelNamesAndValuesServer) (err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()
	finishReadRequest, err := i.startReadRequest()
	if err != nil {
		return err
	}
	defer func() { finishReadRequest(err) }()

	userID, err := tenant.TenantID(stream.Context())
	if err != nil {
		return err
	}

	// Enforce read consistency before getting TSDB (covers the case the tenant's data has not been ingested
	// in this ingester yet, but there's some to ingest in the backlog).
	if err := i.enforceReadConsistency(stream.Context(), userID); err != nil {
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

	var valueFilter func(name, value string) (bool, error)
	switch request.GetCountMethod() {
	case client.IN_MEMORY:
		valueFilter = func(string, string) (bool, error) {
			return true, nil
		}
	case client.ACTIVE:
		valueFilter = func(name, value string) (bool, error) {
			return activeseries.IsLabelValueActive(stream.Context(), index, db.activeSeries, name, value)
		}
	default:
		return fmt.Errorf("unknown count method %q", request.GetCountMethod())
	}

	return labelNamesAndValues(index, matchers, labelNamesAndValuesTargetSizeBytes, stream, valueFilter)
}

// labelValuesCardinalityTargetSizeBytes is the maximum allowed size in bytes for label cardinality response.
// We arbitrarily set it to 1mb to avoid reaching the actual gRPC default limit (4mb).
const labelValuesCardinalityTargetSizeBytes = 1 * 1024 * 1024

func (i *Ingester) LabelValuesCardinality(req *client.LabelValuesCardinalityRequest, srv client.Ingester_LabelValuesCardinalityServer) (err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()
	finishReadRequest, err := i.startReadRequest()
	if err != nil {
		return err
	}
	defer func() { finishReadRequest(err) }()

	userID, err := tenant.TenantID(srv.Context())
	if err != nil {
		return err
	}

	// Enforce read consistency before getting TSDB (covers the case the tenant's data has not been ingested
	// in this ingester yet, but there's some to ingest in the backlog).
	if err := i.enforceReadConsistency(srv.Context(), userID); err != nil {
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

	var postingsForMatchersFn func(context.Context, tsdb.IndexPostingsReader, ...*labels.Matcher) (index.Postings, error)
	switch req.GetCountMethod() {
	case client.IN_MEMORY:
		postingsForMatchersFn = tsdb.PostingsForMatchers
	case client.ACTIVE:
		postingsForMatchersFn = func(ctx context.Context, ix tsdb.IndexPostingsReader, ms ...*labels.Matcher) (index.Postings, error) {
			postings, err := tsdb.PostingsForMatchers(ctx, ix, ms...)
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
func (i *Ingester) QueryStream(req *client.QueryRequest, stream client.Ingester_QueryStreamServer) (err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()
	finishReadRequest, err := i.startReadRequest()
	if err != nil {
		return err
	}
	defer func() { finishReadRequest(err) }()

	spanlog, ctx := spanlogger.NewWithLogger(stream.Context(), i.logger, "Ingester.QueryStream")
	defer spanlog.Finish()
	ctx = tracing.BridgeOpenTracingToOtel(ctx)

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

	// Enforce read consistency before getting TSDB (covers the case the tenant's data has not been ingested
	// in this ingester yet, but there's some to ingest in the backlog).
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return err
	}

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
			spanlog.DebugLog("msg", "using executeStreamingQuery")
			numSeries, numSamples, err = i.executeStreamingQuery(ctx, db, int64(from), int64(through), matchers, shard, stream, req.StreamingChunksBatchSize, spanlog)
		} else {
			spanlog.DebugLog("msg", "using executeChunksQuery")
			numSeries, numSamples, err = i.executeChunksQuery(ctx, db, int64(from), int64(through), matchers, shard, stream)
		}
	} else {
		spanlog.DebugLog("msg", "using executeSamplesQuery")
		numSeries, numSamples, err = i.executeSamplesQuery(ctx, db, int64(from), int64(through), matchers, shard, stream)
	}
	if err != nil {
		return err
	}

	i.metrics.queriedSeries.Observe(float64(numSeries))
	i.metrics.queriedSamples.Observe(float64(numSamples))
	spanlog.DebugLog("series", numSeries, "samples", numSamples)
	return nil
}

func (i *Ingester) executeSamplesQuery(ctx context.Context, db *userTSDB, from, through int64, matchers []*labels.Matcher, shard *sharding.ShardSelector, stream client.Ingester_QueryStreamServer) (numSeries, numSamples int, _ error) {
	q, err := db.Querier(from, through)
	if err != nil {
		return 0, 0, err
	}
	defer q.Close()

	var hints *storage.SelectHints
	if shard != nil {
		hints = configSelectHintsWithShard(initSelectHints(from, through), shard)
	}

	// It's not required to return sorted series because series are sorted by the Mimir querier.
	ss := q.Select(ctx, false, hints, matchers...)
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
				t, v := it.AtHistogram(nil) // Nil argument as we pass the data to the protobuf as-is without copy.
				ts.Histograms = append(ts.Histograms, mimirpb.FromHistogramToHistogramProto(t, v))
			case chunkenc.ValFloatHistogram:
				t, v := it.AtFloatHistogram(nil) // Nil argument as we pass the data to the protobuf as-is without copy.
				ts.Histograms = append(ts.Histograms, mimirpb.FromFloatHistogramToHistogramProto(t, v))
			default:
				return 0, 0, fmt.Errorf("unsupported value type: %v", valType)
			}
		}

		if err := it.Err(); err != nil {
			return 0, 0, err
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
		q, err = db.UnorderedChunkQuerier(from, through)
	} else {
		q, err = db.ChunkQuerier(from, through)
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
	ss := q.Select(ctx, false, hints, matchers...)
	if ss.Err() != nil {
		return 0, 0, errors.Wrap(ss.Err(), "selecting series from ChunkQuerier")
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

		if err := it.Err(); err != nil {
			return 0, 0, err
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
		return 0, 0, errors.Wrap(err, "iterating ChunkSeriesSet")
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
		q, err = db.UnorderedChunkQuerier(from, through)
	} else {
		q, err = db.ChunkQuerier(from, through)
	}
	if err != nil {
		return 0, 0, err
	}

	// The querier must remain open until we've finished streaming chunks.
	defer q.Close()

	allSeries, numSeries, err := i.sendStreamingQuerySeries(ctx, q, from, through, matchers, shard, stream)
	if err != nil {
		return 0, 0, err
	}

	spanlog.DebugLog("msg", "finished sending series", "series", numSeries)

	numSamples, numChunks, numBatches, err := i.sendStreamingQueryChunks(allSeries, stream, batchSize)
	if err != nil {
		return 0, 0, err
	}

	spanlog.DebugLog("msg", "finished sending chunks", "chunks", numChunks, "batches", numBatches)

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

func (i *Ingester) sendStreamingQuerySeries(ctx context.Context, q storage.ChunkQuerier, from, through int64, matchers []*labels.Matcher, shard *sharding.ShardSelector, stream client.Ingester_QueryStreamServer) (*chunkSeriesNode, int, error) {
	// Disable chunks trimming, so that we don't have to rewrite chunks which have samples outside
	// the requested from/through range. PromQL engine can handle it.
	hints := initSelectHints(from, through)
	hints = configSelectHintsWithShard(hints, shard)
	hints = configSelectHintsWithDisabledTrimming(hints)

	// Series must be sorted so that they can be read by the querier in the order the PromQL engine expects.
	ss := q.Select(ctx, true, hints, matchers...)
	if ss.Err() != nil {
		return nil, 0, errors.Wrap(ss.Err(), "selecting series from ChunkQuerier")
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
			return nil, 0, errors.Wrap(err, "getting ChunkSeries chunk count")
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
		return nil, 0, errors.Wrap(err, "iterating ChunkSeriesSet")
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

			if err := it.Err(); err != nil {
				return 0, 0, 0, err
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

func (i *Ingester) getOrCreateTSDB(userID string) (*userTSDB, error) {
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

	// flusher doesn't actually start the ingester services
	initialLocalLimit := 0
	if i.limiter != nil {
		initialLocalLimit = i.limiter.maxSeriesPerUser(userID, 0)
	}
	ownedSeriedStateShardSize := 0
	if i.ownedSeriesService != nil {
		ownedSeriedStateShardSize = i.ownedSeriesService.ringStrategy.shardSizeForUser(userID)
	}

	userDB := &userTSDB{
		userID:                  userID,
		activeSeries:            activeseries.NewActiveSeries(asmodel.NewMatchers(matchersConfig), i.cfg.ActiveSeriesMetrics.IdleTimeout),
		seriesInMetric:          newMetricCounter(i.limiter, i.cfg.getIgnoreSeriesLimitForMetricNamesMap()),
		ingestedAPISamples:      util_math.NewEWMARate(0.2, i.cfg.RateUpdatePeriod),
		ingestedRuleSamples:     util_math.NewEWMARate(0.2, i.cfg.RateUpdatePeriod),
		instanceLimitsFn:        i.getInstanceLimits,
		instanceSeriesCount:     &i.seriesCount,
		instanceErrors:          i.metrics.rejected,
		blockMinRetention:       i.cfg.BlocksStorageConfig.TSDB.Retention,
		useOwnedSeriesForLimits: i.cfg.UseIngesterOwnedSeriesForLimits,

		ownedState: ownedSeriesState{
			shardSize:        ownedSeriedStateShardSize, // initialize series shard size so that it's correct even before we update ownedSeries for the first time
			localSeriesLimit: initialLocalLimit,
		},
	}
	userDB.triggerRecomputeOwnedSeries(recomputeOwnedSeriesReasonNewUser)

	userDBHasDB := atomic.NewBool(false)
	blocksToDelete := func(blocks []*tsdb.Block) map[ulid.ULID]struct{} {
		if !userDBHasDB.Load() {
			return nil
		}
		return userDB.blocksToDelete(blocks)
	}

	oooTW := i.limits.OutOfOrderTimeWindow(userID)
	// Create a new user database
	db, err := tsdb.Open(udir, util_log.SlogFromGoKit(userLogger), tsdbPromReg, &tsdb.Options{
		RetentionDuration:                     i.cfg.BlocksStorageConfig.TSDB.Retention.Milliseconds(),
		MinBlockDuration:                      blockRanges[0],
		MaxBlockDuration:                      blockRanges[len(blockRanges)-1],
		NoLockfile:                            true,
		StripeSize:                            i.cfg.BlocksStorageConfig.TSDB.StripeSize,
		HeadChunksWriteBufferSize:             i.cfg.BlocksStorageConfig.TSDB.HeadChunksWriteBufferSize,
		HeadChunksEndTimeVariance:             i.cfg.BlocksStorageConfig.TSDB.HeadChunksEndTimeVariance,
		WALCompression:                        i.cfg.BlocksStorageConfig.TSDB.WALCompressionType(),
		WALSegmentSize:                        i.cfg.BlocksStorageConfig.TSDB.WALSegmentSizeBytes,
		WALReplayConcurrency:                  walReplayConcurrency,
		SeriesLifecycleCallback:               userDB,
		BlocksToDelete:                        blocksToDelete,
		EnableExemplarStorage:                 true, // enable for everyone so we can raise the limit later
		MaxExemplars:                          int64(i.limiter.maxExemplarsPerUser(userID)),
		SeriesHashCache:                       i.seriesHashCache,
		EnableMemorySnapshotOnShutdown:        i.cfg.BlocksStorageConfig.TSDB.MemorySnapshotOnShutdown,
		IsolationDisabled:                     true,
		HeadChunksWriteQueueSize:              i.cfg.BlocksStorageConfig.TSDB.HeadChunksWriteQueueSize,
		EnableOverlappingCompaction:           false,                // always false since Mimir only uploads lvl 1 compacted blocks
		EnableSharding:                        true,                 // Always enable query sharding support.
		OutOfOrderTimeWindow:                  oooTW.Milliseconds(), // The unit must be same as our timestamps.
		OutOfOrderCapMax:                      int64(i.cfg.BlocksStorageConfig.TSDB.OutOfOrderCapacityMax),
		TimelyCompaction:                      i.cfg.BlocksStorageConfig.TSDB.TimelyHeadCompaction,
		HeadPostingsForMatchersCacheTTL:       i.cfg.BlocksStorageConfig.TSDB.HeadPostingsForMatchersCacheTTL,
		HeadPostingsForMatchersCacheMaxItems:  i.cfg.BlocksStorageConfig.TSDB.HeadPostingsForMatchersCacheMaxItems,
		HeadPostingsForMatchersCacheMaxBytes:  i.cfg.BlocksStorageConfig.TSDB.HeadPostingsForMatchersCacheMaxBytes,
		HeadPostingsForMatchersCacheForce:     i.cfg.BlocksStorageConfig.TSDB.HeadPostingsForMatchersCacheForce,
		BlockPostingsForMatchersCacheTTL:      i.cfg.BlocksStorageConfig.TSDB.BlockPostingsForMatchersCacheTTL,
		BlockPostingsForMatchersCacheMaxItems: i.cfg.BlocksStorageConfig.TSDB.BlockPostingsForMatchersCacheMaxItems,
		BlockPostingsForMatchersCacheMaxBytes: i.cfg.BlocksStorageConfig.TSDB.BlockPostingsForMatchersCacheMaxBytes,
		BlockPostingsForMatchersCacheForce:    i.cfg.BlocksStorageConfig.TSDB.BlockPostingsForMatchersCacheForce,
		EnableNativeHistograms:                i.limits.NativeHistogramsIngestionEnabled(userID),
		EnableOOONativeHistograms:             i.limits.OOONativeHistogramsIngestionEnabled(userID),
		SecondaryHashFunction:                 secondaryTSDBHashFunctionForUser(userID),
	}, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open TSDB: %s", udir)
	}
	db.DisableCompactions() // we will compact on our own schedule

	// Run compaction before using this TSDB. If there is data in head that needs to be put into blocks,
	// this will actually create the blocks. If there is no data (empty TSDB), this is a no-op, although
	// local blocks compaction may still take place if configured.
	level.Info(userLogger).Log("msg", "Running compaction after WAL replay")
	// Note that we want to let TSDB creation finish without being interrupted by eventual context cancellation,
	// so passing an independent context here
	err = db.Compact(context.Background())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to compact TSDB: %s", udir)
	}

	userDB.db = db
	userDBHasDB.Store(true)
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

// compactionServiceRunning is the running function of internal service responsible to periodically
// compact TSDB Head.
func (i *Ingester) compactionServiceRunning(ctx context.Context) error {
	// At ingester startup, spread the first compaction over the configured compaction
	// interval. Then, the next compactions will happen at a regular interval. This logic
	// helps to have different ingesters running the compaction at a different time,
	// effectively spreading the compactions over the configured interval.
	firstInterval, standardInterval := i.compactionServiceInterval()
	stopTicker, tickerChan := util.NewVariableTicker(firstInterval, standardInterval)
	defer func() {
		// We call stopTicker() from an anonymous function because the stopTicker()
		// reference may change during the lifetime of compactionServiceRunning().
		stopTicker()
	}()

	for ctx.Err() == nil {
		select {
		case <-tickerChan:
			// The forcedCompactionMaxTime has no meaning because force=false.
			i.compactBlocks(ctx, false, 0, nil)

			// Check if any TSDB Head should be compacted to reduce the number of in-memory series.
			i.compactBlocksToReduceInMemorySeries(ctx, time.Now())

			// Check if the desired interval has changed. We only compare the standard interval
			// before the first interval may be random due to jittering.
			if newFirstInterval, newStandardInterval := i.compactionServiceInterval(); standardInterval != newStandardInterval {
				// Stop the previous ticker before creating a new one.
				stopTicker()

				standardInterval = newStandardInterval
				stopTicker, tickerChan = util.NewVariableTicker(newFirstInterval, newStandardInterval)
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

// compactionServiceInterval returns how frequently the TSDB Head should be checked for compaction.
// The returned standardInterval is guaranteed to have no jittering applied.
// The returned intervals may change over time, depending on the ingester service state.
func (i *Ingester) compactionServiceInterval() (firstInterval, standardInterval time.Duration) {
	if i.State() == services.Starting {
		// Trigger TSDB Head compaction frequently when starting up, because we may replay data from the partition
		// if ingest storage is enabled.
		standardInterval = min(i.cfg.BlocksStorageConfig.TSDB.HeadCompactionIntervalWhileStarting, i.cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval)
	} else {
		standardInterval = i.cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval
	}

	if i.cfg.BlocksStorageConfig.TSDB.HeadCompactionIntervalJitterEnabled {
		firstInterval = util.DurationWithNegativeJitter(standardInterval, 1)
	} else {
		firstInterval = standardInterval
	}

	return
}

// Compacts all compactable blocks. Force flag will force compaction even if head is not compactable yet.
func (i *Ingester) compactBlocks(ctx context.Context, force bool, forcedCompactionMaxTime int64, allowed *util.AllowedTenants) {
	_ = concurrency.ForEachUser(ctx, i.getTSDBUsers(), i.cfg.BlocksStorageConfig.TSDB.HeadCompactionConcurrency, func(_ context.Context, userID string) error {
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

		minTimeBefore := userDB.Head().MinTime()

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

		minTimeAfter := userDB.Head().MinTime()

		// If head was compacted, its MinTime has changed. We need to recalculate series owned by this ingester,
		// because in-memory series are removed during compaction.
		if minTimeBefore != minTimeAfter {
			r := recomputeOwnedSeriesReasonCompaction
			if force && forcedCompactionMaxTime != math.MaxInt64 {
				r = recomputeOwnedSeriesReasonEarlyCompaction
			}
			userDB.triggerRecomputeOwnedSeries(r)
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
	slices.SortFunc(estimations, func(a, b seriesReductionEstimation) int {
		switch {
		case b.estimatedCount < a.estimatedCount:
			return -1
		case b.estimatedCount > a.estimatedCount:
			return 1
		default:
			return 0
		}
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
	// Don't allow callers to flush TSDB while we're in the middle of starting or shutting down.
	if ingesterState := i.State(); ingesterState != services.Running {
		err := newUnavailableError(ingesterState)
		level.Warn(i.logger).Log("msg", "flushing TSDB blocks is not allowed", "err", err)

		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(err.Error()))
		return
	}

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
		// Reverting the prepared shutdown is currently not supported by the ingest storage.
		if i.cfg.IngestStorageConfig.Enabled {
			level.Error(i.logger).Log("msg", "the ingest storage doesn't support reverting the prepared shutdown")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

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

	if i.ingestPartitionLifecycler != nil {
		// When the prepare shutdown endpoint is called there are two changes in the partitions ring behavior:
		//
		// 1. If setPrepareShutdown() is called at startup, because of the shutdown marker found on disk,
		//    the ingester shouldn't create the partition if doesn't exist, because we expect the ingester will
		//    be scaled down shortly after.
		// 2. When the ingester will shutdown we'll have to remove the ingester from the partition owners,
		//    because we expect the ingester to be scaled down.
		i.ingestPartitionLifecycler.SetCreatePartitionOnStartup(false)
		i.ingestPartitionLifecycler.SetRemoveOwnerOnShutdown(true)
	}
}

func (i *Ingester) unsetPrepareShutdown() {
	i.lifecycler.SetUnregisterOnShutdown(i.cfg.IngesterRing.UnregisterOnShutdown)
	i.lifecycler.SetFlushOnShutdown(i.cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown)
	i.metrics.shutdownMarker.Set(0)
}

// PrepareUnregisterHandler manipulates whether an ingester will unregister from the ring on its next termination.
//
// The following methods are supported:
//   - GET Returns the ingester's current unregister state.
//   - PUT Sets the ingester's unregister state.
//   - DELETE Resets the ingester's unregister state to the value passed via the RingConfig.UnregisterOnShutdown ring
//     configuration option.
//
// All methods are idempotent.
func (i *Ingester) PrepareUnregisterHandler(w http.ResponseWriter, r *http.Request) {
	if i.State() != services.Running {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	type prepareUnregisterBody struct {
		Unregister *bool `json:"unregister"`
	}

	switch r.Method {
	case http.MethodPut:
		dec := json.NewDecoder(r.Body)
		input := prepareUnregisterBody{}
		if err := dec.Decode(&input); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if input.Unregister == nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		i.lifecycler.SetUnregisterOnShutdown(*input.Unregister)
	case http.MethodDelete:
		i.lifecycler.SetUnregisterOnShutdown(i.cfg.IngesterRing.UnregisterOnShutdown)
	}

	shouldUnregister := i.lifecycler.ShouldUnregisterOnShutdown()
	util.WriteJSONResponse(w, &prepareUnregisterBody{Unregister: &shouldUnregister})
}

// PreparePartitionDownscaleHandler prepares the ingester's partition downscaling. The partition owned by the
// ingester will switch to INACTIVE state (read-only).
//
// Following methods are supported:
//
//   - GET
//     Returns timestamp when partition was switched to INACTIVE state, or 0, if partition is not in INACTIVE state.
//
//   - POST
//     Switches the partition to INACTIVE state (if not yet), and returns the timestamp when the switch to
//     INACTIVE state happened.
//
//   - DELETE
//     Sets partition back from INACTIVE to ACTIVE state.
func (i *Ingester) PreparePartitionDownscaleHandler(w http.ResponseWriter, r *http.Request) {
	logger := log.With(i.logger, "partition", i.ingestPartitionID)

	// Don't allow callers to change the shutdown configuration while we're in the middle
	// of starting or shutting down.
	if i.State() != services.Running {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	if !i.cfg.IngestStorageConfig.Enabled {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	switch r.Method {
	case http.MethodPost:
		// It's not allowed to prepare the downscale while in PENDING state. Why? Because if the downscale
		// will be later cancelled, we don't know if it was requested in PENDING or ACTIVE state, so we
		// don't know to which state reverting back. Given a partition is expected to stay in PENDING state
		// for a short period, we simply don't allow this case.
		state, _, err := i.ingestPartitionLifecycler.GetPartitionState(r.Context())
		if err != nil {
			level.Error(logger).Log("msg", "failed to check partition state in the ring", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if state == ring.PartitionPending {
			level.Warn(logger).Log("msg", "received a request to prepare partition for shutdown, but the request can't be satisfied because the partition is in PENDING state")
			w.WriteHeader(http.StatusConflict)
			return
		}

		if err := i.ingestPartitionLifecycler.ChangePartitionState(r.Context(), ring.PartitionInactive); err != nil {
			level.Error(logger).Log("msg", "failed to change partition state to inactive", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	case http.MethodDelete:
		state, _, err := i.ingestPartitionLifecycler.GetPartitionState(r.Context())
		if err != nil {
			level.Error(logger).Log("msg", "failed to check partition state in the ring", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		// If partition is inactive, make it active. We ignore other states Active and especially Pending.
		if state == ring.PartitionInactive {
			// We don't switch it back to PENDING state if there are not enough owners because we want to guarantee consistency
			// in the read path. If the partition is within the lookback period we need to guarantee that partition will be queried.
			// Moving back to PENDING will cause us loosing consistency, because PENDING partitions are not queried by design.
			// We could move back to PENDING if there are not enough owners and the partition moved to INACTIVE more than
			// "lookback period" ago, but since we delete inactive partitions with no owners that moved to inactive since longer
			// than "lookback period" ago, it looks to be an edge case not worth to address.
			if err := i.ingestPartitionLifecycler.ChangePartitionState(r.Context(), ring.PartitionActive); err != nil {
				level.Error(logger).Log("msg", "failed to change partition state to active", "err", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}

	state, stateTimestamp, err := i.ingestPartitionLifecycler.GetPartitionState(r.Context())
	if err != nil {
		level.Error(logger).Log("msg", "failed to check partition state in the ring", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if state == ring.PartitionInactive {
		util.WriteJSONResponse(w, map[string]any{"timestamp": stateTimestamp.Unix()})
	} else {
		util.WriteJSONResponse(w, map[string]any{"timestamp": 0})
	}
}

// ShutdownHandler triggers the following set of operations in order:
//   - Change the state of ring to stop accepting writes.
//   - Flush all the chunks.
func (i *Ingester) ShutdownHandler(w http.ResponseWriter, _ *http.Request) {
	originalFlush := i.lifecycler.FlushOnShutdown()

	// We want to flush the blocks.
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

// startReadRequest tries to start a read request.
// If it was successful, startReadRequest returns a function that should be
// called to finish the started read request once the request is completed.
// If it wasn't successful, the causing error is returned. In this case no
// function is returned.
func (i *Ingester) startReadRequest() (func(error), error) {
	start := time.Now()
	finish, err := i.circuitBreaker.tryAcquireReadPermit()
	if err != nil {
		return nil, err
	}

	finishReadRequest := func(err error) {
		finish(time.Since(start), err)
	}

	if err = i.checkAvailableForRead(); err != nil {
		finishReadRequest(err)
		return nil, err
	}
	if err = i.checkReadOverloaded(); err != nil {
		finishReadRequest(err)
		return nil, err
	}
	return finishReadRequest, nil
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

// PushToStorage implements ingest.Pusher interface for ingestion via ingest-storage.
func (i *Ingester) PushToStorage(ctx context.Context, req *mimirpb.WriteRequest) error {
	err := i.PushWithCleanup(ctx, req, func() { mimirpb.ReuseSlice(req.Timeseries) })
	if err != nil {
		return mapPushErrorToErrorWithStatus(err)
	}
	return nil
}

// Push implements client.IngesterServer, which is registered into gRPC server.
func (i *Ingester) Push(ctx context.Context, req *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	if !i.cfg.PushGrpcMethodEnabled {
		return nil, errPushGrpcDisabled
	}

	err := i.PushToStorage(ctx, req)
	if err != nil {
		return nil, err
	}
	return &mimirpb.WriteResponse{}, err
}

func (i *Ingester) mapReadErrorToErrorWithStatus(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	return mapReadErrorToErrorWithStatus(err)
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
func (i *Ingester) MetricsMetadata(ctx context.Context, req *client.MetricsMetadataRequest) (resp *client.MetricsMetadataResponse, err error) {
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()
	finishReadRequest, err := i.startReadRequest()
	if err != nil {
		return nil, err
	}
	defer func() { finishReadRequest(err) }()

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
	return errTooBusy
}

type utilizationBasedLimiter interface {
	services.Service

	LimitingReason() string
}

func (i *Ingester) enforceReadConsistency(ctx context.Context, tenantID string) error {
	// Read consistency is enforced by design in Mimir, unless using the ingest storage.
	if i.ingestReader == nil {
		return nil
	}

	var level string
	if c, ok := api.ReadConsistencyLevelFromContext(ctx); ok {
		level = c
	} else {
		level = i.limits.IngestStorageReadConsistency(tenantID)
	}

	spanLog := spanlogger.FromContext(ctx, i.logger)
	spanLog.DebugLog("msg", "checked read consistency", "level", level)

	if level != api.ReadConsistencyStrong {
		return nil
	}

	// Check if request already contains the minimum offset we have to guarantee being queried
	// for our partition.
	if offsets, ok := api.ReadConsistencyEncodedOffsetsFromContext(ctx); ok {
		if offset, ok := offsets.Lookup(i.ingestPartitionID); ok {
			spanLog.DebugLog("msg", "enforcing read consistency", "offset", offset)
			return errors.Wrap(i.ingestReader.WaitReadConsistencyUntilOffset(ctx, offset), "wait for read consistency")
		}
	}

	spanLog.DebugLog("msg", "enforcing read consistency", "offset", "last produced")
	return errors.Wrap(i.ingestReader.WaitReadConsistencyUntilLastProducedOffset(ctx), "wait for read consistency")
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
