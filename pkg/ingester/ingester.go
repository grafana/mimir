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
	"io"
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
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	promcfg "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/shipper"
	"github.com/weaveworks/common/httpgrpc"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/storage/sharding"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/tenant"
	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
	util_math "github.com/grafana/mimir/pkg/util/math"
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

	// IngesterRingKey is the key under which we store the ingesters ring in the KVStore.
	IngesterRingKey = "ring"

	errTSDBCreateIncompatibleState = "cannot create a new TSDB while the ingester is not in active state (current state: %s)"
	errTSDBIngest                  = "err: %v. timestamp=%s, series=%s" // Using error.Wrap puts the message before the error and if the series is too long, its truncated.
	errTSDBIngestExemplar          = "err: %v. timestamp=%s, series=%s, exemplar=%s"

	// Jitter applied to the idle timeout to prevent compaction in all ingesters concurrently.
	compactionIdleTimeoutJitter = 0.25

	instanceIngestionRateTickInterval = time.Second

	sampleOutOfOrder     = "sample-out-of-order"
	newValueForTimestamp = "new-value-for-timestamp"
	sampleOutOfBounds    = "sample-out-of-bounds"
)

var (
	errExemplarRef = errors.New("exemplars not ingested because series not already present")
)

// Shipper interface is used to have an easy way to mock it in tests.
type Shipper interface {
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
	LifecyclerConfig ring.LifecyclerConfig `yaml:"lifecycler"`

	// Config for metadata purging.
	MetadataRetainPeriod time.Duration `yaml:"metadata_retain_period" category:"advanced"`

	RateUpdatePeriod time.Duration `yaml:"rate_update_period" category:"advanced"`

	ActiveSeriesMetricsEnabled      bool                             `yaml:"active_series_metrics_enabled"`
	ActiveSeriesMetricsUpdatePeriod time.Duration                    `yaml:"active_series_metrics_update_period" category:"advanced"`
	ActiveSeriesMetricsIdleTimeout  time.Duration                    `yaml:"active_series_metrics_idle_timeout" category:"advanced"`
	ActiveSeriesCustomTrackers      ActiveSeriesCustomTrackersConfig `yaml:"active_series_custom_trackers" doc:"description=Additional custom trackers for active metrics. If there are active series matching a provided matcher (map value), the count will be exposed in the custom trackers metric labeled using the tracker name (map key). Zero valued counts are not exposed (and removed when they go back to zero)."`

	ExemplarsUpdatePeriod time.Duration `yaml:"exemplars_update_period" category:"experimental"`

	BlocksStorageConfig         mimir_tsdb.BlocksStorageConfig `yaml:"-"`
	StreamChunksWhenUsingBlocks bool                           `yaml:"-"`
	// Runtime-override for type of streaming query to use (chunks or samples).
	StreamTypeFn func() QueryStreamType `yaml:"-"`

	DefaultLimits    InstanceLimits         `yaml:"instance_limits"`
	InstanceLimitsFn func() *InstanceLimits `yaml:"-"`

	IgnoreSeriesLimitForMetricNames string `yaml:"ignore_series_limit_for_metric_names" category:"advanced"`

	// For testing, you can override the address and ID of this ingester.
	ingesterClientFactory func(addr string, cfg client.Config) (client.HealthAndIngesterClient, error)
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.LifecyclerConfig.RingConfig.KVStore.Store = "memberlist"
	cfg.LifecyclerConfig.RegisterFlags(f)

	f.DurationVar(&cfg.MetadataRetainPeriod, "ingester.metadata-retain-period", 10*time.Minute, "Period at which metadata we have not seen will remain in memory before being deleted.")

	f.DurationVar(&cfg.RateUpdatePeriod, "ingester.rate-update-period", 15*time.Second, "Period with which to update the per-user ingestion rates.")
	f.BoolVar(&cfg.ActiveSeriesMetricsEnabled, "ingester.active-series-metrics-enabled", true, "Enable tracking of active series and export them as metrics.")
	f.DurationVar(&cfg.ActiveSeriesMetricsUpdatePeriod, "ingester.active-series-metrics-update-period", 1*time.Minute, "How often to update active series metrics.")
	f.DurationVar(&cfg.ActiveSeriesMetricsIdleTimeout, "ingester.active-series-metrics-idle-timeout", 10*time.Minute, "After what time a series is considered to be inactive.")
	f.Var(&cfg.ActiveSeriesCustomTrackers, "ingester.active-series-custom-trackers", "Additional active series metrics, matching the provided matchers. Matchers should be in form <name>:<matcher>, like 'foobar:{foo=\"bar\"}'. Multiple matchers can be provided either providing the flag multiple times or providing multiple semicolon-separated values to a single flag.")

	f.BoolVar(&cfg.StreamChunksWhenUsingBlocks, "ingester.stream-chunks-when-using-blocks", true, "Stream chunks from ingesters to queriers.")
	f.DurationVar(&cfg.ExemplarsUpdatePeriod, "ingester.exemplars-update-period", 15*time.Second, "Period with which to update per-user max exemplars.")

	f.Float64Var(&cfg.DefaultLimits.MaxIngestionRate, "ingester.instance-limits.max-ingestion-rate", 0, "Max ingestion rate (samples/sec) that ingester will accept. This limit is per-ingester, not per-tenant. Additional push requests will be rejected. Current ingestion rate is computed as exponentially weighted moving average, updated every second. 0 = unlimited.")
	f.Int64Var(&cfg.DefaultLimits.MaxInMemoryTenants, "ingester.instance-limits.max-tenants", 0, "Max users (also referred to as 'tenants') that this ingester can hold. Requests from additional users will be rejected. 0 = unlimited.")
	f.Int64Var(&cfg.DefaultLimits.MaxInMemorySeries, "ingester.instance-limits.max-series", 0, "Max series that this ingester can hold (across all tenants). Requests to create additional series will be rejected. 0 = unlimited.")
	f.Int64Var(&cfg.DefaultLimits.MaxInflightPushRequests, "ingester.instance-limits.max-inflight-push-requests", 30000, "Max inflight push requests that this ingester can handle (across all tenants). Additional requests will be rejected. 0 = unlimited.")

	f.StringVar(&cfg.IgnoreSeriesLimitForMetricNames, "ingester.ignore-series-limit-for-metric-names", "", "Comma-separated list of metric names, for which the -ingester.max-global-series-per-metric limit will be ignored. Does not affect the -ingester.max-global-series-per-user limit.")
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

	cfg          Config
	clientConfig client.Config

	metrics *ingesterMetrics
	logger  log.Logger

	activeSeriesMatcher *ActiveSeriesMatchers

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

	subservices *services.Manager

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
}

func newIngester(cfg Config, limits *validation.Overrides, registerer prometheus.Registerer, logger log.Logger) (*Ingester, error) {
	if cfg.BlocksStorageConfig.Bucket.Backend == bucket.Filesystem {
		level.Warn(logger).Log("msg", "-blocks-storage.backend=filesystem is for development and testing only; you should switch to an external object store for production use or use a shared filesystem")
	}

	bucketClient, err := bucket.NewClient(context.Background(), cfg.BlocksStorageConfig.Bucket, "ingester", logger, registerer)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create the bucket client")
	}

	return &Ingester{
		cfg:                 cfg,
		limits:              limits,
		logger:              logger,
		activeSeriesMatcher: &ActiveSeriesMatchers{},

		tsdbs:               make(map[string]*userTSDB),
		usersMetadata:       make(map[string]*userMetricsMetadata),
		bucket:              bucketClient,
		tsdbMetrics:         newTSDBMetrics(registerer),
		forceCompactTrigger: make(chan requestWithUsersAndCallback),
		shipTrigger:         make(chan requestWithUsersAndCallback),
		seriesHashCache:     hashcache.NewSeriesHashCache(cfg.BlocksStorageConfig.TSDB.SeriesHashCacheMaxBytes),
	}, nil
}

// New returns an Ingester that uses Mimir block storage.
func New(cfg Config, clientConfig client.Config, limits *validation.Overrides, registerer prometheus.Registerer, logger log.Logger) (*Ingester, error) {
	defaultInstanceLimits = &cfg.DefaultLimits

	if cfg.ingesterClientFactory == nil {
		cfg.ingesterClientFactory = client.MakeIngesterClient
	}

	i, err := newIngester(cfg, limits, registerer, logger)
	if err != nil {
		return nil, err
	}
	i.clientConfig = clientConfig
	i.ingestionRate = util_math.NewEWMARate(0.2, instanceIngestionRateTickInterval)
	i.metrics = newIngesterMetrics(registerer, cfg.ActiveSeriesMetricsEnabled, i.activeSeriesMatcher.MatcherNames(), i.getInstanceLimits, i.ingestionRate, &i.inflightPushRequests)

	asm, err := NewActiveSeriesMatchers(cfg.ActiveSeriesCustomTrackers)
	if err != nil {
		return nil, err
	}
	i.activeSeriesMatcher = asm

	// Replace specific metrics which we can't directly track but we need to read
	// them from the underlying system (ie. TSDB).
	if registerer != nil {
		promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
			Name: "cortex_ingester_memory_series",
			Help: "The current number of series in memory.",
		}, i.getMemorySeriesMetric)

		promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
			Name: "cortex_ingester_oldest_unshipped_block_timestamp_seconds",
			Help: "Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.",
		}, i.getOldestUnshippedBlockMetric)
	}

	i.lifecycler, err = ring.NewLifecycler(cfg.LifecyclerConfig, i, "ingester", IngesterRingKey, cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown, logger, prometheus.WrapRegistererWithPrefix("cortex_", registerer))
	if err != nil {
		return nil, err
	}
	i.subservicesWatcher = services.NewFailureWatcher()
	i.subservicesWatcher.WatchService(i.lifecycler)

	// Init the limter and instantiate the user states which depend on it
	i.limiter = NewLimiter(
		limits,
		i.lifecycler,
		cfg.LifecyclerConfig.RingConfig.ReplicationFactor,
		cfg.LifecyclerConfig.RingConfig.ZoneAwarenessEnabled)

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
	i.metrics = newIngesterMetrics(registerer, false, nil, i.getInstanceLimits, nil, &i.inflightPushRequests)

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

	var err error
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

	exemplarUpdateTicker := time.NewTicker(i.cfg.ExemplarsUpdatePeriod)
	defer exemplarUpdateTicker.Stop()

	var activeSeriesTickerChan <-chan time.Time
	if i.cfg.ActiveSeriesMetricsEnabled {
		t := time.NewTicker(i.cfg.ActiveSeriesMetricsUpdatePeriod)
		activeSeriesTickerChan = t.C
		defer t.Stop()
	}

	// Similarly to the above, this is a hardcoded value.
	metadataPurgeTicker := time.NewTicker(metadataPurgePeriod)
	defer metadataPurgeTicker.Stop()

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

		case <-exemplarUpdateTicker.C:
			i.applyExemplarsSettings()

		case <-activeSeriesTickerChan:
			i.updateActiveSeries(time.Now())

		case <-ctx.Done():
			return nil
		case err := <-i.subservicesWatcher.Chan():
			return errors.Wrap(err, "ingester subservice failed")
		}
	}
}

func (i *Ingester) updateActiveSeries(now time.Time) {
	purgeTime := now.Add(-i.cfg.ActiveSeriesMetricsIdleTimeout)
	for _, userID := range i.getTSDBUsers() {
		userDB := i.getTSDB(userID)
		if userDB == nil {
			continue
		}

		userDB.activeSeries.Purge(purgeTime)
		allActive, activeMatching := userDB.activeSeries.Active()
		if allActive > 0 {
			i.metrics.activeSeriesPerUser.WithLabelValues(userID).Set(float64(allActive))
		} else {
			i.metrics.activeSeriesPerUser.DeleteLabelValues(userID)
		}
		for idx, name := range i.activeSeriesMatcher.MatcherNames() {
			// We only set the metrics for matchers that actually exist, to avoid increasing cardinality with zero valued metrics.
			if activeMatching[idx] > 0 {
				i.metrics.activeSeriesCustomTrackersPerUser.WithLabelValues(userID, name).Set(float64(activeMatching[idx]))
			} else {
				i.metrics.activeSeriesCustomTrackersPerUser.DeleteLabelValues(userID, name)
			}
		}
	}
}

// Go through all tenants and apply the current max-exemplars setting.
// If it changed, tsdb will resize the buffer; if it didn't change tsdb will return quickly.
func (i *Ingester) applyExemplarsSettings() {
	for _, userID := range i.getTSDBUsers() {
		globalValue := i.limits.MaxGlobalExemplarsPerUser(userID)
		localValue := i.limiter.convertGlobalToLocalLimit(userID, globalValue)
		// We populate a Config struct with just one value, which is OK
		// because Head.ApplyConfig only looks at one value.
		// The other fields in Config are things like Rules, Scrape
		// settings, which don't apply to Head.
		cfg := promcfg.Config{
			StorageConfig: promcfg.StorageConfig{
				ExemplarsConfig: &promcfg.ExemplarsConfig{
					MaxExemplars: int64(localValue),
				},
			},
		}
		tsdb := i.getTSDB(userID)
		if tsdb == nil {
			continue
		}
		if err := tsdb.Head().ApplyConfig(&cfg); err != nil {
			level.Error(i.logger).Log("msg", "failed to apply config to TSDB", "user", userID, "err", err)
		}
	}
}

// GetRef() is an extra method added to TSDB to let Mimir check before calling Add()
type extendedAppender interface {
	storage.Appender
	storage.GetRef
}

// PushWithCleanup is the Push() implementation for blocks storage and takes a WriteRequest and adds it to the TSDB head.
func (i *Ingester) PushWithCleanup(ctx context.Context, req *mimirpb.WriteRequest, cleanup func()) (*mimirpb.WriteResponse, error) {
	// NOTE: because we use `unsafe` in deserialisation, we must not
	// retain anything from `req` past the exit from this function.
	defer cleanup()

	var firstPartialErr error

	if err := i.checkRunning(); err != nil {
		return nil, err
	}

	// We will report *this* request in the error too.
	inflight := i.inflightPushRequests.Inc()
	defer i.inflightPushRequests.Dec()

	il := i.getInstanceLimits()
	if il != nil && il.MaxInflightPushRequests > 0 {
		if inflight > il.MaxInflightPushRequests {
			return nil, errTooManyInflightPushRequests
		}
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	if il != nil && il.MaxIngestionRate > 0 {
		if rate := i.ingestionRate.Rate(); rate >= il.MaxIngestionRate {
			return nil, errMaxSamplesPushRateLimitReached
		}
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
		return nil, wrapWithUser(err, userID)
	}

	if err := db.acquireAppendLock(); err != nil {
		return &mimirpb.WriteResponse{}, httpgrpc.Errorf(http.StatusServiceUnavailable, wrapWithUser(err, userID).Error())
	}
	defer db.releaseAppendLock()

	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		span.LogFields(otlog.String("event", "acquired append lock"))
	}

	// Keep track of some stats which are tracked only if the samples will be
	// successfully committed
	var (
		succeededSamplesCount     = 0
		failedSamplesCount        = 0
		succeededExemplarsCount   = 0
		failedExemplarsCount      = 0
		startAppend               = time.Now()
		sampleOutOfBoundsCount    = 0
		sampleOutOfOrderCount     = 0
		newValueForTimestampCount = 0
		perUserSeriesLimitCount   = 0
		perMetricSeriesLimitCount = 0

		minAppendTime, minAppendTimeAvailable = db.Head().AppendableMinValidTime()

		updateFirstPartial = func(errFn func() error) {
			if firstPartialErr == nil {
				firstPartialErr = errFn()
			}
		}
	)

	// Walk the samples, appending them to the users database
	app := db.Appender(ctx).(extendedAppender)

	if span != nil {
		span.LogFields(otlog.String("event", "got appender"),
			otlog.Int("numseries", len(req.Timeseries)))
	}

	for _, ts := range req.Timeseries {
		// The labels must be sorted (in our case, it's guaranteed a write request
		// has sorted labels once hit the ingester).

		// Fast path in case we only have samples and they are all out of bounds.
		if minAppendTimeAvailable && len(ts.Samples) > 0 && len(ts.Exemplars) == 0 && allOutOfBounds(ts.Samples, minAppendTime) {
			failedSamplesCount += len(ts.Samples)
			sampleOutOfBoundsCount += len(ts.Samples)

			updateFirstPartial(func() error {
				return wrappedTSDBIngestErr(storage.ErrOutOfBounds, model.Time(ts.Samples[0].TimestampMs), ts.Labels)
			})
			continue
		}

		// Look up a reference for this series.
		ref, copiedLabels := app.GetRef(mimirpb.FromLabelAdaptersToLabels(ts.Labels))

		// To find out if any sample was added to this series, we keep old value.
		oldSucceededSamplesCount := succeededSamplesCount

		for _, s := range ts.Samples {
			var err error

			// If the cached reference exists, we try to use it.
			if ref != 0 {
				if _, err = app.Append(ref, copiedLabels, s.TimestampMs, s.Value); err == nil {
					succeededSamplesCount++
					continue
				}
			} else {
				// Copy the label set because both TSDB and the active series tracker may retain it.
				copiedLabels = mimirpb.FromLabelAdaptersToLabelsWithCopy(ts.Labels)

				// Retain the reference in case there are multiple samples for the series.
				if ref, err = app.Append(0, copiedLabels, s.TimestampMs, s.Value); err == nil {
					succeededSamplesCount++
					continue
				}
			}

			failedSamplesCount++

			// Check if the error is a soft error we can proceed on. If so, we keep track
			// of it, so that we can return it back to the distributor, which will return a
			// 400 error to the client. The client (Prometheus) will not retry on 400, and
			// we actually ingested all samples which haven't failed.
			switch cause := errors.Cause(err); cause {
			case storage.ErrOutOfBounds:
				sampleOutOfBoundsCount++
				updateFirstPartial(func() error { return wrappedTSDBIngestErr(err, model.Time(s.TimestampMs), ts.Labels) })
				continue

			case storage.ErrOutOfOrderSample:
				sampleOutOfOrderCount++
				updateFirstPartial(func() error { return wrappedTSDBIngestErr(err, model.Time(s.TimestampMs), ts.Labels) })
				continue

			case storage.ErrDuplicateSampleForTimestamp:
				newValueForTimestampCount++
				updateFirstPartial(func() error { return wrappedTSDBIngestErr(err, model.Time(s.TimestampMs), ts.Labels) })
				continue

			case errMaxSeriesPerUserLimitExceeded:
				perUserSeriesLimitCount++
				updateFirstPartial(func() error { return makeLimitError(perUserSeriesLimit, i.limiter.FormatError(userID, cause)) })
				continue

			case errMaxSeriesPerMetricLimitExceeded:
				perMetricSeriesLimitCount++
				updateFirstPartial(func() error {
					return makeMetricLimitError(perMetricSeriesLimit, copiedLabels, i.limiter.FormatError(userID, cause))
				})
				continue
			}

			// The error looks an issue on our side, so we should rollback
			if rollbackErr := app.Rollback(); rollbackErr != nil {
				level.Warn(i.logger).Log("msg", "failed to rollback on error", "user", userID, "err", rollbackErr)
			}

			return nil, wrapWithUser(err, userID)
		}

		if i.cfg.ActiveSeriesMetricsEnabled && succeededSamplesCount > oldSucceededSamplesCount {
			db.activeSeries.UpdateSeries(mimirpb.FromLabelAdaptersToLabels(ts.Labels), startAppend, func(l labels.Labels) labels.Labels {
				// we must already have copied the labels if succeededSamplesCount has been incremented.
				return copiedLabels
			})
		}

		if len(ts.Exemplars) > 0 && i.limits.MaxGlobalExemplarsPerUser(userID) > 0 {
			// app.AppendExemplar currently doesn't create the series, it must
			// already exist.  If it does not then drop.
			if ref == 0 {
				updateFirstPartial(func() error {
					return wrappedTSDBIngestExemplarErr(errExemplarRef,
						model.Time(ts.Exemplars[0].TimestampMs), ts.Labels, ts.Exemplars[0].Labels)
				})
				failedExemplarsCount += len(ts.Exemplars)
			} else { // Note that else is explicit, rather than a continue in the above if, in case of additional logic post exemplar processing.
				for _, ex := range ts.Exemplars {
					e := exemplar.Exemplar{
						Value:  ex.Value,
						Ts:     ex.TimestampMs,
						HasTs:  true,
						Labels: mimirpb.FromLabelAdaptersToLabelsWithCopy(ex.Labels),
					}

					if _, err = app.AppendExemplar(ref, nil, e); err == nil {
						succeededExemplarsCount++
						continue
					}

					// Error adding exemplar
					updateFirstPartial(func() error {
						return wrappedTSDBIngestExemplarErr(err, model.Time(ex.TimestampMs), ts.Labels, ex.Labels)
					})
					failedExemplarsCount++
				}
			}
		}
	}

	// At this point all samples have been added to the appender, so we can track the time it took.
	i.metrics.appenderAddDuration.Observe(time.Since(startAppend).Seconds())

	if span != nil {
		span.LogFields(otlog.String("event", "start commit"),
			otlog.Int("succeededSamplesCount", succeededSamplesCount),
			otlog.Int("failedSamplesCount", failedSamplesCount),
			otlog.Int("succeededExemplarsCount", succeededExemplarsCount),
			otlog.Int("failedExemplarsCount", failedExemplarsCount))
	}

	startCommit := time.Now()
	if err := app.Commit(); err != nil {
		return nil, wrapWithUser(err, userID)
	}
	i.metrics.appenderCommitDuration.Observe(time.Since(startCommit).Seconds())

	// If only invalid samples are pushed, don't change "last update", as TSDB was not modified.
	if succeededSamplesCount > 0 {
		db.setLastUpdate(time.Now())
	}

	// Increment metrics only if the samples have been successfully committed.
	// If the code didn't reach this point, it means that we returned an error
	// which will be converted into an HTTP 5xx and the client should/will retry.
	i.metrics.ingestedSamples.Add(float64(succeededSamplesCount))
	i.metrics.ingestedSamplesFail.Add(float64(failedSamplesCount))
	i.metrics.ingestedExemplars.Add(float64(succeededExemplarsCount))
	i.metrics.ingestedExemplarsFail.Add(float64(failedExemplarsCount))

	if sampleOutOfBoundsCount > 0 {
		validation.DiscardedSamples.WithLabelValues(sampleOutOfBounds, userID).Add(float64(sampleOutOfBoundsCount))
	}
	if sampleOutOfOrderCount > 0 {
		validation.DiscardedSamples.WithLabelValues(sampleOutOfOrder, userID).Add(float64(sampleOutOfOrderCount))
	}
	if newValueForTimestampCount > 0 {
		validation.DiscardedSamples.WithLabelValues(newValueForTimestamp, userID).Add(float64(newValueForTimestampCount))
	}
	if perUserSeriesLimitCount > 0 {
		validation.DiscardedSamples.WithLabelValues(perUserSeriesLimit, userID).Add(float64(perUserSeriesLimitCount))
	}
	if perMetricSeriesLimitCount > 0 {
		validation.DiscardedSamples.WithLabelValues(perMetricSeriesLimit, userID).Add(float64(perMetricSeriesLimitCount))
	}
	if succeededSamplesCount > 0 {
		i.ingestionRate.Add(int64(succeededSamplesCount))

		switch req.Source {
		case mimirpb.RULE:
			db.ingestedRuleSamples.Add(int64(succeededSamplesCount))
		case mimirpb.API:
			fallthrough
		default:
			db.ingestedAPISamples.Add(int64(succeededSamplesCount))
		}
	}

	if firstPartialErr != nil {
		code := http.StatusBadRequest
		var ve *validationError
		if errors.As(firstPartialErr, &ve) {
			code = ve.code
		}
		return &mimirpb.WriteResponse{}, httpgrpc.Errorf(code, wrapWithUser(firstPartialErr, userID).Error())
	}

	return &mimirpb.WriteResponse{}, nil
}

func (i *Ingester) QueryExemplars(ctx context.Context, req *client.ExemplarQueryRequest) (*client.ExemplarQueryResponse, error) {
	if err := i.checkRunning(); err != nil {
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

func (i *Ingester) MetricsForLabelMatchers(ctx context.Context, req *client.MetricsForLabelMatchersRequest) (*client.MetricsForLabelMatchersResponse, error) {
	if err := i.checkRunning(); err != nil {
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
	_, _, matchersSet, err := client.FromMetricsForLabelMatchersRequest(req)
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

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	db := i.getTSDB(userID)
	if db == nil {
		return &client.UserStatsResponse{}, nil
	}

	return createUserStats(db), nil
}

func (i *Ingester) AllUserStats(ctx context.Context, req *client.UserStatsRequest) (*client.UsersStatsResponse, error) {
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
		response.Stats = append(response.Stats, &client.UserIDStatsResponse{
			UserId: userID,
			Data:   createUserStats(db),
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
	return labelValuesCardinality(
		req.GetLabelNames(),
		matchers,
		idx,
		tsdb.PostingsForMatchers,
		labelValuesCardinalityTargetSizeBytes,
		srv,
	)
}

func createUserStats(db *userTSDB) *client.UserStatsResponse {
	apiRate := db.ingestedAPISamples.Rate()
	ruleRate := db.ingestedRuleSamples.Rate()
	return &client.UserStatsResponse{
		IngestionRate:     apiRate + ruleRate,
		ApiIngestionRate:  apiRate,
		RuleIngestionRate: ruleRate,
		NumSeries:         db.Head().NumSeries(),
	}
}

const queryStreamBatchMessageSize = 1 * 1024 * 1024

// QueryStream streams metrics from a TSDB. This implements the client.IngesterServer interface
func (i *Ingester) QueryStream(req *client.QueryRequest, stream client.Ingester_QueryStreamServer) error {
	if err := i.checkRunning(); err != nil {
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
		level.Debug(spanlog).Log("msg", "using queryStreamChunks")
		numSeries, numSamples, err = i.queryStreamChunks(ctx, db, int64(from), int64(through), matchers, shard, stream)
	} else {
		level.Debug(spanlog).Log("msg", "using queryStreamSamples")
		numSeries, numSamples, err = i.queryStreamSamples(ctx, db, int64(from), int64(through), matchers, shard, stream)
	}
	if err != nil {
		return err
	}

	i.metrics.queriedSeries.Observe(float64(numSeries))
	i.metrics.queriedSamples.Observe(float64(numSamples))
	level.Debug(spanlog).Log("series", numSeries, "samples", numSamples)
	return nil
}

func (i *Ingester) queryStreamSamples(ctx context.Context, db *userTSDB, from, through int64, matchers []*labels.Matcher, shard *sharding.ShardSelector, stream client.Ingester_QueryStreamServer) (numSeries, numSamples int, _ error) {
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
	for ss.Next() {
		series := ss.At()

		// convert labels to LabelAdapter
		ts := mimirpb.TimeSeries{
			Labels: mimirpb.FromLabelsToLabelAdapters(series.Labels()),
		}

		it := series.Iterator()
		for it.Next() {
			t, v := it.At()
			ts.Samples = append(ts.Samples, mimirpb.Sample{Value: v, TimestampMs: t})
		}
		numSamples += len(ts.Samples)
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

	// Final flush any existing metrics
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

// queryStreamChunks streams metrics from a TSDB. This implements the client.IngesterServer interface
func (i *Ingester) queryStreamChunks(ctx context.Context, db *userTSDB, from, through int64, matchers []*labels.Matcher, shard *sharding.ShardSelector, stream client.Ingester_QueryStreamServer) (numSeries, numSamples int, _ error) {
	q, err := db.ChunkQuerier(ctx, from, through)
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
	for ss.Next() {
		series := ss.At()

		// convert labels to LabelAdapter
		ts := client.TimeSeriesChunk{
			Labels: mimirpb.FromLabelsToLabelAdapters(series.Labels()),
		}

		it := series.Iterator()
		for it.Next() {
			// Chunks are ordered by min time.
			meta := it.At()

			// It is not guaranteed that chunk returned by iterator is populated.
			// For now just return error. We could also try to figure out how to read the chunk.
			if meta.Chunk == nil {
				return 0, 0, errors.Errorf("unfilled chunk returned from TSDB chunk querier")
			}

			ch := client.Chunk{
				StartTimestampMs: meta.MinTime,
				EndTimestampMs:   meta.MaxTime,
				Data:             meta.Chunk.Bytes(),
			}

			switch meta.Chunk.Encoding() {
			case chunkenc.EncXOR:
				ch.Encoding = int32(chunk.PrometheusXorChunk)
			default:
				return 0, 0, errors.Errorf("unknown chunk encoding from TSDB chunk querier: %v", meta.Chunk.Encoding())
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
			return nil, errMaxUsersLimitReached
		}
	}

	// Create the database and a shipper for a user
	db, err := i.createTSDB(userID)
	if err != nil {
		return nil, err
	}

	// Add the db to list of user databases
	i.tsdbs[userID] = db
	i.metrics.memUsers.Inc()

	return db, nil
}

// createTSDB creates a TSDB for a given userID, and returns the created db.
func (i *Ingester) createTSDB(userID string) (*userTSDB, error) {
	tsdbPromReg := prometheus.NewRegistry()
	udir := i.cfg.BlocksStorageConfig.TSDB.BlocksDir(userID)
	userLogger := util_log.WithUserID(userID, i.logger)

	blockRanges := i.cfg.BlocksStorageConfig.TSDB.BlockRanges.ToMilliseconds()

	userDB := &userTSDB{
		userID:              userID,
		activeSeries:        NewActiveSeries(i.activeSeriesMatcher),
		seriesInMetric:      newMetricCounter(i.limiter, i.cfg.getIgnoreSeriesLimitForMetricNamesMap()),
		ingestedAPISamples:  util_math.NewEWMARate(0.2, i.cfg.RateUpdatePeriod),
		ingestedRuleSamples: util_math.NewEWMARate(0.2, i.cfg.RateUpdatePeriod),

		instanceLimitsFn:    i.getInstanceLimits,
		instanceSeriesCount: &i.seriesCount,
	}

	maxExemplars := i.limiter.convertGlobalToLocalLimit(userID, i.limits.MaxGlobalExemplarsPerUser(userID))
	// Create a new user database
	db, err := tsdb.Open(udir, userLogger, tsdbPromReg, &tsdb.Options{
		RetentionDuration:              i.cfg.BlocksStorageConfig.TSDB.Retention.Milliseconds(),
		MinBlockDuration:               blockRanges[0],
		MaxBlockDuration:               blockRanges[len(blockRanges)-1],
		NoLockfile:                     true,
		StripeSize:                     i.cfg.BlocksStorageConfig.TSDB.StripeSize,
		HeadChunksWriteBufferSize:      i.cfg.BlocksStorageConfig.TSDB.HeadChunksWriteBufferSize,
		HeadChunksEndTimeVariance:      i.cfg.BlocksStorageConfig.TSDB.HeadChunksEndTimeVariance,
		WALCompression:                 i.cfg.BlocksStorageConfig.TSDB.WALCompressionEnabled,
		WALSegmentSize:                 i.cfg.BlocksStorageConfig.TSDB.WALSegmentSizeBytes,
		SeriesLifecycleCallback:        userDB,
		BlocksToDelete:                 userDB.blocksToDelete,
		EnableExemplarStorage:          true, // enable for everyone so we can raise the limit later
		MaxExemplars:                   int64(maxExemplars),
		SeriesHashCache:                i.seriesHashCache,
		EnableMemorySnapshotOnShutdown: i.cfg.BlocksStorageConfig.TSDB.MemorySnapshotOnShutdown,
		IsolationDisabled:              !i.cfg.BlocksStorageConfig.TSDB.IsolationEnabled,
		HeadChunksWriteQueueSize:       i.cfg.BlocksStorageConfig.TSDB.HeadChunksWriteQueueSize,
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

	if db.Head().NumSeries() > 0 {
		// If there are series in the head, use max time from head. If this time is too old,
		// TSDB will be eligible for flushing and closing sooner, unless more data is pushed to it quickly.
		userDB.setLastUpdate(util.TimeFromMillis(db.Head().MaxTime()))
	} else {
		// If head is empty (eg. new TSDB), don't close it right after.
		userDB.setLastUpdate(time.Now())
	}

	// Thanos shipper requires at least 1 external label to be set. For this reason,
	// we set the tenant ID as external label and we'll filter it out when reading
	// the series from the storage.
	l := labels.Labels{
		{
			Name:  mimir_tsdb.TenantIDExternalLabel,
			Value: userID,
		}, {
			Name:  mimir_tsdb.IngesterIDExternalLabel,
			Value: i.shipperIngesterID,
		},
	}

	// Create a new shipper for this database
	if i.cfg.BlocksStorageConfig.TSDB.IsBlocksShippingEnabled() {
		userDB.shipper = shipper.New(
			userLogger,
			tsdbPromReg,
			udir,
			bucket.NewUserBucketClient(userID, i.bucket, i.limits),
			func() labels.Labels { return l },
			metadata.ReceiveSource,
			false, // No need to upload compacted blocks. Mimir compactor takes care of that.
			true,  // Allow out of order uploads. It's fine in Mimir's context.
			metadata.NoneFunc,
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
			i.metrics.activeSeriesPerUser.DeleteLabelValues(userID)
			for _, name := range i.metrics.activeSeriesCustomTrackerNames {
				i.metrics.activeSeriesCustomTrackersPerUser.DeleteLabelValues(userID, name)
			}
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

	queue := make(chan string)
	group, groupCtx := errgroup.WithContext(ctx)

	// Create a pool of workers which will open existing TSDBs.
	for n := 0; n < i.cfg.BlocksStorageConfig.TSDB.MaxTSDBOpeningConcurrencyOnStartup; n++ {
		group.Go(func() error {
			for userID := range queue {
				startTime := time.Now()

				db, err := i.createTSDB(userID)
				if err != nil {
					level.Error(i.logger).Log("msg", "unable to open TSDB", "err", err, "user", userID)
					return errors.Wrapf(err, "unable to open TSDB for user %s", userID)
				}

				// Add the database to the map of user databases
				i.tsdbsMtx.Lock()
				i.tsdbs[userID] = db
				i.tsdbsMtx.Unlock()
				i.metrics.memUsers.Inc()

				i.metrics.walReplayTime.Observe(time.Since(startTime).Seconds())
			}

			return nil
		})
	}

	// Spawn a goroutine to find all users with a TSDB on the filesystem.
	group.Go(func() error {
		// Close the queue once filesystem walking is done.
		defer close(queue)

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
				if err == io.EOF {
					return filepath.SkipDir
				}

				level.Error(i.logger).Log("msg", "unable to read TSDB dir", "err", err, "user", userID, "path", path)
				return errors.Wrapf(err, "unable to read TSDB dir %s for user %s", path, userID)
			}

			// Enqueue the user to be processed.
			select {
			case queue <- userID:
				// Nothing to do.
			case <-groupCtx.Done():
				// Interrupt in case a failure occurred in another goroutine.
				return nil
			}

			// Don't descend into subdirectories.
			return filepath.SkipDir
		})

		return errors.Wrapf(walkErr, "unable to walk directory %s containing existing TSDBs", i.cfg.BlocksStorageConfig.TSDB.Dir)
	})

	// Wait for all workers to complete.
	err := group.Wait()
	if err != nil {
		level.Error(i.logger).Log("msg", "error while opening existing TSDBs", "err", err)
		return err
	}

	level.Info(i.logger).Log("msg", "successfully opened existing TSDBs")
	return nil
}

// getMemorySeriesMetric returns the total number of in-memory series across all open TSDBs.
func (i *Ingester) getMemorySeriesMetric() float64 {
	if err := i.checkRunning(); err != nil {
		return 0
	}

	i.tsdbsMtx.RLock()
	defer i.tsdbsMtx.RUnlock()

	count := uint64(0)
	for _, db := range i.tsdbs {
		count += db.Head().NumSeries()
	}

	return float64(count)
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
		if !userDB.casState(active, activeShipping) {
			level.Info(i.logger).Log("msg", "shipper skipped because the TSDB is not active", "user", userID)
			return nil
		}
		defer userDB.casState(activeShipping, active)

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
	ticker := time.NewTicker(i.cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval)
	defer ticker.Stop()

	for ctx.Err() == nil {
		select {
		case <-ticker.C:
			i.compactBlocks(ctx, false, nil)

		case req := <-i.forceCompactTrigger:
			i.compactBlocks(ctx, true, req.users)
			close(req.callback) // Notify back.

		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

// Compacts all compactable blocks. Force flag will force compaction even if head is not compactable yet.
func (i *Ingester) compactBlocks(ctx context.Context, force bool, allowed *util.AllowedTenants) {
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
			err = userDB.compactHead(i.cfg.BlocksStorageConfig.TSDB.BlockRanges[0].Milliseconds())

		case i.compactionIdleTimeout > 0 && userDB.isIdle(time.Now(), i.compactionIdleTimeout):
			reason = "idle"
			level.Info(i.logger).Log("msg", "TSDB is idle, forcing compaction", "user", userID)
			err = userDB.compactHead(i.cfg.BlocksStorageConfig.TSDB.BlockRanges[0].Milliseconds())

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
	if !userDB.casState(active, closing) {
		return tsdbNotActive
	}

	// If TSDB is fully closed, we will set state to 'closed', which will prevent this defered closing -> active transition.
	defer userDB.casState(closing, active)

	// Make sure we don't ignore any possible inflight pushes.
	userDB.pushesInFlight.Wait()

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
	userDB.casState(closing, closed)

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

	validation.DeletePerUserValidationMetrics(userID, i.logger)

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

// TransferOut implements ring.FlushTransferer.
func (i *Ingester) TransferOut(_ context.Context) error {
	return ring.ErrTransferDisabled
}

// This method will flush all data. It is called as part of Lifecycler's shutdown (if flush on shutdown is configured), or from the flusher.
//
// When called as during Lifecycler shutdown, this happens as part of normal Ingester shutdown (see stopping method).
// Samples are not received at this stage. Compaction and Shipping loops have already been stopped as well.
//
// When used from flusher, ingester is constructed in a way that compaction, shipping and receiving of samples is never started.
func (i *Ingester) Flush() {
	level.Info(i.logger).Log("msg", "starting to flush and ship TSDB blocks")

	ctx := context.Background()

	i.compactBlocks(ctx, true, nil)
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

func wrappedTSDBIngestErr(ingestErr error, timestamp model.Time, labels []mimirpb.LabelAdapter) error {
	if ingestErr == nil {
		return nil
	}

	return fmt.Errorf(errTSDBIngest, ingestErr, timestamp.Time().UTC().Format(time.RFC3339Nano), mimirpb.FromLabelAdaptersToLabels(labels).String())
}

func wrappedTSDBIngestExemplarErr(ingestErr error, timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) error {
	if ingestErr == nil {
		return nil
	}

	return fmt.Errorf(errTSDBIngestExemplar, ingestErr, timestamp.Time().UTC().Format(time.RFC3339Nano),
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
		return defaultInstanceLimits
	}

	l := i.cfg.InstanceLimitsFn()
	if l == nil {
		return defaultInstanceLimits
	}

	return l
}

// ShutdownHandler triggers the following set of operations in order:
//     * Change the state of ring to stop accepting writes.
//     * Flush all the chunks.
func (i *Ingester) ShutdownHandler(w http.ResponseWriter, r *http.Request) {
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
	return i.PushWithCleanup(ctx, req, func() { mimirpb.ReuseSlice(req.Timeseries) })
}

// pushMetadata returns number of ingested metadata.
func (i *Ingester) pushMetadata(ctx context.Context, userID string, metadata []*mimirpb.MetricMetadata) int {
	ingestedMetadata := 0
	failedMetadata := 0

	var firstMetadataErr error
	for _, metadata := range metadata {
		err := i.appendMetadata(userID, metadata)
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
func (i *Ingester) appendMetadata(userID string, m *mimirpb.MetricMetadata) error {
	userMetadata := i.getOrCreateUserMetadata(userID)

	return userMetadata.add(m.GetMetricFamilyName(), m)
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
		userMetadata = newMetadataMap(i.limiter, i.metrics, userID)
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

// MetricsMetadata returns all the metric metadata of a user.
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

	return &client.MetricsMetadataResponse{Metadata: userMetadata.toClientMetadata()}, nil
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

// allOutOfBounds returns whether all the provided samples are out of bounds.
func allOutOfBounds(samples []mimirpb.Sample, minValidTime int64) bool {
	for _, s := range samples {
		if s.TimestampMs >= minValidTime {
			return false
		}
	}
	return true
}
