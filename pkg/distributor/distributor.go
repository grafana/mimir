// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/distributor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/instrument"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/limiter"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/mtime"
	"github.com/grafana/dskit/ring"
	ring_client "github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/costattribution"
	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerclient"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/extract"
	"github.com/grafana/mimir/pkg/util/globalerror"
	mimir_limiter "github.com/grafana/mimir/pkg/util/limiter"
	util_math "github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/pool"
	"github.com/grafana/mimir/pkg/util/reactivelimiter"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

var tracer = otel.Tracer("pkg/distributor")

var (
	// Validation errors.
	errInvalidTenantShardSize = errors.New("invalid tenant shard size, the value must be greater than or equal to zero")

	reasonDistributorMaxIngestionRate             = globalerror.DistributorMaxIngestionRate.LabelValue()
	reasonDistributorMaxInflightPushRequests      = globalerror.DistributorMaxInflightPushRequests.LabelValue()
	reasonDistributorMaxInflightPushRequestsBytes = globalerror.DistributorMaxInflightPushRequestsBytes.LabelValue()
)

const (
	// distributorRingKey is the key under which we store the distributors ring in the KVStore.
	distributorRingKey = "distributor"

	// metaLabelTenantID is the name of the metric_relabel_configs label with tenant ID.
	metaLabelTenantID = model.MetaLabelPrefix + "tenant_id"

	maxOTLPRequestSizeFlag   = "distributor.max-otlp-request-size"
	maxInfluxRequestSizeFlag = "distributor.max-influx-request-size"

	instanceIngestionRateTickInterval = time.Second

	// Size of "slab" when using pooled buffers for marshaling write requests. When handling single Push request
	// buffers for multiple write requests sent to ingesters will be allocated from single "slab", if there is enough space.
	writeRequestSlabPoolSize = 512 * 1024

	// Multiplier used to estimate the decompressed size from compressed size of an incoming request. It prevents
	// decompressing requests when the distributor is near the inflight bytes limit and the uncompressed request
	// will likely exceed the limit.
	decompressionEstMultiplier = 8
)

type usageTrackerGenericClient interface {
	services.Service
	TrackSeries(ctx context.Context, userID string, series []uint64) ([]uint64, error)
	CanTrackAsync(userID string) bool
}

// Distributor forwards appends and queries to individual ingesters.
type Distributor struct {
	services.Service

	cfg           Config
	log           log.Logger
	ingestersRing ring.ReadRing
	ingesterPool  *ring_client.Pool
	limits        *validation.Overrides

	// The global rate limiter requires a distributors ring to count
	// the number of healthy instances
	distributorsLifecycler *ring.BasicLifecycler
	distributorsRing       *ring.Ring
	healthyInstancesCount  *atomic.Uint32

	costAttributionMgr *costattribution.Manager
	// For handling HA replicas.
	HATracker haTracker

	// Per-user rate limiters.
	requestRateLimiter   *limiter.RateLimiter
	ingestionRateLimiter *limiter.RateLimiter

	// Manager for subservices (HA Tracker, distributor ring and client pool)
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher

	activeUsers  *util.ActiveUsersCleanupService
	activeGroups *util.ActiveGroupsCleanupService

	ingestionRate             *util_math.EwmaRate
	inflightPushRequests      atomic.Int64
	inflightPushRequestsBytes atomic.Int64

	// Metrics
	queryDuration                    *instrument.HistogramCollector
	receivedRequests                 *prometheus.CounterVec
	receivedSamples                  *prometheus.CounterVec
	receivedExemplars                *prometheus.CounterVec
	receivedMetadata                 *prometheus.CounterVec
	receivedNativeHistogramSamples   *prometheus.CounterVec
	receivedNativeHistogramBuckets   *prometheus.CounterVec
	incomingRequests                 *prometheus.CounterVec
	incomingSamples                  *prometheus.CounterVec
	incomingExemplars                *prometheus.CounterVec
	incomingMetadata                 *prometheus.CounterVec
	nonHASamples                     *prometheus.CounterVec
	dedupedSamples                   *prometheus.CounterVec
	labelsHistogram                  prometheus.Histogram
	sampleDelay                      *prometheus.HistogramVec
	incomingSamplesPerRequest        *prometheus.HistogramVec
	incomingExemplarsPerRequest      *prometheus.HistogramVec
	latestSeenSampleTimestampPerUser *prometheus.GaugeVec
	hashCollisionCount               prometheus.Counter

	// Metric for silently dropped native histogram samples
	droppedNativeHistograms *prometheus.CounterVec

	// Metrics for async usage tracking.
	asyncUsageTrackerCalls                   *prometheus.CounterVec
	asyncUsageTrackerCallsWithRejectedSeries *prometheus.CounterVec

	// Metrics for data rejected for hitting per-tenant limits
	discardedSamplesTooManyHaClusters  *prometheus.CounterVec
	discardedSamplesPerUserSeriesLimit *prometheus.CounterVec
	discardedSamplesRateLimited        *prometheus.CounterVec
	discardedRequestsRateLimited       *prometheus.CounterVec
	discardedExemplarsRateLimited      *prometheus.CounterVec
	discardedMetadataRateLimited       *prometheus.CounterVec

	// Metrics for data rejected for hitting per-instance limits
	rejectedRequests *prometheus.CounterVec

	sampleValidationMetrics   *sampleValidationMetrics
	exemplarValidationMetrics *exemplarValidationMetrics
	metadataValidationMetrics *metadataValidationMetrics

	// Metrics to be passed to distributor push handlers
	PushMetrics *PushMetrics

	PushWithMiddlewares PushFunc

	RequestBufferPool util.Pool

	// Pool of []byte used when marshalling write requests.
	writeRequestBytePool sync.Pool

	// doBatchPushWorkers is the Go function passed to ring.DoBatchWithOptions.
	// It can be nil, in which case a simple `go f()` will be used.
	// See Config.ReusableIngesterPushWorkers on how to configure this.
	doBatchPushWorkers func(func())

	// ingestStorageWriter is the writer used when ingest storage is enabled.
	ingestStorageWriter *ingest.Writer

	// partitionsRing is the hash ring holding ingester partitions. It's used when ingest storage is enabled.
	partitionsRing *ring.PartitionInstanceRing

	// usageTrackerClient is the client that should be used to track per-tenant series and
	// enforce max series limit in the distributor. This field is nil if usage-tracker
	// is disabled.
	usageTrackerClient usageTrackerGenericClient

	reactiveLimiter reactivelimiter.ReactiveLimiter

	// For testing functionality that relies on timing without having to sleep in unit tests.
	sleep func(time.Duration)
	now   func() time.Time
}

func defaultSleep(d time.Duration) { time.Sleep(d) }
func defaultNow() time.Time        { return time.Now() }

// OTelResourceAttributePromotionConfig contains methods for configuring OTel resource attribute promotion.
type OTelResourceAttributePromotionConfig interface {
	// PromoteOTelResourceAttributes returns which OTel resource attributes to promote for tenant ID.
	PromoteOTelResourceAttributes(id string) []string
}

// KeepIdentifyingOTelResourceAttributesConfig contains methods for configuring keeping of identifying OTel resource attributes.
type KeepIdentifyingOTelResourceAttributesConfig interface {
	// OTelKeepIdentifyingResourceAttributes returns whether to keep identifying OTel resource attributes.
	OTelKeepIdentifyingResourceAttributes(tenantID string) bool
}

// Config contains the configuration required to
// create a Distributor
type Config struct {
	PoolConfig PoolConfig `yaml:"pool"`

	RetryConfig     RetryConfig     `yaml:"retry_after_header"`
	HATrackerConfig HATrackerConfig `yaml:"ha_tracker"`

	MaxRecvMsgSize           int           `yaml:"max_recv_msg_size" category:"advanced"`
	MaxOTLPRequestSize       int           `yaml:"max_otlp_request_size" category:"experimental"`
	MaxInfluxRequestSize     int           `yaml:"max_influx_request_size" category:"experimental" doc:"hidden"`
	MaxRequestPoolBufferSize int           `yaml:"max_request_pool_buffer_size" category:"experimental"`
	RemoteTimeout            time.Duration `yaml:"remote_timeout" category:"advanced"`

	// Distributors ring
	DistributorRing RingConfig `yaml:"ring"`

	// for testing and for extending the ingester by adding calls to the client
	IngesterClientFactory ring_client.PoolFactory `yaml:"-"`

	// When SkipLabelValidation is true the distributor does not validate the label name and value, Mimir doesn't directly use
	// this (and should never use it) but this feature is used by other projects built on top of it.
	SkipLabelValidation bool `yaml:"-"`

	// When SkipLabelCountValidation is true the distributor does not validate the number of labels, Mimir doesn't directly use
	// this (and should never use it) but this feature is used by other projects built on top of it.
	SkipLabelCountValidation bool `yaml:"-"`

	// This config is dynamically injected because it is defined in the querier config.
	ShuffleShardingEnabled                     bool          `yaml:"-"`
	IngestersLookbackPeriod                    time.Duration `yaml:"-"`
	StreamingChunksPerIngesterSeriesBufferSize uint64        `yaml:"-"`
	MinimizeIngesterRequests                   bool          `yaml:"-"`
	MinimiseIngesterRequestsHedgingDelay       time.Duration `yaml:"-"`
	PreferAvailabilityZones                    []string      `yaml:"-"`

	// IngestStorageConfig is dynamically injected because defined outside of distributor config.
	IngestStorageConfig ingest.Config `yaml:"-"`

	// Limits for distributor
	DefaultLimits    InstanceLimits         `yaml:"instance_limits"`
	InstanceLimitsFn func() *InstanceLimits `yaml:"-"`

	// This allows downstream projects to wrap the distributor push function
	// and access the deserialized write requests before/after they are pushed.
	// These functions will only receive samples that don't get dropped by HA deduplication.
	PushWrappers []PushWrapper `yaml:"-"`

	// OTLPPushMiddlewares are wrappers that are called when an OTLP push request is received.
	OTLPPushMiddlewares []OTLPPushMiddleware `yaml:"-"`

	WriteRequestsBufferPoolingEnabled bool `yaml:"write_requests_buffer_pooling_enabled" category:"experimental"`
	ReusableIngesterPushWorkers       int  `yaml:"reusable_ingester_push_workers" category:"advanced"`

	// OTelResourceAttributePromotionConfig allows for specializing OTel resource attribute promotion.
	OTelResourceAttributePromotionConfig OTelResourceAttributePromotionConfig `yaml:"-"`

	// KeepIdentifyingOTelResourceAttributesConfig allows for specializing keeping of identifying OTel resource attributes.
	KeepIdentifyingOTelResourceAttributesConfig KeepIdentifyingOTelResourceAttributesConfig `yaml:"-"`

	// Influx endpoint disabled by default
	EnableInfluxEndpoint bool `yaml:"influx_endpoint_enabled" category:"experimental" doc:"hidden"`

	// Change the implementation of OTel startTime from a real zero to a special NaN value.
	EnableStartTimeQuietZero bool `yaml:"start_time_quiet_zero" category:"advanced" doc:"hidden"`

	// Usage-tracker (optional).
	UsageTrackerEnabled bool                      `yaml:"-"` // Injected internally.
	UsageTrackerClient  usagetrackerclient.Config `yaml:"usage_tracker_client" doc:"hidden"`

	ReactiveLimiter reactivelimiter.Config `yaml:"reactive_limiter"`
}

// PushWrapper wraps around a push. It is similar to middleware.Interface.
type PushWrapper func(next PushFunc) PushFunc

// WithCleanup wraps the given pushWrapper function with automatic resource cleanup handling.
// It ensures Request.CleanUp() is called via defer if the given next PushFunc isn't invoked.
// See NextOrCleanup for the cleanup detection mechanism.
func WithCleanup(next PushFunc, pushWrapper func(next PushFunc, ctx context.Context, pushReq *Request) error) PushFunc {
	return func(ctx context.Context, pushReq *Request) error {
		next, maybeCleanup := NextOrCleanup(next, pushReq)
		defer maybeCleanup()
		return pushWrapper(next, ctx, pushReq)
	}
}

// NextOrCleanup returns a new PushFunc and a cleanup function that should be deferred by the caller.
// The cleanup function will only call Request.CleanUp() if next() wasn't called previously.
//
// This function is used outside of this codebase.
func NextOrCleanup(next PushFunc, pushReq *Request) (_ PushFunc, maybeCleanup func()) {
	cleanupInDefer := true
	return func(ctx context.Context, req *Request) error {
			cleanupInDefer = false
			return next(ctx, req)
		},
		func() {
			if cleanupInDefer {
				pushReq.CleanUp()
			}
		}
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	cfg.PoolConfig.RegisterFlags(f)
	cfg.HATrackerConfig.RegisterFlags(f)
	cfg.DistributorRing.RegisterFlags(f, logger)
	cfg.RetryConfig.RegisterFlags(f)
	cfg.UsageTrackerClient.RegisterFlagsWithPrefix("distributor.usage-tracker-client.", f)
	cfg.ReactiveLimiter.RegisterFlagsWithPrefixAndRejectionFactors("distributor.reactive-limiter.", f, 1.0, 2.0)

	f.IntVar(&cfg.MaxRecvMsgSize, "distributor.max-recv-msg-size", 100<<20, "Max message size in bytes that the distributors will accept for incoming push requests to the remote write API. If exceeded, the request will be rejected.")
	f.IntVar(&cfg.MaxOTLPRequestSize, maxOTLPRequestSizeFlag, 100<<20, "Maximum OTLP request size in bytes that the distributors accept. Requests exceeding this limit are rejected.")
	f.IntVar(&cfg.MaxInfluxRequestSize, maxInfluxRequestSizeFlag, 100<<20, "Maximum Influx request size in bytes that the distributors accept. Requests exceeding this limit are rejected.")
	f.IntVar(&cfg.MaxRequestPoolBufferSize, "distributor.max-request-pool-buffer-size", 0, "Max size of the pooled buffers used for marshaling write requests. If 0, no max size is enforced.")
	f.DurationVar(&cfg.RemoteTimeout, "distributor.remote-timeout", 2*time.Second, "Timeout for downstream ingesters.")
	f.BoolVar(&cfg.WriteRequestsBufferPoolingEnabled, "distributor.write-requests-buffer-pooling-enabled", true, "Enable pooling of buffers used for marshaling write requests.")
	f.BoolVar(&cfg.EnableInfluxEndpoint, "distributor.influx-endpoint-enabled", false, "Enable Influx endpoint.")
	f.IntVar(&cfg.ReusableIngesterPushWorkers, "distributor.reusable-ingester-push-workers", 2000, "Number of pre-allocated workers used to forward push requests to the ingesters. If 0, no workers will be used and a new goroutine will be spawned for each ingester push request. If not enough workers available, new goroutine will be spawned. (Note: this is a performance optimization, not a limiting feature.)")
	f.BoolVar(&cfg.EnableStartTimeQuietZero, "distributor.otel-start-time-quiet-zero", false, "Change the implementation of OTel startTime from a real zero to a special NaN value.")

	cfg.DefaultLimits.RegisterFlags(f)
}

// Validate config and returns error on failure
func (cfg *Config) Validate(limits validation.Limits) error {
	if limits.IngestionTenantShardSize < 0 {
		return errInvalidTenantShardSize
	}

	return cfg.RetryConfig.Validate()
}

const (
	instanceLimitsMetric     = "cortex_distributor_instance_limits"
	instanceLimitsMetricHelp = "Instance limits used by this distributor." // Must be same for all registrations.
	limitLabel               = "limit"
)

type PushMetrics struct {
	// Influx metrics.
	influxRequestCounter       *prometheus.CounterVec
	influxUncompressedBodySize *prometheus.HistogramVec
	// OTLP metrics.
	otlpRequestCounter     *prometheus.CounterVec
	uncompressedBodySize   *prometheus.HistogramVec
	otlpContentTypeCounter *prometheus.CounterVec
	// Temporary to better understand which array (ResourceMetrics/ScopeMetrics/Metrics) is usually large
	otlpArrayLengths *prometheus.HistogramVec
}

func newPushMetrics(reg prometheus.Registerer) *PushMetrics {
	return &PushMetrics{
		influxRequestCounter: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_influx_requests_total",
			Help: "The total number of Influx requests that have come in to the distributor.",
		}, []string{"user"}),
		influxUncompressedBodySize: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                            "cortex_distributor_influx_uncompressed_request_body_size_bytes",
			Help:                            "Size of uncompressed request body in bytes.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			NativeHistogramMaxBucketNumber:  100,
		}, []string{"user"}),
		otlpRequestCounter: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_otlp_requests_total",
			Help: "The total number of OTLP requests that have come in to the distributor.",
		}, []string{"user"}),
		uncompressedBodySize: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                            "cortex_distributor_uncompressed_request_body_size_bytes",
			Help:                            "Size of uncompressed request body in bytes.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			NativeHistogramMaxBucketNumber:  100,
		}, []string{"user", "handler"}),
		otlpContentTypeCounter: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_otlp_requests_by_content_type_total",
			Help: "Total number of requests with a given content type.",
		}, []string{"content_type"}),
		otlpArrayLengths: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                            "cortex_distributor_otlp_array_lengths",
			Help:                            "Number of elements in the arrays of OTLP requests.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			NativeHistogramMaxBucketNumber:  100,
		}, []string{"field"}),
	}
}

func (m *PushMetrics) IncInfluxRequest(user string) {
	if m != nil {
		m.influxRequestCounter.WithLabelValues(user).Inc()
	}
}

func (m *PushMetrics) ObserveInfluxUncompressedBodySize(user string, size float64) {
	if m != nil {
		m.influxUncompressedBodySize.WithLabelValues(user).Observe(size)
	}
}

func (m *PushMetrics) IncOTLPRequest(user string) {
	if m != nil {
		m.otlpRequestCounter.WithLabelValues(user).Inc()
	}
}

func (m *PushMetrics) ObserveUncompressedBodySize(user string, handler string, size float64) {
	if m != nil {
		m.uncompressedBodySize.WithLabelValues(user, handler).Observe(size)
	}
}

func (m *PushMetrics) IncOTLPContentType(contentType string) {
	if m != nil {
		m.otlpContentTypeCounter.WithLabelValues(contentType).Inc()
	}
}

func (m *PushMetrics) ObserveOTLPArrayLengths(field string, length int) {
	if m != nil {
		m.otlpArrayLengths.WithLabelValues(field).Observe(float64(length))
	}
}

func (m *PushMetrics) deleteUserMetrics(user string) {
	m.influxRequestCounter.DeleteLabelValues(user)
	m.influxUncompressedBodySize.DeleteLabelValues(user)
	m.otlpRequestCounter.DeleteLabelValues(user)
	m.uncompressedBodySize.DeleteLabelValues(user)
}

// New constructs a new Distributor
func New(cfg Config, clientConfig ingester_client.Config, limits *validation.Overrides, activeGroupsCleanupService *util.ActiveGroupsCleanupService, costAttributionMgr *costattribution.Manager, ingestersRing ring.ReadRing, partitionsRing *ring.PartitionInstanceRing, canJoinDistributorsRing bool, usageTrackerPartitionRing *ring.MultiPartitionInstanceRing, usageTrackerInstanceRing ring.ReadRing, reg prometheus.Registerer, log log.Logger) (*Distributor, error) {
	clientMetrics := ingester_client.NewMetrics(reg)
	if cfg.IngesterClientFactory == nil {
		cfg.IngesterClientFactory = ring_client.PoolInstFunc(func(inst ring.InstanceDesc) (ring_client.PoolClient, error) {
			return ingester_client.MakeIngesterClient(inst, clientConfig, clientMetrics, log)
		})
	}

	cfg.PoolConfig.RemoteTimeout = cfg.RemoteTimeout
	subservices := []services.Service(nil)
	requestBufferPool := util.NewBufferPool(cfg.MaxRequestPoolBufferSize)

	d := &Distributor{
		cfg:                   cfg,
		log:                   log,
		ingestersRing:         ingestersRing,
		RequestBufferPool:     requestBufferPool,
		partitionsRing:        partitionsRing,
		ingesterPool:          NewPool(cfg.PoolConfig, ingestersRing, cfg.IngesterClientFactory, log),
		healthyInstancesCount: atomic.NewUint32(0),
		limits:                limits,
		costAttributionMgr:    costAttributionMgr,
		ingestionRate:         util_math.NewEWMARate(0.2, instanceIngestionRateTickInterval),

		queryDuration: instrument.NewHistogramCollector(promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cortex_distributor_query_duration_seconds",
			Help:    "Time spent executing expression and exemplar queries.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30},
		}, []string{"method", "status_code"})),
		receivedRequests: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_received_requests_total",
			Help: "The total number of received requests, excluding rejected and deduped requests.",
		}, []string{"user"}),
		receivedSamples: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_received_samples_total",
			Help: "The total number of received samples, including native histogram samples, excluding rejected and deduped samples.",
		}, []string{"user"}),
		receivedExemplars: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_received_exemplars_total",
			Help: "The total number of received exemplars, excluding rejected and deduped exemplars.",
		}, []string{"user"}),
		receivedMetadata: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_received_metadata_total",
			Help: "The total number of received metadata, excluding rejected.",
		}, []string{"user"}),
		receivedNativeHistogramSamples: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_received_native_histogram_samples_total",
			Help: "The total number of received native histogram samples, excluding rejected and deduped samples.",
		}, []string{"user"}),
		receivedNativeHistogramBuckets: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_received_native_histogram_buckets_total",
			Help: "The total number of received native histogram buckets, excluding rejected and deduped samples.",
		}, []string{"user"}),
		incomingRequests: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_requests_in_total",
			Help: "The total number of requests that have come in to the distributor, including rejected or deduped requests.",
		}, []string{"user", "version"}),
		incomingSamples: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_samples_in_total",
			Help: "The total number of samples that have come in to the distributor, including rejected or deduped samples.",
		}, []string{"user"}),
		incomingExemplars: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_exemplars_in_total",
			Help: "The total number of exemplars that have come in to the distributor, including rejected or deduped exemplars.",
		}, []string{"user"}),
		incomingMetadata: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_metadata_in_total",
			Help: "The total number of metadata the have come in to the distributor, including rejected.",
		}, []string{"user"}),
		nonHASamples: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_non_ha_samples_received_total",
			Help: "The total number of received samples for a user that has HA tracking turned on, but the sample didn't contain both HA labels.",
		}, []string{"user"}),
		dedupedSamples: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_deduped_samples_total",
			Help: "The total number of deduplicated samples.",
		}, []string{"user", "cluster"}),
		labelsHistogram: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_labels_per_sample",
			Help:    "Number of labels per sample.",
			Buckets: []float64{5, 10, 15, 20, 25},
		}),
		sampleDelay: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                            "cortex_distributor_sample_delay_seconds",
			Help:                            "Number of seconds by which a sample came in late wrt wallclock.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			NativeHistogramMaxBucketNumber:  100,
		}, []string{"user"}),
		incomingSamplesPerRequest: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                            "cortex_distributor_samples_per_request",
			Help:                            "Number of samples per request before deduplication and validation.",
			NativeHistogramBucketFactor:     2,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			NativeHistogramMaxBucketNumber:  100,
		}, []string{"user"}),
		incomingExemplarsPerRequest: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                            "cortex_distributor_exemplars_per_request",
			Help:                            "Number of exemplars per request before deduplication and validation.",
			NativeHistogramBucketFactor:     2,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			NativeHistogramMaxBucketNumber:  100,
		}, []string{"user"}),
		latestSeenSampleTimestampPerUser: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_distributor_latest_seen_sample_timestamp_seconds",
			Help: "Unix timestamp of latest received sample per user.",
		}, []string{"user"}),

		droppedNativeHistograms: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_dropped_native_histograms_total",
			Help: "The total number of native histograms that were silently dropped because native histograms ingestion is disabled.",
		}, []string{"user"}),

		asyncUsageTrackerCalls: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_async_usage_tracker_calls_total",
			Help: "The total number of asynchronous usage-tracker calls performed per user.",
		}, []string{"user"}),
		asyncUsageTrackerCallsWithRejectedSeries: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_async_usage_tracker_calls_with_rejected_series_total",
			Help: "The total number of asynchronous usage-tracker calls that rejected series per user.",
		}, []string{"user"}),

		discardedSamplesTooManyHaClusters:  validation.DiscardedSamplesCounter(reg, reasonTooManyHAClusters),
		discardedSamplesPerUserSeriesLimit: validation.DiscardedSamplesCounter(reg, reasonPerUserActiveSeriesLimit),
		discardedSamplesRateLimited:        validation.DiscardedSamplesCounter(reg, reasonRateLimited),
		discardedRequestsRateLimited:       validation.DiscardedRequestsCounter(reg, reasonRateLimited),
		discardedExemplarsRateLimited:      validation.DiscardedExemplarsCounter(reg, reasonRateLimited),
		discardedMetadataRateLimited:       validation.DiscardedMetadataCounter(reg, reasonRateLimited),

		rejectedRequests: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_instance_rejected_requests_total",
			Help: "Requests discarded for hitting per-instance limits",
		}, []string{"reason"}),

		sampleValidationMetrics:   newSampleValidationMetrics(reg),
		exemplarValidationMetrics: newExemplarValidationMetrics(reg),
		metadataValidationMetrics: newMetadataValidationMetrics(reg),

		hashCollisionCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_distributor_hash_collisions_total",
			Help: "Number of times a hash collision was detected when de-duplicating samples.",
		}),

		PushMetrics: newPushMetrics(reg),
		now:         defaultNow,
		sleep:       defaultSleep,
	}

	// Initialize expected rejected request labels
	d.rejectedRequests.WithLabelValues(reasonDistributorMaxIngestionRate)
	d.rejectedRequests.WithLabelValues(reasonDistributorMaxInflightPushRequests)
	d.rejectedRequests.WithLabelValues(reasonDistributorMaxInflightPushRequestsBytes)

	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name:        instanceLimitsMetric,
		Help:        instanceLimitsMetricHelp,
		ConstLabels: map[string]string{limitLabel: "max_inflight_push_requests"},
	}, func() float64 {
		il := d.getInstanceLimits()
		return float64(il.MaxInflightPushRequests)
	})
	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name:        instanceLimitsMetric,
		Help:        instanceLimitsMetricHelp,
		ConstLabels: map[string]string{limitLabel: "max_inflight_push_requests_bytes"},
	}, func() float64 {
		il := d.getInstanceLimits()
		return float64(il.MaxInflightPushRequestsBytes)
	})
	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name:        instanceLimitsMetric,
		Help:        instanceLimitsMetricHelp,
		ConstLabels: map[string]string{limitLabel: "max_ingestion_rate"},
	}, func() float64 {
		il := d.getInstanceLimits()
		return il.MaxIngestionRate
	})

	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_distributor_inflight_push_requests",
		Help: "Current number of inflight push requests in distributor.",
	}, func() float64 {
		return float64(d.inflightPushRequests.Load())
	})
	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_distributor_inflight_push_requests_bytes",
		Help: "Current sum of inflight push requests in distributor in bytes.",
	}, func() float64 {
		return float64(d.inflightPushRequestsBytes.Load())
	})
	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_distributor_ingestion_rate_samples_per_second",
		Help: "Current ingestion rate in samples/sec that distributor is using to limit access.",
	}, func() float64 {
		return d.ingestionRate.Rate()
	})

	// Create the configured ingestion rate limit strategy (local or global). In case
	// it's an internal dependency and we can't join the distributors ring, we skip rate
	// limiting.
	var ingestionRateStrategy, requestRateStrategy limiter.RateLimiterStrategy
	var distributorsLifecycler *ring.BasicLifecycler
	var distributorsRing *ring.Ring
	var err error

	if !canJoinDistributorsRing {
		requestRateStrategy = newInfiniteRateStrategy()
		ingestionRateStrategy = newInfiniteRateStrategy()
	} else {
		distributorsRing, distributorsLifecycler, err = newRingAndLifecycler(cfg.DistributorRing, d.healthyInstancesCount, log, reg)
		if err != nil {
			return nil, err
		}

		subservices = append(subservices, distributorsLifecycler, distributorsRing)
		requestRateStrategy = newGlobalRateStrategy(newRequestRateStrategy(limits), d)
		ingestionRateStrategy = newGlobalRateStrategyWithBurstFactor(limits, d)
	}

	// If this isn't a real distributor that will be accepting writes or if the HA tracker is
	// disabled, use a no-op implementation. We don't want to require it to be configured when
	// it's not used due to being disabled or this being a component that doesn't use it (rulers,
	// queriers).
	var haTrackerImpl haTracker

	if !canJoinDistributorsRing || !cfg.HATrackerConfig.EnableHATracker {
		haTrackerImpl = newNopHaTracker()
	} else {
		haTrackerImpl, err = newHaTracker(cfg.HATrackerConfig, limits, reg, log)
		if err != nil {
			return nil, err
		}

		subservices = append(subservices, haTrackerImpl)
	}

	d.requestRateLimiter = limiter.NewRateLimiter(requestRateStrategy, 10*time.Second)
	d.ingestionRateLimiter = limiter.NewRateLimiter(ingestionRateStrategy, 10*time.Second)
	d.reactiveLimiter = newDistributorReactiveLimiter(d.cfg.ReactiveLimiter, log, reg)
	d.distributorsLifecycler = distributorsLifecycler
	d.distributorsRing = distributorsRing
	d.HATracker = haTrackerImpl

	d.activeUsers = util.NewActiveUsersCleanupWithDefaultValues(d.cleanupInactiveUser)
	d.activeGroups = activeGroupsCleanupService

	d.PushWithMiddlewares = d.wrapPushWithMiddlewares(d.push)

	subservices = append(subservices, d.ingesterPool, d.activeUsers)

	if cfg.ReusableIngesterPushWorkers > 0 {
		wp := concurrency.NewReusableGoroutinesPool(cfg.ReusableIngesterPushWorkers)
		d.doBatchPushWorkers = wp.Go
		// Closing the pool doesn't stop the workload it's running, we're doing this just to avoid leaking goroutines in tests.
		subservices = append(subservices, services.NewBasicService(
			nil,
			func(ctx context.Context) error { <-ctx.Done(); return nil },
			func(_ error) error { wp.Close(); return nil },
		))
	}

	if cfg.IngestStorageConfig.Enabled {
		d.ingestStorageWriter = ingest.NewWriter(d.cfg.IngestStorageConfig.KafkaConfig, log, reg)
		subservices = append(subservices, d.ingestStorageWriter)
	}

	// Init usage-tracker client (if enabled).
	if cfg.UsageTrackerEnabled {
		if usageTrackerPartitionRing == nil {
			return nil, errors.New("usage-tracker partition ring is required")
		}
		if usageTrackerInstanceRing == nil {
			return nil, errors.New("usage-tracker instance ring is required")
		}

		d.usageTrackerClient = usagetrackerclient.NewUsageTrackerClient("distributor", d.cfg.UsageTrackerClient, usageTrackerPartitionRing, usageTrackerInstanceRing, d.limits, log, reg)
		subservices = append(subservices, d.usageTrackerClient)
	}

	// Register each metric only if the corresponding storage is enabled.
	// Some queries in the mixin use the presence of these metrics as indication whether Mimir is running with ingest storage or not.
	exportStorageModeMetrics(reg, cfg.IngestStorageConfig.Migration.DistributorSendToIngestersEnabled || !cfg.IngestStorageConfig.Enabled, cfg.IngestStorageConfig.Enabled, ingestersRing.ReplicationFactor())

	d.subservices, err = services.NewManager(subservices...)
	if err != nil {
		return nil, err
	}
	d.subservicesWatcher = services.NewFailureWatcher()
	d.subservicesWatcher.WatchManager(d.subservices)

	d.Service = services.NewBasicService(d.starting, d.running, d.stopping)
	return d, nil
}

func exportStorageModeMetrics(reg prometheus.Registerer, classicStorageEnabled, ingestStorageEnabled bool, classicStorageReplicationFactor int) {
	ingestStorageEnabledVal := float64(0)
	if ingestStorageEnabled {
		ingestStorageEnabledVal = 1
	}
	promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_distributor_ingest_storage_enabled",
		Help: "Whether writes are being processed via ingest storage. Equal to 1 if ingest storage is enabled, 0 if disabled.",
	}).Set(ingestStorageEnabledVal)

	if classicStorageEnabled {
		promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_distributor_replication_factor",
			Help: "The configured replication factor.",
		}).Set(float64(classicStorageReplicationFactor))
	}
}

// newRingAndLifecycler creates a new distributor ring and lifecycler with all required lifecycler delegates
func newRingAndLifecycler(cfg RingConfig, instanceCount *atomic.Uint32, logger log.Logger, reg prometheus.Registerer) (*ring.Ring, *ring.BasicLifecycler, error) {
	reg = prometheus.WrapRegistererWithPrefix("cortex_", reg)
	kvStore, err := kv.NewClient(cfg.Common.KVStore, ring.GetCodec(), kv.RegistererWithKVName(reg, "distributor-lifecycler"), logger)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialize distributors' KV store")
	}

	lifecyclerCfg, err := cfg.ToBasicLifecyclerConfig(logger)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to build distributors' lifecycler config")
	}

	var delegate ring.BasicLifecyclerDelegate
	delegate = ring.NewInstanceRegisterDelegate(ring.ACTIVE, lifecyclerCfg.NumTokens)
	delegate = newHealthyInstanceDelegate(instanceCount, cfg.Common.HeartbeatTimeout, delegate)
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, logger)
	if cfg.AutoForgetUnhealthyPeriods > 0 {
		delegate = ring.NewAutoForgetDelegate(time.Duration(cfg.AutoForgetUnhealthyPeriods)*cfg.Common.HeartbeatTimeout, delegate, logger)
	}

	distributorsLifecycler, err := ring.NewBasicLifecycler(lifecyclerCfg, "distributor", distributorRingKey, kvStore, delegate, logger, reg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialize distributors' lifecycler")
	}

	distributorsRing, err := ring.New(cfg.toRingConfig(), "distributor", distributorRingKey, logger, reg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialize distributors' ring client")
	}

	return distributorsRing, distributorsLifecycler, nil
}

func (d *Distributor) starting(ctx context.Context) error {
	if err := services.StartManagerAndAwaitHealthy(ctx, d.subservices); err != nil {
		return errors.Wrap(err, "unable to start distributor subservices")
	}

	// Distributors get embedded in rulers and queriers to talk to ingesters on the query path. In that
	// case they won't have a distributor lifecycler or ring so don't try to join the distributor ring.
	if d.distributorsLifecycler != nil && d.distributorsRing != nil {
		level.Info(d.log).Log("msg", "waiting until distributor is ACTIVE in the ring")
		if err := ring.WaitInstanceState(ctx, d.distributorsRing, d.distributorsLifecycler.GetInstanceID(), ring.ACTIVE); err != nil {
			return err
		}
	}

	return nil
}

func (d *Distributor) running(ctx context.Context) error {
	ingestionRateTicker := time.NewTicker(instanceIngestionRateTickInterval)
	defer ingestionRateTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-ingestionRateTicker.C:
			d.ingestionRate.Tick()

		case err := <-d.subservicesWatcher.Chan():
			return errors.Wrap(err, "distributor subservice failed")
		}
	}
}

func (d *Distributor) cleanupInactiveUser(userID string) {
	d.ingestersRing.CleanupShuffleShardCache(userID)

	d.HATracker.cleanupHATrackerMetricsForUser(userID)

	d.receivedRequests.DeleteLabelValues(userID)
	d.receivedSamples.DeleteLabelValues(userID)
	d.receivedExemplars.DeleteLabelValues(userID)
	d.receivedMetadata.DeleteLabelValues(userID)
	d.receivedNativeHistogramSamples.DeleteLabelValues(userID)
	d.receivedNativeHistogramBuckets.DeleteLabelValues(userID)
	d.incomingSamples.DeleteLabelValues(userID)
	d.incomingExemplars.DeleteLabelValues(userID)
	d.incomingMetadata.DeleteLabelValues(userID)
	d.incomingSamplesPerRequest.DeleteLabelValues(userID)
	d.sampleDelay.DeleteLabelValues(userID)
	d.incomingExemplarsPerRequest.DeleteLabelValues(userID)
	d.nonHASamples.DeleteLabelValues(userID)
	d.latestSeenSampleTimestampPerUser.DeleteLabelValues(userID)

	d.PushMetrics.deleteUserMetrics(userID)

	d.droppedNativeHistograms.DeleteLabelValues(userID)
	d.asyncUsageTrackerCalls.DeleteLabelValues(userID)
	d.asyncUsageTrackerCallsWithRejectedSeries.DeleteLabelValues(userID)

	filter := prometheus.Labels{"user": userID}
	d.incomingRequests.DeletePartialMatch(filter)
	d.dedupedSamples.DeletePartialMatch(filter)
	d.discardedSamplesTooManyHaClusters.DeletePartialMatch(filter)
	d.discardedSamplesPerUserSeriesLimit.DeletePartialMatch(filter)
	d.discardedSamplesRateLimited.DeletePartialMatch(filter)
	d.discardedRequestsRateLimited.DeleteLabelValues(userID)
	d.discardedExemplarsRateLimited.DeleteLabelValues(userID)
	d.discardedMetadataRateLimited.DeleteLabelValues(userID)

	d.sampleValidationMetrics.deleteUserMetrics(userID)
	d.exemplarValidationMetrics.deleteUserMetrics(userID)
	d.metadataValidationMetrics.deleteUserMetrics(userID)
}

func (d *Distributor) RemoveGroupMetricsForUser(userID, group string) {
	d.dedupedSamples.DeleteLabelValues(userID, group)
	d.discardedSamplesPerUserSeriesLimit.DeleteLabelValues(userID, group)
	d.discardedSamplesTooManyHaClusters.DeleteLabelValues(userID, group)
	d.discardedSamplesRateLimited.DeleteLabelValues(userID, group)
	d.sampleValidationMetrics.deleteUserMetricsForGroup(userID, group)
}

// Called after distributor is asked to stop via StopAsync.
func (d *Distributor) stopping(_ error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), d.subservices)
}

// Returns a boolean that indicates whether or not we want to remove the replica label going forward,
// and an error that indicates whether we want to accept samples based on the cluster/replica found in ts.
// nil for the error means accept the sample.
func (d *Distributor) checkSample(ctx context.Context, userID, cluster, replica string, ts int64) (removeReplicaLabel bool, _ error) {
	// If the sample doesn't have either HA label, accept it.
	// At the moment we want to accept these samples by default.
	if cluster == "" || replica == "" {
		return false, nil
	}

	// If replica label is too long, don't use it. We accept the sample here, but it will fail validation later anyway.
	if len(replica) > d.limits.MaxLabelValueLength(userID) {
		return false, nil
	}

	// At this point we know we have both HA labels, we should lookup
	// the cluster/instance here to see if we want to accept this sample.
	// Convert the timestamp to a time.Time for checking the replica
	sampleTime := timestamp.Time(ts)
	err := d.HATracker.checkReplica(ctx, userID, cluster, replica, time.Now(), sampleTime)
	// checkReplica would have returned an error if there was a real error talking to Consul,
	// or if the replica is not the currently elected replica.
	if err != nil { // Don't accept the sample.
		return false, err
	}
	return true, nil
}

// validateSamples validates samples of a single timeseries and removes the ones with duplicated timestamps.
// Returns an error explaining the first validation finding.
// May alter timeseries data in-place.
// The returned error MUST NOT retain label strings - they point into a gRPC buffer which is re-used.
func (d *Distributor) validateSamples(now model.Time, ts *mimirpb.PreallocTimeseries, userID, group string, cfg sampleValidationConfig) error {
	if len(ts.Samples) == 0 {
		return nil
	}

	cat := d.costAttributionMgr.SampleTracker(userID)
	if len(ts.Samples) == 1 {
		delta := now - model.Time(ts.Samples[0].TimestampMs)
		d.sampleDelay.WithLabelValues(userID).Observe(float64(delta) / 1000)
		return validateSample(d.sampleValidationMetrics, now, cfg, userID, group, ts.Labels, ts.Samples[0], cat)
	}

	timestamps := make(map[int64]struct{}, min(len(ts.Samples), 100))
	currPos := 0
	duplicatesFound := false
	for _, s := range ts.Samples {
		if _, ok := timestamps[s.TimestampMs]; ok {
			// A sample with the same timestamp has already been validated, so we skip it.
			duplicatesFound = true
			continue
		}

		timestamps[s.TimestampMs] = struct{}{}

		delta := now - model.Time(s.TimestampMs)
		d.sampleDelay.WithLabelValues(userID).Observe(float64(delta) / 1000)

		if err := validateSample(d.sampleValidationMetrics, now, cfg, userID, group, ts.Labels, s, cat); err != nil {
			return err
		}

		ts.Samples[currPos] = s
		currPos++
	}

	if duplicatesFound {
		ts.Samples = ts.Samples[:currPos]
		ts.SamplesUpdated()
	}

	return nil
}

// validateHistograms validates histograms of a single timeseries and removes the ones with duplicated timestamps.
// Returns an error explaining the first validation finding.
// May alter timeseries data in-place.
// The returned error MUST NOT retain label strings - they point into a gRPC buffer which is re-used.
func (d *Distributor) validateHistograms(now model.Time, ts *mimirpb.PreallocTimeseries, userID, group string, cfg sampleValidationConfig) error {
	if len(ts.Histograms) == 0 {
		return nil
	}

	cat := d.costAttributionMgr.SampleTracker(userID)
	if len(ts.Histograms) == 1 {
		delta := now - model.Time(ts.Histograms[0].Timestamp)
		d.sampleDelay.WithLabelValues(userID).Observe(float64(delta) / 1000)

		updated, err := validateSampleHistogram(d.sampleValidationMetrics, now, cfg, userID, group, ts.Labels, &ts.Histograms[0], cat)
		if err != nil {
			return err
		}
		if updated {
			ts.HistogramsUpdated()
		}
		return nil
	}

	timestamps := make(map[int64]struct{}, min(len(ts.Histograms), 100))
	currPos := 0
	histogramsUpdated := false
	for idx := range ts.Histograms {
		if _, ok := timestamps[ts.Histograms[idx].Timestamp]; ok {
			// A sample with the same timestamp has already been validated, so we skip it.
			histogramsUpdated = true
			continue
		}

		timestamps[ts.Histograms[idx].Timestamp] = struct{}{}

		delta := now - model.Time(ts.Histograms[idx].Timestamp)
		d.sampleDelay.WithLabelValues(userID).Observe(float64(delta) / 1000)

		updated, err := validateSampleHistogram(d.sampleValidationMetrics, now, cfg, userID, group, ts.Labels, &ts.Histograms[idx], cat)
		if err != nil {
			return err
		}
		histogramsUpdated = histogramsUpdated || updated

		ts.Histograms[currPos] = ts.Histograms[idx]
		currPos++
	}

	if histogramsUpdated {
		ts.Histograms = ts.Histograms[:currPos]
		ts.HistogramsUpdated()
	}

	return nil
}

// validateExemplars validates exemplars of a single timeseries.
// May alter timeseries data in-place.
func (d *Distributor) validateExemplars(ts *mimirpb.PreallocTimeseries, userID string, minExemplarTS, maxExemplarTS int64) {
	if d.limits.MaxGlobalExemplarsPerUser(userID) == 0 {
		ts.ClearExemplars()
		return
	}
	allowedExemplars := d.limits.MaxExemplarsPerSeriesPerRequest(userID)
	if allowedExemplars > 0 && len(ts.Exemplars) > allowedExemplars {
		d.exemplarValidationMetrics.tooManyExemplars.WithLabelValues(userID).Add(float64(len(ts.Exemplars) - allowedExemplars))
		ts.ResizeExemplars(allowedExemplars)
	}

	var previousExemplarTS int64 = math.MinInt64
	isInOrder := true
	for i := 0; i < len(ts.Exemplars); {
		e := ts.Exemplars[i]
		if err := validateExemplar(d.exemplarValidationMetrics, userID, ts.Labels, e); err != nil {
			// OTel sends empty exemplars by default which aren't useful and are discarded by TSDB, so let's just skip invalid ones and ingest the data we can instead of returning an error.
			ts.DeleteExemplarByMovingLast(i)
			// Don't increase index i. After moving the last exemplar to this index, we want to check it again.
			continue
		}
		if !validateExemplarTimestamp(d.exemplarValidationMetrics, userID, minExemplarTS, maxExemplarTS, e) {
			ts.DeleteExemplarByMovingLast(i)
			// Don't increase index i. After moving the last exemplar to this index, we want to check it again.
			continue
		}
		// We want to check if exemplars are in order. If they are not, we will sort them and invalidate the cache.
		if isInOrder && previousExemplarTS > e.TimestampMs {
			isInOrder = false
		}
		previousExemplarTS = e.TimestampMs
		i++
	}
	if !isInOrder {
		ts.SortExemplars()
	}
}

// Validates a single series from a write request.
// May alter timeseries data in-place.
// Returns an error explaining the first validation finding. Non-nil error means the timeseries should be removed from the request.
// The returned error MUST NOT retain label strings - they point into a gRPC buffer which is re-used.
// It uses the passed nowt time to observe the delay of sample timestamps.
func (d *Distributor) validateSeries(nowt time.Time, ts *mimirpb.PreallocTimeseries, userID, group string, cfg validationConfig, skipLabelValidation, skipLabelCountValidation bool, minExemplarTS, maxExemplarTS int64, valueTooLongSummaries *labelValueTooLongSummaries) error {
	cat := d.costAttributionMgr.SampleTracker(userID)

	if err := validateLabels(d.sampleValidationMetrics, cfg.labels, userID, group, ts.Labels, skipLabelValidation, skipLabelCountValidation, cat, nowt, valueTooLongSummaries); err != nil {
		return err
	}

	now := model.TimeFromUnixNano(nowt.UnixNano())
	totalSamplesAndHistograms := len(ts.Samples) + len(ts.Histograms)

	if err := d.validateSamples(now, ts, userID, group, cfg.samples); err != nil {
		return err
	}

	if err := d.validateHistograms(now, ts, userID, group, cfg.samples); err != nil {
		return err
	}

	d.validateExemplars(ts, userID, minExemplarTS, maxExemplarTS)

	deduplicatedSamplesAndHistograms := totalSamplesAndHistograms - len(ts.Samples) - len(ts.Histograms)
	if deduplicatedSamplesAndHistograms > 0 {
		d.sampleValidationMetrics.duplicateTimestamp.WithLabelValues(userID, group).Add(float64(deduplicatedSamplesAndHistograms))
	}

	return nil
}

// wrapPushWithMiddlewares returns push function wrapped in all Distributor's middlewares.
// push wrappers will be applied to incoming requests in the order in which they are in the slice in the config struct.
func (d *Distributor) wrapPushWithMiddlewares(next PushFunc) PushFunc {
	var middlewares []PushWrapper

	// The middlewares will be applied to the request (!) in the specified order, from first to last.
	// To guarantee that, middleware functions will be called in reversed order, wrapping the
	// result from previous call.
	middlewares = append(middlewares, d.limitsMiddleware) // Should run first because it checks limits before other middlewares need to read the request body.
	middlewares = append(middlewares, d.metricsMiddleware)
	middlewares = append(middlewares, d.prePushHaDedupeMiddleware)
	middlewares = append(middlewares, d.prePushRelabelMiddleware)
	middlewares = append(middlewares, d.prePushSortAndFilterMiddleware)
	middlewares = append(middlewares, d.prePushValidationMiddleware)
	middlewares = append(middlewares, d.cfg.PushWrappers...)             // TODO GEM has a BI middleware. It should probably be applied after prePushMaxSeriesLimitMiddleware
	middlewares = append(middlewares, d.prePushMaxSeriesLimitMiddleware) // Should be the very last, to enforce the max series limit on top of all filtering, relabelling and other changes (e.g. GEM aggregations) previous middlewares could do

	for ix := len(middlewares) - 1; ix >= 0; ix-- {
		next = middlewares[ix](next)
	}

	// The delay middleware must take into account total runtime of all other middlewares and the push func, hence why we wrap all others.
	return d.outerMaybeDelayMiddleware(next)

}

func (d *Distributor) prePushHaDedupeMiddleware(next PushFunc) PushFunc {
	return WithCleanup(next, func(next PushFunc, ctx context.Context, pushReq *Request) error {
		req, err := pushReq.WriteRequest()
		if err != nil {
			return err
		}

		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return err
		}

		if len(req.Timeseries) == 0 || !d.limits.AcceptHASamples(userID) {
			return next(ctx, pushReq)
		}

		haReplicaLabel := d.limits.HAReplicaLabel(userID)
		cluster, replica := findHALabels(haReplicaLabel, d.limits.HAClusterLabel(userID), req.Timeseries[0].Labels)
		// Make a copy of these, since they may be retained as labels on our metrics, e.g. dedupedSamples.
		cluster, replica = strings.Clone(cluster), strings.Clone(replica)

		span := trace.SpanFromContext(ctx)
		span.SetAttributes(
			attribute.String("cluster", cluster),
			attribute.String("replica", replica),
		)

		numSamples := 0
		now := time.Now()
		group := d.activeGroups.UpdateActiveGroupTimestamp(userID, validation.GroupLabel(d.limits, userID, req.Timeseries), now)
		sampleTimestamp := timestamp.FromTime(now)
		if d.limits.HATrackerUseSampleTimeForFailover(userID) {
			earliestSampleTimestamp := sampleTimestamp
			for _, ts := range req.Timeseries {
				if len(ts.Samples) > 0 {
					tsms := ts.Samples[0].TimestampMs
					if tsms < earliestSampleTimestamp {
						earliestSampleTimestamp = tsms
					}
				}
				if len(ts.Histograms) > 0 {
					tsms := ts.Histograms[0].Timestamp
					if tsms < earliestSampleTimestamp {
						earliestSampleTimestamp = tsms
					}
				}
			}
			sampleTimestamp = earliestSampleTimestamp
		}
		for _, ts := range req.Timeseries {
			numSamples += len(ts.Samples) + len(ts.Histograms)
		}

		removeReplica, err := d.checkSample(ctx, userID, cluster, replica, sampleTimestamp)
		if err != nil {
			if errors.As(err, &replicasDidNotMatchError{}) {
				// These samples have been deduped.
				d.dedupedSamples.WithLabelValues(userID, cluster).Add(float64(numSamples))
			}

			if errors.As(err, &tooManyClustersError{}) {
				d.discardedSamplesTooManyHaClusters.WithLabelValues(userID, group).Add(float64(numSamples))
				d.costAttributionMgr.SampleTracker(userID).IncrementDiscardedSamples(req.Timeseries[0].Labels, float64(numSamples), reasonTooManyHAClusters, now)
			}

			return err
		}

		if removeReplica {
			// If we found both the cluster and replica labels, we only want to include the cluster label when
			// storing series in Mimir. If we kept the replica label we would end up with another series for the same
			// series we're trying to dedupe when HA tracking moves over to a different replica.
			for ix := range req.Timeseries {
				req.Timeseries[ix].RemoveLabel(haReplicaLabel)
			}
		} else {
			// If there wasn't an error but removeReplica is false that means we didn't find both HA labels.
			d.nonHASamples.WithLabelValues(userID).Add(float64(numSamples))
		}

		return next(ctx, pushReq)
	})
}

func (d *Distributor) prePushRelabelMiddleware(next PushFunc) PushFunc {
	return WithCleanup(next, func(next PushFunc, ctx context.Context, pushReq *Request) error {
		req, err := pushReq.WriteRequest()
		if err != nil {
			return err
		}

		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return err
		}

		dropLabels := d.limits.DropLabels(userID)
		relabelConfigs := d.limits.MetricRelabelConfigs(userID)

		var removeTsIndexes []int
		lb := labels.NewBuilder(labels.EmptyLabels())
		for tsIdx := 0; tsIdx < len(req.Timeseries); tsIdx++ {
			ts := req.Timeseries[tsIdx]

			if len(relabelConfigs) > 0 {
				mimirpb.FromLabelAdaptersToBuilder(ts.Labels, lb)
				lb.Set(metaLabelTenantID, userID)
				keep := relabel.ProcessBuilder(lb, relabelConfigs...)
				if !keep {
					removeTsIndexes = append(removeTsIndexes, tsIdx)
					continue
				}
				lb.Del(metaLabelTenantID)
				req.Timeseries[tsIdx].SetLabels(mimirpb.FromBuilderToLabelAdapters(lb, ts.Labels))
			}

			for _, labelName := range dropLabels {
				req.Timeseries[tsIdx].RemoveLabel(labelName)
			}

			if len(ts.Labels) == 0 {
				removeTsIndexes = append(removeTsIndexes, tsIdx)
				continue
			}
		}

		if len(removeTsIndexes) > 0 {
			for _, removeTsIndex := range removeTsIndexes {
				mimirpb.ReusePreallocTimeseries(&req.Timeseries[removeTsIndex])
			}
			req.Timeseries = util.RemoveSliceIndexes(req.Timeseries, removeTsIndexes)
		}

		return next(ctx, pushReq)
	})
}

// prePushSortAndFilterMiddleware is responsible for sorting labels and
// filtering empty values. This is a protection mechanism for ingesters.
func (d *Distributor) prePushSortAndFilterMiddleware(next PushFunc) PushFunc {
	return WithCleanup(next, func(next PushFunc, ctx context.Context, pushReq *Request) error {
		req, err := pushReq.WriteRequest()
		if err != nil {
			return err
		}

		var removeTsIndexes []int
		for tsIdx := 0; tsIdx < len(req.Timeseries); tsIdx++ {
			ts := req.Timeseries[tsIdx]

			// Prometheus strips empty values before storing; drop them now, before sharding to ingesters.
			req.Timeseries[tsIdx].RemoveEmptyLabelValues()

			if len(ts.Labels) == 0 {
				removeTsIndexes = append(removeTsIndexes, tsIdx)
				continue
			}

			// We rely on sorted labels in different places:
			// 1) When computing token for labels. Here different order of
			// labels returns different tokens, which is bad.
			// 2) In validation code, when checking for duplicate label names.
			// As duplicate label names are rejected later in the validation
			// phase, we ignore them here.
			// 3) Ingesters expect labels to be sorted in the Push request.
			req.Timeseries[tsIdx].SortLabelsIfNeeded()
		}

		if len(removeTsIndexes) > 0 {
			for _, removeTsIndex := range removeTsIndexes {
				mimirpb.ReusePreallocTimeseries(&req.Timeseries[removeTsIndex])
			}
			req.Timeseries = util.RemoveSliceIndexes(req.Timeseries, removeTsIndexes)
		}

		return next(ctx, pushReq)
	})
}

func (d *Distributor) prePushValidationMiddleware(next PushFunc) PushFunc {
	return WithCleanup(next, func(next PushFunc, ctx context.Context, pushReq *Request) error {
		req, err := pushReq.WriteRequest()
		if err != nil {
			return err
		}

		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return err
		}

		now := mtime.Now()
		d.receivedRequests.WithLabelValues(userID).Add(1)
		d.activeUsers.UpdateUserTimestamp(userID, now)

		pushReq.group = d.activeGroups.UpdateActiveGroupTimestamp(userID, validation.GroupLabel(d.limits, userID, req.Timeseries), now)
		cfg := newValidationConfig(userID, d.limits)

		// A WriteRequest can only contain series or metadata but not both. This might change in the future.
		validatedMetadata := 0
		validatedSamples := 0
		validatedExemplars := 0

		// Find the earliest and latest samples in the batch.
		earliestSampleTimestampMs, latestSampleTimestampMs := int64(math.MaxInt64), int64(0)
		for _, ts := range req.Timeseries {
			for _, s := range ts.Samples {
				earliestSampleTimestampMs = min(earliestSampleTimestampMs, s.TimestampMs)
				latestSampleTimestampMs = max(latestSampleTimestampMs, s.TimestampMs)
			}
			for _, h := range ts.Histograms {
				earliestSampleTimestampMs = min(earliestSampleTimestampMs, h.Timestamp)
				latestSampleTimestampMs = max(latestSampleTimestampMs, h.Timestamp)
			}
		}
		// Update this metric even in case of errors.
		if latestSampleTimestampMs > 0 {
			d.latestSeenSampleTimestampPerUser.WithLabelValues(userID).Set(float64(latestSampleTimestampMs) / 1000)
		}

		// Exemplars are not expired by Prometheus client libraries, therefore we may receive old exemplars
		// repeated on every scrape. Drop any that are more than 5 minutes older than samples in the same batch.
		// (If we didn't find any samples this will be 0, and we won't reject any exemplars.)
		var minExemplarTS int64
		if earliestSampleTimestampMs != math.MaxInt64 {
			minExemplarTS = earliestSampleTimestampMs - 5*time.Minute.Milliseconds()

			if d.limits.PastGracePeriod(userID) > 0 {
				minExemplarTS = max(minExemplarTS, now.Add(-d.limits.PastGracePeriod(userID)).Add(-d.limits.OutOfOrderTimeWindow(userID)).UnixMilli())
			}
		}

		// Enforce the creation grace period on exemplars too.
		maxExemplarTS := now.Add(d.limits.CreationGracePeriod(userID)).UnixMilli()

		// Are we going to drop native histograms? If yes, let's count and report them.
		countDroppedNativeHistograms := !d.limits.NativeHistogramsIngestionEnabled(userID)
		var droppedNativeHistograms int

		var firstPartialErr error
		var removeIndexes []int
		totalSamples, totalExemplars := 0, 0
		const maxMetricsWithDeduplicatedSamplesToTrace = 10
		var dedupedPerUnsafeMetricName map[string]int
		var valueTooLongSummaries labelValueTooLongSummaries

		for tsIdx, ts := range req.Timeseries {
			totalSamples += len(ts.Samples)
			totalExemplars += len(ts.Exemplars)
			if len(ts.Labels) == 0 {
				removeIndexes = append(removeIndexes, tsIdx)
				continue
			}

			d.labelsHistogram.Observe(float64(len(ts.Labels)))

			skipLabelValidation := d.cfg.SkipLabelValidation || req.GetSkipLabelValidation()
			skipLabelCountValidation := d.cfg.SkipLabelCountValidation || req.GetSkipLabelCountValidation()

			// Note that validateSeries may drop some data in ts.
			rawSamples := len(ts.Samples)
			rawHistograms := len(ts.Histograms)
			validationErr := d.validateSeries(now, &req.Timeseries[tsIdx], userID, pushReq.group, cfg, skipLabelValidation, skipLabelCountValidation, minExemplarTS, maxExemplarTS, &valueTooLongSummaries)

			if countDroppedNativeHistograms {
				droppedNativeHistograms += len(ts.Histograms)
			}

			// Errors in validation are considered non-fatal, as one series in a request may contain
			// invalid data but all the remaining series could be perfectly valid.
			if validationErr != nil {
				if firstPartialErr == nil {
					// The series are never retained by validationErr. This is guaranteed by the way the latter is built.
					firstPartialErr = newValidationError(validationErr)
				}
				removeIndexes = append(removeIndexes, tsIdx)
				continue
			}

			dedupedSamplesAndHistograms := (rawSamples - len(ts.Samples)) + (rawHistograms - len(ts.Histograms))
			if dedupedSamplesAndHistograms > 0 {
				if dedupedPerUnsafeMetricName == nil {
					dedupedPerUnsafeMetricName = make(map[string]int, maxMetricsWithDeduplicatedSamplesToTrace)
				}
				name, err := extract.UnsafeMetricNameFromLabelAdapters(ts.Labels)
				if err != nil {
					name = "unnamed"
				}
				increment := len(dedupedPerUnsafeMetricName) < maxMetricsWithDeduplicatedSamplesToTrace
				if !increment {
					// If at max capacity, only touch pre-existing entries.
					_, increment = dedupedPerUnsafeMetricName[name]
				}
				if increment {
					dedupedPerUnsafeMetricName[name] += dedupedSamplesAndHistograms
				}
			}

			validatedSamples += len(ts.Samples) + len(ts.Histograms)
			validatedExemplars += len(ts.Exemplars)
		}

		if len(dedupedPerUnsafeMetricName) > 0 {
			// Emit tracing span events for metrics with deduped samples.
			sp := trace.SpanFromContext(ctx)
			for unsafeMetricName, deduped := range dedupedPerUnsafeMetricName {
				sp.AddEvent(
					"Distributor.prePushValidationMiddleware[deduplicated samples/histograms with conflicting timestamps from write request]",
					trace.WithAttributes(
						// unsafeMetricName is an unsafe reference to a gRPC unmarshalling buffer,
						// clone it for safe retention.
						attribute.String("metric", strings.Clone(unsafeMetricName)),
						attribute.Int("count", deduped),
					),
				)
			}
		}

		if droppedNativeHistograms > 0 {
			d.droppedNativeHistograms.WithLabelValues(userID).Add(float64(droppedNativeHistograms))
		}

		d.logLabelValueTooLongSummaries(userID, valueTooLongSummaries)

		d.incomingSamplesPerRequest.WithLabelValues(userID).Observe(float64(totalSamples))
		d.incomingExemplarsPerRequest.WithLabelValues(userID).Observe(float64(totalExemplars))

		if len(removeIndexes) > 0 {
			for _, removeIndex := range removeIndexes {
				mimirpb.ReusePreallocTimeseries(&req.Timeseries[removeIndex])
			}
			req.Timeseries = util.RemoveSliceIndexes(req.Timeseries, removeIndexes)
			removeIndexes = removeIndexes[:0]
		}

		for mIdx, m := range req.Metadata {
			if validationErr := cleanAndValidateMetadata(d.metadataValidationMetrics, cfg.metadata, userID, m); validationErr != nil {
				if firstPartialErr == nil {
					// The series are never retained by validationErr. This is guaranteed by the way the latter is built.
					firstPartialErr = newValidationError(validationErr)
				}

				removeIndexes = append(removeIndexes, mIdx)
				continue
			}

			validatedMetadata++
		}
		if len(removeIndexes) > 0 {
			req.Metadata = util.RemoveSliceIndexes(req.Metadata, removeIndexes)
		}

		if validatedSamples == 0 && validatedMetadata == 0 {
			return firstPartialErr
		}

		totalN := validatedSamples + validatedExemplars + validatedMetadata
		if !d.ingestionRateLimiter.AllowN(now, userID, totalN) {
			if len(req.Timeseries) > 0 {
				d.costAttributionMgr.SampleTracker(userID).IncrementDiscardedSamples(req.Timeseries[0].Labels, float64(validatedSamples), reasonRateLimited, now)
			}
			d.discardedSamplesRateLimited.WithLabelValues(userID, pushReq.group).Add(float64(validatedSamples))
			d.discardedExemplarsRateLimited.WithLabelValues(userID).Add(float64(validatedExemplars))
			d.discardedMetadataRateLimited.WithLabelValues(userID).Add(float64(validatedMetadata))

			// Determine whether limiter burst size was exceeded.
			limiterBurst := d.ingestionRateLimiter.Burst(now, userID)
			if totalN > limiterBurst {
				return newIngestionBurstSizeLimitedError(limiterBurst, totalN)
			}

			burstSize := d.limits.IngestionBurstSize(userID)
			if d.limits.IngestionBurstFactor(userID) > 0 {
				burstSize = int(d.limits.IngestionRate(userID) * d.limits.IngestionBurstFactor(userID))
			}
			return newIngestionRateLimitedError(d.limits.IngestionRate(userID), burstSize)
		}

		// totalN included samples, exemplars and metadata. Ingester follows this pattern when computing its ingestion rate.
		d.ingestionRate.Add(int64(totalN))

		err = next(ctx, pushReq)
		if err != nil {
			// Errors resulting from the pushing to the ingesters have priority over validation errors.
			return err
		}

		return firstPartialErr
	})
}

func (d *Distributor) logLabelValueTooLongSummaries(userID string, valueTooLongSummaries labelValueTooLongSummaries) {
	if len(valueTooLongSummaries.summaries) == 0 {
		return
	}

	var msg string
	switch strategy := d.limits.LabelValueLengthOverLimitStrategy(userID); strategy {
	case validation.LabelValueLengthOverLimitStrategyTruncate:
		msg = truncatedLabelValueMsg
	case validation.LabelValueLengthOverLimitStrategyDrop:
		msg = droppedLabelValueMsg
	default:
		panic(fmt.Errorf("unexpected value: %v", strategy))
	}

	kvs := make([]any, 0, 10+len(valueTooLongSummaries.summaries)*10)
	kvs = append(kvs,
		"msg", msg,
		"limit", d.limits.MaxLabelValueLength(userID),
		"total_values_exceeding_limit", valueTooLongSummaries.globalCount,
		"user", userID,
		"insight", true)
	for i, summary := range valueTooLongSummaries.summaries {
		id := strconv.Itoa(i + 1)
		kvs = append(kvs,
			"sample_"+id+"_metric_name", summary.metric,
			"sample_"+id+"_label_name", summary.label,
			"sample_"+id+"_values_exceeding_limit", summary.count,
			"sample_"+id+"_value_length", summary.sampleValueLength,
			"sample_"+id+"_value", summary.sampleValue)
	}

	level.Warn(d.log).Log(kvs...)

}

// prePushMaxSeriesLimitMiddleware enforces the per-tenant max series limit when the usage-tracker service is enabled.
func (d *Distributor) prePushMaxSeriesLimitMiddleware(next PushFunc) PushFunc {
	return WithCleanup(next, func(next PushFunc, ctx context.Context, pushReq *Request) error {
		// If the usage-tracker client hasn't been created it means usage-tracker is disabled
		// for this instance.
		if d.usageTrackerClient == nil {
			return next(ctx, pushReq)
		}

		req, err := pushReq.WriteRequest()
		if err != nil {
			return err
		}

		if len(req.Timeseries) == 0 {
			// There's nothing to reject here.
			return next(ctx, pushReq)
		}

		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return err
		}

		// Generate the stable hash of each series.
		var (
			seriesHashes    = make([]uint64, len(req.Timeseries))
			builder         = labels.NewScratchBuilder(100)
			nonCopiedLabels labels.Labels
			totalTimeseries = len(req.Timeseries)
		)

		for idx, series := range req.Timeseries {
			mimirpb.FromLabelAdaptersOverwriteLabels(&builder, series.Labels, &nonCopiedLabels)
			seriesHashes[idx] = labels.StableHash(nonCopiedLabels)
		}

		// Track the series and check if anyone should be rejected because over the limit.
		// For users that are far from their limits, we can do this asynchronously.
		if d.usageTrackerClient.CanTrackAsync(userID) {
			// User is far from limit.
			// We can perform the track call in parallel with the metrics ingestion hoping that no series would be rejected.
			cleanup := d.parallelUsageTrackerClientTrackSeriesCall(ctx, userID, seriesHashes)
			pushReq.AddCleanup(cleanup)
			return next(ctx, pushReq)
		}

		// User is close to limit, track synchronously.
		rejectedHashes, err := d.usageTrackerClient.TrackSeries(ctx, userID, seriesHashes)
		if err != nil {
			return errors.Wrap(err, "failed to enforce max series limit")
		}

		if len(rejectedHashes) > 0 {
			discardedSamples := filterOutRejectedSeries(req, seriesHashes, rejectedHashes)
			d.discardedSamplesPerUserSeriesLimit.WithLabelValues(userID, pushReq.group).Add(float64(discardedSamples))
		}

		if len(req.Timeseries) == 0 {
			// All series have been rejected, no need to talk to ingesters.
			return newActiveSeriesLimitedError(totalTimeseries, len(rejectedHashes), d.limits.MaxActiveOrGlobalSeriesPerUser(userID))
		}

		// If there's an error coming from the ingesters, prioritize that one.
		if err := next(ctx, pushReq); err != nil {
			return err
		}

		if len(rejectedHashes) > 0 {
			return newActiveSeriesLimitedError(totalTimeseries, len(rejectedHashes), d.limits.MaxActiveOrGlobalSeriesPerUser(userID))
		}

		return nil
	})
}

func (d *Distributor) parallelUsageTrackerClientTrackSeriesCall(ctx context.Context, userID string, seriesHashes []uint64) func() {
	d.asyncUsageTrackerCalls.WithLabelValues(userID).Inc()
	done := make(chan struct{}, 1)
	t0 := time.Now()
	asyncTrackingCtx, cancelAsyncTracking := context.WithCancelCause(ctx)
	go func() {
		defer close(done)
		rejected, err := d.usageTrackerClient.TrackSeries(asyncTrackingCtx, userID, seriesHashes)
		if err != nil {
			level.Error(d.log).Log("msg", "failed to track series asynchronously", "err", err, "user", userID, "series", len(seriesHashes))
		}
		if len(rejected) > 0 {
			level.Warn(d.log).Log("msg", "ingested some series that should have been rejected, because they were tracked asynchronously", "user", userID, "rejected", len(rejected))
			d.asyncUsageTrackerCallsWithRejectedSeries.WithLabelValues(userID).Inc()
		}
	}()

	// Add a cleanup function that will wait for the async tracking to complete.
	return func() {
		defer cancelAsyncTracking(nil)

		tCleanup := time.Now()
		select {
		case <-done:
			// No need to wait.
			return
		default:
		}

		select {
		case <-done:
			level.Info(d.log).Log("msg", "async tracking call took longer than ingestion", "user", userID, "series", len(seriesHashes), "tracking_time", time.Since(t0), "time_since_cleanup", time.Since(tCleanup))
		case <-time.After(d.cfg.UsageTrackerClient.MaxTimeToWaitForAsyncTrackingResponseAfterIngestion):
			level.Warn(d.log).Log("msg", "async tracking call took too long, canceling", "user", userID, "series", len(seriesHashes), "tracking_time", time.Since(t0), "time_since_cleanup", time.Since(tCleanup))
			cancelAsyncTracking(errors.New("async tracking call took too long"))
		}
	}
}

// filterOutRejectedSeries filters out time series from the WriteRequest based on rejected hashes and returns discarded samples count.
// It updates the WriteRequest in place and optimizes memory by reusing preallocated time series.
// seriesHashes should contain the hashes of req.Timeseries in the same order.
func filterOutRejectedSeries(req *mimirpb.WriteRequest, seriesHashes []uint64, rejectedHashes []uint64) int {
	// Build a map of rejected hashes so that it's easier to lookup.
	rejectedHashesMap := make(map[uint64]struct{}, len(rejectedHashes))
	for _, hash := range rejectedHashes {
		rejectedHashesMap[hash] = struct{}{}
	}

	// Filter out rejected series.
	discardedSamples := 0
	o := 0
	for i := 0; i < len(req.Timeseries); i++ {
		seriesHash := seriesHashes[i]

		if _, rejected := rejectedHashesMap[seriesHash]; !rejected {
			// Keep this series.
			req.Timeseries[o] = req.Timeseries[i]
			o++
			continue
		}

		// Keep track of the discarded samples and histograms.
		discardedSamples += len(req.Timeseries[i].Samples) + len(req.Timeseries[i].Histograms)

		// This series has been rejected and filtered out from the WriteRequest. We can reuse its memory.
		mimirpb.ReusePreallocTimeseries(&req.Timeseries[i])
	}

	req.Timeseries = req.Timeseries[:o]
	return discardedSamples
}

// metricsMiddleware updates metrics which are expected to account for all received data,
// including data that later gets modified or dropped.
func (d *Distributor) metricsMiddleware(next PushFunc) PushFunc {
	return WithCleanup(next, func(next PushFunc, ctx context.Context, pushReq *Request) error {
		req, err := pushReq.WriteRequest()
		if err != nil {
			return err
		}

		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return err
		}

		numSamples := 0
		numHistograms := 0
		numExemplars := 0
		for _, ts := range req.Timeseries {
			numSamples += len(ts.Samples)
			numHistograms += len(ts.Histograms)
			numExemplars += len(ts.Exemplars)
		}

		span := trace.SpanFromContext(ctx)
		span.SetAttributes(
			attribute.Int("write.samples", numSamples+numHistograms),
			attribute.Int("write.exemplars", numExemplars),
			attribute.Int("write.metadata", len(req.Metadata)),
		)

		d.incomingRequests.WithLabelValues(userID, req.ProtocolVersion()).Inc()
		d.incomingSamples.WithLabelValues(userID).Add(float64(numSamples + numHistograms))
		d.incomingExemplars.WithLabelValues(userID).Add(float64(numExemplars))
		d.incomingMetadata.WithLabelValues(userID).Add(float64(len(req.Metadata)))

		// Update the write response stats, which are returned to callers using remote write
		// version 2.0.
		updateWriteResponseStatsCtx(ctx, numSamples, numHistograms, numExemplars)

		return next(ctx, pushReq)
	})
}

// outerMaybeDelayMiddleware is a middleware that may delay ingestion if configured.
func (d *Distributor) outerMaybeDelayMiddleware(next PushFunc) PushFunc {
	return func(ctx context.Context, pushReq *Request) error {
		start := d.now()
		// Execute the whole middleware chain.
		err := next(ctx, pushReq)

		userID, userErr := tenant.TenantID(ctx) // Log tenant ID if available.
		if userErr == nil {
			if delay := d.limits.DistributorIngestionArtificialDelay(userID); delay > 0 {
				// Target delay - time spent processing the middleware chain including the push.
				// If the request took longer than the target delay, we don't delay at all as sleep will return immediately for a negative value.
				if delay = delay - d.now().Sub(start); delay > 0 {
					// Delay is configured but request is taking less than the delay
					pushReq.artificialDelay = util.DurationWithJitter(delay, 0.10)
					d.sleep(pushReq.artificialDelay)
				} else {
					// Delay is configured but request is already taking longer than the delay
					pushReq.artificialDelay = 0
				}
			}
			return err
		}

		level.Warn(d.log).Log("msg", "failed to get tenant ID while trying to delay ingestion", "err", userErr)
		return err
	}
}

type ctxKey int

const requestStateKey ctxKey = 1

// requestState represents state of checks for given request. If this object is stored in context,
// it means that request has been checked against inflight requests limit, and FinishPushRequest,
// decreaseInflightPushRequestCounters (or both) must be called for given context.
type requestState struct {
	// If set to true, push request will perform cleanup of inflight metrics after request has actually finished
	// (which can be after push handler returns).
	pushHandlerPerformsCleanup bool

	// If positive, it means that size of httpgrpc.HTTPRequest has been checked and added to inflightPushRequestsBytes.
	httpgrpcRequestSize int64

	// If positive, it means that size of mimirpb.WriteRequest has been checked and added to inflightPushRequestsBytes.
	writeRequestSize int64

	// If set to true, it means that a reactive limiter permit has already been acquired for the respective push request.
	reactiveLimiterPermitAcquired bool
}

func (d *Distributor) StartPushRequest(ctx context.Context, httpgrpcRequestSize int64) (context.Context, error) {
	ctx, _, err := d.startPushRequest(ctx, httpgrpcRequestSize)
	return ctx, err
}

func (d *Distributor) PreparePushRequest(_ context.Context) (func(error), error) {
	return nil, nil
}

// acquireReactiveLimiterPermit acquires a reactive limiter permit to control inflight push requests.
//
// If a permit is successfully acquired, it is recorded in the requestState associated with the provided context
// by setting its reactiveLimiterPermitAcquired field. The function returns a cleanup function that must be called
// when the request completes, which will either record or drop the permit based on the request outcome.
//
// If no requestState is found in the provided context, acquireReactiveLimiterPermit returns errMissingRequestState.
// If called more than once for the same request, it returns errReactiveLimiterPermitAlreadyAcquired.
func (d *Distributor) acquireReactiveLimiterPermit(ctx context.Context) (cleanup func(error), _ error) {
	if d.reactiveLimiter == nil {
		return nil, nil
	}
	rs, alreadyInContext := ctx.Value(requestStateKey).(*requestState)
	if !alreadyInContext {
		return nil, errMissingRequestState
	}
	if rs.reactiveLimiterPermitAcquired {
		return nil, errReactiveLimiterPermitAlreadyAcquired
	}

	// Acquire a permit, blocking if needed
	permit, err := d.reactiveLimiter.AcquirePermit(ctx)
	if err != nil {
		d.rejectedRequests.WithLabelValues(reasonDistributorMaxInflightPushRequests).Inc()
		return nil, errReactiveLimiterLimitExceeded
	}
	cleanup = func(err error) {
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			permit.Drop()
		} else {
			permit.Record()
		}
	}
	rs.reactiveLimiterPermitAcquired = true
	return cleanup, nil
}

// startPushRequest does limits checks at the beginning of Push request in distributor.
// This can be called from different places, even multiple times for the same request:
//
//   - from gRPC Method limit check. This only applies if request arrived as httpgrpc.HTTPRequest, and request metadata
//     in gRPC request provided enough information to do the check. Errors are not logged on this path, only returned to client.
//
//   - from Distributor's limitsMiddleware method. If error is returned, limitsMiddleware will wrap the error using util_log.DoNotLogError.
//
// This method creates requestState object and stores it in the context.
// This object describes which checks were already performed on the request,
// and which component is responsible for doing a cleanup.
func (d *Distributor) startPushRequest(ctx context.Context, httpgrpcRequestSize int64) (context.Context, *requestState, error) {
	// The distributor service and all its dependent services must be running
	// before any push request can be started.
	if s := d.State(); s != services.Running {
		return ctx, nil, newUnavailableError(s)
	}

	// If requestState is already in context, it means that StartPushRequest already ran for this request.
	// This check must be performed first, before any other logic in this function.
	rs, alreadyInContext := ctx.Value(requestStateKey).(*requestState)
	if alreadyInContext {
		return ctx, rs, nil
	}

	if d.reactiveLimiter != nil && !d.reactiveLimiter.CanAcquirePermit() {
		d.rejectedRequests.WithLabelValues(reasonDistributorMaxInflightPushRequests).Inc()
		return ctx, nil, errReactiveLimiterLimitExceeded
	}

	rs = &requestState{}

	cleanupInDefer := true

	// Increment number of requests and bytes before doing the checks, so that we hit error if this request crosses the limits.
	inflight := d.inflightPushRequests.Inc()
	defer func() {
		if cleanupInDefer {
			d.decreaseInflightPushRequestCounters(rs)
		}
	}()

	il := d.getInstanceLimits()
	if il.MaxInflightPushRequests > 0 && inflight > int64(il.MaxInflightPushRequests) {
		d.rejectedRequests.WithLabelValues(reasonDistributorMaxInflightPushRequests).Inc()
		return ctx, nil, errMaxInflightRequestsReached
	}

	if il.MaxIngestionRate > 0 {
		if rate := d.ingestionRate.Rate(); rate >= il.MaxIngestionRate {
			d.rejectedRequests.WithLabelValues(reasonDistributorMaxIngestionRate).Inc()
			return ctx, nil, errMaxIngestionRateReached
		}
	}

	// If we know the httpgrpcRequestSize, we can check it.
	if httpgrpcRequestSize > 0 {
		if err := d.checkHttpgrpcRequestSize(rs, httpgrpcRequestSize); err != nil {
			return ctx, nil, err
		}
	}

	ctx = context.WithValue(ctx, requestStateKey, rs)

	cleanupInDefer = false
	return ctx, rs, nil
}

func (d *Distributor) checkHttpgrpcRequestSize(rs *requestState, httpgrpcRequestSize int64) error {
	// If httpgrpcRequestSize was already checked, don't check it again.
	if rs.httpgrpcRequestSize > 0 {
		return nil
	}
	rs.httpgrpcRequestSize = httpgrpcRequestSize
	inflightBytes := d.inflightPushRequestsBytes.Add(httpgrpcRequestSize)
	return d.checkInflightBytes(inflightBytes + decompressionEstMultiplier*httpgrpcRequestSize)
}

func (d *Distributor) checkWriteRequestSize(rs *requestState, writeRequestSize int64) error {
	// If writeRequestSize was already checked, don't check it again.
	if rs.writeRequestSize > 0 {
		return nil
	}

	rs.writeRequestSize = writeRequestSize
	inflightBytes := d.inflightPushRequestsBytes.Add(writeRequestSize)
	return d.checkInflightBytes(inflightBytes)
}

func (d *Distributor) checkInflightBytes(inflightBytes int64) error {
	il := d.getInstanceLimits()

	if il.MaxInflightPushRequestsBytes > 0 && inflightBytes > int64(il.MaxInflightPushRequestsBytes) {
		d.rejectedRequests.WithLabelValues(reasonDistributorMaxInflightPushRequestsBytes).Inc()
		return errMaxInflightRequestsBytesReached
	}
	return nil
}

// FinishPushRequest is a counter-part to StartPushRequest, and must be called exactly once while handling the push request,
// on the same goroutine as push method itself.
func (d *Distributor) FinishPushRequest(ctx context.Context) {
	rs, ok := ctx.Value(requestStateKey).(*requestState)
	if !ok {
		return
	}

	if rs.pushHandlerPerformsCleanup {
		return
	}

	d.decreaseInflightPushRequestCounters(rs)
}

func (d *Distributor) decreaseInflightPushRequestCounters(rs *requestState) {
	d.inflightPushRequests.Dec()
	if rs.httpgrpcRequestSize > 0 {
		d.inflightPushRequestsBytes.Sub(rs.httpgrpcRequestSize)
	}
	if rs.writeRequestSize > 0 {
		d.inflightPushRequestsBytes.Sub(rs.writeRequestSize)
	}
}

// limitsMiddleware checks for instance limits and rejects request if this instance cannot process it at the moment.
func (d *Distributor) limitsMiddleware(next PushFunc) PushFunc {
	return WithCleanup(next, func(next PushFunc, ctx context.Context, pushReq *Request) (retErr error) {
		ctx, rs, err := d.startPushRequest(ctx, pushReq.contentLength)
		if err != nil {
			return middleware.DoNotLogError{Err: err}
		}

		rs.pushHandlerPerformsCleanup = true
		// Decrement in-flight request counters as soon as limitsMiddleware returns.
		pushReq.AddCleanup(func() {
			d.decreaseInflightPushRequestCounters(rs)
		})

		reactiveLimiterCleanup, reactiveLimiterErr := d.acquireReactiveLimiterPermit(ctx)
		if reactiveLimiterErr != nil {
			d.rejectedRequests.WithLabelValues(reasonDistributorMaxInflightPushRequests).Inc()
			return reactiveLimiterErr
		}
		if reactiveLimiterCleanup != nil {
			defer func() {
				reactiveLimiterCleanup(retErr)
			}()
		}

		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return err
		}

		now := mtime.Now()
		if !d.requestRateLimiter.AllowN(now, userID, 1) {
			d.discardedRequestsRateLimited.WithLabelValues(userID).Add(1)

			return newRequestRateLimitedError(d.limits.RequestRate(userID), d.limits.RequestBurstSize(userID))
		}

		// Note that we don't enforce the per-user ingestion rate limit here since we need to apply validation
		// before we know how many samples/exemplars/metadata we'll actually be writing.

		req, err := pushReq.WriteRequest()
		if err != nil {
			return err
		}

		if err := d.checkWriteRequestSize(rs, int64(req.Size())); err != nil {
			return err
		}

		return next(ctx, pushReq)
	})
}

// Push is gRPC method registered as client.IngesterServer and distributor.DistributorServer.
func (d *Distributor) Push(ctx context.Context, req *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	pushReq := NewParsedRequest(req)
	pushReq.AddCleanup(func() {
		req.FreeBuffer()
		mimirpb.ReuseSlice(req.Timeseries)
	})

	pushErr := d.PushWithMiddlewares(ctx, pushReq)
	if pushErr == nil {
		return &mimirpb.WriteResponse{}, nil
	}
	handledErr := d.handlePushError(pushErr)
	return nil, handledErr
}

func (d *Distributor) handlePushError(pushErr error) error {
	if errors.Is(pushErr, context.Canceled) {
		return pushErr
	}

	if errors.Is(pushErr, context.DeadlineExceeded) {
		return pushErr
	}

	return toErrorWithGRPCStatus(pushErr)
}

// push takes a write request and distributes it to ingesters using the ring.
// Strings in pushReq may be pointers into the gRPC buffer which will be reused, so must be copied if retained.
// push does not check limits like ingestion rate and inflight requests.
// These limits are checked either by Push gRPC method (when invoked via gRPC) or limitsMiddleware (when invoked via HTTP)
func (d *Distributor) push(ctx context.Context, pushReq *Request) error {
	cleanupInDefer := true
	defer func() {
		if cleanupInDefer {
			pushReq.CleanUp()
		}
	}()

	req, err := pushReq.WriteRequest()
	if err != nil {
		return err
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}

	d.updateReceivedMetrics(req, userID)

	if len(req.Timeseries) == 0 && len(req.Metadata) == 0 {
		return nil
	}

	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("user", userID))

	if d.cfg.WriteRequestsBufferPoolingEnabled {
		slabPool := pool.NewFastReleasingSlabPool[byte](&d.writeRequestBytePool, writeRequestSlabPoolSize)
		ctx = ingester_client.WithSlabPool(ctx, slabPool)
	}

	// Get both series and metadata keys in one slice.
	keys, initialMetadataIndex := getSeriesAndMetadataTokens(userID, req)

	var (
		ingestersSubring  ring.DoBatchRing
		partitionsSubring ring.DoBatchRing
	)

	// Get the tenant's subring to use to either write to ingesters or partitions.
	if d.cfg.IngestStorageConfig.Enabled {
		subring, err := d.partitionsRing.ShuffleShard(userID, d.limits.IngestionPartitionsTenantShardSize(userID))
		if err != nil {
			return err
		}

		partitionsSubring = ring.NewActivePartitionBatchRing(subring.PartitionRing())
	}

	if !d.cfg.IngestStorageConfig.Enabled || d.cfg.IngestStorageConfig.Migration.DistributorSendToIngestersEnabled {
		ingestersSubring = d.ingestersRing.ShuffleShard(userID, d.limits.IngestionTenantShardSize(userID))
	}

	// we must not re-use buffers now until all writes to backends (e.g. ingesters) have completed, which can happen
	// even after this function returns. For this reason, it's unsafe to cleanup in the defer and we'll do the cleanup
	// once all backend requests have completed (see cleanup function passed to sendWriteRequestToBackends()).
	cleanupInDefer = false

	return d.sendWriteRequestToBackends(ctx, userID, req, keys, initialMetadataIndex, ingestersSubring, partitionsSubring, pushReq.CleanUp)
}

// sendWriteRequestToBackends sends the input req data to backends. The backends could be:
// - Ingesters, when ingestersSubring is not nil
// - Ingest storage partitions, when partitionsSubring is not nil
//
// The input cleanup function is guaranteed to be called after all requests to all backends have completed.
func (d *Distributor) sendWriteRequestToBackends(ctx context.Context, tenantID string, req *mimirpb.WriteRequest, keys []uint32, initialMetadataIndex int, ingestersSubring, partitionsSubring ring.DoBatchRing, cleanup func()) error {
	var (
		wg            = sync.WaitGroup{}
		partitionsErr error
		ingestersErr  error
	)

	// Ensure at least one ring has been provided.
	if ingestersSubring == nil && partitionsSubring == nil {
		// It should never happen. If it happens, it's a logic bug.
		panic("no tenant subring has been provided to sendWriteRequestToBackends()")
	}

	// Use an independent context to make sure all backend instances (e.g. ingesters) get samples even if we return early.
	// It will still take a while to lookup the ring and calculate which instance gets which series,
	// so we'll start the remote timeout once the first callback is called.
	remoteRequestContextAndCancel := sync.OnceValues(func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.WithoutCancel(ctx), d.cfg.RemoteTimeout)
	})

	remoteRequestContext := func() context.Context {
		ctx, _ := remoteRequestContextAndCancel()
		return ctx
	}

	var partitionsRequestContext func() context.Context
	var partitionsRequestContextAndCancel func() (context.Context, context.CancelFunc)

	if d.cfg.IngestStorageConfig.Migration.DistributorSendToIngestersEnabled && d.cfg.IngestStorageConfig.Migration.IngestStorageMaxWaitTime > 0 {
		// Create a separate context for partitions with shorter timeout during migration
		partitionsRequestContextAndCancel = sync.OnceValues(func() (context.Context, context.CancelFunc) {
			timeout := d.cfg.IngestStorageConfig.Migration.IngestStorageMaxWaitTime
			return context.WithTimeout(context.WithoutCancel(ctx), timeout)
		})

		partitionsRequestContext = func() context.Context {
			ctx, _ := partitionsRequestContextAndCancel()
			return ctx
		}
	} else {
		partitionsRequestContext = remoteRequestContext
	}

	batchCleanup := func() {
		// Notify the provided callback that it's now safe to run the cleanup because there are no
		// more in-flight requests to the backend.
		cleanup()

		// All requests have completed, so it's now safe to cancel the requests context to release resources.
		_, cancel := remoteRequestContextAndCancel()
		cancel()
		if partitionsRequestContextAndCancel != nil {
			_, cancel = partitionsRequestContextAndCancel()
			cancel()
		}
	}

	batchOptions := ring.DoBatchOptions{
		Cleanup:       batchCleanup,
		IsClientError: isIngestionClientError,
		Go:            d.doBatchPushWorkers,
	}

	// Keep it easy if there's only 1 backend to write to.
	if partitionsSubring == nil {
		return d.sendWriteRequestToIngesters(ctx, ingestersSubring, req, keys, initialMetadataIndex, remoteRequestContext, batchOptions)
	}
	if ingestersSubring == nil {
		return d.sendWriteRequestToPartitions(ctx, tenantID, partitionsSubring, req, keys, initialMetadataIndex, partitionsRequestContext, batchOptions)
	}

	// Prepare a callback function that will call the input cleanup callback function only after
	// the cleanup has been done for all backends.
	cleanupWaitBackends := atomic.NewInt64(2)
	batchOptions.Cleanup = func() {
		if cleanupWaitBackends.Dec() == 0 {
			batchCleanup()
		}
	}

	// Write both to ingesters and partitions.
	wg.Add(2)

	go func() {
		defer wg.Done()

		ingestersErr = d.sendWriteRequestToIngesters(ctx, ingestersSubring, req, keys, initialMetadataIndex, remoteRequestContext, batchOptions)
	}()

	go func() {
		defer wg.Done()

		partitionsErr = d.sendWriteRequestToPartitions(ctx, tenantID, partitionsSubring, req, keys, initialMetadataIndex, partitionsRequestContext, batchOptions)
	}()

	// Wait until all backends have done.
	wg.Wait()

	// Ingester errors could be soft (e.g. 4xx) or hard errors (e.g. 5xx) errors, while partition errors are always hard
	// errors. For this reason, it's important to give precedence to partition errors, otherwise the distributor may return
	// a 4xx (ingester error) when it should actually be a 5xx (partition error).
	if partitionsErr != nil {
		if d.cfg.IngestStorageConfig.Migration.DistributorSendToIngestersEnabled && d.cfg.IngestStorageConfig.Migration.IgnoreIngestStorageErrors {
			level.Warn(d.log).Log("msg", "failed to write to ingest storage", "error", partitionsErr)
		} else {
			return partitionsErr
		}
	}
	return ingestersErr
}

func (d *Distributor) sendWriteRequestToIngesters(ctx context.Context, tenantRing ring.DoBatchRing, req *mimirpb.WriteRequest, keys []uint32, initialMetadataIndex int, remoteRequestContext func() context.Context, batchOptions ring.DoBatchOptions) error {
	err := ring.DoBatchWithOptions(ctx, ring.WriteNoExtend, tenantRing, keys,
		func(ingester ring.InstanceDesc, indexes []int) error {
			req := req.ForIndexes(indexes, initialMetadataIndex)

			h, err := d.ingesterPool.GetClientForInstance(ingester)
			if err != nil {
				return err
			}
			c := h.(ingester_client.IngesterClient)

			ctx := remoteRequestContext()
			ctx = grpcutil.AppendMessageSizeToOutgoingContext(ctx, req) // Let ingester know the size of the message, without needing to read the message first.

			_, err = c.Push(ctx, req)
			err = wrapIngesterPushError(err, ingester.Id)
			err = wrapDeadlineExceededPushError(err)

			return err
		}, batchOptions)

	// Since data may be written to different backends it may be helpful to clearly identify which backend failed.
	return errors.Wrap(err, "send data to ingesters")
}

func (d *Distributor) sendWriteRequestToPartitions(ctx context.Context, tenantID string, tenantRing ring.DoBatchRing, req *mimirpb.WriteRequest, keys []uint32, initialMetadataIndex int, remoteRequestContext func() context.Context, batchOptions ring.DoBatchOptions) error {
	err := ring.DoBatchWithOptions(ctx, ring.WriteNoExtend, tenantRing, keys,
		func(partition ring.InstanceDesc, indexes []int) error {
			req := req.ForIndexes(indexes, initialMetadataIndex)

			// The partition ID is stored in the ring.InstanceDesc Id.
			partitionID, err := strconv.ParseUint(partition.Id, 10, 31)
			if err != nil {
				return err
			}

			ctx := remoteRequestContext()
			err = d.ingestStorageWriter.WriteSync(ctx, int32(partitionID), tenantID, req)
			err = wrapPartitionPushError(err, int32(partitionID))
			err = wrapDeadlineExceededPushError(err)

			return err
		}, batchOptions,
	)

	// Since data may be written to different backends it may be helpful to clearly identify which backend failed.
	return errors.Wrap(err, "send data to partitions")
}

// getSeriesAndMetadataTokens returns a slice of tokens for the series and metadata from the request in this specific order.
// Metadata tokens start at initialMetadataIndex.
func getSeriesAndMetadataTokens(userID string, req *mimirpb.WriteRequest) (keys []uint32, initialMetadataIndex int) {
	seriesKeys := getTokensForSeries(userID, req.Timeseries)
	metadataKeys := getTokensForMetadata(userID, req.Metadata)

	// All tokens, stored in order: series, metadata.
	keys = make([]uint32, len(seriesKeys)+len(metadataKeys))
	initialMetadataIndex = len(seriesKeys)
	copy(keys, seriesKeys)
	copy(keys[initialMetadataIndex:], metadataKeys)
	return keys, initialMetadataIndex
}

func getTokensForSeries(userID string, series []mimirpb.PreallocTimeseries) []uint32 {
	if len(series) == 0 {
		return nil
	}

	result := make([]uint32, 0, len(series))
	for _, ts := range series {
		result = append(result, tokenForLabels(userID, ts.Labels))
	}
	return result
}

func getTokensForMetadata(userID string, metadata []*mimirpb.MetricMetadata) []uint32 {
	if len(metadata) == 0 {
		return nil
	}
	metadataKeys := make([]uint32, 0, len(metadata))

	for _, m := range metadata {
		metadataKeys = append(metadataKeys, tokenForMetadata(userID, m.MetricFamilyName))
	}
	return metadataKeys
}

func tokenForLabels(userID string, labels []mimirpb.LabelAdapter) uint32 {
	return mimirpb.ShardByAllLabelAdapters(userID, labels)
}

func tokenForMetadata(userID string, metricName string) uint32 {
	return mimirpb.ShardByMetricName(userID, metricName)
}

func (d *Distributor) updateReceivedMetrics(req *mimirpb.WriteRequest, userID string) {
	var receivedSamples, receivedHistograms, receivedHistogramBuckets, receivedExemplars, receivedMetadata int
	for _, ts := range req.Timeseries {
		receivedSamples += len(ts.Samples)
		receivedExemplars += len(ts.Exemplars)
		receivedHistograms += len(ts.Histograms)
		for _, h := range ts.Histograms {
			receivedHistogramBuckets += h.BucketCount()
		}
	}
	d.costAttributionMgr.SampleTracker(userID).IncrementReceivedSamples(req, mtime.Now())
	receivedMetadata = len(req.Metadata)

	d.receivedSamples.WithLabelValues(userID).Add(float64(receivedSamples + receivedHistograms))
	d.receivedNativeHistogramSamples.WithLabelValues(userID).Add(float64(receivedHistograms))
	d.receivedNativeHistogramBuckets.WithLabelValues(userID).Add(float64(receivedHistogramBuckets))
	d.receivedExemplars.WithLabelValues(userID).Add(float64(receivedExemplars))
	d.receivedMetadata.WithLabelValues(userID).Add(float64(receivedMetadata))
}

// forReplicationSets runs f, in parallel, for all ingesters in the input replicationSets.
// Return an error if any f fails for any of the input replicationSets.
func forReplicationSets[R any](ctx context.Context, d *Distributor, replicationSets []ring.ReplicationSet, f func(context.Context, ingester_client.IngesterClient) (R, error)) ([]R, error) {
	wrappedF := func(ctx context.Context, ingester *ring.InstanceDesc) (R, error) {
		client, err := d.ingesterPool.GetClientForInstance(*ingester)
		if err != nil {
			var empty R
			return empty, err
		}

		return f(ctx, client.(ingester_client.IngesterClient))
	}

	cleanup := func(_ R) {
		// Nothing to do.
	}

	quorumConfig := d.queryQuorumConfigForReplicationSets(ctx, replicationSets)

	return concurrency.ForEachJobMergeResults[ring.ReplicationSet, R](ctx, replicationSets, 0, func(ctx context.Context, set ring.ReplicationSet) ([]R, error) {
		return ring.DoUntilQuorum(ctx, set, quorumConfig, wrappedF, cleanup)
	})
}

// queryQuorumConfigForReplicationSets returns the config to use with "do until quorum" functions when running queries.
func (d *Distributor) queryQuorumConfigForReplicationSets(ctx context.Context, replicationSets []ring.ReplicationSet) ring.DoUntilQuorumConfig {
	var zoneSorter ring.ZoneSorter

	if d.cfg.IngestStorageConfig.Enabled {
		zoneSorter = queryIngesterPartitionsRingZoneSorter(d.cfg.PreferAvailabilityZones)
	} else {
		// We expect to always have exactly 1 replication set when ingest storage is disabled.
		// To keep the code safer, we run with no zone sorter if that's not the case.
		if len(replicationSets) == 1 {
			zoneSorter = queryIngestersRingZoneSorter(replicationSets[0])
		}
	}

	return ring.DoUntilQuorumConfig{
		MinimizeRequests:    d.cfg.MinimizeIngesterRequests,
		HedgingDelay:        d.cfg.MinimiseIngesterRequestsHedgingDelay,
		ZoneSorter:          zoneSorter,
		Logger:              spanlogger.FromContext(ctx, d.log),
		IncludeReplicaCount: d.cfg.IngestStorageConfig.Enabled,
	}
}

// queryIngestersRingZoneSorter returns a ring.ZoneSorter that should be used to sort ingester zones
// to attempt to query first, when ingest storage is disabled.
func queryIngestersRingZoneSorter(replicationSet ring.ReplicationSet) ring.ZoneSorter {
	return func(zones []string) []string {
		inactiveCount := make(map[string]int, len(zones))

		for _, i := range replicationSet.Instances {
			if i.State != ring.ACTIVE {
				inactiveCount[i.Zone]++
			}
		}

		slices.SortFunc(zones, func(a, b string) int {
			return inactiveCount[a] - inactiveCount[b]
		})

		return zones
	}
}

// queryIngesterPartitionsRingZoneSorter returns a ring.ZoneSorter that should be used to sort
// ingester zones to attempt to query first, when ingest storage is enabled.
//
// The sorter gives preference to preferredZones if non empty, and then randomizes the other zones.
// All preferred zones are given equal priority.
func queryIngesterPartitionsRingZoneSorter(preferredZones []string) ring.ZoneSorter {
	return func(zones []string) []string {
		// Shuffle the zones to distribute load evenly.
		if len(zones) > 2 || (len(preferredZones) == 0 && len(zones) > 1) {
			rand.Shuffle(len(zones), func(i, j int) {
				zones[i], zones[j] = zones[j], zones[i]
			})
		}

		if len(preferredZones) > 0 {
			// Move all preferred zones to the front.
			// This gives equal priority to all preferred zones (since they were already shuffled).
			nextPos := 0
			for idx, zone := range zones {
				if slices.Contains(preferredZones, zone) {
					zones[nextPos], zones[idx] = zones[idx], zones[nextPos]
					nextPos++
				}
			}
		}

		return zones
	}
}

// LabelValuesForLabelName returns the label values associated with the given labelName, among all series with samples
// timestamp between from and to, and series labels matching the optional matchers.
func (d *Distributor) LabelValuesForLabelName(ctx context.Context, from, to model.Time, labelName model.LabelName, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, error) {
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx)
	if err != nil {
		return nil, err
	}

	req, err := ingester_client.ToLabelValuesRequest(labelName, from, to, hints, matchers)
	if err != nil {
		return nil, err
	}

	resps, err := forReplicationSets(ctx, d, replicationSets, func(ctx context.Context, client ingester_client.IngesterClient) (*ingester_client.LabelValuesResponse, error) {
		return client.LabelValues(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	valueSet := map[string]struct{}{}
	for _, resp := range resps {
		for _, v := range resp.LabelValues {
			valueSet[v] = struct{}{}
		}
	}

	values := make([]string, 0, len(valueSet))
	for v := range valueSet {
		values = append(values, v)
	}

	// We need the values returned to be sorted.
	slices.Sort(values)

	if hints != nil && hints.Limit > 0 && len(values) > hints.Limit {
		values = values[:hints.Limit]
	}

	return values, nil
}

// LabelNamesAndValues returns the label name and value pairs for series matching the input label matchers.
//
// The actual series considered eligible depend on countMethod:
//   - inmemory: in-memory series in ingesters.
//   - active: in-memory series in ingesters which are also tracked as active ones.
func (d *Distributor) LabelNamesAndValues(ctx context.Context, matchers []*labels.Matcher, countMethod cardinality.CountMethod) (*ingester_client.LabelNamesAndValuesResponse, error) {
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx)
	if err != nil {
		return nil, err
	}

	req, err := toLabelNamesCardinalityRequest(matchers, countMethod)
	if err != nil {
		return nil, err
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	sizeLimitBytes := d.limits.LabelNamesAndValuesResultsMaxSizeBytes(userID)
	merger := &labelNamesAndValuesResponseMerger{result: map[string]map[string]struct{}{}, sizeLimitBytes: sizeLimitBytes}

	_, err = forReplicationSets(ctx, d, replicationSets, func(ctx context.Context, client ingester_client.IngesterClient) (ingester_client.Ingester_LabelNamesAndValuesClient, error) {
		stream, err := client.LabelNamesAndValues(ctx, req)
		if err != nil {
			return nil, err
		}
		defer util.CloseAndExhaust[*ingester_client.LabelNamesAndValuesResponse](stream) //nolint:errcheck
		return nil, merger.collectResponses(stream)
	})
	if err != nil {
		return nil, err
	}
	return merger.toLabelNamesAndValuesResponses(), nil
}

type labelNamesAndValuesResponseMerger struct {
	lock             sync.Mutex
	result           map[string]map[string]struct{}
	sizeLimitBytes   int
	currentSizeBytes int
}

func toLabelNamesCardinalityRequest(matchers []*labels.Matcher, countMethod cardinality.CountMethod) (*ingester_client.LabelNamesAndValuesRequest, error) {
	matchersProto, err := ingester_client.ToLabelMatchers(matchers)
	if err != nil {
		return nil, err
	}
	ingesterCountMethod, err := toIngesterCountMethod(countMethod)
	if err != nil {
		return nil, err
	}
	return &ingester_client.LabelNamesAndValuesRequest{
		Matchers:    matchersProto,
		CountMethod: ingesterCountMethod,
	}, nil
}

// toLabelNamesAndValuesResponses converts map with distinct label values to `ingester_client.LabelNamesAndValuesResponse`.
func (m *labelNamesAndValuesResponseMerger) toLabelNamesAndValuesResponses() *ingester_client.LabelNamesAndValuesResponse {
	// we need to acquire the lock to prevent concurrent read/write to the map because it might be a case that some ingesters responses are
	// still being processed if replicationSet.Do() returned execution to this method when it decided that it got enough responses from the quorum of instances.
	m.lock.Lock()
	defer m.lock.Unlock()
	responses := make([]*ingester_client.LabelValues, 0, len(m.result))
	for name, values := range m.result {
		labelValues := make([]string, 0, len(values))
		for val := range values {
			labelValues = append(labelValues, val)
		}
		responses = append(responses, &ingester_client.LabelValues{
			LabelName: name,
			Values:    labelValues,
		})
	}
	return &ingester_client.LabelNamesAndValuesResponse{Items: responses}
}

// collectResponses listens for the stream and once the message is received, puts labels and values to the map with distinct label values.
func (m *labelNamesAndValuesResponseMerger) collectResponses(stream ingester_client.Ingester_LabelNamesAndValuesClient) error {
	for {
		message, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		err = m.putItemsToMap(message)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *labelNamesAndValuesResponseMerger) putItemsToMap(message *ingester_client.LabelNamesAndValuesResponse) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, item := range message.Items {
		values, exists := m.result[item.LabelName]
		if !exists {
			m.currentSizeBytes += len(item.LabelName)
			values = make(map[string]struct{}, len(item.Values))
			m.result[item.LabelName] = values
		}
		for _, val := range item.Values {
			if _, valueExists := values[val]; !valueExists {
				m.currentSizeBytes += len(val)
				if m.currentSizeBytes > m.sizeLimitBytes {
					return fmt.Errorf("size of distinct label names and values is greater than %v bytes", m.sizeLimitBytes)
				}
				values[val] = struct{}{}
			}
		}
	}
	return nil
}

// LabelValuesCardinality performs the following two operations in parallel:
//   - queries ingesters for label values cardinality of a set of labelNames
//   - queries ingesters for user stats to get the ingester's series head count
func (d *Distributor) LabelValuesCardinality(ctx context.Context, labelNames []model.LabelName, matchers []*labels.Matcher, countMethod cardinality.CountMethod) (uint64, *ingester_client.LabelValuesCardinalityResponse, error) {
	var totalSeries uint64
	var labelValuesCardinalityResponse *ingester_client.LabelValuesCardinalityResponse

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return 0, nil, err
	}

	lbNamesLimit := d.limits.LabelValuesMaxCardinalityLabelNamesPerRequest(userID)
	if len(labelNames) > lbNamesLimit {
		return 0, nil, httpgrpc.Errorf(http.StatusBadRequest, "label values cardinality request label names limit (limit: %d actual: %d) exceeded", lbNamesLimit, len(labelNames))
	}

	// Run labelValuesCardinality and UserStats methods in parallel
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		response, err := d.labelValuesCardinality(ctx, labelNames, matchers, countMethod)
		if err == nil {
			labelValuesCardinalityResponse = response
		}
		return err
	})
	group.Go(func() error {
		response, err := d.UserStats(ctx, countMethod)
		if err == nil {
			totalSeries = response.NumSeries
		}
		return err
	})
	if err := group.Wait(); err != nil {
		return 0, nil, err
	}
	return totalSeries, labelValuesCardinalityResponse, nil
}

// labelValuesCardinality queries ingesters for label values cardinality of a set of labelNames
// Returns a LabelValuesCardinalityResponse where each item contains an exclusive label name and associated label values
func (d *Distributor) labelValuesCardinality(ctx context.Context, labelNames []model.LabelName, matchers []*labels.Matcher, countMethod cardinality.CountMethod) (*ingester_client.LabelValuesCardinalityResponse, error) {
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx)
	if err != nil {
		return nil, err
	}

	// When ingest storage is disabled, if ingesters are running in a single zone we can't tolerate any errors.
	// In this case we expect exactly 1 replication set.
	if !d.cfg.IngestStorageConfig.Enabled && len(replicationSets) == 1 && replicationSets[0].ZoneCount() == 1 {
		replicationSets[0].MaxErrors = 0
	}

	cardinalityConcurrentMap := &labelValuesCardinalityConcurrentMap{
		numReplicationSets:  len(replicationSets),
		labelValuesCounters: make(map[string]map[string]countByReplicationSetAndZone, len(labelNames)),
	}

	labelValuesReq, err := toLabelValuesCardinalityRequest(labelNames, matchers, countMethod)
	if err != nil {
		return nil, err
	}

	quorumConfig := d.queryQuorumConfigForReplicationSets(ctx, replicationSets)

	// Fetch labels cardinality from each ingester and collect responses by ReplicationSet.
	//
	// When ingest storage is enabled we expect 1 ReplicationSet for each partition. A series is sharded only to 1
	// partition and the number of successful responses for each partition (ReplicationSet) may be different when
	// ingesters request minimization is disabled. For this reason, we collect responses by ReplicationSet, so that
	// we can later estimate the number of series with a higher accuracy.
	err = concurrency.ForEachJob(ctx, len(replicationSets), 0, func(ctx context.Context, replicationSetIdx int) error {
		replicationSet := replicationSets[replicationSetIdx]

		_, err := ring.DoUntilQuorum[any](ctx, replicationSet, quorumConfig, func(ctx context.Context, desc *ring.InstanceDesc) (any, error) {
			poolClient, err := d.ingesterPool.GetClientForInstance(*desc)
			if err != nil {
				return nil, err
			}

			client := poolClient.(ingester_client.IngesterClient)

			stream, err := client.LabelValuesCardinality(ctx, labelValuesReq)
			if err != nil {
				return nil, err
			}
			defer func() { _ = util.CloseAndExhaust[*ingester_client.LabelValuesCardinalityResponse](stream) }()

			return nil, cardinalityConcurrentMap.processMessages(replicationSetIdx, desc.Zone, stream)
		}, func(_ any) {})

		return err
	})

	if err != nil {
		return nil, err
	}

	zonesTotal := 0
	if len(replicationSets) > 0 {
		zonesTotal = replicationSets[0].ZoneCount()
	}
	approximateFromZonesFunc := func(countByZone map[string]uint64) uint64 {
		return approximateFromZones(zonesTotal, d.ingestersRing.ReplicationFactor(), countByZone)
	}
	// When the ingest storage is enabled a partition is owned by only 1 ingester per zone.
	// So we always approximate the resulting stats as max of what a single zone has.
	if d.cfg.IngestStorageConfig.Enabled {
		approximateFromZonesFunc = maxFromZones[uint64]
	}

	return cardinalityConcurrentMap.toResponse(approximateFromZonesFunc), nil
}

func toLabelValuesCardinalityRequest(labelNames []model.LabelName, matchers []*labels.Matcher, countMethod cardinality.CountMethod) (*ingester_client.LabelValuesCardinalityRequest, error) {
	matchersProto, err := ingester_client.ToLabelMatchers(matchers)
	if err != nil {
		return nil, err
	}
	labelNamesStr := make([]string, 0, len(labelNames))
	for _, labelName := range labelNames {
		labelNamesStr = append(labelNamesStr, string(labelName))
	}
	ingesterCountMethod, err := toIngesterCountMethod(countMethod)
	if err != nil {
		return nil, err
	}
	return &ingester_client.LabelValuesCardinalityRequest{LabelNames: labelNamesStr, Matchers: matchersProto, CountMethod: ingesterCountMethod}, nil
}

func toIngesterCountMethod(countMethod cardinality.CountMethod) (ingester_client.CountMethod, error) {
	switch countMethod {
	case cardinality.InMemoryMethod:
		return ingester_client.IN_MEMORY, nil
	case cardinality.ActiveMethod:
		return ingester_client.ACTIVE, nil
	default:
		return ingester_client.IN_MEMORY, fmt.Errorf("unknown count method %q", countMethod)
	}
}

// countByReplicationSetAndZone keeps track of a counter by replication set index and zone.
type countByReplicationSetAndZone []map[string]uint64

type labelValuesCardinalityConcurrentMap struct {
	numReplicationSets int

	// labelValuesCounters stores the count of label name-value pairs by replication set and zone.
	// The map is: map[label_name]map[label_value]countByReplicationSetAndZone
	labelValuesCountersMx sync.Mutex
	labelValuesCounters   map[string]map[string]countByReplicationSetAndZone
}

// processMessages reads and processes all LabelValuesCardinalityResponse messages received from an ingester
// via gRPC.
func (cm *labelValuesCardinalityConcurrentMap) processMessages(replicationSetIdx int, zone string, stream ingester_client.Ingester_LabelValuesCardinalityClient) error {
	for {
		message, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		cm.processMessage(replicationSetIdx, zone, message)
	}
	return nil
}

// processMessage processes a single LabelValuesCardinalityResponse message received from an ingester
// via gRPC and increment the counters of each label name-value pair.
func (cm *labelValuesCardinalityConcurrentMap) processMessage(replicationSetIdx int, zone string, message *ingester_client.LabelValuesCardinalityResponse) {
	cm.labelValuesCountersMx.Lock()
	defer cm.labelValuesCountersMx.Unlock()

	for _, item := range message.Items {

		// Create a new map for the label name if it doesn't exist
		if _, ok := cm.labelValuesCounters[item.LabelName]; !ok {
			cm.labelValuesCounters[item.LabelName] = make(map[string]countByReplicationSetAndZone, len(item.LabelValueSeries))
		}

		for labelValue, seriesCount := range item.LabelValueSeries {
			// Lazily init the label values counters.
			if _, ok := cm.labelValuesCounters[item.LabelName][labelValue]; !ok {
				cm.labelValuesCounters[item.LabelName][labelValue] = make(countByReplicationSetAndZone, cm.numReplicationSets)
			}

			countByZone := cm.labelValuesCounters[item.LabelName][labelValue][replicationSetIdx]
			if countByZone == nil {
				countByZone = map[string]uint64{}
				cm.labelValuesCounters[item.LabelName][labelValue][replicationSetIdx] = countByZone
			}

			// Accumulate the series count.
			countByZone[zone] += seriesCount
		}
	}
}

// toResponse adjust builds and returns LabelValuesCardinalityResponse containing the count of series by label name
// and value.
func (cm *labelValuesCardinalityConcurrentMap) toResponse(approximateFromZonesFunc func(countByZone map[string]uint64) uint64) *ingester_client.LabelValuesCardinalityResponse {
	// we need to acquire the lock to prevent concurrent read/write to the map
	cm.labelValuesCountersMx.Lock()
	defer cm.labelValuesCountersMx.Unlock()

	cardinalityItems := make([]*ingester_client.LabelValueSeriesCount, 0, len(cm.labelValuesCounters))

	for labelName, dataByLabelValues := range cm.labelValuesCounters {
		countByLabelValue := make(map[string]uint64, len(dataByLabelValues))

		// We accumulate the number of series tracked by label name-value pairs on a per replication set basis.
		// For each replication set, we approximate the actual number of series counted by ingesters, taking in
		// account the replication.
		for labelValue, dataByReplicationSet := range dataByLabelValues {
			total := uint64(0)

			for _, countByZone := range dataByReplicationSet {
				total += approximateFromZonesFunc(countByZone)
			}

			countByLabelValue[labelValue] = total
		}

		cardinalityItems = append(cardinalityItems, &ingester_client.LabelValueSeriesCount{
			LabelName:        labelName,
			LabelValueSeries: countByLabelValue,
		})
	}

	return &ingester_client.LabelValuesCardinalityResponse{
		Items: cardinalityItems,
	}
}

// ActiveSeries queries the ingester replication set for active series matching
// the given selector. It combines and deduplicates the results.
func (d *Distributor) ActiveSeries(ctx context.Context, matchers []*labels.Matcher) ([]labels.Labels, error) {
	res, err := d.deduplicateActiveSeries(ctx, matchers, false)
	if err != nil {
		return nil, err
	}

	deduplicatedSeries, fetchedSeries := res.result()

	reqStats := stats.FromContext(ctx)
	reqStats.AddFetchedSeries(fetchedSeries)

	return deduplicatedSeries, nil
}

func (d *Distributor) ActiveNativeHistogramMetrics(ctx context.Context, matchers []*labels.Matcher) (*cardinality.ActiveNativeHistogramMetricsResponse, error) {
	res, err := d.deduplicateActiveSeries(ctx, matchers, true)
	if err != nil {
		return nil, err
	}

	metrics, fetchedSeries := res.metricResult()

	reqStats := stats.FromContext(ctx)
	reqStats.AddFetchedSeries(fetchedSeries)

	return &cardinality.ActiveNativeHistogramMetricsResponse{Data: metrics}, nil
}

func (d *Distributor) deduplicateActiveSeries(ctx context.Context, matchers []*labels.Matcher, nativeHistograms bool) (*activeSeriesResponse, error) {
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx)
	if err != nil {
		return nil, err
	}

	// When ingest storage is disabled, if ingesters are running in a single zone we can't tolerate any errors.
	// In this case we expect exactly 1 replication set.
	if !d.cfg.IngestStorageConfig.Enabled && len(replicationSets) == 1 && replicationSets[0].ZoneCount() == 1 {
		replicationSets[0].MaxErrors = 0
	}

	req, err := ingester_client.ToActiveSeriesRequest(matchers)
	if err != nil {
		return nil, err
	}
	if nativeHistograms {
		req.Type = ingester_client.NATIVE_HISTOGRAM_SERIES
	}

	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	maxResponseSize := math.MaxInt
	if limit := d.limits.ActiveSeriesResultsMaxSizeBytes(tenantID); limit > 0 {
		maxResponseSize = limit
	}
	res := newActiveSeriesResponse(d.hashCollisionCount, maxResponseSize, !nativeHistograms)

	ingesterQuery := func(ctx context.Context, client ingester_client.IngesterClient) (any, error) {
		// This function is invoked purely for its side effects on the captured
		// activeSeriesResponse, its return value is never used.
		type ignored struct{}

		log, ctx := spanlogger.New(ctx, d.log, tracer, "Distributor.ActiveSeries.queryIngester")
		defer log.Finish()

		stream, err := client.ActiveSeries(ctx, req)
		if err != nil {
			if errors.Is(globalerror.WrapGRPCErrorWithContextError(ctx, err), context.Canceled) {
				return ignored{}, nil
			}
			level.Error(log).Log("msg", "error creating active series response stream", "err", err)
			log.SetError()
			return nil, err
		}

		defer func() {
			err = util.CloseAndExhaust[*ingester_client.ActiveSeriesResponse](stream)
			if err != nil && !errors.Is(globalerror.WrapGRPCErrorWithContextError(ctx, err), context.Canceled) {
				level.Warn(d.log).Log("msg", "error closing active series response stream", "err", err)
			}
		}()

		for {
			msg, err := stream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				if errors.Is(globalerror.WrapGRPCErrorWithContextError(ctx, err), context.Canceled) {
					return ignored{}, nil
				}
				level.Error(log).Log("msg", "error receiving active series response", "err", err)
				log.SetError()
				return nil, err
			}

			err = res.add(msg.Metric, msg.BucketCount)
			if err != nil {
				return nil, err
			}
		}

		return ignored{}, nil
	}

	_, err = forReplicationSets(ctx, d, replicationSets, ingesterQuery)
	if err != nil {
		return nil, err
	}

	return res, nil
}

type labelsWithBucketCount struct {
	labels.Labels
	bucketCount uint64
}

type entry struct {
	first      labelsWithBucketCount
	collisions []labelsWithBucketCount
}

// activeSeriesResponse is a helper to merge/deduplicate ActiveSeries responses from ingesters.
type activeSeriesResponse struct {
	ignoreBucketCount  bool
	m                  sync.Mutex
	series             map[uint64]entry
	builder            labels.ScratchBuilder
	lbls               labels.Labels
	hashCollisionCount prometheus.Counter

	buf     []byte
	size    int
	maxSize int
}

func newActiveSeriesResponse(hashCollisionCount prometheus.Counter, maxSize int, ignoreBucketCount bool) *activeSeriesResponse {
	return &activeSeriesResponse{
		ignoreBucketCount:  ignoreBucketCount,
		series:             map[uint64]entry{},
		builder:            labels.NewScratchBuilder(40),
		hashCollisionCount: hashCollisionCount,
		maxSize:            maxSize,
	}
}

var ErrResponseTooLarge = errors.New("response too large")

func (r *activeSeriesResponse) add(series []*mimirpb.Metric, bucketCounts []uint64) error {

	r.m.Lock()
	defer r.m.Unlock()

	for i, metric := range series {
		mimirpb.FromLabelAdaptersOverwriteLabels(&r.builder, metric.Labels, &r.lbls)
		lblHash := r.lbls.Hash()
		if e, ok := r.series[lblHash]; !ok {
			l := r.lbls.Copy()

			r.buf = l.Bytes(r.buf)
			r.size += len(r.buf)
			if r.size > r.maxSize {
				return ErrResponseTooLarge
			}
			if r.ignoreBucketCount {
				r.series[lblHash] = entry{first: labelsWithBucketCount{labels.Labels(l), 0}}
			} else {
				r.series[lblHash] = entry{first: labelsWithBucketCount{labels.Labels(l), bucketCounts[i]}}
			}
		} else {
			// A series with this hash is already present in the result set, we need to
			// detect potential hash collisions by comparing the labels of the candidate to
			// the labels in the result set and add the candidate if it's not present.
			present := labels.Equal(e.first.Labels, r.lbls)
			for _, lbls := range e.collisions {
				if present {
					break
				}
				present = labels.Equal(lbls.Labels, r.lbls)
			}

			if !present {
				l := r.lbls.Copy()

				r.buf = l.Bytes(r.buf)
				r.size += len(r.buf)
				if r.size > r.maxSize {
					return ErrResponseTooLarge
				}
				if r.ignoreBucketCount {
					e.collisions = append(e.collisions, labelsWithBucketCount{labels.Labels(l), 0})
				} else {
					e.collisions = append(e.collisions, labelsWithBucketCount{labels.Labels(l), bucketCounts[i]})
				}
				r.series[lblHash] = e
				r.hashCollisionCount.Inc()
			}
		}
	}

	return nil
}

func (r *activeSeriesResponse) result() ([]labels.Labels, uint64) {
	r.m.Lock()
	defer r.m.Unlock()

	result := make([]labels.Labels, 0, len(r.series))
	for _, series := range r.series {
		result = append(result, series.first.Labels)
		for _, collision := range series.collisions {
			result = append(result, collision.Labels)
		}
	}
	return result, uint64(len(result))
}

type metricBucketCounts struct {
	SeriesCount    uint64
	BucketCount    uint64
	MaxBucketCount uint64
	MinBucketCount uint64
}

// Aggregate native histogram bucket counts on metric level.
func (r *activeSeriesResponse) metricResult() ([]cardinality.ActiveMetricWithBucketCount, uint64) {
	metrics, fetchedSeries := r.getMetricCounts()
	result := make([]cardinality.ActiveMetricWithBucketCount, 0, len(metrics))
	for name, metric := range metrics {
		result = append(result, cardinality.ActiveMetricWithBucketCount{
			Metric:         name,
			SeriesCount:    metric.SeriesCount,
			BucketCount:    metric.BucketCount,
			MaxBucketCount: metric.MaxBucketCount,
			MinBucketCount: metric.MinBucketCount,
			AvgBucketCount: float64(metric.BucketCount) / float64(metric.SeriesCount),
		})
	}
	slices.SortFunc(result, func(a, b cardinality.ActiveMetricWithBucketCount) int {
		return strings.Compare(a.Metric, b.Metric)
	})
	return result, fetchedSeries
}

func (r *activeSeriesResponse) getMetricCounts() (map[string]*metricBucketCounts, uint64) {
	r.m.Lock()
	defer r.m.Unlock()

	fetchedSeries := uint64(0)
	metrics := map[string]*metricBucketCounts{}
	for _, series := range r.series {
		fetchedSeries += 1 + uint64(len(series.collisions))
		updateMetricCounts(metrics, series.first)
		for _, collision := range series.collisions {
			updateMetricCounts(metrics, collision)
		}
	}
	return metrics, fetchedSeries
}

func updateMetricCounts(metrics map[string]*metricBucketCounts, series labelsWithBucketCount) {
	bucketCount := series.bucketCount
	if m, ok := metrics[series.Get(model.MetricNameLabel)]; ok {
		m.SeriesCount++
		m.BucketCount += bucketCount
		if m.MaxBucketCount < bucketCount {
			m.MaxBucketCount = bucketCount
		}
		if m.MinBucketCount > bucketCount {
			m.MinBucketCount = bucketCount
		}
	} else {
		metrics[series.Get(model.MetricNameLabel)] = &metricBucketCounts{
			SeriesCount:    1,
			BucketCount:    bucketCount,
			MaxBucketCount: bucketCount,
			MinBucketCount: bucketCount,
		}
	}
}

// approximateFromZones computes a zonal value while factoring in replication;
// e.g. series cardinality or ingestion rate.
//
// If Mimir isn't deployed in a multi-zone configuration, approximateFromZones
// divides the sum of all values by the replication factor to come up with an approximation.
func approximateFromZones[T ~float64 | ~uint64](zonesTotal, replicationFactor int, seriesCountByZone map[string]T) T {
	// If we have more than one zone, we return the max value across zones.
	// Values can be different across zones due to incomplete replication or
	// other issues. Any inconsistency should always be an underestimation of
	// the real value, so we take the max to get the best available
	// approximation.
	if zonesTotal > 1 {
		val := maxFromZones(seriesCountByZone)
		if zonesTotal <= replicationFactor {
			return val
		}
		// When the number of zones is larger than the replication factor, we factor
		// that into the approximation. We multiply the max of all zones to a ratio
		// of number of zones to the replication factor to approximate how series
		// are spread across zones.
		return T(math.Round(float64(val) * float64(zonesTotal) / float64(replicationFactor)))
	}

	// If we have a single zone or number of zones is larger than RF, we can't return
	// the value directly. The series will be replicated randomly across ingesters, and there's no way
	// here to know how many unique series really exist. In this case, dividing by the replication factor
	// will give us an approximation of the real value.
	var sum T
	for _, seriesCount := range seriesCountByZone {
		sum += seriesCount
	}
	return T(math.Round(float64(sum) / float64(replicationFactor)))
}

func maxFromZones[T ~float64 | ~uint64](seriesCountByZone map[string]T) (val T) {
	for _, seriesCount := range seriesCountByZone {
		if seriesCount > val {
			val = seriesCount
		}
	}
	return val
}

// LabelNames returns the names of all labels from series with samples timestamp between from and to, and matching
// the input optional series label matchers. The returned label names are sorted.
func (d *Distributor) LabelNames(ctx context.Context, from, to model.Time, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, error) {
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx)
	if err != nil {
		return nil, err
	}

	req, err := ingester_client.ToLabelNamesRequest(from, to, hints, matchers)
	if err != nil {
		return nil, err
	}

	resps, err := forReplicationSets(ctx, d, replicationSets, func(ctx context.Context, client ingester_client.IngesterClient) (*ingester_client.LabelNamesResponse, error) {
		return client.LabelNames(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	valueSet := map[string]struct{}{}
	for _, resp := range resps {
		for _, v := range resp.LabelNames {
			valueSet[v] = struct{}{}
		}
	}

	values := make([]string, 0, len(valueSet))
	for v := range valueSet {
		values = append(values, v)
	}

	slices.Sort(values)

	if hints != nil && hints.Limit > 0 && len(values) > hints.Limit {
		values = values[:hints.Limit]
	}

	return values, nil
}

// MetricsForLabelMatchers returns a list of series with samples timestamps between from and through, and series labels
// matching the optional label matchers. The returned series are not sorted.
func (d *Distributor) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, hints *storage.SelectHints, matchers ...*labels.Matcher) ([]labels.Labels, error) {
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx)
	if err != nil {
		return nil, err
	}

	req, err := ingester_client.ToMetricsForLabelMatchersRequest(from, through, hints, matchers)
	if err != nil {
		return nil, err
	}

	resultLimit := math.MaxInt
	if hints != nil && hints.Limit > 0 {
		resultLimit = hints.Limit

		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return nil, err
		}

		// Adjust the limit passed with the downstream request to ingesters with respect to how series are sharded.
		req.Limit = int64(d.adjustQueryRequestLimit(ctx, userID, resultLimit))
	}

	resps, err := forReplicationSets(ctx, d, replicationSets, func(ctx context.Context, client ingester_client.IngesterClient) (*ingester_client.MetricsForLabelMatchersResponse, error) {
		return client.MetricsForLabelMatchers(ctx, req)
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, resp := range resps {
			resp.FreeBuffer()
		}
	}()

	metrics := map[uint64]labels.Labels{}
respsLoop:
	for _, resp := range resps {
		ms := ingester_client.FromMetricsForLabelMatchersResponse(resp)
		for _, m := range ms {
			if len(metrics) >= resultLimit {
				break respsLoop
			}
			metrics[labels.StableHash(m)] = m
		}
	}

	queryLimiter := mimir_limiter.QueryLimiterFromContextWithFallback(ctx)
	deduplicator, err := mimir_limiter.SeriesLabelsDeduplicatorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	tracker, err := mimir_limiter.MemoryConsumptionTrackerFromContext(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]labels.Labels, 0, len(metrics))
	for _, m := range metrics {
		uniqueSeries, err := deduplicator.Deduplicate(m, tracker)
		if err != nil {
			return nil, err
		}
		if err := queryLimiter.AddSeries(uniqueSeries); err != nil {
			return nil, err
		}

		result = append(result, uniqueSeries)
	}
	return result, nil
}

// adjustQueryRequestLimit recalculated the query request limit.
// The returned value is the approximation, a query to an individual shard needs to be limited with.
func (d *Distributor) adjustQueryRequestLimit(ctx context.Context, userID string, limit int) int {
	if limit == 0 {
		return limit
	}

	var shardSize int
	if d.cfg.IngestStorageConfig.Enabled {
		// Get the number of active partitions in the ring. Here the ShuffleShardSize handles cases when a tenant has 0 or negative
		// number of shards, or more shards than the number of active partitions in the ring.
		shardSize = d.partitionsRing.PartitionRing().ShuffleShardSize(d.limits.IngestionPartitionsTenantShardSize(userID))
	} else {
		// The ShuffleShard filters out read-only instances, leaving us with the number of active ingesters.
		// Note, this can be costly to compute if the ring's caching is not enabled.
		subring := d.ingestersRing.ShuffleShard(userID, d.limits.IngestionTenantShardSize(userID))
		ingestionShardSize := subring.InstancesWithTokensCount()
		// In classic Mimir the ingestion shard size is the number of ingesters in the shard across all zones.
		shardSize = int(float64(ingestionShardSize) / float64(subring.ReplicationFactor()))
	}
	if shardSize == 0 || limit <= shardSize {
		return limit
	}

	// Always round the adjusted limit up so the approximation over-counted, rather under-counted.
	newLimit := int(math.Ceil(float64(limit) / float64(shardSize)))

	spanLog := spanlogger.FromContext(ctx, d.log)
	spanLog.DebugLog(
		"msg", "the limit of query is adjusted to account for sharding",
		"original", limit,
		"updated", newLimit,
		"shard_size (before replication)", shardSize,
	)

	return newLimit
}

// MetricsMetadata returns the metrics metadata based on the provided req.
func (d *Distributor) MetricsMetadata(ctx context.Context, req *ingester_client.MetricsMetadataRequest) ([]scrape.MetricMetadata, error) {
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx)
	if err != nil {
		return nil, err
	}

	resps, err := forReplicationSets(ctx, d, replicationSets, func(ctx context.Context, client ingester_client.IngesterClient) (*ingester_client.MetricsMetadataResponse, error) {
		return client.MetricsMetadata(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	result := []scrape.MetricMetadata{}
	dedupTracker := map[mimirpb.MetricMetadata]struct{}{}
	for _, r := range resps {
		for _, m := range r.Metadata {
			// Given we look across all ingesters - dedup the metadata.
			_, ok := dedupTracker[*m]
			if ok {
				continue
			}
			dedupTracker[*m] = struct{}{}

			result = append(result, scrape.MetricMetadata{
				MetricFamily: m.MetricFamilyName,
				Help:         m.Help,
				Unit:         m.Unit,
				Type:         mimirpb.MetricMetadataMetricTypeToMetricType(m.GetType()),
			})
		}
	}

	return result, nil
}

// UserStats returns statistics about the current user.
func (d *Distributor) UserStats(ctx context.Context, countMethod cardinality.CountMethod) (*UserStats, error) {
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx)
	if err != nil {
		return nil, err
	}

	type zonedUserStatsResponse struct {
		zone string
		resp *ingester_client.UserStatsResponse
	}

	// When ingest storage is disabled, if ingesters are running in a single zone we can't tolerate any errors.
	// In this case we expect exactly 1 replication set.
	if !d.cfg.IngestStorageConfig.Enabled && len(replicationSets) == 1 && replicationSets[0].ZoneCount() == 1 {
		replicationSets[0].MaxErrors = 0
	}

	ingesterCountMethod, err := toIngesterCountMethod(countMethod)
	if err != nil {
		return nil, err
	}

	var (
		req                       = &ingester_client.UserStatsRequest{CountMethod: ingesterCountMethod}
		quorumConfig              = d.queryQuorumConfigForReplicationSets(ctx, replicationSets)
		responsesByReplicationSet = make([][]zonedUserStatsResponse, len(replicationSets))
	)

	// Fetch user stats from each ingester and collect responses by ReplicationSet.
	//
	// When ingest storage is enabled we expect 1 ReplicationSet for each partition. A series is sharded only to 1
	// partition and the number of successful responses for each partition (ReplicationSet) may be different when
	// ingesters request minimization is disabled. For this reason, we collect responses by ReplicationSet, so that
	// we can later estimate the number of series with a higher accuracy.
	err = concurrency.ForEachJob(ctx, len(replicationSets), 0, func(ctx context.Context, replicationSetIdx int) error {
		replicationSet := replicationSets[replicationSetIdx]

		resps, err := ring.DoUntilQuorum[zonedUserStatsResponse](ctx, replicationSet, quorumConfig, func(ctx context.Context, desc *ring.InstanceDesc) (zonedUserStatsResponse, error) {
			poolClient, err := d.ingesterPool.GetClientForInstance(*desc)
			if err != nil {
				return zonedUserStatsResponse{}, err
			}

			client := poolClient.(ingester_client.IngesterClient)
			resp, err := client.UserStats(ctx, req)
			if err != nil {
				return zonedUserStatsResponse{}, err
			}

			return zonedUserStatsResponse{zone: desc.Zone, resp: resp}, nil
		}, func(zonedUserStatsResponse) {})

		if err != nil {
			return err
		}

		// Collect the response. No need to lock around responsesByReplicationSetMx access because each goroutine
		// accesses a different index.
		responsesByReplicationSet[replicationSetIdx] = resps

		return nil
	})

	if err != nil {
		return nil, err
	}

	totalStats := &UserStats{}

	// If we reach this point it's guaranteed no error occurred in concurrency.ForEachJob() and so all jobs have been
	// processed and there are no more goroutines accessing responsesByReplicationSet.
	for replicationSetIdx, resps := range responsesByReplicationSet {
		var (
			replicationSet        = replicationSets[replicationSetIdx]
			zoneIngestionRate     = map[string]float64{}
			zoneAPIIngestionRate  = map[string]float64{}
			zoneRuleIngestionRate = map[string]float64{}
			zoneNumSeries         = map[string]uint64{}
		)

		// Collect responses by zone.
		for _, r := range resps {
			zoneIngestionRate[r.zone] += r.resp.IngestionRate
			zoneAPIIngestionRate[r.zone] += r.resp.ApiIngestionRate
			zoneRuleIngestionRate[r.zone] += r.resp.RuleIngestionRate
			zoneNumSeries[r.zone] += r.resp.NumSeries
		}

		// When the ingest storage is enabled, a partition is owned by only 1 ingester per zone.
		// So we always approximate the resulting stats as max of what a single zone has.
		if d.cfg.IngestStorageConfig.Enabled {
			totalStats.IngestionRate += maxFromZones(zoneIngestionRate)
			totalStats.APIIngestionRate += maxFromZones(zoneAPIIngestionRate)
			totalStats.RuleIngestionRate += maxFromZones(zoneRuleIngestionRate)
			totalStats.NumSeries += maxFromZones(zoneNumSeries)
		} else {
			totalStats.IngestionRate += approximateFromZones(replicationSet.ZoneCount(), d.ingestersRing.ReplicationFactor(), zoneIngestionRate)
			totalStats.APIIngestionRate += approximateFromZones(replicationSet.ZoneCount(), d.ingestersRing.ReplicationFactor(), zoneAPIIngestionRate)
			totalStats.RuleIngestionRate += approximateFromZones(replicationSet.ZoneCount(), d.ingestersRing.ReplicationFactor(), zoneRuleIngestionRate)
			totalStats.NumSeries += approximateFromZones(replicationSet.ZoneCount(), d.ingestersRing.ReplicationFactor(), zoneNumSeries)
		}
	}

	return totalStats, nil
}

// UserIDStats models ingestion statistics for one user, including the user ID
type UserIDStats struct {
	UserID string `json:"userID"`
	UserStats
}

// AllUserStats returns statistics about all users.
// Note it does not divide by the ReplicationFactor like UserStats()
func (d *Distributor) AllUserStats(ctx context.Context) ([]UserIDStats, error) {
	// Add up by user, across all responses from ingesters
	perUserTotals := make(map[string]UserStats)

	req := &ingester_client.UserStatsRequest{}
	ctx = user.InjectOrgID(ctx, "1") // fake: ingester insists on having an org ID
	// Not using d.forReplicationSet(), so we can fail after first error.
	replicationSet, err := d.ingestersRing.GetAllHealthy(readNoExtend)
	if err != nil {
		return nil, err
	}
	for _, ingester := range replicationSet.Instances {
		client, err := d.ingesterPool.GetClientForInstance(ingester)
		if err != nil {
			return nil, err
		}
		resp, err := client.(ingester_client.IngesterClient).AllUserStats(ctx, req)
		if err != nil {
			return nil, err
		}
		for _, u := range resp.Stats {
			s := perUserTotals[u.UserId]
			s.IngestionRate += u.Data.IngestionRate
			s.APIIngestionRate += u.Data.ApiIngestionRate
			s.RuleIngestionRate += u.Data.RuleIngestionRate
			s.NumSeries += u.Data.NumSeries
			perUserTotals[u.UserId] = s
		}
	}

	// Turn aggregated map into a slice for return
	response := make([]UserIDStats, 0, len(perUserTotals))
	for id, stats := range perUserTotals {
		response = append(response, UserIDStats{
			UserID: id,
			UserStats: UserStats{
				IngestionRate:     stats.IngestionRate,
				APIIngestionRate:  stats.APIIngestionRate,
				RuleIngestionRate: stats.RuleIngestionRate,
				NumSeries:         stats.NumSeries,
			},
		})
	}

	return response, nil
}

func (d *Distributor) getInstanceLimits() InstanceLimits {
	if d.cfg.InstanceLimitsFn == nil {
		return d.cfg.DefaultLimits
	}

	l := d.cfg.InstanceLimitsFn()
	if l == nil {
		return d.cfg.DefaultLimits
	}

	return *l
}

func (d *Distributor) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if d.distributorsRing != nil {
		d.distributorsRing.ServeHTTP(w, req)
	} else {
		ringNotEnabledPage := `
			<!DOCTYPE html>
			<html>
				<head>
					<meta charset="UTF-8">
					<title>Distributor Status</title>
				</head>
				<body>
					<h1>Distributor Status</h1>
					<p>Distributor is not running with global limits enabled</p>
				</body>
			</html>`
		util.WriteHTMLResponse(w, ringNotEnabledPage)
	}
}

// HealthyInstancesCount implements the ReadLifecycler interface
//
// We use a ring lifecycler delegate to count the number of members of the
// ring. The count is then used to enforce rate limiting correctly for each
// distributor. $EFFECTIVE_RATE_LIMIT = $GLOBAL_RATE_LIMIT / $NUM_INSTANCES
func (d *Distributor) HealthyInstancesCount() int {
	return int(d.healthyInstancesCount.Load())
}
