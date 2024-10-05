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
	"sort"
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
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/scrape"
	"go.uber.org/atomic"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/costattribution"
	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/globalerror"
	mimir_limiter "github.com/grafana/mimir/pkg/util/limiter"
	util_math "github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/pool"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

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

	// ringAutoForgetUnhealthyPeriods is how many consecutive timeout periods an unhealthy instance
	// in the ring will be automatically removed after.
	ringAutoForgetUnhealthyPeriods = 10

	// metaLabelTenantID is the name of the metric_relabel_configs label with tenant ID.
	metaLabelTenantID = model.MetaLabelPrefix + "tenant_id"

	maxOTLPRequestSizeFlag = "distributor.max-otlp-request-size"

	instanceIngestionRateTickInterval = time.Second

	// Size of "slab" when using pooled buffers for marshaling write requests. When handling single Push request
	// buffers for multiple write requests sent to ingesters will be allocated from single "slab", if there is enough space.
	writeRequestSlabPoolSize = 512 * 1024
)

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
	costAttributionMng     costattribution.Manager
	// For handling HA replicas.
	HATracker *haTracker

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
	incomingRequests                 *prometheus.CounterVec
	incomingSamples                  *prometheus.CounterVec
	incomingExemplars                *prometheus.CounterVec
	incomingMetadata                 *prometheus.CounterVec
	nonHASamples                     *prometheus.CounterVec
	dedupedSamples                   *prometheus.CounterVec
	labelsHistogram                  prometheus.Histogram
	incomingSamplesPerRequest        *prometheus.HistogramVec
	incomingExemplarsPerRequest      *prometheus.HistogramVec
	latestSeenSampleTimestampPerUser *prometheus.GaugeVec
	labelValuesWithNewlinesPerUser   *prometheus.CounterVec
	hashCollisionCount               prometheus.Counter

	// Metrics for data rejected for hitting per-tenant limits
	discardedSamplesTooManyHaClusters *prometheus.CounterVec
	discardedSamplesRateLimited       *prometheus.CounterVec
	discardedRequestsRateLimited      *prometheus.CounterVec
	discardedExemplarsRateLimited     *prometheus.CounterVec
	discardedMetadataRateLimited      *prometheus.CounterVec

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
}

// Config contains the configuration required to
// create a Distributor
type Config struct {
	PoolConfig PoolConfig `yaml:"pool"`

	RetryConfig     RetryConfig     `yaml:"retry_after_header"`
	HATrackerConfig HATrackerConfig `yaml:"ha_tracker"`

	MaxRecvMsgSize           int           `yaml:"max_recv_msg_size" category:"advanced"`
	MaxOTLPRequestSize       int           `yaml:"max_otlp_request_size" category:"experimental"`
	MaxRequestPoolBufferSize int           `yaml:"max_request_pool_buffer_size" category:"experimental"`
	RemoteTimeout            time.Duration `yaml:"remote_timeout" category:"advanced"`

	// Distributors ring
	DistributorRing RingConfig `yaml:"ring"`

	// for testing and for extending the ingester by adding calls to the client
	IngesterClientFactory ring_client.PoolFactory `yaml:"-"`

	// when true the distributor does not validate the label name, Mimir doesn't directly use
	// this (and should never use it) but this feature is used by other projects built on top of it
	SkipLabelNameValidation bool `yaml:"-"`

	// This config is dynamically injected because it is defined in the querier config.
	ShuffleShardingLookbackPeriod              time.Duration `yaml:"-"`
	StreamingChunksPerIngesterSeriesBufferSize uint64        `yaml:"-"`
	MinimizeIngesterRequests                   bool          `yaml:"-"`
	MinimiseIngesterRequestsHedgingDelay       time.Duration `yaml:"-"`
	PreferAvailabilityZone                     string        `yaml:"-"`

	// IngestStorageConfig is dynamically injected because defined outside of distributor config.
	IngestStorageConfig ingest.Config `yaml:"-"`

	// Limits for distributor
	DefaultLimits    InstanceLimits         `yaml:"instance_limits"`
	InstanceLimitsFn func() *InstanceLimits `yaml:"-"`

	// This allows downstream projects to wrap the distributor push function
	// and access the deserialized write requests before/after they are pushed.
	// These functions will only receive samples that don't get dropped by HA deduplication.
	PushWrappers []PushWrapper `yaml:"-"`

	WriteRequestsBufferPoolingEnabled bool `yaml:"write_requests_buffer_pooling_enabled" category:"experimental"`
	ReusableIngesterPushWorkers       int  `yaml:"reusable_ingester_push_workers" category:"advanced"`

	// DirectOTLPTranslationEnabled allows reverting to the older way of translating from OTLP write requests via Prometheus, in case of problems.
	DirectOTLPTranslationEnabled bool `yaml:"direct_otlp_translation_enabled" category:"experimental"`
}

// PushWrapper wraps around a push. It is similar to middleware.Interface.
type PushWrapper func(next PushFunc) PushFunc

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	cfg.PoolConfig.RegisterFlags(f)
	cfg.HATrackerConfig.RegisterFlags(f)
	cfg.DistributorRing.RegisterFlags(f, logger)
	cfg.RetryConfig.RegisterFlags(f)

	f.IntVar(&cfg.MaxRecvMsgSize, "distributor.max-recv-msg-size", 100<<20, "Max message size in bytes that the distributors will accept for incoming push requests to the remote write API. If exceeded, the request will be rejected.")
	f.IntVar(&cfg.MaxOTLPRequestSize, maxOTLPRequestSizeFlag, 100<<20, "Maximum OTLP request size in bytes that the distributors accept. Requests exceeding this limit are rejected.")
	f.IntVar(&cfg.MaxRequestPoolBufferSize, "distributor.max-request-pool-buffer-size", 0, "Max size of the pooled buffers used for marshaling write requests. If 0, no max size is enforced.")
	f.DurationVar(&cfg.RemoteTimeout, "distributor.remote-timeout", 2*time.Second, "Timeout for downstream ingesters.")
	f.BoolVar(&cfg.WriteRequestsBufferPoolingEnabled, "distributor.write-requests-buffer-pooling-enabled", true, "Enable pooling of buffers used for marshaling write requests.")
	f.IntVar(&cfg.ReusableIngesterPushWorkers, "distributor.reusable-ingester-push-workers", 2000, "Number of pre-allocated workers used to forward push requests to the ingesters. If 0, no workers will be used and a new goroutine will be spawned for each ingester push request. If not enough workers available, new goroutine will be spawned. (Note: this is a performance optimization, not a limiting feature.)")
	f.BoolVar(&cfg.DirectOTLPTranslationEnabled, "distributor.direct-otlp-translation-enabled", true, "When enabled, OTLP write requests are directly translated to Mimir equivalents, for optimum performance.")

	cfg.DefaultLimits.RegisterFlags(f)
}

// Validate config and returns error on failure
func (cfg *Config) Validate(limits validation.Limits) error {
	if limits.IngestionTenantShardSize < 0 {
		return errInvalidTenantShardSize
	}

	if err := cfg.HATrackerConfig.Validate(); err != nil {
		return err
	}
	return cfg.RetryConfig.Validate()
}

const (
	instanceLimitsMetric     = "cortex_distributor_instance_limits"
	instanceLimitsMetricHelp = "Instance limits used by this distributor." // Must be same for all registrations.
	limitLabel               = "limit"
)

type PushMetrics struct {
	otlpRequestCounter   *prometheus.CounterVec
	uncompressedBodySize *prometheus.HistogramVec
}

func newPushMetrics(reg prometheus.Registerer) *PushMetrics {
	return &PushMetrics{
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
		}, []string{"user"}),
	}
}

func (m *PushMetrics) IncOTLPRequest(user string) {
	if m != nil {
		m.otlpRequestCounter.WithLabelValues(user).Inc()
	}
}

func (m *PushMetrics) ObserveUncompressedBodySize(user string, size float64) {
	if m != nil {
		m.uncompressedBodySize.WithLabelValues(user).Observe(size)
	}
}

func (m *PushMetrics) deleteUserMetrics(user string) {
	m.otlpRequestCounter.DeleteLabelValues(user)
	m.uncompressedBodySize.DeleteLabelValues(user)
}

// New constructs a new Distributor
func New(cfg Config, clientConfig ingester_client.Config, limits *validation.Overrides, activeGroupsCleanupService *util.ActiveGroupsCleanupService, costAttributionMng costattribution.Manager, ingestersRing ring.ReadRing, partitionsRing *ring.PartitionInstanceRing, canJoinDistributorsRing bool, reg prometheus.Registerer, log log.Logger) (*Distributor, error) {
	clientMetrics := ingester_client.NewMetrics(reg)
	if cfg.IngesterClientFactory == nil {
		cfg.IngesterClientFactory = ring_client.PoolInstFunc(func(inst ring.InstanceDesc) (ring_client.PoolClient, error) {
			return ingester_client.MakeIngesterClient(inst, clientConfig, clientMetrics)
		})
	}

	cfg.PoolConfig.RemoteTimeout = cfg.RemoteTimeout

	haTracker, err := newHATracker(cfg.HATrackerConfig, limits, reg, log)
	if err != nil {
		return nil, err
	}

	subservices := []services.Service(nil)
	subservices = append(subservices, haTracker)

	var requestBufferPool util.Pool
	if cfg.MaxRequestPoolBufferSize > 0 {
		requestBufferPool = util.NewBucketedBufferPool(1<<10, cfg.MaxRequestPoolBufferSize, 4)
	} else {
		requestBufferPool = util.NewBufferPool()
	}

	d := &Distributor{
		cfg:                   cfg,
		log:                   log,
		ingestersRing:         ingestersRing,
		RequestBufferPool:     requestBufferPool,
		partitionsRing:        partitionsRing,
		ingesterPool:          NewPool(cfg.PoolConfig, ingestersRing, cfg.IngesterClientFactory, log),
		healthyInstancesCount: atomic.NewUint32(0),
		limits:                limits,
		HATracker:             haTracker,
		costAttributionMng:    costAttributionMng,
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
			Help: "The total number of received samples, excluding rejected and deduped samples.",
		}, []string{"user"}),
		receivedExemplars: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_received_exemplars_total",
			Help: "The total number of received exemplars, excluding rejected and deduped exemplars.",
		}, []string{"user"}),
		receivedMetadata: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_received_metadata_total",
			Help: "The total number of received metadata, excluding rejected.",
		}, []string{"user"}),
		incomingRequests: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_requests_in_total",
			Help: "The total number of requests that have come in to the distributor, including rejected or deduped requests.",
		}, []string{"user"}),
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
		labelValuesWithNewlinesPerUser: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_label_values_with_newlines_total",
			Help: "Total number of label values with newlines seen at ingestion time.",
		}, []string{"user"}),

		discardedSamplesTooManyHaClusters: validation.DiscardedSamplesCounter(reg, reasonTooManyHAClusters),
		discardedSamplesRateLimited:       validation.DiscardedSamplesCounter(reg, reasonRateLimited),
		discardedRequestsRateLimited:      validation.DiscardedRequestsCounter(reg, reasonRateLimited),
		discardedExemplarsRateLimited:     validation.DiscardedExemplarsCounter(reg, reasonRateLimited),
		discardedMetadataRateLimited:      validation.DiscardedMetadataCounter(reg, reasonRateLimited),

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

	d.requestRateLimiter = limiter.NewRateLimiter(requestRateStrategy, 10*time.Second)
	d.ingestionRateLimiter = limiter.NewRateLimiter(ingestionRateStrategy, 10*time.Second)
	d.distributorsLifecycler = distributorsLifecycler
	d.distributorsRing = distributorsRing

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
	delegate = ring.NewAutoForgetDelegate(ringAutoForgetUnhealthyPeriods*cfg.Common.HeartbeatTimeout, delegate, logger)

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
	d.incomingRequests.DeleteLabelValues(userID)
	d.incomingSamples.DeleteLabelValues(userID)
	d.incomingExemplars.DeleteLabelValues(userID)
	d.incomingMetadata.DeleteLabelValues(userID)
	d.incomingSamplesPerRequest.DeleteLabelValues(userID)
	d.incomingExemplarsPerRequest.DeleteLabelValues(userID)
	d.nonHASamples.DeleteLabelValues(userID)
	d.latestSeenSampleTimestampPerUser.DeleteLabelValues(userID)
	d.labelValuesWithNewlinesPerUser.DeleteLabelValues(userID)

	d.PushMetrics.deleteUserMetrics(userID)

	filter := prometheus.Labels{"user": userID}
	d.dedupedSamples.DeletePartialMatch(filter)
	d.discardedSamplesTooManyHaClusters.DeletePartialMatch(filter)
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
func (d *Distributor) checkSample(ctx context.Context, userID, cluster, replica string) (removeReplicaLabel bool, _ error) {
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
	err := d.HATracker.checkReplica(ctx, userID, cluster, replica, time.Now())
	// checkReplica would have returned an error if there was a real error talking to Consul,
	// or if the replica is not the currently elected replica.
	if err != nil { // Don't accept the sample.
		return false, err
	}
	return true, nil
}

// Validates a single series from a write request.
// May alter timeseries data in-place.
// The returned error may retain the series labels.
// It uses the passed nowt time to observe the delay of sample timestamps.
func (d *Distributor) validateSeries(nowt time.Time, ts *mimirpb.PreallocTimeseries, userID, group string, skipLabelNameValidation bool, minExemplarTS, maxExemplarTS int64) error {
	if err := validateLabels(d.sampleValidationMetrics, d.limits, userID, group, ts.Labels, skipLabelNameValidation); err != nil {
		return err
	}

	now := model.TimeFromUnixNano(nowt.UnixNano())

	for _, s := range ts.Samples {
		if err := validateSample(d.sampleValidationMetrics, now, d.limits, userID, group, ts.Labels, s); err != nil {
			return err
		}
	}

	histogramsUpdated := false
	for i := range ts.Histograms {
		updated, err := validateSampleHistogram(d.sampleValidationMetrics, now, d.limits, userID, group, ts.Labels, &ts.Histograms[i])
		if err != nil {
			return err
		}
		histogramsUpdated = histogramsUpdated || updated
	}
	if histogramsUpdated {
		ts.HistogramsUpdated()
	}

	if d.limits.MaxGlobalExemplarsPerUser(userID) == 0 {
		ts.ClearExemplars()
		return nil
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
	return nil
}
func (d *Distributor) labelValuesWithNewlines(labels []mimirpb.LabelAdapter) int {
	count := 0
	for _, l := range labels {
		if strings.IndexByte(l.Value, '\n') >= 0 {
			count++
		}
	}
	return count
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
	middlewares = append(middlewares, d.cfg.PushWrappers...)

	for ix := len(middlewares) - 1; ix >= 0; ix-- {
		next = middlewares[ix](next)
	}

	return next
}

func (d *Distributor) prePushHaDedupeMiddleware(next PushFunc) PushFunc {
	return func(ctx context.Context, pushReq *Request) error {
		next, maybeCleanup := NextOrCleanup(next, pushReq)
		defer maybeCleanup()

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

		span := opentracing.SpanFromContext(ctx)
		if span != nil {
			span.SetTag("cluster", cluster)
			span.SetTag("replica", replica)
		}

		numSamples := 0
		group := d.activeGroups.UpdateActiveGroupTimestamp(userID, validation.GroupLabel(d.limits, userID, req.Timeseries), time.Now())
		for _, ts := range req.Timeseries {
			numSamples += len(ts.Samples) + len(ts.Histograms)
		}

		removeReplica, err := d.checkSample(ctx, userID, cluster, replica)
		if err != nil {
			if errors.As(err, &replicasDidNotMatchError{}) {
				// These samples have been deduped.
				d.dedupedSamples.WithLabelValues(userID, cluster).Add(float64(numSamples))
			}

			if errors.As(err, &tooManyClustersError{}) {
				d.discardedSamplesTooManyHaClusters.WithLabelValues(userID, group).Add(float64(numSamples))
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
	}
}

func (d *Distributor) prePushRelabelMiddleware(next PushFunc) PushFunc {
	return func(ctx context.Context, pushReq *Request) error {
		next, maybeCleanup := NextOrCleanup(next, pushReq)
		defer maybeCleanup()

		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return err
		}

		if !d.limits.MetricRelabelingEnabled(userID) {
			return next(ctx, pushReq)
		}

		req, err := pushReq.WriteRequest()
		if err != nil {
			return err
		}

		var removeTsIndexes []int
		lb := labels.NewBuilder(labels.EmptyLabels())
		for tsIdx := 0; tsIdx < len(req.Timeseries); tsIdx++ {
			ts := req.Timeseries[tsIdx]

			if mrc := d.limits.MetricRelabelConfigs(userID); len(mrc) > 0 {
				mimirpb.FromLabelAdaptersToBuilder(ts.Labels, lb)
				lb.Set(metaLabelTenantID, userID)
				keep := relabel.ProcessBuilder(lb, mrc...)
				if !keep {
					removeTsIndexes = append(removeTsIndexes, tsIdx)
					continue
				}
				lb.Del(metaLabelTenantID)
				req.Timeseries[tsIdx].SetLabels(mimirpb.FromBuilderToLabelAdapters(lb, ts.Labels))
			}

			for _, labelName := range d.limits.DropLabels(userID) {
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
	}
}

// prePushSortAndFilterMiddleware is responsible for sorting labels and
// filtering empty values. This is a protection mechanism for ingesters.
func (d *Distributor) prePushSortAndFilterMiddleware(next PushFunc) PushFunc {
	return func(ctx context.Context, pushReq *Request) error {
		next, maybeCleanup := NextOrCleanup(next, pushReq)
		defer maybeCleanup()

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
	}
}

func (d *Distributor) prePushValidationMiddleware(next PushFunc) PushFunc {
	return func(ctx context.Context, pushReq *Request) error {
		next, maybeCleanup := NextOrCleanup(next, pushReq)
		defer maybeCleanup()

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

		group := d.activeGroups.UpdateActiveGroupTimestamp(userID, validation.GroupLabel(d.limits, userID, req.Timeseries), now)

		// A WriteRequest can only contain series or metadata but not both. This might change in the future.
		validatedMetadata := 0
		validatedSamples := 0
		validatedExemplars := 0

		// Find the earliest and latest samples in the batch.
		earliestSampleTimestampMs, latestSampleTimestampMs := int64(math.MaxInt64), int64(0)
		for _, ts := range req.Timeseries {
			for _, s := range ts.Samples {
				earliestSampleTimestampMs = util_math.Min(earliestSampleTimestampMs, s.TimestampMs)
				latestSampleTimestampMs = util_math.Max(latestSampleTimestampMs, s.TimestampMs)
			}
			for _, h := range ts.Histograms {
				earliestSampleTimestampMs = util_math.Min(earliestSampleTimestampMs, h.Timestamp)
				latestSampleTimestampMs = util_math.Max(latestSampleTimestampMs, h.Timestamp)
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

		var firstPartialErr error
		var removeIndexes []int
		totalSamples, totalExemplars := 0, 0

		labelValuesWithNewlines := 0
		for tsIdx, ts := range req.Timeseries {
			totalSamples += len(ts.Samples)
			totalExemplars += len(ts.Exemplars)
			if len(ts.Labels) == 0 {
				removeIndexes = append(removeIndexes, tsIdx)
				continue
			}

			d.labelsHistogram.Observe(float64(len(ts.Labels)))

			skipLabelNameValidation := d.cfg.SkipLabelNameValidation || req.GetSkipLabelNameValidation()
			// Note that validateSeries may drop some data in ts.
			validationErr := d.validateSeries(now, &req.Timeseries[tsIdx], userID, group, skipLabelNameValidation, minExemplarTS, maxExemplarTS)

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

			validatedSamples += len(ts.Samples) + len(ts.Histograms)
			validatedExemplars += len(ts.Exemplars)
			labelValuesWithNewlines += d.labelValuesWithNewlines(ts.Labels)
		}

		d.incomingSamplesPerRequest.WithLabelValues(userID).Observe(float64(totalSamples))
		d.incomingExemplarsPerRequest.WithLabelValues(userID).Observe(float64(totalExemplars))
		if labelValuesWithNewlines > 0 {
			d.labelValuesWithNewlinesPerUser.WithLabelValues(userID).Add(float64(labelValuesWithNewlines))
		}

		if len(removeIndexes) > 0 {
			for _, removeIndex := range removeIndexes {
				mimirpb.ReusePreallocTimeseries(&req.Timeseries[removeIndex])
			}
			req.Timeseries = util.RemoveSliceIndexes(req.Timeseries, removeIndexes)
			removeIndexes = removeIndexes[:0]
		}

		for mIdx, m := range req.Metadata {
			if validationErr := cleanAndValidateMetadata(d.metadataValidationMetrics, d.limits, userID, m); validationErr != nil {
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
			d.discardedSamplesRateLimited.WithLabelValues(userID, group).Add(float64(validatedSamples))
			d.discardedExemplarsRateLimited.WithLabelValues(userID).Add(float64(validatedExemplars))
			d.discardedMetadataRateLimited.WithLabelValues(userID).Add(float64(validatedMetadata))

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
	}
}

// metricsMiddleware updates metrics which are expected to account for all received data,
// including data that later gets modified or dropped.
func (d *Distributor) metricsMiddleware(next PushFunc) PushFunc {
	return func(ctx context.Context, pushReq *Request) error {
		next, maybeCleanup := NextOrCleanup(next, pushReq)
		defer maybeCleanup()

		req, err := pushReq.WriteRequest()
		if err != nil {
			return err
		}

		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return err
		}

		numSamples := 0
		numExemplars := 0
		for _, ts := range req.Timeseries {
			numSamples += len(ts.Samples) + len(ts.Histograms)
			numExemplars += len(ts.Exemplars)
		}

		span := opentracing.SpanFromContext(ctx)
		if span != nil {
			span.SetTag("write.samples", numSamples)
			span.SetTag("write.exemplars", numExemplars)
			span.SetTag("write.metadata", len(req.Metadata))
		}

		d.incomingRequests.WithLabelValues(userID).Inc()
		d.incomingSamples.WithLabelValues(userID).Add(float64(numSamples))
		d.incomingExemplars.WithLabelValues(userID).Add(float64(numExemplars))
		d.incomingMetadata.WithLabelValues(userID).Add(float64(len(req.Metadata)))

		return next(ctx, pushReq)
	}
}

type ctxKey int

const requestStateKey ctxKey = 1

// requestState represents state of checks for given request. If this object is stored in context,
// it means that request has been checked against inflight requests limit, and FinishPushRequest,
// cleanupAfterPushFinished (or both) must be called for given context.
type requestState struct {
	// If set to true, push request will perform cleanup of inflight metrics after request has actually finished
	// (which can be after push handler returns).
	pushHandlerPerformsCleanup bool

	// If positive, it means that size of httpgrpc.HTTPRequest has been checked and added to inflightPushRequestsBytes.
	httpgrpcRequestSize int64

	// If positive, it means that size of mimirpb.WriteRequest has been checked and added to inflightPushRequestsBytes.
	writeRequestSize int64
}

func (d *Distributor) StartPushRequest(ctx context.Context, httpgrpcRequestSize int64) (context.Context, error) {
	ctx, _, err := d.startPushRequest(ctx, httpgrpcRequestSize)
	return ctx, err
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
	// If requestState is already in context, it means that StartPushRequest already ran for this request.
	rs, alreadyInContext := ctx.Value(requestStateKey).(*requestState)
	if alreadyInContext {
		return ctx, rs, nil
	}

	rs = &requestState{}

	cleanupInDefer := true

	// Increment number of requests and bytes before doing the checks, so that we hit error if this request crosses the limits.
	inflight := d.inflightPushRequests.Inc()
	defer func() {
		if cleanupInDefer {
			d.cleanupAfterPushFinished(rs)
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
	return d.checkInflightBytes(inflightBytes)
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

	d.cleanupAfterPushFinished(rs)
}

func (d *Distributor) cleanupAfterPushFinished(rs *requestState) {
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
	return func(ctx context.Context, pushReq *Request) error {
		// Make sure the distributor service and all its dependent services have been
		// started. The following checks in this middleware depend on the runtime config
		// service having been started.
		if s := d.State(); s != services.Running {
			return newUnavailableError(s)
		}

		// We don't know request size yet, will check it later.
		ctx, rs, err := d.startPushRequest(ctx, -1)
		if err != nil {
			return middleware.DoNotLogError{Err: err}
		}

		rs.pushHandlerPerformsCleanup = true
		// Decrement counter after all ingester calls have finished or been cancelled.
		pushReq.AddCleanup(func() {
			d.cleanupAfterPushFinished(rs)
		})

		next, maybeCleanup := NextOrCleanup(next, pushReq)
		defer maybeCleanup()

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

// Push is gRPC method registered as client.IngesterServer and distributor.DistributorServer.
func (d *Distributor) Push(ctx context.Context, req *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	pushReq := NewParsedRequest(req)
	pushReq.AddCleanup(func() {
		mimirpb.ReuseSlice(req.Timeseries)
	})

	pushErr := d.PushWithMiddlewares(ctx, pushReq)
	if pushErr == nil {
		return &mimirpb.WriteResponse{}, nil
	}
	handledErr := d.handlePushError(ctx, pushErr)
	return nil, handledErr
}

func (d *Distributor) handlePushError(ctx context.Context, pushErr error) error {
	if errors.Is(pushErr, context.Canceled) {
		return pushErr
	}

	if errors.Is(pushErr, context.DeadlineExceeded) {
		return pushErr
	}

	serviceOverloadErrorEnabled := false
	userID, err := tenant.TenantID(ctx)
	if err == nil {
		serviceOverloadErrorEnabled = d.limits.ServiceOverloadStatusCodeOnRateLimitEnabled(userID)
	}
	return toErrorWithGRPCStatus(pushErr, serviceOverloadErrorEnabled)
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

	now := mtime.Now()

	d.updateReceivedMetrics(req, userID, now)

	if len(req.Timeseries) == 0 && len(req.Metadata) == 0 {
		return nil
	}

	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		span.SetTag("organization", userID)
	}

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

	batchCleanup := func() {
		// Notify the provided callback that it's now safe to run the cleanup because there are no
		// more in-flight requests to the backend.
		cleanup()

		// All requests have completed, so it's now safe to cancel the requests context to release resources.
		_, cancel := remoteRequestContextAndCancel()
		cancel()
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
		return d.sendWriteRequestToPartitions(ctx, tenantID, partitionsSubring, req, keys, initialMetadataIndex, remoteRequestContext, batchOptions)
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

		partitionsErr = d.sendWriteRequestToPartitions(ctx, tenantID, partitionsSubring, req, keys, initialMetadataIndex, remoteRequestContext, batchOptions)
	}()

	// Wait until all backends have done.
	wg.Wait()

	// Ingester errors could be soft (e.g. 4xx) or hard errors (e.g. 5xx) errors, while partition errors are always hard
	// errors. For this reason, it's important to give precedence to partition errors, otherwise the distributor may return
	// a 4xx (ingester error) when it should actually be a 5xx (partition error).
	if partitionsErr != nil {
		return partitionsErr
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

func (d *Distributor) updateReceivedMetrics(req *mimirpb.WriteRequest, userID string, now time.Time) {
	var receivedSamples, receivedExemplars, receivedMetadata int
	costattributionLimit := 0
	caEnabled := d.costAttributionMng != nil && d.costAttributionMng.EnabledForUser(userID)
	if caEnabled {
		costattributionLimit = d.costAttributionMng.GetUserAttributionLimit(userID)
	}
	costAttribution := make(map[string]int, costattributionLimit)

	for _, ts := range req.Timeseries {
		receivedSamples += len(ts.TimeSeries.Samples) + len(ts.TimeSeries.Histograms)
		receivedExemplars += len(ts.TimeSeries.Exemplars)
		if caEnabled {
			attribution := d.costAttributionMng.UpdateAttributionTimestamp(userID, mimirpb.FromLabelAdaptersToLabels(ts.Labels), now)
			costAttribution[attribution]++
		}
	}
	receivedMetadata = len(req.Metadata)
	if caEnabled {
		for lv, count := range costAttribution {
			d.costAttributionMng.IncrementReceivedSamples(userID, lv, float64(count))
		}
	}
	d.receivedSamples.WithLabelValues(userID).Add(float64(receivedSamples))
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
		zoneSorter = queryIngesterPartitionsRingZoneSorter(d.cfg.PreferAvailabilityZone)
	} else {
		// We expect to always have exactly 1 replication set when ingest storage is disabled.
		// To keep the code safer, we run with no zone sorter if that's not the case.
		if len(replicationSets) == 1 {
			zoneSorter = queryIngestersRingZoneSorter(replicationSets[0])
		}
	}

	return ring.DoUntilQuorumConfig{
		MinimizeRequests: d.cfg.MinimizeIngesterRequests,
		HedgingDelay:     d.cfg.MinimiseIngesterRequestsHedgingDelay,
		ZoneSorter:       zoneSorter,
		Logger:           spanlogger.FromContext(ctx, d.log),
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
// The sorter gives preference to preferredZone if non empty, and then randomize the other zones.
func queryIngesterPartitionsRingZoneSorter(preferredZone string) ring.ZoneSorter {
	return func(zones []string) []string {
		// Shuffle the zones to distribute load evenly.
		if len(zones) > 2 || (preferredZone == "" && len(zones) > 1) {
			rand.Shuffle(len(zones), func(i, j int) {
				zones[i], zones[j] = zones[j], zones[i]
			})
		}

		if preferredZone != "" {
			// Give priority to the preferred zone.
			for i, z := range zones {
				if z == preferredZone {
					zones[0], zones[i] = zones[i], zones[0]
					break
				}
			}
		}

		return zones
	}
}

// LabelValuesForLabelName returns the label values associated with the given labelName, among all series with samples
// timestamp between from and to, and series labels matching the optional matchers.
func (d *Distributor) LabelValuesForLabelName(ctx context.Context, from, to model.Time, labelName model.LabelName, matchers ...*labels.Matcher) ([]string, error) {
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx)
	if err != nil {
		return nil, err
	}

	req, err := ingester_client.ToLabelValuesRequest(labelName, from, to, matchers)
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

		log, ctx := spanlogger.NewWithLogger(ctx, d.log, "Distributor.ActiveSeries.queryIngester")
		defer log.Finish()

		stream, err := client.ActiveSeries(ctx, req)
		if err != nil {
			if errors.Is(globalerror.WrapGRPCErrorWithContextError(ctx, err), context.Canceled) {
				return ignored{}, nil
			}
			level.Error(log).Log("msg", "error creating active series response stream", "err", err)
			ext.Error.Set(log.Span, true)
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
				ext.Error.Set(log.Span, true)
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
	sort.Slice(result, func(i, j int) bool {
		return result[i].Metric < result[j].Metric
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
func (d *Distributor) LabelNames(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) ([]string, error) {
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx)
	if err != nil {
		return nil, err
	}

	req, err := ingester_client.ToLabelNamesRequest(from, to, matchers)
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

	return values, nil
}

// MetricsForLabelMatchers returns a list of series with samples timestamps between from and through, and series labels
// matching the optional label matchers. The returned series are not sorted.
func (d *Distributor) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]labels.Labels, error) {
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx)
	if err != nil {
		return nil, err
	}

	req, err := ingester_client.ToMetricsForLabelMatchersRequest(from, through, matchers)
	if err != nil {
		return nil, err
	}

	resps, err := forReplicationSets(ctx, d, replicationSets, func(ctx context.Context, client ingester_client.IngesterClient) (*ingester_client.MetricsForLabelMatchersResponse, error) {
		return client.MetricsForLabelMatchers(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	metrics := map[uint64]labels.Labels{}
	for _, resp := range resps {
		ms := ingester_client.FromMetricsForLabelMatchersResponse(resp)
		for _, m := range ms {
			metrics[labels.StableHash(m)] = m
		}
	}

	queryLimiter := mimir_limiter.QueryLimiterFromContextWithFallback(ctx)

	result := make([]labels.Labels, 0, len(metrics))
	for _, m := range metrics {
		if err := queryLimiter.AddSeries(mimirpb.FromLabelsToLabelAdapters(m)); err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	return result, nil
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
				Metric: m.MetricFamilyName,
				Help:   m.Help,
				Unit:   m.Unit,
				Type:   mimirpb.MetricMetadataMetricTypeToMetricType(m.GetType()),
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
