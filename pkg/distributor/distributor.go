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
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/limiter"
	"github.com/grafana/dskit/ring"
	ring_client "github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/scrape"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/instrument"
	"github.com/weaveworks/common/mtime"
	"github.com/weaveworks/common/user"
	"go.uber.org/atomic"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/distributor/forwarding"
	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/httpgrpcutil"
	util_math "github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/push"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	maxIngestionRateFlag             = "distributor.instance-limits.max-ingestion-rate"
	maxInflightPushRequestsFlag      = "distributor.instance-limits.max-inflight-push-requests"
	maxInflightPushRequestsBytesFlag = "distributor.instance-limits.max-inflight-push-requests-bytes"
)

var (
	// Validation errors.
	errInvalidTenantShardSize = errors.New("invalid tenant shard size, the value must be greater or equal to zero")

	// Distributor instance limits errors.
	errMaxInflightRequestsReached      = errors.New(globalerror.DistributorMaxInflightPushRequests.MessageWithPerInstanceLimitConfig("the write request has been rejected because the distributor exceeded the allowed number of inflight push requests", maxInflightPushRequestsFlag))
	errMaxIngestionRateReached         = errors.New(globalerror.DistributorMaxIngestionRate.MessageWithPerInstanceLimitConfig("the write request has been rejected because the distributor exceeded the ingestion rate limit", maxIngestionRateFlag))
	errMaxInflightRequestsBytesReached = errors.New(globalerror.DistributorMaxInflightPushRequestsBytes.MessageWithPerInstanceLimitConfig("the write request has been rejected because the distributor exceeded the allowed total size in bytes of inflight push requests", maxInflightPushRequestsBytesFlag))
)

const (
	// distributorRingKey is the key under which we store the distributors ring in the KVStore.
	distributorRingKey = "distributor"

	// ringAutoForgetUnhealthyPeriods is how many consecutive timeout periods an unhealthy instance
	// in the ring will be automatically removed after.
	ringAutoForgetUnhealthyPeriods = 10
)

const (
	instanceIngestionRateTickInterval = time.Second
)

// Distributor is a storage.SampleAppender and a client.Querier which
// forwards appends and queries to individual ingesters.
type Distributor struct {
	services.Service

	cfg           Config
	log           log.Logger
	ingestersRing ring.ReadRing
	ingesterPool  *ring_client.Pool
	limits        *validation.Overrides
	forwarder     forwarding.Forwarder

	// The global rate limiter requires a distributors ring to count
	// the number of healthy instances
	distributorsLifecycler *ring.BasicLifecycler
	distributorsRing       *ring.Ring
	healthyInstancesCount  *atomic.Uint32

	// For handling HA replicas.
	HATracker *haTracker

	// Per-user rate limiters.
	requestRateLimiter   *limiter.RateLimiter
	ingestionRateLimiter *limiter.RateLimiter

	// Manager for subservices (HA Tracker, distributor ring, forwarder and client pool)
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher

	activeUsers  *util.ActiveUsersCleanupService
	activeGroups *util.ActiveGroupsCleanupService

	ingestionRate             *util_math.EwmaRate
	inflightPushRequests      atomic.Int64
	inflightPushRequestsBytes atomic.Int64

	// Metrics
	queryDuration                    *instrument.HistogramCollector
	ingesterChunksDeduplicated       prometheus.Counter
	ingesterChunksTotal              prometheus.Counter
	receivedRequests                 *prometheus.CounterVec
	receivedSamples                  *prometheus.CounterVec
	receivedExemplars                *prometheus.CounterVec
	receivedHistograms               *prometheus.CounterVec
	receivedMetadata                 *prometheus.CounterVec
	incomingRequests                 *prometheus.CounterVec
	incomingSamples                  *prometheus.CounterVec
	incomingExemplars                *prometheus.CounterVec
	incomingHistograms               *prometheus.CounterVec
	incomingMetadata                 *prometheus.CounterVec
	nonHASamples                     *prometheus.CounterVec
	nonHAHistograms                  *prometheus.CounterVec
	dedupedSamples                   *prometheus.CounterVec
	dedupedHistograms                *prometheus.CounterVec
	labelsHistogram                  prometheus.Histogram
	sampleDelayHistogram             prometheus.Histogram
	replicationFactor                prometheus.Gauge
	latestSeenSampleTimestampPerUser *prometheus.GaugeVec

	discardedSamplesTooManyHaClusters    *prometheus.CounterVec
	discardedHistogramsTooManyHaClusters *prometheus.CounterVec
	discardedSamplesRateLimited          *prometheus.CounterVec
	discardedRequestsRateLimited         *prometheus.CounterVec
	discardedExemplarsRateLimited        *prometheus.CounterVec
	discardedHistogramsRateLimited       *prometheus.CounterVec
	discardedMetadataRateLimited         *prometheus.CounterVec

	sampleValidationMetrics    *validation.SampleValidationMetrics
	exemplarValidationMetrics  *validation.ExemplarValidationMetrics
	histogramValidationMetrics *validation.HistogramValidationMetrics
	metadataValidationMetrics  *validation.MetadataValidationMetrics

	pushWithMiddlewares push.Func
}

// Config contains the configuration required to
// create a Distributor
type Config struct {
	PoolConfig PoolConfig `yaml:"pool"`

	HATrackerConfig HATrackerConfig `yaml:"ha_tracker"`

	MaxRecvMsgSize int           `yaml:"max_recv_msg_size" category:"advanced"`
	RemoteTimeout  time.Duration `yaml:"remote_timeout" category:"advanced"`

	// Distributors ring
	DistributorRing RingConfig `yaml:"ring"`

	// for testing and for extending the ingester by adding calls to the client
	IngesterClientFactory ring_client.PoolFactory `yaml:"-"`

	// when true the distributor does not validate the label name, Mimir doesn't directly use
	// this (and should never use it) but this feature is used by other projects built on top of it
	SkipLabelNameValidation bool `yaml:"-"`

	// This config is dynamically injected because it is defined in the querier config.
	ShuffleShardingLookbackPeriod time.Duration `yaml:"-"`

	// Limits for distributor
	InstanceLimits InstanceLimits `yaml:"instance_limits"`

	// Configuration for forwarding of metrics to alternative ingestion endpoint.
	Forwarding forwarding.Config
}

type InstanceLimits struct {
	MaxIngestionRate             float64 `yaml:"max_ingestion_rate" category:"advanced"`
	MaxInflightPushRequests      int     `yaml:"max_inflight_push_requests" category:"advanced"`
	MaxInflightPushRequestsBytes int     `yaml:"max_inflight_push_requests_bytes" category:"advanced"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	cfg.PoolConfig.RegisterFlags(f)
	cfg.HATrackerConfig.RegisterFlags(f)
	cfg.DistributorRing.RegisterFlags(f, logger)
	cfg.Forwarding.RegisterFlags(f)

	f.IntVar(&cfg.MaxRecvMsgSize, "distributor.max-recv-msg-size", 100<<20, "Max message size in bytes that the distributors will accept for incoming push requests to the remote write API. If exceeded, the request will be rejected.")
	f.DurationVar(&cfg.RemoteTimeout, "distributor.remote-timeout", 2*time.Second, "Timeout for downstream ingesters.")
	f.Float64Var(&cfg.InstanceLimits.MaxIngestionRate, maxIngestionRateFlag, 0, "Max ingestion rate (samples/sec) that this distributor will accept. This limit is per-distributor, not per-tenant. Additional push requests will be rejected. Current ingestion rate is computed as exponentially weighted moving average, updated every second. 0 = unlimited.")
	f.IntVar(&cfg.InstanceLimits.MaxInflightPushRequests, maxInflightPushRequestsFlag, 2000, "Max inflight push requests that this distributor can handle. This limit is per-distributor, not per-tenant. Additional requests will be rejected. 0 = unlimited.")
	f.IntVar(&cfg.InstanceLimits.MaxInflightPushRequestsBytes, maxInflightPushRequestsBytesFlag, 0, "The sum of the request sizes in bytes of inflight push requests that this distributor can handle. This limit is per-distributor, not per-tenant. Additional requests will be rejected. 0 = unlimited.")
}

// Validate config and returns error on failure
func (cfg *Config) Validate(limits validation.Limits) error {
	if limits.IngestionTenantShardSize < 0 {
		return errInvalidTenantShardSize
	}

	err := cfg.HATrackerConfig.Validate()
	if err != nil {
		return err
	}

	return cfg.Forwarding.Validate()
}

const (
	instanceLimitsMetric     = "cortex_distributor_instance_limits"
	instanceLimitsMetricHelp = "Instance limits used by this distributor." // Must be same for all registrations.
	limitLabel               = "limit"
)

// New constructs a new Distributor
func New(cfg Config, clientConfig ingester_client.Config, limits *validation.Overrides, activeGroupsCleanupService *util.ActiveGroupsCleanupService, ingestersRing ring.ReadRing, canJoinDistributorsRing bool, reg prometheus.Registerer, log log.Logger) (*Distributor, error) {
	if cfg.IngesterClientFactory == nil {
		cfg.IngesterClientFactory = func(addr string) (ring_client.PoolClient, error) {
			return ingester_client.MakeIngesterClient(addr, clientConfig)
		}
	}

	cfg.PoolConfig.RemoteTimeout = cfg.RemoteTimeout

	haTracker, err := newHATracker(cfg.HATrackerConfig, limits, reg, log)
	if err != nil {
		return nil, err
	}

	subservices := []services.Service(nil)
	subservices = append(subservices, haTracker)

	d := &Distributor{
		cfg:                   cfg,
		log:                   log,
		ingestersRing:         ingestersRing,
		ingesterPool:          NewPool(cfg.PoolConfig, ingestersRing, cfg.IngesterClientFactory, log),
		healthyInstancesCount: atomic.NewUint32(0),
		limits:                limits,
		HATracker:             haTracker,
		ingestionRate:         util_math.NewEWMARate(0.2, instanceIngestionRateTickInterval),

		queryDuration: instrument.NewHistogramCollector(promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "distributor_query_duration_seconds",
			Help:      "Time spent executing expression and exemplar queries.",
			Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30},
		}, []string{"method", "status_code"})),
		ingesterChunksDeduplicated: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_query_ingester_chunks_deduped_total",
			Help:      "Number of chunks deduplicated at query time from ingesters.",
		}),
		ingesterChunksTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_query_ingester_chunks_total",
			Help:      "Number of chunks transferred at query time from ingesters.",
		}),
		receivedRequests: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_received_requests_total",
			Help:      "The total number of received requests, excluding rejected, forwarded and deduped requests.",
		}, []string{"user"}),
		receivedSamples: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_received_samples_total",
			Help:      "The total number of received samples, excluding rejected, forwarded and deduped samples.",
		}, []string{"user"}),
		receivedExemplars: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_received_exemplars_total",
			Help:      "The total number of received exemplars, excluding rejected, forwarded and deduped exemplars.",
		}, []string{"user"}),
		receivedHistograms: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_received_histograms_total",
			Help:      "The total number of received histograms, excluding rejected and deduped histograms.",
		}, []string{"user"}),
		receivedMetadata: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_received_metadata_total",
			Help:      "The total number of received metadata, excluding rejected.",
		}, []string{"user"}),
		incomingRequests: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_requests_in_total",
			Help:      "The total number of requests that have come in to the distributor, including rejected, forwarded or deduped requests.",
		}, []string{"user"}),
		incomingSamples: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_samples_in_total",
			Help:      "The total number of samples that have come in to the distributor, including rejected, forwarded or deduped samples.",
		}, []string{"user"}),
		incomingExemplars: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_exemplars_in_total",
			Help:      "The total number of exemplars that have come in to the distributor, including rejected, forwarded or deduped exemplars.",
		}, []string{"user"}),
		incomingHistograms: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_histograms_in_total",
			Help:      "The total number of histograms that have come in to the distributor, including rejected or deduped histograms.",
		}, []string{"user"}),
		incomingMetadata: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_metadata_in_total",
			Help:      "The total number of metadata the have come in to the distributor, including rejected.",
		}, []string{"user"}),
		nonHASamples: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_non_ha_samples_received_total",
			Help:      "The total number of received samples for a user that has HA tracking turned on, but the sample didn't contain both HA labels.",
		}, []string{"user"}),
		nonHAHistograms: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_non_ha_histograms_received_total",
			Help:      "The total number of received histograms for a user that has HA tracking turned on, but the histogram didn't contain both HA labels.",
		}, []string{"user"}),
		dedupedSamples: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_deduped_samples_total",
			Help:      "The total number of deduplicated samples.",
		}, []string{"user", "cluster"}),
		dedupedHistograms: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_deduped_histograms_total",
			Help:      "The total number of deduplicated histograms.",
		}, []string{"user", "cluster"}),
		labelsHistogram: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "labels_per_sample",
			Help:      "Number of labels per sample.",
			Buckets:   []float64{5, 10, 15, 20, 25},
		}),
		sampleDelayHistogram: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "distributor_sample_delay_seconds",
			Help:      "Number of seconds by which a sample came in late wrt wallclock.",
			Buckets: []float64{
				30,           // 30s
				60 * 1,       // 1 min
				60 * 2,       // 2 min
				60 * 4,       // 4 min
				60 * 8,       // 8 min
				60 * 10,      // 10 min
				60 * 30,      // 30 min
				60 * 60,      // 1h
				60 * 60 * 2,  // 2h
				60 * 60 * 3,  // 3h
				60 * 60 * 6,  // 6h
				60 * 60 * 24, // 24h
			},
		}),
		replicationFactor: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Namespace: "cortex",
			Name:      "distributor_replication_factor",
			Help:      "The configured replication factor.",
		}),
		latestSeenSampleTimestampPerUser: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_distributor_latest_seen_sample_timestamp_seconds",
			Help: "Unix timestamp of latest received sample per user.",
		}, []string{"user"}),

		discardedSamplesTooManyHaClusters:    validation.DiscardedSamplesCounter(reg, validation.ReasonTooManyHAClusters),
		discardedHistogramsTooManyHaClusters: validation.DiscardedHistogramsCounter(reg, validation.ReasonTooManyHAClusters),
		discardedSamplesRateLimited:          validation.DiscardedSamplesCounter(reg, validation.ReasonRateLimited),
		discardedRequestsRateLimited:         validation.DiscardedRequestsCounter(reg, validation.ReasonRateLimited),
		discardedExemplarsRateLimited:        validation.DiscardedExemplarsCounter(reg, validation.ReasonRateLimited),
		discardedHistogramsRateLimited:       validation.DiscardedHistogramsCounter(reg, validation.ReasonRateLimited),
		discardedMetadataRateLimited:         validation.DiscardedMetadataCounter(reg, validation.ReasonRateLimited),

		sampleValidationMetrics:    validation.NewSampleValidationMetrics(reg),
		exemplarValidationMetrics:  validation.NewExemplarValidationMetrics(reg),
		histogramValidationMetrics: validation.NewHistogramValidationMetrics(reg),
		metadataValidationMetrics:  validation.NewMetadataValidationMetrics(reg),
	}

	promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name:        instanceLimitsMetric,
		Help:        instanceLimitsMetricHelp,
		ConstLabels: map[string]string{limitLabel: "max_inflight_push_requests"},
	}).Set(float64(cfg.InstanceLimits.MaxInflightPushRequests))
	promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name:        instanceLimitsMetric,
		Help:        instanceLimitsMetricHelp,
		ConstLabels: map[string]string{limitLabel: "max_inflight_push_requests_bytes"},
	}).Set(float64(cfg.InstanceLimits.MaxInflightPushRequestsBytes))
	promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name:        instanceLimitsMetric,
		Help:        instanceLimitsMetricHelp,
		ConstLabels: map[string]string{limitLabel: "max_ingestion_rate"},
	}).Set(cfg.InstanceLimits.MaxIngestionRate)

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
		ingestionRateStrategy = newGlobalRateStrategy(newIngestionRateStrategy(limits), d)
	}

	d.requestRateLimiter = limiter.NewRateLimiter(requestRateStrategy, 10*time.Second)
	d.ingestionRateLimiter = limiter.NewRateLimiter(ingestionRateStrategy, 10*time.Second)
	d.distributorsLifecycler = distributorsLifecycler
	d.distributorsRing = distributorsRing

	d.replicationFactor.Set(float64(ingestersRing.ReplicationFactor()))
	d.activeUsers = util.NewActiveUsersCleanupWithDefaultValues(d.cleanupInactiveUser)
	d.activeGroups = activeGroupsCleanupService

	d.forwarder = forwarding.NewForwarder(cfg.Forwarding, reg, log, limits, d.activeGroups)
	// The forwarder is an optional feature, if it's disabled then d.forwarder will be nil.
	if d.forwarder != nil {
		subservices = append(subservices, d.forwarder)
	}

	d.pushWithMiddlewares = d.GetPushFunc(nil)

	subservices = append(subservices, d.ingesterPool, d.activeUsers)
	d.subservices, err = services.NewManager(subservices...)
	if err != nil {
		return nil, err
	}

	d.subservicesWatcher = services.NewFailureWatcher()
	d.subservicesWatcher.WatchManager(d.subservices)

	d.Service = services.NewBasicService(d.starting, d.running, d.stopping)
	return d, nil
}

// newRingAndLifecycler creates a new distributor ring and lifecycler with all required lifecycler delegates
func newRingAndLifecycler(cfg RingConfig, instanceCount *atomic.Uint32, logger log.Logger, reg prometheus.Registerer) (*ring.Ring, *ring.BasicLifecycler, error) {
	reg = prometheus.WrapRegistererWithPrefix("cortex_", reg)
	kvStore, err := kv.NewClient(cfg.KVStore, ring.GetCodec(), kv.RegistererWithKVName(reg, "distributor-lifecycler"), logger)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialize distributors' KV store")
	}

	lifecyclerCfg, err := cfg.ToBasicLifecyclerConfig(logger)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to build distributors' lifecycler config")
	}

	var delegate ring.BasicLifecyclerDelegate
	delegate = ring.NewInstanceRegisterDelegate(ring.ACTIVE, lifecyclerCfg.NumTokens)
	delegate = newHealthyInstanceDelegate(instanceCount, cfg.HeartbeatTimeout, delegate)
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, logger)
	delegate = ring.NewAutoForgetDelegate(ringAutoForgetUnhealthyPeriods*cfg.HeartbeatTimeout, delegate, logger)

	distributorsLifecycler, err := ring.NewBasicLifecycler(lifecyclerCfg, "distributor", distributorRingKey, kvStore, delegate, logger, reg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialize distributors' lifecycler")
	}

	distributorsRing, err := ring.New(cfg.ToRingConfig(), "distributor", distributorRingKey, logger, reg)
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
	d.receivedHistograms.DeleteLabelValues(userID)
	d.receivedMetadata.DeleteLabelValues(userID)
	d.incomingRequests.DeleteLabelValues(userID)
	d.incomingSamples.DeleteLabelValues(userID)
	d.incomingExemplars.DeleteLabelValues(userID)
	d.incomingHistograms.DeleteLabelValues(userID)
	d.incomingMetadata.DeleteLabelValues(userID)
	d.nonHASamples.DeleteLabelValues(userID)
	d.nonHAHistograms.DeleteLabelValues(userID)
	d.latestSeenSampleTimestampPerUser.DeleteLabelValues(userID)

	filter := prometheus.Labels{"user": userID}
	d.dedupedSamples.DeletePartialMatch(filter)
	d.dedupedHistograms.DeletePartialMatch(filter)

	d.discardedSamplesTooManyHaClusters.DeletePartialMatch(filter)
	d.discardedSamplesRateLimited.DeletePartialMatch(filter)
	d.discardedHistogramsTooManyHaClusters.DeleteLabelValues(userID)
	d.discardedHistogramsRateLimited.DeleteLabelValues(userID)
	d.discardedRequestsRateLimited.DeleteLabelValues(userID)
	d.discardedExemplarsRateLimited.DeleteLabelValues(userID)
	d.discardedMetadataRateLimited.DeleteLabelValues(userID)

	d.sampleValidationMetrics.DeleteUserMetrics(userID)
	d.exemplarValidationMetrics.DeleteUserMetrics(userID)
	d.histogramValidationMetrics.DeleteUserMetrics(userID)
	d.metadataValidationMetrics.DeleteUserMetrics(userID)

	if d.forwarder != nil {
		d.forwarder.DeleteMetricsForUser(userID)
	}
}

func (d *Distributor) RemoveGroupMetricsForUser(userID, group string) {
	d.dedupedSamples.DeleteLabelValues(userID, group)
	d.discardedSamplesTooManyHaClusters.DeleteLabelValues(userID, group)
	d.discardedSamplesRateLimited.DeleteLabelValues(userID, group)
	d.sampleValidationMetrics.DeleteUserMetricsForGroup(userID, group)
}

// Called after distributor is asked to stop via StopAsync.
func (d *Distributor) stopping(_ error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), d.subservices)
}

func (d *Distributor) tokenForLabels(userID string, labels []mimirpb.LabelAdapter) uint32 {
	return shardByAllLabels(userID, labels)
}

func (d *Distributor) tokenForMetadata(userID string, metricName string) uint32 {
	return shardByMetricName(userID, metricName)
}

// shardByMetricName returns the token for the given metric. The provided metricName
// is guaranteed to not be retained.
func shardByMetricName(userID string, metricName string) uint32 {
	h := shardByUser(userID)
	h = ingester_client.HashAdd32(h, metricName)
	return h
}

func shardByUser(userID string) uint32 {
	h := ingester_client.HashNew32()
	h = ingester_client.HashAdd32(h, userID)
	return h
}

// This function generates different values for different order of same labels.
func shardByAllLabels(userID string, labels []mimirpb.LabelAdapter) uint32 {
	h := shardByUser(userID)
	for _, label := range labels {
		h = ingester_client.HashAdd32(h, label.Name)
		h = ingester_client.HashAdd32(h, label.Value)
	}
	return h
}

// Remove the label labelname from a slice of LabelPairs if it exists.
func removeLabel(labelName string, labels *[]mimirpb.LabelAdapter) {
	for i := 0; i < len(*labels); i++ {
		pair := (*labels)[i]
		if pair.Name == labelName {
			*labels = append((*labels)[:i], (*labels)[i+1:]...)
			return
		}
	}
}

// Remove labels with value=="" from a slice of LabelPairs, updating the slice in-place.
func removeEmptyLabelValues(labels *[]mimirpb.LabelAdapter) {
	for i := len(*labels) - 1; i >= 0; i-- {
		if (*labels)[i].Value == "" {
			*labels = append((*labels)[:i], (*labels)[i+1:]...)
		}
	}
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
func (d *Distributor) validateSeries(nowt time.Time, ts mimirpb.PreallocTimeseries, userID, group string, skipLabelNameValidation bool, minExemplarTS int64) error {
	if err := validation.ValidateLabels(d.sampleValidationMetrics, d.limits, userID, group, ts.Labels, skipLabelNameValidation); err != nil {
		return err
	}

	now := model.TimeFromUnixNano(nowt.UnixNano())

	for _, s := range ts.Samples {

		delta := now - model.Time(s.TimestampMs)
		if delta > 0 {
			d.sampleDelayHistogram.Observe(float64(delta) / 1000)
		}

		if err := validation.ValidateSample(d.sampleValidationMetrics, now, d.limits, userID, group, ts.Labels, s); err != nil {
			return err
		}
	}

	for _, h := range ts.Histograms {
		delta := now - model.Time(h.Timestamp)
		if delta > 0 {
			d.sampleDelayHistogram.Observe(float64(delta) / 1000)
		}

		if err := validation.ValidateHistogram(d.histogramValidationMetrics, now, d.limits, userID, ts.Labels, h); err != nil {
			return err
		}
	}

	if d.limits.MaxGlobalExemplarsPerUser(userID) == 0 {
		ts.Exemplars = nil
		return nil
	}

	for i := 0; i < len(ts.Exemplars); {
		e := ts.Exemplars[i]
		if err := validation.ValidateExemplar(d.exemplarValidationMetrics, userID, ts.Labels, e); err != nil {
			// An exemplar validation error prevents ingesting samples
			// in the same series object. However because the current Prometheus
			// remote write implementation only populates one or the other,
			// there never will be any.
			return err
		}
		if !validation.ExemplarTimestampOK(d.exemplarValidationMetrics, userID, minExemplarTS, e) {
			// Delete this exemplar by moving the last one on top and shortening the slice
			last := len(ts.Exemplars) - 1
			if i < last {
				ts.Exemplars[i] = ts.Exemplars[last]
			}
			ts.Exemplars = ts.Exemplars[:last]
			continue
		}
		i++
	}
	return nil
}

// wrapPushWithMiddlewares returns push function wrapped in all Distributor's middlewares.
func (d *Distributor) wrapPushWithMiddlewares(externalMiddleware func(next push.Func) push.Func, next push.Func) push.Func {
	var middlewares []func(push.Func) push.Func

	// The middlewares will be applied to the request (!) in the specified order, from first to last.
	// To guarantee that, middleware functions will be called in reversed order, wrapping the
	// result from previous call.
	middlewares = append(middlewares, d.limitsMiddleware) // should run first because it checks limits before other middlewares need to read the request body
	middlewares = append(middlewares, d.metricsMiddleware)
	middlewares = append(middlewares, d.prePushHaDedupeMiddleware)
	middlewares = append(middlewares, d.prePushRelabelMiddleware)
	middlewares = append(middlewares, d.prePushValidationMiddleware)
	middlewares = append(middlewares, d.prePushForwardingMiddleware)
	if externalMiddleware != nil {
		middlewares = append(middlewares, externalMiddleware)
	}

	for ix := len(middlewares) - 1; ix >= 0; ix-- {
		next = middlewares[ix](next)
	}

	return next
}

func (d *Distributor) prePushHaDedupeMiddleware(next push.Func) push.Func {
	return func(ctx context.Context, pushReq *push.Request) (*mimirpb.WriteResponse, error) {
		cleanupInDefer := true
		defer func() {
			if cleanupInDefer {
				pushReq.CleanUp()
			}
		}()

		req, err := pushReq.WriteRequest()
		if err != nil {
			return nil, err
		}

		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return nil, err
		}

		if len(req.Timeseries) == 0 || !d.limits.AcceptHASamples(userID) {
			cleanupInDefer = false
			return next(ctx, pushReq)
		}

		haReplicaLabel := d.limits.HAReplicaLabel(userID)
		cluster, replica := findHALabels(haReplicaLabel, d.limits.HAClusterLabel(userID), req.Timeseries[0].Labels)
		// Make a copy of these, since they may be retained as labels on our metrics, e.g. dedupedSamples.
		cluster, replica = copyString(cluster), copyString(replica)

		span := opentracing.SpanFromContext(ctx)
		if span != nil {
			span.SetTag("cluster", cluster)
			span.SetTag("replica", replica)
		}

		numSamples := 0
		numHistograms := 0
		group := d.activeGroups.UpdateActiveGroupTimestamp(userID, validation.GroupLabel(d.limits, userID, req.Timeseries), time.Now())
		for _, ts := range req.Timeseries {
			numSamples += len(ts.Samples)
			numHistograms += len(ts.Histograms)
		}

		removeReplica, err := d.checkSample(ctx, userID, cluster, replica)
		if err != nil {
			if errors.Is(err, replicasNotMatchError{}) {
				// These samples and histograms have been deduped.
				d.dedupedSamples.WithLabelValues(userID, cluster).Add(float64(numSamples))
				d.dedupedHistograms.WithLabelValues(userID, cluster).Add(float64(numHistograms))
				return nil, httpgrpc.Errorf(http.StatusAccepted, err.Error())
			}

			if errors.Is(err, tooManyClustersError{}) {
				d.discardedSamplesTooManyHaClusters.WithLabelValues(userID, group).Add(float64(numSamples))
				d.discardedHistogramsTooManyHaClusters.WithLabelValues(userID).Add(float64(numHistograms))

				return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
			}

			return nil, err
		}

		if removeReplica {
			// If we found both the cluster and replica labels, we only want to include the cluster label when
			// storing series in Mimir. If we kept the replica label we would end up with another series for the same
			// series we're trying to dedupe when HA tracking moves over to a different replica.
			for _, ts := range req.Timeseries {
				removeLabel(haReplicaLabel, &ts.Labels)
			}
		} else {
			// If there wasn't an error but removeReplica is false that means we didn't find both HA labels.
			d.nonHASamples.WithLabelValues(userID).Add(float64(numSamples))
			d.nonHASamples.WithLabelValues(userID).Add(float64(numHistograms))
		}

		cleanupInDefer = false
		return next(ctx, pushReq)
	}
}

func (d *Distributor) prePushRelabelMiddleware(next push.Func) push.Func {
	return func(ctx context.Context, pushReq *push.Request) (*mimirpb.WriteResponse, error) {
		cleanupInDefer := true
		defer func() {
			if cleanupInDefer {
				pushReq.CleanUp()
			}
		}()

		req, err := pushReq.WriteRequest()
		if err != nil {
			return nil, err
		}

		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return nil, err
		}

		var removeTsIndexes []int
		for tsIdx := 0; tsIdx < len(req.Timeseries); tsIdx++ {
			ts := req.Timeseries[tsIdx]

			if mrc := d.limits.MetricRelabelConfigs(userID); len(mrc) > 0 {
				l, keep := relabel.Process(mimirpb.FromLabelAdaptersToLabels(ts.Labels), mrc...)
				if !keep {
					removeTsIndexes = append(removeTsIndexes, tsIdx)
					continue
				}
				ts.Labels = mimirpb.FromLabelsToLabelAdapters(l)
			}

			for _, labelName := range d.limits.DropLabels(userID) {
				removeLabel(labelName, &ts.Labels)
			}

			// Prometheus strips empty values before storing; drop them now, before sharding to ingesters.
			removeEmptyLabelValues(&ts.Labels)

			if len(ts.Labels) == 0 {
				removeTsIndexes = append(removeTsIndexes, tsIdx)
				continue
			}

			// We rely on sorted labels in different places:
			// 1) When computing token for labels, and sharding by all labels. Here different order of labels returns
			// different tokens, which is bad.
			// 2) In validation code, when checking for duplicate label names. As duplicate label names are rejected
			// later in the validation phase, we ignore them here.
			// 3) Ingesters expect labels to be sorted in the Push request.
			sortLabelsIfNeeded(ts.Labels)
		}

		if len(removeTsIndexes) > 0 {
			for _, removeTsIndex := range removeTsIndexes {
				mimirpb.ReusePreallocTimeseries(&req.Timeseries[removeTsIndex])
			}
			req.Timeseries = util.RemoveSliceIndexes(req.Timeseries, removeTsIndexes)
		}

		cleanupInDefer = false
		return next(ctx, pushReq)
	}
}

func (d *Distributor) prePushValidationMiddleware(next push.Func) push.Func {
	return func(ctx context.Context, pushReq *push.Request) (*mimirpb.WriteResponse, error) {
		cleanupInDefer := true
		defer func() {
			if cleanupInDefer {
				pushReq.CleanUp()
			}
		}()

		req, err := pushReq.WriteRequest()
		if err != nil {
			return nil, err
		}

		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return nil, err
		}

		now := mtime.Now()
		d.receivedRequests.WithLabelValues(userID).Add(1)
		d.activeUsers.UpdateUserTimestamp(userID, now)

		group := d.activeGroups.UpdateActiveGroupTimestamp(userID, validation.GroupLabel(d.limits, userID, req.Timeseries), now)

		// A WriteRequest can only contain series or metadata but not both. This might change in the future.
		validatedMetadata := 0
		validatedSamples := 0
		validatedExemplars := 0
		validatedHistograms := 0

		// Find the earliest and latest samples in the batch.
		earliestSampleTimestampMs, latestSampleTimestampMs := int64(math.MaxInt64), int64(0)
		for _, ts := range req.Timeseries {
			for _, s := range ts.Samples {
				earliestSampleTimestampMs = util_math.Min64(earliestSampleTimestampMs, s.TimestampMs)
				latestSampleTimestampMs = util_math.Max64(latestSampleTimestampMs, s.TimestampMs)
			}
			for _, h := range ts.Histograms {
				earliestSampleTimestampMs = util_math.Min64(earliestSampleTimestampMs, h.Timestamp)
				latestSampleTimestampMs = util_math.Max64(latestSampleTimestampMs, h.Timestamp)
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
			minExemplarTS = earliestSampleTimestampMs - 300000
		}

		var firstPartialErr error
		var removeIndexes []int
		for tsIdx, ts := range req.Timeseries {
			if len(ts.Labels) == 0 {
				removeIndexes = append(removeIndexes, tsIdx)
				continue
			}

			d.labelsHistogram.Observe(float64(len(ts.Labels)))

			skipLabelNameValidation := d.cfg.SkipLabelNameValidation || req.GetSkipLabelNameValidation()
			// Note that validateSeries may drop some data in ts.
			validationErr := d.validateSeries(now, ts, userID, group, skipLabelNameValidation, minExemplarTS)

			// Errors in validation are considered non-fatal, as one series in a request may contain
			// invalid data but all the remaining series could be perfectly valid.
			if validationErr != nil {
				if firstPartialErr == nil {
					// The series labels may be retained by validationErr but that's not a problem for this
					// use case because we format it calling Error() and then we discard it.
					firstPartialErr = httpgrpc.Errorf(http.StatusBadRequest, validationErr.Error())
				}
				removeIndexes = append(removeIndexes, tsIdx)
				continue
			}

			validatedSamples += len(ts.Samples)
			validatedExemplars += len(ts.Exemplars)
			validatedHistograms += len(ts.Histograms)
		}
		if len(removeIndexes) > 0 {
			for _, removeIndex := range removeIndexes {
				mimirpb.ReusePreallocTimeseries(&req.Timeseries[removeIndex])
			}
			req.Timeseries = util.RemoveSliceIndexes(req.Timeseries, removeIndexes)
			removeIndexes = removeIndexes[:0]
		}

		for mIdx, m := range req.Metadata {
			if validationErr := validation.CleanAndValidateMetadata(d.metadataValidationMetrics, d.limits, userID, m); validationErr != nil {
				if firstPartialErr == nil {
					// The metadata info may be retained by validationErr but that's not a problem for this
					// use case because we format it calling Error() and then we discard it.
					firstPartialErr = httpgrpc.Errorf(http.StatusBadRequest, validationErr.Error())
				}

				removeIndexes = append(removeIndexes, mIdx)
				continue
			}

			validatedMetadata++
		}
		if len(removeIndexes) > 0 {
			req.Metadata = util.RemoveSliceIndexes(req.Metadata, removeIndexes)
		}

		if validatedSamples == 0 && validatedHistograms == 0 && validatedMetadata == 0 {
			return &mimirpb.WriteResponse{}, firstPartialErr
		}

		totalN := validatedSamples + validatedExemplars + validatedHistograms + validatedMetadata
		if !d.ingestionRateLimiter.AllowN(now, userID, totalN) {
			d.discardedSamplesRateLimited.WithLabelValues(userID, group).Add(float64(validatedSamples))
			d.discardedExemplarsRateLimited.WithLabelValues(userID).Add(float64(validatedExemplars))
			d.discardedHistogramsRateLimited.WithLabelValues(userID).Add(float64(validatedHistograms))
			d.discardedMetadataRateLimited.WithLabelValues(userID).Add(float64(validatedMetadata))
			// Return a 429 here to tell the client it is going too fast.
			// Client may discard the data or slow down and re-send.
			// Prometheus v2.26 added a remote-write option 'retry_on_http_429'.
			return nil, httpgrpc.Errorf(http.StatusTooManyRequests, validation.NewIngestionRateLimitedError(d.limits.IngestionRate(userID), d.limits.IngestionBurstSize(userID)).Error())
		}

		// totalN included samples, exemplars and metadata. Ingester follows this pattern when computing its ingestion rate.
		d.ingestionRate.Add(int64(totalN))

		cleanupInDefer = false
		res, err := next(ctx, pushReq)
		if err != nil {
			// Errors resulting from the pushing to the ingesters have priority over validation errors.
			return nil, err
		}

		return res, firstPartialErr
	}
}

// prePushForwardingMiddleware is used as push.Func middleware in front of push method.
// It forwards time series to configured remote_write endpoints if the forwarding rules say so.
func (d *Distributor) prePushForwardingMiddleware(next push.Func) push.Func {
	if d.forwarder == nil {
		// Forwarding is disabled, no need to wrap "next".
		return next
	}

	return func(ctx context.Context, pushReq *push.Request) (*mimirpb.WriteResponse, error) {
		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return nil, err
		}
		req, err := pushReq.WriteRequest()
		if err != nil {
			return nil, err
		}

		var errCh <-chan error
		req.Timeseries, errCh = d.forwardSamples(ctx, userID, req.Timeseries)
		resp, nextErr := next(ctx, pushReq)
		errs := []error{nextErr}

	LOOP:
		for {
			select {
			case err, ok := <-errCh:
				if ok {
					if err != nil {
						errs = append(errs, err)
					}
				} else {
					break LOOP
				}
			case <-ctx.Done():
				return resp, ctx.Err()
			}
		}

		return resp, httpgrpcutil.PrioritizeRecoverableErr(errs...)
	}
}

// metricsMiddleware updates metrics which are expected to account for all received data,
// including data that later gets modified or dropped.
func (d *Distributor) metricsMiddleware(next push.Func) push.Func {
	return func(ctx context.Context, pushReq *push.Request) (*mimirpb.WriteResponse, error) {
		cleanupInDefer := true
		defer func() {
			if cleanupInDefer {
				pushReq.CleanUp()
			}
		}()

		req, err := pushReq.WriteRequest()
		if err != nil {
			return nil, err
		}

		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return nil, err
		}

		numSamples := 0
		numExemplars := 0
		numHistograms := 0
		for _, ts := range req.Timeseries {
			numSamples += len(ts.Samples)
			numExemplars += len(ts.Exemplars)
			numHistograms += len(ts.Histograms)
		}

		d.incomingRequests.WithLabelValues(userID).Inc()
		d.incomingSamples.WithLabelValues(userID).Add(float64(numSamples))
		d.incomingExemplars.WithLabelValues(userID).Add(float64(numExemplars))
		d.incomingHistograms.WithLabelValues(userID).Add(float64(numHistograms))
		d.incomingMetadata.WithLabelValues(userID).Add(float64(len(req.Metadata)))

		cleanupInDefer = false
		return next(ctx, pushReq)
	}
}

// limitsMiddleware checks for instance limits and rejects request if this instance cannot process it at the moment.
func (d *Distributor) limitsMiddleware(next push.Func) push.Func {
	return func(ctx context.Context, pushReq *push.Request) (*mimirpb.WriteResponse, error) {
		// Increment number of requests and bytes before doing the checks, so that we hit error if this request crosses the limits.
		inflight := d.inflightPushRequests.Inc()

		// Decrement counter after all ingester calls have finished or been cancelled.
		pushReq.AddCleanup(func() {
			d.inflightPushRequests.Dec()
		})
		cleanupInDefer := true
		defer func() {
			if cleanupInDefer {
				pushReq.CleanUp()
			}
		}()

		if d.cfg.InstanceLimits.MaxInflightPushRequests > 0 && inflight > int64(d.cfg.InstanceLimits.MaxInflightPushRequests) {
			return nil, errMaxInflightRequestsReached
		}

		if d.cfg.InstanceLimits.MaxIngestionRate > 0 {
			if rate := d.ingestionRate.Rate(); rate >= d.cfg.InstanceLimits.MaxIngestionRate {
				return nil, errMaxIngestionRateReached
			}
		}

		userID, err := tenant.TenantID(ctx)
		if err != nil {
			return nil, err
		}

		now := mtime.Now()
		if !d.requestRateLimiter.AllowN(now, userID, 1) {
			d.discardedRequestsRateLimited.WithLabelValues(userID).Add(1)

			// Return a 429 here to tell the client it is going too fast.
			// Client may discard the data or slow down and re-send.
			// Prometheus v2.26 added a remote-write option 'retry_on_http_429'.
			return nil, httpgrpc.Errorf(http.StatusTooManyRequests, validation.NewRequestRateLimitedError(d.limits.RequestRate(userID), d.limits.RequestBurstSize(userID)).Error())
		}

		req, err := pushReq.WriteRequest()
		if err != nil {
			return nil, err
		}
		reqSize := int64(req.Size())
		inflightBytes := d.inflightPushRequestsBytes.Add(reqSize)
		pushReq.AddCleanup(func() {
			d.inflightPushRequestsBytes.Sub(reqSize)
		})

		if d.cfg.InstanceLimits.MaxInflightPushRequestsBytes > 0 && inflightBytes > int64(d.cfg.InstanceLimits.MaxInflightPushRequestsBytes) {
			return nil, errMaxInflightRequestsBytesReached
		}

		cleanupInDefer = false
		return next(ctx, pushReq)
	}
}

func (d *Distributor) forwardSamples(ctx context.Context, userID string, ts []mimirpb.PreallocTimeseries) ([]mimirpb.PreallocTimeseries, <-chan error) {
	forwardingErrCh := make(chan error)
	forwardingRules := d.limits.ForwardingRules(userID)
	endpoint := d.limits.ForwardingEndpoint(userID)
	if endpoint == "" || len(forwardingRules) == 0 {
		close(forwardingErrCh)
		return ts, forwardingErrCh
	}

	forwardingDropOlderThan := d.limits.ForwardingDropOlderThan(userID)

	var dropSamplesBeforeTimestamp int64
	if forwardingDropOlderThan > 0 {
		dropSamplesBeforeTimestamp = time.Now().Add(-forwardingDropOlderThan).UnixMilli()
	}

	// Reassign req.Timeseries because the forwarder creates a new slice which has been filtered down.
	// The cleanup func will cleanup the new slice, it's the forwarders responsibility to return the old one to the pool.
	ts, forwardingErrCh = d.forwarder.Forward(ctx, endpoint, dropSamplesBeforeTimestamp, forwardingRules, ts, userID)

	return ts, forwardingErrCh
}

// Push is gRPC method registered as client.IngesterServer and distributor.DistributorServer.
func (d *Distributor) Push(ctx context.Context, req *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	pushReq := push.NewParsedRequest(req)
	pushReq.AddCleanup(func() {
		mimirpb.ReuseSlice(req.Timeseries)
		mimirpb.ReuseSlice(req.EphemeralTimeseries)
	})

	return d.pushWithMiddlewares(ctx, pushReq)
}

// GetPushFunc returns push.Func that can be used by push handler.
// Wrapper, if not nil, is added to the list of distributor middlewares.
func (d *Distributor) GetPushFunc(externalMiddleware func(next push.Func) push.Func) push.Func {
	return d.wrapPushWithMiddlewares(externalMiddleware, d.push)
}

// push takes a write request and distributes it to ingesters using the ring.
// Strings in pushReq may be pointers into the gRPC buffer which will be reused, so must be copied if retained.
// push does not check limits like ingestion rate and inflight requests.
// These limits are checked either by Push gRPC method (when invoked via gRPC) or limitsMiddleware (when invoked via HTTP)
func (d *Distributor) push(ctx context.Context, pushReq *push.Request) (*mimirpb.WriteResponse, error) {
	cleanupInDefer := true
	defer func() {
		if cleanupInDefer {
			pushReq.CleanUp()
		}
	}()

	req, err := pushReq.WriteRequest()
	if err != nil {
		return nil, err
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	d.updateReceivedMetrics(req, userID)

	if len(req.Timeseries) == 0 && len(req.EphemeralTimeseries) == 0 && len(req.Metadata) == 0 {
		return &mimirpb.WriteResponse{}, nil
	}

	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		span.SetTag("organization", userID)
	}

	seriesKeys := d.getTokensForSeries(userID, req.Timeseries)
	ephemeralSeriesKeys := d.getTokensForSeries(userID, req.EphemeralTimeseries)
	metadataKeys := make([]uint32, 0, len(req.Metadata))

	for _, m := range req.Metadata {
		metadataKeys = append(metadataKeys, d.tokenForMetadata(userID, m.MetricFamilyName))
	}

	// Get a subring if tenant has shuffle shard size configured.
	subRing := d.ingestersRing.ShuffleShard(userID, d.limits.IngestionTenantShardSize(userID))

	// Use a background context to make sure all ingesters get samples even if we return early
	localCtx, cancel := context.WithTimeout(context.Background(), d.cfg.RemoteTimeout)
	localCtx = user.InjectOrgID(localCtx, userID)
	// Get clientIP(s) from Context and add it to localCtx
	source := util.GetSourceIPsFromOutgoingCtx(ctx)
	localCtx = util.AddSourceIPsToOutgoingContext(localCtx, source)
	if sp := opentracing.SpanFromContext(ctx); sp != nil {
		localCtx = opentracing.ContextWithSpan(localCtx, sp)
	}

	// All tokens, stored in order: series, metadata, ephemeral series.
	keys := make([]uint32, len(seriesKeys)+len(metadataKeys)+len(ephemeralSeriesKeys))
	initialMetadataIndex := len(seriesKeys)
	initialEphemeralIndex := initialMetadataIndex + len(metadataKeys)
	copy(keys, seriesKeys)
	copy(keys[initialMetadataIndex:], metadataKeys)
	copy(keys[initialEphemeralIndex:], ephemeralSeriesKeys)

	// we must not re-use buffers now until all DoBatch goroutines have finished,
	// so set this flag false and pass cleanup() to DoBatch.
	cleanupInDefer = false

	err = ring.DoBatch(ctx, ring.WriteNoExtend, subRing, keys, func(ingester ring.InstanceDesc, indexes []int) error {
		var timeseriesCount, ephemeralCount, metadataCount int
		for _, i := range indexes {
			if i >= initialEphemeralIndex {
				ephemeralCount++
			} else if i >= initialMetadataIndex {
				metadataCount++
			} else {
				timeseriesCount++
			}
		}

		timeseries := preallocSliceIfNeeded[mimirpb.PreallocTimeseries](timeseriesCount)
		ephemeral := preallocSliceIfNeeded[mimirpb.PreallocTimeseries](ephemeralCount)
		metadata := preallocSliceIfNeeded[*mimirpb.MetricMetadata](metadataCount)

		for _, i := range indexes {
			if i >= initialEphemeralIndex {
				ephemeral = append(ephemeral, req.EphemeralTimeseries[i-initialEphemeralIndex])
			} else if i >= initialMetadataIndex {
				metadata = append(metadata, req.Metadata[i-initialMetadataIndex])
			} else {
				timeseries = append(timeseries, req.Timeseries[i])
			}
		}

		err := d.send(localCtx, ingester, timeseries, ephemeral, metadata, req.Source)
		if errors.Is(err, context.DeadlineExceeded) {
			return httpgrpc.Errorf(500, "exceeded configured distributor remote timeout: %s", err.Error())
		}
		return err
	}, func() { pushReq.CleanUp(); cancel() })

	if err != nil {
		return nil, err
	}
	return &mimirpb.WriteResponse{}, nil
}

func preallocSliceIfNeeded[T any](size int) []T {
	if size > 0 {
		return make([]T, 0, size)
	}
	return nil
}

func (d *Distributor) getTokensForSeries(userID string, series []mimirpb.PreallocTimeseries) []uint32 {
	if len(series) == 0 {
		return nil
	}

	result := make([]uint32, 0, len(series))
	for _, ts := range series {
		result = append(result, d.tokenForLabels(userID, ts.Labels))
	}
	return result
}

func (d *Distributor) updateReceivedMetrics(req *mimirpb.WriteRequest, userID string) {
	var receivedSamples, receivedExemplars, receivedHistograms, receivedMetadata int
	for _, ts := range req.Timeseries {
		receivedSamples += len(ts.TimeSeries.Samples)
		receivedExemplars += len(ts.TimeSeries.Exemplars)
		receivedHistograms += len(ts.TimeSeries.Histograms)
	}
	receivedMetadata = len(req.Metadata)

	d.receivedSamples.WithLabelValues(userID).Add(float64(receivedSamples))
	d.receivedExemplars.WithLabelValues(userID).Add(float64(receivedExemplars))
	d.receivedHistograms.WithLabelValues(userID).Add(float64(receivedHistograms))
	d.receivedMetadata.WithLabelValues(userID).Add(float64(receivedMetadata))
}

func copyString(s string) string {
	return string([]byte(s))
}

func sortLabelsIfNeeded(labels []mimirpb.LabelAdapter) {
	// no need to run sort.Slice, if labels are already sorted, which is most of the time.
	// we can avoid extra memory allocations (mostly interface-related) this way.
	sorted := true
	last := ""
	for _, l := range labels {
		if last > l.Name {
			sorted = false
			break
		}
		last = l.Name
	}

	if sorted {
		return
	}

	sort.Slice(labels, func(i, j int) bool {
		return labels[i].Name < labels[j].Name
	})
}

func (d *Distributor) send(ctx context.Context, ingester ring.InstanceDesc, timeseries, ephemeral []mimirpb.PreallocTimeseries, metadata []*mimirpb.MetricMetadata, source mimirpb.WriteRequest_SourceEnum) error {
	h, err := d.ingesterPool.GetClientFor(ingester.Addr)
	if err != nil {
		return err
	}
	c := h.(ingester_client.IngesterClient)

	req := mimirpb.WriteRequest{
		Timeseries:          timeseries,
		Metadata:            metadata,
		Source:              source,
		EphemeralTimeseries: ephemeral,
	}
	_, err = c.Push(ctx, &req)
	if resp, ok := httpgrpc.HTTPResponseFromError(err); ok {
		// Wrap HTTP gRPC error with more explanatory message.
		return httpgrpc.Errorf(int(resp.Code), "failed pushing to ingester: %s", resp.Body)
	}
	return errors.Wrap(err, "failed pushing to ingester")
}

// forReplicationSet runs f, in parallel, for all ingesters in the input replication set.
func (d *Distributor) forReplicationSet(ctx context.Context, replicationSet ring.ReplicationSet, f func(context.Context, ingester_client.IngesterClient) (interface{}, error)) ([]interface{}, error) {
	return replicationSet.Do(ctx, 0, func(ctx context.Context, ing *ring.InstanceDesc) (interface{}, error) {
		client, err := d.ingesterPool.GetClientFor(ing.Addr)
		if err != nil {
			return nil, err
		}

		return f(ctx, client.(ingester_client.IngesterClient))
	})
}

// LabelValuesForLabelName returns all of the label values that are associated with a given label name.
func (d *Distributor) LabelValuesForLabelName(ctx context.Context, from, to model.Time, labelName model.LabelName, matchers ...*labels.Matcher) ([]string, error) {
	replicationSet, err := d.GetIngesters(ctx)
	if err != nil {
		return nil, err
	}

	req, err := ingester_client.ToLabelValuesRequest(labelName, from, to, matchers)
	if err != nil {
		return nil, err
	}

	resps, err := d.forReplicationSet(ctx, replicationSet, func(ctx context.Context, client ingester_client.IngesterClient) (interface{}, error) {
		return client.LabelValues(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	valueSet := map[string]struct{}{}
	for _, resp := range resps {
		for _, v := range resp.(*ingester_client.LabelValuesResponse).LabelValues {
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

// LabelNamesAndValues query ingesters for label names and values and returns labels with distinct list of values.
func (d *Distributor) LabelNamesAndValues(ctx context.Context, matchers []*labels.Matcher) (*ingester_client.LabelNamesAndValuesResponse, error) {
	replicationSet, err := d.GetIngesters(ctx)
	if err != nil {
		return nil, err
	}

	req, err := toLabelNamesCardinalityRequest(matchers)
	if err != nil {
		return nil, err
	}
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	sizeLimitBytes := d.limits.LabelNamesAndValuesResultsMaxSizeBytes(userID)
	merger := &labelNamesAndValuesResponseMerger{result: map[string]map[string]struct{}{}, sizeLimitBytes: sizeLimitBytes}
	_, err = d.forReplicationSet(ctx, replicationSet, func(ctx context.Context, client ingester_client.IngesterClient) (interface{}, error) {
		stream, err := client.LabelNamesAndValues(ctx, req)
		if err != nil {
			return nil, err
		}
		defer stream.CloseSend() //nolint:errcheck
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

func toLabelNamesCardinalityRequest(matchers []*labels.Matcher) (*ingester_client.LabelNamesAndValuesRequest, error) {
	matchersProto, err := ingester_client.ToLabelMatchers(matchers)
	if err != nil {
		return nil, err
	}
	return &ingester_client.LabelNamesAndValuesRequest{Matchers: matchersProto}, nil
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
func (d *Distributor) LabelValuesCardinality(ctx context.Context, labelNames []model.LabelName, matchers []*labels.Matcher) (uint64, *ingester_client.LabelValuesCardinalityResponse, error) {
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
		response, err := d.labelValuesCardinality(ctx, labelNames, matchers)
		if err == nil {
			labelValuesCardinalityResponse = response
		}
		return err
	})
	group.Go(func() error {
		response, err := d.UserStats(ctx)
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
func (d *Distributor) labelValuesCardinality(ctx context.Context, labelNames []model.LabelName, matchers []*labels.Matcher) (*ingester_client.LabelValuesCardinalityResponse, error) {
	replicationSet, err := d.GetIngesters(ctx)
	if err != nil {
		return nil, err
	}

	// Make sure we get a successful response from all the ingesters
	replicationSet.MaxErrors = 0
	replicationSet.MaxUnavailableZones = 0

	cardinalityConcurrentMap := &labelValuesCardinalityConcurrentMap{
		cardinalityMap: map[string]map[string]uint64{},
	}

	labelValuesReq, err := toLabelValuesCardinalityRequest(labelNames, matchers)
	if err != nil {
		return nil, err
	}

	_, err = d.forReplicationSet(ctx, replicationSet, func(ctx context.Context, client ingester_client.IngesterClient) (interface{}, error) {
		stream, err := client.LabelValuesCardinality(ctx, labelValuesReq)
		if err != nil {
			return nil, err
		}
		defer func() { _ = stream.CloseSend() }()

		return nil, cardinalityConcurrentMap.processLabelValuesCardinalityMessages(stream)
	})
	if err != nil {
		return nil, err
	}
	return cardinalityConcurrentMap.toLabelValuesCardinalityResponse(d.ingestersRing.ReplicationFactor()), nil
}

func toLabelValuesCardinalityRequest(labelNames []model.LabelName, matchers []*labels.Matcher) (*ingester_client.LabelValuesCardinalityRequest, error) {
	matchersProto, err := ingester_client.ToLabelMatchers(matchers)
	if err != nil {
		return nil, err
	}
	labelNamesStr := make([]string, 0, len(labelNames))
	for _, labelName := range labelNames {
		labelNamesStr = append(labelNamesStr, string(labelName))
	}
	return &ingester_client.LabelValuesCardinalityRequest{LabelNames: labelNamesStr, Matchers: matchersProto}, nil
}

type labelValuesCardinalityConcurrentMap struct {
	cardinalityMap map[string]map[string]uint64
	lock           sync.Mutex
}

func (cm *labelValuesCardinalityConcurrentMap) processLabelValuesCardinalityMessages(
	stream ingester_client.Ingester_LabelValuesCardinalityClient) error {
	for {
		message, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		cm.processLabelValuesCardinalityMessage(message)
	}
	return nil
}

/*
 * Build a map from all the responses received from all the ingesters.
 * Each label name will represent a key on the cardinalityMap which will have as value a second map, containing
 * as key the label_value and value the respective series_count. This series_count will represent the cumulative result
 * of all (label_name, label_value) tuples from all ingesters.
 *
 * Map: (label_name -> (label_value -> series_count))
 *
 * This method is called per each LabelValuesCardinalityResponse consumed from each ingester
 */
func (cm *labelValuesCardinalityConcurrentMap) processLabelValuesCardinalityMessage(
	message *ingester_client.LabelValuesCardinalityResponse) {

	cm.lock.Lock()
	defer cm.lock.Unlock()

	for _, item := range message.Items {
		if _, exists := cm.cardinalityMap[item.LabelName]; !exists {
			// Label name nonexistent
			cm.cardinalityMap[item.LabelName] = map[string]uint64{}
		}
		for labelValue, seriesCount := range item.LabelValueSeries {
			// Label name existent
			cm.cardinalityMap[item.LabelName][labelValue] += seriesCount
		}
	}
}

// toLabelValuesCardinalityResponse adjust count of series to the replication factor and converts the map to `ingester_client.LabelValuesCardinalityResponse`.
func (cm *labelValuesCardinalityConcurrentMap) toLabelValuesCardinalityResponse(replicationFactor int) *ingester_client.LabelValuesCardinalityResponse {
	// we need to acquire the lock to prevent concurrent read/write to the map
	cm.lock.Lock()
	defer cm.lock.Unlock()

	cardinalityItems := make([]*ingester_client.LabelValueSeriesCount, 0, len(cm.cardinalityMap))
	// Adjust label values' series count based on the ingester's replication factor
	for labelName, labelValueSeriesCountMap := range cm.cardinalityMap {
		adjustedSeriesCountMap := make(map[string]uint64, len(labelValueSeriesCountMap))
		for labelValue, seriesCount := range labelValueSeriesCountMap {
			adjustedSeriesCountMap[labelValue] = seriesCount / uint64(replicationFactor)
		}
		cardinalityItems = append(cardinalityItems, &ingester_client.LabelValueSeriesCount{
			LabelName:        labelName,
			LabelValueSeries: adjustedSeriesCountMap,
		})
	}

	return &ingester_client.LabelValuesCardinalityResponse{
		Items: cardinalityItems,
	}
}

// LabelNames returns all of the label names.
func (d *Distributor) LabelNames(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) ([]string, error) {
	replicationSet, err := d.GetIngesters(ctx)
	if err != nil {
		return nil, err
	}

	req, err := ingester_client.ToLabelNamesRequest(from, to, matchers)
	if err != nil {
		return nil, err
	}

	resps, err := d.forReplicationSet(ctx, replicationSet, func(ctx context.Context, client ingester_client.IngesterClient) (interface{}, error) {
		return client.LabelNames(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	valueSet := map[string]struct{}{}
	for _, resp := range resps {
		for _, v := range resp.(*ingester_client.LabelNamesResponse).LabelNames {
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

// MetricsForLabelMatchers gets the metrics that match said matchers
func (d *Distributor) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]labels.Labels, error) {
	replicationSet, err := d.GetIngesters(ctx)
	if err != nil {
		return nil, err
	}

	req, err := ingester_client.ToMetricsForLabelMatchersRequest(from, through, matchers)
	if err != nil {
		return nil, err
	}

	resps, err := d.forReplicationSet(ctx, replicationSet, func(ctx context.Context, client ingester_client.IngesterClient) (interface{}, error) {
		return client.MetricsForLabelMatchers(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	metrics := map[uint64]labels.Labels{}
	for _, resp := range resps {
		ms := ingester_client.FromMetricsForLabelMatchersResponse(resp.(*ingester_client.MetricsForLabelMatchersResponse))
		for _, m := range ms {
			metrics[m.Hash()] = m
		}
	}

	result := make([]labels.Labels, 0, len(metrics))
	for _, m := range metrics {
		result = append(result, m)
	}
	return result, nil
}

// MetricsMetadata returns all metric metadata of a user.
func (d *Distributor) MetricsMetadata(ctx context.Context) ([]scrape.MetricMetadata, error) {
	replicationSet, err := d.GetIngesters(ctx)
	if err != nil {
		return nil, err
	}

	req := &ingester_client.MetricsMetadataRequest{}
	resps, err := d.forReplicationSet(ctx, replicationSet, func(ctx context.Context, client ingester_client.IngesterClient) (interface{}, error) {
		return client.MetricsMetadata(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	result := []scrape.MetricMetadata{}
	dedupTracker := map[mimirpb.MetricMetadata]struct{}{}
	for _, resp := range resps {
		r := resp.(*ingester_client.MetricsMetadataResponse)
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
func (d *Distributor) UserStats(ctx context.Context) (*UserStats, error) {
	replicationSet, err := d.GetIngesters(ctx)
	if err != nil {
		return nil, err
	}

	// Make sure we get a successful response from all of them.
	replicationSet.MaxErrors = 0
	replicationSet.MaxUnavailableZones = 0

	req := &ingester_client.UserStatsRequest{}
	resps, err := d.forReplicationSet(ctx, replicationSet, func(ctx context.Context, client ingester_client.IngesterClient) (interface{}, error) {
		return client.UserStats(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	totalStats := &UserStats{}
	for _, resp := range resps {
		r := resp.(*ingester_client.UserStatsResponse)
		totalStats.IngestionRate += r.IngestionRate
		totalStats.APIIngestionRate += r.ApiIngestionRate
		totalStats.RuleIngestionRate += r.RuleIngestionRate
		totalStats.NumSeries += r.NumSeries
	}

	totalStats.IngestionRate /= float64(d.ingestersRing.ReplicationFactor())
	totalStats.NumSeries /= uint64(d.ingestersRing.ReplicationFactor())

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
	replicationSet, err := d.ingestersRing.GetAllHealthy(ring.Read)
	if err != nil {
		return nil, err
	}
	for _, ingester := range replicationSet.Instances {
		client, err := d.ingesterPool.GetClientFor(ingester.Addr)
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
