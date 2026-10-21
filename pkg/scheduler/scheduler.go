// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/scheduler.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package scheduler

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/querierpb" //lint:ignore faillint we can't avoid using this given that's where the Protobuf definition lives
	"github.com/grafana/mimir/pkg/queue"
	"github.com/grafana/mimir/pkg/scheduler/schedulerdiscovery"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/grpcencoding/s2"
	"github.com/grafana/mimir/pkg/util/httpgrpcutil"
	"github.com/grafana/mimir/pkg/util/rootqueryid"
)

var errEnqueuingRequestFailed = cancellation.NewErrorf("enqueuing request failed")
var errFrontendDisconnected = cancellation.NewErrorf("frontend disconnected")

var tracer = otel.Tracer("pkg/scheduler")

// Scheduler is responsible for queueing and dispatching queries to Queriers.
type Scheduler struct {
	services.Service

	cfg Config
	log log.Logger

	limits Limits

	connectedFrontendsMu sync.Mutex
	connectedFrontends   map[string]*connectedFrontend

	queue       *schedulerQueue
	activeUsers *util.ActiveUsersCleanupService

	inflightRequestsMu sync.Mutex
	// schedulerInflightRequests tracks requests from the time they are received to be enqueued by the scheduler
	// to the time they are completed by the querier or failed due to cancel, timeout, or disconnect.
	schedulerInflightRequests     map[RequestKey]*SchedulerRequest
	schedulerInflightRequestCount *atomic.Int64

	// Lock order: a goroutine that holds connectedFrontendsMu can take reservationsMu, never the
	// reverse.
	reservationsMu   sync.Mutex
	openReservations map[reservationKey]*openReservation
	// reservationsByFrontend indexes openReservations by frontend address, so the end of a
	// frontend's last stream visits only the reservations of that frontend.
	reservationsByFrontend map[string]map[reservationKey]*openReservation

	// The ring is used to let other components discover query-scheduler replicas.
	// The ring is optional.
	schedulerLifecycler *ring.BasicLifecycler

	// Subservices manager.
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher

	// Metrics.
	queueLength              *prometheus.GaugeVec
	maxQueueLength           *queue.MaxQueueLengthGauge
	discardedRequests        *prometheus.CounterVec
	cancelledRequests        *prometheus.CounterVec
	connectedQuerierClients  prometheus.GaugeFunc
	connectedFrontendClients prometheus.GaugeFunc
	queueDuration            *prometheus.HistogramVec
	inflightRequests         prometheus.Summary
	invalidClusterValidation *prometheus.CounterVec
	queueMaxWait             prometheus.Gauge

	reservationTimeouts    *prometheus.CounterVec
	incompleteReservations *prometheus.CounterVec
	discardedReservations  *prometheus.CounterVec
	unusedReservedSlots    *prometheus.GaugeVec
}

// reservationKey identifies one reservation that a frontend opened for its sub-requests. The
// frontend sets reservationID to a new UUID for each reservation, and must not reuse it on the
// same frontendAddress: a late abort closes the reservation that has the ID.
type reservationKey struct {
	frontendAddr  string
	reservationID string
}

// openReservation tracks the queue slots that the scheduler reserved for sub-requests that are not
// all inserted yet. reservationsMu guards inserted, inflight and deadline. The other fields do not
// change after the reservation is added to openReservations.
type openReservation struct {
	key              reservationKey
	tenantID         string
	totalSubRequests int
	// inserted counts the sub-requests that the queue enqueued. Each one uses a reserved slot.
	inserted int
	// inflight counts the sub-requests that claimed a reserved slot and whose enqueue has not
	// returned yet. A close does not release their slots, because it cannot know if the insert
	// succeeds. An inserted sub-request uses its slot. For a failed one, finishReservedEnqueue
	// releases the slot.
	inflight int
	// deadline is when the reaper closes the reservation, if no sub-request is in flight then.
	// Each start and each successful end of a reserved ENQUEUE moves it forward.
	deadline time.Time
}

var (
	errQueueReservationDisabled = errors.New("queue reservation is not enabled on this query-scheduler")
	errReservationClosed        = errors.New("unknown or already-closed reservation")
	errReservationOtherTenant   = errors.New("the reservation belongs to another tenant")
	errReservationOverDelivered = errors.New("reservation delivered more sub-requests than totalSubRequests")
)

type connectedFrontend struct {
	connections int

	// This context is used for running all queries from the same frontend.
	// When last frontend connection is closed, context is canceled.
	ctx    context.Context
	cancel context.CancelCauseFunc
}

type Config struct {
	MaxOutstandingPerTenant     int           `yaml:"max_outstanding_requests_per_tenant"`
	QuerierForgetDelay          time.Duration `yaml:"querier_forget_delay" category:"experimental"`
	QueueMaxWaitMetricEnabled   bool          `yaml:"queue_max_wait_metric_enabled" category:"experimental"`
	MaxQueueLengthMetricEnabled bool          `yaml:"max_queue_length_metric_enabled" category:"experimental"`
	QueueReservationEnabled     bool          `yaml:"queue_reservation_enabled" category:"experimental"`
	QueueReservationTimeout     time.Duration `yaml:"queue_reservation_timeout" category:"experimental"`

	GRPCClientConfig grpcclient.Config         `yaml:"grpc_client_config" doc:"description=This configures the gRPC client used to report errors back to the query-frontend."`
	ServiceDiscovery schedulerdiscovery.Config `yaml:",inline"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	f.IntVar(&cfg.MaxOutstandingPerTenant, "query-scheduler.max-outstanding-requests-per-tenant", 100, "Maximum number of outstanding requests per tenant per query-scheduler. In-flight requests above this limit will fail with HTTP response status code 429. When -query-scheduler.queue-reservation-enabled is true, the reserved queue slots also count against this limit.")
	f.DurationVar(&cfg.QuerierForgetDelay, "query-scheduler.querier-forget-delay", 0, "If a querier disconnects without sending notification about graceful shutdown, the query-scheduler will keep the querier in the tenant's shard until the forget delay has passed. This feature is useful to reduce the blast radius when shuffle-sharding is enabled.")
	f.BoolVar(&cfg.QueueMaxWaitMetricEnabled, "query-scheduler.queue-max-wait-metric-enabled", true, "Enable the cortex_query_scheduler_queue_max_wait_seconds metric, which reports how long the oldest request still waiting in the queue has been waiting. Disabling it skips the per-tick scan over inflight requests.")
	f.BoolVar(&cfg.MaxQueueLengthMetricEnabled, "query-scheduler.max-queue-length-metric-enabled", false, "Enable the cortex_query_scheduler_max_queue_length metric, which reports the per-tenant peak queue length observed since the last scrape. Disabling it skips per-tenant peak tracking on enqueue and dequeue.")
	f.BoolVar(&cfg.QueueReservationEnabled, "query-scheduler.queue-reservation-enabled", false, "Enable queue reservation: the query-frontend reserves queue slots for all sub-requests of a parent query before it sends them. A query is then either shed entirely before any sub-request is dispatched, or admitted with no tail sub-request rejected. Requires a query-frontend that also supports queue reservation; has no effect otherwise.")
	f.DurationVar(&cfg.QueueReservationTimeout, "query-scheduler.queue-reservation-timeout", 5*time.Second, "Maximum time an open reservation waits for its next sub-requests before the query-scheduler releases its unused queue slots. Sub-requests already in the queue stay there. Only applies when -query-scheduler.queue-reservation-enabled is true.")

	cfg.GRPCClientConfig.CustomCompressors = []string{s2.Name}
	cfg.GRPCClientConfig.RegisterFlagsWithPrefix("query-scheduler.grpc-client-config", f)
	cfg.ServiceDiscovery.RegisterFlags(f, logger)
}

var errInvalidQueueReservationTimeout = errors.New("the queue reservation timeout must be greater than 0 when queue reservation is enabled")

func (cfg *Config) Validate() error {
	if cfg.QueueReservationEnabled && cfg.QueueReservationTimeout <= 0 {
		return errInvalidQueueReservationTimeout
	}
	return cfg.ServiceDiscovery.Validate()
}

// NewScheduler creates a new Scheduler.
func NewScheduler(cfg Config, limits Limits, log log.Logger, registerer prometheus.Registerer) (*Scheduler, error) {
	var err error

	s := &Scheduler{
		cfg:    cfg,
		log:    log,
		limits: limits,

		schedulerInflightRequests:     map[RequestKey]*SchedulerRequest{},
		schedulerInflightRequestCount: atomic.NewInt64(0),
		connectedFrontends:            map[string]*connectedFrontend{},
		openReservations:              map[reservationKey]*openReservation{},
		reservationsByFrontend:        map[string]map[reservationKey]*openReservation{},
		subservicesWatcher:            services.NewFailureWatcher(),
	}

	s.queueLength = promauto.With(registerer).NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_query_scheduler_queue_length",
		Help: "Number of queries in the queue.",
	}, []string{"user"})

	if cfg.MaxQueueLengthMetricEnabled {
		s.maxQueueLength = queue.NewMaxQueueLengthGauge(
			"cortex_query_scheduler_max_queue_length",
			"Maximum number of queries observed in a tenant's queue since the last metric collection (reset on each scrape). Captures the true peak queue depth between scrapes.",
			nil,
		)
		// registerer is nil in some tests; mirror promauto.With(nil) by skipping registration.
		if registerer != nil {
			registerer.MustRegister(s.maxQueueLength)
		}
	}

	s.cancelledRequests = promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_scheduler_cancelled_requests_total",
		Help: "Total number of query requests that were cancelled after enqueuing.",
	}, []string{"user"})
	s.discardedRequests = promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_scheduler_discarded_requests_total",
		Help: "Total number of query requests discarded. A rejected queue reservation counts each of its sub-requests.",
	}, []string{"user"})
	enqueueDuration := promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
		Name: "cortex_query_scheduler_enqueue_duration_seconds",
		Help: "Time spent by requests waiting to join the queue or be rejected.",
	})
	if cfg.QueueMaxWaitMetricEnabled {
		s.queueMaxWait = promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_query_scheduler_queue_max_wait_seconds",
			Help: "Time the oldest request still waiting in the queue has spent waiting since it was enqueued. Requests already dispatched to a querier for execution are excluded. 0 if no requests are waiting in the queue.",
		})
	}
	querierInflightRequestsMetric := promauto.With(registerer).NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "cortex_query_scheduler_querier_inflight_requests",
			Help:       "Number of inflight requests being processed on all querier-scheduler connections. Quantile buckets keep track of inflight requests over the last 60s.",
			Objectives: map[float64]float64{0.5: 0.05, 0.75: 0.02, 0.8: 0.02, 0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
			MaxAge:     time.Minute,
			AgeBuckets: 6,
		},
		[]string{"query_component"},
	)
	s.invalidClusterValidation = util.NewRequestInvalidClusterValidationLabelsTotalCounter(registerer, "query-scheduler", util.GRPCProtocol)

	s.reservationTimeouts = promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_scheduler_reservation_timeouts_total",
		Help: "Total number of reservations released because no further sub-requests arrived within -query-scheduler.queue-reservation-timeout.",
	}, []string{"user"})
	s.incompleteReservations = promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_scheduler_reservation_incomplete_total",
		Help: "Total number of reservations closed with fewer sub-requests inserted than their total number of sub-requests, because of an abort, a reservation timeout, a sub-request failure or the end of the frontend's last stream.",
	}, []string{"user"})

	reservationSize := promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
		Name:                            "cortex_query_scheduler_reservation_size",
		Help:                            "Number of sub-requests in each accepted queue reservation.",
		Buckets:                         prometheus.ExponentialBuckets(1, 2, 11),
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMinResetDuration: 1 * time.Hour,
		NativeHistogramMaxBucketNumber:  100,
	})
	s.discardedReservations = promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_scheduler_reservation_discarded_total",
		Help: "Total number of queue reservations discarded because the tenant had too many outstanding requests.",
	}, []string{"user"})
	s.unusedReservedSlots = promauto.With(registerer).NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_query_scheduler_reservation_unused_slots",
		Help: "Number of reserved queue slots that no queued sub-request uses yet.",
	}, []string{"user"})

	s.queue, err = newSchedulerQueue(
		cfg,
		limits,
		s.log,
		s.queueLength,
		s.maxQueueLength,
		s.discardedRequests,
		enqueueDuration,
		querierInflightRequestsMetric,
		&queue.ReservationMetrics{
			Size:        reservationSize,
			Discarded:   s.discardedReservations,
			UnusedSlots: s.unusedReservedSlots,
		},
	)
	if err != nil {
		return nil, err
	}

	s.queueDuration = promauto.With(registerer).NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "cortex_query_scheduler_queue_duration_seconds",
		Help:                            "Time spent by requests in queue before getting picked up by a querier.",
		Buckets:                         []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120},
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMinResetDuration: 1 * time.Hour,
		NativeHistogramMaxBucketNumber:  100,
	}, []string{"user", "additional_queue_dimensions"})
	s.connectedQuerierClients = promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_query_scheduler_connected_querier_clients",
		Help: "Number of querier worker clients currently connected to the query-scheduler.",
	}, s.queue.GetConnectedQuerierWorkersMetric)
	s.connectedFrontendClients = promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_query_scheduler_connected_frontend_clients",
		Help: "Number of query-frontend worker clients currently connected to the query-scheduler.",
	}, s.getConnectedFrontendClientsMetric)

	s.inflightRequests = promauto.With(registerer).NewSummary(prometheus.SummaryOpts{
		Name:       "cortex_query_scheduler_inflight_requests",
		Help:       "Number of inflight requests (either queued or processing) sampled at a regular interval. Quantile buckets keep track of inflight requests over the last 60s.",
		Objectives: map[float64]float64{0.5: 0.05, 0.75: 0.02, 0.8: 0.02, 0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
		MaxAge:     time.Minute,
		AgeBuckets: 6,
	})

	s.activeUsers = util.NewActiveUsersCleanupWithDefaultValues(s.cleanupMetricsForInactiveUser)
	subservices := []services.Service{s.queue, s.activeUsers}
	if cfg.QueueReservationEnabled {
		// The reaper has its own goroutine. Its releases wait for the queue, so they must not delay
		// the in-flight request samples in running. It uses the same 250ms cadence as those samples.
		reaper := services.NewTimerService(250*time.Millisecond, nil, s.reapExpiredReservations, nil).WithName("reservation reaper")
		subservices = append(subservices, reaper)
	}

	// Init the ring only if the ring-based service discovery mode is used.
	if cfg.ServiceDiscovery.Mode == schedulerdiscovery.ModeRing {
		s.schedulerLifecycler, err = schedulerdiscovery.NewRingLifecycler(cfg.ServiceDiscovery.SchedulerRing, log, registerer)
		if err != nil {
			return nil, err
		}

		subservices = append(subservices, s.schedulerLifecycler)
	}

	s.subservices, err = services.NewManager(subservices...)
	if err != nil {
		return nil, err
	}

	s.Service = services.NewBasicService(s.starting, s.running, s.stopping)
	return s, nil
}

// Limits needed for the Query Scheduler - interface used for decoupling.
type Limits interface {
	// MaxQueriersPerUser returns max queriers to use per tenant, or 0 if shuffle sharding is disabled.
	MaxQueriersPerUser(user string) int
}

// FrontendLoop handles connection from frontend.
func (s *Scheduler) FrontendLoop(frontend schedulerpb.SchedulerForFrontend_FrontendLoopServer) error {
	frontendAddress, frontendCtx, err := s.frontendConnected(frontend)
	if err != nil {
		return err
	}
	defer s.frontendDisconnected(frontendAddress)

	// Response to INIT. If scheduler is not running, we skip for-loop, send SHUTTING_DOWN and exit this method.
	if s.State() == services.Running {
		if err := frontend.Send(&schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK, QueueReservationSupported: s.cfg.QueueReservationEnabled}); err != nil {
			return err
		}
	}

	// We stop accepting new queries in Stopping state. By returning quickly, we disconnect frontends, which in turns
	// cancels all their queries.
	for s.isRunning() {
		msg, err := frontend.Recv()
		if err != nil {
			// No need to report this as error, it is expected when query-frontend performs SendClose() (as frontendSchedulerWorker does).
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		if s.State() != services.Running {
			break // break out of the loop, and send SHUTTING_DOWN message.
		}

		var resp *schedulerpb.SchedulerToFrontend

		switch msg.GetType() {
		case schedulerpb.ENQUEUE:
			// Extract tracing information from headers in HTTP request. FrontendContext doesn't have the correct tracing
			// information, since that is a long-running request.
			var parentSpanContext context.Context

			switch req := msg.Payload.(type) {
			case *schedulerpb.FrontendToScheduler_HttpRequest:
				parentSpanContext, _ = httpgrpcutil.ContextWithSpanFromRequest(frontendCtx, req.HttpRequest)
			case *schedulerpb.FrontendToScheduler_ProtobufRequest:
				carrier := schedulerpb.MetadataMapTracingCarrier(schedulerpb.MetadataSliceToMap(req.ProtobufRequest.Metadata))
				parentSpanContext = otel.GetTextMapPropagator().Extract(frontendCtx, carrier)
			default:
				level.Debug(log.With(s.log, requestLogFields(msg.UserID, msg.QueryID, msg.RootQueryID)...)).Log("msg", "received a message that contained neither a HTTP nor a Protobuf payload, tracing information may be incomplete", "addr", frontendAddress)
			}

			reqCtx, enqueueSpan := tracer.Start(parentSpanContext, "enqueue")
			if msg.RootQueryID != "" {
				enqueueSpan.SetAttributes(attribute.String(rootqueryid.FieldName, msg.RootQueryID))
			}

			err = s.enqueueRequest(reqCtx, frontendAddress, msg)
			if err != nil {
				enqueueSpan.RecordError(err)
				if errors.Is(err, queue.ErrTooManyRequests) {
					level.Warn(log.With(s.log, requestLogFields(msg.UserID, msg.QueryID, msg.RootQueryID)...)).Log("msg", "rejected request because the tenant has too many outstanding requests", "addr", frontendAddress)
				}
			}
			resp = queueErrorResponse(err)

			enqueueSpan.End()

		case schedulerpb.RESERVE:
			resp = s.createReservation(frontendAddress, msg)

		case schedulerpb.CANCEL:
			// A CANCEL with queryID 0 and a non-empty reservationID aborts that reservation.
			if msg.QueryID == 0 && msg.ReservationID != "" {
				resp = s.abortReservation(frontendAddress, msg.ReservationID)
			} else {
				requestKey := NewSchedulerRequestKey(frontendAddress, msg.QueryID)
				schedulerReq := s.cancelRequestAndRemoveFromPending(requestKey, "frontend cancelled query")
				// we may not have reached MarkRequestSent for this query before receiving the cancel,
				// but RequestQueue will just no-op if the request was not yet tracked in the inflightRequests map.
				s.queue.MarkRequestCompleted(schedulerReq)
				resp = &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
			}

		default:
			level.Error(s.log).Log("msg", "unknown request type from frontend", "addr", frontendAddress, "type", msg.GetType())
			return errors.New("unknown request type")
		}

		err = frontend.Send(resp)
		// Failure to send response results in ending this connection.
		if err != nil {
			return err
		}
	}

	// Report shutdown back to frontend, so that it can retry with different scheduler. Also stop the frontend loop.
	return frontend.Send(&schedulerpb.SchedulerToFrontend{Status: schedulerpb.SHUTTING_DOWN})
}

func (s *Scheduler) frontendConnected(frontend schedulerpb.SchedulerForFrontend_FrontendLoopServer) (string, context.Context, error) {
	msg, err := frontend.Recv()
	if err != nil {
		return "", nil, err
	}
	if msg.Type != schedulerpb.INIT || msg.FrontendAddress == "" {
		return "", nil, errors.New("no frontend address")
	}

	s.connectedFrontendsMu.Lock()
	defer s.connectedFrontendsMu.Unlock()

	cf := s.connectedFrontends[msg.FrontendAddress]
	if cf == nil {
		cf = &connectedFrontend{
			connections: 0,
		}
		cf.ctx, cf.cancel = context.WithCancelCause(context.Background())
		s.connectedFrontends[msg.FrontendAddress] = cf
	}

	cf.connections++
	return msg.FrontendAddress, cf.ctx, nil
}

func (s *Scheduler) frontendDisconnected(frontendAddress string) {
	s.connectedFrontendsMu.Lock()
	cf := s.connectedFrontends[frontendAddress]
	cf.connections--
	var closed []closedReservation
	if cf.connections == 0 {
		delete(s.connectedFrontends, frontendAddress)
		cf.cancel(errFrontendDisconnected)
		// removeReservationsOfFrontend runs before the unlock. A new stream of this frontend waits
		// for connectedFrontendsMu, so it cannot open a reservation that this removal deletes.
		closed = s.removeReservationsOfFrontend(frontendAddress)
	}
	s.connectedFrontendsMu.Unlock()

	s.releaseClosedReservations(closed, "frontend disconnected")
	for _, c := range closed {
		s.countClosedReservation(c.tenantID, false, c.incomplete)
	}
}

// enqueueRequest enqueues the request in msg. An ENQUEUE with a reservationID uses one reserved
// slot of that reservation: startReservedEnqueue claims the slot before the insert, and
// finishReservedEnqueue records the result after it.
func (s *Scheduler) enqueueRequest(requestContext context.Context, frontendAddr string, msg *schedulerpb.FrontendToScheduler) error {
	// Create new context for this request, to support cancellation.
	ctx, cancel := context.WithCancelCause(requestContext)
	shouldCancel := true
	defer func() {
		if shouldCancel {
			cancel(errEnqueuingRequestFailed)
		}
	}()

	userID := msg.GetUserID()

	req := &SchedulerRequest{
		FrontendAddr:              frontendAddr,
		UserID:                    msg.UserID,
		QueryID:                   msg.QueryID,
		RootQueryID:               msg.RootQueryID,
		StatsEnabled:              msg.StatsEnabled,
		AdditionalQueueDimensions: msg.AdditionalQueueDimensions,
	}

	switch p := msg.Payload.(type) {
	case *schedulerpb.FrontendToScheduler_HttpRequest:
		req.HttpRequest = p.HttpRequest
	case *schedulerpb.FrontendToScheduler_ProtobufRequest:
		req.ProtobufRequest = p.ProtobufRequest
	default:
		return fmt.Errorf("unsupported payload type: %T", msg.Payload)
	}

	now := time.Now()

	enqueue := s.queue.Enqueue
	var reservation *openReservation
	if msg.ReservationID != "" {
		key := reservationKey{frontendAddr: frontendAddr, reservationID: msg.ReservationID}
		var err error
		if reservation, err = s.startReservedEnqueue(key, msg.UserID, now); err != nil {
			return err
		}
		enqueue = s.queue.EnqueueIntoSlot
	}

	req.ParentSpanContext = trace.SpanContextFromContext(requestContext)
	// The root query ID is not set on this span: it is already on the "enqueue" span this one
	// descends from
	req.Ctx, req.QueueSpan = tracer.Start(ctx, "queued")
	req.EnqueueTime = now
	req.CancelFunc = cancel

	err := enqueue(req, func() {
		shouldCancel = false
		s.activeUsers.UpdateUserTimestamp(userID, now)
		s.addRequestToPending(req)
	})
	if reservation != nil {
		s.finishReservedEnqueue(reservation, err)
	}
	return err
}

// createReservation handles a RESERVE. It reserves msg.TotalSubRequests queue slots for
// msg.UserID, and opens a reservation for frontendAddress and msg.ReservationID. Each reserved
// ENQUEUE then uses one slot. The reservation closes when all its sub-requests are inserted. An
// abort, a reservation timeout, a failed sub-request or the end of the frontend's last stream
// closes it early and releases its unused slots.
func (s *Scheduler) createReservation(frontendAddress string, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
	if !s.cfg.QueueReservationEnabled {
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: errQueueReservationDisabled.Error()}
	}

	if msg.TotalSubRequests == 0 || msg.TotalSubRequests > uint64(math.MaxInt) {
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: "invalid total sub-requests count"}
	}

	if msg.ReservationID == "" {
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: "invalid reservation ID"}
	}

	count := int(msg.TotalSubRequests)
	tenantID := msg.UserID

	if err := s.queue.ReserveSlots(tenantID, count); err != nil {
		var rejected *queue.ReservationRejectedError
		if errors.As(err, &rejected) {
			level.Warn(s.log).Log(
				"msg", "rejected reservation because the tenant has too many outstanding requests",
				"tenant", tenantID, "addr", frontendAddress, "reservation_id", msg.ReservationID,
				"requested", rejected.Requested, "queued", rejected.Queued, "reserved", rejected.Reserved,
				"max_tenant_queue_size", rejected.MaxTenantQueueSize,
			)
			s.activeUsers.UpdateUserTimestamp(tenantID, time.Now())
		}
		return queueErrorResponse(err)
	}

	// The reservation goes into openReservations only after the queue reserved its slots. An abort
	// that the scheduler processes before this point finds no reservation. The reservation timeout
	// then releases the slots.
	committed := s.commitReservation(&openReservation{
		key:              reservationKey{frontendAddr: frontendAddress, reservationID: msg.ReservationID},
		tenantID:         tenantID,
		totalSubRequests: count,
	})
	s.activeUsers.UpdateUserTimestamp(tenantID, time.Now())
	if !committed {
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: "a reservation with this ID is already open"}
	}
	return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
}

// finishReservedEnqueue records the insert result of a reserved ENQUEUE that startReservedEnqueue
// admitted, and releases the slots that the result frees.
//
// An inserted sub-request stays in the queue even when another goroutine closed its reservation
// meanwhile, as a plain ENQUEUE request does.
func (s *Scheduler) finishReservedEnqueue(res *openReservation, enqueueErr error) {
	release, incomplete := s.recordReservedEnqueueResult(res, enqueueErr == nil, time.Now())
	s.releaseUnusedSlots(res.tenantID, release, "sub-request enqueue failed")
	s.countClosedReservation(res.tenantID, false, incomplete)
}

// abortReservation removes the reservation and releases its unused slots. Inserted sub-requests
// stay in the queue. The frontend cancels them with a CANCEL per query when it needs to.
func (s *Scheduler) abortReservation(frontendAddress string, reservationID string) *schedulerpb.SchedulerToFrontend {
	ok := &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	// The frontend can send a CANCEL on any of its streams to this scheduler, so an abort closes
	// the reservation whatever stream opened it. An unknown reservation is a no-op: it already
	// completed, timed out or never existed, or its RESERVE has not added it yet.
	key := reservationKey{frontendAddr: frontendAddress, reservationID: reservationID}
	s.reservationsMu.Lock()
	res := s.openReservations[key]
	if res == nil {
		s.reservationsMu.Unlock()
		return ok
	}
	c := s.removeReservationLocked(res)
	s.reservationsMu.Unlock()

	s.releaseUnusedSlots(c.tenantID, c.release, "abort")
	s.countClosedReservation(c.tenantID, false, c.incomplete)
	return ok
}

// closedReservation is a reservation that left openReservations. release is the number of its
// reserved slots that the closer must release.
type closedReservation struct {
	reservationID    string
	tenantID         string
	totalSubRequests int
	inserted         int
	release          int
	incomplete       bool
}

// removeReservationsOfFrontend removes every reservation of frontendAddress and returns them.
// frontendDisconnected calls it when the frontend's last stream ends, because any stream of the
// frontend can use a reservation.
func (s *Scheduler) removeReservationsOfFrontend(frontendAddress string) []closedReservation {
	s.reservationsMu.Lock()
	defer s.reservationsMu.Unlock()

	frontendReservations := s.reservationsByFrontend[frontendAddress]
	delete(s.reservationsByFrontend, frontendAddress)
	closed := make([]closedReservation, 0, len(frontendReservations))
	for _, res := range frontendReservations {
		closed = append(closed, s.removeReservationLocked(res))
	}
	return closed
}

// commitReservation adds res to openReservations at res.key, after the queue reserved its slots.
// It sets the deadline that reapExpiredReservations checks. From then on, res owns the slots.
// When another open reservation already uses res.key, it releases the slots of res and returns
// false.
func (s *Scheduler) commitReservation(res *openReservation) bool {
	s.reservationsMu.Lock()
	_, exists := s.openReservations[res.key]
	if !exists {
		res.deadline = time.Now().Add(s.cfg.QueueReservationTimeout)
		s.openReservations[res.key] = res
		frontendReservations := s.reservationsByFrontend[res.key.frontendAddr]
		if frontendReservations == nil {
			// The frontend keeps this map until its last stream ends.
			frontendReservations = map[reservationKey]*openReservation{}
			s.reservationsByFrontend[res.key.frontendAddr] = frontendReservations
		}
		frontendReservations[res.key] = res
	}
	s.reservationsMu.Unlock()

	if exists {
		s.releaseUnusedSlots(res.tenantID, res.totalSubRequests, "duplicate reservation")
		return false
	}
	return true
}

// deleteReservationLocked removes the reservation at key from openReservations and from the index
// of its frontend. The caller must hold reservationsMu.
func (s *Scheduler) deleteReservationLocked(key reservationKey) {
	delete(s.openReservations, key)
	delete(s.reservationsByFrontend[key.frontendAddr], key)
}

// startReservedEnqueue claims one reserved slot of the open reservation at key for a reserved
// ENQUEUE that is about to go to the queue, and returns the reservation. It fails when no open
// reservation is at key, when userID is not the reservation's tenant, or when the claim exceeds
// totalSubRequests. With queue reservation disabled, no reservation is open. now moves the
// reservation's deadline forward. The caller reads the clock, so the claim does not read it under
// reservationsMu.
func (s *Scheduler) startReservedEnqueue(key reservationKey, userID string, now time.Time) (*openReservation, error) {
	s.reservationsMu.Lock()
	defer s.reservationsMu.Unlock()
	res := s.openReservations[key]
	switch {
	case res == nil:
		return nil, errReservationClosed
	case userID != res.tenantID:
		return nil, errReservationOtherTenant
	case res.inserted+res.inflight >= res.totalSubRequests:
		return nil, errReservationOverDelivered
	}
	res.inflight++
	s.extendDeadlineLocked(res, now)
	return res, nil
}

// recordReservedEnqueueResult records the insert result of a reserved ENQUEUE that
// startReservedEnqueue admitted. It returns the number of slots that the caller must release, and
// whether the caller must count the reservation as incomplete. A successful insert moves the
// deadline of the open reservation forward from now.
//
// A close excludes in-flight slots from its release. So when another goroutine closed res, a
// failed sub-request still holds its slot, and only its caller can release it. That close also
// leaves the incomplete decision to the last in-flight sub-request, because an in-flight
// sub-request can still be inserted.
func (s *Scheduler) recordReservedEnqueueResult(res *openReservation, inserted bool, now time.Time) (release int, incomplete bool) {
	s.reservationsMu.Lock()
	defer s.reservationsMu.Unlock()
	res.inflight--
	if inserted {
		res.inserted++
	}

	if s.openReservations[res.key] != res {
		if !inserted {
			release = 1
		}
		return release, res.inflight == 0 && res.inserted < res.totalSubRequests
	}
	if !inserted {
		c := s.removeReservationLocked(res)
		return c.release, c.incomplete
	}
	if res.inserted >= res.totalSubRequests {
		s.deleteReservationLocked(res.key)
		return 0, false
	}
	// The reservation timeout is an idle timeout. A slow insert must not use up the time that the
	// frontend has for its next sub-request.
	s.extendDeadlineLocked(res, now)
	return 0, false
}

// extendDeadlineLocked moves the deadline of res to now plus the reservation timeout. It never
// moves the deadline back, because concurrent callers can read the clock out of order. The caller
// must hold reservationsMu.
func (s *Scheduler) extendDeadlineLocked(res *openReservation, now time.Time) {
	if deadline := now.Add(s.cfg.QueueReservationTimeout); deadline.After(res.deadline) {
		res.deadline = deadline
	}
}

// removeReservationLocked deletes res from openReservations and from the index of its frontend.
// The caller must hold reservationsMu, and res must be the reservation at res.key.
//
// The returned release excludes the slots of in-flight sub-requests. The returned incomplete is
// true when res closes with fewer sub-requests than totalSubRequests. When sub-requests are still
// in flight, it is false, and the last of them decides in recordReservedEnqueueResult instead.
func (s *Scheduler) removeReservationLocked(res *openReservation) closedReservation {
	s.deleteReservationLocked(res.key)
	return closedReservation{
		reservationID:    res.key.reservationID,
		tenantID:         res.tenantID,
		totalSubRequests: res.totalSubRequests,
		inserted:         res.inserted,
		release:          res.totalSubRequests - res.inserted - res.inflight,
		incomplete:       res.inflight == 0 && res.inserted < res.totalSubRequests,
	}
}

// releaseClosedReservations releases the unused slots of closed, with one queue operation per
// tenant.
func (s *Scheduler) releaseClosedReservations(closed []closedReservation, reason string) {
	if len(closed) == 0 {
		return
	}
	releaseByTenant := make(map[string]int, len(closed))
	for _, c := range closed {
		releaseByTenant[c.tenantID] += c.release
	}
	for tenantID, release := range releaseByTenant {
		s.releaseUnusedSlots(tenantID, release, reason)
	}
}

// releaseUnusedSlots releases count reserved slots for tenantID.
func (s *Scheduler) releaseUnusedSlots(tenantID string, count int, reason string) {
	if count <= 0 {
		return
	}
	if err := s.queue.ReleaseSlots(tenantID, count); err != nil {
		// During shutdown the queue is stopped, and a failed release is expected.
		logger := level.Error(s.log)
		if errors.Is(err, queue.ErrStopped) {
			logger = level.Debug(s.log)
		}
		logger.Log("msg", "failed to release reserved slots", "reason", reason, "tenant", tenantID, "slots", count, "err", err)
	}
}

// countClosedReservation counts a closed reservation of tenantID in reservationTimeouts when
// timedOut is true, and in incompleteReservations when incomplete is true. The caller releases the
// reservation's unused slots first, so an observed counter means that the release is done.
//
// incomplete is true when the reservation ended with fewer inserted sub-requests than
// totalSubRequests. Only one caller per reservation passes true. A close while sub-requests are in
// flight passes false, because an in-flight sub-request can still be inserted. The last in-flight
// sub-request then decides in recordReservedEnqueueResult.
func (s *Scheduler) countClosedReservation(tenantID string, timedOut, incomplete bool) {
	if timedOut {
		s.reservationTimeouts.WithLabelValues(tenantID).Inc()
	}
	if incomplete {
		s.incompleteReservations.WithLabelValues(tenantID).Inc()
	}
	if timedOut || incomplete {
		// The counters can change after the tenant's last request, so the tenant stays active until
		// a later cleanup deletes them. The update must come after the increments: a purge between
		// the lookup and the store of UpdateUserTimestamp loses the update, and a counter change
		// after that would leave a counter that no cleanup deletes.
		s.activeUsers.UpdateUserTimestamp(tenantID, time.Now())
	}
}

// queueErrorResponse maps the result of a queue operation to the frontend status.
func queueErrorResponse(err error) *schedulerpb.SchedulerToFrontend {
	switch {
	case err == nil:
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	case errors.Is(err, queue.ErrTooManyRequests):
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.TOO_MANY_REQUESTS_PER_TENANT}
	case errors.Is(err, queue.ErrStopped):
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.SHUTTING_DOWN}
	default:
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: err.Error()}
	}
}

func (s *Scheduler) addRequestToPending(req *SchedulerRequest) {
	s.inflightRequestsMu.Lock()
	defer s.inflightRequestsMu.Unlock()

	s.schedulerInflightRequests[req.Key()] = req
	s.schedulerInflightRequestCount.Store(int64(len(s.schedulerInflightRequests)))
}

func (s *Scheduler) markRequestDispatched(req *SchedulerRequest) {
	req.Dispatched.Store(true)
}

// This method doesn't do removal from the queue.
func (s *Scheduler) cancelRequestAndRemoveFromPending(key RequestKey, reason string) *SchedulerRequest {
	s.inflightRequestsMu.Lock()
	defer s.inflightRequestsMu.Unlock()

	req := s.schedulerInflightRequests[key]
	if req != nil {
		req.CancelFunc(cancellation.NewError(errors.New(reason)))
	}

	delete(s.schedulerInflightRequests, key)
	s.schedulerInflightRequestCount.Store(int64(len(s.schedulerInflightRequests)))
	return req
}

func (s *Scheduler) transformRequestQueueError(err error) error {
	if errors.Is(err, queue.ErrStopped) && !s.isRunning() {
		// Return a more clear error if the queue is stopped because the query-scheduler is not running.
		return schedulerpb.ErrSchedulerIsNotRunning
	}

	// the main other error we receive here is if the querier itself is ErrQuerierShuttingDown;
	// this information was submitted via another endpoint and processed internally
	// by the RequestQueue's tracking of querier connections. ErrQuerierShuttingDown bubbles up here
	// as a way to exit this loop and allow the querier to shut down gracefully.
	return err
}

// QuerierLoop is started by querier to receive queries from scheduler.
func (s *Scheduler) QuerierLoop(querier schedulerpb.SchedulerForQuerier_QuerierLoopServer) error {
	resp, err := querier.Recv()
	if err != nil {
		return err
	}

	querierID := resp.GetQuerierID()
	querierWorkerConn := queue.NewUnregisteredConsumerWorkerConn(querier.Context(), querierID)
	err = s.queue.AwaitRegisterQuerierWorkerConn(querierWorkerConn)
	if err != nil {
		return s.transformRequestQueueError(err)
	}
	defer s.queue.SubmitUnregisterQuerierWorkerConn(querierWorkerConn)

	lastTenantIdx := queue.FirstTenant()

	// In stopping state scheduler is not accepting new queries, but still dispatching queries in the queues.
	for s.isRunningOrStopping() {
		dequeueReq := queue.NewConsumerWorkerDequeueRequest(querierWorkerConn, lastTenantIdx)
		queryReq, idx, err := s.queue.AwaitRequestForQuerier(dequeueReq)
		if err != nil {
			// The error returned can either be ErrQuerierShuttingDown or ErrQuerierWorkerDisconnected.
			// ErrQuerierShuttingDown is a control-flow message between services to exit this loop.
			// (see note in transformRequestQueueError).
			// ErrQuerierWorkerDisconnected is caused by connection/goroutine crash;
			// we expect this loop is no longer alive to receive the error anyway.
			level.Info(s.log).Log("msg", "AwaitRequestForQuerier returned an error", "err", err)
			return s.transformRequestQueueError(err)
		}

		lastTenantIdx = idx

		schedulerReq := queryReq.(*SchedulerRequest)

		queueTime := time.Since(schedulerReq.EnqueueTime)
		additionalQueueDimensionLabels := strings.Join(schedulerReq.AdditionalQueueDimensions, ":")
		s.queueDuration.WithLabelValues(schedulerReq.UserID, additionalQueueDimensionLabels).Observe(queueTime.Seconds())
		schedulerReq.QueueSpan.End()

		/*
		  We want to dequeue the next unexpired request from the chosen tenant queue.
		  The chance of choosing a particular tenant for dequeuing is (1/active_tenants).
		  This is problematic under load, especially with other middleware enabled such as
		  querier.split-by-interval, where one request may fan out into many.
		  If expired requests aren't exhausted before checking another tenant, it would take
		  n_active_tenants * n_expired_requests_at_front_of_queue requests being processed
		  before an active request was handled for the tenant in question.
		  If this tenant meanwhile continued to queue requests,
		  it's possible that its own queue would perpetually contain only expired requests.
		*/

		if schedulerReq.Ctx.Err() != nil {
			// remove from pending requests
			s.cancelRequestAndRemoveFromPending(schedulerReq.Key(), "request cancelled")
			s.queue.MarkRequestCompleted(schedulerReq)
			lastTenantIdx = lastTenantIdx.ReuseLastTenant()
			continue
		}

		if err := s.forwardRequestToQuerier(querier, querierID, schedulerReq, queueTime); err != nil {
			return err
		}
	}

	level.Info(s.log).Log("msg", "query loop closed because scheduler is not running", "querier_id", querierID)

	return schedulerpb.ErrSchedulerIsNotRunning
}

func (s *Scheduler) NotifyQuerierShutdown(ctx context.Context, req *schedulerpb.NotifyQuerierShutdownRequest) (*schedulerpb.NotifyQuerierShutdownResponse, error) {
	level.Info(s.log).Log("msg", "received shutdown notification from querier", "querier", req.GetQuerierID())

	s.queue.SubmitNotifyQuerierShutdown(ctx, req.GetQuerierID())

	return &schedulerpb.NotifyQuerierShutdownResponse{}, nil
}

func (s *Scheduler) forwardRequestToQuerier(querier schedulerpb.SchedulerForQuerier_QuerierLoopServer, querierID string, req *SchedulerRequest, queueTime time.Duration) error {
	s.markRequestDispatched(req)
	s.queue.MarkRequestSent(req)
	defer s.queue.MarkRequestCompleted(req)
	defer s.cancelRequestAndRemoveFromPending(req.Key(), "request complete")

	// Handle the stream sending & receiving on a goroutine so we can
	// monitor the contexts in a select and cancel things appropriately.
	errCh := make(chan error, 1)
	go func() {
		_, span := tracer.Start(req.Ctx, "forwardRequestToQuerier")
		span.SetAttributes(attribute.String("querier_id", querierID))

		msg := &schedulerpb.SchedulerToQuerier{
			UserID:          req.UserID,
			QueryID:         req.QueryID,
			RootQueryID:     req.RootQueryID,
			FrontendAddress: req.FrontendAddr,
			StatsEnabled:    req.StatsEnabled,
			QueueTimeNanos:  queueTime.Nanoseconds(),
		}

		if req.HttpRequest != nil {
			msg.Payload = &schedulerpb.SchedulerToQuerier_HttpRequest{HttpRequest: req.HttpRequest}
		} else if req.ProtobufRequest != nil {
			msg.Payload = &schedulerpb.SchedulerToQuerier_ProtobufRequest{ProtobufRequest: req.ProtobufRequest}
		}

		var err error

		if msg.Payload == nil {
			err = errors.New("queued request contains neither a HTTP nor a Protobuf payload, this should never happen")
		} else {
			err = querier.Send(msg)
		}

		if grpcutil.IsCanceled(err) {
			// The querier abruptly closing the connection should be the only reason we'd get a cancellation error here.
			err = fmt.Errorf("querier disconnected ungracefully")
		}

		if err != nil {
			errCh <- fmt.Errorf("failed to send query to querier '%v': %w", querierID, err)
			span.AddEvent("log", trace.WithAttributes(attribute.String("msg", "sending query to querier failed")))
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, err.Error())
			span.End()
			return
		}

		span.End()

		_, err = querier.Recv()

		if grpcutil.IsCanceled(err) {
			// The querier abruptly closing the connection should be the only reason we'd get a cancellation error here.
			err = fmt.Errorf("querier disconnected ungracefully")
		}

		if err != nil {
			errCh <- fmt.Errorf("failed to receive response from querier '%v': %w", querierID, err)
			return
		}

		errCh <- nil
	}()

	select {
	case <-req.Ctx.Done():
		// If the upstream request is cancelled (eg. frontend issued CANCEL or closed connection),
		// we need to cancel the downstream req. Only way we can do that is to return a gRPC error
		// here with code Canceled and close the stream.
		// Querier is expecting this semantics.
		s.cancelledRequests.WithLabelValues(req.UserID).Inc()
		return status.Error(codes.Canceled, context.Cause(req.Ctx).Error())

	case err := <-errCh:
		// Is there was an error handling this request due to network IO,
		// then error out this upstream request _and_ stream.

		if err != nil {
			s.forwardErrorToFrontend(req.Ctx, req, err)
		}
		return err
	}
}

func (s *Scheduler) forwardErrorToFrontend(ctx context.Context, req *SchedulerRequest, requestErr error) {
	logger := log.With(s.log, req.LogFields()...)

	opts, err := s.cfg.GRPCClientConfig.DialOption(
		[]grpc.UnaryClientInterceptor{
			middleware.ClientUserHeaderInterceptor,
		},
		[]grpc.StreamClientInterceptor{
			middleware.StreamClientUserHeaderInterceptor,
		},
		util.NewInvalidClusterValidationReporter(s.cfg.GRPCClientConfig.ClusterValidation.Label, s.invalidClusterValidation, s.log),
	)
	if err != nil {
		level.Warn(logger).Log("msg", "failed to create gRPC options for the connection to frontend to report error", "frontend", req.FrontendAddr, "err", err, "requestErr", requestErr)
		return
	}

	// nolint:staticcheck // grpc.DialContext() has been deprecated; we'll address it before upgrading to gRPC 2.
	conn, err := grpc.DialContext(ctx, req.FrontendAddr, opts...)
	if err != nil {
		level.Warn(logger).Log("msg", "failed to create gRPC connection to frontend to report error", "frontend", req.FrontendAddr, "err", err, "requestErr", requestErr)
		return
	}

	defer func() {
		_ = conn.Close()
	}()

	client := frontendv2pb.NewFrontendForQuerierClient(conn)

	userCtx := user.InjectOrgID(ctx, req.UserID)

	if req.HttpRequest != nil {
		_, err = client.QueryResult(userCtx, &frontendv2pb.QueryResultRequest{
			QueryID: req.QueryID,
			HttpResponse: &httpgrpc.HTTPResponse{
				Code: http.StatusInternalServerError,
				Body: []byte(requestErr.Error()),
			},
		})

		if err != nil {
			level.Warn(logger).Log("msg", "failed to forward error to frontend", "frontend", req.FrontendAddr, "err", err, "requestErr", requestErr)
			return
		}
	} else { // Protobuf request, so send a streaming response.
		stream, err := client.QueryResultStream(userCtx)
		if err != nil {
			level.Warn(logger).Log("msg", "failed to create stream to forward error to frontend", "frontend", req.FrontendAddr, "err", err, "requestErr", requestErr)
			return
		}

		msg := frontendv2pb.QueryResultStreamRequest{
			QueryID: req.QueryID,
			Data: &frontendv2pb.QueryResultStreamRequest_Error{
				Error: &querierpb.Error{
					Type:    mimirpb.QUERY_ERROR_TYPE_INTERNAL,
					Message: requestErr.Error(),
				},
			},
		}

		if err := stream.Send(&msg); err != nil {
			level.Warn(logger).Log("msg", "failed to forward error to frontend", "frontend", req.FrontendAddr, "err", err, "requestErr", requestErr)
			return
		}

		if _, err = stream.CloseAndRecv(); err != nil {
			level.Warn(logger).Log("msg", "failed to close stream to frontend after forwarding error", "frontend", req.FrontendAddr, "err", err, "requestErr", requestErr)
			return
		}
	}
}

func (s *Scheduler) isRunning() bool {
	st := s.State()
	return st == services.Running
}

func (s *Scheduler) isRunningOrStopping() bool {
	st := s.State()
	return st == services.Running || st == services.Stopping
}

func (s *Scheduler) starting(ctx context.Context) error {
	s.subservicesWatcher.WatchManager(s.subservices)

	if err := services.StartManagerAndAwaitHealthy(ctx, s.subservices); err != nil {
		return fmt.Errorf("unable to start scheduler subservices: %w", err)
	}

	return nil
}

func (s *Scheduler) running(ctx context.Context) error {
	// We observe inflight requests frequently and at regular intervals, to have a good
	// approximation of max inflight requests over percentiles of time. We also do it with
	// a ticker so that we keep tracking it even if we have no new queries but stuck inflight
	// requests (e.g. queriers are all crashing).
	inflightRequestsTicker := time.NewTicker(250 * time.Millisecond)
	defer inflightRequestsTicker.Stop()

	for {
		select {
		case <-inflightRequestsTicker.C:
			s.inflightRequests.Observe(float64(s.schedulerInflightRequestCount.Load()))
			if s.cfg.QueueMaxWaitMetricEnabled {
				s.observeQueueMaxWait()
			}
		case <-ctx.Done():
			return nil
		case err := <-s.subservicesWatcher.Chan():
			return fmt.Errorf("scheduler subservice failed: %w", err)
		}
	}
}

// reapExpiredReservations removes every open reservation whose deadline has passed and that has no
// sub-request in flight, and releases its unused slots. An in-flight sub-request shows that the
// frontend still sends, and the end of its enqueue moves the deadline forward.
func (s *Scheduler) reapExpiredReservations(_ context.Context) error {
	now := time.Now()

	s.reservationsMu.Lock()
	var expired []closedReservation
	for _, res := range s.openReservations {
		if res.inflight > 0 || !now.After(res.deadline) {
			continue
		}
		expired = append(expired, s.removeReservationLocked(res))
	}
	s.reservationsMu.Unlock()

	s.releaseClosedReservations(expired, "timeout")
	for _, c := range expired {
		s.countClosedReservation(c.tenantID, true, c.incomplete)
		level.Warn(s.log).Log(
			"msg", "reservation timed out, so the query-scheduler released its unused slots",
			"tenant", c.tenantID, "reservation_id", c.reservationID,
			"total_sub_requests", c.totalSubRequests, "inserted", c.inserted, "released", c.release,
		)
	}
	return nil
}

// Close the Scheduler.
func (s *Scheduler) stopping(_ error) error {
	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Minute) // 10 minute hard upper timeout to avoid blocking forever in worst case scenario
	defer cancel()

	// This will also stop the requests queue, which stop accepting new requests and wait for any pending requests to be dispatched to queriers.
	return services.StopManagerAndAwaitStopped(timeout, s.subservices)
}

func (s *Scheduler) cleanupMetricsForInactiveUser(user string) {
	s.queueLength.DeleteLabelValues(user)
	if s.maxQueueLength != nil {
		s.maxQueueLength.DeleteTenant(user)
	}
	s.discardedRequests.DeleteLabelValues(user)
	s.cancelledRequests.DeleteLabelValues(user)
	s.reservationTimeouts.DeleteLabelValues(user)
	s.incompleteReservations.DeleteLabelValues(user)
	s.discardedReservations.DeleteLabelValues(user)

	// A deleted unusedReservedSlots gauge restarts at 0, and a later release takes it negative. So
	// the queue deletes it on its dispatcher goroutine, and only while user holds no reserved slot.
	// Without queue reservation the gauge never exists, and the call would only wait on the queue.
	if s.cfg.QueueReservationEnabled {
		if err := s.queue.DeleteUnusedSlotsGauge(user); err != nil && !errors.Is(err, queue.ErrStopped) {
			level.Warn(s.log).Log("msg", "failed to delete the reserved slots metric", "user", user, "err", err)
		}
	}
}

func (s *Scheduler) getConnectedFrontendClientsMetric() float64 {
	s.connectedFrontendsMu.Lock()
	defer s.connectedFrontendsMu.Unlock()

	count := 0
	for _, workers := range s.connectedFrontends {
		count += workers.connections
	}

	return float64(count)
}

func (s *Scheduler) observeQueueMaxWait() {
	if s.schedulerInflightRequestCount.Load() == 0 {
		// no inflight requests
		s.queueMaxWait.Set(0)
		return
	}

	s.inflightRequestsMu.Lock()
	defer s.inflightRequestsMu.Unlock()

	var oldest time.Time
	for _, req := range s.schedulerInflightRequests {
		// Only requests still waiting in the queue count towards the metric.
		if req.Dispatched.Load() {
			continue
		}
		if oldest.IsZero() || req.EnqueueTime.Before(oldest) {
			oldest = req.EnqueueTime
		}
	}

	if oldest.IsZero() {
		s.queueMaxWait.Set(0)
		return
	}

	latency := time.Since(oldest).Seconds()
	s.queueMaxWait.Set(latency)
}

func (s *Scheduler) RingHandler(w http.ResponseWriter, req *http.Request) {
	if s.schedulerLifecycler != nil {
		s.schedulerLifecycler.ServeHTTP(w, req)
		return
	}

	ringDisabledPage := `
		<!DOCTYPE html>
		<html>
			<head>
				<meta charset="UTF-8">
				<title>Query-scheduler Status</title>
			</head>
			<body>
				<h1>Query-scheduler Status</h1>
				<p>Query-scheduler hash ring is disabled.</p>
			</body>
		</html>`
	util.WriteHTMLResponse(w, ringDisabledPage)
}
