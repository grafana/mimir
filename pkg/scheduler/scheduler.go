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
	"github.com/grafana/dskit/tenant"
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
	querierRequestFailures   *prometheus.CounterVec
	frontendErrorForwards    *prometheus.CounterVec
	connectedQuerierClients  prometheus.GaugeFunc
	connectedFrontendClients prometheus.GaugeFunc
	queueDuration            *prometheus.HistogramVec
	inflightRequests         prometheus.Summary
	invalidClusterValidation *prometheus.CounterVec
	queueMaxWait             prometheus.Gauge

	reservationsClosed   *prometheus.CounterVec // per user and outcome
	rejectedReservations *prometheus.CounterVec // per user and reason
}

var errQueueReservationDisabled = errors.New("queue reservation is not enabled on this query-scheduler")

// The reasons of cortex_query_scheduler_reservations_rejected_total.
const (
	reservationRejectedDisabled        = "disabled"
	reservationRejectedInvalid         = "invalid"
	reservationRejectedSlotsAboveLimit = "slots_above_limit"
	reservationRejectedDuplicate       = "duplicate"
	reservationRejectedPerTenantLimit  = "per_tenant_limit"
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
	f.IntVar(&cfg.MaxOutstandingPerTenant, "query-scheduler.max-outstanding-requests-per-tenant", 100, "Maximum number of outstanding requests per tenant per query-scheduler. In-flight requests above this limit will fail with HTTP response status code 429. When -query-scheduler.queue-reservation-enabled is true, the idle slots of each queue reservation also count against this limit.")
	f.DurationVar(&cfg.QuerierForgetDelay, "query-scheduler.querier-forget-delay", 0, "If a querier disconnects without sending notification about graceful shutdown, the query-scheduler will keep the querier in the tenant's shard until the forget delay has passed. This feature is useful to reduce the blast radius when shuffle-sharding is enabled.")
	f.BoolVar(&cfg.QueueMaxWaitMetricEnabled, "query-scheduler.queue-max-wait-metric-enabled", true, "Enable the cortex_query_scheduler_queue_max_wait_seconds metric, which reports how long the oldest request still waiting in the queue has been waiting. Disabling it skips the per-tick scan over inflight requests.")
	f.BoolVar(&cfg.MaxQueueLengthMetricEnabled, "query-scheduler.max-queue-length-metric-enabled", false, "Enable the cortex_query_scheduler_max_queue_length metric, which reports the per-tenant peak queue length observed since the last scrape. Disabling it skips per-tenant peak tracking on enqueue and dequeue.")
	f.BoolVar(&cfg.QueueReservationEnabled, "query-scheduler.queue-reservation-enabled", false, "Enable queue reservation: the query-frontend reserves queue slots for the sub-requests of a parent query before it sends them. A query is then either shed before any sub-request is dispatched, or admitted. An admitted sub-request uses an idle slot of its reservation, so the per-tenant limit does not reject it. A dequeue returns the slot to the reservation. Requires a query-frontend that also supports queue reservation; has no effect otherwise.")
	f.DurationVar(&cfg.QueueReservationTimeout, "query-scheduler.queue-reservation-timeout", 5*time.Second, "Time after which the query-scheduler closes an open reservation and releases its idle queue slots, when no sub-request of the reservation is queued or running and none was inserted or finished in that time. Sub-requests already in the queue stay there. Only applies when -query-scheduler.queue-reservation-enabled is true.")

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
		Help: "Total number of query requests that were cancelled after enqueuing, by stage and reason.",
	}, []string{"user", "stage", "reason"})
	s.querierRequestFailures = promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_scheduler_querier_request_failures_total",
		Help: "Total number of requests that failed because the query-scheduler could not send them to a querier or receive the querier's reply.",
	}, []string{"user"})
	s.frontendErrorForwards = promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_scheduler_frontend_error_forwards_total",
		Help: "Total number of times the query-scheduler tried to send a request error to the query-frontend, by outcome.",
	}, []string{"outcome"})
	s.frontendErrorForwards.WithLabelValues("success")
	s.frontendErrorForwards.WithLabelValues("frontend_not_waiting")
	s.frontendErrorForwards.WithLabelValues("frontend_disconnected")
	s.frontendErrorForwards.WithLabelValues("unavailable")
	s.frontendErrorForwards.WithLabelValues("other")
	s.discardedRequests = promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_scheduler_discarded_requests_total",
		Help: "Total number of query requests discarded.",
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

	s.reservationsClosed = promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_scheduler_reservations_closed_total",
		Help: "Total number of queue reservations that closed, by outcome: completed, aborted, timed_out or frontend_disconnected.",
	}, []string{"user", "outcome"})
	s.rejectedReservations = promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_scheduler_reservations_rejected_total",
		Help: "Total number of queue reservations that the query-scheduler rejected, by reason: disabled, invalid, slots_above_limit, duplicate or per_tenant_limit. Only per_tenant_limit replies with the retryable 429. The user label is empty for a tenant ID that is not valid.",
	}, []string{"user", "reason"})
	queueReservationEnabled := promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_query_scheduler_queue_reservation_enabled",
		Help: "1 when queue reservation is enabled on this query-scheduler, 0 otherwise.",
	})
	if cfg.QueueReservationEnabled {
		queueReservationEnabled.Set(1)
	}

	reservationSlots := promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
		Name:                            "cortex_query_scheduler_reservation_slots",
		Help:                            "Number of queue slots that each accepted queue reservation holds, observed when the reservation opens.",
		Buckets:                         prometheus.ExponentialBuckets(1, 2, 11),
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMinResetDuration: 1 * time.Hour,
		NativeHistogramMaxBucketNumber:  100,
	})
	idleSlots := queue.NewIdleSlotsGauge(
		"cortex_query_scheduler_reservation_idle_slots",
		"Number of queue slots that open reservations hold and no queued sub-request uses. These slots count towards the per-tenant queue cap.",
	)
	if registerer != nil {
		registerer.MustRegister(idleSlots)
	}

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
			Slots:     reservationSlots,
			Rejected:  s.rejectedReservations.MustCurryWith(prometheus.Labels{"reason": reservationRejectedPerTenantLimit}),
			IdleSlots: idleSlots,
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
		// The reaper has its own goroutine. Its tick waits for the queue, so it must not delay the
		// in-flight request samples in running. It uses the same 250ms cadence as those samples.
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

		case schedulerpb.COMPLETE_RESERVATION:
			resp = s.closeReservation(frontendAddress, msg.ReservationID, queue.ReservationCompleted)

		case schedulerpb.CANCEL:
			// A CANCEL with queryID 0 and a non-empty reservationID aborts that reservation.
			if msg.QueryID == 0 && msg.ReservationID != "" {
				resp = s.closeReservation(frontendAddress, msg.ReservationID, queue.ReservationAborted)
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
	var closed []queue.ClosedReservation
	if cf.connections == 0 {
		delete(s.connectedFrontends, frontendAddress)
		cf.cancel(errFrontendDisconnected)
		// Any stream of the frontend can use its reservations, so they close with its last stream.
		// The close runs before the unlock. A new stream of this frontend waits for
		// connectedFrontendsMu, so its first RESERVE sees the released slots.
		if s.cfg.QueueReservationEnabled {
			var err error
			if closed, err = s.queue.CloseReservationsOfOwner(frontendAddress); err != nil {
				logQueueReservationError(s.log, "failed to close the reservations of a disconnected frontend", err, "addr", frontendAddress)
			}
		}
	}
	s.connectedFrontendsMu.Unlock()

	for _, c := range closed {
		s.countClosedReservation(c)
	}
}

// enqueueRequest enqueues the request in msg. An ENQUEUE with a reservationID is a sub-request of
// that reservation, and it uses an idle slot of the reservation.
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

	req.ParentSpanContext = trace.SpanContextFromContext(requestContext)
	// The root query ID is not set on this span: it is already on the "enqueue" span this one
	// descends from
	req.Ctx, req.QueueSpan = tracer.Start(ctx, "queued")
	req.EnqueueTime = now
	req.CancelFunc = cancel

	successFn := func() {
		shouldCancel = false
		s.activeUsers.UpdateUserTimestamp(userID, now)
		s.addRequestToPending(req)
	}
	if msg.ReservationID == "" {
		return s.queue.Enqueue(req, successFn)
	}

	key := queue.ReservationKey{Owner: frontendAddr, ID: msg.ReservationID}
	return s.queue.EnqueueIntoReservation(key, req, now, func(res *queue.Reservation) {
		req.reservation = res
		successFn()
	})
}

// createReservation handles a RESERVE. It opens a reservation for frontendAddress and
// msg.ReservationID, with msg.Slots queue slots of msg.UserID.
func (s *Scheduler) createReservation(frontendAddress string, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
	// A tenant ID that is not valid would panic as a metric label value.
	userLabel := ""
	if _, err := tenant.TenantIDsFromOrgID(msg.UserID); err == nil {
		userLabel = msg.UserID
	}

	if !s.cfg.QueueReservationEnabled {
		s.countRejectedReservation(userLabel, reservationRejectedDisabled)
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: errQueueReservationDisabled.Error()}
	}

	var invalid string
	switch {
	case msg.Slots <= 0 || msg.Slots > uint64(math.MaxInt):
		invalid = "invalid slot count"
	case msg.ReservationID == "":
		invalid = "invalid reservation ID"
	}
	if invalid != "" {
		s.countRejectedReservation(userLabel, reservationRejectedInvalid)
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: invalid}
	}

	tenantID := msg.UserID
	now := time.Now()
	key := queue.ReservationKey{Owner: frontendAddress, ID: msg.ReservationID}
	err := s.queue.OpenReservation(key, tenantID, int(msg.Slots), now)

	var rejected *queue.ReservationRejectedError
	switch {
	case err == nil, errors.Is(err, queue.ErrStopped):
	case errors.As(err, &rejected):
		level.Warn(s.log).Log(
			"msg", "rejected reservation because the tenant has too many outstanding requests",
			"tenant", tenantID, "addr", frontendAddress, "reservation_id", msg.ReservationID,
			"requested", rejected.Requested, "queued", rejected.Queued, "idle", rejected.Idle,
			"max_tenant_queue_size", rejected.MaxTenantQueueSize,
		)
	case errors.Is(err, queue.ErrReservationExists):
		s.countRejectedReservation(userLabel, reservationRejectedDuplicate)
	case errors.Is(err, queue.ErrReservationExceedsCap):
		s.countRejectedReservation(userLabel, reservationRejectedSlotsAboveLimit)
	default:
		s.countRejectedReservation(userLabel, reservationRejectedInvalid)
	}
	if err == nil || rejected != nil {
		// An admitted reservation is tenant activity. A rejected one changed the tenant's rejected
		// counter, so the update makes the cleanup delete that series later.
		s.activeUsers.UpdateUserTimestamp(tenantID, now)
	}
	return queueErrorResponse(err)
}

// countRejectedReservation counts a RESERVE that got an ERROR reply. userLabel is empty when the
// RESERVE carries a tenant ID that is not valid.
func (s *Scheduler) countRejectedReservation(userLabel, reason string) {
	s.rejectedReservations.WithLabelValues(userLabel, reason).Inc()
	if userLabel != "" {
		// The update comes after the increment, so the inactive-user cleanup deletes the series.
		s.activeUsers.UpdateUserTimestamp(userLabel, time.Now())
	}
}

// closeReservation handles a COMPLETE_RESERVATION, with outcome completed, and a CANCEL that
// aborts a reservation, with outcome aborted. It closes the reservation and releases its idle
// slots. Inserted sub-requests stay in the queue. The frontend cancels them with a CANCEL per query
// when it needs to.
func (s *Scheduler) closeReservation(frontendAddress, reservationID string, outcome queue.ReservationOutcome) *schedulerpb.SchedulerToFrontend {
	// The frontend can send either message on any of its streams to this scheduler, so it closes
	// the reservation whatever stream opened it. An unknown reservation is a no-op: it already
	// closed or never existed, or its RESERVE has not added it yet.
	key := queue.ReservationKey{Owner: frontendAddress, ID: reservationID}
	var closed *queue.ClosedReservation
	var err error
	if outcome == queue.ReservationCompleted {
		closed, err = s.queue.CompleteReservation(key)
	} else {
		closed, err = s.queue.AbortReservation(key)
	}
	if err != nil {
		logQueueReservationError(s.log, "failed to close a reservation", err, "addr", frontendAddress, "reservation_id", reservationID, "outcome", outcome)
	}
	if closed != nil {
		s.countClosedReservation(*closed)
	}
	return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
}

// countClosedReservation counts c in reservationsClosed.
func (s *Scheduler) countClosedReservation(c queue.ClosedReservation) {
	s.reservationsClosed.WithLabelValues(c.TenantID, string(c.Outcome)).Inc()
	// The counter can change after the tenant's last request, so the tenant stays active until a
	// later cleanup deletes it. The update must come after the increment: a purge between the lookup
	// and the store of UpdateUserTimestamp loses the update, and a counter change after that would
	// leave a counter that no cleanup deletes.
	s.activeUsers.UpdateUserTimestamp(c.TenantID, time.Now())
}

// logQueueReservationError logs err of a reservation operation. During shutdown the queue is
// stopped, and a failed operation is expected.
func logQueueReservationError(logger log.Logger, msg string, err error, keyvals ...any) {
	lvl := level.Error(logger)
	if errors.Is(err, queue.ErrStopped) {
		lvl = level.Debug(logger)
	}
	lvl.Log(append([]any{"msg", msg, "err", err}, keyvals...)...)
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
			s.cancelledRequests.WithLabelValues(schedulerReq.UserID, "queued", cancelReason(schedulerReq.Ctx)).Inc()
			// remove from pending requests
			s.cancelRequestAndRemoveFromPending(schedulerReq.Key(), "request cancelled")
			s.queue.MarkRequestCompleted(schedulerReq)
			schedulerReq.finishReservedSubRequest()
			lastTenantIdx = lastTenantIdx.ReuseLastTenant()
			continue
		}

		err = s.forwardRequestToQuerier(querier, querierID, schedulerReq, queueTime)
		schedulerReq.finishReservedSubRequest()
		if err != nil {
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
		s.cancelledRequests.WithLabelValues(req.UserID, "dispatched", cancelReason(req.Ctx)).Inc()
		return status.Error(codes.Canceled, context.Cause(req.Ctx).Error())

	case err := <-errCh:
		// Is there was an error handling this request due to network IO,
		// then error out this upstream request _and_ stream.

		if err != nil {
			s.querierRequestFailures.WithLabelValues(req.UserID).Inc()
			s.forwardErrorToFrontend(req.Ctx, req, err)
		}
		return err
	}
}

func (s *Scheduler) forwardErrorToFrontend(ctx context.Context, req *SchedulerRequest, requestErr error) {
	logger := log.With(s.log, req.LogFields()...)
	outcome := "other"
	defer func() {
		s.frontendErrorForwards.WithLabelValues(outcome).Inc()
	}()

	// A call with a cancelled request context fails with Canceled or Unavailable, depending on the connection state.
	// So the forward takes the outcome from the cause, and makes no call.
	if ctx.Err() != nil {
		outcome = cancelledRequestErrorForwardOutcome(ctx)
		level.Warn(logger).Log("msg", "not forwarding error to frontend, because the request context is cancelled", "frontend", req.FrontendAddr, "cause", context.Cause(ctx), "requestErr", requestErr)
		return
	}

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
			outcome = frontendErrorForwardOutcome(ctx, err)
			level.Warn(logger).Log("msg", "failed to forward error to frontend", "frontend", req.FrontendAddr, "err", err, "requestErr", requestErr)
			return
		}
	} else { // Protobuf request, so send a streaming response.
		stream, err := client.QueryResultStream(userCtx)
		if err != nil {
			outcome = frontendErrorForwardOutcome(ctx, err)
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
			outcome = frontendErrorForwardOutcome(ctx, err)
			level.Warn(logger).Log("msg", "failed to forward error to frontend", "frontend", req.FrontendAddr, "err", err, "requestErr", requestErr)
			return
		}

		if _, err = stream.CloseAndRecv(); err != nil {
			outcome = frontendErrorForwardOutcome(ctx, err)
			level.Warn(logger).Log("msg", "failed to close stream to frontend after forwarding error", "frontend", req.FrontendAddr, "err", err, "requestErr", requestErr)
			return
		}
	}

	outcome = "success"
}

// cancelReason returns the reason label for a cancelled request context.
func cancelReason(ctx context.Context) string {
	if errors.Is(context.Cause(ctx), errFrontendDisconnected) {
		return "frontend_disconnected"
	}
	return "frontend_cancelled"
}

// frontendErrorForwardOutcome returns the outcome of an error forward that failed with err.
func frontendErrorForwardOutcome(ctx context.Context, err error) string {
	switch status.Code(err) {
	case codes.FailedPrecondition:
		return "frontend_not_waiting"
	case codes.Canceled:
		if ctx.Err() != nil {
			return cancelledRequestErrorForwardOutcome(ctx)
		}
	case codes.Unavailable:
		return "unavailable"
	}
	return "other"
}

// cancelledRequestErrorForwardOutcome returns the outcome of an error forward with a cancelled request context.
// Only a CANCEL from the query-frontend or a frontend disconnect cancels the request context.
// After a disconnect, the query-frontend can still wait for the query.
func cancelledRequestErrorForwardOutcome(ctx context.Context) string {
	if errors.Is(context.Cause(ctx), errFrontendDisconnected) {
		return "frontend_disconnected"
	}
	return "frontend_not_waiting"
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

// reapExpiredReservations closes every open reservation that has no queued or running sub-request,
// and no sub-request inserted or finished within the reservation timeout. It releases their
// idle slots.
func (s *Scheduler) reapExpiredReservations(_ context.Context) error {
	s.reapReservationsAt(time.Now())
	return nil
}

func (s *Scheduler) reapReservationsAt(now time.Time) {
	expired, err := s.queue.ReapReservations(now, s.cfg.QueueReservationTimeout)
	if err != nil {
		logQueueReservationError(s.log, "failed to reap the expired reservations", err)
	}
	for _, c := range expired {
		s.countClosedReservation(c)
		level.Warn(s.log).Log(
			"msg", "reservation timed out, so the query-scheduler released its idle slots",
			"tenant", c.TenantID, "reservation_id", c.ID,
			"inserted", c.Inserted, "released", c.Released,
		)
	}
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
	s.cancelledRequests.DeletePartialMatch(prometheus.Labels{"user": user})
	s.querierRequestFailures.DeleteLabelValues(user)
	s.rejectedReservations.DeletePartialMatch(prometheus.Labels{"user": user})
	s.reservationsClosed.DeletePartialMatch(prometheus.Labels{"user": user})
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
