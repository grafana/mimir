// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/scheduler.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package scheduler

import (
	"context"
	"flag"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/scheduler/queue"
	"github.com/grafana/mimir/pkg/scheduler/schedulerdiscovery"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/httpgrpcutil"
	"github.com/grafana/mimir/pkg/util/validation"
)

var errEnqueuingRequestFailed = cancellation.NewErrorf("enqueuing request failed")
var errFrontendDisconnected = cancellation.NewErrorf("frontend disconnected")

// Scheduler is responsible for queueing and dispatching queries to Queriers.
type Scheduler struct {
	services.Service

	cfg Config
	log log.Logger

	limits Limits

	connectedFrontendsMu sync.Mutex
	connectedFrontends   map[string]*connectedFrontend

	requestQueue *queue.RequestQueue
	activeUsers  *util.ActiveUsersCleanupService

	pendingRequestsMu sync.Mutex
	pendingRequests   map[requestKey]*queue.SchedulerRequest // request is kept in this map even after being dispatched to querier. It can still be canceled at that time.

	// The ring is used to let other components discover query-scheduler replicas.
	// The ring is optional.
	schedulerLifecycler *ring.BasicLifecycler

	// Subservices manager.
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher

	// Metrics.
	queueLength              *prometheus.GaugeVec
	discardedRequests        *prometheus.CounterVec
	cancelledRequests        *prometheus.CounterVec
	connectedQuerierClients  prometheus.GaugeFunc
	connectedFrontendClients prometheus.GaugeFunc
	queueDuration            *prometheus.HistogramVec
	inflightRequests         prometheus.Summary
}

type requestKey struct {
	frontendAddr string
	queryID      uint64
}

type connectedFrontend struct {
	connections int

	// This context is used for running all queries from the same frontend.
	// When last frontend connection is closed, context is canceled.
	ctx    context.Context
	cancel context.CancelCauseFunc
}

type Config struct {
	MaxOutstandingPerTenant               int           `yaml:"max_outstanding_requests_per_tenant"`
	AdditionalQueryQueueDimensionsEnabled bool          `yaml:"additional_query_queue_dimensions_enabled" category:"experimental"`
	QuerierForgetDelay                    time.Duration `yaml:"querier_forget_delay" category:"experimental"`

	GRPCClientConfig grpcclient.Config         `yaml:"grpc_client_config" doc:"description=This configures the gRPC client used to report errors back to the query-frontend."`
	ServiceDiscovery schedulerdiscovery.Config `yaml:",inline"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	f.IntVar(&cfg.MaxOutstandingPerTenant, "query-scheduler.max-outstanding-requests-per-tenant", 100, "Maximum number of outstanding requests per tenant per query-scheduler. In-flight requests above this limit will fail with HTTP response status code 429.")
	f.BoolVar(&cfg.AdditionalQueryQueueDimensionsEnabled, "query-scheduler.additional-query-queue-dimensions-enabled", false, "Enqueue query requests with additional queue dimensions to split tenant request queues into subqueues. This enables separate requests to proceed from a tenant's subqueues even when other subqueues are blocked on slow query requests. Must be set on both query-frontend and scheduler to take effect. (default false)")
	f.DurationVar(&cfg.QuerierForgetDelay, "query-scheduler.querier-forget-delay", 0, "If a querier disconnects without sending notification about graceful shutdown, the query-scheduler will keep the querier in the tenant's shard until the forget delay has passed. This feature is useful to reduce the blast radius when shuffle-sharding is enabled.")

	cfg.GRPCClientConfig.RegisterFlagsWithPrefix("query-scheduler.grpc-client-config", f)
	cfg.ServiceDiscovery.RegisterFlags(f, logger)
}

func (cfg *Config) Validate() error {
	return cfg.ServiceDiscovery.Validate()
}

// NewScheduler creates a new Scheduler.
func NewScheduler(cfg Config, limits Limits, log log.Logger, registerer prometheus.Registerer) (*Scheduler, error) {
	var err error

	s := &Scheduler{
		cfg:    cfg,
		log:    log,
		limits: limits,

		pendingRequests:    map[requestKey]*queue.SchedulerRequest{},
		connectedFrontends: map[string]*connectedFrontend{},
		subservicesWatcher: services.NewFailureWatcher(),
	}

	s.queueLength = promauto.With(registerer).NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_query_scheduler_queue_length",
		Help: "Number of queries in the queue.",
	}, []string{"user"})

	s.cancelledRequests = promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_scheduler_cancelled_requests_total",
		Help: "Total number of query requests that were cancelled after enqueuing.",
	}, []string{"user"})
	s.discardedRequests = promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_scheduler_discarded_requests_total",
		Help: "Total number of query requests discarded.",
	}, []string{"user"})
	enqueueDuration := promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
		Name: "cortex_query_scheduler_enqueue_duration_seconds",
		Help: "Time spent by requests waiting to join the queue or be rejected.",
	})
	s.requestQueue = queue.NewRequestQueue(s.log, cfg.MaxOutstandingPerTenant, cfg.AdditionalQueryQueueDimensionsEnabled, cfg.QuerierForgetDelay, s.queueLength, s.discardedRequests, enqueueDuration)

	s.queueDuration = promauto.With(registerer).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cortex_query_scheduler_queue_duration_seconds",
		Help:    "Time spent by requests in queue before getting picked up by a querier.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120},
	}, []string{"user", "additional_queue_dimensions"})
	s.connectedQuerierClients = promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_query_scheduler_connected_querier_clients",
		Help: "Number of querier worker clients currently connected to the query-scheduler.",
	}, s.requestQueue.GetConnectedQuerierWorkersMetric)
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
	subservices := []services.Service{s.requestQueue, s.activeUsers}

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
		if err := frontend.Send(&schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}); err != nil {
			return err
		}
	}

	// We stop accepting new queries in Stopping state. By returning quickly, we disconnect frontends, which in turns
	// cancels all their queries.
	for s.State() == services.Running {
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
			tracer := opentracing.GlobalTracer()
			parentSpanContext, err := httpgrpcutil.GetParentSpanForRequest(tracer, msg.HttpRequest)
			if err != nil {
				resp = &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: err.Error()}
				break
			}
			enqueueSpan, reqCtx := opentracing.StartSpanFromContextWithTracer(frontendCtx, tracer, "enqueue", opentracing.ChildOf(parentSpanContext))

			err = s.enqueueRequest(reqCtx, frontendAddress, msg)
			switch {
			case err == nil:
				resp = &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
			case errors.Is(err, queue.ErrTooManyRequests):
				enqueueSpan.LogKV("error", err.Error())
				resp = &schedulerpb.SchedulerToFrontend{Status: schedulerpb.TOO_MANY_REQUESTS_PER_TENANT}
			default:
				enqueueSpan.LogKV("error", err.Error())
				resp = &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: err.Error()}
			}

			enqueueSpan.Finish()
		case schedulerpb.CANCEL:
			s.cancelRequestAndRemoveFromPending(frontendAddress, msg.QueryID, "frontend cancelled query")
			resp = &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}

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
	defer s.connectedFrontendsMu.Unlock()

	cf := s.connectedFrontends[frontendAddress]
	cf.connections--
	if cf.connections == 0 {
		delete(s.connectedFrontends, frontendAddress)
		cf.cancel(errFrontendDisconnected)
	}
}

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

	req := &queue.SchedulerRequest{
		FrontendAddress:           frontendAddr,
		UserID:                    msg.UserID,
		QueryID:                   msg.QueryID,
		Request:                   msg.HttpRequest,
		StatsEnabled:              msg.StatsEnabled,
		AdditionalQueueDimensions: msg.AdditionalQueueDimensions,
	}

	now := time.Now()

	req.ParentSpanContext = opentracing.SpanFromContext(requestContext).Context()
	req.QueueSpan, req.Ctx = opentracing.StartSpanFromContext(ctx, "queued")
	req.EnqueueTime = now
	req.CancelFunc = cancel

	// aggregate the max queriers limit in the case of a multi tenant query
	tenantIDs, err := tenant.TenantIDsFromOrgID(userID)
	if err != nil {
		return err
	}
	maxQueriers := validation.SmallestPositiveNonZeroIntPerTenant(tenantIDs, s.limits.MaxQueriersPerUser)

	s.activeUsers.UpdateUserTimestamp(userID, now)
	return s.requestQueue.EnqueueRequestToDispatcher(userID, req, maxQueriers, func() {
		shouldCancel = false

		s.pendingRequestsMu.Lock()
		s.pendingRequests[requestKey{frontendAddr: frontendAddr, queryID: msg.QueryID}] = req
		s.pendingRequestsMu.Unlock()
	})
}

// This method doesn't do removal from the queue.
func (s *Scheduler) cancelRequestAndRemoveFromPending(frontendAddr string, queryID uint64, reason string) {
	s.pendingRequestsMu.Lock()
	defer s.pendingRequestsMu.Unlock()

	key := requestKey{frontendAddr: frontendAddr, queryID: queryID}
	req := s.pendingRequests[key]
	if req != nil {
		req.CancelFunc(cancellation.NewErrorf(reason))
	}

	delete(s.pendingRequests, key)
}

// QuerierLoop is started by querier to receive queries from scheduler.
func (s *Scheduler) QuerierLoop(querier schedulerpb.SchedulerForQuerier_QuerierLoopServer) error {
	resp, err := querier.Recv()
	if err != nil {
		return err
	}

	querierID := resp.GetQuerierID()

	s.requestQueue.RegisterQuerierConnection(querierID)
	defer s.requestQueue.UnregisterQuerierConnection(querierID)

	lastUserIndex := queue.FirstUser()

	// In stopping state scheduler is not accepting new queries, but still dispatching queries in the queues.
	for s.isRunningOrStopping() {
		req, idx, err := s.requestQueue.GetNextRequestForQuerier(querier.Context(), lastUserIndex, querierID)
		if err != nil {
			// Return a more clear error if the queue is stopped because the query-scheduler is not running.
			if errors.Is(err, queue.ErrStopped) && !s.isRunning() {
				return schedulerpb.ErrSchedulerIsNotRunning
			}

			return err
		}
		lastUserIndex = idx

		r := req.(*queue.SchedulerRequest)

		queueTime := time.Since(r.EnqueueTime)
		additionalQueueDimensionLabels := strings.Join(r.AdditionalQueueDimensions, ":")
		s.queueDuration.WithLabelValues(r.UserID, additionalQueueDimensionLabels).Observe(queueTime.Seconds())
		r.QueueSpan.Finish()

		/*
		  We want to dequeue the next unexpired request from the chosen tenant queue.
		  The chance of choosing a particular tenant for dequeueing is (1/active_tenants).
		  This is problematic under load, especially with other middleware enabled such as
		  querier.split-by-interval, where one request may fan out into many.
		  If expired requests aren't exhausted before checking another tenant, it would take
		  n_active_tenants * n_expired_requests_at_front_of_queue requests being processed
		  before an active request was handled for the tenant in question.
		  If this tenant meanwhile continued to queue requests,
		  it's possible that its own queue would perpetually contain only expired requests.
		*/

		if r.Ctx.Err() != nil {
			// Remove from pending requests.
			s.cancelRequestAndRemoveFromPending(r.FrontendAddress, r.QueryID, "request cancelled")

			lastUserIndex = lastUserIndex.ReuseLastUser()
			continue
		}

		if err := s.forwardRequestToQuerier(querier, r, queueTime); err != nil {
			return err
		}
	}

	return schedulerpb.ErrSchedulerIsNotRunning
}

func (s *Scheduler) NotifyQuerierShutdown(_ context.Context, req *schedulerpb.NotifyQuerierShutdownRequest) (*schedulerpb.NotifyQuerierShutdownResponse, error) {
	level.Info(s.log).Log("msg", "received shutdown notification from querier", "querier", req.GetQuerierID())
	s.requestQueue.NotifyQuerierShutdown(req.GetQuerierID())

	return &schedulerpb.NotifyQuerierShutdownResponse{}, nil
}

func (s *Scheduler) forwardRequestToQuerier(querier schedulerpb.SchedulerForQuerier_QuerierLoopServer, req *queue.SchedulerRequest, queueTime time.Duration) error {
	// Make sure to cancel request at the end to clean up resources.
	defer s.cancelRequestAndRemoveFromPending(req.FrontendAddress, req.QueryID, "request complete")

	// Handle the stream sending & receiving on a goroutine so we can
	// monitor the contexts in a select and cancel things appropriately.
	errCh := make(chan error, 1)
	go func() {
		err := querier.Send(&schedulerpb.SchedulerToQuerier{
			UserID:          req.UserID,
			QueryID:         req.QueryID,
			FrontendAddress: req.FrontendAddress,
			HttpRequest:     req.Request,
			StatsEnabled:    req.StatsEnabled,
			QueueTimeNanos:  queueTime.Nanoseconds(),
		})
		if err != nil {
			errCh <- err
			return
		}

		_, err = querier.Recv()
		errCh <- err
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

func (s *Scheduler) forwardErrorToFrontend(ctx context.Context, req *queue.SchedulerRequest, requestErr error) {
	opts, err := s.cfg.GRPCClientConfig.DialOption([]grpc.UnaryClientInterceptor{
		otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
		middleware.ClientUserHeaderInterceptor},
		nil)
	if err != nil {
		level.Warn(s.log).Log("msg", "failed to create gRPC options for the connection to frontend to report error", "frontend", req.FrontendAddress, "err", err, "requestErr", requestErr)
		return
	}

	conn, err := grpc.DialContext(ctx, req.FrontendAddress, opts...)
	if err != nil {
		level.Warn(s.log).Log("msg", "failed to create gRPC connection to frontend to report error", "frontend", req.FrontendAddress, "err", err, "requestErr", requestErr)
		return
	}

	defer func() {
		_ = conn.Close()
	}()

	client := frontendv2pb.NewFrontendForQuerierClient(conn)

	userCtx := user.InjectOrgID(ctx, req.UserID)
	_, err = client.QueryResult(userCtx, &frontendv2pb.QueryResultRequest{
		QueryID: req.QueryID,
		HttpResponse: &httpgrpc.HTTPResponse{
			Code: http.StatusInternalServerError,
			Body: []byte(requestErr.Error()),
		},
	})

	if err != nil {
		level.Warn(s.log).Log("msg", "failed to forward error to frontend", "frontend", req.FrontendAddress, "err", err, "requestErr", requestErr)
		return
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
		return errors.Wrap(err, "unable to start scheduler subservices")
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
			s.pendingRequestsMu.Lock()
			inflight := len(s.pendingRequests)
			s.pendingRequestsMu.Unlock()

			s.inflightRequests.Observe(float64(inflight))
		case <-ctx.Done():
			return nil
		case err := <-s.subservicesWatcher.Chan():
			return errors.Wrap(err, "scheduler subservice failed")
		}
	}
}

// Close the Scheduler.
func (s *Scheduler) stopping(_ error) error {
	// This will also stop the requests queue, which stop accepting new requests and errors out any pending requests.
	return services.StopManagerAndAwaitStopped(context.Background(), s.subservices)
}

func (s *Scheduler) cleanupMetricsForInactiveUser(user string) {
	s.queueLength.DeleteLabelValues(user)
	s.discardedRequests.DeleteLabelValues(user)
	s.cancelledRequests.DeleteLabelValues(user)
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
