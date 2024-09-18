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
	"github.com/grafana/mimir/pkg/util/grpcencoding/s2"
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

	inflightRequestsMu sync.Mutex
	// schedulerInflightRequests tracks requests from the time they are received to be enqueued by the scheduler
	// to the time they are completed by the querier or failed due to cancel, timeout, or disconnect.
	schedulerInflightRequests map[queue.RequestKey]*queue.SchedulerRequest

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

type connectedFrontend struct {
	connections int

	// This context is used for running all queries from the same frontend.
	// When last frontend connection is closed, context is canceled.
	ctx    context.Context
	cancel context.CancelCauseFunc
}

type Config struct {
	MaxOutstandingPerTenant   int           `yaml:"max_outstanding_requests_per_tenant"`
	PrioritizeQueryComponents bool          `yaml:"prioritize_query_components" category:"experimental"`
	QuerierForgetDelay        time.Duration `yaml:"querier_forget_delay" category:"experimental"`

	GRPCClientConfig grpcclient.Config         `yaml:"grpc_client_config" doc:"description=This configures the gRPC client used to report errors back to the query-frontend."`
	ServiceDiscovery schedulerdiscovery.Config `yaml:",inline"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	f.IntVar(&cfg.MaxOutstandingPerTenant, "query-scheduler.max-outstanding-requests-per-tenant", 100, "Maximum number of outstanding requests per tenant per query-scheduler. In-flight requests above this limit will fail with HTTP response status code 429.")
	f.BoolVar(&cfg.PrioritizeQueryComponents, "query-scheduler.prioritize-query-components", false, "When enabled, the query scheduler primarily prioritizes dequeuing fairly from queue components and secondarily prioritizes dequeuing fairly across tenants. When disabled, the query scheduler primarily prioritizes tenant fairness. You must enable the `query-scheduler.use-multi-algorithm-query-queue` setting to use this flag.")
	f.DurationVar(&cfg.QuerierForgetDelay, "query-scheduler.querier-forget-delay", 0, "If a querier disconnects without sending notification about graceful shutdown, the query-scheduler will keep the querier in the tenant's shard until the forget delay has passed. This feature is useful to reduce the blast radius when shuffle-sharding is enabled.")

	cfg.GRPCClientConfig.CustomCompressors = []string{s2.Name, s2.SnappyCompatName}
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

		schedulerInflightRequests: map[queue.RequestKey]*queue.SchedulerRequest{},
		connectedFrontends:        map[string]*connectedFrontend{},
		subservicesWatcher:        services.NewFailureWatcher(),
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

	s.requestQueue, err = queue.NewRequestQueue(
		s.log,
		cfg.MaxOutstandingPerTenant,
		cfg.PrioritizeQueryComponents,
		cfg.QuerierForgetDelay,
		s.queueLength,
		s.discardedRequests,
		enqueueDuration,
		querierInflightRequestsMetric,
	)
	if err != nil {
		return nil, err
	}

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
			requestKey := queue.NewSchedulerRequestKey(frontendAddress, msg.QueryID)
			schedulerReq := s.cancelRequestAndRemoveFromPending(requestKey, "frontend cancelled query")
			// we may not have reached MarkRequestSent for this query before receiving the cancel,
			// but RequestQueue will just no-op if the request was not yet tracked in the inflightRequests map.
			s.requestQueue.QueryComponentUtilization.MarkRequestCompleted(schedulerReq)
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
		FrontendAddr:              frontendAddr,
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
	return s.requestQueue.SubmitRequestToEnqueue(userID, req, maxQueriers, func() {
		shouldCancel = false
		s.addRequestToPending(req)
	})
}

func (s *Scheduler) addRequestToPending(req *queue.SchedulerRequest) {
	s.inflightRequestsMu.Lock()
	defer s.inflightRequestsMu.Unlock()

	s.schedulerInflightRequests[req.Key()] = req
}

// This method doesn't do removal from the queue.
func (s *Scheduler) cancelRequestAndRemoveFromPending(key queue.RequestKey, reason string) *queue.SchedulerRequest {
	s.inflightRequestsMu.Lock()
	defer s.inflightRequestsMu.Unlock()

	req := s.schedulerInflightRequests[key]
	if req != nil {
		req.CancelFunc(cancellation.NewError(errors.New(reason)))
	}

	delete(s.schedulerInflightRequests, key)
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
	querierWorkerConn := queue.NewUnregisteredQuerierWorkerConn(querier.Context(), queue.QuerierID(querierID))
	err = s.requestQueue.AwaitRegisterQuerierWorkerConn(querierWorkerConn)
	if err != nil {
		return s.transformRequestQueueError(err)
	}
	defer s.requestQueue.SubmitUnregisterQuerierWorkerConn(querierWorkerConn)

	lastTenantIdx := queue.FirstTenant()

	// In stopping state scheduler is not accepting new queries, but still dispatching queries in the queues.
	for s.isRunningOrStopping() {
		dequeueReq := queue.NewQuerierWorkerDequeueRequest(querierWorkerConn, lastTenantIdx)
		queryReq, idx, err := s.requestQueue.AwaitRequestForQuerier(dequeueReq)
		if err != nil {
			return s.transformRequestQueueError(err)
		}

		lastTenantIdx = idx

		schedulerReq := queryReq.(*queue.SchedulerRequest)

		queueTime := time.Since(schedulerReq.EnqueueTime)
		additionalQueueDimensionLabels := strings.Join(schedulerReq.AdditionalQueueDimensions, ":")
		s.queueDuration.WithLabelValues(schedulerReq.UserID, additionalQueueDimensionLabels).Observe(queueTime.Seconds())
		schedulerReq.QueueSpan.Finish()

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
			s.requestQueue.QueryComponentUtilization.MarkRequestCompleted(schedulerReq)
			lastTenantIdx = lastTenantIdx.ReuseLastTenant()
			continue
		}

		if err := s.forwardRequestToQuerier(querier, schedulerReq, queueTime); err != nil {
			return err
		}
	}

	return schedulerpb.ErrSchedulerIsNotRunning
}

func (s *Scheduler) NotifyQuerierShutdown(ctx context.Context, req *schedulerpb.NotifyQuerierShutdownRequest) (*schedulerpb.NotifyQuerierShutdownResponse, error) {
	level.Info(s.log).Log("msg", "received shutdown notification from querier", "querier", req.GetQuerierID())

	s.requestQueue.SubmitNotifyQuerierShutdown(ctx, queue.QuerierID(req.GetQuerierID()))

	return &schedulerpb.NotifyQuerierShutdownResponse{}, nil
}

func (s *Scheduler) forwardRequestToQuerier(querier schedulerpb.SchedulerForQuerier_QuerierLoopServer, req *queue.SchedulerRequest, queueTime time.Duration) error {
	s.requestQueue.QueryComponentUtilization.MarkRequestSent(req)
	defer s.requestQueue.QueryComponentUtilization.MarkRequestCompleted(req)
	defer s.cancelRequestAndRemoveFromPending(req.Key(), "request complete")

	// Handle the stream sending & receiving on a goroutine so we can
	// monitor the contexts in a select and cancel things appropriately.
	errCh := make(chan error, 1)
	go func() {
		err := querier.Send(&schedulerpb.SchedulerToQuerier{
			UserID:          req.UserID,
			QueryID:         req.QueryID,
			FrontendAddress: req.FrontendAddr,
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
		level.Warn(s.log).Log("msg", "failed to create gRPC options for the connection to frontend to report error", "frontend", req.FrontendAddr, "err", err, "requestErr", requestErr)
		return
	}

	// nolint:staticcheck // grpc.DialContext() has been deprecated; we'll address it before upgrading to gRPC 2.
	conn, err := grpc.DialContext(ctx, req.FrontendAddr, opts...)
	if err != nil {
		level.Warn(s.log).Log("msg", "failed to create gRPC connection to frontend to report error", "frontend", req.FrontendAddr, "err", err, "requestErr", requestErr)
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
		level.Warn(s.log).Log("msg", "failed to forward error to frontend", "frontend", req.FrontendAddr, "err", err, "requestErr", requestErr)
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
			s.inflightRequestsMu.Lock()
			inflight := len(s.schedulerInflightRequests)
			s.inflightRequestsMu.Unlock()

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
