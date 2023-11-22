// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/worker/scheduler_processor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package worker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/status"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	querier_stats "github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/httpgrpcutil"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

const maxNotifyFrontendRetries = 5

func newSchedulerProcessor(cfg Config, handler RequestHandler, log log.Logger, reg prometheus.Registerer) (*schedulerProcessor, []services.Service) {
	p := &schedulerProcessor{
		log:            log,
		handler:        handler,
		maxMessageSize: cfg.QueryFrontendGRPCClientConfig.MaxSendMsgSize,
		querierID:      cfg.QuerierID,
		grpcConfig:     cfg.QueryFrontendGRPCClientConfig,

		schedulerClientFactory: func(conn *grpc.ClientConn) schedulerpb.SchedulerForQuerierClient {
			return schedulerpb.NewSchedulerForQuerierClient(conn)
		},

		frontendClientRequestDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cortex_querier_query_frontend_request_duration_seconds",
			Help:    "Time spend doing requests to frontend.",
			Buckets: prometheus.ExponentialBuckets(0.001, 4, 6),
		}, []string{"operation", "status_code"}),
	}

	frontendClientsGauge := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_querier_query_frontend_clients",
		Help: "The current number of clients connected to query-frontend.",
	})

	poolConfig := client.PoolConfig{
		CheckInterval:      5 * time.Second,
		HealthCheckEnabled: true,
		HealthCheckTimeout: 1 * time.Second,
	}

	p.frontendPool = client.NewPool("frontend", poolConfig, nil, client.PoolAddrFunc(p.createFrontendClient), frontendClientsGauge, log)
	return p, []services.Service{p.frontendPool}
}

// Handles incoming queries from query-scheduler.
type schedulerProcessor struct {
	log            log.Logger
	handler        RequestHandler
	grpcConfig     grpcclient.Config
	maxMessageSize int
	querierID      string

	frontendPool                  *client.Pool
	frontendClientRequestDuration *prometheus.HistogramVec

	schedulerClientFactory func(conn *grpc.ClientConn) schedulerpb.SchedulerForQuerierClient
}

// notifyShutdown implements processor.
func (sp *schedulerProcessor) notifyShutdown(ctx context.Context, conn *grpc.ClientConn, address string) {
	client := sp.schedulerClientFactory(conn)

	req := &schedulerpb.NotifyQuerierShutdownRequest{QuerierID: sp.querierID}
	if _, err := client.NotifyQuerierShutdown(ctx, req); err != nil {
		// Since we're shutting down there's nothing we can do except logging it.
		level.Warn(sp.log).Log("msg", "failed to notify querier shutdown to query-scheduler", "address", address, "err", err)
	}
}

func (sp *schedulerProcessor) processQueriesOnSingleStream(ctx context.Context, conn *grpc.ClientConn, address string) {
	schedulerClient := sp.schedulerClientFactory(conn)

	backoff := backoff.New(ctx, processorBackoffConfig)
	for backoff.Ongoing() {
		c, err := schedulerClient.QuerierLoop(ctx)
		if err == nil {
			err = c.Send(&schedulerpb.QuerierToScheduler{QuerierID: sp.querierID})
		}

		if err != nil {
			level.Warn(sp.log).Log("msg", "error contacting scheduler", "err", err, "addr", address)
			backoff.Wait()
			continue
		}

		if err := sp.querierLoop(ctx, c, address); err != nil {
			// Do not log an error if the query-scheduler is shutting down.
			if s, ok := status.FromError(err); !ok || !strings.Contains(s.Message(), schedulerpb.ErrSchedulerIsNotRunning.Error()) {
				level.Error(sp.log).Log("msg", "error processing requests from scheduler", "err", err, "addr", address)
			}

			backoff.Wait()
			continue
		}

		backoff.Reset()
	}
}

// querierLoop loops processing requests on an established stream.
func (sp *schedulerProcessor) querierLoop(workerCtx context.Context, c schedulerpb.SchedulerForQuerier_QuerierLoopClient, address string) (err error) {
	// Build a child context so we can cancel a query when the stream is closed (which is how the scheduler signals that a query should be cancelled).
	// We deliberately don't make this a child of workerCtx so that we can complete an inflight query even after a worker is stopped - workerCtx is
	// cancelled when the scheduler begins shutting down.
	ctx, cancel := context.WithCancelCause(context.Background())
	defer func() {
		cancel(util.NewCancellationErrorf("query-scheduler loop in querier for query-scheduler %v terminated with error: %w", address, err))
	}()

	var queryComplete chan struct{}

	for {
		request, err := c.Recv()
		if err != nil {
			if status.Code(err) == codes.Canceled && workerCtx.Err() != nil && queryComplete != nil {
				// This worker has been asked to shut down, and the Recv() call aborted because of this.
				// Wait for query execution (if any) to finish.
				//
				// Note that the gRPC client translates client-side context cancellations to a gRPC-style error with codes.Canceled -
				// it does not return the 'raw' context.Canceled error.
				//
				// We can't use c.Context() in the check above because the gRPC client will cancel this context before Recv() returns any kind of error.
				level.Debug(sp.log).Log("msg", "querier worker context has been canceled, waiting until inflight query is complete", "addr", address)
				<-queryComplete
				level.Debug(sp.log).Log("msg", "querier worker context has been canceled and inflight query is complete, canceling the execution context too", "addr", address)
			}

			// Once we get to here, we want to abort the running query (if any), which is handled by the deferred cancel call above.

			return err
		}

		queryComplete = make(chan struct{})

		// Handle the request on a "background" goroutine, so we go back to
		// blocking on c.Recv().  This allows us to detect the stream closing
		// and cancel the query.  We don't actually handle queries in parallel
		// here, as we're running in lock step with the server - each Recv is
		// paired with a Send.
		go func() {
			defer close(queryComplete)

			// Create a per-request context and cancel it once we're done processing the request.
			// This is important for queries that stream chunks from ingesters to the querier, as SeriesChunksStreamReader relies
			// on the context being cancelled to abort streaming and terminate a goroutine if the query is aborted. Requests that
			// go direct to a querier's HTTP API have a context created and cancelled in a similar way by the Go runtime's
			// net/http package.
			queryCtx, cancel := context.WithCancelCause(ctx)
			defer cancel(util.NewCancellationError(errors.New("query evaluation finished")))

			// We need to inject user into context for sending response back.
			queryCtx = user.InjectOrgID(queryCtx, request.UserID)

			tracer := opentracing.GlobalTracer()
			// Ignore errors here. If we cannot get parent span, we just don't create new one.
			parentSpanContext, _ := httpgrpcutil.GetParentSpanForRequest(tracer, request.HttpRequest)
			if parentSpanContext != nil {
				queueSpan, spanCtx := opentracing.StartSpanFromContextWithTracer(queryCtx, tracer, "querier_processor_runRequest", opentracing.ChildOf(parentSpanContext))
				defer queueSpan.Finish()

				queryCtx = spanCtx
			}
			logger := util_log.WithContext(queryCtx, sp.log)

			sp.runRequest(queryCtx, logger, request.QueryID, request.FrontendAddress, request.StatsEnabled, request.HttpRequest, time.Duration(request.QueueTimeNanos))

			// Report back to scheduler that processing of the query has finished.
			if err := c.Send(&schedulerpb.QuerierToScheduler{}); err != nil {
				level.Error(logger).Log("msg", "error notifying scheduler about finished query", "err", err, "addr", address)
			}
		}()
	}
}

func (sp *schedulerProcessor) runRequest(ctx context.Context, logger log.Logger, queryID uint64, frontendAddress string, statsEnabled bool, request *httpgrpc.HTTPRequest, queueTime time.Duration) {
	var stats *querier_stats.Stats
	if statsEnabled {
		stats, ctx = querier_stats.ContextWithEmptyStats(ctx)
		stats.AddQueueTime(queueTime)
	}

	response, err := sp.handler.Handle(ctx, request)
	if err != nil {
		var ok bool
		response, ok = httpgrpc.HTTPResponseFromError(err)
		if !ok {
			response = &httpgrpc.HTTPResponse{
				Code: http.StatusInternalServerError,
				Body: []byte(err.Error()),
			}
		}
	}

	// Ensure responses that are too big are not retried.
	if len(response.Body) >= sp.maxMessageSize {
		level.Error(logger).Log("msg", "response larger than max message size", "size", len(response.Body), "maxMessageSize", sp.maxMessageSize)

		errMsg := fmt.Sprintf("response larger than the max message size (%d vs %d)", len(response.Body), sp.maxMessageSize)
		response = &httpgrpc.HTTPResponse{
			Code: http.StatusRequestEntityTooLarge,
			Body: []byte(errMsg),
		}
	}
	var c client.PoolClient
	var retries int

	for {
		c, err = sp.frontendPool.GetClientFor(frontendAddress)
		if err != nil {
			break
		}

		// Even if this query has been cancelled, we still want to tell the frontend about it, otherwise the frontend will wait for a result until it times out.
		frontendCtx := context.WithoutCancel(ctx)

		// Response is empty and uninteresting.
		_, err = c.(frontendv2pb.FrontendForQuerierClient).QueryResult(frontendCtx, &frontendv2pb.QueryResultRequest{
			QueryID:      queryID,
			HttpResponse: response,
			Stats:        stats,
		})
		if err == nil || retries >= maxNotifyFrontendRetries {
			break
		}
		// If the used connection returned and error, remove it from the pool and retry.
		level.Warn(logger).Log("msg", "retrying to notify frontend about finished query", "err", err, "frontend", frontendAddress, "retries", retries)

		sp.frontendPool.RemoveClientFor(frontendAddress)
		retries++
	}

	if err != nil {
		level.Error(logger).Log("msg", "error notifying frontend about finished query", "err", err, "frontend", frontendAddress)
	}
}

func (sp *schedulerProcessor) createFrontendClient(addr string) (client.PoolClient, error) {
	opts, err := sp.grpcConfig.DialOption([]grpc.UnaryClientInterceptor{
		otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
		middleware.ClientUserHeaderInterceptor,
		middleware.UnaryClientInstrumentInterceptor(sp.frontendClientRequestDuration),
	}, nil)

	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}

	return &frontendClient{
		FrontendForQuerierClient: frontendv2pb.NewFrontendForQuerierClient(conn),
		HealthClient:             grpc_health_v1.NewHealthClient(conn),
		conn:                     conn,
	}, nil
}

type frontendClient struct {
	frontendv2pb.FrontendForQuerierClient
	grpc_health_v1.HealthClient
	conn *grpc.ClientConn
}

func (fc *frontendClient) Close() error {
	return fc.conn.Close()
}
