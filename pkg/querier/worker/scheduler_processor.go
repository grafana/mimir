// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/worker/scheduler_processor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package worker

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/mimir/pkg/util"

	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	querier_stats "github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/util/httpgrpcutil"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

const (
	// ResponseStreamingEnabledHeader is the header key used by http handlers to
	// indicate to the scheduler processor that its response should be streamed. This
	// header is internal to the querier only and removed before the response is sent
	// over the network.
	ResponseStreamingEnabledHeader      = "X-Mimir-Stream-Grpc-Response"
	responseStreamingBodyChunkSizeBytes = 1 * 1024 * 1024

	maxNotifyFrontendRetries = 5
)

var errQuerierQuerySchedulerProcessingLoopTerminated = cancellation.NewErrorf("querier query-scheduler processing loop terminated")
var errQueryEvaluationFinished = cancellation.NewErrorf("query evaluation finished")

func newSchedulerProcessor(cfg Config, handler RequestHandler, log log.Logger, reg prometheus.Registerer, cluster string) (*schedulerProcessor, []services.Service) {
	p := &schedulerProcessor{
		log:              log,
		handler:          handler,
		streamResponse:   streamResponse,
		maxMessageSize:   cfg.QueryFrontendGRPCClientConfig.MaxSendMsgSize,
		querierID:        cfg.QuerierID,
		cluster:          cluster,
		grpcConfig:       cfg.QueryFrontendGRPCClientConfig,
		streamingEnabled: cfg.ResponseStreamingEnabled,

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

type frontendResponseStreamer func(
	ctx context.Context,
	reqCtx context.Context,
	c client.PoolClient,
	queryID uint64,
	response *httpgrpc.HTTPResponse,
	stats *querier_stats.Stats,
	logger log.Logger) error

// Handles incoming queries from query-scheduler.
type schedulerProcessor struct {
	log              log.Logger
	handler          RequestHandler
	streamResponse   frontendResponseStreamer
	grpcConfig       grpcclient.Config
	maxMessageSize   int
	querierID        string
	cluster          string
	streamingEnabled bool

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

func (sp *schedulerProcessor) processQueriesOnSingleStream(workerCtx context.Context, conn *grpc.ClientConn, address string) {
	schedulerClient := sp.schedulerClientFactory(conn)

	// Run the querier loop (and so all the queries) in a dedicated context that we call the "execution context".
	// The execution context is cancelled once the workerCtx is cancelled AND there's no inflight query executing.
	execCtx, execCancel, inflightQuery := newExecutionContext(workerCtx, sp.log)
	defer execCancel(errQuerierQuerySchedulerProcessingLoopTerminated)

	backoff := backoff.New(execCtx, processorBackoffConfig)
	for backoff.Ongoing() {
		c, err := schedulerClient.QuerierLoop(execCtx)
		if err == nil {
			err = c.Send(&schedulerpb.QuerierToScheduler{QuerierID: sp.querierID})
		}

		if err != nil {
			level.Warn(sp.log).Log("msg", "error contacting scheduler", "err", err, "addr", address)
			backoff.Wait()
			continue
		}

		if err := sp.querierLoop(execCtx, c, address, inflightQuery); err != nil {
			if !isErrCancel(err, log.With(sp.log, "addr", address)) {
				// Do not log an error if the query-scheduler is shutting down.
				if s, ok := grpcutil.ErrorToStatus(err); !ok || !strings.Contains(s.Message(), schedulerpb.ErrSchedulerIsNotRunning.Error()) {
					level.Error(sp.log).Log("msg", "error processing requests from scheduler", "err", err, "addr", address)
				}
				backoff.Wait()
				continue
			}
		}

		backoff.Reset()
	}
}

// process loops processing requests on an established stream.
func (sp *schedulerProcessor) querierLoop(execCtx context.Context, c schedulerpb.SchedulerForQuerier_QuerierLoopClient, address string, inflightQuery *atomic.Bool) (err error) {
	// Build a child context so we can cancel a query when the stream is closed.
	// Note that we deliberately don't use c.Context() here, as that is cancelled as soon as the gRPC client observes an error,
	// but we don't always want to cancel queries if the scheduler stream reports an error (eg. if the scheduler crashed).
	ctx, cancel := context.WithCancelCause(execCtx)
	defer func() {
		cancel(cancellation.NewErrorf("query-scheduler loop in querier for query-scheduler %v terminated with error: %w", address, err))
	}()

	queryComplete := make(chan struct{})
	close(queryComplete) // Close the channel (signaling no query in progress) to simplify the logic below in the case where we receive no queries.

	waitForQuery := func(err error) {
		select {
		case <-queryComplete:
			// Query is already complete, nothing to do.
			return
		default:
			// Query is not complete.
			level.Info(sp.log).Log("msg", "query-scheduler loop in querier received non-cancellation error, waiting for inflight query to complete...", "err", err, "addr", address)
			<-queryComplete
			level.Info(sp.log).Log("msg", "query-scheduler loop in querier received non-cancellation error and inflight query is complete, continuing", "err", err, "addr", address)
		}
	}

	schedulerStreamError := atomic.NewError(nil)

	for {
		request, err := c.Recv()
		if err != nil {
			schedulerStreamError.Store(err)

			if grpcutil.IsCanceled(err) {
				cancel(cancellation.NewErrorf("query cancelled: %w", err))
			} else {
				// If we got another kind of error (eg. scheduler crashed), continue processing the query.
				waitForQuery(err)
			}

			return err
		}

		inflightQuery.Store(true)
		queryComplete = make(chan struct{})

		// Handle the request on a "background" goroutine, so we go back to
		// blocking on c.Recv().  This allows us to detect the stream closing
		// and cancel the query.  We don't actually handle queries in parallel
		// here, as we're running in lock step with the server - each Recv is
		// paired with a Send.
		go func() {
			defer inflightQuery.Store(false)
			defer close(queryComplete)

			// Create a per-request context and cancel it once we're done processing the request.
			// This is important for queries that stream chunks from ingesters to the querier, as SeriesChunksStreamReader relies
			// on the context being cancelled to abort streaming and terminate a goroutine if the query is aborted. Requests that
			// go direct to a querier's HTTP API have a context created and cancelled in a similar way by the Go runtime's
			// net/http package.
			ctx, cancel := context.WithCancelCause(ctx)
			defer cancel(errQueryEvaluationFinished)

			// We need to inject user into context for sending response back.
			ctx = user.InjectOrgID(ctx, request.UserID)

			tracer := opentracing.GlobalTracer()
			// Ignore errors here. If we cannot get parent span, we just don't create new one.
			parentSpanContext, _ := httpgrpcutil.GetParentSpanForRequest(tracer, request.HttpRequest)
			if parentSpanContext != nil {
				queueSpan, spanCtx := opentracing.StartSpanFromContextWithTracer(ctx, tracer, "querier_processor_runRequest", opentracing.ChildOf(parentSpanContext))
				defer queueSpan.Finish()

				ctx = spanCtx

				if err := sp.updateTracingHeaders(request.HttpRequest, queueSpan); err != nil {
					level.Warn(sp.log).Log("msg", "could not update trace headers on httpgrpc request, trace may be malformed", "err", err)
				}
			}
			logger := util_log.WithContext(ctx, sp.log)

			sp.runRequest(ctx, logger, request.QueryID, request.FrontendAddress, request.StatsEnabled, request.HttpRequest, time.Duration(request.QueueTimeNanos))

			// Report back to scheduler that processing of the query has finished.
			if err := c.Send(&schedulerpb.QuerierToScheduler{}); err != nil {
				if previousErr := schedulerStreamError.Load(); previousErr != nil {
					// If the stream has already been broken, it's expected that the Send() call will fail too.
					// The error returned by Recv() is often more descriptive, so we include it in this log line as well.
					level.Error(logger).Log(
						"msg", "error notifying scheduler about finished query after the scheduler stream previously failed and returned error",
						"err", err,
						"addr", address,
						"previousErr", previousErr,
					)
				} else {
					level.Error(logger).Log("msg", "error notifying scheduler about finished query", "err", err, "addr", address)
				}
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

	// Even if this query has been cancelled, we still want to tell the frontend about it, otherwise the frontend will wait for a result until it times out.
	frontendCtx := context.WithoutCancel(ctx)
	bof := backoff.New(frontendCtx, backoff.Config{
		MinBackoff: 5 * time.Millisecond,
		MaxBackoff: 100 * time.Millisecond,
		MaxRetries: maxNotifyFrontendRetries,
	})

	var hasStreamHeader bool
	response.Headers, hasStreamHeader = removeStreamingHeader(response.Headers)
	shouldStream := hasStreamHeader && sp.streamingEnabled && len(response.Body) > responseStreamingBodyChunkSizeBytes

	// Protect against not-yet-exited querier handler goroutines that could
	// still be incrementing stats when sent for marshaling below.
	stats = stats.Copy()

	for bof.Ongoing() {
		c, err = sp.frontendPool.GetClientFor(frontendAddress)
		if err != nil {
			break
		}

		if shouldStream {
			err = sp.streamResponse(frontendCtx, ctx, c, queryID, response, stats, sp.log)
		} else {
			// Response is empty and uninteresting.
			_, err = c.(frontendv2pb.FrontendForQuerierClient).QueryResult(frontendCtx, &frontendv2pb.QueryResultRequest{
				QueryID:      queryID,
				HttpResponse: response,
				Stats:        stats,
			})
		}
		if err == nil {
			break
		}

		level.Warn(logger).Log("msg", "retrying to notify frontend about finished query", "err", err, "frontend", frontendAddress, "retries", bof.NumRetries(), "query_id", queryID)
		sp.frontendPool.RemoveClient(c, frontendAddress)
		bof.Wait()
	}

	if err != nil {
		level.Error(logger).Log("msg", "error notifying frontend about finished query", "err", err, "frontend", frontendAddress, "query_id", queryID)
	}
}

func removeStreamingHeader(headers []*httpgrpc.Header) ([]*httpgrpc.Header, bool) {
	streamEnabledViaHeader := false
	for i, header := range headers {
		if header.Key == ResponseStreamingEnabledHeader {
			if header.Values[0] == "true" {
				streamEnabledViaHeader = true
			}
			headers = append(headers[:i], headers[i+1:]...)
			break
		}
	}
	return headers, streamEnabledViaHeader
}

func streamResponse(
	ctx context.Context,
	reqCtx context.Context,
	c client.PoolClient,
	queryID uint64,
	response *httpgrpc.HTTPResponse,
	stats *querier_stats.Stats,
	logger log.Logger,
) error {
	sc, err := c.(frontendv2pb.FrontendForQuerierClient).QueryResultStream(ctx)
	if err != nil {
		return fmt.Errorf("error creating stream to frontend: %w", err)
	}

	// Send metadata
	err = sc.Send(&frontendv2pb.QueryResultStreamRequest{
		QueryID: queryID,
		Data: &frontendv2pb.QueryResultStreamRequest_Metadata{Metadata: &frontendv2pb.QueryResultMetadata{
			Code:    response.Code,
			Headers: response.Headers,
			Stats:   stats,
		}},
	})
	if err != nil {
		return fmt.Errorf("error sending initial response to frontend: %w", err)
	}

	// The response metadata has been sent successfully. After this point we can no longer
	// return an error from this function as that would cause the response metadata to be sent
	// again. This would be rejected by the frontend and the retry could never succeed.
sendBody:
	// Send body chunks.
	for offset := 0; offset < len(response.Body); {
		select {
		case <-reqCtx.Done():
			level.Warn(logger).Log("msg", "response stream aborted", "cause", context.Cause(reqCtx))
			break sendBody
		default:
			err = sc.Send(&frontendv2pb.QueryResultStreamRequest{
				QueryID: queryID,
				Data: &frontendv2pb.QueryResultStreamRequest_Body{Body: &frontendv2pb.QueryResultBody{
					Chunk: response.Body[offset:min(offset+responseStreamingBodyChunkSizeBytes, len(response.Body))],
				}},
			})
			if err != nil {
				level.Warn(logger).Log("msg", "error streaming response body to frontend, aborting response stream", "err", err)
				break sendBody
			}
			offset += responseStreamingBodyChunkSizeBytes
		}
	}

	// Ignore error here because there's nothing we can do about it.
	_, _ = sc.CloseAndRecv()

	return nil
}

func (sp *schedulerProcessor) updateTracingHeaders(request *httpgrpc.HTTPRequest, span opentracing.Span) error {
	// Reset any trace headers on the HTTP request with the new parent span ID: the child span for the HTTP request created
	// by the HTTP tracing infrastructure uses the trace information in the HTTP request headers, ignoring the trace
	// information in the Golang context.
	return span.Tracer().Inject(span.Context(), opentracing.HTTPHeaders, httpGrpcHeaderWriter{request})
}

type httpGrpcHeaderWriter struct {
	request *httpgrpc.HTTPRequest
}

var _ opentracing.TextMapWriter = httpGrpcHeaderWriter{}

func (w httpGrpcHeaderWriter) Set(key, val string) {
	for _, h := range w.request.Headers {
		if h.Key == key {
			h.Values = []string{val}
			return
		}
	}

	w.request.Headers = append(w.request.Headers, &httpgrpc.Header{Key: key, Values: []string{val}})
}

func (sp *schedulerProcessor) createFrontendClient(addr string) (client.PoolClient, error) {
	loggerWithRate := util.NewLoggerWithRate(sp.log)
	unary, stream := grpcclient.Instrument(sp.frontendClientRequestDuration)
	unary = append(unary, middleware.ClusterUnaryClientInterceptor(sp.cluster, loggerWithRate.LogIfNeeded))
	opts, err := sp.grpcConfig.DialOption(unary, stream)
	if err != nil {
		return nil, err
	}

	// nolint:staticcheck // grpc.Dial() has been deprecated; we'll address it before upgrading to gRPC 2.
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
