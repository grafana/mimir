// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/worker/scheduler_processor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/types"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/querierpb"
	querier_stats "github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/httpgrpcutil"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/propagation"
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

var (
	tracer = otel.Tracer("querier/worker")

	errQuerierQuerySchedulerProcessingLoopTerminated = cancellation.NewErrorf("querier query-scheduler processing loop terminated")
	errQueryEvaluationFinished                       = cancellation.NewErrorf("query evaluation finished")
	errAlreadyFailed                                 = errors.New("the query-frontend stream has already failed")

	processorBackoffConfig = backoff.Config{
		MinBackoff: 250 * time.Millisecond,
		MaxBackoff: 2 * time.Second,
	}
)

func newSchedulerProcessor(cfg Config, httpHandler RequestHandler, protobufHandler ProtobufRequestHandler, log log.Logger, reg prometheus.Registerer) (*schedulerProcessor, []services.Service) {
	p := &schedulerProcessor{
		log:             log,
		httpHandler:     httpHandler,
		protobufHandler: protobufHandler,
		streamResponse:  streamResponse,
		maxMessageSize:  cfg.QueryFrontendGRPCClientConfig.MaxSendMsgSize,
		querierID:       cfg.QuerierID,
		grpcConfig:      cfg.QueryFrontendGRPCClientConfig.Config,

		schedulerClientFactory: func(conn *grpc.ClientConn) schedulerpb.SchedulerForQuerierClient {
			return schedulerpb.NewSchedulerForQuerierClient(conn)
		},

		frontendClientRequestDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cortex_querier_query_frontend_request_duration_seconds",
			Help:    "Time spend doing requests to frontend.",
			Buckets: prometheus.ExponentialBuckets(0.001, 4, 6),
		}, []string{"operation", "status_code"}),

		invalidClusterValidation: util.NewRequestInvalidClusterValidationLabelsTotalCounter(reg, "query-scheduler-processor", util.GRPCProtocol),
	}

	frontendClientsGauge := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_querier_query_frontend_clients",
		Help: "The current number of clients connected to query-frontend.",
	})

	poolConfig := client.PoolConfig{
		CheckInterval:          5 * time.Second,
		HealthCheckEnabled:     true,
		HealthCheckTimeout:     1 * time.Second,
		HealthCheckGracePeriod: cfg.QueryFrontendGRPCClientConfig.HealthCheckGracePeriod,
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
	stats *querier_stats.SafeStats,
	logger log.Logger) error

// Handles incoming queries from query-scheduler.
type schedulerProcessor struct {
	log             log.Logger
	httpHandler     RequestHandler
	protobufHandler ProtobufRequestHandler
	streamResponse  frontendResponseStreamer
	grpcConfig      grpcclient.Config
	maxMessageSize  int
	querierID       string

	frontendPool                  *client.Pool
	frontendClientRequestDuration *prometheus.HistogramVec
	invalidClusterValidation      *prometheus.CounterVec

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

	var schedulerStreamError error
	haveSchedulerStreamError := make(chan struct{})

	for {
		request, err := c.Recv()
		if err != nil {
			schedulerStreamError = err
			close(haveSchedulerStreamError)

			if grpcutil.IsCanceled(err) {
				cancel(cancellation.NewErrorf("query cancelled: %w", err))
			} else {
				// If we got another kind of error (eg. scheduler crashed), continue processing the query before returning.
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

			// Ignore errors here. If we cannot get parent span, we just don't create new one.
			if parentSpanContext, valid := contextWithSpanFromRequest(ctx, request); valid {
				var spanDescription string

				if request.GetProtobufRequest() != nil {
					if messageName, err := types.AnyMessageName(request.GetProtobufRequest().Payload); err == nil {
						spanDescription = messageName
					} else {
						spanDescription = request.GetProtobufRequest().Payload.TypeUrl
					}
				} else {
					spanDescription = "HTTP-over-gRPC"
				}

				var queueSpan trace.Span
				ctx, queueSpan = tracer.Start(parentSpanContext, fmt.Sprintf("runRequest: %v", spanDescription))
				defer queueSpan.End()
			}
			logger := util_log.WithContext(ctx, sp.log)

			var stats *querier_stats.SafeStats
			if request.StatsEnabled {
				stats, ctx = querier_stats.ContextWithEmptyStats(ctx)
				stats.AddQueueTime(time.Duration(request.QueueTimeNanos))
			}

			switch payload := request.Payload.(type) {
			case *schedulerpb.SchedulerToQuerier_HttpRequest:
				// We might have created a new span above, so reset the trace ID and span ID in the embedded HTTP request so
				// the HTTP tracing middleware creates its span beneath the one created above.
				otel.GetTextMapPropagator().Inject(ctx, (*httpgrpcutil.HttpgrpcHeadersCarrier)(request.GetHttpRequest()))
				sp.runHttpRequest(ctx, logger, request.QueryID, request.FrontendAddress, stats, payload.HttpRequest)
			case *schedulerpb.SchedulerToQuerier_ProtobufRequest:
				sp.runProtobufRequest(ctx, logger, request.QueryID, request.FrontendAddress, payload.ProtobufRequest)
			default:
				response := &httpgrpc.HTTPResponse{
					Code: http.StatusBadRequest,
					Body: []byte(fmt.Sprintf("unknown request payload type %T", request.Payload)),
				}

				sp.sendHttpResponseToQueryFrontend(ctx, logger, request.QueryID, request.FrontendAddress, nil, response, false)
			}

			// Report back to scheduler that processing of the query has finished.
			if err := c.Send(&schedulerpb.QuerierToScheduler{}); err != nil {
				// If the stream has been broken, as happens when the scheduler wants to notify the querier of a cancelled query,
				// it's expected that Send() will return an EOF and the 'real' error will be returned by Recv(). So if we get an
				// EOF, try to get the error received by Recv() above.
				// This is racy (we might get an error from Send() above before Recv() returns), so we wait a short while for
				// the error to be received.
				useSchedulerStreamError := false

				if errors.Is(err, io.EOF) {
					select {
					case <-haveSchedulerStreamError:
						useSchedulerStreamError = true
					case <-time.After(500 * time.Millisecond):
						// Give up.
					}
				}

				if !useSchedulerStreamError {
					level.Error(logger).Log("msg", "error notifying scheduler about finished query", "err", err, "addr", address)
				} else if grpcutil.IsCanceled(schedulerStreamError) {
					level.Debug(logger).Log("msg", "could not notify scheduler about finished query because query execution was cancelled", "err", schedulerStreamError, "addr", address)
				} else {
					level.Error(logger).Log(
						"msg", "error notifying scheduler about finished query after the scheduler stream failed",
						"err", err,
						"addr", address,
						"schedulerStreamError", schedulerStreamError,
					)
				}
			}
		}()
	}
}

func contextWithSpanFromRequest(ctx context.Context, request *schedulerpb.SchedulerToQuerier) (context.Context, bool) {
	switch request.Payload.(type) {
	case *schedulerpb.SchedulerToQuerier_HttpRequest:
		return httpgrpcutil.ContextWithSpanFromRequest(ctx, request.GetHttpRequest())
	case *schedulerpb.SchedulerToQuerier_ProtobufRequest:
		carrier := schedulerpb.MetadataMapTracingCarrier(schedulerpb.MetadataSliceToMap(request.GetProtobufRequest().Metadata))
		ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)
		return ctx, trace.SpanFromContext(ctx).SpanContext().IsValid()
	default:
		return ctx, false
	}
}

func (sp *schedulerProcessor) runHttpRequest(ctx context.Context, logger log.Logger, queryID uint64, frontendAddress string, stats *querier_stats.SafeStats, request *httpgrpc.HTTPRequest) {
	response, err := sp.httpHandler.Handle(ctx, request)
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
	if msgSize := response.Size(); msgSize >= sp.maxMessageSize {
		level.Error(logger).Log("msg", "response larger than max message size", "size", msgSize, "max_message_size", sp.maxMessageSize)

		errMsg := fmt.Sprintf("response larger than the max message size (%d vs %d)", msgSize, sp.maxMessageSize)
		response = &httpgrpc.HTTPResponse{
			Code: http.StatusRequestEntityTooLarge,
			Body: []byte(errMsg),
		}
	}

	var hasStreamHeader bool
	response.Headers, hasStreamHeader = removeStreamingHeader(response.Headers)
	shouldStream := hasStreamHeader && len(response.Body) > responseStreamingBodyChunkSizeBytes

	// Protect against not-yet-exited querier handler goroutines that could
	// still be incrementing stats when sent for marshaling below.
	stats = stats.Copy()
	sp.sendHttpResponseToQueryFrontend(ctx, logger, queryID, frontendAddress, stats, response, shouldStream)
}

func (sp *schedulerProcessor) sendHttpResponseToQueryFrontend(ctx context.Context, logger log.Logger, queryID uint64, frontendAddress string, stats *querier_stats.SafeStats, response *httpgrpc.HTTPResponse, shouldStream bool) {
	var c client.PoolClient

	// Even if this query has been cancelled, we still want to tell the frontend about it, otherwise the frontend will wait for a result until it times out.
	frontendCtx := context.WithoutCancel(ctx)
	bof := createFrontendBackoff(frontendCtx)

	var err error
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
		bof.Wait()
	}

	if err != nil {
		level.Error(logger).Log("msg", "error notifying frontend about finished query", "err", err, "frontend", frontendAddress, "query_id", queryID)
	}
}

func createFrontendBackoff(ctx context.Context) *backoff.Backoff {
	return backoff.New(ctx, backoff.Config{
		MinBackoff: 5 * time.Millisecond,
		MaxBackoff: 100 * time.Millisecond,
		MaxRetries: maxNotifyFrontendRetries,
	})
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
	stats *querier_stats.SafeStats,
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

func (sp *schedulerProcessor) runProtobufRequest(ctx context.Context, logger log.Logger, queryID uint64, frontendAddress string, request *schedulerpb.ProtobufRequest) {
	writer := newGrpcStreamWriter(queryID, frontendAddress, sp.frontendPool, logger)
	metadataMap := schedulerpb.MetadataSliceToMap(request.Metadata)
	sp.protobufHandler.HandleProtobuf(ctx, request.Payload, propagation.MapCarrier(metadataMap), writer)
	writer.Close(ctx)
}

type grpcStreamWriter struct {
	queryID         uint64
	frontendAddress string
	clientPool      frontendClientPool
	logger          log.Logger

	client client.PoolClient
	stream frontendv2pb.FrontendForQuerier_QueryResultStreamClient
	failed bool
}

type frontendClientPool interface {
	GetClientFor(addr string) (client.PoolClient, error)
}

func newGrpcStreamWriter(queryID uint64, frontendAddress string, clientPool frontendClientPool, logger log.Logger) *grpcStreamWriter {
	return &grpcStreamWriter{
		queryID:         queryID,
		frontendAddress: frontendAddress,
		clientPool:      clientPool,
		logger:          logger,
	}
}

func (g *grpcStreamWriter) Write(ctx context.Context, msg *frontendv2pb.QueryResultStreamRequest) error {
	if g.failed {
		return errAlreadyFailed
	}

	msg.QueryID = g.queryID

	if g.stream != nil {
		// We already have a stream, use it.
		// If this fails, don't retry, as we can't know if the message made it to the frontend or not, which could lead to incorrect query results.
		if err := g.writeMessageToStream(g.stream, g.client, msg); err != nil {
			g.failed = true
			if grpcutil.IsCanceled(err) {
				level.Debug(g.logger).Log("msg", "attempt to send subsequent message to query-frontend failed because the request was canceled", "err", err, "frontendAddress", g.frontendAddress, "queryID", msg.QueryID)
			} else {
				level.Warn(g.logger).Log("msg", "attempt to send subsequent message to query-frontend failed", "err", err, "frontendAddress", g.frontendAddress, "queryID", msg.QueryID)
			}

			return err
		}

		return nil
	}

	// This is the first message, try to send the message with retries.
	// This is safe to do (unlike for subsequent messages), as the query-frontend will reject
	// the QueryResultStream calls if it has already seen any message for this query.
	ctx = context.WithoutCancel(ctx) // Even if the request has been cancelled, we want to inform the query-frontend, so that it doesn't wait for a response until it times out.
	bof := createFrontendBackoff(ctx)
	var err error
	for bof.Ongoing() {
		err = g.tryToStartStreamAndWriteFirstMessage(ctx, msg)
		if err == nil {
			return nil
		}

		level.Warn(g.logger).Log("msg", "attempt to send initial message to query-frontend failed", "err", err, "frontendAddress", g.frontendAddress, "queryID", msg.QueryID, "attempt", bof.NumRetries()+1)
		bof.Wait()
	}

	level.Error(g.logger).Log("msg", "abandoned attempt to send initial message to query-frontend", "lastErr", err, "frontendAddress", g.frontendAddress, "queryID", msg.QueryID)
	g.failed = true
	return err
}

func (g *grpcStreamWriter) tryToStartStreamAndWriteFirstMessage(ctx context.Context, msg *frontendv2pb.QueryResultStreamRequest) error {
	client, err := g.clientPool.GetClientFor(g.frontendAddress)
	if err != nil {
		return err
	}

	stream, err := client.(frontendv2pb.FrontendForQuerierClient).QueryResultStream(ctx)
	if err != nil {
		return err
	}

	if err := g.writeMessageToStream(stream, client, msg); err != nil {
		return err
	}

	g.client = client
	g.stream = stream

	return nil
}

func (g *grpcStreamWriter) writeMessageToStream(stream frontendv2pb.FrontendForQuerier_QueryResultStreamClient, client client.PoolClient, msg *frontendv2pb.QueryResultStreamRequest) error {
	err := stream.Send(msg)

	if err == nil {
		return nil
	}

	if errors.Is(err, io.EOF) {
		// If Send returns EOF, then that means the error was not generated by the client (ie. the querier) and we need to call RecvMsg to get that error.
		// See the docs for grpc.ClientStream for more details.
		receivedErr := stream.RecvMsg(&frontendv2pb.QueryResultResponse{})
		if receivedErr == nil {
			return fmt.Errorf("writing message to stream failed with EOF error (ie. not generated by client), and RecvMsg returned no error")
		} else {
			return fmt.Errorf("writing message to stream failed due to error not generated by client: %w", receivedErr)
		}
	}

	return err
}

func (g *grpcStreamWriter) Close(ctx context.Context) {
	if !g.failed && g.stream == nil {
		// We haven't sent anything to the query-frontend yet.
		// This should never happen, but if it does, send a message to the query-frontend so it's not waiting
		// for a response that will never come.
		msg := &frontendv2pb.QueryResultStreamRequest{
			Data: &frontendv2pb.QueryResultStreamRequest_Error{
				Error: &querierpb.Error{
					Type:    mimirpb.QUERY_ERROR_TYPE_INTERNAL,
					Message: "query execution completed without sending any messages (this is a bug)",
				},
			},
		}

		if err := g.Write(ctx, msg); err != nil {
			level.Warn(g.logger).Log("msg", "could not send message to frontend to notify of request that completed without any messages", "err", err)
			return
		}
	}

	if g.stream != nil {
		if _, err := g.stream.CloseAndRecv(); err != nil {
			if grpcutil.IsCanceled(err) {
				level.Debug(g.logger).Log("msg", "error closing query-frontend stream because the request was canceled", "err", err)
			} else {
				level.Warn(g.logger).Log("msg", "error closing query-frontend stream", "err", err)
			}
		}
	}
}

func (sp *schedulerProcessor) createFrontendClient(addr string) (client.PoolClient, error) {
	unary, stream := grpcclient.Instrument(sp.frontendClientRequestDuration)
	opts, err := sp.grpcConfig.DialOption(unary, stream, util.NewInvalidClusterValidationReporter(sp.grpcConfig.ClusterValidation.Label, sp.invalidClusterValidation, sp.log))
	if err != nil {
		return nil, err
	}
	opts = append(opts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))

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
