// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/worker/frontend_processor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package worker

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/httpgrpc"
	"go.uber.org/atomic"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/frontend/v1/frontendv1pb"
	querier_stats "github.com/grafana/mimir/pkg/querier/stats"
)

var (
	processorBackoffConfig = backoff.Config{
		MinBackoff: 250 * time.Millisecond,
		MaxBackoff: 2 * time.Second,
	}

	errQuerierFrontendProcessingLoopTerminated = cancellation.NewErrorf("querier frontend processing loop terminated")
)

func newFrontendProcessor(cfg Config, handler RequestHandler, log log.Logger) *frontendProcessor {
	return &frontendProcessor{
		log:            log,
		handler:        handler,
		maxMessageSize: cfg.QueryFrontendGRPCClientConfig.MaxSendMsgSize,
		querierID:      cfg.QuerierID,

		frontendClientFactory: func(conn *grpc.ClientConn) frontendv1pb.FrontendClient {
			return frontendv1pb.NewFrontendClient(conn)
		},
	}
}

// Handles incoming queries from frontend.
type frontendProcessor struct {
	handler        RequestHandler
	maxMessageSize int
	querierID      string

	log log.Logger

	frontendClientFactory func(conn *grpc.ClientConn) frontendv1pb.FrontendClient
}

// notifyShutdown implements processor.
func (fp *frontendProcessor) notifyShutdown(ctx context.Context, conn *grpc.ClientConn, address string) {
	client := fp.frontendClientFactory(conn)

	req := &frontendv1pb.NotifyClientShutdownRequest{ClientID: fp.querierID}
	if _, err := client.NotifyClientShutdown(ctx, req); err != nil {
		// Since we're shutting down there's nothing we can do except logging it.
		level.Warn(fp.log).Log("msg", "failed to notify querier shutdown to query-frontend", "address", address, "err", err)
	}
}

// processQueriesOnSingleStream tries to establish a stream to the query-frontend and then process queries received
// on the stream. This function loops until workerCtx is canceled.
func (fp *frontendProcessor) processQueriesOnSingleStream(workerCtx context.Context, conn *grpc.ClientConn, address string) {
	client := fp.frontendClientFactory(conn)

	// Run the gRPC client and process all the queries in a dedicated context that we call the "execution context".
	// The execution context is cancelled once the workerCtx is cancelled AND there's no inflight query executing.
	execCtx, execCancel, inflightQuery := newExecutionContext(workerCtx, fp.log)
	defer execCancel(errQuerierFrontendProcessingLoopTerminated)

	backoff := backoff.New(execCtx, processorBackoffConfig)
	for backoff.Ongoing() {
		c, err := client.Process(execCtx)
		if err != nil {
			level.Error(fp.log).Log("msg", "error contacting frontend", "address", address, "err", err)
			backoff.Wait()
			continue
		}

		if err := fp.process(execCtx, c, address, inflightQuery); err != nil {
			if !isErrCancel(err, log.With(fp.log, "address", address)) {
				level.Error(fp.log).Log("msg", "error processing requests", "address", address, "err", err)
				backoff.Wait()
				continue
			}
		}

		backoff.Reset()
	}
}

// process loops processing requests on an established stream.
func (fp *frontendProcessor) process(execCtx context.Context, c frontendv1pb.Frontend_ProcessClient, address string, inflightQuery *atomic.Bool) (err error) {
	// Build a child context so we can cancel a query when the stream is closed.
	ctx, cancel := context.WithCancelCause(execCtx)
	defer cancel(cancellation.NewErrorf("query-frontend loop in querier for query-frontend %v terminated with error: %v", address, err))

	for ctx.Err() == nil {
		request, err := c.Recv()
		if err != nil {
			return err
		}

		switch request.Type {
		case frontendv1pb.HTTP_REQUEST:
			inflightQuery.Store(true)

			// Handle the request on a "background" goroutine, so we go back to
			// blocking on c.Recv().  This allows us to detect the stream closing
			// and cancel the query.  We don't actually handle queries in parallel
			// here, as we're running in lock step with the server - each Recv is
			// paired with a Send.
			go fp.runRequest(ctx, request.HttpRequest, request.StatsEnabled, time.Duration(request.QueueTimeNanos), func(response *httpgrpc.HTTPResponse, stats *querier_stats.Stats) error {
				defer inflightQuery.Store(false)

				// Ensure responses that are too big are not retried.
				msgToFrontend := &frontendv1pb.ClientToFrontend{
					HttpResponse: response,
					Stats:        stats,
				}

				if msgSize := msgToFrontend.Size(); msgSize >= fp.maxMessageSize {
					errMsg := fmt.Sprintf("response larger than the max (%d vs %d)", msgSize, fp.maxMessageSize)
					msgToFrontend.HttpResponse.Code = http.StatusRequestEntityTooLarge
					msgToFrontend.HttpResponse.Body = []byte(errMsg)
					level.Error(fp.log).Log("msg", "query response larger than limit", "err", errMsg)
				}

				return c.Send(msgToFrontend)
			})

		case frontendv1pb.GET_ID:
			err := c.Send(&frontendv1pb.ClientToFrontend{ClientID: fp.querierID})
			if err != nil {
				return err
			}

		default:
			return fmt.Errorf("unknown request type: %v", request.Type)
		}
	}

	return ctx.Err()
}

func (fp *frontendProcessor) runRequest(ctx context.Context, request *httpgrpc.HTTPRequest, statsEnabled bool, queueTime time.Duration, sendHTTPResponse func(response *httpgrpc.HTTPResponse, stats *querier_stats.Stats) error) {
	// Create a per-request context and cancel it once we're done processing the request.
	// This is important for queries that stream chunks from ingesters to the querier, as SeriesChunksStreamReader relies
	// on the context being cancelled to abort streaming and terminate a goroutine if the query is aborted. Requests that
	// go direct to a querier's HTTP API have a context created and cancelled in a similar way by the Go runtime's
	// net/http package.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(errQueryEvaluationFinished)

	var stats *querier_stats.Stats
	if statsEnabled {
		stats, ctx = querier_stats.ContextWithEmptyStats(ctx)
		stats.AddQueueTime(queueTime)
	}

	response, err := fp.handler.Handle(ctx, request)
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

	if err := sendHTTPResponse(response, stats); err != nil {
		level.Error(fp.log).Log("msg", "error sending query response", "err", err)
	}
}
