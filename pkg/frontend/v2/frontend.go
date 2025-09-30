// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/frontend/v2/frontend.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package v2

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"maps"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/netutil"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/timestamp"
	"go.opentelemetry.io/otel"
	"go.uber.org/atomic"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/scheduler/schedulerdiscovery"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/grpcencoding/s2"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

var tracer = otel.Tracer("pkg/frontend/v2")

var errExecutingQueryRoundTripFinished = cancellation.NewErrorf("executing query round trip finished")
var errFinishedReceivingResponse = cancellation.NewErrorf("finished receiving response from querier")
var errStreamClosed = cancellation.NewErrorf("stream closed")
var errUnexpectedHTTPResponse = errors.New("unexpected HTTP response to non-HTTP request")

// Config for a Frontend.
type Config struct {
	SchedulerAddress  string            `yaml:"scheduler_address"`
	DNSLookupPeriod   time.Duration     `yaml:"scheduler_dns_lookup_period" category:"advanced"`
	WorkerConcurrency int               `yaml:"scheduler_worker_concurrency" category:"advanced"`
	GRPCClientConfig  grpcclient.Config `yaml:"grpc_client_config" doc:"description=Configures the gRPC client used to communicate between the query-frontends and the query-schedulers."`

	// Used to find local IP address, that is sent to scheduler and querier-worker.
	InfNames   []string `yaml:"instance_interface_names" category:"advanced" doc:"default=[<private network interfaces>]"`
	EnableIPv6 bool     `yaml:"instance_enable_ipv6" category:"advanced"`

	// If set, address is not computed from interfaces.
	Addr string `yaml:"address" category:"advanced"`
	Port int    `category:"advanced"`

	// These configuration options are injected internally.
	QuerySchedulerDiscovery schedulerdiscovery.Config `yaml:"-"`
	LookBackDelta           time.Duration             `yaml:"-"`
	QueryStoreAfter         time.Duration             `yaml:"-"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	f.StringVar(&cfg.SchedulerAddress, "query-frontend.scheduler-address", "", fmt.Sprintf("Address of the query-scheduler component, in host:port format. The host should resolve to all query-scheduler instances. This option should be set only when query-scheduler component is in use and -%s is set to '%s'.", schedulerdiscovery.ModeFlagName, schedulerdiscovery.ModeDNS))
	f.DurationVar(&cfg.DNSLookupPeriod, "query-frontend.scheduler-dns-lookup-period", 10*time.Second, "How often to resolve the scheduler-address, in order to look for new query-scheduler instances.")
	f.IntVar(&cfg.WorkerConcurrency, "query-frontend.scheduler-worker-concurrency", 5, "Number of concurrent workers forwarding queries to single query-scheduler.")

	cfg.InfNames = netutil.PrivateNetworkInterfacesWithFallback([]string{"eth0", "en0"}, logger)
	f.BoolVar(&cfg.EnableIPv6, "query-frontend.instance-enable-ipv6", false, "Enable using a IPv6 instance address (default false).")
	f.Var((*flagext.StringSlice)(&cfg.InfNames), "query-frontend.instance-interface-names", "List of network interface names to look up when finding the instance IP address. This address is sent to query-scheduler and querier, which uses it to send the query response back to query-frontend.")
	f.StringVar(&cfg.Addr, "query-frontend.instance-addr", "", "IP address to advertise to the querier (via scheduler) (default is auto-detected from network interfaces).")
	f.IntVar(&cfg.Port, "query-frontend.instance-port", 0, "Port to advertise to querier (via scheduler) (defaults to server.grpc-listen-port).")

	cfg.GRPCClientConfig.CustomCompressors = []string{s2.Name}
	cfg.GRPCClientConfig.RegisterFlagsWithPrefix("query-frontend.grpc-client-config", f)
}

func (cfg *Config) Validate() error {
	if cfg.QuerySchedulerDiscovery.Mode == schedulerdiscovery.ModeRing && cfg.SchedulerAddress != "" {
		return fmt.Errorf("scheduler address cannot be specified when query-scheduler service discovery mode is set to '%s'", cfg.QuerySchedulerDiscovery.Mode)
	}

	return cfg.GRPCClientConfig.Validate()
}

func (cfg *Config) IsSchedulerConfigured() bool {
	return cfg.SchedulerAddress != "" || cfg.QuerySchedulerDiscovery.Mode == schedulerdiscovery.ModeRing
}

type Limits interface {
	// QueryIngestersWithin returns the maximum lookback beyond which queries are not sent to ingester.
	QueryIngestersWithin(user string) time.Duration
}

// Frontend implements GrpcRoundTripper. It queues HTTP requests,
// dispatches them to backends via gRPC, and handles retries for requests which failed.
type Frontend struct {
	services.Service

	cfg    Config
	log    log.Logger
	limits Limits
	codec  querymiddleware.Codec

	lastQueryID atomic.Uint64

	// frontend workers will read from this channel, and send request to scheduler.
	requestsCh chan *frontendRequest

	schedulerWorkers        *frontendSchedulerWorkers
	schedulerWorkersWatcher *services.FailureWatcher
	requests                *requestsInProgress
	inflightRequestCount    prometheus.Gauge
}

// queryResultWithBody contains the result for a query and optionally a streaming version of the response body.
// In the non-streaming case, the response body is contained in queryResult.HttpResponse.Body and bodyStream is nil.
// In the streaming case, queryResult.HttpResponse.Body is empty and bodyStream contains the streaming response body.
type queryResultWithBody struct {
	queryResult *frontendv2pb.QueryResultRequest
	bodyStream  io.ReadCloser
}

type frontendRequest struct {
	queryID                uint64
	userID                 string
	statsEnabled           bool
	touchedQueryComponents []string

	ctx        context.Context
	spanLogger *spanlogger.SpanLogger

	enqueue chan enqueueResult

	// If this is a httpgrpc request, then these fields will be populated:
	httpRequest  *httpgrpc.HTTPRequest
	httpResponse chan queryResultWithBody

	// If this is a Protobuf request, then these fields will be populated:
	protobufRequest         proto.Message
	protobufRequestHeaders  map[string][]string
	protobufResponseStream  *ProtobufResponseStream
	protobufResponseStarted *atomic.Bool
	protobufResponseDone    chan struct{} // Used to signal when the response has been completely read (but possibly not yet consumed) and we can stop monitoring the request context for cancellation.
}

type enqueueStatus int

const (
	// Sent to scheduler successfully, and frontend should wait for response now.
	waitForResponse enqueueStatus = iota

	// Failed to forward request to scheduler, frontend will try again.
	failed

	// User has too many outstanding requests. Frontend should not try again.
	tooManyRequests

	// The scheduler returned an error. Frontend should not try again.
	schedulerReturnedError
)

type enqueueResult struct {
	status enqueueStatus

	// If status is failed and if it was because of a client error on the frontend,
	// the clientErr should be updated with the appropriate error.
	clientErr error

	// If status is schedulerReturnedError, schedulerErr contains the error returned by the scheduler.
	schedulerErr string

	cancelCh chan<- uint64 // Channel that can be used for request cancellation. If nil, cancellation is not possible.
}

// NewFrontend creates a new frontend.
func NewFrontend(cfg Config, limits Limits, log log.Logger, reg prometheus.Registerer, codec querymiddleware.Codec) (*Frontend, error) {
	requestsCh := make(chan *frontendRequest)
	toSchedulerAdapter := frontendToSchedulerAdapter{}
	schedulerWorkers, err := newFrontendSchedulerWorkers(cfg, net.JoinHostPort(cfg.Addr, strconv.Itoa(cfg.Port)), requestsCh, toSchedulerAdapter, log, reg)
	if err != nil {
		return nil, err
	}

	f := &Frontend{
		cfg:                     cfg,
		log:                     log,
		limits:                  limits,
		codec:                   codec,
		requestsCh:              requestsCh,
		schedulerWorkers:        schedulerWorkers,
		schedulerWorkersWatcher: services.NewFailureWatcher(),
		requests:                newRequestsInProgress(),
		inflightRequestCount: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_query_frontend_queries_in_progress",
			Help: "Number of queries in progress handled by this frontend.",
		}),
	}
	// Randomize to avoid getting responses from queries sent before restart, which could lead to mixing results
	// between different queries. Note that frontend verifies the user, so it cannot leak results between tenants.
	// This isn't perfect, but better than nothing.
	f.lastQueryID.Store(rand.Uint64())

	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_query_frontend_connected_schedulers",
		Help: "Number of schedulers this frontend is connected to.",
	}, func() float64 {
		return float64(f.schedulerWorkers.getWorkersCount())
	})

	f.Service = services.NewBasicService(f.starting, f.running, f.stopping)
	return f, nil
}

func (f *Frontend) starting(ctx context.Context) error {
	f.schedulerWorkersWatcher.WatchService(f.schedulerWorkers)

	return errors.Wrap(services.StartAndAwaitRunning(ctx, f.schedulerWorkers), "failed to start frontend scheduler workers")
}

func (f *Frontend) running(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-f.schedulerWorkersWatcher.Chan():
		return errors.Wrap(err, "query-frontend subservice failed")
	}
}

func (f *Frontend) stopping(_ error) error {
	return errors.Wrap(services.StopAndAwaitTerminated(context.Background(), f.schedulerWorkers), "failed to stop frontend scheduler workers")
}

func (f *Frontend) createNewRequest(ctx context.Context) (*frontendRequest, context.Context, context.CancelCauseFunc, error) {
	if s := f.State(); s != services.Running {
		// This should never happen: requests should be blocked by frontendRunningRoundTripper before they get here.
		return nil, nil, nil, fmt.Errorf("frontend not running: %v", s)
	}

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	userID := tenant.JoinTenantIDs(tenantIDs)

	ctx, cancel := context.WithCancelCause(ctx)

	freq := &frontendRequest{
		queryID:      f.lastQueryID.Inc(),
		userID:       userID,
		statsEnabled: stats.IsEnabled(ctx),

		ctx: ctx,

		// Buffer of 1 to ensure response or error can be written to the channel
		// even if this goroutine goes away due to client context cancellation.
		enqueue: make(chan enqueueResult, 1),

		spanLogger: spanlogger.FromContext(ctx, f.log),
	}

	return freq, ctx, cancel, nil
}

// RoundTripGRPC round trips a httpgrpc request.
func (f *Frontend) RoundTripGRPC(ctx context.Context, httpRequest *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, io.ReadCloser, error) {
	freq, ctx, cancel, err := f.createNewRequest(ctx)
	if err != nil {
		return nil, nil, err
	}

	freq.httpRequest = httpRequest
	// Buffer of 1 to ensure response or error can be written to the channel
	// even if this goroutine goes away due to client context cancellation.
	freq.httpResponse = make(chan queryResultWithBody, 1)

	freq.touchedQueryComponents, err = f.extractTouchedQueryComponentsForHTTPRequest(ctx, httpRequest, time.Now())
	if err != nil {
		return nil, nil, err
	}

	f.requests.put(freq)
	f.inflightRequestCount.Inc()
	// delete is called through the cleanup func executed either in the defer or by the caller closing the body.

	cleanup := func() {
		f.requests.delete(freq.queryID)
		cancel(errExecutingQueryRoundTripFinished)
		f.inflightRequestCount.Dec()
	}
	cleanupInDefer := true
	defer func() {
		if cleanupInDefer {
			cleanup()
		}
	}()

	cancelCh, err := f.enqueueRequestWithRetries(ctx, freq)
	if err != nil {
		return nil, nil, err
	}

	freq.spanLogger.DebugLog("msg", "request enqueued successfully, waiting for response")

	select {
	case <-ctx.Done():
		freq.spanLogger.DebugLog("msg", "request context cancelled after enqueuing request, aborting", "cause", context.Cause(ctx))

		select {
		case cancelCh <- freq.queryID:
			// cancellation sent.
		default:
			// failed to cancel, ignore.
			level.Warn(freq.spanLogger).Log("msg", "failed to send cancellation request to scheduler, queue full")
		}

		return nil, nil, context.Cause(ctx)

	case resp := <-freq.httpResponse:
		freq.spanLogger.DebugLog("msg", "received response")

		if stats.ShouldTrackHTTPGRPCResponse(resp.queryResult.HttpResponse) {
			stats := stats.FromContext(ctx)
			stats.Merge(resp.queryResult.Stats) // Safe if stats is nil.
		}

		// the cleanup will be triggered by the caller closing the body.
		cleanupInDefer = false
		body := &cleanupReadCloser{cleanup: cleanup}
		if resp.bodyStream != nil {
			body.rc = resp.bodyStream
		} else {
			body.rc = io.NopCloser(bytes.NewReader(resp.queryResult.HttpResponse.Body))
		}
		return resp.queryResult.HttpResponse, body, nil
	}
}

// DoProtobufRequest initiates a Protobuf request to queriers.
//
// If the returned error is nil, then callers must either Close the returned stream
// or cancel ctx to ensure resources are not leaked.
//
// minT and maxT should be the start and end time of the queried data.
// These timestamps should consider the lookback delta (ie. are not necessarily the time range provided in the query request).
func (f *Frontend) DoProtobufRequest(ctx context.Context, req proto.Message, minT, maxT time.Time) (*ProtobufResponseStream, error) {
	logger, ctx := spanlogger.New(ctx, f.log, tracer, "frontend.DoProtobufRequest")
	logger.SetTag("request.type", proto.MessageName(req))

	freq, ctx, cancel, err := f.createNewRequest(ctx)
	if err != nil {
		logger.Finish()
		return nil, err
	}

	freq.touchedQueryComponents = f.queryComponentQueueDimensionFromTimeParams([]string{freq.userID}, timestamp.FromTime(minT), timestamp.FromTime(maxT), time.Now())
	freq.protobufRequest = req
	freq.protobufRequestHeaders = maps.Clone(querymiddleware.HeadersToPropagateFromContext(ctx)) // Take a shallow copy of the headers, so that we don't mutate the shared map when adding trace headers later.
	freq.protobufResponseStarted = atomic.NewBool(false)
	freq.protobufResponseDone = make(chan struct{})
	freq.protobufResponseStream = &ProtobufResponseStream{
		ctx:        ctx,
		cancel:     cancel,
		spanLogger: freq.spanLogger,
		// Buffer of 1 to ensure response or error can be written to the channel
		// even if this goroutine goes away due to client context cancellation.
		messages:     make(chan protobufResponseMessage, 1),
		enqueueError: make(chan error, 1), // Note that we never close this channel, otherwise ProtobufResponseStream.Next() will not reliably return any buffered messages in the stream channel.
	}

	f.requests.put(freq)
	f.inflightRequestCount.Inc()

	go func() {
		defer func() {
			f.requests.delete(freq.queryID)
			cancel(errExecutingQueryRoundTripFinished)
			logger.Finish()
			f.inflightRequestCount.Dec()
		}()

		cancelCh, err := f.enqueueRequestWithRetries(ctx, freq)
		if err != nil {
			freq.protobufResponseStream.writeEnqueueError(err)
			return
		}

		freq.spanLogger.DebugLog("msg", "request enqueued successfully, waiting for response")
		<-ctx.Done() // The context will be cancelled if the request is cancelled by DoProtobufRequest's caller, if the response has been completely read, or if the stream is closed by the caller.

		if freq.protobufResponseStarted.Load() {
			select {
			case <-freq.protobufResponseDone:
				freq.spanLogger.DebugLog("msg", "finished receiving response")
				return

			// If we've already received some of the response, wait a short time for it to be completely read
			// by receiveResultForProtobufRequest.
			// This mitigates a race condition where the consumer reads the last message in the stream and
			// calls Close before protobufResponseDone is closed by receiveResultForProtobufRequest.
			// This would cause us to send an unnecessary cancellation request to the scheduler, which would
			// then break the connection between the querier and the scheduler. If this happened to all of a
			// querier's workers at the same time, it caused shuffle shard assignment churn.
			case <-time.After(20 * time.Millisecond):
				// Give up, fall through to below.
			}
		}

		freq.spanLogger.DebugLog("msg", "request context cancelled or response stream closed by caller after enqueuing request, aborting", "cause", context.Cause(ctx))

		select {
		case cancelCh <- freq.queryID:
			// cancellation sent.
		default:
			// failed to cancel, ignore.
			level.Warn(freq.spanLogger).Log("msg", "failed to send cancellation request to scheduler, queue full")
		}

		freq.protobufResponseStream.writeEnqueueError(context.Cause(ctx))
	}()

	return freq.protobufResponseStream, nil
}

type ProtobufResponseStream struct {
	// Why do we have two channels here?
	// Different goroutines write to each, and each needs to close its corresponding channel when finished.
	// There's no guarantee which order the goroutines finish in, and one may never be called at all.
	messages     chan protobufResponseMessage
	enqueueError chan error
	cancel       context.CancelCauseFunc

	ctx        context.Context
	spanLogger *spanlogger.SpanLogger
}

type protobufResponseMessage struct {
	msg *frontendv2pb.QueryResultStreamRequest
	err error
}

func (s *ProtobufResponseStream) write(msg *frontendv2pb.QueryResultStreamRequest, err error) error {
	if err == nil {
		err = s.errorFromMessage(msg)

		if err != nil {
			msg = nil
		}
	}

	if err != nil && !errors.Is(err, errStreamClosed) {
		_ = s.spanLogger.Error(err)
	}

	select {
	case s.messages <- protobufResponseMessage{msg: msg, err: err}:
		return nil
	case <-s.ctx.Done():
		return context.Cause(s.ctx)
	}
}

// writeEnqueueError writes an error message to the stream.
// This method must only be called once per ProtobufResponseStream instance to ensure it does not block.
func (s *ProtobufResponseStream) writeEnqueueError(err error) {
	if !errors.Is(err, errStreamClosed) {
		_ = s.spanLogger.Error(err)
	}

	// This is guaranteed not to block provided this method is only called once per request,
	// as enqueueError is buffered with a size of 1.
	s.enqueueError <- err
}

// Next returns the next available message from this stream, or an error if the stream
// has failed or the context provided to DoProtobufRequest or Next was cancelled.
//
// If no message is available and neither context has been cancelled, then Next blocks
// until either a message is received or either context is cancelled.
//
// Calling Next after an error has been returned by a previous Next call may lead to
// undefined behaviour.
func (s *ProtobufResponseStream) Next(ctx context.Context) (*frontendv2pb.QueryResultStreamRequest, error) {
	select {
	case resp := <-s.messages:
		if resp.err != nil {
			return nil, resp.err
		}

		return resp.msg, nil
	case err := <-s.enqueueError:
		// If the context provided to DoProtobufRequest is cancelled, it will call writeEnqueueError and so
		// s.enqueueError will contain the cancellation error.
		return nil, err
	case <-ctx.Done():
		// Note that we deliberately wait on the passed context, rather than s.ctx, as s.ctx is cancelled as soon
		// as the response has been completely received, but we want to continue reading any outstanding messages
		// from the stream unless the provided context (presumably for the query as a whole) is cancelled.
		return nil, context.Cause(ctx)
	}
}

func (s *ProtobufResponseStream) errorFromMessage(msg *frontendv2pb.QueryResultStreamRequest) error {
	e := msg.GetError()
	if e == nil {
		return nil
	}

	errorType, err := e.Type.ToPrometheusString()
	if err != nil {
		return err
	}

	return apierror.New(apierror.Type(errorType), e.Message)
}

func (s *ProtobufResponseStream) Close() {
	s.spanLogger.DebugLog("msg", "response stream closed")
	s.cancel(errStreamClosed)
}

func (f *Frontend) enqueueRequestWithRetries(ctx context.Context, freq *frontendRequest) (chan<- uint64, error) {
	maxAttempts := f.cfg.WorkerConcurrency + 1 // To make sure we hit at least two different schedulers.

	for attempt := range maxAttempts {
		freq.spanLogger.DebugLog("msg", "enqueuing request", "attempt", attempt+1, "maxAttempts", maxAttempts)

		select {
		case <-ctx.Done():
			freq.spanLogger.DebugLog("msg", "request context cancelled while enqueuing request, aborting", "cause", context.Cause(ctx))
			return nil, context.Cause(ctx)

		case f.requestsCh <- freq:
			// Enqueued in our worker queue, let's wait for response from scheduler.
			enqRes := <-freq.enqueue
			switch enqRes.status {
			case waitForResponse:
				// Succeeded, go wait for response from querier.
				return enqRes.cancelCh, nil

			case failed:
				if enqRes.clientErr != nil {
					// It failed because of a client error. No need to retry.
					return nil, httpgrpc.Errorf(http.StatusBadRequest, "failed to enqueue request: %s", enqRes.clientErr.Error())
				}

			case schedulerReturnedError:
				if freq.httpRequest != nil {
					freq.httpResponse <- queryResultWithBody{
						queryResult: &frontendv2pb.QueryResultRequest{
							HttpResponse: &httpgrpc.HTTPResponse{
								Code: http.StatusInternalServerError,
								Body: []byte(enqRes.schedulerErr),
							},
						}}

					return nil, nil
				}

				return nil, apierror.New(apierror.TypeInternal, enqRes.schedulerErr)

			case tooManyRequests:
				if freq.httpRequest != nil {
					freq.httpResponse <- queryResultWithBody{
						queryResult: &frontendv2pb.QueryResultRequest{
							HttpResponse: &httpgrpc.HTTPResponse{
								Code: http.StatusTooManyRequests,
								Body: []byte("too many outstanding requests"),
							},
						}}
					return nil, nil
				}

				return nil, apierror.New(apierror.TypeTooManyRequests, "too many outstanding requests")
			}

			// If we get to here, then the enqueue failed, so loop around and start another attempt if we can.
		}
	}

	freq.spanLogger.DebugLog("msg", "enqueuing request failed, retries are exhausted, aborting")

	if freq.httpRequest != nil {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "failed to enqueue request")
	}

	return nil, apierror.New(apierror.TypeInternal, "failed to enqueue request")
}

type cleanupReadCloser struct {
	cleanup func()
	rc      io.ReadCloser
}

func (c cleanupReadCloser) Read(p []byte) (n int, err error) {
	return c.rc.Read(p)
}

func (c cleanupReadCloser) Close() error {
	c.cleanup()
	return c.rc.Close()
}

func (f *Frontend) QueryResult(ctx context.Context, qrReq *frontendv2pb.QueryResultRequest) (*frontendv2pb.QueryResultResponse, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, err
	}
	userID := tenant.JoinTenantIDs(tenantIDs)

	req := f.requests.getAndDelete(qrReq.QueryID)
	// It is possible that some old response belonging to different user was received, if frontend has restarted.
	// To avoid leaking query results between users, we verify the user here.
	// To avoid mixing results from different queries, we randomize queryID counter on start.
	if req == nil {
		return nil, fmt.Errorf("query %d not found or response already received", qrReq.QueryID)
	}

	if req.userID != userID {
		return nil, fmt.Errorf("got response for query ID %d, expected user %q, but response had %q", qrReq.QueryID, req.userID, userID)
	}

	if req.httpResponse == nil {
		return nil, errUnexpectedHTTPResponse
	}

	select {
	case req.httpResponse <- queryResultWithBody{
		queryResult: qrReq,
	}:
		// Should always be possible, unless QueryResult is called multiple times with the same queryID.
	default:
		level.Warn(f.log).Log("msg", "failed to receive query result, a result for the same query has likely already been received", "queryID", qrReq.QueryID, "user", userID)
	}

	return &frontendv2pb.QueryResultResponse{}, nil
}

func (f *Frontend) QueryResultStream(stream frontendv2pb.FrontendForQuerier_QueryResultStreamServer) (err error) {
	defer func(s frontendv2pb.FrontendForQuerier_QueryResultStreamServer) {
		err := s.SendAndClose(&frontendv2pb.QueryResultResponse{})
		if err != nil && !errors.Is(globalerror.WrapGRPCErrorWithContextError(stream.Context(), err), context.Canceled) {
			level.Warn(f.log).Log("msg", "failed to close query result body stream", "err", err)
		}
	}(stream)

	tenantIDs, err := tenant.TenantIDs(stream.Context())
	if err != nil {
		return err
	}
	userID := tenant.JoinTenantIDs(tenantIDs)

	firstMessage, err := f.receiveFromStream(stream)
	if err != nil {
		return err
	}
	if firstMessage == nil {
		return errors.New("received EOF at start of stream")
	}

	req := f.requests.getAndDelete(firstMessage.QueryID)

	if req == nil {
		return fmt.Errorf("query %d not found or response already received", firstMessage.QueryID)
	}

	if req.userID != userID {
		return fmt.Errorf("got response for query ID %d, expected user %q, but response had %q", firstMessage.QueryID, req.userID, userID)
	}

	switch d := firstMessage.Data.(type) {
	case *frontendv2pb.QueryResultStreamRequest_Metadata:
		if req.httpResponse == nil {
			return errUnexpectedHTTPResponse
		}

		return f.receiveResultForHTTPRequest(req, firstMessage, d, stream)
	default:
		if req.protobufResponseStream == nil {
			return fmt.Errorf("unexpected first message type: %T", firstMessage.Data)
		}

		return f.receiveResultForProtobufRequest(req, firstMessage, stream)
	}
}

func (f *Frontend) receiveFromStream(stream frontendv2pb.FrontendForQuerier_QueryResultStreamServer) (*frontendv2pb.QueryResultStreamRequest, error) {
	resp, err := stream.Recv()
	if err == nil {
		return resp, nil
	}

	if errors.Is(err, io.EOF) {
		return nil, nil
	}

	if errors.Is(err, context.Canceled) {
		if cause := context.Cause(stream.Context()); cause != nil {
			return nil, fmt.Errorf("aborted streaming on canceled context: %w", cause)
		}
	}

	return nil, fmt.Errorf("failed to receive query result stream message: %w", err)
}

func (f *Frontend) receiveResultForHTTPRequest(req *frontendRequest, firstMessage *frontendv2pb.QueryResultStreamRequest, metadata *frontendv2pb.QueryResultStreamRequest_Metadata, stream frontendv2pb.FrontendForQuerier_QueryResultStreamServer) (err error) {
	reader, writer := io.Pipe()
	defer func(c *io.PipeWriter) {
		if err := c.CloseWithError(err); err != nil {
			level.Warn(f.log).Log("msg", "failed to close query result body writer", "err", err)
		}
	}(writer)

	res := queryResultWithBody{
		queryResult: &frontendv2pb.QueryResultRequest{
			QueryID: firstMessage.QueryID,
			Stats:   metadata.Metadata.Stats,
			HttpResponse: &httpgrpc.HTTPResponse{
				Code:    metadata.Metadata.Code,
				Headers: metadata.Metadata.Headers,
			},
		},
		bodyStream: reader,
	}

	select {
	case req.httpResponse <- res:
		// Should always be possible unless QueryResultStream is called multiple times with the same queryID.
	default:
		level.Warn(f.log).Log("msg", "failed to write query result to the response channel", "queryID", firstMessage.QueryID, "user", req.userID)
		return fmt.Errorf("failed to write query result to the response channel for query ID %d for user %q", firstMessage.QueryID, req.userID)
	}

	for {
		resp, err := f.receiveFromStream(stream)
		if err != nil {
			return err
		}
		if resp == nil {
			// EOF. We are done.
			return nil
		}

		d, ok := resp.Data.(*frontendv2pb.QueryResultStreamRequest_Body)
		if !ok {
			return fmt.Errorf("unexpected query result stream message type after first message: %T", resp.Data)
		}

		_, err = writer.Write(d.Body.Chunk)
		if err != nil {
			return fmt.Errorf("failed to write query result body chunk: %w", err)
		}
	}
}

func (f *Frontend) receiveResultForProtobufRequest(req *frontendRequest, firstMessage *frontendv2pb.QueryResultStreamRequest, stream frontendv2pb.FrontendForQuerier_QueryResultStreamServer) error {
	defer func() {
		// Signal that DoProtobufRequest can stop monitoring the request context for cancellation.
		close(req.protobufResponseDone)
		req.protobufResponseStream.cancel(errFinishedReceivingResponse)

		// Signal that there are no more messages coming.
		close(req.protobufResponseStream.messages)
	}()

	req.spanLogger.DebugLog("msg", "got first response message")
	req.protobufResponseStarted.Store(true)

	if err := req.protobufResponseStream.write(firstMessage, nil); err != nil {
		return err
	}

	for {
		msg, err := f.receiveFromStream(stream)
		if err != nil {
			req.spanLogger.DebugLog("msg", "received error", "err", err)
			_ = req.protobufResponseStream.write(nil, err) // If the context has already been cancelled, then we don't care.
			return err
		}
		if msg == nil {
			// EOF. We are done.
			// The response channel will be closed in the deferred close() call above.
			req.spanLogger.DebugLog("msg", "finished reading response stream")
			return nil
		}

		if err := req.protobufResponseStream.write(msg, nil); err != nil {
			return err
		}
	}
}

const ingesterQueryComponent = "ingester"
const storeGatewayQueryComponent = "store-gateway"
const ingesterAndStoreGatewayQueryComponent = "ingester-and-store-gateway"

func (f *Frontend) extractTouchedQueryComponentsForHTTPRequest(
	ctx context.Context, request *httpgrpc.HTTPRequest, now time.Time,
) ([]string, error) {
	var err error

	httpRequest, err := httpgrpc.ToHTTPRequest(ctx, request)
	if err != nil {
		return nil, err
	}

	tenantIDs, err := tenant.TenantIDs(httpRequest.Context())
	if err != nil {
		return nil, err
	}

	switch {
	case querymiddleware.IsRangeQuery(httpRequest.URL.Path), querymiddleware.IsInstantQuery(httpRequest.URL.Path):
		decodedRequest, err := f.codec.DecodeMetricsQueryRequest(httpRequest.Context(), httpRequest)
		if err != nil {
			return nil, err
		}
		minT := decodedRequest.GetMinT()
		maxT := decodedRequest.GetMaxT()

		return f.queryComponentQueueDimensionFromTimeParams(tenantIDs, minT, maxT, now), nil
	case querymiddleware.IsLabelsQuery(httpRequest.URL.Path):
		decodedRequest, err := f.codec.DecodeLabelsSeriesQueryRequest(httpRequest.Context(), httpRequest)
		if err != nil {
			return nil, err
		}

		return f.queryComponentQueueDimensionFromTimeParams(
			tenantIDs, decodedRequest.GetStart(), decodedRequest.GetEnd(), now,
		), nil
	case querymiddleware.IsCardinalityQuery(httpRequest.URL.Path), querymiddleware.IsActiveSeriesQuery(httpRequest.URL.Path), querymiddleware.IsActiveNativeHistogramMetricsQuery(httpRequest.URL.Path):
		// cardinality only hits ingesters
		return []string{ingesterQueryComponent}, nil
	default:
		// no query time params to parse; cannot infer query component
		level.Debug(f.log).Log("msg", "unsupported request type for additional queue dimensions", "query", httpRequest.URL)
		return nil, nil
	}
}

func (f *Frontend) queryComponentQueueDimensionFromTimeParams(
	tenantIDs []string, queryStartUnixMs, queryEndUnixMs int64, now time.Time,
) []string {
	longestQueryIngestersWithinWindow := validation.MaxDurationPerTenant(tenantIDs, f.limits.QueryIngestersWithin)
	shouldQueryIngesters := querier.ShouldQueryIngesters(
		longestQueryIngestersWithinWindow, now, queryEndUnixMs,
	)
	shouldQueryBlockStore := querier.ShouldQueryBlockStore(
		f.cfg.QueryStoreAfter, now, queryStartUnixMs,
	)

	if shouldQueryIngesters && !shouldQueryBlockStore {
		return []string{ingesterQueryComponent}
	} else if !shouldQueryIngesters && shouldQueryBlockStore {
		return []string{storeGatewayQueryComponent}
	}
	return []string{ingesterAndStoreGatewayQueryComponent}
}

// CheckReady determines if the query frontend is ready.  Function parameters/return
// chosen to match the same method in the ingester
func (f *Frontend) CheckReady(_ context.Context) error {
	workers := f.schedulerWorkers.getWorkersCount()

	// If frontend is connected to at least one scheduler, we are ready.
	if workers > 0 {
		return nil
	}

	msg := fmt.Sprintf("not ready: number of schedulers this worker is connected to is %d", workers)
	level.Info(f.log).Log("msg", msg)
	return errors.New(msg)
}

type requestsInProgress struct {
	mu       sync.Mutex
	requests map[uint64]*frontendRequest
}

func newRequestsInProgress() *requestsInProgress {
	return &requestsInProgress{
		requests: map[uint64]*frontendRequest{},
	}
}

func (r *requestsInProgress) put(req *frontendRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.requests[req.queryID] = req
}

func (r *requestsInProgress) delete(queryID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.requests, queryID)
}

func (r *requestsInProgress) getAndDelete(queryID uint64) *frontendRequest {
	r.mu.Lock()
	defer r.mu.Unlock()

	req := r.requests[queryID]
	delete(r.requests, queryID)
	return req
}
