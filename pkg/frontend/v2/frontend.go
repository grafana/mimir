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
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/netutil"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/scheduler/schedulerdiscovery"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/grpcencoding/s2"
	"github.com/grafana/mimir/pkg/util/httpgrpcutil"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

var errExecutingQueryRoundTripFinished = cancellation.NewErrorf("executing query round trip finished")

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

type Limits interface {
	// QueryIngestersWithin returns the maximum lookback beyond which queries are not sent to ingester.
	QueryIngestersWithin(user string) time.Duration
}

// Frontend implements GrpcRoundTripper. It queues HTTP requests,
// dispatches them to backends via gRPC, and handles retries for requests which failed.
type Frontend struct {
	services.Service

	cfg Config
	log log.Logger

	lastQueryID atomic.Uint64

	// frontend workers will read from this channel, and send request to scheduler.
	requestsCh chan *frontendRequest

	schedulerWorkers        *frontendSchedulerWorkers
	schedulerWorkersWatcher *services.FailureWatcher
	requests                *requestsInProgress
}

// queryResultWithBody contains the result for a query and optionally a streaming version of the response body.
// In the non-streaming case, the response body is contained in queryResult.HttpResponse.Body and bodyStream is nil.
// In the streaming case, queryResult.HttpResponse.Body is empty and bodyStream contains the streaming response body.
type queryResultWithBody struct {
	queryResult *frontendv2pb.QueryResultRequest
	bodyStream  io.ReadCloser
}

type frontendRequest struct {
	queryID      uint64
	request      *httpgrpc.HTTPRequest
	userID       string
	statsEnabled bool

	ctx context.Context

	enqueue  chan enqueueResult
	response chan queryResultWithBody
}

type enqueueStatus int

const (
	// Sent to scheduler successfully, and frontend should wait for response now.
	waitForResponse enqueueStatus = iota

	// Failed to forward request to scheduler, frontend will try again.
	failed
)

type enqueueResult struct {
	status enqueueStatus
	// If the status is failed and if it was because of a client error on the frontend,
	// the clientErr should be updated with the appropriate error.
	clientErr error

	cancelCh chan<- uint64 // Channel that can be used for request cancellation. If nil, cancellation is not possible.
}

// NewFrontend creates a new frontend.
func NewFrontend(cfg Config, limits Limits, log log.Logger, reg prometheus.Registerer, codec querymiddleware.Codec, cluster string) (*Frontend, error) {
	requestsCh := make(chan *frontendRequest)
	toSchedulerAdapter := frontendToSchedulerAdapter{
		log:    log,
		cfg:    cfg,
		limits: limits,
		codec:  codec,
	}

	schedulerWorkers, err := newFrontendSchedulerWorkers(cfg, net.JoinHostPort(cfg.Addr, strconv.Itoa(cfg.Port)), requestsCh, toSchedulerAdapter, log, reg, cluster)
	if err != nil {
		return nil, err
	}

	f := &Frontend{
		cfg:                     cfg,
		log:                     log,
		requestsCh:              requestsCh,
		schedulerWorkers:        schedulerWorkers,
		schedulerWorkersWatcher: services.NewFailureWatcher(),
		requests:                newRequestsInProgress(),
	}
	// Randomize to avoid getting responses from queries sent before restart, which could lead to mixing results
	// between different queries. Note that frontend verifies the user, so it cannot leak results between tenants.
	// This isn't perfect, but better than nothing.
	f.lastQueryID.Store(rand.Uint64())

	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_query_frontend_queries_in_progress",
		Help: "Number of queries in progress handled by this frontend.",
	}, func() float64 {
		return float64(f.requests.count())
	})

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

// RoundTripGRPC round trips a proto (instead of an HTTP request).
func (f *Frontend) RoundTripGRPC(ctx context.Context, req *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, io.ReadCloser, error) {
	if s := f.State(); s != services.Running {
		// This should never happen: requests should be blocked by frontendRunningRoundTripper before they get here.
		return nil, nil, fmt.Errorf("frontend not running: %v", s)
	}

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, nil, err
	}
	userID := tenant.JoinTenantIDs(tenantIDs)

	// Propagate trace context in gRPC too - this will be ignored if using HTTP.
	tracer, span := opentracing.GlobalTracer(), opentracing.SpanFromContext(ctx)
	if tracer != nil && span != nil {
		carrier := (*httpgrpcutil.HttpgrpcHeadersCarrier)(req)
		if err := tracer.Inject(span.Context(), opentracing.HTTPHeaders, carrier); err != nil {
			return nil, nil, err
		}
	}

	spanLogger := spanlogger.FromContext(ctx, f.log)
	ctx, cancel := context.WithCancelCause(ctx)
	// cancel is passed to the cleanup function and invoked from there

	freq := &frontendRequest{
		queryID:      f.lastQueryID.Inc(),
		request:      req,
		userID:       userID,
		statsEnabled: stats.IsEnabled(ctx),

		ctx: ctx,

		// Buffer of 1 to ensure response or error can be written to the channel
		// even if this goroutine goes away due to client context cancellation.
		enqueue:  make(chan enqueueResult, 1),
		response: make(chan queryResultWithBody, 1),
	}

	f.requests.put(freq)
	// delete is called through the cleanup func executed either in the defer or by the caller closing the body.

	cleanup := func() {
		f.requests.delete(freq.queryID)
		cancel(errExecutingQueryRoundTripFinished)
	}
	cleanupInDefer := true
	defer func() {
		if cleanupInDefer {
			cleanup()
		}
	}()

	retries := f.cfg.WorkerConcurrency + 1 // To make sure we hit at least two different schedulers.

enqueueAgain:
	spanLogger.DebugLog("msg", "enqueuing request")

	var cancelCh chan<- uint64
	select {
	case <-ctx.Done():
		spanLogger.DebugLog("msg", "request context cancelled while enqueuing request, aborting", "err", ctx.Err())
		return nil, nil, ctx.Err()

	case f.requestsCh <- freq:
		// Enqueued, let's wait for response.
		enqRes := <-freq.enqueue
		if enqRes.status == waitForResponse {
			cancelCh = enqRes.cancelCh
			break // go wait for response.
		} else if enqRes.status == failed {
			if enqRes.clientErr != nil {
				// It failed because of a client error. No need to retry.
				return nil, nil, httpgrpc.Errorf(http.StatusBadRequest, "failed to enqueue request: %s", enqRes.clientErr.Error())
			}

			retries--
			if retries > 0 {
				spanLogger.DebugLog("msg", "enqueuing request failed, will retry")
				goto enqueueAgain
			}
		}

		spanLogger.DebugLog("msg", "enqueuing request failed, retries are exhausted, aborting")

		return nil, nil, httpgrpc.Errorf(http.StatusInternalServerError, "failed to enqueue request")
	}

	spanLogger.DebugLog("msg", "request enqueued successfully, waiting for response")

	select {
	case <-ctx.Done():
		spanLogger.DebugLog("msg", "request context cancelled after enqueuing request, aborting", "err", ctx.Err())

		if cancelCh != nil {
			select {
			case cancelCh <- freq.queryID:
				// cancellation sent.
			default:
				// failed to cancel, ignore.
				level.Warn(spanLogger).Log("msg", "failed to send cancellation request to scheduler, queue full")
			}
		}
		return nil, nil, ctx.Err()

	case resp := <-freq.response:
		spanLogger.DebugLog("msg", "received response")

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

	req := f.requests.get(qrReq.QueryID)
	// It is possible that some old response belonging to different user was received, if frontend has restarted.
	// To avoid leaking query results between users, we verify the user here.
	// To avoid mixing results from different queries, we randomize queryID counter on start.
	if req != nil && req.userID == userID {
		select {
		case req.response <- queryResultWithBody{
			queryResult: qrReq,
		}:
			// Should always be possible, unless QueryResult is called multiple times with the same queryID.
		default:
			level.Warn(f.log).Log("msg", "failed to write query result to the response channel", "queryID", qrReq.QueryID, "user", userID)
		}
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

	reader, writer := io.Pipe()
	defer func(c *io.PipeWriter) {
		if err := c.CloseWithError(err); err != nil {
			level.Warn(f.log).Log("msg", "failed to close query result body writer", "err", err)
		}
	}(writer)

	metadataReceived := false

	for {
		var resp *frontendv2pb.QueryResultStreamRequest
		resp, err = stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, context.Canceled) {
				if cause := context.Cause(stream.Context()); cause != nil {
					return fmt.Errorf("aborted streaming on canceled context: %w", cause)
				}
			}
			return fmt.Errorf("failed to receive query result stream message: %w", err)
		}
		switch d := resp.Data.(type) {
		case *frontendv2pb.QueryResultStreamRequest_Metadata:
			if metadataReceived {
				return fmt.Errorf("metadata for query ID %d received more than once", resp.QueryID)
			}
			req := f.requests.get(resp.QueryID)
			if req == nil {
				return fmt.Errorf("query %d not found", resp.QueryID)
			}
			if req.userID != userID {
				return fmt.Errorf("expected metadata for user: %s, got: %s", req.userID, userID)
			}
			res := queryResultWithBody{
				queryResult: &frontendv2pb.QueryResultRequest{
					QueryID: resp.QueryID,
					Stats:   d.Metadata.Stats,
					HttpResponse: &httpgrpc.HTTPResponse{
						Code:    d.Metadata.Code,
						Headers: d.Metadata.Headers,
					},
				},
				bodyStream: reader,
			}
			select {
			case req.response <- res: // Should always be possible unless QueryResultStream is called multiple times with the same queryID.
				metadataReceived = true
			default:
				level.Warn(f.log).Log("msg", "failed to write query result to the response channel",
					"queryID", resp.QueryID, "user", req.userID)
			}
		case *frontendv2pb.QueryResultStreamRequest_Body:
			if !metadataReceived {
				return fmt.Errorf("result body for query ID %d received before metadata", resp.QueryID)
			}
			_, err = writer.Write(d.Body.Chunk)
			if err != nil {
				return fmt.Errorf("failed to write query result body chunk: %w", err)
			}
		default:
			return fmt.Errorf("unknown query result stream message type: %T", resp.Data)
		}
	}

	return nil
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

func (r *requestsInProgress) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.requests)
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

func (r *requestsInProgress) get(queryID uint64) *frontendRequest {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.requests[queryID]
}
