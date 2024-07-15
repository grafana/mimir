// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/frontend/v2/frontend_scheduler_worker.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package v2

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/servicediscovery"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/scheduler/schedulerdiscovery"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	schedulerAddressLabel = "scheduler_address"
	// schedulerWorkerCancelChanCapacity should be at least as big as the number of sub-queries issued by a single query
	// per scheduler (after splitting and sharding) in order to allow all of them being canceled while scheduler worker is busy.
	schedulerWorkerCancelChanCapacity = 1000
)

var errFrontendSchedulerWorkerLoopIterationStopping = cancellation.NewErrorf("frontend scheduler worker loop iteration stopping")
var errFrontendSchedulerWorkerStopping = cancellation.NewErrorf("frontend scheduler worker stopping")

type frontendSchedulerWorkers struct {
	services.Service

	cfg             Config
	log             log.Logger
	frontendAddress string

	// Channel with requests that should be forwarded to the scheduler.
	requestsCh         <-chan *frontendRequest
	toSchedulerAdapter frontendToSchedulerAdapter

	schedulerDiscovery        services.Service
	schedulerDiscoveryWatcher *services.FailureWatcher

	mu sync.Mutex
	// Set to nil when stop is called... no more workers are created afterwards.
	workers map[string]*frontendSchedulerWorker

	enqueueDuration *prometheus.HistogramVec
}

func newFrontendSchedulerWorkers(
	cfg Config,
	frontendAddress string,
	requestsCh <-chan *frontendRequest,
	toSchedulerAdapter frontendToSchedulerAdapter,
	log log.Logger,
	reg prometheus.Registerer,
) (*frontendSchedulerWorkers, error) {
	f := &frontendSchedulerWorkers{
		cfg:                       cfg,
		log:                       log,
		frontendAddress:           frontendAddress,
		requestsCh:                requestsCh,
		toSchedulerAdapter:        toSchedulerAdapter,
		workers:                   map[string]*frontendSchedulerWorker{},
		schedulerDiscoveryWatcher: services.NewFailureWatcher(),
		enqueueDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name: "cortex_query_frontend_enqueue_duration_seconds",
			Help: "Time spent by requests waiting to join the queue or be rejected.",
			// We expect the enqueue operation to be very fast, so we're using custom buckets to
			// track 1ms latency too and removing any bucket bigger than 1s.
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
		}, []string{schedulerAddressLabel}),
	}

	var err error
	f.schedulerDiscovery, err = schedulerdiscovery.New(cfg.QuerySchedulerDiscovery, cfg.SchedulerAddress, cfg.DNSLookupPeriod, "query-frontend", f, log, reg)
	if err != nil {
		return nil, err
	}

	f.Service = services.NewBasicService(f.starting, f.running, f.stopping)
	return f, nil
}

func (f *frontendSchedulerWorkers) starting(ctx context.Context) error {
	f.schedulerDiscoveryWatcher.WatchService(f.schedulerDiscovery)

	return services.StartAndAwaitRunning(ctx, f.schedulerDiscovery)
}

func (f *frontendSchedulerWorkers) running(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-f.schedulerDiscoveryWatcher.Chan():
		return errors.Wrap(err, "query-frontend workers subservice failed")
	}
}

func (f *frontendSchedulerWorkers) stopping(_ error) error {
	err := services.StopAndAwaitTerminated(context.Background(), f.schedulerDiscovery)

	f.mu.Lock()
	defer f.mu.Unlock()

	for _, w := range f.workers {
		w.stop()
	}
	f.workers = nil

	return err
}

func (f *frontendSchedulerWorkers) InstanceAdded(instance servicediscovery.Instance) {
	// Connect only to in-use query-scheduler instances.
	if instance.InUse {
		f.addScheduler(instance.Address)
	}
}

func (f *frontendSchedulerWorkers) addScheduler(address string) {
	f.mu.Lock()
	ws := f.workers
	w := f.workers[address]

	// Already stopped or we already have worker for this address.
	if ws == nil || w != nil {
		f.mu.Unlock()
		return
	}
	f.mu.Unlock()

	level.Info(f.log).Log("msg", "adding connection to query-scheduler", "addr", address)
	conn, err := f.connectToScheduler(context.Background(), address)
	if err != nil {
		level.Error(f.log).Log("msg", "error connecting to query-scheduler", "addr", address, "err", err)
		return
	}

	// No worker for this address yet, start a new one.
	w = newFrontendSchedulerWorker(
		conn,
		address,
		f.frontendAddress,
		f.requestsCh,
		f.toSchedulerAdapter,
		f.cfg.WorkerConcurrency,
		f.enqueueDuration.WithLabelValues(address),
		f.log,
	)

	f.mu.Lock()
	defer f.mu.Unlock()

	// Can be nil if stopping has been called already.
	if f.workers == nil {
		return
	}
	// We have to recheck for presence in case we got called again while we were
	// connecting and that one finished first.
	if f.workers[address] != nil {
		return
	}
	f.workers[address] = w
	w.start()
}

func (f *frontendSchedulerWorkers) InstanceRemoved(instance servicediscovery.Instance) {
	f.removeScheduler(instance.Address)
}

func (f *frontendSchedulerWorkers) removeScheduler(address string) {
	f.mu.Lock()
	// This works fine if f.workers is nil already or the worker is missing
	// because the query-scheduler instance was not in use.
	w := f.workers[address]
	delete(f.workers, address)
	f.mu.Unlock()

	if w != nil {
		level.Info(f.log).Log("msg", "removing connection to query-scheduler", "addr", address)
		w.stop()
	}
	f.enqueueDuration.Delete(prometheus.Labels{schedulerAddressLabel: address})
}

func (f *frontendSchedulerWorkers) InstanceChanged(instance servicediscovery.Instance) {
	// Ensure the query-frontend connects to in-use query-scheduler instances and disconnect from ones no more in use.
	// The called methods correctly handle the case the query-frontend is already connected/disconnected
	// to/from the given query-scheduler instance.
	if instance.InUse {
		f.addScheduler(instance.Address)
	} else {
		f.removeScheduler(instance.Address)
	}
}

// Get number of workers.
func (f *frontendSchedulerWorkers) getWorkersCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.workers)
}

func (f *frontendSchedulerWorkers) connectToScheduler(ctx context.Context, address string) (*grpc.ClientConn, error) {
	// Because we only use single long-running method, it doesn't make sense to inject user ID, send over tracing or add metrics.
	opts, err := f.cfg.GRPCClientConfig.DialOption(nil, nil)
	if err != nil {
		return nil, err
	}

	// nolint:staticcheck // grpc.DialContext() has been deprecated; we'll address it before upgrading to gRPC 2.
	conn, err := grpc.DialContext(ctx, address, opts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Worker managing single gRPC connection to Scheduler. Each worker starts multiple goroutines for forwarding
// requests and cancellations to scheduler.
type frontendSchedulerWorker struct {
	log log.Logger

	conn          *grpc.ClientConn
	concurrency   int
	schedulerAddr string
	frontendAddr  string

	// Context and cancellation used by individual goroutines.
	ctx    context.Context
	cancel context.CancelCauseFunc
	wg     sync.WaitGroup

	// Shared between all frontend workers.
	requestsCh <-chan *frontendRequest

	toSchedulerAdapter frontendToSchedulerAdapter

	// Cancellation requests for this scheduler are received via this channel. It is passed to frontend after
	// query has been enqueued to scheduler.
	cancelCh chan uint64

	// How long it takes to enqueue a query.
	enqueueDuration prometheus.Observer
}

func newFrontendSchedulerWorker(
	conn *grpc.ClientConn,
	schedulerAddr string,
	frontendAddr string,
	requestsCh <-chan *frontendRequest,
	toSchedulerAdapter frontendToSchedulerAdapter,
	concurrency int,
	enqueueDuration prometheus.Observer,
	log log.Logger,
) *frontendSchedulerWorker {
	w := &frontendSchedulerWorker{
		log:                log,
		conn:               conn,
		concurrency:        concurrency,
		schedulerAddr:      schedulerAddr,
		frontendAddr:       frontendAddr,
		requestsCh:         requestsCh,
		toSchedulerAdapter: toSchedulerAdapter,
		cancelCh:           make(chan uint64, schedulerWorkerCancelChanCapacity),
		enqueueDuration:    enqueueDuration,
	}
	w.ctx, w.cancel = context.WithCancelCause(context.Background())

	return w
}

func (w *frontendSchedulerWorker) start() {
	client := schedulerpb.NewSchedulerForFrontendClient(w.conn)
	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.runOne(w.ctx, client)
		}()
	}
}

func (w *frontendSchedulerWorker) stop() {
	w.cancel(errFrontendSchedulerWorkerStopping)
	w.wg.Wait()
	if err := w.conn.Close(); err != nil {
		level.Error(w.log).Log("msg", "error while closing connection to scheduler", "err", err)
	}
}

func (w *frontendSchedulerWorker) runOne(ctx context.Context, client schedulerpb.SchedulerForFrontendClient) {
	// attemptLoop returns false if there was any error with forwarding requests to scheduler.
	attemptLoop := func() bool {
		ctx, cancel := context.WithCancelCause(ctx)
		defer cancel(errFrontendSchedulerWorkerLoopIterationStopping) // cancel the stream after we are done to release resources

		loop, loopErr := client.FrontendLoop(ctx)
		if loopErr != nil {
			level.Error(w.log).Log("msg", "error contacting scheduler", "err", loopErr, "addr", w.schedulerAddr)
			return false
		}

		loopErr = w.schedulerLoop(loop)
		if closeErr := util.CloseAndExhaust[*schedulerpb.SchedulerToFrontend](loop); closeErr != nil {
			level.Debug(w.log).Log("msg", "failed to close frontend loop", "err", closeErr, "addr", w.schedulerAddr)
		}

		if loopErr != nil {
			level.Error(w.log).Log("msg", "error sending requests to scheduler", "err", loopErr, "addr", w.schedulerAddr)
			return false
		}
		return true
	}

	backoffConfig := backoff.Config{
		MinBackoff: 250 * time.Millisecond,
		MaxBackoff: 2 * time.Second,
	}
	backoff := backoff.New(ctx, backoffConfig)
	for backoff.Ongoing() {
		if !attemptLoop() {
			backoff.Wait()
		} else {
			backoff.Reset()
		}
	}
}

func (w *frontendSchedulerWorker) schedulerLoop(loop schedulerpb.SchedulerForFrontend_FrontendLoopClient) error {
	if err := loop.Send(&schedulerpb.FrontendToScheduler{
		Type:            schedulerpb.INIT,
		FrontendAddress: w.frontendAddr,
	}); err != nil {
		return err
	}

	if resp, err := loop.Recv(); err != nil || resp.Status != schedulerpb.OK {
		if err != nil {
			return err
		}
		return errors.Errorf("unexpected status received for init: %v", resp.Status)
	}

	ctx := loop.Context()

	for {
		select {
		case <-ctx.Done():
			// No need to report error if our internal context is canceled. This can happen during shutdown,
			// or when scheduler is no longer resolvable. (It would be nice if this context reported "done" also when
			// connection scheduler stops the call, but that doesn't seem to be the case).
			//
			// Reporting error here would delay reopening the stream (if the worker context is not done yet).
			level.Debug(w.log).Log("msg", "stream context finished", "err", ctx.Err())
			return nil

		case req := <-w.requestsCh:
			if err := w.enqueueRequest(loop, req); err != nil {
				return err
			}

		case reqID := <-w.cancelCh:
			err := loop.Send(&schedulerpb.FrontendToScheduler{
				Type:    schedulerpb.CANCEL,
				QueryID: reqID,
			})

			if err != nil {
				return err
			}

			resp, err := loop.Recv()
			if err != nil {
				return err
			}

			// Scheduler may be shutting down, report that.
			if resp.Status != schedulerpb.OK {
				return errors.Errorf("unexpected status received for cancellation: %v", resp.Status)
			}
		}
	}
}

// enqueueRequest sends a request to this worker's scheduler, and returns an error if no further requests should be sent to the scheduler.
func (w *frontendSchedulerWorker) enqueueRequest(loop schedulerpb.SchedulerForFrontend_FrontendLoopClient, req *frontendRequest) error {
	spanLogger, _ := spanlogger.NewWithLogger(req.ctx, w.log, "frontendSchedulerWorker.enqueueRequest")
	spanLogger.Span.SetTag("scheduler_address", w.conn.Target())
	defer spanLogger.Span.Finish()

	// Keep track of how long it takes to enqueue a request end-to-end.
	durationTimer := prometheus.NewTimer(w.enqueueDuration)
	defer durationTimer.ObserveDuration()

	frontendToSchedulerRequest, err := w.toSchedulerAdapter.frontendToSchedulerEnqueueRequest(req, w.frontendAddr)
	if err != nil {
		level.Warn(spanLogger).Log("msg", "error converting frontend request to scheduler request", "err", err)
		req.enqueue <- enqueueResult{status: failed}
		return err
	}

	err = loop.Send(frontendToSchedulerRequest)
	if err != nil {
		level.Warn(spanLogger).Log("msg", "received error while sending request to scheduler", "err", err)
		req.enqueue <- enqueueResult{status: failed}
		return err
	}

	resp, err := loop.Recv()
	if err != nil {
		level.Warn(spanLogger).Log("msg", "received error while receiving response", "err", err)
		req.enqueue <- enqueueResult{status: failed}
		return err
	}

	switch resp.Status {
	case schedulerpb.OK:
		req.enqueue <- enqueueResult{status: waitForResponse, cancelCh: w.cancelCh}
		// Response will come from querier.

	case schedulerpb.SHUTTING_DOWN:
		// Scheduler is shutting down, report failure to enqueue and stop this loop.
		level.Warn(spanLogger).Log("msg", "scheduler reported that it is shutting down")
		req.enqueue <- enqueueResult{status: failed}
		return errors.New("scheduler is shutting down")

	case schedulerpb.ERROR:
		level.Warn(spanLogger).Log("msg", "scheduler returned error", "err", resp.Error)
		req.enqueue <- enqueueResult{status: waitForResponse}
		req.response <- queryResultWithBody{
			queryResult: &frontendv2pb.QueryResultRequest{
				HttpResponse: &httpgrpc.HTTPResponse{
					Code: http.StatusInternalServerError,
					Body: []byte(resp.Error),
				},
			}}

	case schedulerpb.TOO_MANY_REQUESTS_PER_TENANT:
		level.Warn(spanLogger).Log("msg", "scheduler reported it has too many outstanding requests")
		req.enqueue <- enqueueResult{status: waitForResponse}
		req.response <- queryResultWithBody{
			queryResult: &frontendv2pb.QueryResultRequest{
				HttpResponse: &httpgrpc.HTTPResponse{
					Code: http.StatusTooManyRequests,
					Body: []byte("too many outstanding requests"),
				},
			}}

	default:
		level.Error(spanLogger).Log("msg", "unknown response status from the scheduler", "resp", resp, "queryID", req.queryID)
		req.enqueue <- enqueueResult{status: failed}
	}

	return nil
}
