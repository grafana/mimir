// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/queue.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queue

import (
	"container/list"
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/services"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
)

const (
	// How frequently to check for disconnected queriers that should be forgotten.
	forgetCheckPeriod = 5 * time.Second
)

var (
	ErrInvalidTenantID     = errors.New("invalid tenant id")
	ErrTooManyRequests     = errors.New("too many outstanding requests")
	ErrStopped             = errors.New("queue is stopped")
	ErrQuerierShuttingDown = errors.New("querier has informed the scheduler it is shutting down")
)

type RequestKey struct {
	frontendAddr string
	queryID      uint64
}

func NewSchedulerRequestKey(frontendAddr string, queryID uint64) RequestKey {
	return RequestKey{
		frontendAddr: frontendAddr,
		queryID:      queryID,
	}
}

type SchedulerRequest struct {
	FrontendAddr              string
	UserID                    string
	QueryID                   uint64
	Request                   *httpgrpc.HTTPRequest
	StatsEnabled              bool
	AdditionalQueueDimensions []string

	EnqueueTime time.Time

	Ctx        context.Context
	CancelFunc context.CancelCauseFunc
	QueueSpan  opentracing.Span

	ParentSpanContext opentracing.SpanContext
}

func (sr *SchedulerRequest) Key() RequestKey {
	return RequestKey{
		frontendAddr: sr.FrontendAddr,
		queryID:      sr.QueryID,
	}
}

// ExpectedQueryComponentName parses the expected query component from annotations by the frontend.
func (sr *SchedulerRequest) ExpectedQueryComponentName() string {
	if len(sr.AdditionalQueueDimensions) > 0 {
		return sr.AdditionalQueueDimensions[0]
	}
	return ""
}

// TenantIndex is opaque type that allows to resume iteration over tenants
// between successive calls of RequestQueue.WaitForRequestForQuerier method.
type TenantIndex struct {
	last int
}

// ReuseLastTenant modifies index to start iteration on the same tenant, for which last queue was returned.
func (ui TenantIndex) ReuseLastTenant() TenantIndex {
	if ui.last >= 0 {
		return TenantIndex{last: ui.last - 1}
	}
	return ui
}

// FirstTenant returns TenantIndex that starts iteration over tenant queues from the very first tenant.
func FirstTenant() TenantIndex {
	return TenantIndex{last: -1}
}

// Request stored into the queue.
type Request interface{}

// RequestQueue holds incoming requests in queues, split by multiple dimensions based on properties of the request.
// Dequeuing selects the next request from an appropriate queue given the state of the system.
// Two separate system states are managed by the RequestQueue and used to select the next request:
//  1. TenantQuerierAssignments
//     Tenants with shuffle-sharding enabled by setting maxQueriers > 0 are assigned a subset of queriers.
//     The RequestQueue receives waitingQuerierConn messages with QuerierIDs
//     in order to dequeue requests from a tenant assigned to that querier.
//  2. QueryComponentUtilization
//     Requests sent to queriers are tracked per query component until the requests are completed or failed.
//     The RequestQueue will dequeue requests such that one query component does not utilize
//     all querier-worker connections while requests for the other query component are waiting.
//
// If no specific behavior is required by TenantQuerierAssignments and QueryComponentUtilization,
// such as when shuffle-sharding is disabled or query component utilization is not a concern,
// requests are dequeued in a fair round-robin fashion across all tenants and query components.
type RequestQueue struct {
	services.Service
	log log.Logger

	// settings
	maxOutstandingPerTenant          int
	additionalQueueDimensionsEnabled bool
	useMultiAlgoQueue                bool
	forgetDelay                      time.Duration

	// metrics for reporting
	connectedQuerierWorkers *atomic.Int64
	// metrics are broken out with "user" label for backwards compat, despite update to "tenant" terminology
	queueLength       *prometheus.GaugeVec   // per user
	discardedRequests *prometheus.CounterVec // per user
	enqueueDuration   prometheus.Histogram

	stopRequested chan struct{} // Written to by stop() to wake up dispatcherLoop() in response to a stop request.
	stopCompleted chan struct{} // Closed by dispatcherLoop() after a stop is requested and the dispatcher has stopped.

	requestsToEnqueue             chan requestToEnqueue
	requestsSent                  chan *SchedulerRequest
	requestsCompleted             chan *SchedulerRequest
	querierOperations             chan querierOperation
	waitingQuerierConns           chan *waitingQuerierConn
	waitingQuerierConnsToDispatch *list.List

	// QueryComponentUtilization encapsulates tracking requests from the time they are forwarded to a querier
	// to the time are completed by the querier or failed due to cancel, timeout, or disconnect.
	// Unlike schedulerInflightRequests, tracking begins only when the request is sent to a querier.
	QueryComponentUtilization *QueryComponentUtilization

	queueBroker *queueBroker
}

type querierOperation struct {
	querierID QuerierID
	operation querierOperationType
}

type querierOperationType int

const (
	registerConnection querierOperationType = iota
	unregisterConnection
	notifyShutdown
	forgetDisconnected
)

type requestToEnqueue struct {
	tenantID    TenantID
	req         Request
	maxQueriers int
	successFn   func()
	errChan     chan error
}

func NewRequestQueue(
	log log.Logger,
	maxOutstandingPerTenant int,
	additionalQueueDimensionsEnabled bool,
	useMultiAlgoQueue bool,
	forgetDelay time.Duration,
	queueLength *prometheus.GaugeVec,
	discardedRequests *prometheus.CounterVec,
	enqueueDuration prometheus.Histogram,
	querierInflightRequestsMetric *prometheus.SummaryVec,
) (*RequestQueue, error) {
	queryComponentCapacity, err := NewQueryComponentUtilization(DefaultReservedQueryComponentCapacity, querierInflightRequestsMetric)
	if err != nil {
		return nil, err
	}

	q := &RequestQueue{
		// settings
		log:                              log,
		maxOutstandingPerTenant:          maxOutstandingPerTenant,
		additionalQueueDimensionsEnabled: additionalQueueDimensionsEnabled,
		forgetDelay:                      forgetDelay,

		// metrics for reporting
		connectedQuerierWorkers: atomic.NewInt64(0),
		queueLength:             queueLength,
		discardedRequests:       discardedRequests,
		enqueueDuration:         enqueueDuration,

		// channels must not be buffered so that we can detect when dispatcherLoop() has finished.
		stopRequested: make(chan struct{}),
		stopCompleted: make(chan struct{}),

		requestsToEnqueue:             make(chan requestToEnqueue),
		requestsSent:                  make(chan *SchedulerRequest),
		requestsCompleted:             make(chan *SchedulerRequest),
		querierOperations:             make(chan querierOperation),
		waitingQuerierConns:           make(chan *waitingQuerierConn),
		waitingQuerierConnsToDispatch: list.New(),

		QueryComponentUtilization: queryComponentCapacity,
		queueBroker:               newQueueBroker(maxOutstandingPerTenant, additionalQueueDimensionsEnabled, useMultiAlgoQueue, forgetDelay),
	}

	q.Service = services.NewBasicService(q.starting, q.running, q.stop).WithName("request queue")

	return q, nil
}

func (q *RequestQueue) starting(_ context.Context) error {

	go q.dispatcherLoop()

	return nil
}

func (q *RequestQueue) running(ctx context.Context) error {
	// periodically submit a message to dispatcherLoop to forget disconnected queriers
	forgetDisconnectedQueriersTicker := time.NewTicker(forgetCheckPeriod)
	defer forgetDisconnectedQueriersTicker.Stop()

	// periodically submit a message to dispatcherLoop to observe inflight requests;
	// same as scheduler, we observe inflight requests frequently and at regular intervals
	// to have a good approximation of max inflight requests over percentiles of time.
	inflightRequestsTicker := time.NewTicker(250 * time.Millisecond)
	defer inflightRequestsTicker.Stop()

	for {
		select {
		case <-forgetDisconnectedQueriersTicker.C:
			q.submitForgetDisconnectedQueriers(ctx)
		case <-inflightRequestsTicker.C:
			q.QueryComponentUtilization.ObserveInflightRequests()
		case <-ctx.Done():
			return nil
		}
	}
}

func (q *RequestQueue) dispatcherLoop() {
	stopping := false

	for {
		needToDispatchQueries := false

		select {
		case <-q.stopRequested:
			// Nothing much to do here - fall through to the stop logic below to see if we can stop immediately.
			stopping = true
		case querierOp := <-q.querierOperations:
			// Need to attempt to dispatch queries only if querier operation results in a resharding
			needToDispatchQueries = q.processQuerierOperation(querierOp)
		case reqToEnqueue := <-q.requestsToEnqueue:
			err := q.enqueueRequestInternal(reqToEnqueue)
			reqToEnqueue.errChan <- err
			if err == nil {
				needToDispatchQueries = true
			}
		case waitingConn := <-q.waitingQuerierConns:
			requestSent := q.trySendNextRequestForQuerier(waitingConn)
			if !requestSent {
				// No requests available for this querier; add it to the list to try later.
				q.waitingQuerierConnsToDispatch.PushBack(waitingConn)
			}
		}

		if needToDispatchQueries {
			currentElement := q.waitingQuerierConnsToDispatch.Front()

			for currentElement != nil && !q.queueBroker.isEmpty() {
				call := currentElement.Value.(*waitingQuerierConn)
				nextElement := currentElement.Next() // We have to capture the next element before calling Remove(), as Remove() clears it.

				if q.trySendNextRequestForQuerier(call) {
					q.waitingQuerierConnsToDispatch.Remove(currentElement)
				}

				currentElement = nextElement
			}
		}

		// if we have received a signal to stop, we continue to dispatch queries until
		// the queue is empty or until we have no more connected querier workers.
		if stopping && (q.queueBroker.isEmpty() || q.connectedQuerierWorkers.Load() == 0) {
			// tell any waiting querier connections that nothing is coming
			currentElement := q.waitingQuerierConnsToDispatch.Front()

			for currentElement != nil {
				waitingConn := currentElement.Value.(*waitingQuerierConn)
				waitingConn.sendError(ErrStopped)
				currentElement = currentElement.Next()
			}

			if !q.queueBroker.isEmpty() {
				// All queriers have disconnected, but we still have requests in the queue.
				// Without any consumers we have nothing to do but stop the RequestQueue.
				// This should never happen, but if this does happen, we want to know about it.
				level.Warn(q.log).Log("msg", "shutting down dispatcher loop: have no connected querier workers, but request queue is not empty, so these requests will be abandoned")
			}

			// We are done.
			close(q.stopCompleted)
			return
		}
	}
}

// enqueueRequestInternal processes a request into the RequestQueue's internal queue structure.
//
// If request is enqueued successFn is called before the request can be dispatched to a querier.
func (q *RequestQueue) enqueueRequestInternal(r requestToEnqueue) error {
	tr := tenantRequest{
		tenantID: r.tenantID,
		req:      r.req,
	}
	err := q.queueBroker.enqueueRequestBack(&tr, r.maxQueriers)
	if err != nil {
		if errors.Is(err, ErrTooManyRequests) {
			q.discardedRequests.WithLabelValues(string(r.tenantID)).Inc()
		}
		return err
	}
	if r.successFn != nil {
		r.successFn()
	}

	q.queueLength.WithLabelValues(string(r.tenantID)).Inc()
	return nil
}

// trySendNextRequestForQuerier attempts to dequeue and send a request for a waiting querier-worker connection.
//
// Returns true if the waitingQuerierConn can be removed from the list of waiting connections,
// meaning a requestForQuerier was sent through the waitingQuerierConn's receiving channel
// or the waiting querier-worker connection's context was canceled.
//
// The requestForQuerier message can contain either:
// a) a query request which was successfully dequeued for the querier, or
// b) an ErrShuttingDown indicating the querier has been placed in a graceful shutdown state.
func (q *RequestQueue) trySendNextRequestForQuerier(waitingConn *waitingQuerierConn) (done bool) {
	req, tenant, idx, err := q.queueBroker.dequeueRequestForQuerier(waitingConn.lastTenantIndex.last, waitingConn.querierID)
	if err != nil {
		// If this querier has told us it's shutting down, terminate WaitForRequestForQuerier with an error now...
		waitingConn.sendError(err)
		// ...and remove the waiting WaitForRequestForQuerier waitingConn from our list.
		return true
	}

	waitingConn.lastTenantIndex.last = idx
	if req == nil {
		// Nothing available for this querier, try again next time.
		return false
	}

	{
		// temporary observation of query component load balancing behavior before full implementation
		schedulerRequest, ok := req.req.(*SchedulerRequest)
		if ok {
			queryComponentName := schedulerRequest.ExpectedQueryComponentName()
			exceedsThreshold, queryComponent := q.QueryComponentUtilization.ExceedsThresholdForComponentName(
				queryComponentName,
				int(q.connectedQuerierWorkers.Load()),
				q.queueBroker.tree.ItemCount(),
				q.waitingQuerierConnsToDispatch.Len(),
			)

			if exceedsThreshold {
				level.Info(q.log).Log(
					"msg", "experimental: querier worker connections in use by query component exceed utilization threshold. no action taken",
					"query_component_name", queryComponentName,
					"overloaded_query_component", queryComponent,
				)
			}
		}
	}

	reqForQuerier := requestForQuerier{
		req:             req.req,
		lastTenantIndex: waitingConn.lastTenantIndex,
		err:             nil,
	}

	requestSent := waitingConn.send(reqForQuerier)
	if requestSent {
		q.queueLength.WithLabelValues(string(tenant.tenantID)).Dec()
	} else {
		// should never error; any item previously in the queue already passed validation
		err := q.queueBroker.enqueueRequestFront(req, tenant.maxQueriers)
		if err != nil {
			level.Error(q.log).Log(
				"msg", "failed to re-enqueue query request after dequeue",
				"err", err, "tenant", tenant.tenantID, "querier", waitingConn.querierID,
			)
		}
	}
	return true
}

// SubmitRequestToEnqueue handles a query request from the query frontend or scheduler and submits it to the queue.
// This method will block until the queue's processing loop has enqueued the request into its internal queue structure.
//
// If request is successfully enqueued, successFn is called before any querier can receive the request.
// Returns error if any occurred during enqueuing, or if the RequestQueue service stopped before enqueuing the request.
//
// maxQueriers is tenant-specific value to compute which queriers should handle requests for this tenant.
// It is passed to SubmitRequestToEnqueue because the value can change between calls.
func (q *RequestQueue) SubmitRequestToEnqueue(tenantID string, req Request, maxQueriers int, successFn func()) error {
	start := time.Now()
	defer func() {
		q.enqueueDuration.Observe(time.Since(start).Seconds())
	}()

	r := requestToEnqueue{
		tenantID:    TenantID(tenantID),
		req:         req,
		maxQueriers: maxQueriers,
		successFn:   successFn,
		errChan:     make(chan error),
	}

	select {
	case q.requestsToEnqueue <- r:
		return <-r.errChan
	case <-q.stopCompleted:
		return ErrStopped
	}
}

// WaitForRequestForQuerier is called by a querier-worker to submit a waitingQuerierConn message to the RequestQueue.
//
// This method blocks until the waitingQuerierConn gets a requestForQuerier message on its receiving channel,
// the querier-worker connection context is canceled, or the RequestQueue service stops.
//
// Querier-workers should pass the last TenantIndex received from their previous call to WaitForRequestForQuerier,
// which enables the RequestQueue to iterate fairly across all tenants assigned to a querier.
// If a querier-worker finds that the query request received for the tenant is already expired,
// it can get another request for the same tenant by using TenantIndex.ReuseLastTenant.
// Newly-connected querier-workers should pass FirstTenant as the TenantIndex to start iteration from the beginning.
func (q *RequestQueue) WaitForRequestForQuerier(ctx context.Context, last TenantIndex, querierID string) (Request, TenantIndex, error) {
	waitingConn := &waitingQuerierConn{
		querierConnCtx:  ctx,
		querierID:       QuerierID(querierID),
		lastTenantIndex: last,
		recvChan:        make(chan requestForQuerier),
	}

	select {
	case q.waitingQuerierConns <- waitingConn:
		select {
		case reqForQuerier := <-waitingConn.recvChan:
			return reqForQuerier.req, reqForQuerier.lastTenantIndex, reqForQuerier.err
		case <-ctx.Done():
			return nil, last, ctx.Err()
		}
	case <-ctx.Done():
		return nil, last, ctx.Err()
	case <-q.stopCompleted:
		return nil, last, ErrStopped
	}
}

func (q *RequestQueue) stop(_ error) error {
	// Do not close the stopRequested channel;
	// this would cause the read from stopRequested to preempt all other select cases in dispatcherLoop.
	// Reads from stopRequested tell dispatcherLoop to enter a stopping state where it tries to clear the queue.
	// The loop needs to keep executing other select branches while stopping in order to clear the queue.
	q.stopRequested <- struct{}{}
	<-q.stopCompleted

	return nil
}

func (q *RequestQueue) GetConnectedQuerierWorkersMetric() float64 {
	return float64(q.connectedQuerierWorkers.Load())
}

func (q *RequestQueue) submitForgetDisconnectedQueriers(_ context.Context) {
	q.submitQuerierOperation("", forgetDisconnected)
}

func (q *RequestQueue) SubmitRegisterQuerierConnection(querierID string) {
	q.submitQuerierOperation(querierID, registerConnection)
}

func (q *RequestQueue) SubmitUnregisterQuerierConnection(querierID string) {
	q.submitQuerierOperation(querierID, unregisterConnection)
}

func (q *RequestQueue) SubmitNotifyQuerierShutdown(querierID string) {
	q.submitQuerierOperation(querierID, notifyShutdown)
}

func (q *RequestQueue) submitQuerierOperation(querierID string, operation querierOperationType) {
	op := querierOperation{
		querierID: QuerierID(querierID),
		operation: operation,
	}

	select {
	case q.querierOperations <- op:
		// The dispatcher has received the operation. There's nothing more to do.
	case <-q.stopCompleted:
		// The dispatcher stopped before it could process the operation. There's nothing more to do.
	}
}

func (q *RequestQueue) processQuerierOperation(querierOp querierOperation) (resharded bool) {
	switch querierOp.operation {
	case registerConnection:
		return q.processRegisterQuerierConnection(querierOp.querierID)
	case unregisterConnection:
		return q.processUnregisterQuerierConnection(querierOp.querierID)
	case notifyShutdown:
		// No cleanup needed here in response to a graceful shutdown; just set querier state to shutting down.
		// All subsequent waitingQuerierConns for the querier will receive an ErrShuttingDown.
		// The querier-worker's end of the QuerierLoop will exit once it has received enough errors,
		// and the Querier connection counts will be decremented as the workers disconnect.
		return q.queueBroker.notifyQuerierShutdown(querierOp.querierID)
	case forgetDisconnected:
		return q.processForgetDisconnectedQueriers()
	default:
		panic(fmt.Sprintf("received unknown querier event %v for querier ID %v", querierOp.operation, querierOp.querierID))
	}
}

func (q *RequestQueue) processRegisterQuerierConnection(querierID QuerierID) (resharded bool) {
	q.connectedQuerierWorkers.Inc()
	return q.queueBroker.addQuerierConnection(querierID)
}

func (q *RequestQueue) processUnregisterQuerierConnection(querierID QuerierID) (resharded bool) {
	q.connectedQuerierWorkers.Dec()
	return q.queueBroker.removeQuerierConnection(querierID, time.Now())
}

func (q *RequestQueue) processForgetDisconnectedQueriers() (resharded bool) {
	return q.queueBroker.forgetDisconnectedQueriers(time.Now())
}

// waitingQuerierConn is a "request" indicating that the querier is ready to receive the next query request.
// It embeds the unbuffered `recvChan` to receive the requestForQuerier "response" from the RequestQueue.
// The request/response terminology is avoided in naming to disambiguate with the actual query requests.
type waitingQuerierConn struct {
	querierConnCtx  context.Context
	querierID       QuerierID
	lastTenantIndex TenantIndex
	recvChan        chan requestForQuerier
}

// requestForQuerier is the "response" for a waitingQuerierConn, to be written to its receiving channel.
// Errors are embedded in this response rather than written to a separate error channel
// so that lastTenantIndex can still be returned back to the querier connection.
type requestForQuerier struct {
	req             Request
	lastTenantIndex TenantIndex
	err             error
}

func (wqc *waitingQuerierConn) sendError(err error) {
	// querier or request queue may be shutting down; ignore the result from send
	// as the querier may not receive the message before the context is canceled
	_ = wqc.send(requestForQuerier{err: err})
}

// send sends req to the waitingQuerierConn result channel that is waiting for a new query.
// Returns true if sending succeeds, or false if req context is timed out or canceled.
func (wqc *waitingQuerierConn) send(req requestForQuerier) bool {
	select {
	case wqc.recvChan <- req:
		return true
	case <-wqc.querierConnCtx.Done():
		return false
	}
}
