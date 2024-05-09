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

type SchedulerRequest struct {
	FrontendAddress           string
	UserID                    string
	QueryID                   uint64
	Request                   *httpgrpc.HTTPRequest
	StatsEnabled              bool
	AdditionalQueueDimensions []string

	EnqueueTime time.Time

	Ctx        context.Context
	CancelFunc context.CancelCauseFunc
	QueueSpan  opentracing.Span

	// This is only used for testing.
	ParentSpanContext opentracing.SpanContext
}

// TenantIndex is opaque type that allows to resume iteration over tenants
// between successive calls of RequestQueue.GetNextRequestForQuerier method.
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

// RequestQueue holds incoming requests in per-tenant queues. It also assigns each tenant specified number of queriers,
// and when querier asks for next request to handle (using GetNextRequestForQuerier), it returns requests
// in a fair fashion.
type RequestQueue struct {
	services.Service
	log log.Logger

	// settings
	maxOutstandingPerTenant          int
	additionalQueueDimensionsEnabled bool
	forgetDelay                      time.Duration

	// metrics for reporting
	connectedQuerierWorkers *atomic.Int32
	// metrics are broken out with "user" label for backwards compat, despite update to "tenant" terminology
	queueLength       *prometheus.GaugeVec   // per user
	discardedRequests *prometheus.CounterVec // per user
	enqueueDuration   prometheus.Histogram

	stopRequested chan struct{} // Written to by stop() to wake up dispatcherLoop() in response to a stop request.
	stopCompleted chan struct{} // Closed by dispatcherLoop() after a stop is requested and the dispatcher has stopped.

	querierOperations chan querierOperation
	requestsToEnqueue chan requestToEnqueue
	queueBroker       *queueBroker

	nextRequestForQuerierCalls chan *nextRequestForQuerierCall
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
	forgetDelay time.Duration,
	queueLength *prometheus.GaugeVec,
	discardedRequests *prometheus.CounterVec,
	enqueueDuration prometheus.Histogram,
) *RequestQueue {
	q := &RequestQueue{
		// settings
		log:                              log,
		maxOutstandingPerTenant:          maxOutstandingPerTenant,
		additionalQueueDimensionsEnabled: additionalQueueDimensionsEnabled,
		forgetDelay:                      forgetDelay,

		// metrics for reporting
		connectedQuerierWorkers: atomic.NewInt32(0),
		queueLength:             queueLength,
		discardedRequests:       discardedRequests,
		enqueueDuration:         enqueueDuration,

		stopRequested: make(chan struct{}),
		stopCompleted: make(chan struct{}),

		// channels must not be buffered so that we can detect when dispatcherLoop() has finished.
		querierOperations: make(chan querierOperation),
		requestsToEnqueue: make(chan requestToEnqueue),
		queueBroker:       newQueueBroker(maxOutstandingPerTenant, additionalQueueDimensionsEnabled, forgetDelay),

		nextRequestForQuerierCalls: make(chan *nextRequestForQuerierCall),
	}

	q.Service = services.NewTimerService(forgetCheckPeriod, q.starting, q.forgetDisconnectedQueriers, q.stop).WithName("request queue")

	return q
}

func (q *RequestQueue) starting(_ context.Context) error {

	go q.dispatcherLoop()

	return nil
}

func (q *RequestQueue) dispatcherLoop() {
	stopping := false

	waitingGetNextRequestForQuerierCalls := list.New()

	for {
		needToDispatchQueries := false

		select {
		case <-q.stopRequested:
			// Nothing much to do here - fall through to the stop logic below to see if we can stop immediately.
			stopping = true
		case querierOp := <-q.querierOperations:
			// Need to attempt to dispatch queries only if querier operation results in a resharding
			needToDispatchQueries = q.processQuerierOperation(querierOp)
		case r := <-q.requestsToEnqueue:
			err := q.enqueueRequestToBroker(r)
			r.errChan <- err
			if err == nil {
				needToDispatchQueries = true
			}
		case call := <-q.nextRequestForQuerierCalls:
			requestSent := q.trySendNextRequestForQuerier(call)
			if !requestSent {
				// No requests available for this querier; add it to the list to try later.
				waitingGetNextRequestForQuerierCalls.PushBack(call)
			}
		}

		if needToDispatchQueries {
			currentElement := waitingGetNextRequestForQuerierCalls.Front()

			for currentElement != nil && !q.queueBroker.isEmpty() {
				call := currentElement.Value.(*nextRequestForQuerierCall)
				nextElement := currentElement.Next() // We have to capture the next element before calling Remove(), as Remove() clears it.

				if q.trySendNextRequestForQuerier(call) {
					waitingGetNextRequestForQuerierCalls.Remove(currentElement)
				}

				currentElement = nextElement
			}
		}

		if stopping && (q.queueBroker.isEmpty() || q.connectedQuerierWorkers.Load() == 0) {
			// Tell any waiting GetNextRequestForQuerier calls that nothing is coming.
			currentElement := waitingGetNextRequestForQuerierCalls.Front()

			for currentElement != nil {
				call := currentElement.Value.(*nextRequestForQuerierCall)
				call.sendError(ErrStopped)
				currentElement = currentElement.Next()
			}

			if !q.queueBroker.isEmpty() {
				// This should never happen: unless all queriers have shut down themselves, they should remain connected
				// until the RequestQueue service enters the stopped state (see Scheduler.QuerierLoop()), and so we won't
				// stop the RequestQueue until we've drained all enqueued queries.
				// But if this does happen, we want to know about it.
				level.Warn(q.log).Log("msg", "shutting down dispatcher loop: have no connected querier workers, but request queue is not empty, so these requests will be abandoned")
			}

			// We are done.
			close(q.stopCompleted)
			return
		}
	}
}

// enqueueRequestToBroker handles a request from the dispatcher's queue and submits it to the scheduler's queue broker.
//
// The scheduler's queue broker manages the relationship between queriers and tenant query queues,
// enforcing queueing fairness and limits on tenant query queue depth.
//
// If request is successfully enqueued, successFn is called before any querier can receive the request.
func (q *RequestQueue) enqueueRequestToBroker(r requestToEnqueue) error {
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
	q.queueLength.WithLabelValues(string(r.tenantID)).Inc()

	// Call the successFn here to ensure we call it before sending this request to a waiting querier.
	if r.successFn != nil {
		r.successFn()
	}

	return nil
}

// trySendNextRequestForQuerier finds and forwards a request to a waiting GetNextRequestForQuerier call.
//
// Returns true if a nextRequestForQuerier was written to the nextRequestForQuerierCall's result channel.
// The nextRequestForQuerier result can contain either:
// a) a query request which was successfully dequeued for the querier, or
// b) an ErrShuttingDown indicating the querier has been placed in a graceful shutdown state.
// Returns false if no message was sent to the querier, meaning neither of the above cases occurred.
//
// Sending the request to the call's result channel will block until the result is read or the call is canceled.
// The call can be discarded once it has received a result, otherwise it can remain in the list of waiting calls.
func (q *RequestQueue) trySendNextRequestForQuerier(call *nextRequestForQuerierCall) (sent bool) {
	req, tenant, idx, err := q.queueBroker.dequeueRequestForQuerier(call.lastTenantIndex.last, call.querierID)
	if err != nil {
		// If this querier has told us it's shutting down, terminate GetNextRequestForQuerier with an error now...
		call.sendError(err)
		// ...and remove the waiting GetNextRequestForQuerier call from our list.
		return true
	}

	call.lastTenantIndex.last = idx
	if req == nil {
		// Nothing available for this querier, try again next time.
		return false
	}

	reqForQuerier := nextRequestForQuerier{
		req:             req.req,
		lastTenantIndex: call.lastTenantIndex,
		err:             nil,
	}

	requestSent := call.send(reqForQuerier)
	if requestSent {
		q.queueLength.WithLabelValues(string(tenant.tenantID)).Dec()
	} else {
		// should never error; any item previously in the queue already passed validation
		err := q.queueBroker.enqueueRequestFront(req, tenant.maxQueriers)
		if err != nil {
			level.Error(q.log).Log(
				"msg", "failed to re-enqueue query request after dequeue",
				"err", err, "tenant", tenant.tenantID, "querier", call.querierID,
			)
		}
	}
	return true
}

// EnqueueRequestToDispatcher handles a request from the query frontend and submits it to the initial dispatcher queue
//
// maxQueries is tenant-specific value to compute which queriers should handle requests for this tenant.
// It is passed to each EnqueueRequestToDispatcher, because it can change between calls.
//
// If request is successfully enqueued, successFn is called before any querier can receive the request.
func (q *RequestQueue) EnqueueRequestToDispatcher(tenantID string, req Request, maxQueriers int, successFn func()) error {
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

// GetNextRequestForQuerier finds next tenant queue and takes the next request off of it. Will block if there are no requests.
// By passing tenant index from previous call of this method, querier guarantees that it iterates over all tenants fairly.
// If querier finds that request from the tenant is already expired, it can get a request for the same tenant by using TenantIndex.ReuseLastTenant.
func (q *RequestQueue) GetNextRequestForQuerier(ctx context.Context, last TenantIndex, querierID string) (Request, TenantIndex, error) {
	call := &nextRequestForQuerierCall{
		ctx:             ctx,
		querierID:       QuerierID(querierID),
		lastTenantIndex: last,
		resultChan:      make(chan nextRequestForQuerier),
	}

	select {
	case q.nextRequestForQuerierCalls <- call:
		// The dispatcher now knows we're waiting. Either we'll get a request to send to a querier, or we'll cancel.
		select {
		case result := <-call.resultChan:
			return result.req, result.lastTenantIndex, result.err
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

func (q *RequestQueue) forgetDisconnectedQueriers(_ context.Context) error {
	q.submitQuerierOperation("", forgetDisconnected)

	return nil
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
		// All subsequent nextRequestForQuerierCalls for the querier will receive an ErrShuttingDown.
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

// nextRequestForQuerierCall is a "request" indicating that the querier is ready to receive the next query request.
// It embeds an unbuffered result channel for the nextRequestForQuerier "response" to be written to.
// "Request" and "Response" terminology is avoided in the naming here to avoid confusion with the actual query requests.
type nextRequestForQuerierCall struct {
	ctx             context.Context
	querierID       QuerierID
	lastTenantIndex TenantIndex
	resultChan      chan nextRequestForQuerier
}

// nextRequestForQuerier is the "response" to a nextRequestForQuerierCall, to be written to the call's result channel.
// Errors are embedded in this response rather than written to a separate error channel
// so that lastTenantIndex can still be returned back to the querier connection.
type nextRequestForQuerier struct {
	req             Request
	lastTenantIndex TenantIndex
	err             error
}

func (q *nextRequestForQuerierCall) sendError(err error) {
	// If GetNextRequestForQuerier is already gone, we don't care, so ignore the result from send.
	_ = q.send(nextRequestForQuerier{err: err})
}

// send sends req to the nextRequestForQuerierCall result channel that is waiting for a new query.
// Returns true if sending succeeds, or false if req context is timed out or canceled.
func (q *nextRequestForQuerierCall) send(req nextRequestForQuerier) bool {
	defer close(q.resultChan)

	select {
	case q.resultChan <- req:
		return true
	case <-q.ctx.Done():
		return false
	}
}
