// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/queue.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
)

const (
	// How frequently to check for disconnected queriers that should be forgotten.
	forgetCheckPeriod = 5 * time.Second
)

var (
	ErrTooManyRequests = errors.New("too many outstanding requests")
	ErrStopped         = errors.New("queue is stopped")
)

// UserIndex is opaque type that allows to resume iteration over users between successive calls
// of RequestQueue.GetNextRequestForQuerier method.
type UserIndex struct {
	last int
}

// Modify index to start iteration on the same user, for which last queue was returned.
func (ui UserIndex) ReuseLastUser() UserIndex {
	if ui.last >= 0 {
		return UserIndex{last: ui.last - 1}
	}
	return ui
}

// FirstUser returns UserIndex that starts iteration over user queues from the very first user.
func FirstUser() UserIndex {
	return UserIndex{last: -1}
}

// Request stored into the queue.
type Request interface{}

// RequestQueue holds incoming requests in per-user queues. It also assigns each user specified number of queriers,
// and when querier asks for next request to handle (using GetNextRequestForQuerier), it returns requests
// in a fair fashion.
type RequestQueue struct {
	services.Service

	maxOutstandingPerTenant int
	forgetDelay             time.Duration

	connectedQuerierWorkers *atomic.Int32

	stopRequested     chan struct{} // Closed by stop() to wake up dispatcherLoop() in response to a stop request.
	stopCompleted     chan struct{} // Closed by dispatcherLoop() after a stop is requested and the dispatcher has stopped.
	querierOperations chan querierOperation
	enqueueRequests   chan enqueueRequest
	availableQueriers chan *availableQuerier

	queueLength       *prometheus.GaugeVec   // Per user and reason.
	discardedRequests *prometheus.CounterVec // Per user.
}

type querierOperation struct {
	querierID string
	operation querierOperationType
	processed chan struct{}
}

type querierOperationType int

const (
	registerConnection querierOperationType = iota
	unregisterConnection
	notifyShutdown
	forgetDisconnected
)

type enqueueRequest struct {
	userID      string
	req         Request
	maxQueriers int
	successFn   func()
	processed   chan error
}

type availableQuerier struct {
	ctx           context.Context
	querierID     string
	lastUserIndex UserIndex
	processed     chan nextRequestForQuerier
	next          *availableQuerier
}

type nextRequestForQuerier struct {
	req  Request
	next UserIndex
	err  error
}

func NewRequestQueue(maxOutstandingPerTenant int, forgetDelay time.Duration, queueLength *prometheus.GaugeVec, discardedRequests *prometheus.CounterVec) *RequestQueue {
	q := &RequestQueue{
		maxOutstandingPerTenant: maxOutstandingPerTenant,
		forgetDelay:             forgetDelay,
		connectedQuerierWorkers: atomic.NewInt32(0),
		queueLength:             queueLength,
		discardedRequests:       discardedRequests,
	}

	q.Service = services.NewTimerService(forgetCheckPeriod, q.starting, q.forgetDisconnectedQueriers, q.stop).WithName("request queue")

	return q
}

func (q *RequestQueue) starting(_ context.Context) error {
	q.stopRequested = make(chan struct{})
	q.stopCompleted = make(chan struct{})

	// These channels must not be buffered so that we can detect when dispatcherLoop() has finished.
	q.querierOperations = make(chan querierOperation)
	q.enqueueRequests = make(chan enqueueRequest)
	q.availableQueriers = make(chan *availableQuerier)

	go q.dispatcherLoop()

	return nil
}

func (q *RequestQueue) dispatcherLoop() {
	stopping := false
	queues := newUserQueues(q.maxOutstandingPerTenant, q.forgetDelay)

	var firstAvailableQuerier *availableQuerier
	var lastAvailableQuerier *availableQuerier

	for {
		needToDispatchQueries := false

		select {
		case <-q.stopRequested:
			// Nothing much to do here - fall through to the stop logic below to see if we can stop immediately.
			stopping = true
		case qe := <-q.querierOperations:
			switch qe.operation {
			case registerConnection:
				queues.addQuerierConnection(qe.querierID)
			case unregisterConnection:
				queues.removeQuerierConnection(qe.querierID, time.Now())
			case notifyShutdown:
				queues.notifyQuerierShutdown(qe.querierID)
			case forgetDisconnected:
				if queues.forgetDisconnectedQueriers(time.Now()) > 0 {
					// Removing some queriers may have caused a resharding.
					needToDispatchQueries = true
				}
			default:
				panic(fmt.Sprintf("received unknown querier event %v for querier ID %v", qe.operation, qe.querierID))
			}
			qe.processed <- struct{}{}
		case r := <-q.enqueueRequests:
			err := q.handleNewRequest(queues, r)
			r.processed <- err

			if err == nil {
				// TODO: might be able to be much smarter here and try to find a querier that can take the request directly
				needToDispatchQueries = true
			}
		case querier := <-q.availableQueriers:
			if !q.dispatchRequestToQuerier(queues, querier) {
				// No requests available for this querier right now. Add it to the list to try later.
				if lastAvailableQuerier == nil {
					firstAvailableQuerier = querier
					lastAvailableQuerier = querier
				} else {
					lastAvailableQuerier.next = querier
					lastAvailableQuerier = querier
				}
			}
		}

		if needToDispatchQueries {
			var previousQuerier *availableQuerier
			currentQuerier := firstAvailableQuerier

			for currentQuerier != nil && queues.len() > 0 {
				if q.dispatchRequestToQuerier(queues, currentQuerier) {
					// Remove this querier from our list.

					if previousQuerier == nil {
						// This was the first available querier.
						firstAvailableQuerier = currentQuerier.next
					} else {
						previousQuerier.next = currentQuerier.next
					}

					if currentQuerier.next == nil {
						// This was the last available querier.
						lastAvailableQuerier = previousQuerier
					}

					currentQuerier = currentQuerier.next
				} else {
					// Nothing for this querier right now, advance through the list.
					previousQuerier = currentQuerier
					currentQuerier = currentQuerier.next
				}
			}
		}

		if stopping && (queues.len() == 0 || q.connectedQuerierWorkers.Load() == 0) {
			// Tell any waiting GetNextRequestForQuerier calls that nothing is coming.
			for firstAvailableQuerier != nil {
				firstAvailableQuerier.processed <- nextRequestForQuerier{err: ErrStopped}
				firstAvailableQuerier = firstAvailableQuerier.next
			}

			// We are done.
			close(q.stopCompleted)
			return
		}
	}
}

func (q *RequestQueue) handleNewRequest(queues *queues, r enqueueRequest) error {
	queue := queues.getOrAddQueue(r.userID, r.maxQueriers)
	if queue == nil {
		// This can only happen if userID is "".
		return errors.New("no queue found")
	}

	select {
	case queue <- r.req:
		q.queueLength.WithLabelValues(r.userID).Inc()
		// Call the successFn before sending this request to a waiting querier.
		if r.successFn != nil {
			r.successFn()
		}
		return nil
	default:
		q.discardedRequests.WithLabelValues(r.userID).Inc()
		return ErrTooManyRequests
	}
}

// dispatchRequestToQuerier finds and forwards a request to a querier, if a suitable request is available.
// Returns true if this querier should be removed from the list of waiting queriers (eg. because a request has been forwarded to it), false otherwise.
func (q *RequestQueue) dispatchRequestToQuerier(queues *queues, querier *availableQuerier) bool {
	if querier.ctx.Err() != nil {
		querier.processed <- nextRequestForQuerier{
			next: querier.lastUserIndex,
			err:  querier.ctx.Err(),
		}
		return true
	}

	queue, userID, idx := queues.getNextQueueForQuerier(querier.lastUserIndex.last, querier.querierID)
	querier.lastUserIndex.last = idx
	if queue == nil {
		// Nothing available for this querier, try again next time.
		return false
	}

	// Pick next request from the queue.
	request := <-queue
	if len(queue) == 0 {
		queues.deleteQueue(userID)
	}

	q.queueLength.WithLabelValues(userID).Dec()

	querier.processed <- nextRequestForQuerier{
		req:  request,
		next: querier.lastUserIndex,
		err:  querier.ctx.Err(),
	}
	return true
}

// EnqueueRequest puts the request into the queue. MaxQueries is user-specific value that specifies how many queriers can
// this user use (zero or negative = all queriers). It is passed to each EnqueueRequest, because it can change
// between calls.
//
// If request is successfully enqueued, successFn is called with the lock held, before any querier can receive the request.
func (q *RequestQueue) EnqueueRequest(userID string, req Request, maxQueriers int, successFn func()) error {
	// TODO: pool these?
	r := enqueueRequest{
		userID:      userID,
		req:         req,
		maxQueriers: maxQueriers,
		successFn:   successFn,
		processed:   make(chan error),
	}

	select {
	case q.enqueueRequests <- r:
		return <-r.processed
	case <-q.stopCompleted:
		return ErrStopped
	}
}

// GetNextRequestForQuerier find next user queue and takes the next request off of it. Will block if there are no requests.
// By passing user index from previous call of this method, querier guarantees that it iterates over all users fairly.
// If querier finds that request from the user is already expired, it can get a request for the same user by using UserIndex.ReuseLastUser.
func (q *RequestQueue) GetNextRequestForQuerier(ctx context.Context, last UserIndex, querierID string) (Request, UserIndex, error) {
	// TODO: pool these?
	querier := &availableQuerier{
		ctx:           ctx,
		querierID:     querierID,
		lastUserIndex: last,
		processed:     make(chan nextRequestForQuerier),
	}

	select {
	case q.availableQueriers <- querier:
		result := <-querier.processed
		return result.req, result.next, result.err
	case <-q.stopCompleted:
		return nil, last, ErrStopped
	}
}

func (q *RequestQueue) forgetDisconnectedQueriers(_ context.Context) error {
	op := querierOperation{
		operation: forgetDisconnected,
		processed: make(chan struct{}),
	}

	select {
	case q.querierOperations <- op:
		<-op.processed
	case <-q.stopCompleted:
		// Nothing to do.
	}

	return nil
}

func (q *RequestQueue) stop(_ error) error {
	q.stopRequested <- struct{}{} // Why not close the channel? We only want to trigger dispatcherLoop() once.
	<-q.stopCompleted

	return nil
}

func (q *RequestQueue) RegisterQuerierConnection(querierID string) {
	q.connectedQuerierWorkers.Inc()

	op := querierOperation{
		querierID: querierID,
		operation: registerConnection,
		processed: make(chan struct{}),
	}

	select {
	case q.querierOperations <- op:
		<-op.processed
	case <-q.stopCompleted:
		// TODO: return error?
	}
}

func (q *RequestQueue) UnregisterQuerierConnection(querierID string) {
	q.connectedQuerierWorkers.Inc()

	op := querierOperation{
		querierID: querierID,
		operation: unregisterConnection,
		processed: make(chan struct{}),
	}

	select {
	case q.querierOperations <- op:
		<-op.processed
	case <-q.stopCompleted:
		// TODO: return error?
	}
}

func (q *RequestQueue) NotifyQuerierShutdown(querierID string) {
	q.connectedQuerierWorkers.Inc()

	op := querierOperation{
		querierID: querierID,
		operation: notifyShutdown,
		processed: make(chan struct{}),
	}

	select {
	case q.querierOperations <- op:
		<-op.processed
	case <-q.stopCompleted:
		// TODO: return error?
	}
}

func (q *RequestQueue) GetConnectedQuerierWorkersMetric() float64 {
	return float64(q.connectedQuerierWorkers.Load())
}
