// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/queue.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queue

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"
)

const (
	// How frequently to check for disconnected consumers that should be forgotten.
	forgetCheckPeriod = 5 * time.Second
)

var (
	ErrInvalidTenantID            = errors.New("invalid tenant id")
	ErrTooManyRequests            = errors.New("too many outstanding requests")
	ErrStopped                    = errors.New("queue is stopped")
	ErrConsumerShuttingDown       = errors.New("consumer has informed the scheduler it is shutting down")
	ErrConsumerWorkerDisconnected = errors.New("consumer worker has disconnected")

	errInvalidReservationSlots = errors.New("the slots of a reservation must be greater than 0")
)

// ReservationRejectedError is the error of a reservation that the per-tenant queue cap rejects. It
// wraps ErrTooManyRequests. It carries the tenant's occupancy, so the caller can log it outside
// dispatcherLoop.
type ReservationRejectedError struct {
	Requested          int
	Queued             int
	Idle               int
	MaxTenantQueueSize int
}

func (e *ReservationRejectedError) Error() string {
	return ErrTooManyRequests.Error()
}

func (e *ReservationRejectedError) Unwrap() error {
	return ErrTooManyRequests
}

// ReservationMetrics holds the metrics of queue reservations.
type ReservationMetrics struct {
	// Slots observes the number of queue slots that each accepted reservation holds.
	Slots prometheus.Histogram
	// Rejected counts the reservations that the per-tenant queue cap rejected, per user.
	Rejected *prometheus.CounterVec
	// IdleSlots reports the reserved slots that no queued item uses, per user.
	IdleSlots *IdleSlotsGauge
}

// Item is the opaque type the queue dispatches between producers and
// consumers. Callers cast to the concrete type they enqueued.
type Item interface{}

// Queue holds incoming requests in queues, split by multiple dimensions based on properties of the request.
// Dequeuing selects the next request from an appropriate queue given the state of the system.
// Two layers of QueueAlgorithms are used by the Queue to select the next queue to dequeue a request from:
//
//   - Tenant-Consumer Assignments
//     Tenants with shuffle-sharding enabled by setting maxConsumers > 0 are assigned a subset of consumers.
//     The Queue utilizes the consumer assignments to only dequeue requests for a tenant assigned to that consumer.
//     If shuffle-sharding is disabled, requests are dequeued in a fair round-robin fashion across all tenants.
//
//   - Consumer-Worker Queue Priority
//     Consumer-worker connections are distributed across queue partitions which separate query requests
//     based on the query component expected to be utilized to service the query.
//     This division prevents a query component experiencing high latency from dominating the utilization
//     of consumer-worker connections and preventing requests for other query components from being serviced.
//
// See each QueueAlgorithm implementation for more details.
type Queue struct {
	services.Service
	log log.Logger

	// settings
	maxOutstandingPerTenant int
	forgetDelay             time.Duration

	// metrics for reporting
	connectedConsumerWorkers *atomic.Int64
	// metrics are broken out with "user" label for backwards compat, despite update to "tenant" terminology
	queueLength       *prometheus.GaugeVec   // per user
	discardedRequests *prometheus.CounterVec // per user
	enqueueDuration   prometheus.Histogram
	maxQueueLength    *MaxQueueLengthGauge // per user; nil when the metric is disabled

	reservationSlots     prometheus.Histogram
	rejectedReservations *prometheus.CounterVec // per user
	idleSlots            *IdleSlotsGauge        // per user

	// Only dispatcherLoop uses the open reservations.
	reservations        map[ReservationKey]*Reservation
	reservationsByOwner map[string]map[ReservationKey]*Reservation
	reservationHeap     reservationHeap

	stopRequested chan struct{} // Written to by stop() to wake up dispatcherLoop() in response to a stop request.
	stopCompleted chan struct{} // Closed by dispatcherLoop() after a stop is requested and the dispatcher has stopped.

	producerOperations                    chan producerOperation
	consumerWorkerOperations              chan *consumerWorkerOperation
	waitingDequeueRequests                chan *ConsumerWorkerDequeueRequest
	waitingDequeueRequestsToDispatch      *list.List
	waitingDequeueRequestsToDispatchCount *atomic.Int64

	queueBroker *queueBroker
}

type consumerOperationType int

const (
	registerConnection consumerOperationType = iota
	unregisterConnection
	notifyShutdown
	forgetDisconnected
)

// consumerWorkerOperation is a message to the Queue's dispatcherLoop to perform operations
// on consumer and consumer-worker connections, such as registering, unregistering, or notifying shutdown.
//
// Initializing the done chan as non-nil indicates that the operation is awaitable.
// For awaitable operations, done is written to when the operation is processed,
// and updates are reflected on the referenced ConsumerWorkerConn.
//
// A nil done chan indicates that the operation is not awaitable;
// the caller does not care to wait for the result to be written
// and the processor will not bother to write the result.
type consumerWorkerOperation struct {
	conn      *ConsumerWorkerConn
	operation consumerOperationType

	done chan struct{}
}

func newConsumerWorkerOperation(
	consumerWorkerConn *ConsumerWorkerConn, opType consumerOperationType,
) *consumerWorkerOperation {
	return &consumerWorkerOperation{
		conn:      consumerWorkerConn,
		operation: opType,
		done:      nil,
	}
}

func newAwaitableConsumerWorkerOperation(
	consumerWorkerConn *ConsumerWorkerConn, opType consumerOperationType,
) *consumerWorkerOperation {
	return &consumerWorkerOperation{
		conn:      consumerWorkerConn,
		operation: opType,
		done:      make(chan struct{}),
	}
}

func (qwo *consumerWorkerOperation) IsAwaitable() bool {
	return qwo.done != nil
}

func (qwo *consumerWorkerOperation) AwaitConsumerWorkerConnUpdate() error {
	if !qwo.IsAwaitable() {
		// if the operation was not created with a receiver channel, the request queue will
		// process it asynchronously and the caller will not be able to wait for the result.
		return errors.New("cannot await update for non-awaitable consumer-worker operation")
	}

	select {
	case <-qwo.conn.ctx.Done():
		// context done case serves as a default case to bail out
		// if the waiting consumer-worker connection's context times out or is canceled,
		// allowing the dispatcherLoop to proceed with its next iteration
		return qwo.conn.ctx.Err()
	case <-qwo.done:
		return nil
	}
}

type producerOpKind int

const (
	// opEnqueue is the zero value, so a producerOperation that does not set op is a plain enqueue.
	opEnqueue producerOpKind = iota
	opOpenReservation
	opEnqueueIntoReservation
	opCompleteReservation
	opAbortReservation
	opCloseReservationsOfOwner
	opReapReservations
)

// producerOperation is one operation that dispatcherLoop applies to the queue. op selects the
// operation, and each operation uses a subset of the fields.
type producerOperation struct {
	op             producerOpKind
	tenantID       string
	queueDimension string
	item           Item
	maxConsumers   int
	successFn      func()

	// The reservation ops use the fields below.
	reservationKey    ReservationKey
	slots             int
	now               time.Time
	timeout           time.Duration
	reservedSuccessFn func(*Reservation)
	// result holds the output of a reservation op. dispatcherLoop fills it before it sends on
	// errChan, so the caller reads it after the receive.
	result *producerOpResult

	errChan chan error
}

type producerOpResult struct {
	closed []ClosedReservation
}

// New creates a Queue. It rejects an enqueue or a reservation that would make a tenant's queued
// items plus idle slots exceed maxOutstandingPerTenant.
//
// A caller that never reserves slots can pass a nil reservationMetrics. The queue then uses
// reservation metrics that no registry exposes.
func New(
	log log.Logger,
	maxOutstandingPerTenant int,
	forgetDelay time.Duration,
	queueLength *prometheus.GaugeVec,
	maxQueueLength *MaxQueueLengthGauge,
	discardedRequests *prometheus.CounterVec,
	enqueueDuration prometheus.Histogram,
	reservationMetrics *ReservationMetrics,
) (*Queue, error) {
	if reservationMetrics == nil {
		reservationMetrics = &ReservationMetrics{
			Slots:     promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
			Rejected:  promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
			IdleSlots: NewIdleSlotsGauge("", ""),
		}
	}

	q := &Queue{
		// settings
		log:                     log,
		maxOutstandingPerTenant: maxOutstandingPerTenant,
		forgetDelay:             forgetDelay,

		// metrics for reporting
		connectedConsumerWorkers: atomic.NewInt64(0),
		queueLength:              queueLength,
		maxQueueLength:           maxQueueLength,
		discardedRequests:        discardedRequests,
		enqueueDuration:          enqueueDuration,
		reservationSlots:         reservationMetrics.Slots,
		rejectedReservations:     reservationMetrics.Rejected,
		idleSlots:                reservationMetrics.IdleSlots,

		reservations:        map[ReservationKey]*Reservation{},
		reservationsByOwner: map[string]map[ReservationKey]*Reservation{},

		// channels must not be buffered so that we can detect when dispatcherLoop() has finished.
		stopRequested: make(chan struct{}),
		stopCompleted: make(chan struct{}),

		producerOperations:                    make(chan producerOperation),
		consumerWorkerOperations:              make(chan *consumerWorkerOperation),
		waitingDequeueRequests:                make(chan *ConsumerWorkerDequeueRequest),
		waitingDequeueRequestsToDispatch:      list.New(),
		waitingDequeueRequestsToDispatchCount: atomic.NewInt64(0),

		queueBroker: newQueueBroker(maxOutstandingPerTenant, forgetDelay),
	}

	q.Service = services.NewBasicService(q.starting, q.running, q.stop).WithName("request queue")

	return q, nil
}

func (q *Queue) starting(_ context.Context) error {

	go q.dispatcherLoop()

	return nil
}

func (q *Queue) running(ctx context.Context) error {
	// periodically submit a message to dispatcherLoop to forget disconnected consumers
	forgetDisconnectedConsumersTicker := time.NewTicker(forgetCheckPeriod)
	defer forgetDisconnectedConsumersTicker.Stop()

	for {
		select {
		case <-forgetDisconnectedConsumersTicker.C:
			q.submitForgetDisconnectedConsumers(ctx)
		case <-ctx.Done():
			// context done case serves as a default case to bail out
			// if the waiting consumer-worker connection's context times out or is canceled,
			// allowing the dispatcherLoop to proceed with its next iteration
			return nil
		}
	}
}

func (q *Queue) dispatcherLoop() {
	// The only way for this loop to exit is for a valid stop state to have been reached, or for it to have crashed.
	defer close(q.stopCompleted)

	isStopping := false

	for {
		needToDispatchQueries := false

		select {
		case <-q.stopRequested:
			// Nothing much to do here - fall through to the stop logic below to see if we can stop immediately.
			isStopping = true

		case consumerWorkerOp := <-q.consumerWorkerOperations:
			// Need to attempt to dispatch queries only if consumer-worker operation results in a resharding
			needToDispatchQueries = q.processConsumerWorkerOperation(consumerWorkerOp)
		case producerOp := <-q.producerOperations:
			err := q.processProducerOperation(producerOp)
			producerOp.errChan <- err
			// Only an enqueue adds an item, so the other ops cannot make a waiting dequeue succeed.
			if err == nil && (producerOp.op == opEnqueue || producerOp.op == opEnqueueIntoReservation) {
				needToDispatchQueries = true
			}
		case waitingDequeueReq := <-q.waitingDequeueRequests:
			requestSent := q.trySendNextItemForConsumer(waitingDequeueReq)
			if !requestSent {
				// No requests available for this consumer; add it to the list to try later.
				q.waitingDequeueRequestsToDispatchCount.Inc()
				q.waitingDequeueRequestsToDispatch.PushBack(waitingDequeueReq)
			}
		}

		if needToDispatchQueries {
			currentElement := q.waitingDequeueRequestsToDispatch.Front()

			for currentElement != nil && !q.queueBroker.isEmpty() {
				call := currentElement.Value.(*ConsumerWorkerDequeueRequest)
				nextElement := currentElement.Next() // We have to capture the next element before calling Remove(), as Remove() clears it.

				if q.trySendNextItemForConsumer(call) {
					q.waitingDequeueRequestsToDispatchCount.Dec()
					q.waitingDequeueRequestsToDispatch.Remove(currentElement)
				}

				currentElement = nextElement
			}
		}

		if isStopping {
			// We need to check both the inflight requests at the scheduler level, and the items remaining in the local queue
			// to ensure that we do not terminate the consumer and frontend connections before everything has been cleanly processed.
			// If we exit while there are inflight requests, the consumers will receive a context cancellation and return an error to the frontend/user
			// And if there are pending items in the queue that are not yet inflight, we still need to dispatch them to the consumers.
			if q.queueBroker.itemCount() == 0 {
				// If the queue is stopping and theres no connected query workers,
				// we exit immediately because there is no way for (any) remaining queries to be processed
				if q.connectedConsumerWorkers.Load() == 0 {
					level.Info(q.log).Log("msg", "queue stop completed: query queue is empty and all workers have been disconnected")
					return
				}

				// If the queue is stopping and theres no requests in the queue we cancel any remaining dequeue requests,
				// which stops the query workers and exit the service
				level.Info(q.log).Log("msg", "queue stop requested and all pending requests have been processed, disconnecting consumers")

				currentElement := q.waitingDequeueRequestsToDispatch.Front()

				for currentElement != nil {
					waitingDequeueReq := currentElement.Value.(*ConsumerWorkerDequeueRequest)
					waitingDequeueReq.sendError(ErrStopped)

					level.Debug(q.log).Log("msg", "cancelled dequeue request", "consumer_id", waitingDequeueReq.ConsumerID, "worker_id", waitingDequeueReq.WorkerID)

					nextElement := currentElement.Next()
					q.waitingDequeueRequestsToDispatchCount.Dec()
					q.waitingDequeueRequestsToDispatch.Remove(currentElement)
					currentElement = nextElement
				}

				// We are done.
				level.Info(q.log).Log("msg", "queue stop completed: all query dequeue requests closed")
				return
			}

			level.Debug(q.log).Log(
				"msg", "queue is stopping but query queue is not empty, waiting for query workers to complete remaining requests",
				"queued_requests", q.queueBroker.itemCount(),
			)
		}
	}
}

// processProducerOperation applies one producerOperation that dispatcherLoop received.
//
// For opEnqueue and opEnqueueIntoReservation, if the item is enqueued its success function is
// called before the item can be dispatched to a consumer.
func (q *Queue) processProducerOperation(r producerOperation) error {
	switch r.op {
	case opEnqueue:
		return q.enqueue(&r, nil)

	case opOpenReservation:
		return q.openReservation(r)

	case opEnqueueIntoReservation:
		return q.enqueueIntoReservation(r)

	case opCompleteReservation, opAbortReservation:
		outcome := ReservationCompleted
		if r.op == opAbortReservation {
			outcome = ReservationAborted
		}
		if res := q.reservations[r.reservationKey]; res != nil {
			r.result.closed = append(r.result.closed, q.closeReservation(res, outcome))
		}
		return nil

	case opCloseReservationsOfOwner:
		// closeReservation deletes from this map while the loop ranges over it, which Go allows.
		for _, res := range q.reservationsByOwner[r.reservationKey.Owner] {
			r.result.closed = append(r.result.closed, q.closeReservation(res, ReservationOwnerDisconnected))
		}
		return nil

	case opReapReservations:
		return q.reapReservations(r)

	default:
		return fmt.Errorf("unknown queue op: %v", r.op)
	}
}

// enqueue inserts r's item. A nil res means a plain item, which passes the per-tenant cap check.
// A non-nil res means a member, which uses an idle slot of res. Only dispatcherLoop calls it.
func (q *Queue) enqueue(r *producerOperation, res *Reservation) error {
	ti := tenantItem{
		tenantID:       r.tenantID,
		queueDimension: r.queueDimension,
		item:           r.item,
		reservation:    res,
	}
	var (
		leaked bool
		err    error
	)
	if res != nil {
		leaked, err = q.queueBroker.enqueueReservedItemBack(&ti, r.maxConsumers)
	} else {
		err = q.queueBroker.enqueueItemBack(&ti, r.maxConsumers)
	}
	if err != nil {
		if errors.Is(err, ErrTooManyRequests) {
			q.discardedRequests.WithLabelValues(r.tenantID).Inc()
		}
		return err
	}
	if leaked {
		level.Warn(q.log).Log("msg", "reserved enqueue found no idle slot for the tenant; this is a bug", "tenant", r.tenantID)
	}

	if res != nil {
		res.inserted++
		res.queued++
		res.inflight.Inc()
		res.storeLastActivity(r.now)
		q.updateIdleSlots(res.tenantID)
	}
	if r.reservedSuccessFn != nil {
		r.reservedSuccessFn(res)
	} else if r.successFn != nil {
		r.successFn()
	}
	q.queueLength.WithLabelValues(r.tenantID).Inc()
	if q.maxQueueLength != nil {
		q.maxQueueLength.inc(r.tenantID)
	}
	return nil
}

// trySendNextItemForConsumer attempts to dequeue and send a request for a waiting consumer-worker connection.
//
// Returns true if the ConsumerWorkerDequeueRequest can be removed from the list of waiting dequeue requests,
// meaning a consumerWorkerDequeueResponse was sent through the ConsumerWorkerDequeueRequest's receiving channel
// or the waiting consumer-worker connection's context was canceled.
//
// The consumerWorkerDequeueResponse message can contain either:
// a) a query request which was successfully dequeued for the consumer, or
// b) an ErrConsumerShuttingDown indicating the consumer has been placed in a graceful shutdown state.
func (q *Queue) trySendNextItemForConsumer(dequeueReq *ConsumerWorkerDequeueRequest) (done bool) {

	dequeued, tenant, idx, err := q.queueBroker.dequeueItemForConsumer(dequeueReq)
	if err != nil {
		// If this consumer has told us it's shutting down, terminate AwaitItemForConsumer with an error now...
		dequeueReq.sendError(err)
		// ...and remove the waiting dequeueReq from our list.
		return true
	}

	if dequeued == nil {
		// Nothing available for this consumer, try again next time.
		return false
	}

	response := consumerWorkerDequeueResponse{
		item:            dequeued.item,
		lastTenantIndex: TenantIndex{last: idx},
		err:             nil,
	}

	sent := dequeueReq.sendResponse(response)
	if sent {
		if dequeued.reservation != nil {
			q.returnReservedSlot(dequeued.reservation)
		}
		q.queueLength.WithLabelValues(tenant.tenantID).Dec()
		if q.maxQueueLength != nil {
			q.maxQueueLength.dec(tenant.tenantID)
		}
	} else {
		// should never error; any item previously in the queue already passed validation
		err := q.queueBroker.enqueueItemFront(dequeued, tenant.maxConsumers)
		if err != nil {
			level.Error(q.log).Log(
				"msg", "failed to re-enqueue query request after dequeue",
				"err", err, "tenant", tenant.tenantID, "consumer", dequeueReq.ConsumerID,
			)
		}
	}
	return true
}

// SubmitItemToEnqueue handles a query request from the query frontend or scheduler and submits it to the queue.
// This method will block until the queue's processing loop has enqueued the request into its internal queue structure.
//
// If request is successfully enqueued, successFn is called before any consumer can receive the request.
// Returns error if any occurred during enqueuing, or if the Queue service stopped before enqueuing the request.
//
// queueDimension is the first-layer queue dimension to file the request under (e.g. a query component name).
// An empty string is treated as the "unknown" dimension.
//
// maxConsumers is tenant-specific value to compute which consumers should handle requests for this tenant.
// It is passed to SubmitItemToEnqueue because the value can change between calls.
func (q *Queue) SubmitItemToEnqueue(tenantID string, item Item, queueDimension string, maxConsumers int, successFn func()) error {
	start := time.Now()
	defer func() {
		q.enqueueDuration.Observe(time.Since(start).Seconds())
	}()

	return q.submitProducerOperation(producerOperation{
		op:             opEnqueue,
		tenantID:       tenantID,
		queueDimension: queueDimension,
		item:           item,
		maxConsumers:   maxConsumers,
		successFn:      successFn,
	})
}

// SubmitOpenReservation opens a reservation at key for tenantID. It reserves slots queue slots
// against the per-tenant queue cap, for the items that SubmitItemIntoReservation inserts later.
// The reservation stays open until SubmitCompleteReservation or another close. The cap check
// counts the queued items and the idle slots of the tenant's other reservations. now is the start
// of the reservation's idle time.
//
// The caller blocks until dispatcherLoop has processed the operation. Returns
// ErrReservationExists if a reservation at key is open, ErrReservationExceedsCap if slots is
// larger than the cap, a *ReservationRejectedError if slots does not fit under the cap now,
// ErrInvalidTenantID if tenantID is empty, errInvalidReservationSlots if slots is less than 1, or
// ErrStopped if the Queue service stopped first.
//
// The reservation does not create the tenant. The first inserted item creates it. A reservation
// enqueues no item, so it does not observe enqueueDuration.
func (q *Queue) SubmitOpenReservation(key ReservationKey, tenantID string, slots int, now time.Time) error {
	return q.submitProducerOperation(producerOperation{
		op:             opOpenReservation,
		reservationKey: key,
		tenantID:       tenantID,
		slots:          slots,
		now:            now,
	})
}

// SubmitItemIntoReservation inserts item for tenantID as a member of the open reservation at key.
// The item uses an idle slot of the reservation and skips the per-tenant cap check.
//
// If the item is enqueued, successFn is called with the reservation before any consumer can
// receive the item. The caller must call ItemFinished on that reservation once its consumer
// finished or skipped the item.
//
// queueDimension and maxConsumers have the same meaning as for SubmitItemToEnqueue. now is the
// activity time of the reservation. Returns, without an enqueue, ErrReservationNotFound if no
// reservation is open at key, ErrReservationOtherTenant if it belongs to another tenant,
// ErrReservationNoIdleSlot if it has no idle slot, or ErrStopped if the Queue service
// stopped first.
func (q *Queue) SubmitItemIntoReservation(key ReservationKey, tenantID string, item Item, queueDimension string, maxConsumers int, now time.Time, successFn func(*Reservation)) error {
	start := time.Now()
	defer func() {
		q.enqueueDuration.Observe(time.Since(start).Seconds())
	}()

	return q.submitProducerOperation(producerOperation{
		op:                opEnqueueIntoReservation,
		reservationKey:    key,
		tenantID:          tenantID,
		queueDimension:    queueDimension,
		item:              item,
		maxConsumers:      maxConsumers,
		now:               now,
		reservedSuccessFn: successFn,
	})
}

// SubmitCompleteReservation closes the open reservation at key after its owner inserted its last
// item, and releases its idle slots. Its queued items stay in the queue. Returns nil when no
// reservation is open at key, or ErrStopped if the Queue service stopped first.
func (q *Queue) SubmitCompleteReservation(key ReservationKey) (*ClosedReservation, error) {
	return q.submitReservationCloseOne(opCompleteReservation, key)
}

// SubmitAbortReservation aborts the open reservation at key and releases its idle slots. Its
// queued items stay in the queue. Returns nil when no reservation is open at key, or ErrStopped if
// the Queue service stopped first.
func (q *Queue) SubmitAbortReservation(key ReservationKey) (*ClosedReservation, error) {
	return q.submitReservationCloseOne(opAbortReservation, key)
}

func (q *Queue) submitReservationCloseOne(op producerOpKind, key ReservationKey) (*ClosedReservation, error) {
	closed, err := q.submitReservationClose(producerOperation{op: op, reservationKey: key})
	if len(closed) == 0 {
		return nil, err
	}
	return &closed[0], err
}

// SubmitCloseReservationsOfOwner closes every open reservation of owner and releases their idle
// slots. Their queued items stay in the queue. Returns ErrStopped if the Queue service stopped
// first.
func (q *Queue) SubmitCloseReservationsOfOwner(owner string) ([]ClosedReservation, error) {
	return q.submitReservationClose(producerOperation{op: opCloseReservationsOfOwner, reservationKey: ReservationKey{Owner: owner}})
}

// SubmitReapReservations closes every open reservation that has no inflight member and no
// activity since now minus timeout, and releases its idle slots. Returns
// errInvalidReservationTimeout if timeout is not greater than 0, or ErrStopped if the Queue
// service stopped first.
func (q *Queue) SubmitReapReservations(now time.Time, timeout time.Duration) ([]ClosedReservation, error) {
	return q.submitReservationClose(producerOperation{op: opReapReservations, now: now, timeout: timeout})
}

func (q *Queue) submitReservationClose(r producerOperation) ([]ClosedReservation, error) {
	r.result = &producerOpResult{}
	err := q.submitProducerOperation(r)
	return r.result.closed, err
}

// submitProducerOperation sends r to dispatcherLoop and blocks until dispatcherLoop returns its
// result. Returns ErrStopped if the Queue service stopped before dispatcherLoop received r.
func (q *Queue) submitProducerOperation(r producerOperation) error {
	r.errChan = make(chan error)

	select {
	case q.producerOperations <- r:
		return <-r.errChan
	case <-q.stopCompleted:
		return ErrStopped
	}
}

// AwaitItemForConsumer is called by a consumer-worker to submit a ConsumerWorkerDequeueRequest message to the Queue.
//
// This method blocks until the ConsumerWorkerDequeueRequest gets a consumerWorkerDequeueResponse message on its receiving channel,
// the consumer-worker connection context is canceled, or the Queue service stops.
//
// Consumer-workers should pass the last TenantIndex received from their previous call to AwaitItemForConsumer,
// which enables the Queue to iterate fairly across all tenants assigned to a consumer.
// If a consumer-worker finds that the query request received for the tenant is already expired,
// it can get another request for the same tenant by using TenantIndex.ReuseLastTenant.
// Newly-connected consumer-workers should pass FirstTenant as the TenantIndex to start iteration from the beginning.
func (q *Queue) AwaitItemForConsumer(dequeueReq *ConsumerWorkerDequeueRequest) (Item, TenantIndex, error) {
	// context done cases serves as a default case to bail out
	// if the waiting consumer-worker connection's context times out or is canceled,
	// allowing the dispatcherLoop to proceed with its next iteration
	select {
	case q.waitingDequeueRequests <- dequeueReq:
		select {
		case response := <-dequeueReq.recvChan:
			return response.item, response.lastTenantIndex, response.err
		case <-dequeueReq.ctx.Done():
			return nil, dequeueReq.lastTenantIndex, dequeueReq.ctx.Err()
		case <-q.stopCompleted:
			return nil, dequeueReq.lastTenantIndex, ErrStopped
		}
	case <-dequeueReq.ctx.Done():
		return nil, dequeueReq.lastTenantIndex, dequeueReq.ctx.Err()
	case <-q.stopCompleted:
		return nil, dequeueReq.lastTenantIndex, ErrStopped
	}
}

func (q *Queue) stop(_ error) error {
	// Do not close the stopRequested channel;
	// this would cause the read from stopRequested to preempt all other select cases in dispatcherLoop.
	// Reads from stopRequested tell dispatcherLoop to enter a stopping state where it tries to clear the queue.
	// The loop needs to keep executing other select branches while stopping in order to clear the queue.
	q.stopRequested <- struct{}{}

	<-q.stopCompleted
	return nil
}

func (q *Queue) GetConnectedConsumerWorkersMetric() float64 {
	return float64(q.connectedConsumerWorkers.Load())
}

func (q *Queue) AwaitRegisterConsumerWorkerConn(conn *ConsumerWorkerConn) error {
	return q.awaitConsumerWorkerOperation(conn, registerConnection)
}

func (q *Queue) awaitConsumerWorkerOperation(
	conn *ConsumerWorkerConn, opType consumerOperationType,
) error {
	op := newAwaitableConsumerWorkerOperation(conn, opType)

	// we do not check for a context cancel here;
	// if the caller's context is canceled, we still want the dispatcherLoop to process the operation
	// and update its tracking of consumer-worker connections and consumer shutting down / forget-delay states
	select {
	case q.consumerWorkerOperations <- op:
		// client context cancels will be checked for in AwaitConsumerWorkerConnUpdate
		return op.AwaitConsumerWorkerConnUpdate()
	case <-q.stopCompleted:
		return ErrStopped
	}
}

func (q *Queue) SubmitUnregisterConsumerWorkerConn(conn *ConsumerWorkerConn) {
	q.submitConsumerWorkerOperation(conn, unregisterConnection)
}

// submitForgetDisconnectedConsumers is called in a ticker from the Queue's `running` goroutine.
// The operation is not specific to any one consumer and this method can stay private.
func (q *Queue) submitForgetDisconnectedConsumers(ctx context.Context) {
	// Create a generic consumer-worker connection to submit the operation.
	consumerWorkerOp := NewUnregisteredConsumerWorkerConn(ctx, "")
	q.submitConsumerWorkerOperation(consumerWorkerOp, forgetDisconnected)
}

// SubmitNotifyConsumerShutdown is called by the v1 frontend or scheduler when NotifyConsumerShutdown requests
// are submitted from the consumer to an endpoint, separate from any specific consumer-worker connection.
func (q *Queue) SubmitNotifyConsumerShutdown(ctx context.Context, consumerID string) {
	// Create a generic consumer-worker connection to submit the operation.
	conn := NewUnregisteredConsumerWorkerConn(ctx, consumerID) // consumerID matters but workerID does not
	q.submitConsumerWorkerOperation(conn, notifyShutdown)
}

func (q *Queue) submitConsumerWorkerOperation(
	conn *ConsumerWorkerConn, opType consumerOperationType,
) {
	op := newConsumerWorkerOperation(conn, opType)

	// we do not check for a context cancel here;
	// if the caller's context is canceled, we still want the dispatcherLoop to process the operation
	// and update its tracking of consumer-worker connections and consumer shutting down / forget-delay states
	select {
	case q.consumerWorkerOperations <- op:
		// The dispatcher has received the operation. There's nothing more to do.
	case <-q.stopCompleted:
		// The dispatcher stopped before it could process the operation. There's nothing more to do.
	}
}

func (q *Queue) processConsumerWorkerOperation(consumerWorkerOp *consumerWorkerOperation) (resharded bool) {
	switch consumerWorkerOp.operation {
	case registerConnection:
		resharded = q.processRegisterConsumerWorkerConn(consumerWorkerOp.conn)
	case unregisterConnection:
		resharded = q.processUnregisterConsumerWorkerConn(consumerWorkerOp.conn)
	case notifyShutdown:
		// No cleanup needed here in response to a graceful shutdown; just set consumer state to shutting down.
		// All subsequent waitingDequeueRequests for the consumer will receive an ErrConsumerShuttingDown.
		// The consumer-worker's end of the ConsumerLoop will exit once it has received enough errors,
		// and the Consumer connection counts will be decremented as the workers disconnect.
		resharded = q.queueBroker.notifyConsumerShutdown(consumerWorkerOp.conn.ConsumerID)
	case forgetDisconnected:
		resharded = q.processForgetDisconnectedConsumers()
	default:
		msg := fmt.Sprintf(
			"received unknown consumer-worker event %v for consumer ID %v",
			consumerWorkerOp.operation, consumerWorkerOp.conn.ConsumerID,
		)
		panic(msg)
	}
	if consumerWorkerOp.IsAwaitable() {
		select {
		case consumerWorkerOp.done <- struct{}{}:
		case <-consumerWorkerOp.conn.ctx.Done():
		case <-q.stopCompleted:
		}
	}

	return resharded
}

func (q *Queue) processRegisterConsumerWorkerConn(conn *ConsumerWorkerConn) (resharded bool) {
	q.connectedConsumerWorkers.Inc()
	return q.queueBroker.addConsumerWorkerConn(conn)
}

func (q *Queue) processUnregisterConsumerWorkerConn(conn *ConsumerWorkerConn) (resharded bool) {
	q.connectedConsumerWorkers.Dec()
	return q.queueBroker.removeConsumerWorkerConn(conn, time.Now())
}

func (q *Queue) processForgetDisconnectedConsumers() (resharded bool) {
	return q.queueBroker.forgetDisconnectedConsumers(time.Now())
}

func (q *Queue) IsEmpty() bool {
	return q.queueBroker.isEmpty()
}

// TenantIndex is opaque type that allows to resume iteration over tenants
// between successive calls of Queue.AwaitItemForConsumer method.
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

// ConsumerWorkerDequeueRequest is a request from a consumer-worker which is ready to receive the next query.
// It embeds the unbuffered `recvChan` to receive the consumerWorkerDequeueResponse from the Queue.
type ConsumerWorkerDequeueRequest struct {
	*ConsumerWorkerConn
	lastTenantIndex TenantIndex
	recvChan        chan consumerWorkerDequeueResponse
}

func NewConsumerWorkerDequeueRequest(consumerWorkerConn *ConsumerWorkerConn, lastTenantIdx TenantIndex) *ConsumerWorkerDequeueRequest {
	return &ConsumerWorkerDequeueRequest{
		ConsumerWorkerConn: consumerWorkerConn,
		lastTenantIndex:    lastTenantIdx,
		recvChan:           make(chan consumerWorkerDequeueResponse),
	}
}

// consumerWorkerDequeueResponse is the response for a ConsumerWorkerDequeueRequest,
// to be written to the dequeue request's receiver channel.
// Errors are embedded in this response rather than written to a separate error channel
// so that lastTenantIndex can still be returned back to the consumer connection.
type consumerWorkerDequeueResponse struct {
	item            Item
	lastTenantIndex TenantIndex
	err             error
}

func (wqc *ConsumerWorkerDequeueRequest) sendError(err error) {
	// consumer or request queue may be shutting down; ignore the result from sendResponse
	// as the consumer may not receive the message before the context is canceled
	_ = wqc.sendResponse(consumerWorkerDequeueResponse{err: err})
}

// sendResponse sends the dequeued item to the receiver channel that is waiting for a new item.
// Returns true if sending succeeds, or false if the receiver's context is timed out or canceled.
func (wqc *ConsumerWorkerDequeueRequest) sendResponse(req consumerWorkerDequeueResponse) bool {
	select {
	case wqc.recvChan <- req:
		return true
	case <-wqc.ctx.Done():
		// context done case serves as a default case to bail out
		// if the waiting consumer-worker connection's context times out or is canceled,
		// allowing the dispatcherLoop to proceed with its next iteration
		return false
	}
}
