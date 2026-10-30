// SPDX-License-Identifier: AGPL-3.0-only

package v2

import (
	"context"
	"net/http"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	"github.com/grafana/dskit/httpgrpc"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
)

const (
	reservationResultOK              = "ok"
	reservationResultTooManyRequests = "too_many_requests"
	reservationResultShuttingDown    = "shutting_down"
	reservationResultSchedulerError  = "scheduler_error"
	reservationResultSendFailed      = "send_failed"
)

type reserveRequest struct {
	userID        string
	reservationID string
	slots         int

	// Buffered with a size of 1, so the worker never blocks on the reply.
	reply chan reserveResult
}

type reserveResult struct {
	// worker is the worker whose stream sent the reservation.
	worker *frontendSchedulerWorker
	// streamsGone is the done channel of worker.reservationStreams.
	streamsGone <-chan struct{}
	// transportErr is set when the stream failed before the reply came.
	transportErr error
	status       schedulerpb.SchedulerToFrontendStatus
	schedulerErr string
}

// queueReservation reserves query-scheduler queue slots for the remote execution sub-requests of
// one MQE evaluation, then sends each sub-request to the query-scheduler that granted the
// reservation. The ParallelismLimiter keeps at most slots sub-requests in flight, so at most slots
// sub-requests wait in the queue.
type queueReservation struct {
	f      *Frontend
	userID string
	slots  int

	// done is closed when the result of the reservation is known. worker, reservationID,
	// streamsGone, err and errOutcome do not change after that.
	done          chan struct{}
	worker        *frontendSchedulerWorker
	reservationID string
	// streamsGone is closed when the query-scheduler has removed the reservation, because all
	// streams of worker that support queue reservation ended.
	streamsGone <-chan struct{}
	err         error
	// errOutcome is the cortex_query_frontend_enqueue_requests_total outcome of each sub-request
	// that fails with err.
	errOutcome string

	mu sync.Mutex
	// closed is true after Close. closeAborted is the argument of Close.
	closed       bool
	closeAborted bool
	// closeSent is true after the query-frontend sent the close of the granted reservation.
	closeSent bool
}

// queueReservationForRequest returns the queue reservation that a remote execution sub-request
// uses, or nil when the sub-request uses the plain path.
func (f *Frontend) queueReservationForRequest(ctx context.Context, userID string) *queueReservation {
	member := querymiddleware.RemoteExecutionReservationMemberFromContext(ctx)
	if member == nil {
		return nil
	}

	r, _ := member.SealAndReserve(func(groups int) querymiddleware.RemoteExecutionReservation {
		return f.newQueueReservation(ctx, userID, groups, querymiddleware.ParallelismLimiterFromContext(ctx).MaxParallelism())
	}).(*queueReservation)
	return r
}

// newQueueReservation starts the reservation of min(groups, maxParallelism) slots in the
// background. It returns nil when queue reservation is disabled or no query-scheduler supports it,
// so that the sub-requests use the plain path. It must not block, because it runs with the lock of
// the scope held.
func (f *Frontend) newQueueReservation(ctx context.Context, userID string, groups, maxParallelism int) querymiddleware.RemoteExecutionReservation {
	slots := min(groups, maxParallelism)
	if !f.cfg.QueueReservationEnabled || slots <= 0 {
		return nil
	}

	if !f.schedulerWorkers.hasReservationStreams() {
		return nil
	}

	r := &queueReservation{
		f:      f,
		userID: userID,
		slots:  slots,
		done:   make(chan struct{}),
	}
	go r.run(ctx)
	return r
}

// run sends the reservation on the first free stream that supports queue reservation, to any
// query-scheduler. After a transport error it tries again with a new reservation ID, as many times
// as the plain path tries a sub-request. Any other failed reservation is final.
func (r *queueReservation) run(ctx context.Context) {
	defer close(r.done)

	maxAttempts := r.f.cfg.WorkerConcurrency + 1 // To make sure we hit at least two different schedulers.
	for range maxAttempts {
		// Each attempt needs a new ID. With the same ID, the abort of a failed attempt could close the
		// next reservation, or the next RESERVE could get the duplicate rejection.
		reservationID := uuid.NewString()
		res, err := r.reserve(ctx, reservationID)
		if err != nil {
			r.err = err
			r.errOutcome = "cancelled"
			return
		}

		w := res.worker
		logger := log.With(r.f.log, "user", r.userID, "scheduler_address", w.schedulerAddr, "reservation_id", reservationID, "slots", r.slots)

		switch {
		case res.transportErr != nil:
			r.f.queueReservations.WithLabelValues(reservationResultSendFailed).Inc()
			level.Warn(logger).Log("msg", "failed to send queue reservation to query-scheduler", "err", res.transportErr)
			// The query-scheduler can have granted the reservation before the stream failed.
			r.f.closeReservation(w, reservationID, false)
			// No sub-request uses the reservation yet, so another attempt cannot queue a sub-request twice.
			continue

		case res.status == schedulerpb.OK:
			r.f.queueReservations.WithLabelValues(reservationResultOK).Inc()
			r.mu.Lock()
			r.worker = w
			r.reservationID = reservationID
			r.streamsGone = res.streamsGone
			closed, aborted := r.closed, r.closeAborted
			r.mu.Unlock()
			if closed {
				r.sendClose(aborted)
			}
			return

		case res.status == schedulerpb.TOO_MANY_REQUESTS_PER_TENANT:
			r.f.queueReservations.WithLabelValues(reservationResultTooManyRequests).Inc()
			level.Warn(logger).Log("msg", "query-scheduler rejected queue reservation because the tenant has too many outstanding requests")
			r.err = apierror.New(apierror.TypeTooManyRequests, "too many outstanding requests")
			r.errOutcome = "too_many_requests"
			return

		case res.status == schedulerpb.SHUTTING_DOWN:
			r.f.queueReservations.WithLabelValues(reservationResultShuttingDown).Inc()
			level.Warn(logger).Log("msg", "query-scheduler reported that it is shutting down")

		case res.status == schedulerpb.ERROR:
			r.f.queueReservations.WithLabelValues(reservationResultSchedulerError).Inc()
			level.Warn(logger).Log("msg", "query-scheduler returned error for queue reservation", "err", res.schedulerErr)
			r.err = apierror.New(apierror.TypeInternal, res.schedulerErr)
			r.errOutcome = "scheduler_error"
			return

		default:
			r.f.queueReservations.WithLabelValues(reservationResultSchedulerError).Inc()
			level.Error(logger).Log("msg", "unknown response status from the query-scheduler for queue reservation", "status", res.status)
			r.err = apierror.New(apierror.TypeInternal, "failed to enqueue request")
			r.errOutcome = "scheduler_error"
			return
		}

		break
	}

	r.err = apierror.New(apierror.TypeInternal, "failed to enqueue request")
	r.errOutcome = "reservation_lost"
}

// reserve sends one RESERVE on the first free stream that takes it. Like a plain sub-request, it
// waits for a stream until ctx is done. It returns an error only when ctx is done.
func (r *queueReservation) reserve(ctx context.Context, reservationID string) (reserveResult, error) {
	req := &reserveRequest{
		userID:        r.userID,
		reservationID: reservationID,
		slots:         r.slots,
		reply:         make(chan reserveResult, 1),
	}

	select {
	case <-ctx.Done():
		return reserveResult{}, context.Cause(ctx)
	case r.f.reserveCh <- req:
	}

	select {
	case res := <-req.reply:
		return res, nil
	case <-ctx.Done():
		// The query-scheduler can still grant the reservation. It ignores an abort that comes
		// before its reply, so abort only after the reply. A failed stream can follow a grant too.
		go func() {
			if res := <-req.reply; res.transportErr != nil || res.status == schedulerpb.OK {
				r.f.closeReservation(res.worker, reservationID, false)
			}
		}()
		return reserveResult{}, context.Cause(ctx)
	}
}

// enqueue sends freq as a sub-request of the reservation, once the reservation is granted. The
// sub-request does not retry on another query-scheduler, because the reservation is valid only on
// the query-scheduler that granted it.
func (r *queueReservation) enqueue(ctx context.Context, freq *frontendRequest) (chan<- cancelRequest, error) {
	// The reserved path does not retry, so a lost stream or reservation counts as reservation_lost.
	outcome := "reservation_lost"
	defer func() {
		r.f.enqueueRequests.WithLabelValues(outcome).Inc()
	}()

	select {
	case <-r.done:
	case <-ctx.Done():
		outcome = "cancelled"
		return nil, context.Cause(ctx)
	}

	// A select picks at random among ready cases, and the ParallelismLimiter can grant a slot after
	// ctx is done. Without this check, a cancelled sub-request can still send an ENQUEUE, while the
	// cancelled sub-requests in the queue hold all the reserved slots.
	if ctx.Err() != nil {
		outcome = "cancelled"
		return nil, context.Cause(ctx)
	}

	if r.err != nil {
		outcome = r.errOutcome
		return nil, r.err
	}

	freq.reservationID = r.reservationID
	w := r.worker

	select {
	case <-ctx.Done():
		outcome = "cancelled"
		return nil, context.Cause(ctx)
	case <-w.ctx.Done():
		r.abort()
		return nil, apierror.New(apierror.TypeInternal, "failed to enqueue request")
	case <-r.streamsGone:
		r.abort()
		return nil, apierror.New(apierror.TypeInternal, "failed to enqueue request")
	case w.reservedRequestsCh <- freq:
	}

	enqRes := <-freq.enqueue
	if enqRes.status == waitForResponse {
		outcome = "enqueued"
		return enqRes.cancelCh, nil
	}

	// Read before the abort below: a close that this query-frontend sent, or a loss of all streams,
	// also removes the reservation, so then the scheduler did not time it out.
	r.mu.Lock()
	closeSent := r.closeSent
	r.mu.Unlock()
	streamsLost := false
	select {
	case <-r.streamsGone:
		streamsLost = true
	default:
	}

	// The parent query fails, so the idle slots of the reservation are not needed any more.
	r.abort()

	// ctx can end after the check above, while the ENQUEUE is in flight.
	if ctx.Err() != nil {
		outcome = "cancelled"
		return nil, context.Cause(ctx)
	}

	switch enqRes.status {
	case failed:
		if enqRes.clientErr != nil {
			outcome = "invalid_request"
			return nil, httpgrpc.Errorf(http.StatusBadRequest, "failed to enqueue request: %s", enqRes.clientErr.Error())
		}
	case schedulerReturnedError:
		outcome = "scheduler_error"
		return nil, apierror.New(apierror.TypeInternal, enqRes.schedulerErr)
	case reservationNotFound:
		switch {
		case closeSent:
			outcome = "scheduler_error"
		case !streamsLost:
			outcome = "reservation_timed_out"
		}
		return nil, apierror.New(apierror.TypeInternal, enqRes.schedulerErr)
	case tooManyRequests:
		outcome = "too_many_requests"
	}

	// A reserved ENQUEUE cannot fail on the per-tenant limit, so a 429 is unexpected here and the
	// failure after a granted reservation is internal.
	return nil, apierror.New(apierror.TypeInternal, "failed to enqueue request")
}

// Close closes the granted reservation once: an abort when aborted is true, otherwise a
// COMPLETE_RESERVATION. When the reservation is not granted yet, run sends the close after the grant.
func (r *queueReservation) Close(aborted bool) {
	r.mu.Lock()
	r.closed = true
	r.closeAborted = aborted
	granted := r.worker != nil
	r.mu.Unlock()

	if granted {
		r.sendClose(aborted)
	}
}

// abort aborts the granted reservation, unless the query-frontend already sent its close.
func (r *queueReservation) abort() {
	r.sendClose(true)
}

func (r *queueReservation) sendClose(aborted bool) {
	r.mu.Lock()
	if r.closeSent || r.worker == nil {
		r.mu.Unlock()
		return
	}
	r.closeSent = true
	w, reservationID := r.worker, r.reservationID
	r.mu.Unlock()

	r.f.closeReservation(w, reservationID, !aborted)
}

// closeReservation sends a COMPLETE_RESERVATION, when complete is true, or an abort of the
// reservation to w. Like a CANCEL per query, it drops the message when the cancel channel is full.
// The query-scheduler then closes the reservation after -query-scheduler.queue-reservation-timeout.
func (f *Frontend) closeReservation(w *frontendSchedulerWorker, reservationID string, complete bool) {
	select {
	case w.cancelCh <- cancelRequest{reservationID: reservationID, complete: complete}:
	default:
		level.Warn(f.log).Log("msg", "failed to send queue reservation close to query-scheduler, queue full", "scheduler_address", w.schedulerAddr, "reservation_id", reservationID, "complete", complete)
	}
}
