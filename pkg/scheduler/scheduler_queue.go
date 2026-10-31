// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/queue"
	"github.com/grafana/mimir/pkg/util/validation"
)

// schedulerQueue wraps the generic queue.Queue with scheduler-specific queue logic:
//   - the first queue dimension is the query component,
//     determined from the annotation that the query-frontend attaches to the request.
//   - the per-tenant max-queriers limit is looked up from the configured Limits.
//   - the QueryComponentUtilization tracker, and its periodic inflight observation,
//     live here rather than in the generic queue.
type schedulerQueue struct {
	services.Service

	queue                     *queue.Queue
	queryComponentUtilization *QueryComponentUtilization
	limits                    Limits

	subservicesWatcher *services.FailureWatcher
}

func newSchedulerQueue(
	cfg Config,
	limits Limits,
	logger log.Logger,
	queueLength *prometheus.GaugeVec,
	maxQueueLength *queue.MaxQueueLengthGauge,
	discardedRequests *prometheus.CounterVec,
	enqueueDuration prometheus.Histogram,
	querierInflightRequestsMetric *prometheus.SummaryVec,
	reservationMetrics *queue.ReservationMetrics,
) (*schedulerQueue, error) {
	q, err := queue.New(
		logger,
		cfg.MaxOutstandingPerTenant,
		cfg.QuerierForgetDelay,
		queueLength,
		maxQueueLength,
		discardedRequests,
		enqueueDuration,
		reservationMetrics,
	)
	if err != nil {
		return nil, err
	}

	qcu, err := NewQueryComponentUtilization(querierInflightRequestsMetric)
	if err != nil {
		return nil, err
	}

	sq := &schedulerQueue{
		queue:                     q,
		queryComponentUtilization: qcu,
		limits:                    limits,
		subservicesWatcher:        services.NewFailureWatcher(),
	}
	sq.Service = services.NewBasicService(sq.starting, sq.running, sq.stopping).WithName("scheduler queue")
	return sq, nil
}

func (sq *schedulerQueue) starting(ctx context.Context) error {
	if err := services.StartAndAwaitRunning(ctx, sq.queue); err != nil {
		// BasicService will not call our stopping() when starting() returns an error,
		// so we must clean up the inner queue ourselves if it managed to reach Running
		// before ctx was cancelled.
		_ = services.StopAndAwaitTerminated(context.Background(), sq.queue)
		return err
	}
	sq.subservicesWatcher.WatchService(sq.queue)
	return nil
}

func (sq *schedulerQueue) running(ctx context.Context) error {
	// Observe inflight requests on a regular tick to approximate max-inflight
	// percentiles even when no new queries are arriving. Matches the cadence
	// used by the scheduler's own inflight observation.
	inflightRequestsTicker := time.NewTicker(250 * time.Millisecond)
	defer inflightRequestsTicker.Stop()

	for {
		select {
		case <-inflightRequestsTicker.C:
			sq.queryComponentUtilization.ObserveInflightRequests()
		case err := <-sq.subservicesWatcher.Chan():
			return fmt.Errorf("scheduler queue subservice failed: %w", err)
		case <-ctx.Done():
			return nil
		}
	}
}

func (sq *schedulerQueue) stopping(_ error) error {
	return services.StopAndAwaitTerminated(context.Background(), sq.queue)
}

// Enqueue submits a SchedulerRequest, deriving the queue dimension from the
// request itself and the per-tenant max-queriers from the configured Limits.
func (sq *schedulerQueue) Enqueue(req *SchedulerRequest, successFn func()) error {
	maxQueriers, err := sq.maxQueriers(req.UserID)
	if err != nil {
		return err
	}
	return sq.queue.SubmitItemToEnqueue(req.UserID, req, req.ExpectedQueryComponentName(), maxQueriers, successFn)
}

// EnqueueIntoReservation is Enqueue for a sub-request of the reservation at key.
func (sq *schedulerQueue) EnqueueIntoReservation(key queue.ReservationKey, req *SchedulerRequest, now time.Time, successFn func(*queue.Reservation)) error {
	maxQueriers, err := sq.maxQueriers(req.UserID)
	if err != nil {
		return err
	}
	return sq.queue.SubmitItemIntoReservation(key, req.UserID, req, req.ExpectedQueryComponentName(), maxQueriers, now, successFn)
}

func (sq *schedulerQueue) maxQueriers(userID string) (int, error) {
	tenantIDs, err := tenant.TenantIDsFromOrgID(userID)
	if err != nil {
		return 0, err
	}
	return validation.SmallestPositiveNonZeroIntPerTenant(tenantIDs, sq.limits.MaxQueriersPerUser), nil
}

// OpenReservation opens a reservation at key for tenantID. It rejects an invalid tenant ID first,
// because the queue uses tenantID as a metric label value.
func (sq *schedulerQueue) OpenReservation(key queue.ReservationKey, tenantID string, slots int, now time.Time) error {
	if _, err := tenant.TenantIDsFromOrgID(tenantID); err != nil {
		return err
	}
	return sq.queue.SubmitOpenReservation(key, tenantID, slots, now)
}

// CompleteReservation closes the open reservation at key after its last sub-request. It returns nil
// when no reservation is open at key.
func (sq *schedulerQueue) CompleteReservation(key queue.ReservationKey) (*queue.ClosedReservation, error) {
	return sq.queue.SubmitCompleteReservation(key)
}

// AbortReservation aborts the open reservation at key. It returns nil when no reservation is open
// at key.
func (sq *schedulerQueue) AbortReservation(key queue.ReservationKey) (*queue.ClosedReservation, error) {
	return sq.queue.SubmitAbortReservation(key)
}

// CloseReservationsOfOwner closes every open reservation of owner.
func (sq *schedulerQueue) CloseReservationsOfOwner(owner string) ([]queue.ClosedReservation, error) {
	return sq.queue.SubmitCloseReservationsOfOwner(owner)
}

// ReapReservations closes the open reservations that are idle for at least timeout.
func (sq *schedulerQueue) ReapReservations(now time.Time, timeout time.Duration) ([]queue.ClosedReservation, error) {
	return sq.queue.SubmitReapReservations(now, timeout)
}

func (sq *schedulerQueue) AwaitRegisterQuerierWorkerConn(conn *queue.ConsumerWorkerConn) error {
	return sq.queue.AwaitRegisterConsumerWorkerConn(conn)
}

func (sq *schedulerQueue) SubmitUnregisterQuerierWorkerConn(conn *queue.ConsumerWorkerConn) {
	sq.queue.SubmitUnregisterConsumerWorkerConn(conn)
}

func (sq *schedulerQueue) AwaitRequestForQuerier(req *queue.ConsumerWorkerDequeueRequest) (queue.Item, queue.TenantIndex, error) {
	return sq.queue.AwaitItemForConsumer(req)
}

func (sq *schedulerQueue) SubmitNotifyQuerierShutdown(ctx context.Context, querierID string) {
	sq.queue.SubmitNotifyConsumerShutdown(ctx, querierID)
}

func (sq *schedulerQueue) GetConnectedQuerierWorkersMetric() float64 {
	return sq.queue.GetConnectedConsumerWorkersMetric()
}

func (sq *schedulerQueue) IsEmpty() bool {
	return sq.queue.IsEmpty()
}

// MarkRequestSent reports the request was forwarded to a querier so the
// QueryComponentUtilization sidecar starts counting it as inflight.
func (sq *schedulerQueue) MarkRequestSent(req *SchedulerRequest) {
	sq.queryComponentUtilization.MarkRequestSent(req)
}

// MarkRequestCompleted reports the request was completed or cancelled so the
// QueryComponentUtilization sidecar stops counting it as inflight.
func (sq *schedulerQueue) MarkRequestCompleted(req *SchedulerRequest) {
	sq.queryComponentUtilization.MarkRequestCompleted(req)
}
