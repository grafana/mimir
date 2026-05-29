// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/scheduler/queue"
	"github.com/grafana/mimir/pkg/util/validation"
)

// schedulerQueue wraps the generic queue.RequestQueue with scheduler-flavored
// ergonomics: it owns dimension extraction and per-tenant max-queriers lookup
// so call sites in the scheduler don't have to thread either value through,
// and bundles in the QueryComponentUtilization tracking sidecar.
type schedulerQueue struct {
	services.Service

	queue                     *queue.RequestQueue
	queryComponentUtilization *queue.QueryComponentUtilization
	limits                    Limits
}

func newSchedulerQueue(
	cfg Config,
	limits Limits,
	logger log.Logger,
	queueLength *prometheus.GaugeVec,
	discardedRequests *prometheus.CounterVec,
	enqueueDuration prometheus.Histogram,
	querierInflightRequestsMetric *prometheus.SummaryVec,
) (*schedulerQueue, error) {
	q, err := queue.NewRequestQueue(
		logger,
		cfg.MaxOutstandingPerTenant,
		cfg.QuerierForgetDelay,
		queueLength,
		discardedRequests,
		enqueueDuration,
	)
	if err != nil {
		return nil, err
	}

	qcu, err := queue.NewQueryComponentUtilization(querierInflightRequestsMetric)
	if err != nil {
		return nil, err
	}

	sq := &schedulerQueue{
		queue:                     q,
		queryComponentUtilization: qcu,
		limits:                    limits,
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
func (sq *schedulerQueue) Enqueue(req *queue.SchedulerRequest, successFn func()) error {
	tenantIDs, err := tenant.TenantIDsFromOrgID(req.UserID)
	if err != nil {
		return err
	}
	maxQueriers := validation.SmallestPositiveNonZeroIntPerTenant(tenantIDs, sq.limits.MaxQueriersPerUser)
	return sq.queue.SubmitRequestToEnqueue(req.UserID, req, req.ExpectedQueryComponentName(), maxQueriers, successFn)
}

func (sq *schedulerQueue) AwaitRegisterQuerierWorkerConn(conn *queue.QuerierWorkerConn) error {
	return sq.queue.AwaitRegisterQuerierWorkerConn(conn)
}

func (sq *schedulerQueue) SubmitUnregisterQuerierWorkerConn(conn *queue.QuerierWorkerConn) {
	sq.queue.SubmitUnregisterQuerierWorkerConn(conn)
}

func (sq *schedulerQueue) AwaitRequestForQuerier(req *queue.QuerierWorkerDequeueRequest) (queue.QueryRequest, queue.TenantIndex, error) {
	return sq.queue.AwaitRequestForQuerier(req)
}

func (sq *schedulerQueue) SubmitNotifyQuerierShutdown(ctx context.Context, querierID string) {
	sq.queue.SubmitNotifyQuerierShutdown(ctx, querierID)
}

func (sq *schedulerQueue) GetConnectedQuerierWorkersMetric() float64 {
	return sq.queue.GetConnectedQuerierWorkersMetric()
}

func (sq *schedulerQueue) IsEmpty() bool {
	return sq.queue.IsEmpty()
}

// MarkRequestSent reports the request was forwarded to a querier so the
// QueryComponentUtilization sidecar starts counting it as inflight.
func (sq *schedulerQueue) MarkRequestSent(req *queue.SchedulerRequest) {
	sq.queryComponentUtilization.MarkRequestSent(req)
}

// MarkRequestCompleted reports the request was completed or cancelled so the
// QueryComponentUtilization sidecar stops counting it as inflight.
func (sq *schedulerQueue) MarkRequestCompleted(req *queue.SchedulerRequest) {
	sq.queryComponentUtilization.MarkRequestCompleted(req)
}
