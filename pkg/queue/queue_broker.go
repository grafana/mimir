// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/user_queues.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queue

import (
	"fmt"
	"time"

	"github.com/grafana/mimir/pkg/queue/tree"
)

// UnknownDimension is used internally when a caller submits a request with an
// empty queueDimension string. Callers (e.g. the scheduler) define their own
// dimension vocabularies; the queue treats them as opaque routing labels. It is
// exported so callers that need the same "no dimension" bucket the queue uses
// (notably the scheduler's QueryComponentUtilization) can reference it directly
// rather than redeclaring the literal.
const UnknownDimension = "unknown"

// tenantItem is the queue's internal wrapper for an enqueued Item.
// queueDimension is the first-layer queue dimension to enqueue under (e.g. a
// query component name); the tenantID is appended internally to form the full
// queue path.
type tenantItem struct {
	tenantID       string
	queueDimension string
	item           Item
}

// queueBroker encapsulates access to the Tree queue for pending requests, and brokers logic dependencies between
// querier connections and tenant-querier assignments (e.g., assigning newly-connected queriers to tenants, or
// reshuffling queriers when a querier has disconnected).
type queueBroker struct {
	tree tree.Tree

	tenantQuerierAssignments *tenantQuerierShards
	querierConnections       *querierConnections

	maxTenantQueueSize int
}

func newQueueBroker(
	maxTenantQueueSize int,
	forgetDelay time.Duration,
) *queueBroker {
	qc := newQuerierConnections(forgetDelay)
	tqas := newTenantQuerierAssignments()
	var treeQueue tree.Tree
	var err error
	algos := []tree.QueuingAlgorithm{
		tree.NewQuerierWorkerQueuePriorityAlgo(), // root; algorithm selects query component based on worker ID
		tqas.queuingAlgorithm,                    // query components; algorithm selects tenants
	}
	treeQueue, err = tree.NewTree(algos...)

	// An error building the tree is fatal; we must panic
	if err != nil {
		panic(fmt.Sprintf("error creating the tree queue: %v", err))
	}
	qb := &queueBroker{
		tree:                     treeQueue,
		querierConnections:       qc,
		tenantQuerierAssignments: tqas,
		maxTenantQueueSize:       maxTenantQueueSize,
	}

	return qb
}

func (qb *queueBroker) isEmpty() bool {
	return qb.tree.IsEmpty()
}

func (qb *queueBroker) itemCount() int {
	return qb.tree.ItemCount()
}

// enqueueItemBack is the standard interface to enqueue items for dispatch to queriers.
//
// Tenants and tenant-querier shuffle sharding relationships are managed internally as needed.
func (qb *queueBroker) enqueueItemBack(item *tenantItem, tenantMaxQueriers int) error {
	err := qb.tenantQuerierAssignments.createOrUpdateTenant(item.tenantID, tenantMaxQueriers)
	if err != nil {
		return err
	}

	tenantQueueSize := qb.tenantQuerierAssignments.queuingAlgorithm.TotalQueueSizeForTenant(item.tenantID)
	if tenantQueueSize+1 > qb.maxTenantQueueSize {
		return ErrTooManyRequests
	}

	return qb.tree.EnqueueBackByPath(qb.queuePath(item), item)
}

// enqueueItemFront should only be used for re-enqueueing previously dequeued items
// to the front of the queue when there was a failure in dispatching to a querier.
//
// max tenant queue size checks are skipped even though queue size violations
// are not expected to occur when re-enqueuing a previously dequeued item.
func (qb *queueBroker) enqueueItemFront(item *tenantItem, tenantMaxQueriers int) error {
	err := qb.tenantQuerierAssignments.createOrUpdateTenant(item.tenantID, tenantMaxQueriers)
	if err != nil {
		return err
	}
	return qb.tree.EnqueueFrontByPath(qb.queuePath(item), item)
}

// queuePath returns the two-level queue path for an enqueued tenantItem:
// the first-layer queue dimension (typically a query component name)
// supplied by the caller, followed by the tenantID.
func (qb *queueBroker) queuePath(item *tenantItem) tree.QueuePath {
	dim := item.queueDimension
	if dim == "" {
		dim = UnknownDimension
	}
	return tree.QueuePath{dim, item.tenantID}
}

func (qb *queueBroker) dequeueItemForQuerier(
	dequeueReq *QuerierWorkerDequeueRequest,
) (
	*tenantItem,
	*queueTenant,
	int,
	error,
) {
	// check if querier is registered and is not shutting down
	if !qb.querierConnections.querierIsAvailable(dequeueReq.QuerierID) {
		return nil, nil, qb.tenantQuerierAssignments.queuingAlgorithm.TenantOrderIndex(), ErrQuerierShuttingDown
	}

	// check if the specific querier-worker connection was deregistered since its dequeue request was submitted
	if dequeueReq.WorkerID == unregisteredWorkerID {
		return nil, nil, qb.tenantQuerierAssignments.queuingAlgorithm.TenantOrderIndex(), ErrQuerierWorkerDisconnected
	}

	var queuePath tree.QueuePath
	var queueElement any
	queuePath, queueElement = qb.tree.Dequeue(
		&tree.DequeueArgs{
			QuerierID:       dequeueReq.QuerierID,
			WorkerID:        dequeueReq.WorkerID,
			LastTenantIndex: dequeueReq.lastTenantIndex.last,
		})

	if queueElement == nil {
		return nil, nil, qb.tenantQuerierAssignments.queuingAlgorithm.TenantOrderIndex(), nil
	}

	// re-casting to same type it was enqueued as; panic would indicate a bug
	item := queueElement.(*tenantItem)
	tenantID := item.tenantID

	var tenant *queueTenant
	if tenantID != "" {
		tenant = qb.tenantQuerierAssignments.tenantsByID[tenantID]
	}

	queueNodeAfterDequeue := qb.tree.GetNode(queuePath)
	if queueNodeAfterDequeue == nil && qb.tenantQuerierAssignments.queuingAlgorithm.TotalQueueSizeForTenant(tenantID) == 0 {
		// queue node was deleted due to being empty after dequeue, and there are no remaining queue items for this tenant
		qb.tenantQuerierAssignments.removeTenant(tenantID)
	}

	return item, tenant, qb.tenantQuerierAssignments.queuingAlgorithm.TenantOrderIndex(), nil
}

// below methods simply pass through to the queueBroker's tenantQuerierShards; this layering could be skipped
// but there is no reason to make consumers know that they need to call through to the tenantQuerierShards.

func (qb *queueBroker) addQuerierWorkerConn(conn *QuerierWorkerConn) (resharded bool) {
	// if conn is for a new querier, we need to recompute tenant querier relationship; otherwise, we don't reshard
	if addQuerier := qb.querierConnections.addQuerierWorkerConn(conn); addQuerier {
		qb.tenantQuerierAssignments.addQuerier(conn.QuerierID)
		return qb.tenantQuerierAssignments.recomputeTenantQueriers()
	}
	return false
}

func (qb *queueBroker) removeQuerierWorkerConn(conn *QuerierWorkerConn, now time.Time) (resharded bool) {
	// if we're removing the last active connection for the querier, the querier may need to be removed
	if removedQuerier := qb.querierConnections.removeQuerierWorkerConn(conn, now); removedQuerier {
		return qb.tenantQuerierAssignments.removeQueriers(conn.QuerierID)
	}
	return false
}

// notifyQuerierShutdown handles a graceful shutdown notification from a querier.
// Returns true if tenant-querier reshard was triggered.
func (qb *queueBroker) notifyQuerierShutdown(querierID string) (resharded bool) {
	if removedQuerier := qb.querierConnections.shutdownQuerier(querierID); removedQuerier {
		return qb.tenantQuerierAssignments.removeQueriers(querierID)
	}
	return false
}

// forgetDisconnectedQueriers removes all queriers which have had zero connections for longer than the forget delay.
// Returns true if tenant-querier reshard was triggered.
func (qb *queueBroker) forgetDisconnectedQueriers(now time.Time) (resharded bool) {
	return qb.tenantQuerierAssignments.removeQueriers(qb.querierConnections.removeForgettableQueriers(now)...)
}
