// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/user_queues.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queue

import (
	"fmt"
	"time"
)

type TenantID string

const emptyTenantID = TenantID("")
const unknownQueueDimension = "unknown"

type QuerierID string

type tenantRequest struct {
	tenantID TenantID
	req      QueryRequest
}

// queueBroker encapsulates access to tenant queues for pending requests
// and maintains consistency with the tenant-querier assignments
type queueBroker struct {
	tree Tree

	tenantQuerierAssignments *tenantQuerierAssignments

	maxTenantQueueSize        int
	prioritizeQueryComponents bool
}

func newQueueBroker(
	maxTenantQueueSize int,
	prioritizeQueryComponents bool,
	forgetDelay time.Duration,
) *queueBroker {
	tqas := newTenantQuerierAssignments(forgetDelay)
	var tree Tree
	var err error
	var algos []QueuingAlgorithm
	if prioritizeQueryComponents {
		algos = []QueuingAlgorithm{
			&roundRobinState{}, // root; QueuingAlgorithm selects query component
			tqas,               // query components; QueuingAlgorithm selects tenants
			&roundRobinState{}, // tenant queues; QueuingAlgorithm selects from local queue

		}
	} else {
		algos = []QueuingAlgorithm{
			tqas,               // root; QueuingAlgorithm selects tenants
			&roundRobinState{}, // tenant queues; QueuingAlgorithm selects query component
			&roundRobinState{}, // query components; QueuingAlgorithm selects query from local queue
		}
	}
	tree, err = NewTree(algos...)

	// An error building the tree is fatal; we must panic
	if err != nil {
		panic(fmt.Sprintf("error creating the tree queue: %v", err))
	}
	qb := &queueBroker{
		tree:                      tree,
		tenantQuerierAssignments:  tqas,
		maxTenantQueueSize:        maxTenantQueueSize,
		prioritizeQueryComponents: prioritizeQueryComponents,
	}

	return qb
}

func (qb *queueBroker) isEmpty() bool {
	return qb.tree.IsEmpty()
}

// enqueueRequestBack is the standard interface to enqueue requests for dispatch to queriers.
//
// Tenants and tenant-querier shuffle sharding relationships are managed internally as needed.
func (qb *queueBroker) enqueueRequestBack(request *tenantRequest, tenantMaxQueriers int) error {
	err := qb.tenantQuerierAssignments.createOrUpdateTenant(request.tenantID, tenantMaxQueriers)
	if err != nil {
		return err
	}

	queuePath, err := qb.makeQueuePath(request)
	if err != nil {
		return err
	}

	if _, ok := qb.tree.(*MultiQueuingAlgorithmTreeQueue); ok {
		itemCount := 0
		for _, tenantNode := range qb.tenantQuerierAssignments.tenantNodes[string(request.tenantID)] {
			itemCount += tenantNode.ItemCount()
		}
		if itemCount+1 > qb.maxTenantQueueSize {
			return ErrTooManyRequests
		}
	}

	err = qb.tree.EnqueueBackByPath(queuePath, request)
	return err
}

// enqueueRequestFront should only be used for re-enqueueing previously dequeued requests
// to the front of the queue when there was a failure in dispatching to a querier.
//
// max tenant queue size checks are skipped even though queue size violations
// are not expected to occur when re-enqueuing a previously dequeued request.
func (qb *queueBroker) enqueueRequestFront(request *tenantRequest, tenantMaxQueriers int) error {
	err := qb.tenantQuerierAssignments.createOrUpdateTenant(request.tenantID, tenantMaxQueriers)
	if err != nil {
		return err
	}

	queuePath, err := qb.makeQueuePath(request)
	if err != nil {
		return err
	}
	return qb.tree.EnqueueFrontByPath(queuePath, request)
}

func (qb *queueBroker) makeQueuePath(request *tenantRequest) (QueuePath, error) {
	// some requests may not be type asserted to a schedulerRequest; in this case,
	// they should also be queued as "unknown" query components
	queryComponent := unknownQueueDimension
	if schedulerRequest, ok := request.req.(*SchedulerRequest); ok {
		queryComponent = schedulerRequest.ExpectedQueryComponentName()
	}
	if qb.prioritizeQueryComponents {
		return append([]string{queryComponent}, string(request.tenantID)), nil
	}
	return append(QueuePath{string(request.tenantID)}, queryComponent), nil
}

func (qb *queueBroker) dequeueRequestForQuerier(
	dequeueReq *QuerierWorkerDequeueRequest,
) (
	*tenantRequest,
	*queueTenant,
	int,
	error,
) {
	// check if querier is registered and is not shutting down
	if q := qb.tenantQuerierAssignments.queriersByID[dequeueReq.QuerierID]; q == nil || q.shuttingDown {
		return nil, nil, qb.tenantQuerierAssignments.tenantOrderIndex, ErrQuerierShuttingDown
	}

	var queuePath QueuePath
	var queueElement any
	if itq, ok := qb.tree.(*MultiQueuingAlgorithmTreeQueue); ok {
		queuePath, queueElement = itq.Dequeue(
			&DequeueArgs{
				querierID:       dequeueReq.QuerierID,
				workerID:        dequeueReq.WorkerID,
				lastTenantIndex: dequeueReq.lastTenantIndex.last,
			})
	}

	if queueElement == nil {
		return nil, nil, qb.tenantQuerierAssignments.tenantOrderIndex, nil
	}

	var request *tenantRequest
	var tenantID TenantID

	// re-casting to same type it was enqueued as; panic would indicate a bug
	request = queueElement.(*tenantRequest)
	tenantID = request.tenantID

	var tenant *queueTenant
	if tenantID != "" {
		tenant = qb.tenantQuerierAssignments.tenantsByID[tenantID]
	}

	if itq, ok := qb.tree.(*MultiQueuingAlgorithmTreeQueue); ok {
		queueNodeAfterDequeue := itq.GetNode(queuePath)
		if queueNodeAfterDequeue == nil && len(qb.tenantQuerierAssignments.tenantNodes[string(tenantID)]) == 0 {
			// queue node was deleted due to being empty after dequeue
			qb.tenantQuerierAssignments.removeTenant(tenantID)
		}
	}
	return request, tenant, qb.tenantQuerierAssignments.tenantOrderIndex, nil
}

// below methods simply pass through to the queueBroker's tenantQuerierAssignments; this layering could be skipped
// but there is no reason to make consumers know that they need to call through to the tenantQuerierAssignments.

func (qb *queueBroker) addQuerierWorkerConn(conn *QuerierWorkerConn) (resharded bool) {
	return qb.tenantQuerierAssignments.addQuerierWorkerConn(conn)
}

func (qb *queueBroker) removeQuerierWorkerConn(conn *QuerierWorkerConn, now time.Time) (resharded bool) {
	return qb.tenantQuerierAssignments.removeQuerierWorkerConn(conn, now)
}

func (qb *queueBroker) notifyQuerierShutdown(querierID QuerierID) (resharded bool) {
	return qb.tenantQuerierAssignments.notifyQuerierShutdown(querierID)
}

func (qb *queueBroker) forgetDisconnectedQueriers(now time.Time) (resharded bool) {
	return qb.tenantQuerierAssignments.forgetDisconnectedQueriers(now)
}
