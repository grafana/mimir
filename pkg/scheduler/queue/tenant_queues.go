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

type QuerierID string

type tenantRequest struct {
	tenantID TenantID
	req      Request
}

// queueBroker encapsulates access to tenant queues for pending requests
// and maintains consistency with the tenant-querier assignments
type queueBroker struct {
	tree Tree

	tenantQuerierAssignments *tenantQuerierAssignments

	maxTenantQueueSize               int
	additionalQueueDimensionsEnabled bool
	prioritizeQueryComponents        bool
}

func newQueueBroker(
	maxTenantQueueSize int,
	additionalQueueDimensionsEnabled bool,
	useMultiAlgoTreeQueue bool,
	forgetDelay time.Duration,
) *queueBroker {
	currentQuerier := QuerierID("")
	tqas := &tenantQuerierAssignments{
		queriersByID:       map[QuerierID]*querierConn{},
		querierIDsSorted:   nil,
		querierForgetDelay: forgetDelay,
		tenantIDOrder:      nil,
		tenantsByID:        map[TenantID]*queueTenant{},
		tenantQuerierIDs:   map[TenantID]map[QuerierID]struct{}{},
		tenantNodes:        map[string][]*Node{},
		currentQuerier:     currentQuerier,
		tenantOrderIndex:   localQueueIndex,
	}

	var tree Tree
	var err error
	if useMultiAlgoTreeQueue {
		tree, err = NewTree(
			tqas,               // root; QueuingAlgorithm selects tenants
			&roundRobinState{}, // tenant queues; QueuingAlgorithm selects query component
			&roundRobinState{}, // query components; QueuingAlgorithm selects query from local queue
		)
	} else {
		// by default, use the legacy tree queue
		tree = NewTreeQueue("root")
	}

	// An error building the tree is fatal; we must panic
	if err != nil {
		panic(fmt.Sprintf("error creating the tree queue: %v", err))
	}
	qb := &queueBroker{
		tree:                             tree,
		tenantQuerierAssignments:         tqas,
		maxTenantQueueSize:               maxTenantQueueSize,
		additionalQueueDimensionsEnabled: additionalQueueDimensionsEnabled,
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

	// TODO (casie): When deprecating TreeQueue, clean this up.
	// Technically, the MultiQueuingAlgorithmTreeQueue approach is adequate for both tree types, but we are temporarily
	// maintaining the legacy tree behavior as much as possible for stability reasons.
	if tq, ok := qb.tree.(*TreeQueue); ok {
		if tenantQueueNode := tq.getNode(queuePath[:1]); tenantQueueNode != nil {
			if tenantQueueNode.ItemCount()+1 > qb.maxTenantQueueSize {
				return ErrTooManyRequests
			}
		}
	} else if _, ok := qb.tree.(*MultiQueuingAlgorithmTreeQueue); ok {
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
	if qb.additionalQueueDimensionsEnabled {
		if schedulerRequest, ok := request.req.(*SchedulerRequest); ok {
			if qb.prioritizeQueryComponents {
				return append(schedulerRequest.AdditionalQueueDimensions, string(request.tenantID)), nil
			}
			return append(QueuePath{string(request.tenantID)}, schedulerRequest.AdditionalQueueDimensions...), nil
		}
	}

	// else request.req is a frontend/v1.request, or additional queue dimensions are disabled
	return QueuePath{string(request.tenantID)}, nil
}

func (qb *queueBroker) dequeueRequestForQuerier(
	lastTenantIndex int,
	querierID QuerierID,
) (
	*tenantRequest,
	*queueTenant,
	int,
	error,
) {
	// check if querier is registered and is not shutting down
	if q := qb.tenantQuerierAssignments.queriersByID[querierID]; q == nil || q.shuttingDown {
		return nil, nil, qb.tenantQuerierAssignments.tenantOrderIndex, ErrQuerierShuttingDown
	}

	var queuePath QueuePath
	var queueElement any
	if tq, ok := qb.tree.(*TreeQueue); ok {
		tenant, tenantIndex, err := qb.tenantQuerierAssignments.getNextTenantForQuerier(lastTenantIndex, querierID)
		if tenant == nil || err != nil {
			return nil, tenant, tenantIndex, err
		}
		qb.tenantQuerierAssignments.tenantOrderIndex = tenantIndex
		// We can manually build queuePath here because TreeQueue only supports one tree structure ordering:
		// root --> tenant --> (optional: query dimensions)
		queuePath = QueuePath{string(tenant.tenantID)}
		queueElement = tq.DequeueByPath(queuePath)
	} else if itq, ok := qb.tree.(*MultiQueuingAlgorithmTreeQueue); ok {
		qb.tenantQuerierAssignments.updateQueuingAlgorithmState(querierID, lastTenantIndex)

		queuePath, queueElement = itq.Dequeue()
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

	// TODO (casie): When deprecating TreeQueue, clean this up.
	// This cannot be handled by the Tree interface without defining some other, more expansive interfaces
	// between the legacy and integrated tree queues, which would be more overhead than it's worth, given
	// that we will eventually retire the legacy tree queue.
	if tq, ok := qb.tree.(*TreeQueue); ok {
		queueNodeAfterDequeue := tq.getNode(queuePath)
		if queueNodeAfterDequeue == nil {
			// queue node was deleted due to being empty after dequeue
			qb.tenantQuerierAssignments.removeTenant(tenant.tenantID)
		}
	} else if itq, ok := qb.tree.(*MultiQueuingAlgorithmTreeQueue); ok {
		queueNodeAfterDequeue := itq.GetNode(queuePath)
		if queueNodeAfterDequeue == nil && len(qb.tenantQuerierAssignments.tenantNodes[string(tenantID)]) == 0 {
			// queue node was deleted due to being empty after dequeue
			qb.tenantQuerierAssignments.removeTenant(tenantID)
		}
	}
	return request, tenant, qb.tenantQuerierAssignments.tenantOrderIndex, nil
}

func (qb *queueBroker) addQuerierConnection(querierID QuerierID) (resharded bool) {
	return qb.tenantQuerierAssignments.addQuerierConnection(querierID)
}

func (qb *queueBroker) removeQuerierConnection(querierID QuerierID, now time.Time) (resharded bool) {
	return qb.tenantQuerierAssignments.removeQuerierConnection(querierID, now)
}

func (qb *queueBroker) notifyQuerierShutdown(querierID QuerierID) (resharded bool) {
	return qb.tenantQuerierAssignments.notifyQuerierShutdown(querierID)
}

func (qb *queueBroker) forgetDisconnectedQueriers(now time.Time) (resharded bool) {
	return qb.tenantQuerierAssignments.forgetDisconnectedQueriers(now)
}
