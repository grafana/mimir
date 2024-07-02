// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"math"

	"github.com/pkg/errors"
)

type QueryComponentUtilizationTriggerCheck interface {
	TriggerThresholdCheck() bool
}

type QueryComponentUtilizationTriggerCheckByQueueLenAndWaitingConns struct {
	waitingWorkers   int
	queueLen         int
	queueLenMultiple int
}

func NewQueryComponentUtilizationTriggerCheckByQueueLenAndWaitingConns(queueLenMultiple int) *QueryComponentUtilizationTriggerCheckByQueueLenAndWaitingConns {
	return &QueryComponentUtilizationTriggerCheckByQueueLenAndWaitingConns{
		waitingWorkers:   0,
		queueLen:         0,
		queueLenMultiple: 1,
	}
}

func (tc *QueryComponentUtilizationTriggerCheckByQueueLenAndWaitingConns) TriggerThresholdCheck() bool {
	if tc.waitingWorkers > (tc.queueLen * tc.queueLenMultiple) {
		// excess querier-worker capacity; no need to reserve any for now
		return false
	}
	return true
}

type QueryComponentUtilizationCheckThreshold interface {
	ExceedsThresholdForComponent(componentName string) (bool, QueryComponent)
}

// DefaultReservedQueryComponentCapacity reserves 1 / 3 of querier-worker connections
// for the query component utilizing fewer of the available connections.
// Chosen to represent an even balance between the three possible combinations of query components:
// ingesters only, store-gateways only, or both ingesters and store-gateways.
const DefaultReservedQueryComponentCapacity = 0.33

// MaxReservedQueryComponentCapacity is an exclusive upper bound on the targetReservedCapacity.
// The threshold for a QueryComponent's utilization of querier-worker connections
// can only be exceeded by one QueryComponent at a time as long as targetReservedCapacity is < 0.5.
// Therefore, one of the components will always be given the OK to dequeue queries for.
const MaxReservedQueryComponentCapacity = 0.5

type queryComponentUtilizationReserveConnections struct {
	utilization *QueryComponentUtilization
	// targetReservedCapacity sets the portion of querier-worker connections we attempt to reserve
	// for queries to the less-utilized query component when the query queue becomes backlogged.
	targetReservedCapacity float64
	connectedWorkers       int
}

func NewQueryComponentUtilizationReserveConnections(
	utilization *QueryComponentUtilization,
	targetReservedCapacity float64,
) (QueryComponentUtilizationCheckThreshold, error) {
	if targetReservedCapacity >= MaxReservedQueryComponentCapacity {
		return nil, errors.New("invalid targetReservedCapacity")
	}

	return &queryComponentUtilizationReserveConnections{
		utilization:            utilization,
		targetReservedCapacity: targetReservedCapacity,
		connectedWorkers:       0,
	}, nil
}

// ExceedsThresholdForComponent checks whether a query component has exceeded the capacity utilization threshold.
// This enables the dequeuing algorithm to skip requests for a query component experiencing heavy load,
// reserving querier-worker connection capacity to continue servicing requests for the other component.
// If there are no requests in queue for the other, less-utilized component, the dequeuing algorithm may choose
// to continue servicing requests for the component which has exceeded the reserved capacity threshold.
//
// Capacity utilization for a QueryComponent is defined by the portion of the querier-worker connections
// which are currently in flight processing a query which requires that QueryComponent.
//
// The component name can indicate the usage of one or both of ingesters and store-gateways.
// If both ingesters and store-gateways will be used, this method will flag the threshold as exceeded
// if either ingesters or store-gateways are currently in excess of the reserved capacity.
//
// Capacity reservation only occurs when the queue backlogged, where backlogged is defined as
// (length of the query queue) >= (number of querier-worker connections waiting for a query).
//
// A QueryComponent's utilization is allowed to exceed the reserved capacity when the queue is not backlogged.
// If an influx of queries then creates a backlog, this method will indicate to skip queries for the component.
// As the inflight queries complete or fail, the component's utilization will naturally decrease.
// This method will continue to indicate to skip queries for the component until it is back under the threshold.
func (qcurc *queryComponentUtilizationReserveConnections) ExceedsThresholdForComponent(name string) (bool, QueryComponent) {
	if qcurc.connectedWorkers <= 1 {
		// corner case; cannot reserve capacity with only one worker available
		return false, ""
	}

	// allow the functionality to be turned off via setting targetReservedCapacity to 0
	minReservedConnections := 0
	if qcurc.targetReservedCapacity > 0 {
		// reserve at least one connection in case (connected workers) * (reserved capacity) is less than one
		minReservedConnections = int(
			math.Ceil(
				math.Max(qcurc.targetReservedCapacity*float64(qcurc.connectedWorkers), 1),
			),
		)
	}

	isIngester, isStoreGateway := queryComponentFlags(name)
	ingesterInflightRequests, storeGatewayInflightRequests := qcurc.utilization.GetInflightRequests()
	if isIngester {
		if qcurc.connectedWorkers-ingesterInflightRequests <= minReservedConnections {
			return true, Ingester
		}
	}
	if isStoreGateway {
		if qcurc.connectedWorkers-storeGatewayInflightRequests <= minReservedConnections {
			return true, StoreGateway
		}
	}
	return false, ""
}

type queryComponentUtilizationDequeueSkipOverThreshold struct {
	queryComponentUtilizationTriggerCheck   QueryComponentUtilizationTriggerCheck
	queryComponentUtilizationCheckThreshold QueryComponentUtilizationCheckThreshold
	currentNodeOrderIndex                   int
	nodeOrder                               []string
	nodesChecked                            int
	nodesSkippedIndexOrder                  []int

	// TODO implement global state to only delete nodes from rotation when all corresponding nodes are deleted from the tree
	//queueNodeCounts                    map[string]int
}

func (qcud *queryComponentUtilizationDequeueSkipOverThreshold) incrementWrapIndex() {
	qcud.currentNodeOrderIndex++
	if qcud.currentNodeOrderIndex >= len(qcud.nodeOrder) {
		qcud.currentNodeOrderIndex = 0
	}
}

func (qcud *queryComponentUtilizationDequeueSkipOverThreshold) checkedAllNodes() bool {
	return qcud.nodesChecked == len(qcud.nodeOrder)+1 && len(qcud.nodesSkippedIndexOrder) == 0

}

func (qcud *queryComponentUtilizationDequeueSkipOverThreshold) addChildNode(parent, child *Node) {
	// add childNode to n.queueMap
	parent.queueMap[child.Name()] = child

	if qcud.currentNodeOrderIndex == localQueueIndex {
		// special case; cannot slice into nodeOrder with index -1
		// place at end of slice, which is the last slot before the local queue slot
		qcud.nodeOrder = append(qcud.nodeOrder, child.Name())
	} else {
		// insert into the order behind current child queue index
		// to prevent the possibility of new nodes continually jumping the line
		qcud.nodeOrder = append(
			qcud.nodeOrder[:qcud.currentNodeOrderIndex],
			append(
				[]string{child.Name()},
				qcud.nodeOrder[qcud.currentNodeOrderIndex:]...,
			)...,
		)
		// update current child queue index to its new place in the expanded slice
		qcud.incrementWrapIndex()
	}
}

func (qcud *queryComponentUtilizationDequeueSkipOverThreshold) dequeueSelectNode(node *Node) (*Node, bool) {
	if qcud.currentNodeOrderIndex == localQueueIndex {
		return node, qcud.checkedAllNodes()
	}

	checkedAllNodesBeforeSkips := func() bool {
		return qcud.nodesChecked == len(qcud.nodeOrder)+1
	}

	for !checkedAllNodesBeforeSkips() {
		// have not made it through first rotation yet
		currentNodeName := qcud.nodeOrder[qcud.currentNodeOrderIndex]
		if qcud.queryComponentUtilizationTriggerCheck.TriggerThresholdCheck() {
			// triggered a utilization check
			exceedsThreshold, _ := qcud.queryComponentUtilizationCheckThreshold.ExceedsThresholdForComponent(currentNodeName)

			if exceedsThreshold {
				// triggered a utilization check *and* the query component for this node
				// is utilizing querier-worker connections in excess of the target threshold

				// skip over this node for now, but add to the skipped order
				// in case we cannot find an item to dequeue from non-skipped nodes
				qcud.nodesSkippedIndexOrder = append(qcud.nodesSkippedIndexOrder, qcud.currentNodeOrderIndex)
				qcud.incrementWrapIndex()

				// increment nodesChecked;
				// we will not return true for checkedAllNodes until we check the skipped nodes too
				qcud.nodesChecked++
			} else {
				// triggered a utilization check but query component for this node is not over utilization threshold;
				// select the current node
				qcud.nodesChecked++
				return node.queueMap[currentNodeName], qcud.checkedAllNodes()
			}
		} else {
			// no utilization check triggered; select the current node
			qcud.nodesChecked++
			return node.queueMap[currentNodeName], qcud.checkedAllNodes()
		}
	}

	// else; we have checked all nodes the first time
	// if we are here, none of the nodes that did not get skipped were able to be dequeued from
	// and checkedAllNodes() has not returned true yet, so there are skipped nodes to select from
	for len(qcud.nodesSkippedIndexOrder) > 0 {
		skippedNodeIndex := qcud.nodesSkippedIndexOrder[0]
		skippedNodeName := qcud.nodeOrder[skippedNodeIndex]

		// update state before returning the first skipped node:
		// set currentNodexOrderIndex to the index of the skipped node being selected
		qcud.currentNodeOrderIndex = skippedNodeIndex
		// and drop skipped node index from front of the skipped node queue list
		qcud.nodesSkippedIndexOrder = qcud.nodesSkippedIndexOrder[1:]
		return node.queueMap[skippedNodeName], qcud.checkedAllNodes()
	}
	return nil, qcud.checkedAllNodes()
}

// dequeueUpdateState does the following:
//   - deletes the dequeued-from child node if it is empty after the dequeue operation
//   - increments queuePosition if no child was deleted
func (qcud *queryComponentUtilizationDequeueSkipOverThreshold) dequeueUpdateState(node *Node, dequeuedFrom *Node) {
	// if the child node is nil, we haven't done anything to the tree; return early
	if dequeuedFrom == nil {
		return
	}

	// if the child is empty, we should delete it, but not increment queue position, since removing an element
	// from queueOrder sets our position to the next element already.
	if dequeuedFrom != node && dequeuedFrom.IsEmpty() {
		childName := dequeuedFrom.Name()
		delete(node.queueMap, childName)
		for idx, name := range qcud.nodeOrder {
			if name == childName {
				qcud.nodeOrder = append(qcud.nodeOrder[:idx], qcud.nodeOrder[idx+1:]...)
				break
			}
		}
		if qcud.currentNodeOrderIndex >= len(qcud.nodeOrder) {
			qcud.currentNodeOrderIndex = 0
		}
	} else {
		qcud.incrementWrapIndex()
	}

	qcud.nodesChecked = 0
	qcud.nodesSkippedIndexOrder = nil
}
