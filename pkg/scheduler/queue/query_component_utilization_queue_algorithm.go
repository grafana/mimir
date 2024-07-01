// SPDX-License-Identifier: AGPL-3.0-only

package queue

import "math"

type queryComponentUtilizationCheck interface {
	TriggerUtilizationCheck() bool
	ExceedsThresholdForComponentName(name string) (bool, QueryComponent)
}

type queryComponentUtilizationReserveConnections struct {
	utilization      *QueryComponentUtilization
	connectedWorkers int
	waitingWorkers   int
	queueLen         int
}

func (qcurc *queryComponentUtilizationReserveConnections) ExceedsThresholdForComponentName(name string) (bool, QueryComponent) {
	if qcurc.connectedWorkers <= 1 {
		// corner case; cannot reserve capacity with only one worker available
		return false, ""
	}

	// allow the functionality to be turned off via setting targetReservedCapacity to 0
	minReservedConnections := 0
	if qcurc.utilization.targetReservedCapacity > 0 {
		// reserve at least one connection in case (connected workers) * (reserved capacity) is less than one
		minReservedConnections = int(
			math.Ceil(
				math.Max(qcurc.utilization.targetReservedCapacity*float64(qcurc.connectedWorkers), 1),
			),
		)
	}

	isIngester, isStoreGateway := queryComponentFlags(name)
	if isIngester {
		if qcurc.connectedWorkers-(qcurc.utilization.ingesterInflightRequests) <= minReservedConnections {
			return true, Ingester
		}
	}
	if isStoreGateway {
		if qcurc.connectedWorkers-(qcurc.utilization.storeGatewayInflightRequests) <= minReservedConnections {
			return true, StoreGateway
		}
	}
	return false, ""
}

func (qcurc *queryComponentUtilizationReserveConnections) TriggerUtilizationCheck() bool {
	if qcurc.waitingWorkers > qcurc.queueLen {
		// excess querier-worker capacity; no need to reserve any for now
		return false
	}
	return true
}

type queryComponentUtilizationDequeueSkipOverThreshold struct {
	queryComponentUtilizationThreshold queryComponentUtilizationCheck
	currentNodeOrderIndex              int
	nodeOrder                          []string
	nodesChecked                       int
	nodesSkippedIndexOrder             []int

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
		if qcud.queryComponentUtilizationThreshold.TriggerUtilizationCheck() {
			// triggered a utilization check
			exceedsThreshold, _ := qcud.queryComponentUtilizationThreshold.ExceedsThresholdForComponentName(currentNodeName)

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
