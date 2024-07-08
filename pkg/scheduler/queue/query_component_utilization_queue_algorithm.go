// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"math"

	"github.com/pkg/errors"
)

type QueryComponentUtilizationLimit interface {
	// IsOverUtilized implementations check whether a query component has exceeded a utilization threshold.
	// This enables the dequeuing algorithm to skip requests for a query component experiencing heavy load,
	// reserving querier-worker connection capacity to continue servicing requests for the other component.
	// If there are no requests in queue for the other less-utilized component, the dequeuing algorithm may choose
	// to continue servicing requests for the component which has exceeded the reserved capacity threshold.
	//
	// The implementation should return true if the query component is over-utilized, and false otherwise.
	// Defining utilization and the threshold for over-utilization is left to the implementation.
	IsOverUtilized(utilization *QueryComponentUtilization, queryComponentName string) (bool, QueryComponent)
}

// DefaultReservedQueryComponentCapacity reserves 1 / 3 of querier-worker connections
// for the query component utilizing fewer of the available connections.
// Chosen to represent an even balance between the three possible combinations of query components:
// ingesters only, store-gateways only, or both ingesters and store-gateways.
const DefaultReservedQueryComponentCapacity = 0.33

// MaxReservedQueryComponentCapacity is an inclusive upper bound on the targetReservedCapacity.
// The threshold for a QueryComponent's utilization of querier-worker connections
// can only be exceeded by one QueryComponent at a time as long as targetReservedCapacity is < 0.5.
// Therefore, one of the components will always be given the OK to dequeue queries for.
const MaxReservedQueryComponentCapacity = 0.5

// QueryComponentUtilizationLimitByConnections implements a QueryComponentUtilizationLimit.
// Capacity utilization for a QueryComponent is defined by the portion of the querier-worker connections
// which are currently in flight processing a query which requires that QueryComponent.
//
// The component name can indicate the usage of one or both of ingesters and store-gateways.
// If both ingesters and store-gateways will be used, this method will flag the threshold as exceeded
// if either ingesters or store-gateways are currently in excess of the reserved capacity.
type QueryComponentUtilizationLimitByConnections struct {
	// targetReservedCapacity sets the portion of querier-worker connections
	// we aim to reserve for queries to the less-utilized query component;
	targetReservedCapacity float64
	connectedWorkers       int
}

func NewQueryComponentUtilizationLimitByConnections(
	targetReservedCapacity float64,
) (*QueryComponentUtilizationLimitByConnections, error) {
	if targetReservedCapacity > MaxReservedQueryComponentCapacity {
		return nil, errors.New("invalid targetReservedCapacity")
	}

	return &QueryComponentUtilizationLimitByConnections{
		targetReservedCapacity: targetReservedCapacity,
		connectedWorkers:       0,
	}, nil
}

func (qcul *QueryComponentUtilizationLimitByConnections) SetConnectedWorkers(connectedWorkers int) {
	qcul.connectedWorkers = connectedWorkers
}

func (qcul *QueryComponentUtilizationLimitByConnections) IsOverUtilized(
	utilization *QueryComponentUtilization, queryComponentName string,
) (bool, QueryComponent) {
	// allow the functionality to be turned off via setting targetReservedCapacity to 0
	if qcul.targetReservedCapacity == 0 {
		return false, ""
	}

	if qcul.connectedWorkers <= 1 {
		// corner case; cannot reserve capacity with only one worker available
		return false, ""
	}

	minReservedConnections := 0
	//reserve at least one connection in case (connected workers) * (reserved capacity) is less than one
	minReservedConnections = int(
		math.Ceil(
			math.Max(qcul.targetReservedCapacity*float64(qcul.connectedWorkers), 1),
		),
	)

	isIngester, isStoreGateway := queryComponentFlags(queryComponentName)
	ingesterInflightRequests, storeGatewayInflightRequests := utilization.GetInflightRequests()
	if isIngester {
		if qcul.connectedWorkers-ingesterInflightRequests <= minReservedConnections {
			return true, Ingester
		}
	}
	if isStoreGateway {
		if qcul.connectedWorkers-storeGatewayInflightRequests <= minReservedConnections {
			return true, StoreGateway
		}
	}
	return false, ""
}

type queryComponentQueueAlgoSkipOverUtilized struct {
	utilization            *QueryComponentUtilization
	limit                  QueryComponentUtilizationLimit
	currentNodeOrderIndex  int
	nodeOrder              []string
	nodesChecked           int
	nodesSkippedIndexOrder []int

	// TODO implement global state to only delete nodes from rotation when all corresponding nodes are deleted from the tree
	//queueNodeCounts                    map[string]int
}

func (qa *queryComponentQueueAlgoSkipOverUtilized) incrementWrapCurrentNodeOrderIndex() {
	qa.currentNodeOrderIndex++
	if qa.currentNodeOrderIndex >= len(qa.nodeOrder) {
		qa.currentNodeOrderIndex = 0
	}
}

func (qa *queryComponentQueueAlgoSkipOverUtilized) addChildNode(parent, child *Node) {
	// add childNode to n.queueMap
	parent.queueMap[child.Name()] = child

	if qa.currentNodeOrderIndex == localQueueIndex {
		// special case; cannot slice into nodeOrder with index -1
		// place at end of slice, which is the last slot before the local queue slot
		qa.nodeOrder = append(qa.nodeOrder, child.Name())
	} else if qa.currentNodeOrderIndex == 0 {
		// special case; since we are at the beginning of the order,
		// only a simple append is needed to add the new node to the end,
		// which also creates a more intuitive initial order for tests
		qa.nodeOrder = append(qa.nodeOrder, child.Name())
	} else {
		// insert into the order behind current child queue index
		// to prevent the possibility of new nodes continually jumping the line
		qa.nodeOrder = append(
			qa.nodeOrder[:qa.currentNodeOrderIndex],
			append(
				[]string{child.Name()},
				qa.nodeOrder[qa.currentNodeOrderIndex:]...,
			)...,
		)
		// update current child queue index to its new place in the expanded slice
		qa.incrementWrapCurrentNodeOrderIndex()
	}
}

func (qa *queryComponentQueueAlgoSkipOverUtilized) checkedAllNodesBeforeSkips() bool {
	return qa.nodesChecked == len(qa.nodeOrder)+1
}
func (qa *queryComponentQueueAlgoSkipOverUtilized) checkedAllNodes() bool {
	return qa.nodesChecked == len(qa.nodeOrder)+1 && len(qa.nodesSkippedIndexOrder) == 0
}

func (qa *queryComponentQueueAlgoSkipOverUtilized) dequeueSelectNode(node *Node) (*Node, bool) {
	if qa.currentNodeOrderIndex == localQueueIndex {
		return node, qa.checkedAllNodes()
	}

	for !qa.checkedAllNodesBeforeSkips() {
		// have not made it through first rotation yet
		currentNodeName := qa.nodeOrder[qa.currentNodeOrderIndex]
		isOverUtilized, _ := qa.limit.IsOverUtilized(qa.utilization, currentNodeName)

		if isOverUtilized {
			// a query component associated with this queue node is utilizing
			// querier-worker connections in excess of the target connections limit;
			// skip over this node for now, but add to the skipped order
			// in case we cannot find an item to dequeue from non-skipped nodes
			qa.nodesSkippedIndexOrder = append(qa.nodesSkippedIndexOrder, qa.currentNodeOrderIndex)
			qa.incrementWrapCurrentNodeOrderIndex()

			// increment nodesChecked;
			// we will not return true for checkedAllNodes until we check the skipped nodes too
			qa.nodesChecked++
		} else {
			// no query component associated with this queue node is over the utilization limit;
			// select the current node
			qa.nodesChecked++
			return node.queueMap[currentNodeName], qa.checkedAllNodes()
		}

	}
	// else; we have checked all nodes the first time
	// if we are here, none of the nodes that did not get skipped were able to be dequeued from
	// and checkedAllNodes() has not returned true yet, so there are skipped nodes to select from
	for len(qa.nodesSkippedIndexOrder) > 0 {
		skippedNodeIndex := qa.nodesSkippedIndexOrder[0]
		skippedNodeName := qa.nodeOrder[skippedNodeIndex]

		// update state before returning the first skipped node:
		// set currentNodexOrderIndex to the index of the skipped node being selected
		qa.currentNodeOrderIndex = skippedNodeIndex
		// and drop skipped node index from front of the skipped node queue list
		qa.nodesSkippedIndexOrder = qa.nodesSkippedIndexOrder[1:]
		return node.queueMap[skippedNodeName], qa.checkedAllNodes()
	}
	return nil, qa.checkedAllNodes()
}

// dequeueUpdateState does the following:
//   - deletes the dequeued-from child node if it is empty after the dequeue operation
//   - increments queuePosition if no child was deleted
func (qa *queryComponentQueueAlgoSkipOverUtilized) dequeueUpdateState(node *Node, dequeuedFrom *Node) {
	// if the child node is nil, we haven't done anything to the tree; return early
	if dequeuedFrom == nil {
		return
	}

	// if the child is empty, we should delete it, but not increment queue position, since removing an element
	// from queueOrder sets our position to the next element already.
	if dequeuedFrom != node && dequeuedFrom.IsEmpty() {
		childName := dequeuedFrom.Name()
		delete(node.queueMap, childName)
		for idx, name := range qa.nodeOrder {
			if name == childName {
				qa.nodeOrder = append(qa.nodeOrder[:idx], qa.nodeOrder[idx+1:]...)
				break
			}
		}
		if qa.currentNodeOrderIndex >= len(qa.nodeOrder) {
			qa.currentNodeOrderIndex = 0
		}
	} else {
		qa.incrementWrapCurrentNodeOrderIndex()
	}

	qa.nodesChecked = 0
	qa.nodesSkippedIndexOrder = nil
}
