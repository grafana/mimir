package queue

import "fmt"

type DequeueAlgorithm interface {
	addChildNode(*Node, *Node)
	deleteChildNode(*Node, *Node) bool
	dequeueOps() dequeueOps
	getQueueOrder(*Node) []string
}

type dequeueGetNodeFunc func(*Node) (*Node, bool)
type dequeueUpdateStateFunc func(*Node, any, bool)

type dequeueOps struct {
	getNode     dequeueGetNodeFunc
	updateState dequeueUpdateStateFunc
}

type roundRobinState struct {
}

func (rrs *roundRobinState) addChildNode(parent, child *Node) {
	// add childNode to n.queueMap
	parent.queueMap[child.Name()] = child

	// add childNode to n.queueOrder before the current position, update n.queuePosition to current element
	if parent.queuePosition <= localQueueIndex {
		parent.queueOrder = append(parent.queueOrder, child.Name())
	} else {
		parent.queueOrder = append(parent.queueOrder[:parent.queuePosition], append([]string{child.Name()}, parent.queueOrder[parent.queuePosition:]...)...)
		parent.queuePosition++ // keep position pointed at same element
	}
}

func (rrs *roundRobinState) deleteChildNode(parent, child *Node) bool {
	var childFound bool
	childName := child.Name()

	for idx, name := range parent.queueOrder {
		if name == childName {
			parent.queueOrder = append(parent.queueOrder[:idx], parent.queueOrder[idx+1:]...)
			childFound = true
		}
	}
	return childFound
}

func (rrs *roundRobinState) dequeueOps() dequeueOps {
	getNode := func(n *Node) (*Node, bool) {
		// advance the queue position for this dequeue
		n.queuePosition++
		if n.queuePosition >= len(n.queueOrder) {
			n.queuePosition = localQueueIndex
		}

		checkedAllNodes := n.childrenChecked == len(n.queueOrder)+1
		if n.queuePosition == localQueueIndex {
			return n, checkedAllNodes
		}

		currentNodeName := n.queueOrder[n.queuePosition]
		if node, ok := n.queueMap[currentNodeName]; ok {
			return node, checkedAllNodes
		}
		return nil, checkedAllNodes
	}

	// TODO (casie): Ignoring nodeName here because it doesn't matter...ugly
	updateState := func(n *Node, v any, deletedNode bool) {
		if v != nil {
			n.childrenChecked = 0
		} else {
			n.childrenChecked++
		}
		if deletedNode {
			n.queuePosition--
			// if we try to go beyond something that would increment to
			// the localQueueIndex or in queueOrder, we should wrap around to the
			// back of queueOrder.
			if n.queuePosition < localQueueIndex-1 {
				n.queuePosition = len(n.queueOrder) - 1
			}
		}
	}

	return dequeueOps{
		getNode:     getNode,
		updateState: updateState,
	}
}

func (rrs *roundRobinState) getQueueOrder(n *Node) []string {
	return n.queueOrder
}

type shuffleShardState struct {
	tenantQuerierMap    map[TenantID]map[QuerierID]struct{}
	tenantNodes         map[string][]*Node
	currentQuerier      *QuerierID
	sharedQueueOrder    []string
	sharedQueuePosition int
}

func (sss *shuffleShardState) addChildNode(parent, child *Node) {
	childName := child.Name()
	_, childInOrder := sss.tenantNodes[childName]

	// add childNode to node's queueMap,
	// and to the shared tenantNodes map
	parent.queueMap[childName] = child
	sss.tenantNodes[childName] = append(sss.tenantNodes[childName], child)

	// if child already exists in shared queueOrder, return without altering order
	if childInOrder {
		return
	}

	// otherwise, add childNode to n.queueOrder before the current position, and
	// update n.queuePosition to current element
	// TODO (casie): Mirrors tenantQuerierAssignments.createOrUpdateTenant; move logic here?
	for i, elt := range sss.sharedQueueOrder {
		if elt == "" {
			sss.sharedQueueOrder[i] = childName
			return
		}
	}
	// if we get here, we didn't find any empty elements in sharedQueueOrder; append
	sss.sharedQueueOrder = append(sss.sharedQueueOrder, childName)

}

func (sss *shuffleShardState) deleteChildNode(parent, child *Node) bool {
	childName := child.Name()

	// delete from parent's children
	delete(parent.queueMap, childName)

	// delete from shared tenantNodes
	for i, tenantNode := range sss.tenantNodes[childName] {
		if tenantNode == child {
			sss.tenantNodes[childName] = append(sss.tenantNodes[childName][:i], sss.tenantNodes[childName][i+1:]...)
		}
	}

	// check tenantNodes; to mirror tenantQuerierAssignments, we only remove from sharedQueueOrder
	// if all nodes with this name are gone. If the child is at the _end_ of sharedQueueOrder, we
	// remove it outright; otherwise, we replace it with an empty string so as not to reindex all
	// slice elements
	// TODO (casie): Mirrors tenantQuerierAssignments.removeTenant. Pull logic here?

	removeFromSharedQueueOrder := len(sss.tenantNodes[childName]) == 0

	var positionNeedsUpdate bool
	if removeFromSharedQueueOrder {
		for idx, name := range sss.sharedQueueOrder {
			if name == childName {
				sss.sharedQueueOrder[idx] = ""
			}
		}
		// clear all sequential empty elts from sharedQueueOrder
		lastElementIndex := len(sss.sharedQueueOrder) - 1
		for i := lastElementIndex; i >= 0 && sss.sharedQueueOrder[i] == ""; i-- {
			sss.sharedQueueOrder = sss.sharedQueueOrder[:i]
		}
	}
	return positionNeedsUpdate
}

func (sss *shuffleShardState) dequeueOps() dequeueOps {
	// start from sss.sharedQueuePosition
	// check next tenant for querierID against availabilityMap
	// if exists, move element to "back" of queue
	// if doesn't exist, check next child
	// nothing here to dequeue
	getNode := func(n *Node) (*Node, bool) {
		// can't get a tenant if no querier set
		if sss.currentQuerier == nil {
			return nil, true
		}

		checkedAllNodes := n.childrenChecked == len(n.queueMap)+1 // must check local queue as well

		// advance queue position for dequeue
		sss.sharedQueuePosition++
		if sss.sharedQueuePosition >= len(sss.sharedQueueOrder) {
			sss.sharedQueuePosition = localQueueIndex
		}

		// no children
		if len(n.queueMap) == 0 || sss.sharedQueuePosition == localQueueIndex {
			return n, checkedAllNodes
		}

		checkIndex := sss.sharedQueuePosition

		for iters := 0; iters < len(sss.sharedQueueOrder); iters++ {
			if checkIndex >= len(sss.sharedQueueOrder) {
				checkIndex = 0
			}
			tenantName := sss.sharedQueueOrder[checkIndex]

			if _, ok := n.queueMap[tenantName]; !ok {
				// tenant not in _this_ node's children, move on
				checkIndex++
				continue
			}

			// increment nodes checked even if not in tenant-querier map
			n.childrenChecked++
			checkedAllNodes = n.childrenChecked == len(n.queueMap)+1

			// if the tenant-querier set is nil, any querier can serve this tenant
			if sss.tenantQuerierMap[TenantID(tenantName)] == nil {
				sss.sharedQueuePosition = checkIndex
				return n.queueMap[tenantName], checkedAllNodes
			}
			if tenantQuerierSet, ok := sss.tenantQuerierMap[TenantID(tenantName)]; ok {
				if _, ok := tenantQuerierSet[*sss.currentQuerier]; ok {
					sss.sharedQueuePosition = checkIndex
					return n.queueMap[tenantName], checkedAllNodes
				}
			}
			checkIndex++
		}
		return nil, checkedAllNodes
	}

	updateState := func(n *Node, v any, deletedNode bool) {
		if deletedNode {
			// when we delete a child node, we want to step back in the queue so the
			// previously-next tenant will still be next (e.g., if queueOrder is
			// [childA, childB, childC] and childB (position 1) is deleted, we want to
			// step back to position 0 so that in the next dequeue, position 1 (childC)
			// is dequeued
			fmt.Println("node deleted")
			sss.sharedQueuePosition--
			fmt.Println(sss.sharedQueuePosition)
		}
		// we need to reset our checked nodes if we found a value to dequeue, or if we've checked all nodes
		if v != nil {
			n.childrenChecked = 0
			return
		}

		n.childrenChecked++

		// if dequeueNode was self, advance queue position no matter what, and return
		if sss.sharedQueuePosition == localQueueIndex {
			return
		}

		// We never change the order of sharedQueueOrder; we expect sss.queuePosition to be updated
		// to the last index used for the first dequeue, and hand it the result for the next one.

		// otherwise, don't change queuePosition, only move element to back
		//for i, child := range sss.queueOrder {
		//	if child == nodeName {
		//		sss.queueOrder = append(sss.queueOrder[:i], append(sss.queueOrder[i+1:], child)...) // ugly
		//		n.queuePosition--
		//		// wrap around to back of order if we go past the first element in n.queueOrder.
		//		// In the next dequeue, we will step forward again.
		//		if n.queuePosition <= localQueueIndex {
		//			n.queuePosition = len(sss.queueOrder) - 1
		//		}
		//	}
		//}
	}

	return dequeueOps{
		getNode:     getNode,
		updateState: updateState,
	}

}

func (sss *shuffleShardState) getQueueOrder(_ *Node) []string {
	return sss.sharedQueueOrder
}
