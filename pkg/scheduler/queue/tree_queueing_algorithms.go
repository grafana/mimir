// SPDX-License-Identifier: AGPL-3.0-only

package queue

// QueuingAlgorithm represents the set of operations specific to different approaches to queuing/dequeuing. It is
// applied at the layer-level -- every Node at the same depth in a MultiQueuingAlgorithmTreeQueue shares the same QueuingAlgorithm,
// including any state in structs that implement QueuingAlgorithm.
type QueuingAlgorithm interface {
	// addChildNode updates a parent's queueMap and other child-tracking structures with a new child Node. QueuePaths
	// passed to enqueue functions are allowed to contain node names for nodes which do not yet exist; in those cases,
	// we create the missing nodes. Implementers are responsible for adding the newly-created node to whatever state(s)
	// keep track of nodes in the subtree, as well as updating any positional pointers that have been shifted by the
	// addition of a new node.
	addChildNode(parent, child *Node)

	// dequeueSelectNode uses information in the given node and/or the QueuingAlgorithm state to select and return
	// the node which an element will be dequeued from next, as well as a bool representing whether the passed node
	// and all its children have been checked for dequeue-ability. The returned node may be one of the following:
	//	- the given node: this triggers the base case for the recursive Node.dequeue, and results in dequeuing from
	//    the node's local queue.
	//	- a direct child of the given node: this continues the dequeue recursion.
	// The Tree uses the bool returned to determine when no dequeue-able value can be found at this child. If all
	// children in the subtree have been checked, but no dequeue-able value is found, the Tree traverses to the next
	// child in the given node's order. dequeueSelectNode does *not* update any Node fields.
	dequeueSelectNode(node *Node) (*Node, bool)

	// dequeueUpdateState is called after we have finished dequeuing from a node. When a child is left empty after the
	// dequeue operation, dequeueUpdateState should perform cleanup by deleting that child from the Node and update
	// any other necessary state to reflect the child's removal. It should also update the relevant position to point
	// to the next node to dequeue from.
	dequeueUpdateState(node *Node, dequeuedFrom *Node)
}

// roundRobinState is the simplest type of QueuingAlgorithm; nodes which use this QueuingAlgorithm and are at
// the same depth in a MultiQueuingAlgorithmTreeQueue do not share any state. When children are added to these nodes, they are placed at
// the "end" of the order from the perspective of the node's current queuePosition (e.g., if queuePosition is 3,
// a new child will be placed at index 2). Children are dequeued from using a simple round-robin ordering;
// queuePosition is incremented on every dequeue.
type roundRobinState struct {
}

// addChildNode adds a child Node to the "end" of the parent's queueOrder from the perspective
// of the parent's queuePosition. Thus, If queuePosition is localQueueIndex, the new child is added to the end of
// queueOrder. Otherwise, the new child is added at queuePosition - 1, and queuePosition is incremented to keep
// it pointed to the same child.
func (rrs *roundRobinState) addChildNode(parent, child *Node) {
	// add childNode to n.queueMap
	parent.queueMap[child.Name()] = child

	// add childNode to n.queueOrder before the next-to-dequeue position, update n.queuePosition to current element
	if parent.queuePosition <= localQueueIndex {
		parent.queueOrder = append(parent.queueOrder, child.Name())
	} else {
		parent.queueOrder = append(parent.queueOrder[:parent.queuePosition], append([]string{child.Name()}, parent.queueOrder[parent.queuePosition:]...)...)
		parent.queuePosition++ // keep position pointed at same element
	}
}

// dequeueSelectNode returns the node at the node's queuePosition. queuePosition represents the position of
// the next node to dequeue from, and is incremented in dequeueUpdateState.
func (rrs *roundRobinState) dequeueSelectNode(node *Node) (*Node, bool) {
	checkedAllNodes := node.childrenChecked == len(node.queueOrder)+1 // must check local queue as well
	if node.queuePosition == localQueueIndex {
		return node, checkedAllNodes
	}

	currentNodeName := node.queueOrder[node.queuePosition]
	if node, ok := node.queueMap[currentNodeName]; ok {
		return node, checkedAllNodes
	}
	return nil, checkedAllNodes
}

// dequeueUpdateState does the following:
//   - deletes the dequeued-from child node if it is empty after the dequeue operation
//   - increments queuePosition if no child was deleted
func (rrs *roundRobinState) dequeueUpdateState(node *Node, dequeuedFrom *Node) {
	// if the child node is nil, we haven't done anything to the tree; return early
	if dequeuedFrom == nil {
		return
	}

	// corner case: if node == dequeuedFrom and is empty, we will try to delete a "child" (no-op),
	// and won't increment position. This is fine, because if the node is empty, there's nothing to
	// increment to.
	childIsEmpty := dequeuedFrom.IsEmpty()

	// if the child is empty, we should delete it, but not increment queue position, since removing an element
	// from queueOrder sets our position to the next element already.
	if childIsEmpty {
		childName := dequeuedFrom.Name()
		delete(node.queueMap, childName)
		for idx, name := range node.queueOrder {
			if name == childName {
				node.queueOrder = append(node.queueOrder[:idx], node.queueOrder[idx+1:]...)
			}
		}

	} else {
		node.queuePosition++
	}
	if node.queuePosition >= len(node.queueOrder) {
		node.queuePosition = localQueueIndex
	}
}

type tenantSelectionState struct {
	// List of all tenants with queues, used for iteration when searching for next queue to handle.
	tenantIDOrder []TenantID
	// tenantOrderIndex is the index of the _last_ tenant dequeued from; it is passed by
	// the querier, and then updated to the index of the last tenant dequeued from, so it
	// can be returned to the querier. Newly connected queriers should pass -1 to start at the
	// beginning of tenantIDOrder.
	tenantOrderIndex int
	tenantNodes      map[string][]*Node
	// Tenant assigned querier ID set as determined by shuffle sharding; this is a reference to the map
	// maintained by tenantQuerierAssignments, and should _never_ be updated by tenantSelectionState
	tenantQuerierIDs map[TenantID]map[QuerierID]struct{}
	currentQuerier   QuerierID
}

// dequeueSelectNode chooses the next node to dequeue from based on tenantIDOrder and tenantOrderIndex, which are
// shared across all nodes to maintain an O(n) (where n = # tenants) time-to-dequeue for each tenant.
// If tenant order were maintained by individual nodes, we would end up with O(mn) (where m = # query components)
// time-to-dequeue for a given tenant.
//
// tenantOrderIndex is incremented, checks if the tenant at that index is a child of the current node (it may not be,
// e.g., in the case that a tenant has queries queued for one query component but not others), and if so, returns
// the tenant node if currentQuerier can handle queries for that tenant.
//
// Note that because we use the shared  tenantIDOrder and tenantOrderIndex to manage the queue, we functionally
// ignore each Node's individual queueOrder and queuePosition.
func (tss *tenantSelectionState) dequeueSelectNode(node *Node) (*Node, bool) {
	// can't get a tenant if no querier set
	if tss.currentQuerier == "" {
		return nil, true
	}

	checkedAllNodes := node.childrenChecked == len(node.queueMap)+1 // must check local queue as well

	// advance queue position for dequeue
	tss.tenantOrderIndex++
	if tss.tenantOrderIndex >= len(tss.tenantIDOrder) {
		tss.tenantOrderIndex = localQueueIndex
	}

	// no children or local queue reached
	if len(node.queueMap) == 0 || tss.tenantOrderIndex == localQueueIndex {
		return node, checkedAllNodes
	}

	checkIndex := tss.tenantOrderIndex

	// iterate through the tenant order until we find a tenant that is assigned to the current querier, or
	// have checked the entire tenantIDOrder, whichever comes first
	for iters := 0; iters < len(tss.tenantIDOrder); iters++ {
		if checkIndex >= len(tss.tenantIDOrder) {
			// do not use modulo to wrap this index; tenantOrderIndex is provided from an outer process
			// which does not know if the tenant list has changed since the last dequeue
			// wrapping with modulo after shrinking the list could cause us to skip tenants
			checkIndex = 0
		}
		tenantID := tss.tenantIDOrder[checkIndex]
		tenantName := string(tenantID)

		if _, ok := node.queueMap[tenantName]; !ok {
			// tenant not in _this_ node's children, move on
			checkIndex++
			continue
		}

		checkedAllNodes = node.childrenChecked == len(node.queueMap)+1

		// if the tenant-querier set is nil, any querier can serve this tenant
		if tss.tenantQuerierIDs[tenantID] == nil {
			tss.tenantOrderIndex = checkIndex
			return node.queueMap[tenantName], checkedAllNodes
		}
		// otherwise, check if the querier is assigned to this tenant
		if tenantQuerierSet, ok := tss.tenantQuerierIDs[tenantID]; ok {
			if _, ok := tenantQuerierSet[tss.currentQuerier]; ok {
				tss.tenantOrderIndex = checkIndex
				return node.queueMap[tenantName], checkedAllNodes
			}
		}
		checkIndex++
	}
	return nil, checkedAllNodes
}

// dequeueUpdateState deletes the dequeued-from node from the following locations if it is empty:
//   - parent's queueMap,
//   - tenantNodes
//   - tenantIDOrder iff there are no other nodes by the same name in tenantNodes. If the child is at the end of
//     tenantIDOrder, it is removed outright; otherwise, it is replaced with an empty ("") element
//
// dequeueUpdateState would normally also handle incrementing the queue position after performing a dequeue, but
// tenantQuerierAssignments currently expects the caller to handle this by having the querier set tenantOrderIndex.
func (tss *tenantSelectionState) dequeueUpdateState(node *Node, dequeuedFrom *Node) {
	// if dequeuedFrom is nil or is not empty, we don't need to do anything;
	// position updates will be handled by the caller, and we don't need to remove any nodes.
	if dequeuedFrom == nil || !dequeuedFrom.IsEmpty() {
		return
	}

	// delete from the node's children
	childName := dequeuedFrom.Name()
	delete(node.queueMap, childName)

	// delete from shared tenantNodes
	for i, tenantNode := range tss.tenantNodes[childName] {
		if tenantNode == dequeuedFrom {
			tss.tenantNodes[childName] = append(tss.tenantNodes[childName][:i], tss.tenantNodes[childName][i+1:]...)
		}
	}

	// check tenantNodes; we only remove from tenantIDOrder if all nodes with this name are gone.
	// If the removed child is at the _end_ of tenantIDOrder, we remove it outright; otherwise,
	// we replace it with an empty string. Removal of elements from anywhere other than the end of the slice
	// would re-index all tenant IDs, resulting in skipped tenants when starting iteration from the
	// querier-provided lastTenantIndex.
	removeFromSharedQueueOrder := len(tss.tenantNodes[childName]) == 0

	if removeFromSharedQueueOrder {
		for idx, name := range tss.tenantIDOrder {
			if string(name) == childName {
				tss.tenantIDOrder[idx] = emptyTenantID
			}
		}
		// clear all sequential empty elements from tenantIDOrder
		lastElementIndex := len(tss.tenantIDOrder) - 1
		for i := lastElementIndex; i >= 0 && tss.tenantIDOrder[i] == ""; i-- {
			tss.tenantIDOrder = tss.tenantIDOrder[:i]
		}
	}
}

// addChildNode adds a child to:
//   - the node's own queueMap
//   - tenantNodes, which maintains a slice of all nodes with the same name. tenantNodes is checked on node deletion
//     to ensure that we only remove a tenant from tenantIDOrder if _all_ nodes with the same name have been removed.
//   - tenantIDOrder iff the node did not already exist in tenantNodes or tenantIDOrder. addChildNode will place
//     a new tenant in the first empty ("") element it finds in tenantIDOrder, or at the end if no empty elements exist.
func (tss *tenantSelectionState) addChildNode(parent, child *Node) {
	childName := child.Name()
	_, tenantHasAnyQueue := tss.tenantNodes[childName]

	// add childNode to node's queueMap,
	// and to the shared tenantNodes map
	parent.queueMap[childName] = child
	tss.tenantNodes[childName] = append(tss.tenantNodes[childName], child)

	// if child has any queue, it should already be in tenantIDOrder, return without altering order
	if tenantHasAnyQueue {
		return
	}

	// otherwise, replace the first empty element in n.tenantIDOrder with childName, or append to the end
	for i, elt := range tss.tenantIDOrder {
		// if we encounter a tenant with this name in tenantIDOrder already, return without altering order.
		// This is a weak check (childName could exist farther down tenantIDOrder, but be inserted here as well),
		// but should be fine, since the only time we hit this case is when createOrUpdateTenant is called, which
		// only happens immediately before enqueueing.
		if elt == TenantID(childName) {
			return
		}
		if elt == "" {
			tss.tenantIDOrder[i] = TenantID(childName)
			return
		}
	}
	// if we get here, we didn't find any empty elements in tenantIDOrder; append
	tss.tenantIDOrder = append(tss.tenantIDOrder, TenantID(childName))
}

// updateQueuingAlgorithmState should be called before attempting to dequeue, and updates inputs required by this
// QueuingAlgorithm to dequeue the appropriate value for the given querier. In some test cases, it need not be called
// before consecutive dequeues for the same querier, but in all operating cases, it should be called ahead of a dequeue.
func (tss *tenantSelectionState) updateQueuingAlgorithmState(querierID QuerierID, tenantOrderIndex int) {
	tss.currentQuerier = querierID
	tss.tenantOrderIndex = tenantOrderIndex
}

func (tss *tenantSelectionState) itemCountForTenant(tenantID string) int {
	itemCount := 0
	for _, tenantNode := range tss.tenantNodes[tenantID] {
		itemCount += tenantNode.ItemCount()
	}
	return itemCount
}
