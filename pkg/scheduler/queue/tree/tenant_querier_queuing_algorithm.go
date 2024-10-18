// SPDX-License-Identifier: AGPL-3.0-only

package tree

type (
	TenantID  string
	QuerierID string
)

const emptyTenantID = TenantID("")

// Newly-connected queriers should start from this index in TenantQuerierQueuingAlgorithm.TenantIDOrder.
const newQuerierTenantIndex = -1

// TenantQuerierQueuingAlgorithm implements QueuingAlgorithm.
type TenantQuerierQueuingAlgorithm struct {
	// Set of Querier IDs assigned to each tenant as determined by shuffle sharding.
	// If TenantQuerierIDs[tenantID] for a given tenantID is non-nil, only those queriers can handle
	// the tenant's requests,
	// TenantQuerierIDs[tenantID] is set to nil if sharding is off or available queriers <= tenant's maxQueriers.
	TenantQuerierIDs map[TenantID]map[QuerierID]struct{}

	// List of all tenants with queues, used for iteration when searching for next queue to handle.
	TenantIDOrder []string

	// The querier currently making a dequeue request; updated before dequeues by setup.
	currentQuerier QuerierID

	// tenantNodes tracks nodes of the same name (i.e., multiple nodes that exist for the same tenant);
	// it is used to track whether a tenant should be removed from the tenantIDOrder when a node in Tree is exhausted.
	tenantNodes map[string][]*Node

	// tenantOrderIndex is the index of the _last_ tenant which a querier-worker dequeued from; it is passed in,
	// and then when a request is dequeued, it is updated to the index of the tenant for the dequeued request,
	// so it can be returned to the querier.
	// Newly connected queriers should pass -1 as DequeueArgs.LastTenantIndex to start at the beginning of tenantIDOrder.
	tenantOrderIndex int
}

func NewTenantQuerierQueuingAlgorithm() *TenantQuerierQueuingAlgorithm {
	return &TenantQuerierQueuingAlgorithm{
		TenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{},
		currentQuerier:   "",
		TenantIDOrder:    nil,
		tenantNodes:      map[string][]*Node{},
	}
}

// AddTenant inserts a tenantID into TenantIDOrder, replacing the first empty string it finds,
// or appending to the end if no empty strings are found. It returns the index at which the tenantID was inserted.
func (qa *TenantQuerierQueuingAlgorithm) AddTenant(tenantID string) int {
	for i, id := range qa.TenantIDOrder {
		if id == "" {
			// previously removed tenant not yet cleaned up; take its place
			qa.TenantIDOrder[i] = tenantID
			return i
		}
	}
	qa.TenantIDOrder = append(qa.TenantIDOrder, tenantID)
	// the index of the appended tenantID is len-1
	return len(qa.TenantIDOrder) - 1
}

func (qa *TenantQuerierQueuingAlgorithm) TenantOrderIndex() int {
	return qa.tenantOrderIndex
}

// TotalQueueSizeForTenant counts up items for all the nodes in tenantNodes[tenantID] and returns the total.
// This is the total number of requests queued for that tenant.
func (qa *TenantQuerierQueuingAlgorithm) TotalQueueSizeForTenant(tenantID string) int {
	itemCount := 0
	for _, tenantNode := range qa.tenantNodes[tenantID] {
		itemCount += tenantNode.ItemCount()
	}
	return itemCount
}

// CurrentQuerier is a test utility for reading the currently-set querier.
func CurrentQuerier(qa *TenantQuerierQueuingAlgorithm) string {
	return string(qa.currentQuerier)
}

func (qa *TenantQuerierQueuingAlgorithm) setup(dequeueArgs *DequeueArgs) {
	qa.currentQuerier = QuerierID(dequeueArgs.QuerierID)
	qa.tenantOrderIndex = dequeueArgs.LastTenantIndex
}

// addChildNode adds a child to:
//   - the node's own queueMap
//   - tenantNodes, which maintains a slice of all nodes with the same name. tenantNodes is checked on node deletion
//     to ensure that we only remove a tenant from tenantIDOrder if _all_ nodes with the same name have been removed.
//   - tenantIDOrder iff the node did not already exist in tenantNodes or tenantIDOrder. addChildNode will place
//     a new tenant in the first empty ("") element it finds in tenantIDOrder, or at the end if no empty elements exist.
func (qa *TenantQuerierQueuingAlgorithm) addChildNode(parent, child *Node) {
	childName := child.Name()
	_, tenantHasAnyQueue := qa.tenantNodes[childName]

	// add childNode to node's queueMap,
	// and to the shared tenantNodes map
	parent.queueMap[childName] = child
	qa.tenantNodes[childName] = append(qa.tenantNodes[childName], child)

	// if child has any queue, it should already be in tenantIDOrder, return without altering order
	if tenantHasAnyQueue {
		return
	}

	// otherwise, replace the first empty element in n.tenantIDOrder with childName, or append to the end
	for i, elt := range qa.TenantIDOrder {
		// if we encounter a tenant with this name in tenantIDOrder already, return without altering order.
		// This is a weak check (childName could exist farther down tenantIDOrder, but be inserted here as well),
		// but should be fine, since the only time we hit this case is when createOrUpdateTenant is called, which
		// only happens immediately before enqueueing.
		if elt == childName {
			return
		}
		if elt == "" {
			qa.TenantIDOrder[i] = childName
			return
		}
	}
	// if we get here, we didn't find any empty elements in tenantIDOrder; append
	qa.TenantIDOrder = append(qa.TenantIDOrder, childName)
}

// dequeueSelectNode chooses the next node to dequeue from based on TenantIDOrder and tenantOrderIndex, which are
// shared across all nodes to maintain an O(n) (where n = # tenants) time-to-dequeue for each tenant.
// If tenant order were maintained by individual nodes, we would end up with O(mn) (where m = # query components)
// time-to-dequeue for a given tenant.
//
// tenantOrderIndex is incremented, checks if the tenant at that index is a child of the current node (it may not be,
// e.g., in the case that a tenant has queries queued for one query component but not others), and if so, returns
// the tenant node if currentQuerier can handle queries for that tenant.
//
// Note that because we use the shared TenantIDOrder and tenantOrderIndex to manage the queue, we functionally
// ignore each Node's individual queueOrder and queuePosition.
func (qa *TenantQuerierQueuingAlgorithm) dequeueSelectNode(node *Node) *Node {
	if node.isLeaf() || len(node.queueMap) == 0 {
		return node
	}
	// can't get a tenant if no querier set
	if qa.currentQuerier == "" {
		return nil
	}

	// Before this point, tenantOrderIndex represents the _last_ tenant we dequeued from;
	// here, we advance queue position to point to the index we _will_ dequeue from
	qa.tenantOrderIndex++
	if qa.tenantOrderIndex >= len(qa.TenantIDOrder) {
		qa.tenantOrderIndex = 0
	}

	checkIndex := qa.tenantOrderIndex

	// iterate through the tenant order until we find a tenant that is assigned to the current querier, or
	// have checked the entire tenantIDOrder, whichever comes first
	for iters := 0; iters < len(qa.TenantIDOrder); iters++ {
		if checkIndex >= len(qa.TenantIDOrder) {
			// do not use modulo to wrap this index; tenantOrderIndex is provided from an outer process
			// which does not know if the tenant list has changed since the last dequeue
			// wrapping with modulo after shrinking the list could cause us to skip tenants
			checkIndex = 0
		}
		tenantName := qa.TenantIDOrder[checkIndex]
		tenantID := TenantID(tenantName)

		if _, ok := node.queueMap[tenantName]; !ok {
			// tenant not in _this_ node's children, move on
			checkIndex++
			continue
		}

		// if the tenant-querier set is nil, any querier can serve this tenant
		if qa.TenantQuerierIDs[tenantID] == nil {
			qa.tenantOrderIndex = checkIndex
			return node.queueMap[tenantName]
		}
		// otherwise, check if the querier is assigned to this tenant
		if tenantQuerierSet, ok := qa.TenantQuerierIDs[tenantID]; ok {
			if _, ok := tenantQuerierSet[qa.currentQuerier]; ok {
				qa.tenantOrderIndex = checkIndex
				return node.queueMap[tenantName]
			}
		}
		checkIndex++
	}
	return nil
}

// dequeueUpdateState deletes the dequeued-from node from the following locations if it is empty:
//   - parent's queueMap,
//   - tenantNodes
//   - TenantIDOrder iff there are no other nodes by the same name in tenantNodes. If the child is at the end of
//     TenantIDOrder, it is removed outright; otherwise, it is replaced with an empty ("") element
//
// dequeueUpdateState would normally also handle incrementing the queue position after performing a dequeue, but
// tenantQuerierAssignments currently expects the caller to handle this by having the querier set tenantOrderIndex.
func (qa *TenantQuerierQueuingAlgorithm) dequeueUpdateState(node *Node, dequeuedFrom *Node) {
	// if dequeuedFrom is nil or is not empty, we don't need to do anything;
	// position updates will be handled by the caller, and we don't need to remove any nodes.
	if dequeuedFrom == nil || !dequeuedFrom.IsEmpty() {
		return
	}

	// delete from the node's children
	childName := dequeuedFrom.Name()
	delete(node.queueMap, childName)

	// delete from shared tenantNodes
	for i, tenantNode := range qa.tenantNodes[childName] {
		if tenantNode == dequeuedFrom {
			qa.tenantNodes[childName] = append(qa.tenantNodes[childName][:i], qa.tenantNodes[childName][i+1:]...)
		}
	}

	// check tenantNodes; we only remove from TenantIDOrder if all nodes with this name are gone.
	// If the removed child is at the _end_ of tenantIDOrder, we remove it outright; otherwise,
	// we replace it with an empty string. Removal of elements from anywhere other than the end of the slice
	// would re-index all tenant IDs, resulting in skipped tenants when starting iteration from the
	// querier-provided LastTenantIndex.
	removeFromSharedQueueOrder := len(qa.tenantNodes[childName]) == 0

	if removeFromSharedQueueOrder {
		for idx, name := range qa.TenantIDOrder {
			if name == childName {
				qa.TenantIDOrder[idx] = string(emptyTenantID)
			}
		}
		// clear all sequential empty elements from tenantIDOrder
		lastElementIndex := len(qa.TenantIDOrder) - 1
		for i := lastElementIndex; i >= 0 && qa.TenantIDOrder[i] == ""; i-- {
			qa.TenantIDOrder = qa.TenantIDOrder[:i]
		}
	}
}
