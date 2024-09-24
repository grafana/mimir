// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/user_queues.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queue

import (
	"math/rand"
	"sort"

	"github.com/grafana/mimir/pkg/util"
)

// Newly-connected queriers should start from this index in tenantQuerierAssignments.tenantIDOrder.
const newQuerierTenantIndex = -1

type queueTenant struct {
	tenantID    TenantID
	maxQueriers int

	// seed for shuffle sharding of queriers; computed from tenantID only,
	// and is therefore consistent between different frontends.
	shuffleShardSeed int64

	// points up to tenant order to enable efficient removal
	orderIndex int
}

type querierIDSlice []QuerierID

// Len implements sort.Interface for querierIDSlice
func (s querierIDSlice) Len() int { return len(s) }

// Swap implements sort.Interface for querierIDSlice
func (s querierIDSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// Less implements sort.Interface for querierIDSlice
func (s querierIDSlice) Less(i, j int) bool { return s[i] < s[j] }

// Search method covers for sort.Search's functionality,
// as sort.Search does not allow anything interface-based or generic yet.
func (s querierIDSlice) Search(x QuerierID) int {
	return sort.Search(len(s), func(i int) bool { return s[i] >= x })
}

// tenantQuerierAssignments implements QueuingAlgorithm. In the context of a MultiQueuingAlgorithmTreeQueue,
// it maintains a mapping of tenants to queriers in order to support dequeuing from an appropriate tenant
// if shuffle-sharding is enabled. A tenant has many queriers which can process its requests.
// A tenant has *all* queriers if:
//   - sharding is disabled (query-frontend.max-queriers-per-tenant=0)
//   - OR if max-queriers-per-tenant >= the number of queriers
//
// Queriers are assigned to a tenant via a shuffle-shard seed, which is consistently hashed from the tenant ID.
// tenantQuerierAssignments keeps track of these assignments, and determines which tenant requests a given querier can
// process when it is attempting to dequeue a request.
//
// The tenant-querier mapping is reshuffled when:
//   - a querier connection is added or removed
//   - it is detected during request enqueueing that a tenant's queriers were calculated from
//     an outdated max-queriers-per-tenant value
type tenantQuerierAssignments struct {
	// Sorted list of querier ids, used when shuffle-sharding queriers for tenant
	querierIDsSorted querierIDSlice

	// List of all tenants with queues, used for iteration when searching for next queue to handle.
	tenantIDOrder []TenantID
	tenantsByID   map[TenantID]*queueTenant

	// tenantOrderIndex is the index of the _last_ tenant dequeued from; it is passed by
	// the querier, and then updated to the index of the last tenant dequeued from, so it
	// can be returned to the querier. Newly connected queriers should pass -1 to start at the
	// beginning of tenantIDOrder.
	tenantOrderIndex int

	// tenantNodes tracks all tree nodes of the same name; it is used to track whether a tenant should be
	// removed from the tenantIDOrder when a node in Tree is exhausted.
	tenantNodes map[string][]*Node

	// Set of Querier IDs assigned to each tenant as determined by shuffle sharding.
	// If tenantQuerierIDs[tenantID] for a given tenantID is non-nil, only those queriers can handle
	// the tenant's requests,
	// tenantQuerierIDs[tenantID] is set to nil if sharding is off or available queriers <= tenant's maxQueriers.
	tenantQuerierIDs map[TenantID]map[QuerierID]struct{}

	// The querier currently making a dequeue request; updated before dequeues by setup.
	currentQuerier QuerierID
}

func newTenantQuerierAssignments() *tenantQuerierAssignments {
	return &tenantQuerierAssignments{
		querierIDsSorted: nil,
		tenantIDOrder:    nil,
		tenantsByID:      map[TenantID]*queueTenant{},
		tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{},
		tenantNodes:      map[string][]*Node{},
		currentQuerier:   "",
	}
}

// createOrUpdateTenant creates or updates a tenant into the tenant-querier assignment state.
//
// New tenants are added to the tenant order list and tenant-querier shards are shuffled if needed.
// Existing tenants have the tenant-querier shards shuffled only if their maxQueriers has changed.
func (tqa *tenantQuerierAssignments) createOrUpdateTenant(tenantID TenantID, maxQueriers int) error {
	if tenantID == emptyTenantID {
		// empty tenantID is not allowed; "" is used for free spot
		return ErrInvalidTenantID
	}

	if maxQueriers < 0 {
		maxQueriers = 0
	}

	tenant := tqa.tenantsByID[tenantID]

	if tenant == nil {
		tenant = &queueTenant{
			tenantID: tenantID,
			// maxQueriers 0 enables a later check to trigger tenant-querier assignment
			// for new queue tenants with shuffle sharding enabled
			maxQueriers:      0,
			shuffleShardSeed: util.ShuffleShardSeed(string(tenantID), ""),
			// orderIndex set to sentinel value to indicate it is not inserted yet
			orderIndex: -1,
		}
		for i, id := range tqa.tenantIDOrder {
			if id == emptyTenantID {
				// previously removed tenant not yet cleaned up; take its place
				tenant.orderIndex = i
				tqa.tenantIDOrder[i] = tenantID
				tqa.tenantsByID[tenantID] = tenant
				break
			}
		}

		if tenant.orderIndex < 0 {
			// there were no empty spaces in tenant order; append
			tenant.orderIndex = len(tqa.tenantIDOrder)
			tqa.tenantIDOrder = append(tqa.tenantIDOrder, tenantID)
			tqa.tenantsByID[tenantID] = tenant
		}
	}

	// tenant now either retrieved or created
	if tenant.maxQueriers != maxQueriers {
		// tenant queriers need to be computed/recomputed;
		// either this is a new tenant with sharding enabled,
		// or the tenant already existed but its maxQueriers has changed
		tenant.maxQueriers = maxQueriers
		tqa.shuffleTenantQueriers(tenantID, nil)
	}
	return nil
}

// removeTenant only manages deletion of a *queueTenant from tenantsByID. All other
// tenant deletion (e.g., from tenantIDOrder, or tenantNodes) is done during the dequeue operation,
// as we cannot remove from those things arbitrarily; we must check whether other tenant
// queues exist for the same tenant before removing.
func (tqa *tenantQuerierAssignments) removeTenant(tenantID TenantID) {
	tenant := tqa.tenantsByID[tenantID]
	if tenant == nil {
		return
	}
	delete(tqa.tenantsByID, tenantID)
}

// addQuerier adds the given querierID to tenantQuerierAssignments' querierIDsSorted. It does not do any checks to
// validate that a querier connection matching this ID exists. That logic is handled by querierConnections,
// and coordinated by the queueBroker.
func (tqa *tenantQuerierAssignments) addQuerier(querierID QuerierID) {
	tqa.querierIDsSorted = append(tqa.querierIDsSorted, querierID)
	sort.Sort(tqa.querierIDsSorted)
}

// removeQueriers deletes an arbitrary number of queriers from the querier connection manager, and returns true if
// the tenant-querier sharding was recomputed.
func (tqa *tenantQuerierAssignments) removeQueriers(querierIDs ...QuerierID) (resharded bool) {
	for _, querierID := range querierIDs {
		ix := tqa.querierIDsSorted.Search(querierID)
		if ix >= len(tqa.querierIDsSorted) || tqa.querierIDsSorted[ix] != querierID {
			panic("incorrect state of sorted queriers")
		}

		tqa.querierIDsSorted = append(tqa.querierIDsSorted[:ix], tqa.querierIDsSorted[ix+1:]...)
	}
	return tqa.recomputeTenantQueriers()
}

func (tqa *tenantQuerierAssignments) recomputeTenantQueriers() (resharded bool) {
	var scratchpad querierIDSlice
	for tenantID, tenant := range tqa.tenantsByID {
		if tenant.maxQueriers > 0 && tenant.maxQueriers < len(tqa.querierIDsSorted) && scratchpad == nil {
			// shuffle sharding is enabled and the number of queriers exceeds tenant maxQueriers,
			// meaning tenant querier assignments need computed via shuffle sharding;
			// allocate the scratchpad the first time this case is hit and it will be reused after
			scratchpad = make(querierIDSlice, 0, len(tqa.querierIDsSorted))
		}
		// operation must be on left to avoid short-circuiting and skipping the operation
		resharded = tqa.shuffleTenantQueriers(tenantID, scratchpad) || resharded
	}
	return resharded
}

func (tqa *tenantQuerierAssignments) shuffleTenantQueriers(tenantID TenantID, scratchpad querierIDSlice) (resharded bool) {
	tenant := tqa.tenantsByID[tenantID]
	if tenant == nil {
		return false
	}

	if tenant.maxQueriers == 0 || len(tqa.querierIDsSorted) <= tenant.maxQueriers {
		// shuffle shard is either disabled or calculation is unnecessary;
		prevQuerierIDSet := tqa.tenantQuerierIDs[tenantID]
		// assigning querier set to nil for the tenant indicates tenant can use all queriers
		tqa.tenantQuerierIDs[tenantID] = nil
		// tenant may have already been assigned all queriers; only indicate reshard if this changed
		return prevQuerierIDSet != nil
	}

	querierIDSet := make(map[QuerierID]struct{}, tenant.maxQueriers)
	rnd := rand.New(rand.NewSource(tenant.shuffleShardSeed))

	scratchpad = append(scratchpad[:0], tqa.querierIDsSorted...)

	last := len(scratchpad) - 1
	for i := 0; i < tenant.maxQueriers; i++ {
		r := rnd.Intn(last + 1)
		querierIDSet[scratchpad[r]] = struct{}{}
		// move selected item to the end, it won't be selected anymore.
		scratchpad[r], scratchpad[last] = scratchpad[last], scratchpad[r]
		last--
	}
	tqa.tenantQuerierIDs[tenantID] = querierIDSet
	return true
}

func (tqa *tenantQuerierAssignments) setup(dequeueArgs *DequeueArgs) {
	tqa.currentQuerier = dequeueArgs.querierID
	tqa.tenantOrderIndex = dequeueArgs.lastTenantIndex
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
func (tqa *tenantQuerierAssignments) dequeueSelectNode(node *Node) *Node {
	if node.isLeaf() || len(node.queueMap) == 0 {
		return node
	}

	// can't get a tenant if no querier set
	if tqa.currentQuerier == "" {
		return nil
	}

	// tenantOrderIndex is set to the _last_ tenant we dequeued from; advance queue position for dequeue
	tqa.tenantOrderIndex++
	if tqa.tenantOrderIndex >= len(tqa.tenantIDOrder) {
		tqa.tenantOrderIndex = 0
	}

	checkIndex := tqa.tenantOrderIndex

	// iterate through the tenant order until we find a tenant that is assigned to the current querier, or
	// have checked the entire tenantIDOrder, whichever comes first
	for iters := 0; iters < len(tqa.tenantIDOrder); iters++ {
		if checkIndex >= len(tqa.tenantIDOrder) {
			// do not use modulo to wrap this index; tenantOrderIndex is provided from an outer process
			// which does not know if the tenant list has changed since the last dequeue
			// wrapping with modulo after shrinking the list could cause us to skip tenants
			checkIndex = 0
		}
		tenantID := tqa.tenantIDOrder[checkIndex]
		tenantName := string(tenantID)

		if _, ok := node.queueMap[tenantName]; !ok {
			// tenant not in _this_ node's children, move on
			checkIndex++
			continue
		}

		// if the tenant-querier set is nil, any querier can serve this tenant
		if tqa.tenantQuerierIDs[tenantID] == nil {
			tqa.tenantOrderIndex = checkIndex
			return node.queueMap[tenantName]
		}
		// otherwise, check if the querier is assigned to this tenant
		if tenantQuerierSet, ok := tqa.tenantQuerierIDs[tenantID]; ok {
			if _, ok := tenantQuerierSet[tqa.currentQuerier]; ok {
				tqa.tenantOrderIndex = checkIndex
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
//   - tenantIDOrder iff there are no other nodes by the same name in tenantNodes. If the child is at the end of
//     tenantIDOrder, it is removed outright; otherwise, it is replaced with an empty ("") element
//
// dequeueUpdateState would normally also handle incrementing the queue position after performing a dequeue, but
// tenantQuerierAssignments currently expects the caller to handle this by having the querier set tenantOrderIndex.
func (tqa *tenantQuerierAssignments) dequeueUpdateState(node *Node, dequeuedFrom *Node) {
	// if dequeuedFrom is nil or is not empty, we don't need to do anything;
	// position updates will be handled by the caller, and we don't need to remove any nodes.
	if dequeuedFrom == nil || !dequeuedFrom.IsEmpty() {
		return
	}

	// delete from the node's children
	childName := dequeuedFrom.Name()
	delete(node.queueMap, childName)

	// delete from shared tenantNodes
	for i, tenantNode := range tqa.tenantNodes[childName] {
		if tenantNode == dequeuedFrom {
			tqa.tenantNodes[childName] = append(tqa.tenantNodes[childName][:i], tqa.tenantNodes[childName][i+1:]...)
		}
	}

	// check tenantNodes; we only remove from tenantIDOrder if all nodes with this name are gone.
	// If the removed child is at the _end_ of tenantIDOrder, we remove it outright; otherwise,
	// we replace it with an empty string. Removal of elements from anywhere other than the end of the slice
	// would re-index all tenant IDs, resulting in skipped tenants when starting iteration from the
	// querier-provided lastTenantIndex.
	removeFromSharedQueueOrder := len(tqa.tenantNodes[childName]) == 0

	if removeFromSharedQueueOrder {
		for idx, name := range tqa.tenantIDOrder {
			if string(name) == childName {
				tqa.tenantIDOrder[idx] = emptyTenantID
			}
		}
		// clear all sequential empty elements from tenantIDOrder
		lastElementIndex := len(tqa.tenantIDOrder) - 1
		for i := lastElementIndex; i >= 0 && tqa.tenantIDOrder[i] == ""; i-- {
			tqa.tenantIDOrder = tqa.tenantIDOrder[:i]
		}
	}
}

// addChildNode adds a child to:
//   - the node's own queueMap
//   - tenantNodes, which maintains a slice of all nodes with the same name. tenantNodes is checked on node deletion
//     to ensure that we only remove a tenant from tenantIDOrder if _all_ nodes with the same name have been removed.
//   - tenantIDOrder iff the node did not already exist in tenantNodes or tenantIDOrder. addChildNode will place
//     a new tenant in the first empty ("") element it finds in tenantIDOrder, or at the end if no empty elements exist.
func (tqa *tenantQuerierAssignments) addChildNode(parent, child *Node) {
	childName := child.Name()
	_, tenantHasAnyQueue := tqa.tenantNodes[childName]

	// add childNode to node's queueMap,
	// and to the shared tenantNodes map
	parent.queueMap[childName] = child
	tqa.tenantNodes[childName] = append(tqa.tenantNodes[childName], child)

	// if child has any queue, it should already be in tenantIDOrder, return without altering order
	if tenantHasAnyQueue {
		return
	}

	// otherwise, replace the first empty element in n.tenantIDOrder with childName, or append to the end
	for i, elt := range tqa.tenantIDOrder {
		// if we encounter a tenant with this name in tenantIDOrder already, return without altering order.
		// This is a weak check (childName could exist farther down tenantIDOrder, but be inserted here as well),
		// but should be fine, since the only time we hit this case is when createOrUpdateTenant is called, which
		// only happens immediately before enqueueing.
		if elt == TenantID(childName) {
			return
		}
		if elt == "" {
			tqa.tenantIDOrder[i] = TenantID(childName)
			return
		}
	}
	// if we get here, we didn't find any empty elements in tenantIDOrder; append
	tqa.tenantIDOrder = append(tqa.tenantIDOrder, TenantID(childName))
}
