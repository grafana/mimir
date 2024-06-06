// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/user_queues.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queue

import (
	"math/rand"
	"sort"
	"time"

	"github.com/grafana/mimir/pkg/util"
)

type queueTenant struct {
	tenantID    TenantID
	maxQueriers int

	// seed for shuffle sharding of queriers; computed from tenantID only,
	// and is therefore consistent between different frontends.
	shuffleShardSeed int64

	// points up to tenant order to enable efficient removal
	orderIndex int
}

type querierConn struct {
	// Number of active connections.
	connections int

	// True if the querier notified it's gracefully shutting down.
	shuttingDown bool

	// When the last connection has been unregistered.
	disconnectedAt time.Time
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

// tenantQuerierAssignments implements DequeueAlgorithm. In the context of a Tree, it maintains a mapping of
// tenants to queriers in order to support dequeueing from an appropriate tenant if shuffle-sharding is enabled.
type tenantQuerierAssignments struct {
	// a tenant has many queriers
	// a tenant has *all* queriers if:
	//  - sharding is disabled (max-queriers-per-tenant=0)
	//  - or if max-queriers-per-tenant >= the number of queriers
	//
	// Tenant -> Queriers is the core relationship randomized from the shuffle shard seed.
	// The shuffle shard seed is itself consistently hashed from the tenant ID.
	// However, the most common operation is the querier asking for its next request,
	// which requires a relatively efficient lookup or check of Querier -> Tenant.
	//
	// Reshuffling is done when:
	//  - a querier connection is added or removed
	//  - it is detected during request enqueueing that a tenant's queriers
	//    were calculated from an outdated max-queriers-per-tenant value

	queriersByID map[QuerierID]*querierConn
	// Sorted list of querier ids, used when shuffle sharding queriers for tenant
	querierIDsSorted querierIDSlice

	// How long to wait before removing a querier which has got disconnected
	// but hasn't notified about a graceful shutdown.
	querierForgetDelay time.Duration

	// List of all tenants with queues, used for iteration when searching for next queue to handle.
	tenantIDOrder []TenantID
	tenantsByID   map[TenantID]*queueTenant
	// tenantOrderIndex is the index of the _last_ tenant dequeued from; it is passed by
	// the querier, and then updated to the index of the last tenant dequeued from, so it
	// can be returned to the querier. Newly connected queries should pass -1 to start at the
	// beginning of tenantIDOrder.
	tenantOrderIndex int
	tenantNodes      map[string][]*Node

	// Tenant assigned querier ID set as determined by shuffle sharding.
	// If tenant querier ID set is not nil, only those queriers can handle the tenant's requests,
	// Tenant querier ID is set to nil if sharding is off or available queriers <= tenant's maxQueriers.
	tenantQuerierIDs map[TenantID]map[QuerierID]struct{}
	currentQuerier   *QuerierID
}

func (tqa *tenantQuerierAssignments) getTenant(tenantID TenantID) (*queueTenant, error) {
	if tenantID == emptyTenantID {
		return nil, ErrInvalidTenantID
	}
	tenant := tqa.tenantsByID[tenantID]
	return tenant, nil
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

func (tqa *tenantQuerierAssignments) addQuerierConnection(querierID QuerierID) (resharded bool) {
	querier := tqa.queriersByID[querierID]
	if querier != nil {
		querier.connections++

		// Reset in case the querier re-connected while it was in the forget waiting period.
		querier.shuttingDown = false
		querier.disconnectedAt = time.Time{}

		return
	}

	// First connection from this querier.
	tqa.queriersByID[querierID] = &querierConn{connections: 1}
	tqa.querierIDsSorted = append(tqa.querierIDsSorted, querierID)
	sort.Sort(tqa.querierIDsSorted)

	return tqa.recomputeTenantQueriers()
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

func (tqa *tenantQuerierAssignments) removeQuerierConnection(querierID QuerierID, now time.Time) (resharded bool) {
	querier := tqa.queriersByID[querierID]
	if querier == nil || querier.connections <= 0 {
		panic("unexpected number of connections for querier")
	}

	// Decrease the number of active connections.
	querier.connections--
	if querier.connections > 0 {
		return false
	}

	// No more active connections. We can remove the querier only if
	// the querier has sent a shutdown signal or if no forget delay is enabled.
	if querier.shuttingDown || tqa.querierForgetDelay == 0 {
		return tqa.removeQuerier(querierID)
	}

	// No graceful shutdown has been notified yet, so we should track the current time
	// so that we'll remove the querier as soon as we receive the graceful shutdown
	// notification (if any) or once the threshold expires.
	querier.disconnectedAt = now
	return false
}

// removeQuerier deletes a querier from the tenant-querier assignments.
// Returns true if tenant-querier reshard was triggered.
func (tqa *tenantQuerierAssignments) removeQuerier(querierID QuerierID) (resharded bool) {
	delete(tqa.queriersByID, querierID)

	ix := tqa.querierIDsSorted.Search(querierID)
	if ix >= len(tqa.querierIDsSorted) || tqa.querierIDsSorted[ix] != querierID {
		panic("incorrect state of sorted queriers")
	}

	tqa.querierIDsSorted = append(tqa.querierIDsSorted[:ix], tqa.querierIDsSorted[ix+1:]...)

	return tqa.recomputeTenantQueriers()
}

// notifyQuerierShutdown handles a graceful shutdown notification from a querier.
// Returns true if tenant-querier reshard was triggered.
func (tqa *tenantQuerierAssignments) notifyQuerierShutdown(querierID QuerierID) (resharded bool) {
	querier := tqa.queriersByID[querierID]
	if querier == nil {
		// The querier may have already been removed, so we just ignore it.
		return false
	}

	// If there are no more connections, we should remove the querier - Shutdown signals ignore forgetDelay.
	// forgetDelay is only for queriers which have deregistered all connections but have not sent a shutdown signal
	if querier.connections == 0 {
		tqa.removeQuerier(querierID)
		return
	}

	// place in graceful shutdown state; any queued requests to dispatch queries
	// to this querier will receive error responses until all querier workers disconnect
	querier.shuttingDown = true
	return false
}

// forgetDisconnectedQueriers removes all queriers which have had zero connections for longer than the forget delay.
// Returns true if tenant-querier reshard was triggered.
func (tqa *tenantQuerierAssignments) forgetDisconnectedQueriers(now time.Time) (resharded bool) {
	// if forget delay is disabled, removal is done immediately on querier disconnect or shutdown; do nothing
	if tqa.querierForgetDelay == 0 {
		return false
	}

	// Remove all queriers with no connections that have gone since at least the forget delay.
	threshold := now.Add(-tqa.querierForgetDelay)
	for querierID := range tqa.queriersByID {
		if querier := tqa.queriersByID[querierID]; querier.connections == 0 && querier.disconnectedAt.Before(threshold) {
			// operation must be on left to avoid short-circuiting and skipping the operation
			resharded = tqa.removeQuerier(querierID) || resharded
		}
	}

	return resharded
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

// dequeueGetNode chooses the next node to dequeue from based on tenantIDOrder and tenantOrderIndex, which are
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
func (tqa *tenantQuerierAssignments) dequeueGetNode(n *Node) (*Node, bool) {
	// can't get a tenant if no querier set
	if tqa.currentQuerier == nil {
		return nil, true
	}

	checkedAllNodes := n.childrenChecked == len(n.queueMap)+1 // must check local queue as well

	// advance queue position for dequeue
	tqa.tenantOrderIndex++
	if tqa.tenantOrderIndex >= len(tqa.tenantIDOrder) {
		tqa.tenantOrderIndex = localQueueIndex
	}

	// no children
	if len(n.queueMap) == 0 || tqa.tenantOrderIndex == localQueueIndex {
		return n, checkedAllNodes
	}

	checkIndex := tqa.tenantOrderIndex

	for iters := 0; iters < len(tqa.tenantIDOrder); iters++ {
		if checkIndex >= len(tqa.tenantIDOrder) {
			checkIndex = 0
		}
		tenantID := tqa.tenantIDOrder[checkIndex]
		tenantName := string(tenantID)

		if _, ok := n.queueMap[tenantName]; !ok {
			// tenant not in _this_ node's children, move on
			checkIndex++
			continue
		}

		// increment nodes checked even if not in tenant-querier map
		n.childrenChecked++
		checkedAllNodes = n.childrenChecked == len(n.queueMap)+1

		// if the tenant-querier set is nil, any querier can serve this tenant
		if tqa.tenantQuerierIDs[tenantID] == nil {
			tqa.tenantOrderIndex = checkIndex
			return n.queueMap[tenantName], checkedAllNodes
		}
		if tenantQuerierSet, ok := tqa.tenantQuerierIDs[tenantID]; ok {
			if _, ok := tenantQuerierSet[*tqa.currentQuerier]; ok {
				tqa.tenantOrderIndex = checkIndex
				return n.queueMap[tenantName], checkedAllNodes
			}
		}
		checkIndex++
	}
	return nil, checkedAllNodes
}

// dequeueUpdateState updates the node state to reflect the number of children that have been checked
// for dequeueable elements, so that dequeueGetNode will stop checking if it has checked all possible nodes.
func (tqa *tenantQuerierAssignments) dequeueUpdateState(n *Node, v any, _ bool) {
	// we need to reset our checked nodes if we found a value to dequeue
	if v != nil {
		n.childrenChecked = 0
		return
	}

	n.childrenChecked++
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

// deleteChildNode removes a child from:
//   - parent's queueMap,
//   - tenantNodes
//   - tenantIDOrder iff there are no other nodes by the same name in tenantNodes. If the child is at the end of
//     tenantIDOrder, it is removed outright; otherwise, it is replaced with an empty ("") element
func (tqa *tenantQuerierAssignments) deleteChildNode(parent, child *Node) bool {
	childName := child.Name()

	// delete from parent's children
	delete(parent.queueMap, childName)

	// delete from shared tenantNodes
	for i, tenantNode := range tqa.tenantNodes[childName] {
		if tenantNode == child {
			tqa.tenantNodes[childName] = append(tqa.tenantNodes[childName][:i], tqa.tenantNodes[childName][i+1:]...)
		}
	}

	// check tenantNodes; we only remove from tenantIDOrder if all nodes with this name are gone.
	// If the removed child is at the _end_ of tenantIDOrder, we remove it outright; otherwise,
	// we replace it with an empty string so as not to reindex all slice elements
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
	// The only time we remove elements from tenantIDOrder is when they are empty nodes, _and_ at the end of
	// tenantIDOrder, so we will never remove a tenant from tenantIDOrder at a position before tenantOrderIndex.
	// Thus, we will never need to decrement tenantOrderIndex.
	return false
}
