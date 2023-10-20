// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/user_queues.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queue

import (
	"container/list"
	"math/rand"
	"sort"
	"time"

	"github.com/grafana/mimir/pkg/util"
)

type TenantID string

const emptyTenantID = TenantID("")

type QuerierID string
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

type querierConn struct {
	// Number of active connections.
	connections int

	// True if the querier notified it's gracefully shutting down.
	shuttingDown bool

	// When the last connection has been unregistered.
	disconnectedAt time.Time
}

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

	// Tenant assigned querier ID set as determined by shuffle sharding.
	// If tenant querier ID set is not nil, only those queriers can handle the tenant's requests,
	// Tenant querier ID is set to nil if sharding is off or available queriers <= tenant's maxQueriers.
	tenantQuerierIDs map[TenantID]map[QuerierID]struct{}
}

type queueTenant struct {
	// seed for shuffle sharding of queriers; computed from tenantID only,
	// and is therefore consistent between different frontends.
	shuffleShardSeed int64

	// points up to tenant order to enable efficient removal
	orderIndex int

	maxQueriers int
}

// queueBroker encapsulates access to tenant queues for pending requests
// and maintains consistency with the tenant-querier assignments
type queueBroker struct {
	tenantQueues map[TenantID]*tenantQueue

	tenantQuerierAssignments tenantQuerierAssignments

	maxTenantQueueSize int
}

type tenantQueue struct {
	requests *list.List
}

func newQueueBroker(maxTenantQueueSize int, forgetDelay time.Duration) *queueBroker {
	return &queueBroker{
		tenantQueues: map[TenantID]*tenantQueue{},
		tenantQuerierAssignments: tenantQuerierAssignments{
			queriersByID:       map[QuerierID]*querierConn{},
			querierIDsSorted:   nil,
			querierForgetDelay: forgetDelay,
			tenantIDOrder:      nil,
			tenantsByID:        map[TenantID]*queueTenant{},
			tenantQuerierIDs:   map[TenantID]map[QuerierID]struct{}{},
		},
		maxTenantQueueSize: maxTenantQueueSize,
	}
}

func (qb *queueBroker) len() int {
	return len(qb.tenantQueues)
}

func (qb *queueBroker) enqueueRequestBack(r requestToEnqueue) error {
	queue, err := qb.getOrAddTenantQueue(r.tenantID, r.maxQueriers)
	if err != nil {
		return err
	}

	if queue.Len()+1 > qb.maxTenantQueueSize {
		return ErrTooManyRequests
	}

	queue.PushBack(r.req)
	return nil
}

func (qb *queueBroker) enqueueRequestFront(r requestToEnqueue) error {
	queue, err := qb.getOrAddTenantQueue(r.tenantID, r.maxQueriers)
	if err != nil {
		return err
	}

	queue.PushFront(r.req)
	return nil
}

// getOrAddTenantQueue returns existing or new queue for tenant.
// maxQueriers is used to compute which queriers should handle requests for this tenant.
// If maxQueriers is <= 0, all queriers can handle this tenant's requests.
// If maxQueriers has changed since the last call, queriers for this are recomputed.
func (qb *queueBroker) getOrAddTenantQueue(tenantID TenantID, maxQueriers int) (*list.List, error) {
	_, err := qb.tenantQuerierAssignments.getOrAddTenant(tenantID, maxQueriers)
	if err != nil {
		return nil, err
	}
	queue := qb.tenantQueues[tenantID]

	if queue == nil {
		queue = &tenantQueue{
			requests: list.New(),
		}
		qb.tenantQueues[tenantID] = queue
	}

	return queue.requests, nil
}

func (qb *queueBroker) dequeueRequestForQuerier(lastTenantIndex int, querierID QuerierID) (Request, TenantID, int, error) {
	tenantID, tenantIndex, err := qb.tenantQuerierAssignments.getNextTenantIDForQuerier(lastTenantIndex, querierID)
	if err != nil {
		return nil, tenantID, tenantIndex, err
	}

	tenantQueue := qb.tenantQueues[tenantID]
	if tenantQueue == nil {
		return nil, tenantID, tenantIndex, nil
	}

	// queue will be nonempty as empty queues are deleted
	queueElement := tenantQueue.requests.Front()
	tenantQueue.requests.Remove(queueElement)

	if tenantQueue.requests.Len() == 0 {
		qb.deleteQueue(tenantID)
	}
	return queueElement.Value, tenantID, tenantIndex, nil

}

// Finds next queue for the querier. To support fair scheduling between tenants, client is expected
// to pass last tenant index returned by this function as argument. If there was no previous
// last tenant index, use -1.
//
// getNextQueueForQuerier returns an error if the querier has already notified this scheduler that it is shutting down.
func (qb *queueBroker) getNextQueueForQuerier(lastTenantIndex int, querierID QuerierID) (*list.List, TenantID, int, error) {
	nextTenantID, nextTenantIndex, err := qb.tenantQuerierAssignments.getNextTenantIDForQuerier(lastTenantIndex, querierID)
	if err != nil || nextTenantID == emptyTenantID {
		return nil, nextTenantID, nextTenantIndex, err
	}

	tenantQueue := qb.tenantQueues[nextTenantID]
	return tenantQueue.requests, nextTenantID, nextTenantIndex, nil
}

func (qb *queueBroker) addQuerierConnection(querierID QuerierID) {
	qb.tenantQuerierAssignments.addQuerierConnection(querierID)
}

func (qb *queueBroker) removeQuerierConnection(querierID QuerierID, now time.Time) {
	qb.tenantQuerierAssignments.removeQuerierConnection(querierID, now)
}

func (qb *queueBroker) notifyQuerierShutdown(querierID QuerierID) {
	qb.tenantQuerierAssignments.notifyQuerierShutdown(querierID)
}

func (qb *queueBroker) forgetDisconnectedQueriers(now time.Time) int {
	return qb.tenantQuerierAssignments.forgetDisconnectedQueriers(now)
}

func (qb *queueBroker) deleteQueue(tenantID TenantID) {
	tenantQueue := qb.tenantQueues[tenantID]
	if tenantQueue == nil {
		return
	}
	delete(qb.tenantQueues, tenantID)
	qb.tenantQuerierAssignments.removeTenant(tenantID)
}

func (tqa *tenantQuerierAssignments) getNextTenantIDForQuerier(lastTenantIndex int, querierID QuerierID) (TenantID, int, error) {
	// check if querier is registered and is not shutting down
	if q := tqa.queriersByID[querierID]; q == nil || q.shuttingDown {
		return emptyTenantID, lastTenantIndex, ErrQuerierShuttingDown
	}
	tenantOrderIndex := lastTenantIndex
	for iters := 0; iters < len(tqa.tenantIDOrder); iters++ {
		tenantOrderIndex++
		if tenantOrderIndex >= len(tqa.tenantIDOrder) {
			// do not use modulo (e.g. i = (i + 1) % len(slice)) to wrap around this list,
			// as it could skip tenant IDs if the slice has changed size since the last call
			tenantOrderIndex = 0
		}

		tenantID := tqa.tenantIDOrder[tenantOrderIndex]
		if tenantID == emptyTenantID {
			continue
		}

		tenantQuerierSet := tqa.tenantQuerierIDs[tenantID]
		if tenantQuerierSet == nil {
			// tenant can use all queriers
			return tenantID, tenantOrderIndex, nil
		} else if _, ok := tenantQuerierSet[querierID]; ok {
			// tenant is assigned this querier
			return tenantID, tenantOrderIndex, nil
		}
	}

	return emptyTenantID, lastTenantIndex, nil
}

func (tqa *tenantQuerierAssignments) getOrAddTenant(tenantID TenantID, maxQueriers int) (*queueTenant, error) {
	if tenantID == emptyTenantID {
		// empty tenantID is not allowed; "" is used for free spot
		return nil, ErrInvalidTenantID
	}

	if maxQueriers < 0 {
		maxQueriers = 0
	}

	tenant := tqa.tenantsByID[tenantID]

	if tenant == nil {
		tenant = &queueTenant{
			shuffleShardSeed: util.ShuffleShardSeed(string(tenantID), ""),
			// orderIndex set to sentinel value to indicate it is not inserted yet
			orderIndex: -1,
			// maxQueriers 0 enables a later check to trigger tenant-querier assignment
			// for new queue tenants with shuffle sharding enabled
			maxQueriers: 0,
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

	// tenant now either retrieved or created;
	// tenant queriers need computed for new tenant if sharding enabled,
	// or if the tenant already existed but its maxQueriers has changed
	if tenant.maxQueriers != maxQueriers {
		tenant.maxQueriers = maxQueriers
		tqa.shuffleTenantQueriers(tenantID, nil)
	}
	return tenant, nil
}

func (tqa *tenantQuerierAssignments) addQuerierConnection(querierID QuerierID) {
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

	tqa.recomputeTenantQueriers()
}

func (tqa *tenantQuerierAssignments) removeTenant(tenantID TenantID) {
	tenant := tqa.tenantsByID[tenantID]
	if tenant == nil {
		return
	}
	delete(tqa.tenantsByID, tenantID)
	tqa.tenantIDOrder[tenant.orderIndex] = emptyTenantID

	// Shrink tenant list if possible by removing empty tenant IDs.
	// We remove only from the end; removing from the middle would re-index all tenant IDs
	// and skip tenants when starting iteration from a querier-provided lastTenantIndex.
	// Empty tenant IDs stuck in the middle of the slice are handled
	// by replacing them when a new tenant ID arrives in the queue.
	for i := len(tqa.tenantIDOrder) - 1; i >= 0 && tqa.tenantIDOrder[i] == emptyTenantID; i-- {
		tqa.tenantIDOrder = tqa.tenantIDOrder[:i]
	}
}

func (tqa *tenantQuerierAssignments) removeQuerierConnection(querierID QuerierID, now time.Time) {
	querier := tqa.queriersByID[querierID]
	if querier == nil || querier.connections <= 0 {
		panic("unexpected number of connections for querier")
	}

	// Decrease the number of active connections.
	querier.connections--
	if querier.connections > 0 {
		return
	}

	// There no more active connections. If the forget delay is configured then
	// we can remove it only if querier has announced a graceful shutdown.
	if querier.shuttingDown || tqa.querierForgetDelay == 0 {
		tqa.removeQuerier(querierID)
		return
	}

	// No graceful shutdown has been notified yet, so we should track the current time
	// so that we'll remove the querier as soon as we receive the graceful shutdown
	// notification (if any) or once the threshold expires.
	querier.disconnectedAt = now
}

func (tqa *tenantQuerierAssignments) removeQuerier(querierID QuerierID) {
	delete(tqa.queriersByID, querierID)

	ix := tqa.querierIDsSorted.Search(querierID)
	if ix >= len(tqa.querierIDsSorted) || tqa.querierIDsSorted[ix] != querierID {
		panic("incorrect state of sorted queriers")
	}

	tqa.querierIDsSorted = append(tqa.querierIDsSorted[:ix], tqa.querierIDsSorted[ix+1:]...)

	tqa.recomputeTenantQueriers()
}

// notifyQuerierShutdown records that a querier has sent notification about a graceful shutdown.
func (tqa *tenantQuerierAssignments) notifyQuerierShutdown(querierID QuerierID) {
	querier := tqa.queriersByID[querierID]
	if querier == nil {
		// The querier may have already been removed, so we just ignore it.
		return
	}

	// If there are no more connections, we should remove the querier.
	if querier.connections == 0 {
		tqa.removeQuerier(querierID)
		return
	}

	// Otherwise we should annotate we received a graceful shutdown notification
	// and the querier will be removed once all connections are unregistered.
	querier.shuttingDown = true
}

// forgetDisconnectedQueriers removes all disconnected queriers that have gone since at least
// the forget delay. Returns the number of forgotten queriers.
func (tqa *tenantQuerierAssignments) forgetDisconnectedQueriers(now time.Time) int {
	// Nothing to do if the forget delay is disabled.
	if tqa.querierForgetDelay == 0 {
		return 0
	}

	// Remove all queriers with no connections that have gone since at least the forget delay.
	threshold := now.Add(-tqa.querierForgetDelay)
	forgotten := 0

	for querierID := range tqa.queriersByID {
		if querier := tqa.queriersByID[querierID]; querier.connections == 0 && querier.disconnectedAt.Before(threshold) {
			tqa.removeQuerier(querierID)
			forgotten++
		}
	}

	return forgotten
}

func (tqa *tenantQuerierAssignments) recomputeTenantQueriers() {
	// Only allocate the scratchpad the first time we need it.
	// If shuffle-sharding is disabled, it will not be used.
	var scratchpad querierIDSlice

	for tenantID, tenant := range tqa.tenantsByID {
		if tenant.maxQueriers > 0 && tenant.maxQueriers < len(tqa.querierIDsSorted) && scratchpad == nil {
			scratchpad = make(querierIDSlice, 0, len(tqa.querierIDsSorted))
		}

		tqa.shuffleTenantQueriers(tenantID, scratchpad)
	}
}

func (tqa *tenantQuerierAssignments) shuffleTenantQueriers(tenantID TenantID, scratchpad querierIDSlice) {
	tenant := tqa.tenantsByID[tenantID]
	if tenant == nil {
		return
	}

	if tenant.maxQueriers == 0 || len(tqa.querierIDsSorted) <= tenant.maxQueriers {
		// shuffle shard is either disabled or calculation is unnecessary
		tqa.tenantQuerierIDs[tenantID] = nil
		return
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
}
