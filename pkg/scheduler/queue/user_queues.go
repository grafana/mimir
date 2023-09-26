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

	"golang.org/x/exp/slices"

	"github.com/grafana/mimir/pkg/util"
)

type querierConn struct {
	// Number of active connections.
	connections int

	// True if the querier notified it's gracefully shutting down.
	shuttingDown bool

	// When the last connection has been unregistered.
	disconnectedAt time.Time
}

type tenantQuerierState struct {
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

	queriersByID map[string]*querierConn
	// Sorted list of querier ids, used when shuffle sharding queriers for tenant
	querierIDsSorted []string

	// How long to wait before removing a querier which has got disconnected
	// but hasn't notified about a graceful shutdown.
	querierForgetDelay time.Duration

	// List of all tenants with queues, used for iteration when searching for next queue to handle.
	// Tenants removed from the middle are replaced with "". To avoid skipping users during iteration,
	// we only shrink this list when there are ""'s at the end of it.
	tenantIDOrder []string
	tenantsByID   map[string]*queueTenant

	// Tenant assigned querier ID set as determined by shuffle sharding.
	// If tenant querier ID set is not nil, only those queriers can handle the tenant's requests,
	// Tenant querier ID is set to nil if sharding is off or available queriers <= tenant's maxQueriers.
	tenantQuerierIDs map[string]map[string]struct{}
}

type queueTenant struct {
	// seed for shuffle sharding of queriers; computed from userID only,
	// and is therefore consistent between different frontends.
	shuffleShardSeed int64

	// points up to user order to enable efficient removal
	orderIndex int

	maxQueriers int
}

// This struct holds user queues for pending requests. It also keeps track of connected queriers,
// and mapping between users and queriers.
type queues struct {
	tenantQueues map[string]*userQueue

	tenantQuerierState tenantQuerierState

	maxUserQueueSize int
}

type userQueue struct {
	requests *list.List
}

func newUserQueues(maxUserQueueSize int, forgetDelay time.Duration) *queues {
	return &queues{
		tenantQueues: map[string]*userQueue{},
		tenantQuerierState: tenantQuerierState{
			queriersByID:       map[string]*querierConn{},
			querierIDsSorted:   nil,
			querierForgetDelay: forgetDelay,
			tenantIDOrder:      nil,
			tenantsByID:        map[string]*queueTenant{},
			tenantQuerierIDs:   map[string]map[string]struct{}{},
		},
		maxUserQueueSize: maxUserQueueSize,
	}
}

func (q *queues) len() int {
	return len(q.tenantQueues)
}

func (q *queues) deleteQueue(userID string) {
	uq := q.tenantQueues[userID]
	if uq == nil {
		return
	}
	u := q.tenantQuerierState.tenantsByID[userID]

	delete(q.tenantQueues, userID)
	delete(q.tenantQuerierState.tenantsByID, userID)
	q.tenantQuerierState.tenantIDOrder[u.orderIndex] = ""

	// Shrink users list size if possible. This is safe, and no users will be skipped during iteration.
	for ix := len(q.tenantQuerierState.tenantIDOrder) - 1; ix >= 0 && q.tenantQuerierState.tenantIDOrder[ix] == ""; ix-- {
		q.tenantQuerierState.tenantIDOrder = q.tenantQuerierState.tenantIDOrder[:ix]
	}
}

// Returns existing or new queue for user.
// MaxQueriers is used to compute which queriers should handle requests for this user.
// If maxQueriers is <= 0, all queriers can handle this user's requests.
// If maxQueriers has changed since the last call, queriers for this are recomputed.
func (q *queues) getOrAddTenantQueue(tenantID string, maxQueriers int) *list.List {
	tenant := q.tenantQuerierState.getOrAddTenant(tenantID, maxQueriers)
	if tenant == nil {
		return nil
	}
	queue := q.tenantQueues[tenantID]

	if queue == nil {
		queue = &userQueue{
			requests: list.New(),
		}
		q.tenantQueues[tenantID] = queue
	}

	return queue.requests
}

// Finds next queue for the querier. To support fair scheduling between users, client is expected
// to pass last user index returned by this function as argument. If there was no previous
// last user index, use -1.
func (q *queues) getNextQueueForQuerier(lastUserIndex int, querierID string) (*list.List, string, int, error) {
	userIndex := lastUserIndex

	// Ensure the querier is not shutting down. If the querier is shutting down, we shouldn't forward
	// any more queries to it.
	if info := q.tenantQuerierState.queriersByID[querierID]; info == nil || info.shuttingDown {
		return nil, "", userIndex, ErrQuerierShuttingDown
	}

	for iters := 0; iters < len(q.tenantQuerierState.tenantIDOrder); iters++ {
		userIndex = userIndex + 1

		// Don't use "mod len(q.tenantQuerierState.tenantIDOrder)", as that could skip users at the beginning of the list
		// for example when q.tenantQuerierState.tenantIDOrder has shrunk since last call.
		if userIndex >= len(q.tenantQuerierState.tenantIDOrder) {
			userIndex = 0
		}

		userID := q.tenantQuerierState.tenantIDOrder[userIndex]
		if userID == "" {
			continue
		}

		userQueue := q.tenantQueues[userID]

		if querierSet := q.tenantQuerierState.tenantQuerierIDs[userID]; querierSet != nil {
			if _, ok := querierSet[querierID]; !ok {
				// This querier is not handling the user.
				continue
			}
		}

		return userQueue.requests, userID, userIndex, nil
	}
	return nil, "", userIndex, nil
}

func (tqs *tenantQuerierState) getOrAddTenant(tenantID string, maxQueriers int) *queueTenant {
	// empty tenantID is not allowed; "" is used for free spot
	if tenantID == "" {
		return nil
	}

	if maxQueriers < 0 {
		maxQueriers = 0
	}

	tenant := tqs.tenantsByID[tenantID]

	if tenant == nil {
		tenant = &queueTenant{
			shuffleShardSeed: util.ShuffleShardSeed(tenantID, ""),
			orderIndex:       -1,
			maxQueriers:      0,
		}
		for i, id := range tqs.tenantIDOrder {
			if id == "" {
				// previously removed tenant not yet cleaned up; take its place
				tenant.orderIndex = i
				tqs.tenantIDOrder[i] = tenantID
				tqs.tenantsByID[tenantID] = tenant
				break
			}
		}

		if tenant.orderIndex < 0 {
			// no empty spaces in tenant order; append
			tenant.orderIndex = len(tqs.tenantIDOrder)
			tqs.tenantIDOrder = append(tqs.tenantIDOrder, tenantID)
			tqs.tenantsByID[tenantID] = tenant
		}
	}

	// tenant now either retrieved or created;
	// tenant queriers need computed for new tenant if sharding enabled,
	// or if the tenant already existed but its maxQueriers has changed
	if tenant.maxQueriers != maxQueriers {
		tenant.maxQueriers = maxQueriers
		tqs.shuffleTenantQueriers(tenantID, nil)
	}
	return tenant
}

func (tqs *tenantQuerierState) addQuerierConnection(querierID string) {
	querier := tqs.queriersByID[querierID]
	if querier != nil {
		querier.connections++

		// Reset in case the querier re-connected while it was in the forget waiting period.
		querier.shuttingDown = false
		querier.disconnectedAt = time.Time{}

		return
	}

	// First connection from this querier.
	tqs.queriersByID[querierID] = &querierConn{connections: 1}
	tqs.querierIDsSorted = append(tqs.querierIDsSorted, querierID)
	slices.Sort(tqs.querierIDsSorted)

	tqs.recomputeTenantQueriers()
}

func (tqs *tenantQuerierState) removeQuerierConnection(querierID string, now time.Time) {
	querier := tqs.queriersByID[querierID]
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
	if querier.shuttingDown || tqs.querierForgetDelay == 0 {
		tqs.removeQuerier(querierID)
		return
	}

	// No graceful shutdown has been notified yet, so we should track the current time
	// so that we'll remove the querier as soon as we receive the graceful shutdown
	// notification (if any) or once the threshold expires.
	querier.disconnectedAt = now
}

func (tqs *tenantQuerierState) removeQuerier(querierID string) {
	delete(tqs.queriersByID, querierID)

	ix := sort.SearchStrings(tqs.querierIDsSorted, querierID)
	if ix >= len(tqs.querierIDsSorted) || tqs.querierIDsSorted[ix] != querierID {
		panic("incorrect state of sorted queriers")
	}

	tqs.querierIDsSorted = append(tqs.querierIDsSorted[:ix], tqs.querierIDsSorted[ix+1:]...)

	tqs.recomputeTenantQueriers()
}

// notifyQuerierShutdown records that a querier has sent notification about a graceful shutdown.
func (tqs *tenantQuerierState) notifyQuerierShutdown(querierID string) {
	querier := tqs.queriersByID[querierID]
	if querier == nil {
		// The querier may have already been removed, so we just ignore it.
		return
	}

	// If there are no more connections, we should remove the querier.
	if querier.connections == 0 {
		tqs.removeQuerier(querierID)
		return
	}

	// Otherwise we should annotate we received a graceful shutdown notification
	// and the querier will be removed once all connections are unregistered.
	querier.shuttingDown = true
}

// forgetDisconnectedQueriers removes all disconnected queriers that have gone since at least
// the forget delay. Returns the number of forgotten queriers.
func (tqs *tenantQuerierState) forgetDisconnectedQueriers(now time.Time) int {
	// Nothing to do if the forget delay is disabled.
	if tqs.querierForgetDelay == 0 {
		return 0
	}

	// Remove all queriers with no connections that have gone since at least the forget delay.
	threshold := now.Add(-tqs.querierForgetDelay)
	forgotten := 0

	for querierID := range tqs.queriersByID {
		if querier := tqs.queriersByID[querierID]; querier.connections == 0 && querier.disconnectedAt.Before(threshold) {
			tqs.removeQuerier(querierID)
			forgotten++
		}
	}

	return forgotten
}

func (tqs *tenantQuerierState) recomputeTenantQueriers() {
	// Only allocate the scratchpad the first time we need it.
	// If shuffle-sharding is disabled, it will not be used.
	var scratchpad []string

	for tenantID, tenant := range tqs.tenantsByID {
		if tenant.maxQueriers > 0 && tenant.maxQueriers < len(tqs.querierIDsSorted) && scratchpad == nil {
			scratchpad = make([]string, 0, len(tqs.querierIDsSorted))
		}

		tqs.shuffleTenantQueriers(tenantID, scratchpad)
	}
}

func (tqs *tenantQuerierState) shuffleTenantQueriers(tenantID string, scratchpad []string) {
	tenant := tqs.tenantsByID[tenantID]
	if tenant == nil {
		return
	}

	if tenant.maxQueriers == 0 || len(tqs.querierIDsSorted) <= tenant.maxQueriers {
		// shuffle shard is either disabled or calculation is unnecessary
		tqs.tenantQuerierIDs[tenantID] = nil
		return
	}

	querierIDSet := make(map[string]struct{}, tenant.maxQueriers)
	rnd := rand.New(rand.NewSource(tenant.shuffleShardSeed))

	scratchpad = append(scratchpad[:0], tqs.querierIDsSorted...)

	last := len(scratchpad) - 1
	for i := 0; i < tenant.maxQueriers; i++ {
		r := rnd.Intn(last + 1)
		querierIDSet[scratchpad[r]] = struct{}{}
		// move selected item to the end, it won't be selected anymore.
		scratchpad[r], scratchpad[last] = scratchpad[last], scratchpad[r]
		last--
	}
	tqs.tenantQuerierIDs[tenantID] = querierIDSet
}
