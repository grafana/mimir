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
	queriersByID       map[string]*querierConn
	querierIDsSorted   []string
	querierForgetDelay time.Duration

	// List of all tenants with queues, used for iteration when searching for next queue to handle.
	// Tenants removed from the middle are replaced with "". To avoid skipping users during iteration,
	// we only shrink this list when there are ""'s at the end of it.
	tenantIDOrder []string
	tenantsByID   map[string]*queueUser

	tenantQuerierIDs map[string]map[string]struct{}
}

type queueUser struct {
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
	userQueues map[string]*userQueue

	tenantQuerierState tenantQuerierState

	maxUserQueueSize int

	// How long to wait before removing a querier which has got disconnected
	// but hasn't notified about a graceful shutdown.
	forgetDelay time.Duration

	// Tracks queriers registered to the queue.
	queriers map[string]*querierConn

	// Sorted list of querier names, used when creating per-user shard.
	sortedQueriers []string

	// If not nil, only these queriers can handle user requests. If nil, all queriers can.
	// We set this to nil if number of available queriers <= maxQueriers.
	userQueriers map[string]map[string]struct{}
}

type userQueue struct {
	requests *list.List
}

func newUserQueues(maxUserQueueSize int, forgetDelay time.Duration) *queues {
	return &queues{
		userQueues: map[string]*userQueue{},
		tenantQuerierState: tenantQuerierState{
			tenantIDOrder: nil,
			tenantsByID:   map[string]*queueUser{},
		},
		maxUserQueueSize: maxUserQueueSize,
		forgetDelay:      forgetDelay,
		queriers:         map[string]*querierConn{},
		sortedQueriers:   nil,
		userQueriers:     map[string]map[string]struct{}{},
	}
}

func (q *queues) len() int {
	return len(q.userQueues)
}

func (q *queues) deleteQueue(userID string) {
	uq := q.userQueues[userID]
	if uq == nil {
		return
	}
	u := q.tenantQuerierState.tenantsByID[userID]

	delete(q.userQueues, userID)
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
func (q *queues) getOrAddQueue(userID string, maxQueriers int) *list.List {
	// Empty user is not allowed, as that would break our users list ("" is used for free spot).
	if userID == "" {
		return nil
	}

	if maxQueriers < 0 {
		maxQueriers = 0
	}

	uq := q.userQueues[userID]
	u := q.tenantQuerierState.tenantsByID[userID]

	if uq == nil {
		u = &queueUser{
			shuffleShardSeed: util.ShuffleShardSeed(userID, ""),
			orderIndex:       -1,
		}
		q.tenantQuerierState.tenantsByID[userID] = u
		q.userQueriers[userID] = nil

		uq = &userQueue{
			requests: list.New(),
		}
		q.userQueues[userID] = uq

		// Add user to the list of users... find first free spot, and put it there.
		for ix, user := range q.tenantQuerierState.tenantIDOrder {
			if user == "" {
				u.orderIndex = ix
				q.tenantQuerierState.tenantIDOrder[ix] = userID
				break
			}
		}

		// ... or add to the end.
		if u.orderIndex < 0 {
			u.orderIndex = len(q.tenantQuerierState.tenantIDOrder)
			q.tenantQuerierState.tenantIDOrder = append(q.tenantQuerierState.tenantIDOrder, userID)
		}
	}

	if u.maxQueriers != maxQueriers {
		u.maxQueriers = maxQueriers
		q.userQueriers[userID] = shuffleQueriersForUser(q.tenantQuerierState.tenantsByID[userID].shuffleShardSeed, maxQueriers, q.sortedQueriers, nil)
	}

	return uq.requests
}

// Finds next queue for the querier. To support fair scheduling between users, client is expected
// to pass last user index returned by this function as argument. If there was no previous
// last user index, use -1.
func (q *queues) getNextQueueForQuerier(lastUserIndex int, querierID string) (*list.List, string, int, error) {
	userIndex := lastUserIndex

	// Ensure the querier is not shutting down. If the querier is shutting down, we shouldn't forward
	// any more queries to it.
	if info := q.queriers[querierID]; info == nil || info.shuttingDown {
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

		userQueue := q.userQueues[userID]

		if querierSet := q.userQueriers[userID]; querierSet != nil {
			if _, ok := querierSet[querierID]; !ok {
				// This querier is not handling the user.
				continue
			}
		}

		return userQueue.requests, userID, userIndex, nil
	}
	return nil, "", userIndex, nil
}

func (q *queues) addQuerierConnection(querierID string) {
	info := q.queriers[querierID]
	if info != nil {
		info.connections++

		// Reset in case the querier re-connected while it was in the forget waiting period.
		info.shuttingDown = false
		info.disconnectedAt = time.Time{}

		return
	}

	// First connection from this querier.
	q.queriers[querierID] = &querierConn{connections: 1}
	q.sortedQueriers = append(q.sortedQueriers, querierID)
	slices.Sort(q.sortedQueriers)

	q.recomputeUserQueriers()
}

func (q *queues) removeQuerierConnection(querierID string, now time.Time) {
	info := q.queriers[querierID]
	if info == nil || info.connections <= 0 {
		panic("unexpected number of connections for querier")
	}

	// Decrease the number of active connections.
	info.connections--
	if info.connections > 0 {
		return
	}

	// There no more active connections. If the forget delay is configured then
	// we can remove it only if querier has announced a graceful shutdown.
	if info.shuttingDown || q.forgetDelay == 0 {
		q.removeQuerier(querierID)
		return
	}

	// No graceful shutdown has been notified yet, so we should track the current time
	// so that we'll remove the querier as soon as we receive the graceful shutdown
	// notification (if any) or once the threshold expires.
	info.disconnectedAt = now
}

func (q *queues) removeQuerier(querierID string) {
	delete(q.queriers, querierID)

	ix := sort.SearchStrings(q.sortedQueriers, querierID)
	if ix >= len(q.sortedQueriers) || q.sortedQueriers[ix] != querierID {
		panic("incorrect state of sorted queriers")
	}

	q.sortedQueriers = append(q.sortedQueriers[:ix], q.sortedQueriers[ix+1:]...)

	q.recomputeUserQueriers()
}

// notifyQuerierShutdown records that a querier has sent notification about a graceful shutdown.
func (q *queues) notifyQuerierShutdown(querierID string) {
	info := q.queriers[querierID]
	if info == nil {
		// The querier may have already been removed, so we just ignore it.
		return
	}

	// If there are no more connections, we should remove the querier.
	if info.connections == 0 {
		q.removeQuerier(querierID)
		return
	}

	// Otherwise we should annotate we received a graceful shutdown notification
	// and the querier will be removed once all connections are unregistered.
	info.shuttingDown = true
}

// forgetDisconnectedQueriers removes all disconnected queriers that have gone since at least
// the forget delay. Returns the number of forgotten queriers.
func (q *queues) forgetDisconnectedQueriers(now time.Time) int {
	// Nothing to do if the forget delay is disabled.
	if q.forgetDelay == 0 {
		return 0
	}

	// Remove all queriers with no connections that have gone since at least the forget delay.
	threshold := now.Add(-q.forgetDelay)
	forgotten := 0

	for querierID := range q.queriers {
		if info := q.queriers[querierID]; info.connections == 0 && info.disconnectedAt.Before(threshold) {
			q.removeQuerier(querierID)
			forgotten++
		}
	}

	return forgotten
}

func (q *queues) recomputeUserQueriers() {
	// Only allocate the scratchpad the first time we need it.
	// If shuffle-sharding is disabled, we never need this.
	var scratchpad []string

	for uid, u := range q.tenantQuerierState.tenantsByID {
		if u.maxQueriers > 0 && u.maxQueriers < len(q.sortedQueriers) && scratchpad == nil {
			scratchpad = make([]string, 0, len(q.sortedQueriers))
		}

		q.userQueriers[uid] = shuffleQueriersForUser(u.shuffleShardSeed, u.maxQueriers, q.sortedQueriers, scratchpad)
	}
}

// shuffleQueriersForUser returns nil if queriersToSelect is 0 or there are not enough queriers to select from.
// In that case *all* queriers should be used.
// Scratchpad is used for shuffling, to avoid new allocations. If nil, new slice is allocated.
func shuffleQueriersForUser(userSeed int64, queriersToSelect int, allSortedQueriers []string, scratchpad []string) map[string]struct{} {
	if queriersToSelect == 0 || len(allSortedQueriers) <= queriersToSelect {
		return nil
	}

	result := make(map[string]struct{}, queriersToSelect)
	rnd := rand.New(rand.NewSource(userSeed))

	scratchpad = scratchpad[:0]
	scratchpad = append(scratchpad, allSortedQueriers...)

	last := len(scratchpad) - 1
	for i := 0; i < queriersToSelect; i++ {
		r := rnd.Intn(last + 1)
		result[scratchpad[r]] = struct{}{}
		// move selected item to the end, it won't be selected anymore.
		scratchpad[r], scratchpad[last] = scratchpad[last], scratchpad[r]
		last--
	}

	return result
}
