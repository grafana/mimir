// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/user_queues.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queue

import (
	"math/rand"
	"sort"

	"github.com/grafana/mimir/pkg/scheduler/queue/tree"
	"github.com/grafana/mimir/pkg/util"
)

type queueTenant struct {
	tenantID    string
	maxQueriers int

	// seed for shuffle sharding of queriers; computed from tenantID only,
	// and is therefore consistent between different frontends.
	shuffleShardSeed int64

	// points up to tenant order to enable efficient removal
	orderIndex int
}

type querierIDSlice []tree.QuerierID

// Len implements sort.Interface for querierIDSlice
func (s querierIDSlice) Len() int { return len(s) }

// Swap implements sort.Interface for querierIDSlice
func (s querierIDSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// Less implements sort.Interface for querierIDSlice
func (s querierIDSlice) Less(i, j int) bool { return s[i] < s[j] }

// Search method covers for sort.Search's functionality,
// as sort.Search does not allow anything interface-based or generic yet.
func (s querierIDSlice) Search(x tree.QuerierID) int {
	return sort.Search(len(s), func(i int) bool { return s[i] >= x })
}

// tenantQuerierSharding maintains information about tenants and connected queriers, and uses
// this information to update a mapping of queriers to tenants in tree.TenantQuerierQueuingAlgorithm.
// This supports dequeuing from an appropriate tenant for a given querier when shuffle-sharding is enabled.
//
// A tenant has many queriers which can process its requests. Queriers are assigned to a tenant via a shuffle-shard seed,
// which is consistently hashed from the tenant ID. A tenant has *all* queriers if:
//   - sharding is disabled (query-frontend.max-queriers-per-tenant=0)
//   - OR if max-queriers-per-tenant >= the number of queriers.
//
// The tenant-querier mapping is reshuffled when:
//   - a querier connection is added or removed
//   - it is detected during request enqueueing that a tenant's queriers were calculated from
//     an outdated max-queriers-per-tenant value
type tenantQuerierSharding struct {
	// Sorted list of querier ids, used when shuffle-sharding queriers for tenant
	querierIDsSorted querierIDSlice

	// Tenant information used in shuffle-sharding
	tenantsByID map[string]*queueTenant

	// State used by the tree queue to dequeue queries, including a tenant-querier mapping which
	// will be updated by tenantQuerierSharding when the mapping is changed.
	queuingAlgorithm *tree.TenantQuerierQueuingAlgorithm
}

func newTenantQuerierAssignments() *tenantQuerierSharding {
	tqqa := tree.NewTenantQuerierQueuingAlgorithm()
	return &tenantQuerierSharding{
		querierIDsSorted: nil,
		tenantsByID:      map[string]*queueTenant{},
		queuingAlgorithm: tqqa,
	}
}

// createOrUpdateTenant creates or updates a tenant into the tenant-querier assignment state.
//
// New tenants are added to tenantsByID and the queuing algorithm state, and tenant-querier shards are shuffled if needed.
// Existing tenants have the tenant-querier shards shuffled only if their maxQueriers has changed.
func (tqa *tenantQuerierSharding) createOrUpdateTenant(tenantID string, maxQueriers int) error {
	if tenantID == "" {
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
			shuffleShardSeed: util.ShuffleShardSeed(tenantID, ""),
			// orderIndex set to sentinel value to indicate it is not inserted yet
			orderIndex: -1,
		}
		tenantPosition := tqa.queuingAlgorithm.AddTenant(tenantID)
		tenant.orderIndex = tenantPosition
		tqa.tenantsByID[tenantID] = tenant
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

// removeTenant deletes a *queueTenant from tenantsByID. Any updates to remove a tenant from queuingAlgorithm
// are managed by a tree.Tree during dequeue.
func (tqa *tenantQuerierSharding) removeTenant(tenantID string) {
	tenant := tqa.tenantsByID[tenantID]
	if tenant == nil {
		return
	}
	delete(tqa.tenantsByID, tenantID)
}

// addQuerier adds the given querierID to tenantQuerierSharding' querierIDsSorted. It does not do any checks to
// validate that a querier connection matching this ID exists. That logic is handled by querierConnections,
// and coordinated by the queueBroker.
func (tqa *tenantQuerierSharding) addQuerier(querierID string) {
	tqa.querierIDsSorted = append(tqa.querierIDsSorted, tree.QuerierID(querierID))
	sort.Sort(tqa.querierIDsSorted)
}

// removeQueriers deletes an arbitrary number of queriers from the querier connection manager, and returns true if
// the tenant-querier sharding was recomputed.
func (tqa *tenantQuerierSharding) removeQueriers(querierIDs ...string) (resharded bool) {
	for _, querierID := range querierIDs {
		ix := tqa.querierIDsSorted.Search(tree.QuerierID(querierID))
		if ix >= len(tqa.querierIDsSorted) || tqa.querierIDsSorted[ix] != tree.QuerierID(querierID) {
			panic("incorrect state of sorted queriers")
		}

		tqa.querierIDsSorted = append(tqa.querierIDsSorted[:ix], tqa.querierIDsSorted[ix+1:]...)
	}
	return tqa.recomputeTenantQueriers()
}

func (tqa *tenantQuerierSharding) recomputeTenantQueriers() (resharded bool) {
	var scratchpad []tree.QuerierID
	for tenantID, tenant := range tqa.tenantsByID {
		if tenant.maxQueriers > 0 && tenant.maxQueriers < len(tqa.querierIDsSorted) && scratchpad == nil {
			// shuffle sharding is enabled and the number of queriers exceeds tenant maxQueriers,
			// meaning tenant querier assignments need computed via shuffle sharding;
			// allocate the scratchpad the first time this case is hit and it will be reused after
			scratchpad = make([]tree.QuerierID, 0, len(tqa.querierIDsSorted))
		}
		// operation must be on left to avoid short-circuiting and skipping the operation
		resharded = tqa.shuffleTenantQueriers(tenantID, scratchpad) || resharded
	}
	return resharded
}

func (tqa *tenantQuerierSharding) shuffleTenantQueriers(tenantID string, scratchpad []tree.QuerierID) (resharded bool) {
	tenant := tqa.tenantsByID[tenantID]
	if tenant == nil {
		return false
	}

	if tenant.maxQueriers == 0 || len(tqa.querierIDsSorted) <= tenant.maxQueriers {
		// shuffle shard is either disabled or calculation is unnecessary;
		prevQuerierIDSet := tqa.queriersForTenant(tenantID)
		// assigning querier set to nil for the tenant indicates tenant can use all queriers
		tqa.queuingAlgorithm.SetQueriersForTenant(tenantID, nil)
		// tenant may have already been assigned all queriers; only indicate reshard if this changed
		return prevQuerierIDSet != nil
	}

	querierIDSet := make(map[tree.QuerierID]struct{}, tenant.maxQueriers)
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
	tqa.queuingAlgorithm.SetQueriersForTenant(tenantID, querierIDSet)
	return true
}

func (tqa *tenantQuerierSharding) queriersForTenant(tenantID string) map[tree.QuerierID]struct{} {
	return tqa.queuingAlgorithm.QueriersForTenant(tenantID)
}
