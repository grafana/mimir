// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/user_queues.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queue

import (
	"math/rand"
	"sort"

	"github.com/grafana/mimir/pkg/queue/tree"
	"github.com/grafana/mimir/pkg/util"
)

type queueTenant struct {
	tenantID     string
	maxConsumers int

	// seed for shuffle sharding of consumers; computed from tenantID only,
	// and is therefore consistent between different frontends.
	shuffleShardSeed int64

	// points up to tenant order to enable efficient removal
	orderIndex int
}

type consumerIDSlice []tree.ConsumerID

// Len implements sort.Interface for consumerIDSlice
func (s consumerIDSlice) Len() int { return len(s) }

// Swap implements sort.Interface for consumerIDSlice
func (s consumerIDSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// Less implements sort.Interface for consumerIDSlice
func (s consumerIDSlice) Less(i, j int) bool { return s[i] < s[j] }

// Search method covers for sort.Search's functionality,
// as sort.Search does not allow anything interface-based or generic yet.
func (s consumerIDSlice) Search(x tree.ConsumerID) int {
	return sort.Search(len(s), func(i int) bool { return s[i] >= x })
}

// tenantConsumerShards maintains information about tenants and connected consumers, and uses
// this information to update a mapping of consumers to tenants in tree.TenantConsumerQueuingAlgorithm.
// This supports dequeuing from an appropriate tenant for a given consumer when shuffle-sharding is enabled.
//
// A tenant has many consumers which can process its requests. Consumers are assigned to a tenant via a shuffle-shard seed,
// which is consistently hashed from the tenant ID. A tenant has *all* consumers if:
//   - sharding is disabled (query-frontend.max-consumers-per-tenant=0)
//   - OR if max-consumers-per-tenant >= the number of consumers.
//
// The tenant-consumer mapping is reshuffled when:
//   - a consumer connection is added or removed
//   - it is detected during request enqueueing that a tenant's consumers were calculated from
//     an outdated max-consumers-per-tenant value
type tenantConsumerShards struct {
	// Sorted list of consumer ids, used when shuffle-sharding consumers for tenant
	consumerIDsSorted consumerIDSlice

	// Tenant information used in shuffle-sharding
	tenantsByID map[string]*queueTenant

	// State used by the tree queue to dequeue queries, including a tenant-consumer mapping which
	// will be updated by tenantConsumerShards when the mapping is changed.
	queuingAlgorithm *tree.TenantConsumerQueuingAlgorithm
}

func newTenantConsumerAssignments() *tenantConsumerShards {
	tqqa := tree.NewTenantConsumerQueuingAlgorithm()
	return &tenantConsumerShards{
		consumerIDsSorted: nil,
		tenantsByID:       map[string]*queueTenant{},
		queuingAlgorithm:  tqqa,
	}
}

// createOrUpdateTenant creates or updates a tenant into the tenant-consumer assignment state.
//
// New tenants are added to tenantsByID and the queuing algorithm state, and tenant-consumer shards are shuffled if needed.
// Existing tenants have the tenant-consumer shards shuffled only if their maxConsumers has changed.
func (tqa *tenantConsumerShards) createOrUpdateTenant(tenantID string, maxConsumers int) error {
	if tenantID == "" {
		// empty tenantID is not allowed; "" is used for free spot
		return ErrInvalidTenantID
	}

	if maxConsumers < 0 {
		maxConsumers = 0
	}

	tenant := tqa.tenantsByID[tenantID]

	if tenant == nil {
		tenant = &queueTenant{
			tenantID: tenantID,
			// maxConsumers 0 enables a later check to trigger tenant-consumer assignment
			// for new queue tenants with shuffle sharding enabled
			maxConsumers:     0,
			shuffleShardSeed: util.ShuffleShardSeed(tenantID, ""),
			// orderIndex set to sentinel value to indicate it is not inserted yet
			orderIndex: -1,
		}
		tenantPosition := tqa.queuingAlgorithm.AddTenant(tenantID)
		tenant.orderIndex = tenantPosition
		tqa.tenantsByID[tenantID] = tenant
	}

	// tenant now either retrieved or created
	if tenant.maxConsumers != maxConsumers {
		// tenant consumers need to be computed/recomputed;
		// either this is a new tenant with sharding enabled,
		// or the tenant already existed but its maxConsumers has changed
		tenant.maxConsumers = maxConsumers
		tqa.shuffleTenantConsumers(tenantID, nil)
	}
	return nil
}

// removeTenant deletes a *queueTenant from tenantsByID. Any updates to remove a tenant from queuingAlgorithm
// are managed by a tree.Tree during dequeue.
func (tqa *tenantConsumerShards) removeTenant(tenantID string) {
	tenant := tqa.tenantsByID[tenantID]
	if tenant == nil {
		return
	}
	delete(tqa.tenantsByID, tenantID)
}

// addConsumer adds the given consumerID to tenantConsumerShards' consumerIDsSorted. It does not do any checks to
// validate that a consumer connection matching this ID exists. That logic is handled by consumerConnections,
// and coordinated by the queueBroker.
func (tqa *tenantConsumerShards) addConsumer(consumerID string) {
	tqa.consumerIDsSorted = append(tqa.consumerIDsSorted, tree.ConsumerID(consumerID))
	sort.Sort(tqa.consumerIDsSorted)
}

// removeConsumers deletes an arbitrary number of consumers from the consumer connection manager, and returns true if
// the tenant-consumer sharding was recomputed.
func (tqa *tenantConsumerShards) removeConsumers(consumerIDs ...string) (resharded bool) {
	for _, consumerID := range consumerIDs {
		ix := tqa.consumerIDsSorted.Search(tree.ConsumerID(consumerID))
		if ix >= len(tqa.consumerIDsSorted) || tqa.consumerIDsSorted[ix] != tree.ConsumerID(consumerID) {
			panic("incorrect state of sorted consumers")
		}

		tqa.consumerIDsSorted = append(tqa.consumerIDsSorted[:ix], tqa.consumerIDsSorted[ix+1:]...)
	}
	return tqa.recomputeTenantConsumers()
}

func (tqa *tenantConsumerShards) recomputeTenantConsumers() (resharded bool) {
	var scratchpad []tree.ConsumerID
	for tenantID, tenant := range tqa.tenantsByID {
		if tenant.maxConsumers > 0 && tenant.maxConsumers < len(tqa.consumerIDsSorted) && scratchpad == nil {
			// shuffle sharding is enabled and the number of consumers exceeds tenant maxConsumers,
			// meaning tenant consumer assignments need computed via shuffle sharding;
			// allocate the scratchpad the first time this case is hit and it will be reused after
			scratchpad = make([]tree.ConsumerID, 0, len(tqa.consumerIDsSorted))
		}
		// operation must be on left to avoid short-circuiting and skipping the operation
		resharded = tqa.shuffleTenantConsumers(tenantID, scratchpad) || resharded
	}
	return resharded
}

func (tqa *tenantConsumerShards) shuffleTenantConsumers(tenantID string, scratchpad []tree.ConsumerID) (resharded bool) {
	tenant := tqa.tenantsByID[tenantID]
	if tenant == nil {
		return false
	}

	if tenant.maxConsumers == 0 || len(tqa.consumerIDsSorted) <= tenant.maxConsumers {
		// shuffle shard is either disabled or calculation is unnecessary;
		prevConsumerIDSet := tqa.consumersForTenant(tenantID)
		// assigning consumer set to nil for the tenant indicates tenant can use all consumers
		tqa.queuingAlgorithm.SetConsumersForTenant(tenantID, nil)
		// tenant may have already been assigned all consumers; only indicate reshard if this changed
		return prevConsumerIDSet != nil
	}

	consumerIDSet := make(map[tree.ConsumerID]struct{}, tenant.maxConsumers)
	// Deterministic, seeded PRNG: every replica must derive the same shuffle-shard
	// assignment for a given tenant.
	// This intentionally uses math/rand (seedable), not crypto/rand (not seedable).
	rnd := rand.New(rand.NewSource(tenant.shuffleShardSeed)) // nosemgrep

	scratchpad = append(scratchpad[:0], tqa.consumerIDsSorted...)

	last := len(scratchpad) - 1
	for i := 0; i < tenant.maxConsumers; i++ {
		r := rnd.Intn(last + 1)
		consumerIDSet[scratchpad[r]] = struct{}{}
		// move selected item to the end, it won't be selected anymore.
		scratchpad[r], scratchpad[last] = scratchpad[last], scratchpad[r]
		last--
	}
	tqa.queuingAlgorithm.SetConsumersForTenant(tenantID, consumerIDSet)
	return true
}

func (tqa *tenantConsumerShards) consumersForTenant(tenantID string) map[tree.ConsumerID]struct{} {
	return tqa.queuingAlgorithm.ConsumersForTenant(tenantID)
}
