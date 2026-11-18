// SPDX-License-Identifier: AGPL-3.0-only

package tree

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A node with no tenant for the consumer must not move the tenant index.
// Otherwise the scan of the next node starts one tenant later, and the tenant after the last one served loses its turn.
//
// Querier shuffle sharding gives tenant C only to consumer-2. C is in node-1, and tenants A and B are in node-2.
// The round-robin root tries node-2 for the first dequeue.
// From the second dequeue on, it tries node-1 first, finds no tenant for consumer-1 there, and goes on to node-2.
// The tenant algorithm does not read the queue length, so the share does not depend on the number of queued requests.
func TestTenantConsumerQueuingAlgorithm_NodeWithoutTenantForConsumerKeepsTenantIndex(t *testing.T) {
	for _, tc := range []struct {
		name  string
		order []string
	}{
		{name: "A first in the tenant order", order: []string{"A", "B"}},
		{name: "B first in the tenant order", order: []string{"B", "A"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tenants := NewTenantConsumerQueuingAlgorithm()
			tree, err := NewTree(&RoundRobinState{}, tenants)
			require.NoError(t, err)

			for range 20 {
				for _, tenantID := range tc.order {
					require.NoError(t, tree.EnqueueBackByPath(QueuePath{"node-2", tenantID}, tenantID))
				}
			}
			require.NoError(t, tree.EnqueueBackByPath(QueuePath{"node-1", "C"}, "C"))
			tenants.SetConsumersForTenant("A", map[ConsumerID]struct{}{"consumer-1": {}})
			tenants.SetConsumersForTenant("B", map[ConsumerID]struct{}{"consumer-1": {}})
			tenants.SetConsumersForTenant("C", map[ConsumerID]struct{}{"consumer-2": {}})
			require.Equal(t, append(tc.order, "C"), tenants.TenantIDOrder())

			served := map[any]int{}
			lastTenantIndex := newConsumerTenantIndex
			for range 20 {
				_, obj := tree.Dequeue(&DequeueArgs{ConsumerID: "consumer-1", LastTenantIndex: lastTenantIndex})
				require.NotNil(t, obj)
				served[obj]++
				lastTenantIndex = tenants.TenantOrderIndex()
			}
			require.Equal(t, map[any]int{"A": 10, "B": 10}, served)
		})
	}
}
