// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/user_queues_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queue

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/queue/tree"
)

func TestShuffleConsumers(t *testing.T) {
	allConsumers := []tree.ConsumerID{"a", "b", "c", "d", "e"}
	tqs := &tenantConsumerShards{
		consumerIDsSorted: allConsumers,
		tenantsByID: map[string]*queueTenant{
			"team-a": {
				shuffleShardSeed: 12345,
			},
		},
		queuingAlgorithm: tree.NewTenantConsumerQueuingAlgorithm(),
	}

	// maxConsumers is 0, so sharding is off
	tqs.shuffleTenantConsumers("team-a", nil)
	require.Nil(t, tqs.consumersForTenant("team-a"))

	// maxConsumers is equal to the total consumers, so the sharding calculation is unnecessary
	tqs.tenantsByID["team-a"].maxConsumers = len(allConsumers)
	tqs.shuffleTenantConsumers("team-a", nil)
	require.Nil(t, tqs.consumersForTenant("team-a"))

	// maxConsumers is greater than the total consumers, so the sharding calculation is unnecessary
	tqs.tenantsByID["team-a"].maxConsumers = len(allConsumers) + 1
	tqs.shuffleTenantConsumers("team-a", nil)
	require.Nil(t, tqs.consumersForTenant("team-a"))

	// now maxConsumers is nonzero and less than the total consumers, so we shuffle shard and assign
	tqs.tenantsByID["team-a"].maxConsumers = 3
	tqs.shuffleTenantConsumers("team-a", nil)
	r1 := tqs.consumersForTenant("team-a")
	require.Equal(t, 3, len(r1))

	// Same input produces same output.
	tqs.shuffleTenantConsumers("team-a", nil)
	r2 := tqs.consumersForTenant("team-a")

	require.Equal(t, 3, len(r2))
	require.Equal(t, r1, r2)
}

func TestShuffleConsumersCorrectness(t *testing.T) {
	const consumersCount = 100

	var allSortedConsumers consumerIDSlice
	for i := 0; i < consumersCount; i++ {
		allSortedConsumers = append(allSortedConsumers, tree.ConsumerID(fmt.Sprintf("%d", i)))
	}
	slices.Sort(allSortedConsumers)

	tqs := tenantConsumerShards{
		consumerIDsSorted: allSortedConsumers,
		tenantsByID: map[string]*queueTenant{
			"team-a": {
				shuffleShardSeed: 12345,
			},
		},
		queuingAlgorithm: tree.NewTenantConsumerQueuingAlgorithm(),
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	const tests = 1000
	for i := 0; i < tests; i++ {
		toSelect := r.Intn(consumersCount)
		if toSelect == 0 {
			toSelect = 3
		}

		tqs.tenantsByID["team-a"].maxConsumers = toSelect
		tqs.tenantsByID["team-a"].shuffleShardSeed = r.Int63()

		tqs.shuffleTenantConsumers("team-a", nil)
		selectedConsumers := tqs.consumersForTenant("team-a")
		require.Equal(t, toSelect, len(selectedConsumers))

		slices.Sort(allSortedConsumers)
		prevConsumer := tree.ConsumerID("")
		for _, q := range allSortedConsumers {
			require.True(t, prevConsumer < q, "non-unique consumer")
			prevConsumer = q

			ix := allSortedConsumers.Search(q)
			require.True(t, ix < len(allSortedConsumers) && allSortedConsumers[ix] == q, "selected consumer is not between all consumers")
		}
	}
}
