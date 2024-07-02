// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDequeueBalancedTree_TreeQueueImplA checks dequeuing behavior from a balanced tree.
//
// Dequeuing from a balanced tree allows the test to have a simple looped structures
// while running checks to ensure that round-robin order is respected.
func TestDequeueBalancedTree_TreeQueueImplA(t *testing.T) {
	roundRobinState1 := NewRoundRobinState()
	roundRobinState2 := NewRoundRobinState()

	tree := TreeQueueImplA{
		name:         "root",
		localQueue:   nil,
		childNodeMap: nil,
	}

	roundRobinState1EnqueueOps := roundRobinState1.EnqueueFuncs()
	roundRobinState2EnqueueOps := roundRobinState2.EnqueueFuncs()

	makeEnqueueOps := func(nodeNameLevel1, nodenameLevel2 string) []EnqueueLevelOps {
		return []EnqueueLevelOps{
			{
				nodeNameLevel1,
				roundRobinState1EnqueueOps,
			},
			{
				nodenameLevel2,
				roundRobinState2EnqueueOps,
			},
		}
	}
	enqueueOps := makeEnqueueOps("ingester", "tenantA")
	err := tree.EnqueueBackByPath("ingester:tenantA", enqueueOps)
	require.NoError(t, err)

	enqueueOps = makeEnqueueOps("ingester-and-store-gateway", "tenantA")
	err = tree.EnqueueBackByPath("ingester-and-store-gateway:tenantA", enqueueOps)
	require.NoError(t, err)

	enqueueOps = makeEnqueueOps("store-gateway", "tenantA")
	err = tree.EnqueueBackByPath("store-gateway:tenantA", enqueueOps)
	require.NoError(t, err)

	enqueueOps = makeEnqueueOps("ingester", "tenantB")
	err = tree.EnqueueBackByPath("ingester:tenantB", enqueueOps)
	require.NoError(t, err)

	enqueueOps = makeEnqueueOps("ingester-and-store-gateway", "tenantB")
	err = tree.EnqueueBackByPath("ingester-and-store-gateway:tenantB", enqueueOps)
	require.NoError(t, err)

	enqueueOps = makeEnqueueOps("store-gateway", "tenantB")
	err = tree.EnqueueBackByPath("store-gateway:tenantB", enqueueOps)
	require.NoError(t, err)

	require.Equal(t, 6, tree.ItemCount())

	rrs1DequeueNodeSelect, rrs1DequeueUpdateState := roundRobinState1.DequeueFuncs()
	rrs2DequeueNodeSelect, rrs2DequeueUpdateState := roundRobinState2.DequeueFuncs()
	dequeueOps := []*DequeueLevelOps{
		{Select: rrs1DequeueNodeSelect, UpdateState: rrs1DequeueUpdateState},
		{Select: rrs2DequeueNodeSelect, UpdateState: rrs2DequeueUpdateState},
	}

	expectedItemDequeueOrder := []string{
		"ingester:tenantA",
		"ingester-and-store-gateway:tenantB",
		"store-gateway:tenantA",
		"ingester:tenantB",
		"ingester-and-store-gateway:tenantA",
		"store-gateway:tenantB",
	}
	itemDequeueOrder := make([]string, len(expectedItemDequeueOrder))

	itemCount := tree.ItemCount()
	for i := 0; i < itemCount; i++ {

		v, err := tree.Dequeue(dequeueOps)
		require.NoError(t, err)

		itemDequeueOrder[i] = v.(string)
	}

	require.Equal(t, expectedItemDequeueOrder, itemDequeueOrder)
}
