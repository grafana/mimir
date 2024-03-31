// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDequeueBalancedTree_TreeQueueImplA checks dequeuing behavior from a balanced tree.
//
// Dequeuing from a balanced tree allows the test to have a simple looped structures
// while running checks to ensure that round-robin order is respected.
func TestDequeueBalancedTree_TreeQueueImplA(t *testing.T) {
	roundRobinState1 := &RoundRobinState{
		currentChildQueueIndex: -1,
		childQueueOrder:        []string{"ingester", "ingester-and-store-gateway", "store-gateway"},
		currentDequeueAttempts: 0,
	}
	roundRobinState2 := &RoundRobinState{
		currentChildQueueIndex: -1,
		childQueueOrder:        []string{"tenantA", "tenantB"},
		currentDequeueAttempts: 0,
	}

	treeLevelQueueAlgoStates := []*RoundRobinState{roundRobinState1, roundRobinState2}

	tree := TreeQueueImplA{
		name:         "root",
		localQueue:   nil,
		childNodeMap: nil,
	}

	treeLevelEnqueueOps := []EnqueueLevelOps{
		{
			"ingester",
			roundRobinState1.MakeEnqueueStateUpdateFunc(),
		},
		{
			"tenantA",
			roundRobinState2.MakeEnqueueStateUpdateFunc(),
		},
	}
	err := tree.EnqueueBackByPath("ingester:tenantA", treeLevelEnqueueOps)
	require.NoError(t, err)

	treeLevelEnqueueOps = []EnqueueLevelOps{
		{
			"ingester-and-store-gateway",
			roundRobinState1.MakeEnqueueStateUpdateFunc(),
		},
		{
			"tenantA",
			roundRobinState2.MakeEnqueueStateUpdateFunc(),
		},
	}
	err = tree.EnqueueBackByPath("ingester-and-store-gateway:tenantA", treeLevelEnqueueOps)
	require.NoError(t, err)

	treeLevelEnqueueOps = []EnqueueLevelOps{
		{
			"store-gateway",
			roundRobinState1.MakeEnqueueStateUpdateFunc(),
		},
		{
			"tenantA",
			roundRobinState2.MakeEnqueueStateUpdateFunc(),
		},
	}
	err = tree.EnqueueBackByPath("store-gateway:tenantA", treeLevelEnqueueOps)
	require.NoError(t, err)

	treeLevelEnqueueOps = []EnqueueLevelOps{
		{
			"ingester",
			roundRobinState1.MakeEnqueueStateUpdateFunc(),
		},
		{
			"tenantB",
			roundRobinState2.MakeEnqueueStateUpdateFunc(),
		},
	}
	err = tree.EnqueueBackByPath("ingester:tenantB", treeLevelEnqueueOps)
	require.NoError(t, err)

	treeLevelEnqueueOps = []EnqueueLevelOps{
		{
			"ingester-and-store-gateway",
			roundRobinState1.MakeEnqueueStateUpdateFunc(),
		},
		{
			"tenantB",
			roundRobinState2.MakeEnqueueStateUpdateFunc(),
		},
	}
	err = tree.EnqueueBackByPath("ingester-and-store-gateway:tenantB", treeLevelEnqueueOps)
	require.NoError(t, err)

	treeLevelEnqueueOps = []EnqueueLevelOps{
		{
			"store-gateway",
			roundRobinState1.MakeEnqueueStateUpdateFunc(),
		},
		{
			"tenantB",
			roundRobinState2.MakeEnqueueStateUpdateFunc(),
		},
	}
	err = tree.EnqueueBackByPath("store-gateway:tenantB", treeLevelEnqueueOps)
	require.NoError(t, err)

	require.Equal(t, 6, tree.ItemCount())

	dequeueOps := make([]*DequeueLevelOps, len(treeLevelQueueAlgoStates))

	//var v any
	for i, algoState := range treeLevelQueueAlgoStates {
		dequeueOps[i] = algoState.MakeDequeueNodeSelectFunc()
	}

	itemCount := tree.ItemCount()
	for i := 0; i < itemCount; i++ {
		for level, algoState := range treeLevelQueueAlgoStates {
			dequeueOps[level] = algoState.MakeDequeueNodeSelectFunc()
		}

		v, err := tree.Dequeue(dequeueOps)
		require.NoError(t, err)
		fmt.Println(fmt.Sprintf("%v", v))
	}

}
