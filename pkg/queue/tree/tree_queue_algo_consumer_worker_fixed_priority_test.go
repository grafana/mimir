// SPDX-License-Identifier: AGPL-3.0-only

package tree

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func newConsumerWorkerPriorityAlgoForTest() QueuingAlgorithm {
	return NewConsumerWorkerQueuePriorityAlgo()
}

// An ingester request completes fast.
// While the store-gateways are slow, a store-gateway request keeps its consumer-worker for many seconds.
// A consumer-worker that prioritizes the ingester node must keep dequeuing from it while ingester requests wait.
// Its priority node must not change when the tree deletes the empty ingester node and creates it again.
func TestConsumerWorkerFixedPriority_IngesterWorkerStaysOnIngesterNode(t *testing.T) {
	const (
		ingester     = "ingester"
		storeGateway = "store-gateway"
	)
	tree, err := NewTree(newConsumerWorkerPriorityAlgoForTest())
	require.NoError(t, err)

	require.NoError(t, tree.EnqueueBackByPath(QueuePath{ingester}, "ing-1"))
	for _, obj := range []string{"sg-1", "sg-2", "sg-3"} {
		require.NoError(t, tree.EnqueueBackByPath(QueuePath{storeGateway}, obj))
	}

	// Worker 0 prioritizes the ingester node.
	path, obj := tree.Dequeue(&DequeueArgs{WorkerID: 0})
	require.Equal(t, QueuePath{ingester}, path)
	require.Equal(t, "ing-1", obj)

	// The tree deleted the empty ingester node.
	// The next ingester request creates it again.
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{ingester}, "ing-2"))

	path, obj = tree.Dequeue(&DequeueArgs{WorkerID: 0})
	require.Equal(t, QueuePath{ingester}, path, "worker 0 dequeued %v while an ingester request waits", obj)
	require.Equal(t, "ing-2", obj)

	// Worker 1 prioritizes the store-gateway node.
	path, obj = tree.Dequeue(&DequeueArgs{WorkerID: 1})
	require.Equal(t, QueuePath{storeGateway}, path)
	require.Equal(t, "sg-1", obj)
}
