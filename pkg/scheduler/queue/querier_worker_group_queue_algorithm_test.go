// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuerierWorkerGroupDequeue_DefaultRoundRobin(t *testing.T) {
	type opType string
	enqueue := opType("enqueue")
	dequeue := opType("dequeue")

	type op struct {
		kind opType
		path QueuePath
		obj  any
	}

	operationOrder := []op{
		// enqueue 2 objects each to 3 different children;
		// nodes are only added to a rotation on first enqueue,
		// so only order of first enqueue sets the dequeue order
		{enqueue, QueuePath{"child-1"}, "obj-1"}, // child-1 node created
		{enqueue, QueuePath{"child-2"}, "obj-2"}, // child-2 node created
		{enqueue, QueuePath{"child-3"}, "obj-3"}, // child-3 node created
		// order of nodes is set, further enqueues in a different order will not change it
		{enqueue, QueuePath{"child-3"}, "obj-4"},
		{enqueue, QueuePath{"child-2"}, "obj-5"},
		{enqueue, QueuePath{"child-1"}, "obj-6"},

		// dequeue proceeds in order of first enqueue until a node is emptied and removed from rotation
		{dequeue, QueuePath{"child-1"}, "obj-1"},
		{dequeue, QueuePath{"child-2"}, "obj-2"},
		{dequeue, QueuePath{"child-3"}, "obj-3"},
		{dequeue, QueuePath{"child-1"}, "obj-6"},
		// child-1 is now empty and removed from rotation
		{dequeue, QueuePath{"child-2"}, "obj-5"},
		// child-2 is now empty and removed from rotation

		// enqueue for child-1 again to verify it is added back to rotation
		{enqueue, QueuePath{"child-1"}, "obj-7"},

		// child-3 is still next; child-1 was added back to rotation
		{dequeue, QueuePath{"child-3"}, "obj-4"},
		// child-3 is now empty and removed from rotation; only child-1 remains
		{dequeue, QueuePath{"child-1"}, "obj-7"},
		// nothing left to dequeue
		{dequeue, QueuePath{}, nil},
	}

	querierWorkerPrioritizationQueueAlgo := &querierWorkerPrioritizationQueueAlgo{
		currentQuerierWorker: 0,
	}

	tree, err := NewTree(querierWorkerPrioritizationQueueAlgo, &roundRobinState{})
	require.NoError(t, err)

	for _, operation := range operationOrder {
		if operation.kind == enqueue {
			err = tree.EnqueueBackByPath(operation.path, operation.obj)
			require.NoError(t, err)
		}
		if operation.kind == dequeue {
			path, obj := tree.Dequeue()
			require.Equal(t, operation.path, path)
			require.Equal(t, operation.obj, obj)
		}
	}
}

func TestQuerierWorkerGroupDequeue_StartPositionByQuerierWorker(t *testing.T) {
	querierWorkerPrioritizationQueueAlgo := &querierWorkerPrioritizationQueueAlgo{}

	tree, err := NewTree(querierWorkerPrioritizationQueueAlgo, &roundRobinState{})
	require.NoError(t, err)

	// enqueue 3 objects each to 3 different children;
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{ingesterQueueDimension}, "obj-1"))                // ingester node created
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{storeGatewayQueueDimension}, "obj-2"))            // store-gateway node created
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{ingesterAndStoreGatewayQueueDimension}, "obj-3")) // ingester-and-store-gateway node created
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{ingesterQueueDimension}, "obj-4"))
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{storeGatewayQueueDimension}, "obj-5"))
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{ingesterAndStoreGatewayQueueDimension}, "obj-6"))
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{ingesterQueueDimension}, "obj-7"))
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{storeGatewayQueueDimension}, "obj-8"))
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{ingesterAndStoreGatewayQueueDimension}, "obj-9"))

	// node order was set by initial enqueue order;
	// order will remain until a node is deleted for being empty or a new node is added by an enqueue
	expectedInitialNodeOrder := []string{ingesterQueueDimension, storeGatewayQueueDimension, ingesterAndStoreGatewayQueueDimension}
	assert.Equal(t, expectedInitialNodeOrder, querierWorkerPrioritizationQueueAlgo.nodeOrder)

	// with 3 queues present, first node to be dequeued from is determined by worker ID % 3
	querierWorkerPrioritizationQueueAlgo.SetCurrentQuerierWorker(0)
	path, obj := tree.Dequeue()
	assert.Equal(t, QueuePath{ingesterQueueDimension}, path)
	assert.Equal(t, "obj-1", obj)

	querierWorkerPrioritizationQueueAlgo.SetCurrentQuerierWorker(1)
	path, obj = tree.Dequeue()
	assert.Equal(t, QueuePath{storeGatewayQueueDimension}, path)
	assert.Equal(t, "obj-2", obj)

	querierWorkerPrioritizationQueueAlgo.SetCurrentQuerierWorker(2)
	path, obj = tree.Dequeue()
	assert.Equal(t, QueuePath{ingesterAndStoreGatewayQueueDimension}, path)
	assert.Equal(t, "obj-3", obj)

	// worker IDs can come in to dequeue out of order, and they will still start at the correct queue
	querierWorkerPrioritizationQueueAlgo.SetCurrentQuerierWorker(5)
	path, obj = tree.Dequeue()
	assert.Equal(t, QueuePath{ingesterAndStoreGatewayQueueDimension}, path)
	assert.Equal(t, "obj-6", obj)

	querierWorkerPrioritizationQueueAlgo.SetCurrentQuerierWorker(3)
	path, obj = tree.Dequeue()
	assert.Equal(t, QueuePath{ingesterQueueDimension}, path)
	assert.Equal(t, "obj-4", obj)

	querierWorkerPrioritizationQueueAlgo.SetCurrentQuerierWorker(4)
	path, obj = tree.Dequeue()
	assert.Equal(t, QueuePath{storeGatewayQueueDimension}, path)
	assert.Equal(t, "obj-5", obj)

	// only 1 item left in each queue; as queue nodes are emptied and deleted,
	// worker IDs will be shuffled to different queues by the modulo operation
	querierWorkerPrioritizationQueueAlgo.SetCurrentQuerierWorker(0)
	path, obj = tree.Dequeue()
	assert.Equal(t, QueuePath{ingesterQueueDimension}, path)
	assert.Equal(t, "obj-7", obj)

	// ingester queue empty and deleted: 2 queues left are ["store-gateway", "ingester-and-store-gateway"]
	// with 2 queues present, first node to be dequeued from is determined by worker ID % 2
	querierWorkerPrioritizationQueueAlgo.SetCurrentQuerierWorker(1)
	path, obj = tree.Dequeue()
	assert.Equal(t, QueuePath{ingesterAndStoreGatewayQueueDimension}, path)
	assert.Equal(t, "obj-9", obj)

	// ingester-and-store-gateway queue empty and deleted: 1 queue left is just ["store-gateway"]
	// every worker will dequeue from the same queue since there is only 1 left
	querierWorkerPrioritizationQueueAlgo.SetCurrentQuerierWorker(999)
	path, obj = tree.Dequeue()
	assert.Equal(t, QueuePath{storeGatewayQueueDimension}, path)
	assert.Equal(t, "obj-8", obj)

	path, obj = tree.Dequeue()
	assert.Equal(t, QueuePath{}, path)
	assert.Nil(t, obj)
}
