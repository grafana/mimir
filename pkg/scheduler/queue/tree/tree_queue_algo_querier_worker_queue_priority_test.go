// SPDX-License-Identifier: AGPL-3.0-only

package tree

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test for the expected behavior of the queue algorithm when there is only a single worker.
// Having a number of active workers less than the number of queue nodes is the worst-case scenario
// for the algorithm, potentially resulting in complete queue starvation for some nodes.
// Workers will keep coming back to their assigned queue as long as the queue is not empty,
// and any queues without assigned workers may wait indefinitely until other queue nodes are empty.
func TestQuerierWorkerQueuePriority_SingleWorkerBehavior(t *testing.T) {
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

		// without changing worker ID, dequeue will clear full queue nodes,
		// starting with the first node in the order then moving on to the next empty queue
		{dequeue, QueuePath{"child-1"}, "obj-1"},
		{dequeue, QueuePath{"child-1"}, "obj-6"},
		// child-1 is now empty and removed from rotation
		{dequeue, QueuePath{"child-2"}, "obj-2"},
		{dequeue, QueuePath{"child-2"}, "obj-5"},
		// child-2 is now empty and removed from rotation
		{dequeue, QueuePath{"child-3"}, "obj-3"},

		// enqueue for child-1 again to verify it is added back to rotation
		{enqueue, QueuePath{"child-1"}, "obj-7"},

		// child-3 is still next; child-1 was added back to rotation
		{dequeue, QueuePath{"child-3"}, "obj-4"},
		// child-3 is now empty and removed from rotation; only child-1 remains
		{dequeue, QueuePath{"child-1"}, "obj-7"},
		// nothing left to dequeue
		{dequeue, QueuePath{}, nil},
	}

	querierWorkerPrioritizationQueueAlgo := NewQuerierWorkerQueuePriorityAlgo()

	tree, err := NewTree(querierWorkerPrioritizationQueueAlgo)
	require.NoError(t, err)

	for _, operation := range operationOrder {
		if operation.kind == enqueue {
			err = tree.EnqueueBackByPath(operation.path, operation.obj)
			require.NoError(t, err)
		}
		if operation.kind == dequeue {
			path, obj := tree.Dequeue(nil)
			require.Equal(t, operation.path, path)
			require.Equal(t, operation.obj, obj)
		}
	}
}

// Test for the expected behavior of the queue algorithm when there are multiple workers
// and when the querier-worker queue priority algorithm is used only at the highest layer of the tree.
// When at the highest layer of the tree, the global node count for any existing node is always 1,
// so the queue algorithm will always delete the node from the global node order rotation after it is emptied.
func TestQuerierWorkerQueuePriority_StartPositionByWorker(t *testing.T) {
	const (
		ingesterQueueDimension                = "ingester"
		storeGatewayQueueDimension            = "store-gateway"
		ingesterAndStoreGatewayQueueDimension = "ingester-and-store-gateway"
	)
	querierWorkerPrioritizationQueueAlgo := NewQuerierWorkerQueuePriorityAlgo()

	tree, err := NewTree(querierWorkerPrioritizationQueueAlgo)
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
	path, obj := tree.Dequeue(&DequeueArgs{WorkerID: 0})
	assert.Equal(t, QueuePath{ingesterQueueDimension}, path)
	assert.Equal(t, "obj-1", obj)

	path, obj = tree.Dequeue(&DequeueArgs{WorkerID: 1})
	assert.Equal(t, QueuePath{storeGatewayQueueDimension}, path)
	assert.Equal(t, "obj-2", obj)

	path, obj = tree.Dequeue(&DequeueArgs{WorkerID: 2})
	assert.Equal(t, QueuePath{ingesterAndStoreGatewayQueueDimension}, path)
	assert.Equal(t, "obj-3", obj)

	// worker IDs can come in "out of order" to dequeue, and they will still start at the correct queue
	path, obj = tree.Dequeue(&DequeueArgs{WorkerID: 5})
	assert.Equal(t, QueuePath{ingesterAndStoreGatewayQueueDimension}, path)
	assert.Equal(t, "obj-6", obj)

	path, obj = tree.Dequeue(&DequeueArgs{WorkerID: 3})
	assert.Equal(t, QueuePath{ingesterQueueDimension}, path)
	assert.Equal(t, "obj-4", obj)

	path, obj = tree.Dequeue(&DequeueArgs{WorkerID: 4})
	assert.Equal(t, QueuePath{storeGatewayQueueDimension}, path)
	assert.Equal(t, "obj-5", obj)

	// only 1 item left in each queue; as queue nodes are emptied and deleted,
	// worker IDs will be remapped to different node types by the modulo operation
	path, obj = tree.Dequeue(&DequeueArgs{WorkerID: 0})
	assert.Equal(t, QueuePath{ingesterQueueDimension}, path)
	assert.Equal(t, "obj-7", obj)

	// ingester queue empty and deleted: 2 queues left are ["store-gateway", "ingester-and-store-gateway"]
	// with 2 queues present, first node to be dequeued from is determined by worker ID % 2
	path, obj = tree.Dequeue(&DequeueArgs{WorkerID: 1})
	assert.Equal(t, QueuePath{ingesterAndStoreGatewayQueueDimension}, path)
	assert.Equal(t, "obj-9", obj)

	// ingester-and-store-gateway queue empty and deleted: 1 queue left is just ["store-gateway"]
	// every worker will dequeue from the same queue since there is only 1 left
	path, obj = tree.Dequeue(&DequeueArgs{WorkerID: 999})
	assert.Equal(t, QueuePath{storeGatewayQueueDimension}, path)
	assert.Equal(t, "obj-8", obj)

	path, obj = tree.Dequeue(nil)
	assert.Equal(t, QueuePath{}, path)
	// tree is now empty
	assert.Nil(t, obj)
}

// Test for the expected behavior of the queue algorithm when there are multiple workers
// and when the querier-worker queue priority algorithm is used at a layer of the tree below the highest layer.
//
// The node types managed by this queue algorithm can have many instances created within a tree layer,
// as multiple nodes in the parent tree layer can each have a child node of each type.
//
// The algorithm must only add the node type to the global node order rotation
// if the node is the first of its type created in the tree layer.
// The algorithm must only remove the node type from the global node order rotation
// once all instances of the node type are emptied and removed from the tree layer.
func TestQuerierWorkerQueuePriority_StartPositionByWorker_MultipleNodeCountsInTree(t *testing.T) {
	const (
		ingesterQueueDimension                = "ingester"
		storeGatewayQueueDimension            = "store-gateway"
		ingesterAndStoreGatewayQueueDimension = "ingester-and-store-gateway"
	)
	querierWorkerPrioritizationQueueAlgo := NewQuerierWorkerQueuePriorityAlgo()

	tree, err := NewTree(&RoundRobinState{}, querierWorkerPrioritizationQueueAlgo)
	require.NoError(t, err)

	// enqueue 2 objects each to 2 different children, each with 3 different grandchildren;
	// the highest layer is managed by a vanilla round-robin rotation
	// and the second-highest layer is managed by the querier-worker queue priority algorithm.
	// The global node order rotation for the querier-worker queue priority algorithm is only affected
	// when the first node of a type is created in the tree layer or the last node of a type is emptied and removed.
	// To keep things brief "i", "sg", and "isg" are used for "ingester", "store-gateway", and "ingester-and-store-gateway".

	// first creation of an ingester node anywhere adds ingester node to global node order
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{"a", ingesterQueueDimension}, "obj-a-i-1"))
	// same for store-gateway node
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{"b", storeGatewayQueueDimension}, "obj-b-sg-1"))
	// same for ingester-and-store-gateway node
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{"b", ingesterAndStoreGatewayQueueDimension}, "obj-b-isg-1"))
	// further node creation of those same types does not affect the global order
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{"b", ingesterQueueDimension}, "obj-b-i-1"))
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{"a", storeGatewayQueueDimension}, "obj-a-sg-1"))
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{"a", ingesterAndStoreGatewayQueueDimension}, "obj-a-isg-1"))

	// fill in a second object for each of the 6 paths created above
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{"a", ingesterQueueDimension}, "obj-a-i-2"))
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{"a", storeGatewayQueueDimension}, "obj-a-sg-2"))
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{"a", ingesterAndStoreGatewayQueueDimension}, "obj-a-isg-2"))
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{"b", ingesterQueueDimension}, "obj-b-i-2"))
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{"b", storeGatewayQueueDimension}, "obj-b-sg-2"))
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{"b", ingesterAndStoreGatewayQueueDimension}, "obj-b-isg-2"))

	/* balanced tree structure before dequeuing:
		root
		├── a
		│   ├── ingester
		│	│	├── obj-a-i-1
	    │   │   ├── obj-a-i-2
		│   ├── store-gateway
		│	│	├── obj-a-sg-1
	    │   │   ├── obj-a-sg-2
		│   └── ingester-and-store-gateway
		│	 	├── obj-a-isg-1
	    │       ├── obj-a-isg-2
		├── b
		│   ├── ingester
		│	│	├── obj-b-i-1
	    │   │   ├── obj-b-i-2
		│   ├── store-gateway
		│	│	├── obj-b-sg-1
	    │   │   ├── obj-b-sg-2
		│   └── ingester-and-store-gateway
		│	 	├── obj-b-isg-1
	    │       ├── obj-b-isg-2
	*/

	// node order was set by initial enqueue order;
	assert.Equal(t,
		[]string{ingesterQueueDimension, storeGatewayQueueDimension, ingesterAndStoreGatewayQueueDimension},
		querierWorkerPrioritizationQueueAlgo.nodeOrder,
	)

	// show two-layer behavior (with top layer as vanilla round-robin) before any nodes are deleted
	// with 3 queues present, first node to be dequeued from is determined by worker ID % 3
	path, obj := tree.Dequeue(&DequeueArgs{WorkerID: 0})
	assert.Equal(t, QueuePath{"a", ingesterQueueDimension}, path)
	assert.Equal(t, "obj-a-i-1", obj)

	path, obj = tree.Dequeue(&DequeueArgs{WorkerID: 1})
	assert.Equal(t, QueuePath{"b", storeGatewayQueueDimension}, path)
	assert.Equal(t, "obj-b-sg-1", obj)

	path, obj = tree.Dequeue(&DequeueArgs{WorkerID: 2})
	assert.Equal(t, QueuePath{"a", ingesterAndStoreGatewayQueueDimension}, path)
	assert.Equal(t, "obj-a-isg-1", obj)

	// worker IDs can come in "out of order" to dequeue, and they will still start at the correct queue
	path, obj = tree.Dequeue(&DequeueArgs{WorkerID: 5})
	assert.Equal(t, QueuePath{"b", ingesterAndStoreGatewayQueueDimension}, path)
	assert.Equal(t, "obj-b-isg-1", obj)

	path, obj = tree.Dequeue(&DequeueArgs{WorkerID: 4})
	assert.Equal(t, QueuePath{"a", storeGatewayQueueDimension}, path)
	assert.Equal(t, "obj-a-sg-1", obj)

	path, obj = tree.Dequeue(&DequeueArgs{WorkerID: 3})
	assert.Equal(t, QueuePath{"b", ingesterQueueDimension}, path)
	assert.Equal(t, "obj-b-i-1", obj)

	// only 1 item left in each queue but there are still two queues of each type in the layer;
	// as queue nodes are emptied and deleted, worker IDs will *only* be remapped to different node types
	// when the last node of each type is deleted and the node type is removed from the global order.

	// dequeue with a worker ID mapped to the ingester node type
	path, obj = tree.Dequeue(&DequeueArgs{WorkerID: 0})
	assert.Equal(t, QueuePath{"a", ingesterQueueDimension}, path)
	assert.Equal(t, "obj-a-i-2", obj)
	// node at path "a/ingester" is now empty and deleted but "ingester" is still in the global order
	assert.Equal(t,
		[]string{ingesterQueueDimension, storeGatewayQueueDimension, ingesterAndStoreGatewayQueueDimension},
		querierWorkerPrioritizationQueueAlgo.nodeOrder,
	)

	// dequeue again with a worker ID mapped to the ingester node type
	path, obj = tree.Dequeue(&DequeueArgs{WorkerID: 3})
	assert.Equal(t, QueuePath{"b", ingesterQueueDimension}, path)
	assert.Equal(t, "obj-b-i-2", obj)
	// the last node of the "ingester" type is empty and deleted, it is removed from the global order
	assert.Equal(t,
		[]string{storeGatewayQueueDimension, ingesterAndStoreGatewayQueueDimension},
		querierWorkerPrioritizationQueueAlgo.nodeOrder,
	)

	// subsequent dequeues demonstrate that worker IDs are remapped to the remaining node types

	// dequeue with a worker ID mapped to the ingester-and-store-gateway node type
	path, obj = tree.Dequeue(&DequeueArgs{WorkerID: 1})
	assert.Equal(t, QueuePath{"a", ingesterAndStoreGatewayQueueDimension}, path)
	assert.Equal(t, "obj-a-isg-2", obj)
	// node at path "a/ingester-and-store-gateway" is now empty and deleted but "ingester-and-store-gateway" is still in the global order
	assert.Equal(t,
		[]string{storeGatewayQueueDimension, ingesterAndStoreGatewayQueueDimension},
		querierWorkerPrioritizationQueueAlgo.nodeOrder,
	)

	// dequeue with a worker ID mapped to the store-gateway node type
	path, obj = tree.Dequeue(&DequeueArgs{WorkerID: 2})
	assert.Equal(t, QueuePath{"b", storeGatewayQueueDimension}, path)
	assert.Equal(t, "obj-b-sg-2", obj)
	// node at path "b/store-gateway" is now empty and deleted but "store-gateway" is still in the global order
	assert.Equal(t,
		[]string{storeGatewayQueueDimension, ingesterAndStoreGatewayQueueDimension},
		querierWorkerPrioritizationQueueAlgo.nodeOrder,
	)

	// dequeue with a worker ID mapped to the ingester-and-store-gateway node type
	path, obj = tree.Dequeue(&DequeueArgs{WorkerID: 1})
	assert.Equal(t, QueuePath{"b", ingesterAndStoreGatewayQueueDimension}, path)
	assert.Equal(t, "obj-b-isg-2", obj)
	// the last node of the "ingester-and-store-gateway" type is empty and deleted, it is removed from the global order
	assert.Equal(t,
		[]string{storeGatewayQueueDimension},
		querierWorkerPrioritizationQueueAlgo.nodeOrder,
	)

	// no need to re-assign the current querier-worker ID; there is only 1 node type left
	// so any worker ID will dequeue from the same node type
	path, obj = tree.Dequeue(nil)
	assert.Equal(t, QueuePath{"a", storeGatewayQueueDimension}, path)
	assert.Equal(t, "obj-a-sg-2", obj)
	// the last node of the "store-gateway" type is empty and deleted, it is removed from the global order
	assert.Equal(t,
		[]string{},
		querierWorkerPrioritizationQueueAlgo.nodeOrder,
	)

	path, obj = tree.Dequeue(nil)
	assert.Equal(t, QueuePath{}, path)
	// tree is now empty
	assert.Nil(t, obj)
}
