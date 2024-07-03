// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryComponentUtilizationDequeue_DefaultRoundRobin(t *testing.T) {
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

	connectedWorkers := 10
	// with 10 connected workers, if queue len >= waiting workers,
	// no component can have more than 6 inflight requests.
	testReservedCapacity := 0.4

	var err error
	queryComponentUtilization, err := NewQueryComponentUtilization(testQuerierInflightRequestsGauge())
	require.NoError(t, err)

	utilizationTriggerCheckImpl := NewQueryComponentUtilizationTriggerCheckByQueueLenAndWaitingConns(1)
	utilizationCheckThresholdImpl := &queryComponentUtilizationReserveConnections{
		utilization:            queryComponentUtilization,
		targetReservedCapacity: testReservedCapacity,
		connectedWorkers:       connectedWorkers,
	}

	queryComponentUtilizationQueueAlgo := queryComponentUtilizationDequeueSkipOverThreshold{
		queryComponentUtilizationTriggerCheck:   utilizationTriggerCheckImpl,
		queryComponentUtilizationCheckThreshold: utilizationCheckThresholdImpl,
		currentNodeOrderIndex:                   -1,
	}

	tree, err := NewTree(&queryComponentUtilizationQueueAlgo, &roundRobinState{})
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

func TestQueryComponentUtilizationDequeue_SkipComponentExceedsThreshold(t *testing.T) {
	connectedWorkers := 5
	// with 5 connected workers, if queue len >= waiting workers,
	// no component can have more than 3 inflight requests.
	testReservedCapacity := 0.4

	var err error
	queryComponentUtilization, err := NewQueryComponentUtilization(testQuerierInflightRequestsGauge())
	require.NoError(t, err)

	utilizationTriggerCheckImpl := NewQueryComponentUtilizationTriggerCheckByQueueLenAndWaitingConns(1)
	utilizationCheckThresholdImpl := &queryComponentUtilizationReserveConnections{
		utilization:            queryComponentUtilization,
		targetReservedCapacity: testReservedCapacity,
		connectedWorkers:       connectedWorkers,
	}

	queryComponentUtilizationQueueAlgo := queryComponentUtilizationDequeueSkipOverThreshold{
		queryComponentUtilizationTriggerCheck:   utilizationTriggerCheckImpl,
		queryComponentUtilizationCheckThreshold: utilizationCheckThresholdImpl,
		currentNodeOrderIndex:                   -1,
	}

	tree, err := NewTree(&queryComponentUtilizationQueueAlgo, &roundRobinState{})
	require.NoError(t, err)

	// enqueue 2 objects each to 3 different children;
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{ingesterQueueDimension}, "obj-1"))                // ingester node created
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{storeGatewayQueueDimension}, "obj-2"))            // store-gateway node created
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{ingesterAndStoreGatewayQueueDimension}, "obj-3")) // ingester-and-store-gateway node created
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{ingesterQueueDimension}, "obj-4"))
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{storeGatewayQueueDimension}, "obj-5"))
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{ingesterAndStoreGatewayQueueDimension}, "obj-6"))

	// set initial query component utilization to a state where:
	// * queue is backlogged, so the utilization check will be triggered
	// * ingester queue dimension exceeds the utilization threshold
	queryComponentUtilization.querierInflightRequestsTotal = 4
	queryComponentUtilization.ingesterInflightRequests = 4
	utilizationTriggerCheckImpl.queueLen = tree.ItemCount()

	// start dequeue

	// next node would be the ingester queue dimension if there were no utilization checks
	queryComponentUtilizationQueueAlgo.currentNodeOrderIndex = 0 // set to first node since we do not care about local queue
	nextNodeName := defaultNextNodeName(queryComponentUtilizationQueueAlgo)
	assert.Equal(t, ingesterQueueDimension, nextNodeName)

	// ingester queue dimension exceeds the utilization threshold,
	// so it should be skipped and we get store gateway instead
	path, obj := tree.Dequeue()
	assert.Equal(t, QueuePath{storeGatewayQueueDimension}, path)
	assert.Equal(t, "obj-2", obj)

	// next node would be the ingester-and-store-gateway queue dimension
	nextNodeName = defaultNextNodeName(queryComponentUtilizationQueueAlgo)
	assert.Equal(t, ingesterAndStoreGatewayQueueDimension, nextNodeName)

	// but ingester-and-store-gateway queue dimension involves the ingester queue dimension;
	// it will be skipped, we will rotate back to ingester node again and skip that as well,
	// so we get the store-gateway queue dimension again
	path, obj = tree.Dequeue()
	assert.Equal(t, QueuePath{storeGatewayQueueDimension}, path)
	assert.Equal(t, "obj-5", obj)

	// store-gateway queue dimension node is now empty and deleted from the tree and the rotation
	// the algorithm will continue to first skip the ingester and ingester-and-store-gateway nodes
	// in search of a node that does not involve the ingester queue dimension.

	// next node would be the ingester-and-store-gateway queue dimension again;
	nextNodeName = defaultNextNodeName(queryComponentUtilizationQueueAlgo)
	assert.Equal(t, ingesterAndStoreGatewayQueueDimension, nextNodeName)

	// both remaining nodes involve the ingesters and will be marked as exceeding the utilization threshold
	// after skipping both, we will return to the front of the skipped list and get the ingester-and-store-gateway node
	path, obj = tree.Dequeue()
	assert.Equal(t, QueuePath{ingesterAndStoreGatewayQueueDimension}, path)
	assert.Equal(t, "obj-3", obj)

	// now next node would be the ingester queue dimension
	nextNodeName = defaultNextNodeName(queryComponentUtilizationQueueAlgo)
	assert.Equal(t, ingesterQueueDimension, nextNodeName)

	// again we first skip both, but return to the front of the skipped list and get the ingester node
	path, obj = tree.Dequeue()
	assert.Equal(t, QueuePath{ingesterQueueDimension}, path)
	assert.Equal(t, "obj-1", obj)

	// next node would be the ingester-and-store-gateway queue dimension again
	nextNodeName = defaultNextNodeName(queryComponentUtilizationQueueAlgo)
	assert.Equal(t, ingesterAndStoreGatewayQueueDimension, nextNodeName)

	// enqueue to the store-gateway queue again to show that it will be selected next
	// actual place in the rotation will be before the current next node, which is the ingester-and-store-gateway node
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{storeGatewayQueueDimension}, "obj-7"))
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{storeGatewayQueueDimension}, "obj-8"))

	// utilization threshold is still exceeded; anything involving ingesters is skipped and we get store gateway instead
	path, obj = tree.Dequeue()
	assert.Equal(t, QueuePath{storeGatewayQueueDimension}, path)
	assert.Equal(t, "obj-7", obj)

	// next node after store-gateway node is ingester-and-store-gateway node
	nextNodeName = defaultNextNodeName(queryComponentUtilizationQueueAlgo)
	assert.Equal(t, ingesterAndStoreGatewayQueueDimension, nextNodeName)

	// now set query component utilization to a state where:
	// * queue is backlogged, so the utilization check will be triggered
	// * ingester queue dimension does NOT exceed the utilization threshold
	queryComponentUtilization.querierInflightRequestsTotal = 4
	queryComponentUtilization.ingesterInflightRequests = 2

	// as utilization threshold is no longer exceeded, we will get a standard round-robin until empty
	path, obj = tree.Dequeue()
	assert.Equal(t, QueuePath{ingesterAndStoreGatewayQueueDimension}, path)
	assert.Equal(t, "obj-6", obj)

	path, obj = tree.Dequeue()
	assert.Equal(t, QueuePath{ingesterQueueDimension}, path)
	assert.Equal(t, "obj-4", obj)

	path, obj = tree.Dequeue()
	assert.Equal(t, QueuePath{storeGatewayQueueDimension}, path)
	assert.Equal(t, "obj-8", obj)

	path, obj = tree.Dequeue()
	assert.Equal(t, QueuePath{}, path)
	assert.Nil(t, obj)
}

func defaultNextNodeName(queryComponentUtilizationQueueAlgo queryComponentUtilizationDequeueSkipOverThreshold) string {
	// no handling of index -1 here for localQueue as the algorithm is implemented to always skip it
	return queryComponentUtilizationQueueAlgo.nodeOrder[queryComponentUtilizationQueueAlgo.currentNodeOrderIndex]
}
