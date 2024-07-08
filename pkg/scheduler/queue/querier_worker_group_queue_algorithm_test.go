// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"testing"

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
