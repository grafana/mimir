// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const maxTestQueueLen = 8

func TestTreeQueue(t *testing.T) {

	root := NewTreeQueue("root", maxTestQueueLen) // creates path: root

	type queuePathItem struct {
		path QueuePath
		item any
	}

	// note no queue at a given level is dequeued from twice in a row
	// unless all others at the same level are empty down to the leaf node
	queuePathItems := []queuePathItem{
		{QueuePath{"root", "0"}, "root:0:val0"},
		{QueuePath{"root", "1"}, "root:1:val0"},
		{QueuePath{"root", "2"}, "root:2:val0"},
		{QueuePath{"root", "1", "0"}, "root:1:0:val0"},
		{QueuePath{"root", "2", "0"}, "root:2:0:val0"},
		{QueuePath{"root", "1"}, "root:1:val1"},
		{QueuePath{"root", "2", "1"}, "root:2:1:val0"},
		{QueuePath{"root", "1", "0"}, "root:1:0:val1"},
		{QueuePath{"root", "2", "0"}, "root:2:0:val1"},
		{QueuePath{"root", "2", "1"}, "root:2:1:val1"},
		{QueuePath{"root", "2", "1"}, "root:2:1:val2"},
	}

	// build tree by enqueueing in order
	for _, pathItem := range queuePathItems {
		childPath := pathItem.path[1:]
		require.Nil(t, root.EnqueueBackByPath(childPath, pathItem.item))
	}

	// assert all expected child paths have been created; empty child path {} serves as root path
	childQueuePaths := []QueuePath{
		{}, {"0"}, {"1"}, {"1", "0"}, {"2"}, {"2", "0"}, {"2", "1"},
	}
	assert.Equal(t, len(childQueuePaths), root.NodeCount())
	for _, childQueuePath := range childQueuePaths {
		assert.NotNil(t, root.getNode(childQueuePath))
	}

	// check some nonexistent paths
	assert.Nil(t, root.getNode(QueuePath{"3"}))
	assert.Nil(t, root.getNode(QueuePath{"1", "1"}))
	assert.Nil(t, root.getNode(QueuePath{"2", "2"}))

	// queuePathItems, plus an extra item which will be enqueued during dequeueing
	// note no queue at a given level is dequeued from twice in a row
	// unless all others at the same level are empty down to the leaf node
	expectedQueueOutput := []queuePathItem{
		{QueuePath{"root", "0"}, "root:0:val0"}, // root:0:localQueue is done
		{QueuePath{"root", "1"}, "root:1:val0"},
		{QueuePath{"root", "2"}, "root:2:val0"}, // root:2:localQueue is done
		{QueuePath{"root", "1", "0"}, "root:1:0:val0"},
		{QueuePath{"root", "2", "0"}, "root:2:0:val0"},
		{QueuePath{"root", "1"}, "root:1:val1"}, // root:1:localQueue is done
		{QueuePath{"root", "2", "1"}, "root:2:1:val0"},
		{QueuePath{"root", "1", "0"}, "root:1:0:val1"}, // root:1:0:localQueue is done; no other queues in root:1, so root:1 is done as well
		{QueuePath{"root", "2", "0"}, "root:2:0:val1"}, // root:2:0:localQueue is done
		{QueuePath{"root", "1", "0"}, "root:1:0:val2"}, // this was enqueued during dequeueing, which re-creates the deleted root:1 and its child root:1:0
		{QueuePath{"root", "2", "1"}, "root:2:1:val1"}, // once again root:1:0:localQueue is done; no other queues in root:1, so root:1 is done as well
		{QueuePath{"root", "2", "1"}, "root:2:1:val2"}, // root:2:1:localQueue is done; no other queues in root:2, so root:2 is done as well
		// back up to root; its local queue is done and all childQueueOrder are done, so the full tree is done
	}

	var queueOutput []queuePathItem
	for range expectedQueueOutput {
		path, v := root.Dequeue()
		if v == nil {
			fmt.Println(path)
			break
		}
		queueOutput = append(queueOutput, queuePathItem{path, v})
		if v == "root:1:0:val1" {
			// root:1 and all subqueues are completely exhausted;
			// root:2 will be next in the rotation
			// here we insert something new into root:1 to test that:
			//  - the new root:1 insert does not jump the line in front of root:2
			//  - root:2 will not be dequeued from twice in a row now that there is a item in root:1 again
			require.NoError(t, root.EnqueueBackByPath(QueuePath{"1", "0"}, "root:1:0:val2"))
		}
	}
	assert.Equal(t, expectedQueueOutput, queueOutput)

	// Dequeue one more time;
	path, v := root.Dequeue()
	assert.Nil(t, v) // assert we get nil back,
	assert.Nil(t, path)
	assert.True(t, root.IsEmpty()) // assert nothing in local or child queues
}

func TestDequeue(t *testing.T) {
	root := makeUnbalancedTreeQueue(t)

	// dequeue from root until exhausted
	dequeuedPath, v := root.Dequeue()
	assert.Equal(t, "root:0:val0", v)
	assertDequeuedPathValueMatch(t, dequeuedPath, v)

	// root:0 and any subtrees are exhausted
	path := QueuePath{"0"}
	dequeuedPath, v = root.DequeueByPath(path)
	assert.Nil(t, v)
	assert.Nil(t, dequeuedPath)
	// root:0 was deleted
	assert.Nil(t, root.getNode(path))

	dequeuedPath, v = root.Dequeue()
	assert.Equal(t, "root:1:val0", v)
	assertDequeuedPathValueMatch(t, dequeuedPath, v)

	dequeuedPath, v = root.Dequeue()
	assert.Equal(t, "root:2:0:val0", v)
	assertDequeuedPathValueMatch(t, dequeuedPath, v)

	dequeuedPath, v = root.Dequeue()
	assert.Equal(t, "root:1:0:val0", v)
	assertDequeuedPathValueMatch(t, dequeuedPath, v)

	dequeuedPath, v = root.Dequeue()
	assert.Equal(t, "root:2:1:val0", v)
	assertDequeuedPathValueMatch(t, dequeuedPath, v)

	dequeuedPath, v = root.Dequeue()
	assert.Equal(t, "root:1:val1", v)
	assertDequeuedPathValueMatch(t, dequeuedPath, v)

	dequeuedPath, v = root.Dequeue()
	assert.Equal(t, "root:2:0:val1", v)
	assertDequeuedPathValueMatch(t, dequeuedPath, v)

	dequeuedPath, v = root.Dequeue()
	assert.Equal(t, "root:1:0:val1", v)
	assertDequeuedPathValueMatch(t, dequeuedPath, v)

	// root:1 and any subtrees are exhausted
	path = QueuePath{"1"}
	dequeuedPath, v = root.DequeueByPath(path)
	assert.Nil(t, v)
	assert.Nil(t, dequeuedPath)
	// root:1 was deleted
	assert.Nil(t, root.getNode(path))

	dequeuedPath, v = root.Dequeue()
	assert.Equal(t, "root:2:1:val1", v)
	assertDequeuedPathValueMatch(t, dequeuedPath, v)

	dequeuedPath, v = root.Dequeue()
	assert.Equal(t, "root:2:1:val2", v)
	assertDequeuedPathValueMatch(t, dequeuedPath, v)

	// root:2 and any subtrees are exhausted
	path = QueuePath{"2"}
	dequeuedPath, v = root.DequeueByPath(path)
	assert.Nil(t, v)
	assert.Nil(t, dequeuedPath)
	// root:2 was deleted
	assert.Nil(t, root.getNode(path))

	// all items have been dequeued
	assert.Equal(t, 0, root.ItemCount())
	assert.Equal(t, 1, root.NodeCount())

	// assert nothing in local or child queues
	assert.True(t, root.IsEmpty())
}

func TestDequeuePath(t *testing.T) {
	root := makeUnbalancedTreeQueue(t)

	// dequeue from root:2 until exhausted
	path := QueuePath{"2"}
	dequeuedPath, v := root.DequeueByPath(path)
	assert.Equal(t, "root:2:0:val0", v)
	assertDequeuedPathValueMatch(t, dequeuedPath, v)

	dequeuedPath, v = root.DequeueByPath(path)
	assert.Equal(t, "root:2:1:val0", v)
	assertDequeuedPathValueMatch(t, dequeuedPath, v)

	dequeuedPath, v = root.DequeueByPath(path)
	assert.Equal(t, "root:2:0:val1", v)
	assertDequeuedPathValueMatch(t, dequeuedPath, v)

	dequeuedPath, v = root.DequeueByPath(path)
	assert.Equal(t, "root:2:1:val1", v)
	assertDequeuedPathValueMatch(t, dequeuedPath, v)

	dequeuedPath, v = root.DequeueByPath(path)
	assert.Equal(t, "root:2:1:val2", v)
	assertDequeuedPathValueMatch(t, dequeuedPath, v)

	// root:2 is exhausted;
	dequeuedPath, v = root.DequeueByPath(path)
	assert.Nil(t, v)
	assert.Nil(t, dequeuedPath)
	// root:2 and its two children root:2:0 and root:2:1 were deleted
	assert.Nil(t, root.getNode(path))
	assert.Equal(t, 2, len(root.childQueueMap))
	assert.Equal(t, 2, len(root.childQueueOrder))
	assert.Equal(t, 4, root.NodeCount())
	// 5 of 10 items were dequeued
	assert.Equal(t, 5, root.ItemCount())

	// dequeue from root:1 until exhausted
	path = QueuePath{"1"}
	dequeuedPath, v = root.DequeueByPath(path)
	assert.Equal(t, "root:1:val0", v)
	assertDequeuedPathValueMatch(t, dequeuedPath, v)

	dequeuedPath, v = root.DequeueByPath(path)
	assert.Equal(t, "root:1:0:val0", v)
	assertDequeuedPathValueMatch(t, dequeuedPath, v)

	dequeuedPath, v = root.DequeueByPath(path)
	assert.Equal(t, "root:1:val1", v)
	assertDequeuedPathValueMatch(t, dequeuedPath, v)

	dequeuedPath, v = root.DequeueByPath(path)
	assert.Equal(t, "root:1:0:val1", v)
	assertDequeuedPathValueMatch(t, dequeuedPath, v)

	// root:1 is exhausted;
	dequeuedPath, v = root.DequeueByPath(path)
	assert.Nil(t, v)
	assert.Nil(t, dequeuedPath)
	// root:1 and its child root:1:0 were deleted
	assert.Nil(t, root.getNode(path))
	assert.Equal(t, 1, len(root.childQueueMap))
	assert.Equal(t, 1, len(root.childQueueOrder))
	assert.Equal(t, 2, root.NodeCount())
	// 9 of 10 items have been dequeued
	assert.Equal(t, 1, root.ItemCount())

	// dequeue from root:0 until exhausted
	path = QueuePath{"0"}
	dequeuedPath, v = root.DequeueByPath(path)
	assert.Equal(t, "root:0:val0", v)
	assertDequeuedPathValueMatch(t, dequeuedPath, v)

	// root:0 is exhausted;
	dequeuedPath, v = root.DequeueByPath(path)
	assert.Nil(t, v)
	assert.Nil(t, dequeuedPath)
	// root:0 was deleted
	assert.Nil(t, root.getNode(path))
	assert.Equal(t, 0, len(root.childQueueMap))
	assert.Equal(t, 0, len(root.childQueueOrder))
	assert.Equal(t, 1, root.NodeCount())
	// 10 of 10 items have been dequeued
	assert.Equal(t, 0, root.ItemCount())
	assert.Equal(t, 1, root.NodeCount())

	// assert nothing in local or child queues
	assert.True(t, root.IsEmpty())
}

func TestEnqueueDuringDequeueRespectsRoundRobin(t *testing.T) {
	root := NewTreeQueue("root", maxTestQueueLen)

	cache := map[string]struct{}{}

	// enqueue two items to path root:0
	childPath := QueuePath{"0"}
	item := makeQueueItemForChildPath(root, childPath, cache)
	require.NoError(t, root.EnqueueBackByPath(childPath, item))
	item = makeQueueItemForChildPath(root, childPath, cache)
	require.NoError(t, root.EnqueueBackByPath(childPath, item))

	// enqueue one item to path root:1
	childPath = QueuePath{"1"}
	item = makeQueueItemForChildPath(root, childPath, cache)
	require.NoError(t, root.EnqueueBackByPath(childPath, item))

	// enqueue two items to path root:1
	childPath = QueuePath{"2"}
	item = makeQueueItemForChildPath(root, childPath, cache)
	require.NoError(t, root.EnqueueBackByPath(childPath, item))
	item = makeQueueItemForChildPath(root, childPath, cache)
	require.NoError(t, root.EnqueueBackByPath(childPath, item))

	assert.Equal(t, []string{"0", "1", "2"}, root.childQueueOrder)

	// dequeue first item
	dequeuedPath, _ := root.Dequeue()
	assert.Equal(t, QueuePath{"root", "0"}, dequeuedPath)

	// dequeue second item; root:1 is now exhausted and deleted
	dequeuedPath, _ = root.Dequeue()
	assert.Equal(t, QueuePath{"root", "1"}, dequeuedPath)
	assert.Nil(t, root.getNode(QueuePath{"1"}))
	assert.Equal(t, []string{"0", "2"}, root.childQueueOrder)

	// dequeue third item
	dequeuedPath, _ = root.Dequeue()
	assert.Equal(t, QueuePath{"root", "2"}, dequeuedPath)

	// root:1 was previously exhausted; root:0, then root:2 will be next in the rotation
	// here we insert something new into root:1 to test that it
	// does not jump the line in front of root:0 or root:2
	item = makeQueueItemForChildPath(root, QueuePath{"1"}, cache)
	require.NoError(t, root.EnqueueBackByPath(QueuePath{"1"}, item))
	assert.NotNil(t, root.getNode(QueuePath{"1"}))
	assert.Equal(t, []string{"0", "2", "1"}, root.childQueueOrder)

	// dequeue fourth item; the newly-enqueued root:1 item
	// has not jumped the line in front of root:0
	dequeuedPath, _ = root.Dequeue()
	assert.Equal(t, QueuePath{"root", "0"}, dequeuedPath)

	// dequeue fifth item; the newly-enqueued root:1 item
	// has not jumped the line in front of root:2
	dequeuedPath, _ = root.Dequeue()
	assert.Equal(t, QueuePath{"root", "2"}, dequeuedPath)

	// dequeue sixth item; verifying the order 0->2->1 is being followed
	dequeuedPath, _ = root.Dequeue()
	assert.Equal(t, QueuePath{"root", "1"}, dequeuedPath)

	// all items have been dequeued
	assert.Equal(t, 0, root.ItemCount())
	assert.Equal(t, 1, root.NodeCount())

	// assert nothing in local or child queues
	assert.True(t, root.IsEmpty())
}

func makeUnbalancedTreeQueue(t *testing.T) *TreeQueue {
	/*
	   root
	   ├── child0
	   │		 └── localQueue
	   │		     └── val0
	   ├── child1
	   │		 ├── child0
	   │		 │		 └── localQueue
	   │		 │		     ├── val0
	   │		 │		     └── val1
	   │		 └── localQueue
	   │		     ├── val0
	   │		     └── val1
	   ├── child2
	   │		 ├── child0
	   │		 │		 └── localQueue
	   │		 │		     ├── val0
	   │		 │		     └── val1
	   │		 ├── child1
	   │		 │		 └── localQueue
	   │		 │		     ├── val0
	   │		 │		     ├── val1
	   │		 │		     └── val2
	   │		 └── localQueue
	   └── localQueue
	*/
	root := NewTreeQueue("root", maxTestQueueLen)
	require.Equal(t, 1, root.NodeCount())
	require.Equal(t, 0, root.ItemCount())

	cache := map[string]struct{}{}

	// enqueue one item to root:0
	childPath := QueuePath{"0"}
	item := makeQueueItemForChildPath(root, childPath, cache)
	require.NoError(t, root.EnqueueBackByPath(childPath, item))
	require.Equal(t, 2, root.NodeCount())
	require.Equal(t, 1, root.ItemCount())

	// enqueue two items to root:1
	childPath = QueuePath{"1"}
	item = makeQueueItemForChildPath(root, childPath, cache)
	require.NoError(t, root.EnqueueBackByPath(childPath, item))
	require.Equal(t, 3, root.NodeCount())
	require.Equal(t, 2, root.ItemCount())

	item = makeQueueItemForChildPath(root, childPath, cache)
	require.NoError(t, root.EnqueueBackByPath(childPath, item))
	require.Equal(t, 3, root.NodeCount())
	require.Equal(t, 3, root.ItemCount())

	// enqueue two items to root:1:0
	childPath = QueuePath{"1", "0"}
	item = makeQueueItemForChildPath(root, childPath, cache)
	require.NoError(t, root.EnqueueBackByPath(childPath, item))
	require.Equal(t, 4, root.NodeCount())
	require.Equal(t, 4, root.ItemCount())

	item = makeQueueItemForChildPath(root, childPath, cache)
	require.NoError(t, root.EnqueueBackByPath(childPath, item))
	require.Equal(t, 4, root.NodeCount())
	require.Equal(t, 5, root.ItemCount())

	// enqueue two items to root:2:0
	childPath = QueuePath{"2", "0"}
	item = makeQueueItemForChildPath(root, childPath, cache)
	require.NoError(t, root.EnqueueBackByPath(childPath, item))
	require.Equal(t, 6, root.NodeCount())
	require.Equal(t, 6, root.ItemCount())

	item = makeQueueItemForChildPath(root, childPath, cache)
	require.NoError(t, root.EnqueueBackByPath(childPath, item))
	require.Equal(t, 6, root.NodeCount())
	require.Equal(t, 7, root.ItemCount())

	// enqueue three items to root:2:1
	childPath = QueuePath{"2", "1"}
	item = makeQueueItemForChildPath(root, childPath, cache)
	require.NoError(t, root.EnqueueBackByPath(childPath, item))
	require.Equal(t, 7, root.NodeCount())
	require.Equal(t, 8, root.ItemCount())

	item = makeQueueItemForChildPath(root, childPath, cache)
	require.NoError(t, root.EnqueueBackByPath(childPath, item))
	require.Equal(t, 7, root.NodeCount())
	require.Equal(t, 9, root.ItemCount())

	item = makeQueueItemForChildPath(root, childPath, cache)
	require.NoError(t, root.EnqueueBackByPath(childPath, item))
	require.Equal(t, 7, root.NodeCount())
	require.Equal(t, 10, root.ItemCount())

	return root
}

// makeQueueItemForChildPath constructs a queue item to match its enqueued path
// by joining the path components and appending an incrementing value for each path.
//
// e.g. for a tree named "root":
//   - childQueuePath{"1", "0"}'s first item will be "root:1:0:val0"
//   - childQueuePath{"1", "0"}'s second item will be "root:1:0:val1"
func makeQueueItemForChildPath(
	treeQueue *TreeQueue, childPath QueuePath, cache map[string]struct{},
) string {
	path := append(QueuePath{treeQueue.name}, childPath...)

	i := 0
	for {
		item := strings.Join(path, ":") + fmt.Sprintf(":val%d", i)
		if _, ok := cache[item]; !ok {
			cache[item] = struct{}{}
			return item
		}
		i++
	}
}

// assertDequeuedPathValueMatch checks a dequeued item and its dequeued path
// to ensure the item was dequeued from the expected node of the tree, based
// on the item being generated to match its path via makeQueueItemForChildPath
func assertDequeuedPathValueMatch(t *testing.T, dequeuedPath QueuePath, v any) {
	itemPath := strings.Split(v.(string), ":")
	itemPathPrefix := itemPath[:len(itemPath)-1] // strip value from the end

	require.Equal(t, len(dequeuedPath), len(itemPathPrefix))

	for i, nodeName := range dequeuedPath {
		require.Equal(t, nodeName, itemPathPrefix[i])
	}
}
