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

// TestDequeueBalancedTree checks dequeuing behavior from a balanced tree.
//
// Dequeuing from a balanced tree allows the test to have a simple looped structures
// while running checks to ensure that round-robin order is respected.
func TestDequeueBalancedTree(t *testing.T) {
	firstDimensions := []string{"0", "1", "2"}
	secondDimensions := []string{"a", "b", "c"}
	itemsPerDimension := 5
	root := makeBalancedTreeQueue(t, firstDimensions, secondDimensions, itemsPerDimension)
	assert.NotNil(t, root)

	count := 0
	// tree queue will fairly dequeue from all levels of the tree
	rotationsBeforeRepeat := len(firstDimensions) * len(secondDimensions)
	// track dequeued paths to ensure round-robin dequeuing does not repeat before expected
	dequeuedPathCache := make([]QueuePath, rotationsBeforeRepeat)

	for !root.IsEmpty() {
		dequeuedPath, v := root.Dequeue()
		assertDequeuedPathValueMatch(t, dequeuedPath, v)

		// assert dequeued path has not repeated before the expected number of rotations
		for _, previousDequeuedPath := range dequeuedPathCache {
			assert.NotEqual(t, previousDequeuedPath, dequeuedPath)
		}

		dequeuedPathCache = append(dequeuedPathCache[1:], dequeuedPath)
		count++
	}

	// count items enqueued to nodes at depth 1
	expectedFirstDimensionCount := len(firstDimensions) * itemsPerDimension
	// count items enqueued to nodes at depth 2
	expectedSecondDimensionCount := len(firstDimensions) * len(secondDimensions) * itemsPerDimension

	assert.Equal(t, expectedFirstDimensionCount+expectedSecondDimensionCount, count)
}

// TestDequeueBalancedTree checks dequeuing behavior by path from a balanced tree.
//
// Dequeuing from a balanced tree allows the test to have a simple looped structures
// while running checks to ensure that round-robin order is respected.
func TestDequeueByPathBalancedTree(t *testing.T) {
	firstDimensions := []string{"0", "1", "2"}
	secondDimensions := []string{"a", "b", "c"}
	itemsPerDimension := 5
	root := makeBalancedTreeQueue(t, firstDimensions, secondDimensions, itemsPerDimension)
	assert.NotNil(t, root)

	count := 0
	// tree queue will fairly dequeue from all levels of the tree below the path provided;
	// dequeuing by path skips the top level the tree and rotates only through the
	// second-layer subtrees of the first layer node selected by the queue path
	rotationsBeforeRepeat := len(secondDimensions)
	// track dequeued paths to ensure round-robin dequeuing does not repeat before expected
	dequeuedPathCache := make([]QueuePath, rotationsBeforeRepeat)

	for _, firstDimName := range firstDimensions {
		firstDimPath := QueuePath{firstDimName}
		for root.getNode(firstDimPath) != nil {
			dequeuedPath, v := root.DequeueByPath(firstDimPath)
			assertDequeuedPathValueMatch(t, dequeuedPath, v)

			// assert dequeued path has not repeated before the expected number of rotations
			for _, previousDequeuedPath := range dequeuedPathCache {
				assert.NotEqual(t, previousDequeuedPath, dequeuedPath)
			}

			dequeuedPathCache = append(dequeuedPathCache[1:], dequeuedPath)
			count++
		}
	}

	// count items enqueued to nodes at depth 1
	expectedFirstDimensionCount := len(firstDimensions) * itemsPerDimension
	// count items enqueued to nodes at depth 2
	expectedSecondDimensionCount := len(firstDimensions) * len(secondDimensions) * itemsPerDimension

	assert.Equal(t, expectedFirstDimensionCount+expectedSecondDimensionCount, count)
}

// TestDequeuePathUnbalancedTree checks dequeuing behavior from an unbalanced tree.
//
// Assertions are done one by one to illustrate and check the behaviors of dequeuing from
// an unbalanced tree, where the same node will be dequeued from twice if the node remains
// nonempty while its sibling nodes have been exhausted and deleted from the tree.
func TestDequeueUnbalancedTree(t *testing.T) {
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

// TestDequeueByPathUnbalancedTree checks dequeuing behavior from an unbalanced tree by path.
//
// Assertions are done one by one to illustrate and check the behaviors of dequeuing from
// an unbalanced tree, where the same node will be dequeued from twice if the node remains
// nonempty while its sibling nodes have been exhausted and deleted from the tree.
func TestDequeueByPathUnbalancedTree(t *testing.T) {
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

func makeBalancedTreeQueue(t *testing.T, firstDimensions, secondDimensions []string, itemsPerDimensions int) *TreeQueue {
	root := NewTreeQueue("root", maxTestQueueLen)
	require.Equal(t, 1, root.NodeCount())
	require.Equal(t, 0, root.ItemCount())

	cache := map[string]struct{}{}

	for _, firstDimName := range firstDimensions {
		// insert first dimension local queue items
		for k := 0; k < itemsPerDimensions; k++ {
			childPath := QueuePath{firstDimName}
			item := makeQueueItemForChildPath(root, childPath, cache)
			require.NoError(t, root.EnqueueBackByPath(childPath, item))
		}
		for _, secondDimName := range secondDimensions {
			// insert second dimension local queue items
			for k := 0; k < itemsPerDimensions; k++ {
				childPath := QueuePath{firstDimName, secondDimName}
				item := makeQueueItemForChildPath(root, childPath, cache)
				require.NoError(t, root.EnqueueBackByPath(childPath, item))
			}
		}
	}

	return root
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
