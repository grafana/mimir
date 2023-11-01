// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"container/list"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const maxTestQueueLen = 8

func TestTreeQueue(t *testing.T) {

	expectedTreeQueue := &TreeQueue{
		name:            "root",
		maxQueueLen:     maxTestQueueLen,
		localQueue:      list.New(),
		index:           -1,
		childQueueOrder: []string{"0", "1", "2"},
		childQueueMap: map[string]*TreeQueue{
			"0": {
				name:            "0",
				maxQueueLen:     maxTestQueueLen,
				localQueue:      list.New(),
				index:           -1,
				childQueueOrder: nil,
				childQueueMap:   map[string]*TreeQueue{},
			},
			"1": {
				name:            "1",
				maxQueueLen:     maxTestQueueLen,
				localQueue:      list.New(),
				index:           -1,
				childQueueOrder: []string{"0"},
				childQueueMap: map[string]*TreeQueue{
					"0": {
						name:            "0",
						maxQueueLen:     maxTestQueueLen,
						localQueue:      list.New(),
						index:           -1,
						childQueueOrder: nil,
						childQueueMap:   map[string]*TreeQueue{},
					},
				},
			},
			"2": {
				name:            "2",
				maxQueueLen:     maxTestQueueLen,
				localQueue:      list.New(),
				index:           -1,
				childQueueOrder: []string{"0", "1"},
				childQueueMap: map[string]*TreeQueue{
					"0": {
						name:            "0",
						maxQueueLen:     maxTestQueueLen,
						localQueue:      list.New(),
						index:           -1,
						childQueueOrder: nil,
						childQueueMap:   map[string]*TreeQueue{},
					},
					"1": {
						name:            "1",
						maxQueueLen:     maxTestQueueLen,
						localQueue:      list.New(),
						index:           -1,
						childQueueOrder: nil,
						childQueueMap:   map[string]*TreeQueue{},
					},
				},
			},
		},
	}

	root := NewTreeQueue("root", maxTestQueueLen) // creates path: root

	_, _ = root.getOrAddNode([]string{"root", "0"})      // creates paths: root:0
	_, _ = root.getOrAddNode([]string{"root", "1", "0"}) // creates paths: root:1 and root:1:0
	_, _ = root.getOrAddNode([]string{"root", "2", "0"}) // creates paths: root:2 and root:2:0
	_, _ = root.getOrAddNode([]string{"root", "2", "1"}) // creates paths: root:2:1 only, as root:2 already exists

	assert.Equal(t, expectedTreeQueue, root)

	child := root.getNode(QueuePath{"root", "0"})
	assert.NotNil(t, child)

	child = root.getNode(QueuePath{"root", "1"})
	assert.NotNil(t, child)

	child = root.getNode(QueuePath{"root", "1", "0"})
	assert.NotNil(t, child)

	child = root.getNode([]string{"root", "2"})
	assert.NotNil(t, child)

	child = root.getNode(QueuePath{"root", "2", "0"})
	assert.NotNil(t, child)

	child = root.getNode(QueuePath{"root", "2", "1"})
	assert.NotNil(t, child)

	// nonexistent paths
	child = root.getNode(QueuePath{"root", "3"})

	assert.Nil(t, child)

	child = root.getNode(QueuePath{"root", "1", "1"})

	assert.Nil(t, child)

	child = root.getNode(QueuePath{"root", "2", "2"})
	assert.Nil(t, child)

	// enqueue in order
	require.NoError(t, root.EnqueueBackByPath([]string{"root", "0"}, "root:0:val0"))
	require.NoError(t, root.EnqueueBackByPath([]string{"root", "1"}, "root:1:val0"))
	require.NoError(t, root.EnqueueBackByPath([]string{"root", "1"}, "root:1:val1"))
	require.NoError(t, root.EnqueueBackByPath([]string{"root", "2"}, "root:2:val0"))
	require.NoError(t, root.EnqueueBackByPath([]string{"root", "1", "0"}, "root:1:0:val0"))
	require.NoError(t, root.EnqueueBackByPath([]string{"root", "1", "0"}, "root:1:0:val1"))
	require.NoError(t, root.EnqueueBackByPath([]string{"root", "2", "0"}, "root:2:0:val0"))
	require.NoError(t, root.EnqueueBackByPath([]string{"root", "2", "0"}, "root:2:0:val1"))
	require.NoError(t, root.EnqueueBackByPath([]string{"root", "2", "1"}, "root:2:1:val0"))
	require.NoError(t, root.EnqueueBackByPath([]string{"root", "2", "1"}, "root:2:1:val1"))
	require.NoError(t, root.EnqueueBackByPath([]string{"root", "2", "1"}, "root:2:1:val2"))

	// note no queue at a given level is dequeued from twice in a row
	// unless all others at the same level are empty down to the leaf node
	expectedQueueOutput := []any{
		"root:0:val0", // root:0:localQueue is done
		"root:1:val0",
		"root:2:val0", // root:2:localQueue is done
		"root:1:0:val0",
		"root:2:0:val0",
		"root:1:val1", // root:1:localQueue is done
		"root:2:1:val0",
		"root:1:0:val1", // root:1:0:localQueue is done; no other queues in root:1, so root:1 is done as well
		"root:2:0:val1", // root:2:0 :localQueue is done
		"root:1:0:val2", // this is enqueued during dequeueing
		"root:2:1:val1",
		"root:2:1:val2", // root:2:1:localQueue is done; no other queues in root:2, so root:2 is done as well
		// back up to root; its local queue is done and all childQueueOrder are done, so the full tree is done
	}

	expectedQueuePaths := []QueuePath{
		{"root", "0"},
		{"root", "1"},
		{"root", "2"},
		{"root", "1", "0"},
		{"root", "2", "0"},
		{"root", "1"},
		{"root", "2", "1"},
		{"root", "1", "0"},
		{"root", "2", "0"},
		{"root", "1", "0"},
		{"root", "2", "1"},
		{"root", "2", "1"},
	}

	var queueOutput []any
	var queuePaths []QueuePath
	for range expectedQueueOutput {
		path, v := root.Dequeue()
		if v == nil {
			fmt.Println(path)
			break
		}
		queueOutput = append(queueOutput, v)
		queuePaths = append(queuePaths, path)
		if v == "root:1:0:val1" {
			// root:1 and all subqueues are completely exhausted;
			// root:2 will be next in the rotation
			// here we insert something new into root:1 to test that:
			//  - the new root:1 insert does not jump the line in front of root:2
			//  - root:2 will not be dequeued from twice in a row now that there is a value in root:1 again
			require.NoError(t, root.EnqueueBackByPath([]string{"root", "1", "0"}, "root:1:0:val2"))
		}
	}
	assert.Equal(t, expectedQueueOutput, queueOutput)
	assert.Equal(t, expectedQueuePaths, queuePaths)

	// Dequeue one more time;
	path, v := root.Dequeue()
	assert.Nil(t, v) // assert we get nil back,
	assert.Nil(t, path)
	assert.True(t, root.isEmpty()) // assert nothing in local or child queues
}

func TestDequeuePath(t *testing.T) {
	root := NewTreeQueue("root", maxTestQueueLen)
	require.NoError(t, root.EnqueueBackByPath(QueuePath{"root", "0"}, "root:0:val0"))
	require.NoError(t, root.EnqueueBackByPath(QueuePath{"root", "1"}, "root:1:val0"))
	require.NoError(t, root.EnqueueBackByPath(QueuePath{"root", "1"}, "root:1:val1"))
	require.NoError(t, root.EnqueueBackByPath(QueuePath{"root", "2"}, "root:2:val0"))
	require.NoError(t, root.EnqueueBackByPath(QueuePath{"root", "1", "0"}, "root:1:0:val0"))
	require.NoError(t, root.EnqueueBackByPath(QueuePath{"root", "1", "0"}, "root:1:0:val1"))
	require.NoError(t, root.EnqueueBackByPath(QueuePath{"root", "2", "0"}, "root:2:0:val0"))
	require.NoError(t, root.EnqueueBackByPath(QueuePath{"root", "2", "0"}, "root:2:0:val1"))
	require.NoError(t, root.EnqueueBackByPath(QueuePath{"root", "2", "1"}, "root:2:1:val0"))
	require.NoError(t, root.EnqueueBackByPath(QueuePath{"root", "2", "1"}, "root:2:1:val1"))
	require.NoError(t, root.EnqueueBackByPath(QueuePath{"root", "2", "1"}, "root:2:1:val2"))

	path := QueuePath{"root", "2"}
	dequeuedPath, v := root.DequeueByPath(path)
	assert.Equal(t, "root:2:val0", v)
	assert.Equal(t, path, dequeuedPath[:len(path)])

	dequeuedPath, v = root.DequeueByPath(path)
	assert.Equal(t, "root:2:0:val0", v)
	assert.Equal(t, path, dequeuedPath[:len(path)])

	dequeuedPath, v = root.DequeueByPath(path)
	assert.Equal(t, "root:2:1:val0", v)
	assert.Equal(t, path, dequeuedPath[:len(path)])

	dequeuedPath, v = root.DequeueByPath(path)
	assert.Equal(t, "root:2:0:val1", v)
	assert.Equal(t, path, dequeuedPath[:len(path)])

	dequeuedPath, v = root.DequeueByPath(path)
	assert.Equal(t, "root:2:1:val1", v)
	assert.Equal(t, path, dequeuedPath[:len(path)])

	dequeuedPath, v = root.DequeueByPath(path)
	assert.Equal(t, "root:2:1:val2", v)
	assert.Equal(t, path, dequeuedPath[:len(path)])

	// root:2 is exhausted
	dequeuedPath, v = root.DequeueByPath(path)
	assert.Nil(t, v)
	assert.Nil(t, dequeuedPath)

}
