package queue

import (
	"container/list"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTreeQueue(t *testing.T) {

	expectedTreeQueue := &TreeQueue{
		name:            "root",
		localQueue:      list.New(),
		index:           -1,
		childQueueOrder: []string{"0", "1", "2"},
		childQueueMap: map[string]*TreeQueue{
			"0": {
				name:            "0",
				localQueue:      list.New(),
				index:           -1,
				childQueueOrder: nil,
				childQueueMap:   map[string]*TreeQueue{},
			},
			"1": {
				name:            "1",
				localQueue:      list.New(),
				index:           -1,
				childQueueOrder: []string{"0"},
				childQueueMap: map[string]*TreeQueue{
					"0": {
						name:            "0",
						localQueue:      list.New(),
						index:           -1,
						childQueueOrder: nil,
						childQueueMap:   map[string]*TreeQueue{},
					},
				},
			},
			"2": {
				name:            "2",
				localQueue:      list.New(),
				index:           -1,
				childQueueOrder: []string{"0", "1"},
				childQueueMap: map[string]*TreeQueue{
					"0": {
						name:            "0",
						localQueue:      list.New(),
						index:           -1,
						childQueueOrder: nil,
						childQueueMap:   map[string]*TreeQueue{},
					},
					"1": {
						name:            "1",
						localQueue:      list.New(),
						index:           -1,
						childQueueOrder: nil,
						childQueueMap:   map[string]*TreeQueue{},
					},
				},
			},
		},
	}

	root := NewTreeQueue("root") // creates path: root

	root.getOrAddQueue([]string{"0"})      // creates paths: root:0
	root.getOrAddQueue([]string{"1", "0"}) // creates paths: root:1 and root:1:0
	root.getOrAddQueue([]string{"2", "0"}) // creates paths: root:2 and root:2:0
	root.getOrAddQueue([]string{"2", "1"}) // creates paths: root:2:1 only, as root:2 already exists

	assert.Equal(t, expectedTreeQueue, root)

	// enqueue in order
	root.Enqueue([]string{"0"}, "root:0:val0")
	root.Enqueue([]string{"1"}, "root:1:val0")
	root.Enqueue([]string{"1"}, "root:1:val1")
	root.Enqueue([]string{"2"}, "root:2:val0")
	root.Enqueue([]string{"1", "0"}, "root:1:0:val0")
	root.Enqueue([]string{"1", "0"}, "root:1:0:val1")
	root.Enqueue([]string{"2", "0"}, "root:2:0:val0")
	root.Enqueue([]string{"2", "0"}, "root:2:0:val1")
	root.Enqueue([]string{"2", "1"}, "root:2:1:val0")
	root.Enqueue([]string{"2", "1"}, "root:2:1:val1")
	root.Enqueue([]string{"2", "1"}, "root:2:1:val2")

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
		"root:2:1:val1",
		"root:2:1:val2", // root:2:1:localQueue is done; no other queues in root:2, so root:2 is done as well
		// back up to root; its local queue is done and all childQueueOrder are done, so the full tree is done
	}

	var queueOutput []any
	for {
		v := root.Dequeue()
		if v == nil {
			break
		}
		queueOutput = append(queueOutput, v)
	}
	assert.Equal(t, expectedQueueOutput, queueOutput)
}
