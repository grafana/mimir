package queue

import (
	"container/list"
	"encoding/json"
	"fmt"
)

type QueuePath []string //nolint:revive
type QueueIndex int

const localQueueIndex = -1

var removedQueueName = string([]byte{byte(0)})

func isValidQueueName(name string) bool {
	return !(name == removedQueueName)
}

// TreeQueue is a hierarchical queue implementation with an arbitrary amount of child queues.
//
// TreeQueue internally maintains round-robin fair queuing across all of its queue dimensions.
// Each queuing dimension is modeled as a node in the tree, internally reachable through a QueuePath.
//
// The QueuePath is an ordered array of strings describing the path through the tree to the node,
// which contains the FIFO local queue of all items enqueued for that queuing dimension.
//
// When dequeuing from a given node, the node will round-robin equally between dequeuing directly
// from its own local queue and dequeuing recursively from its list of child TreeQueues.
// No queue at a given level of the tree is dequeued from consecutively unless all others
// at the same level of the tree are empty down to the leaf node.
type TreeQueue struct {
	// name of the tree node will be set to its segment of the queue path
	name            string
	localQueue      *list.List
	index           int
	childQueueOrder []string
	childQueueMap   map[string]*TreeQueue
}

func NewTreeQueue(name string) *TreeQueue {
	return &TreeQueue{
		name:            name,
		localQueue:      list.New(),
		index:           localQueueIndex,
		childQueueMap:   map[string]*TreeQueue{},
		childQueueOrder: nil,
	}
}

func (q *TreeQueue) Enqueue(path QueuePath, v any) {
	childQueue := q.getOrAddQueue(path)
	childQueue.localQueue.PushBack(v)
}

// getOrAddQueue recursively adds queues based on given path
func (q *TreeQueue) getOrAddQueue(path QueuePath) *TreeQueue {
	if len(path) == 0 {
		return q
	}
	currentPathSegment, remainingPath := path[0], path[1:]
	if !isValidQueueName(currentPathSegment) {
		return nil
	}

	if childQueue, ok := q.childQueueMap[currentPathSegment]; ok {
		// this level of the tree already exists; recur
		return childQueue.getOrAddQueue(remainingPath)
	}

	newChildQueue := NewTreeQueue(currentPathSegment)
	// add new child queue to ordered list for round-robining
	q.childQueueOrder = append(q.childQueueOrder, newChildQueue.name)
	// attach new child queue to lookup map
	q.childQueueMap[currentPathSegment] = newChildQueue

	if len(remainingPath) > 0 {
		// still further tree depth to create; recur
		return newChildQueue.getOrAddQueue(remainingPath)
	}
	// else: no further levels to create; recursion complete
	return newChildQueue
}

func (q *TreeQueue) Dequeue() any {
	v := q.dequeue()
	return v
}

func (q *TreeQueue) dequeue() any {
	var v any
	initialLen := len(q.childQueueOrder)

	for iters := 0; iters <= initialLen && v == nil; iters++ {
		if q.index == localQueueIndex {
			v = q.dequeueLocal()
			if len(q.childQueueMap) > 0 {
				q.index++
			}
		} else {
			childQueueName := q.childQueueOrder[q.index]
			childQueue := q.childQueueMap[childQueueName]

			v = childQueue.dequeue()
			if v != nil {
				q.index++
			} else {
				// selected child tree was checked recursively and is empty
				delete(q.childQueueMap, childQueueName)
				q.childQueueOrder = append(q.childQueueOrder[:q.index], q.childQueueOrder[q.index+1:]...)
				// no need to increment; remainder of the slice has moved left to be under q.index
			}
			if q.index == len(q.childQueueOrder) {
				q.index = localQueueIndex
			}
		}
	}
	return v
}

func (q *TreeQueue) dequeueLocal() any {
	if q.localQueue.Len() == 0 {
		return nil
	}
	elem := q.localQueue.Front()
	q.localQueue.Remove(elem)
	return elem.Value
}

func (q *TreeQueue) incrementIndex() {
	if q.index+1 >= len(q.childQueueOrder) {
		q.index = localQueueIndex
	} else {
		q.index++
	}
}

//func (q *TreeQueue) deleteChildQueue(name string) {
//	delete(q.childQueueMap, name)
//	q.childQueueOrder = append(q.childQueueOrder[:q.index], q.childQueueOrder[q.index+1:]...)
//}

// String makes the queue printable
func (q *TreeQueue) String() string {
	bytes, _ := json.MarshalIndent(q, "\t", "\t")
	return fmt.Sprintln(string(bytes))
}
