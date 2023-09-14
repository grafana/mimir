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

	if directChildQueue, ok := q.childQueueMap[currentPathSegment]; ok {
		// this level of the tree already exists; recur
		return directChildQueue.getOrAddQueue(remainingPath)
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

	// recursion complete
	return newChildQueue
}

func (q *TreeQueue) Dequeue() any {
	v, _ := q.dequeue()
	return v
}

func (q *TreeQueue) dequeue() (any, bool) {
	var v any
	initialIndex := q.index // to check for when we have wrapped all the way around
	for {
		if q.index == localQueueIndex {
			v = q.dequeueLocal()
		} else {
			childQueueName := q.childQueueOrder[q.index]
			if childQueueName != removedQueueName {
				childQueue := q.childQueueMap[childQueueName]

				val, childQueueEmpty := childQueue.dequeue()
				v = val

				if childQueueEmpty {
					q.deleteChildQueue(childQueueName)
					// incrementIndex will take care of resetting the index
				}
			} // else: childQueue for this index was marked removed but not cleaned up yet
		}

		q.incrementIndex()
		if v != nil || q.index == initialIndex {
			return v, q.isEmpty()
		}
	}
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

func (q *TreeQueue) isEmpty() bool {
	return q.localQueue.Len() == 0 && len(q.childQueueMap) == 0
}

func (q *TreeQueue) deleteChildQueue(name string) {
	delete(q.childQueueMap, name)
	q.childQueueOrder[q.index] = removedQueueName
	sliceBoundary := len(q.childQueueOrder)
	for i := len(q.childQueueOrder) - 1; i >= 0 && q.childQueueOrder[i] == removedQueueName; i-- {
		sliceBoundary = i
	}
	// all elements after sliceBoundary are ""; truncate the slice.
	// this does not clean up "" elements in the middle of the slice,
	// but only truncating at the end of the slice is much more performant
	q.childQueueOrder = q.childQueueOrder[:sliceBoundary]
}

// String makes the queue printable
func (q *TreeQueue) String() string {
	bytes, _ := json.MarshalIndent(q, "\t", "\t")
	return fmt.Sprintln(string(bytes))
}
