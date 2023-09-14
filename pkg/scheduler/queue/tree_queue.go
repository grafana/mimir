package queue

import (
	"encoding/json"
	"fmt"
)

type QueuePath []string //nolint:revive
type QueueIndex int

var localQueueIdx int = -1

// TreeQueue is an hierarchical queue implementation where each sub-queue
// has the same guarantees to be chosen from.
// Each queue has also a local queue, which gets chosen with equal preference as the sub-queues.
type TreeQueue struct {
	// name of the queue
	name string
	// local queue
	localQueue []any
	// index of where this item is located in the mapping
	//pos QueueIndex
	//index of the sub-queues
	currentIdx int
	// mapping for sub-queues
	//mapping *Mapping[*TreeQueue]
	childQueueIndices map[string]int
	childQueues       []*TreeQueue

	// maximum queue size of the local queue
}

func NewTreeQueue(name string) *TreeQueue {
	return &TreeQueue{
		name:              name,
		localQueue:        []any{},
		currentIdx:        -1,
		childQueueIndices: map[string]int{},
		childQueues:       []*TreeQueue{},
	}
}

// GetOrCreateChildQueue recursively adds queues based on given path
func (q *TreeQueue) GetOrCreateChildQueue(path QueuePath) *TreeQueue {
	if len(path) == 0 {
		return q
	}
	currentPathSegment, remainingPath := path[0], path[1:]

	if queueIdx, ok := q.childQueueIndices[currentPathSegment]; ok {
		// this level of the tree already exists; recur
		return q.childQueues[queueIdx].GetOrCreateChildQueue(remainingPath)
	}

	// add queue to childQueues
	newChildQueue := NewTreeQueue(currentPathSegment)
	q.childQueues = append(q.childQueues, newChildQueue)

	// add index tracking for the new queue
	q.childQueueIndices[currentPathSegment] = len(q.childQueues) - 1

	if len(remainingPath) > 0 {
		// still further tree depth to create; recur
		return newChildQueue.GetOrCreateChildQueue(remainingPath)
	}

	// recursion complete

	return newChildQueue
}

func (q *TreeQueue) Enqueue(path QueuePath, v any) {
	childQueue := q.GetOrCreateChildQueue(path)
	childQueue.localQueue = append(childQueue.localQueue, v)
}

func (q *TreeQueue) Dequeue() any {
	if (q.currentIdx < localQueueIdx) || (q.currentIdx > len(q.childQueues)-1) {
		// reset current index
		q.currentIdx = localQueueIdx
	}

	var v any
	initialIndex := q.currentIdx // to check for when we have wrapped all the way around

	for {
		if q.currentIdx == localQueueIdx {
			v = q.dequeueLocal()

		} else {
			currentQueue := q.childQueues[q.currentIdx]
			v = currentQueue.Dequeue()
		}

		q.incrementCurrentIndex()

		if v != nil || q.currentIdx == initialIndex {
			return v
		}
	}
}

func (q *TreeQueue) dequeueLocal() any {
	if len(q.localQueue) == 0 {
		return nil
	}
	v := q.localQueue[0]
	q.localQueue = q.localQueue[1:]
	return v
}

func (q *TreeQueue) incrementCurrentIndex() {
	if q.currentIdx+1 == len(q.childQueues) {
		q.currentIdx = localQueueIdx
	} else {
		q.currentIdx++
	}
}

// String makes the queue printable
func (q *TreeQueue) String() string {
	bytes, _ := json.MarshalIndent(q, "\t", "\t")
	return fmt.Sprintln(string(bytes))
}
