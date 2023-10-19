package queue

import (
	"container/list"

	"github.com/pkg/errors"
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

func (q *TreeQueue) isEmpty() bool {
	// avoid recursion to make this a cheap operation
	//
	// Because we dereference empty child nodes during dequeuing,
	// we assume that emptiness means there are no child nodes
	// and nothing in this tree node's local queue.
	//
	// In reality a package member could attach empty child queues with getOrAddNode
	// in order to get a functionally-empty tree that would report false for isEmpty.
	// We assume this does not occur or is not relevant during normal operation.
	return q.localQueue.Len() == 0 && len(q.childQueueMap) == 0
}

func (q *TreeQueue) EnqueueBackByPath(path QueuePath, v any) error {
	if path[0] != q.name {
		return errors.New("path must begin with root node name")
	}
	childQueue, err := q.getOrAddNode(path)
	if err != nil {
		return err
	}
	childQueue.localQueue.PushBack(v)
	return nil
}

// getOrAddNode recursively adds queues based on given path
func (q *TreeQueue) getOrAddNode(path QueuePath) (*TreeQueue, error) {
	if len(path) == 0 {
		// non-nil tree must have at least the path equal to the root name
		// not a recursion case; only occurs if empty path provided to root node
		return nil, nil
	}

	if path[0] != q.name {
		return nil, errors.New("path must begin with this node name")
	}

	childPath := path[1:]
	if len(childPath) == 0 {
		// no path left to create; we have arrived
		return q, nil
	}

	var childQueue *TreeQueue
	var ok bool

	if childQueue, ok = q.childQueueMap[childPath[0]]; !ok {
		// no child node matches next path segment
		// create next child before recurring
		childQueue = NewTreeQueue(childPath[0])
		// add new child queue to ordered list for round-robining
		q.childQueueOrder = append(q.childQueueOrder, childQueue.name)
		// attach new child queue to lookup map
		q.childQueueMap[childPath[0]] = childQueue
	}

	return childQueue.getOrAddNode(childPath)

}

func (q *TreeQueue) getNode(path QueuePath) *TreeQueue {
	if len(path) == 0 {
		// non-nil tree must have at least the path equal to the root name
		// not a recursion case; only occurs if empty path provided to root node
		return nil
	}

	childPath := path[1:]
	if len(childPath) == 0 {
		// no path left to search for; we have arrived
		return q
	}

	if childQueue, ok := q.childQueueMap[childPath[0]]; ok {
		return childQueue.getNode(childPath)
	} else {
		// no child node matches next path segment
		return nil
	}
}

func (q *TreeQueue) DequeueByPath(path QueuePath) (QueuePath, any) {
	childQueue := q.getNode(path)
	if childQueue == nil {
		return nil, nil
	}

	dequeuedPathFromChild, v := childQueue.Dequeue()

	if v == nil {
		// guard against slicing into nil path
		return nil, nil
	}
	return append(path, dequeuedPathFromChild[1:]...), v
}

func (q *TreeQueue) Dequeue() (QueuePath, any) {
	var dequeuedPath QueuePath
	var v any
	initialLen := len(q.childQueueOrder)

	for iters := 0; iters <= initialLen && v == nil; iters++ {
		increment := true

		if q.index == localQueueIndex {
			// dequeuing from local queue; either we have:
			//  1. reached a leaf node, or
			//  2. reached an inner node when it is the local queue's turn
			if elem := q.localQueue.Front(); elem != nil {
				q.localQueue.Remove(elem)
				v = elem.Value
			}
		} else {
			// dequeuing from child queue node;
			// pick the child node whose turn it is and recur
			childQueueName := q.childQueueOrder[q.index]
			childQueue := q.childQueueMap[childQueueName]
			dequeuedPath, v = childQueue.Dequeue()

			// perform cleanup if child node is empty after dequeuing recursively
			if childQueue.isEmpty() {
				delete(q.childQueueMap, childQueueName)
				q.childQueueOrder = append(q.childQueueOrder[:q.index], q.childQueueOrder[q.index+1:]...)
				// no need to increment; remainder of the slice has moved left to be under q.index
				increment = false
			}
		}
		q.wrapIndex(increment)
	}
	if v == nil {
		// don't report path when nothing was dequeued
		return nil, nil
	}
	return append(QueuePath{q.name}, dequeuedPath...), v
}

func (q *TreeQueue) wrapIndex(increment bool) {
	if increment {
		q.index++
	}
	if q.index >= len(q.childQueueOrder) {
		q.index = localQueueIndex
	}
}
