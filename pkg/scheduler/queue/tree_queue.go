// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"container/list"
)

type QueuePath []string //nolint:revive
type QueueIndex int     //nolint:revive

const localQueueIndex = -1

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
	maxQueueLen     int
	localQueue      *list.List
	index           int
	childQueueOrder []string
	childQueueMap   map[string]*TreeQueue
}

func NewTreeQueue(name string, maxQueueLen int) *TreeQueue {
	return &TreeQueue{
		name:            name,
		maxQueueLen:     maxQueueLen,
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

// EnqueueBackByPath enqueues an item in the back of the local queue of the node
// located at a given path through the tree; nodes for the path are created as needed.
//
// QueuePath must be relative to the receiver node; providing a QueuePath beginning with
// the receiver/parent node name will create a child node of the same name as the parent.
func (q *TreeQueue) EnqueueBackByPath(childPath QueuePath, v any) error {
	childQueue, err := q.getOrAddNode(childPath)
	if err != nil {
		return err
	}
	if childQueue.localQueue.Len()+1 > childQueue.maxQueueLen {
		return ErrTooManyRequests
	}

	childQueue.localQueue.PushBack(v)
	return nil
}

func (q *TreeQueue) EnqueueFrontByPath(childPath QueuePath, v any) error {
	childQueue, err := q.getOrAddNode(childPath)
	if err != nil {
		return err
	}
	childQueue.localQueue.PushFront(v)
	return nil
}

// getOrAddNode recursively adds tree queue nodes based on given path
func (q *TreeQueue) getOrAddNode(childPath QueuePath) (*TreeQueue, error) {
	if len(childPath) == 0 {
		return q, nil
	}

	var childQueue *TreeQueue
	var ok bool

	if childQueue, ok = q.childQueueMap[childPath[0]]; !ok {
		// no child node matches next path segment
		// create next child before recurring
		childQueue = NewTreeQueue(childPath[0], q.maxQueueLen)
		// add new child queue to ordered list for round-robining
		q.childQueueOrder = append(q.childQueueOrder, childQueue.name)
		// attach new child queue to lookup map
		q.childQueueMap[childPath[0]] = childQueue
	}

	return childQueue.getOrAddNode(childPath[1:])

}

func (q *TreeQueue) getNode(childPath QueuePath) *TreeQueue {
	if len(childPath) == 0 {
		return q
	}

	if childQueue, ok := q.childQueueMap[childPath[0]]; ok {
		return childQueue.getNode(childPath[1:])
	}

	// no child node matches next path segment
	return nil
}

func (q *TreeQueue) DequeueByPath(childPath QueuePath) (QueuePath, any) {
	childQueue := q.getNode(childPath)
	if childQueue == nil {
		return nil, nil
	}

	dequeuedPathFromChild, v := childQueue.Dequeue()

	// perform cleanup if child node is empty after dequeuing recursively
	if childQueue.isEmpty() {
		delete(q.childQueueMap, childQueue.name)
		directChildQueueName := dequeuedPathFromChild[0]
		for i, name := range q.childQueueOrder {
			if name == directChildQueueName {
				q.childQueueOrder = append(q.childQueueOrder[:i], q.childQueueOrder[i+1:]...)
				q.wrapIndex(false)
			}
		}

	}

	if v == nil {
		// guard against slicing into nil path
		return nil, nil
	}
	return append(childPath, dequeuedPathFromChild[1:]...), v
}

func (q *TreeQueue) Dequeue() (QueuePath, any) {
	var dequeuedPath QueuePath
	var v any
	initialLen := len(q.childQueueOrder)

	for iters := 0; iters <= initialLen && v == nil; iters++ {
		incrementQueueIndex := true

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
				incrementQueueIndex = false
			}
		}
		q.wrapIndex(incrementQueueIndex)
	}
	if v == nil {
		// don't report path when nothing was dequeued
		return nil, nil
	}
	return append(QueuePath{q.name}, dequeuedPath...), v
}

func (q *TreeQueue) deleteNode(childPath QueuePath) bool {
	parentPath, childQueueName := childPath[:len(childPath)-1], childPath[len(childPath)-1]

	parentNode := q.getNode(parentPath)
	if parentNode == nil {
		// not found
		return false
	}

	delete(parentNode.childQueueMap, childQueueName)
	for i, name := range parentNode.childQueueOrder {
		if name == childQueueName {
			parentNode.childQueueOrder = append(q.childQueueOrder[:i], q.childQueueOrder[i+1:]...)
			parentNode.wrapIndex(false)
			break
		}
	}
	return true
}

func (q *TreeQueue) wrapIndex(increment bool) {
	if increment {
		q.index++
	}
	if q.index >= len(q.childQueueOrder) {
		q.index = localQueueIndex
	}
}
