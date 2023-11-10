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
	name                   string
	maxQueueLen            int
	localQueue             *list.List
	currentChildQueueIndex int
	childQueueOrder        []string
	childQueueMap          map[string]*TreeQueue
}

func NewTreeQueue(name string, maxQueueLen int) *TreeQueue {
	return &TreeQueue{
		name:                   name,
		maxQueueLen:            maxQueueLen,
		localQueue:             nil,
		currentChildQueueIndex: localQueueIndex,
		childQueueMap:          map[string]*TreeQueue{},
		childQueueOrder:        nil,
	}
}

func (q *TreeQueue) IsEmpty() bool {
	// avoid recursion to make this a cheap operation
	//
	// Because we dereference empty child nodes during dequeuing,
	// we assume that emptiness means there are no child nodes
	// and nothing in this tree node's local queue.
	//
	// In reality a package member could attach empty child queues with getOrAddNode
	// in order to get a functionally-empty tree that would report false for IsEmpty.
	// We assume this does not occur or is not relevant during normal operation.
	return q.LocalQueueLen() == 0 && len(q.childQueueMap) == 0
}

func (q *TreeQueue) NodeCount() int {
	count := 1 // count self
	for _, childQueue := range q.childQueueMap {
		count += childQueue.NodeCount()
	}
	return count
}

func (q *TreeQueue) ItemCount() int {
	count := q.LocalQueueLen() // count self
	for _, childQueue := range q.childQueueMap {
		count += childQueue.ItemCount()
	}
	return count
}

func (q *TreeQueue) LocalQueueLen() int {
	localQueueLen := 0
	if q.localQueue != nil {
		localQueueLen = q.localQueue.Len()
	}
	return localQueueLen
}

// EnqueueBackByPath enqueues an item in the back of the local queue of the node
// located at a given path through the tree; nodes for the path are created as needed.
//
// childPath must be relative to the receiver node; providing a QueuePath beginning with
// the receiver/parent node name will create a child node of the same name as the parent.
func (q *TreeQueue) EnqueueBackByPath(childPath QueuePath, v any) error {
	childQueue, err := q.getOrAddNode(childPath)
	if err != nil {
		return err
	}
	if childQueue.LocalQueueLen()+1 > childQueue.maxQueueLen {
		return ErrTooManyRequests
	}

	if childQueue.localQueue == nil {
		childQueue.localQueue = list.New()
	}
	childQueue.localQueue.PushBack(v)
	return nil
}

// EnqueueFrontByPath enqueues an item in the front of the local queue of the node
// located at a given path through the tree; nodes for the path are created as needed.
//
// Max queue length check is skipped; enqueueing to the front is intended only for items
// which were first enqueued to the back and then dequeued after reaching the front.
//
// Re-enqueueing to the front is intended for cases where a queue consumer fails to
// complete operations on the dequeued item, but failure is not yet final, and the
// operations should be retried by a subsequent queue consumer.
//
// childPath must be relative to the receiver node; providing a QueuePath beginning with
// the receiver/parent node name will create a child node of the same name as the parent.
func (q *TreeQueue) EnqueueFrontByPath(childPath QueuePath, v any) error {
	childQueue, err := q.getOrAddNode(childPath)
	if err != nil {
		return err
	}
	if childQueue.localQueue == nil {
		childQueue.localQueue = list.New()
	}
	childQueue.localQueue.PushFront(v)
	return nil
}

// getOrAddNode recursively adds tree queue nodes based on given relative child path.
//
// childPath must be relative to the receiver node; providing a QueuePath beginning with
// the receiver/parent node name will create a child node of the same name as the parent.
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

		// add new child queue to ordered list for round-robining;
		// in order to maintain round-robin order as nodes are created and deleted,
		// the new child queue should be inserted directly before the current child
		// queue index, essentially placing the new node at the end of the line
		if q.currentChildQueueIndex == localQueueIndex {
			// special case; cannot slice into childQueueOrder with index -1
			// place at end of slice, which is the last slot before the local queue slot
			q.childQueueOrder = append(q.childQueueOrder, childQueue.name)
		} else {
			// insert into order behind current child queue index
			q.childQueueOrder = append(
				q.childQueueOrder[:q.currentChildQueueIndex],
				append(
					[]string{childQueue.name},
					q.childQueueOrder[q.currentChildQueueIndex:]...,
				)...,
			)
			// update current child queue index to its new place in the expanded slice
			q.currentChildQueueIndex++
		}

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

// DequeueByPath selects a child node by a given relative child path and calls Dequeue on the node.
//
// While the child node will recursively clean up its own empty children during dequeue,
// nodes cannot delete themselves; DequeueByPath cleans up the child node as well if it is empty.
// This maintains structural guarantees relied on to make IsEmpty() non-recursive.
//
// childPath is relative to the receiver node; pass a zero-length path to refer to the node itself.
func (q *TreeQueue) DequeueByPath(childPath QueuePath) any {
	childQueue := q.getNode(childPath)
	if childQueue == nil {
		return nil
	}

	v := childQueue.Dequeue()

	if childQueue.IsEmpty() {
		// child node will recursively clean up its own empty children during dequeue,
		// but nodes cannot delete themselves; delete the empty child in order to
		// maintain structural guarantees relied on to make IsEmpty() non-recursive
		q.deleteNode(childPath)
	}

	return v
}

// Dequeue removes and returns an item from the front of the next nonempty queue node in the tree.
//
// Dequeuing from a node follows the round-robin order of the node's childQueueOrder,
// dequeuing either from the node's localQueue or selecting the next child node in the order
// and recursively calling Dequeue on the child nodes until a nonempty queue is found.
//
// Nodes that empty down to the leaf after being dequeued from are deleted as the recursion returns
// up the stack. This maintains structural guarantees relied on to make IsEmpty() non-recursive.
func (q *TreeQueue) Dequeue() any {
	var v any
	initialLen := len(q.childQueueOrder)

	for iters := 0; iters <= initialLen && v == nil; iters++ {
		if q.currentChildQueueIndex == localQueueIndex {
			// dequeuing from local queue; either we have:
			//  1. reached a leaf node, or
			//  2. reached an inner node when it is the local queue's turn
			if q.localQueue != nil {
				if elem := q.localQueue.Front(); elem != nil {
					q.localQueue.Remove(elem)
					v = elem.Value
				}
			}
			q.wrapIndex(true)
		} else {
			// dequeuing from child queue node;
			// pick the child node whose turn it is and recur
			childQueueName := q.childQueueOrder[q.currentChildQueueIndex]
			childQueue := q.childQueueMap[childQueueName]
			v = childQueue.Dequeue()

			// perform cleanup if child node is empty after dequeuing recursively
			if childQueue.IsEmpty() {
				// deleteNode wraps index for us
				q.deleteNode(QueuePath{childQueueName})
			} else {
				q.wrapIndex(true)
			}
		}
	}
	return v
}

// deleteNode removes a child node from the tree and the childQueueOrder and corrects the indices.
func (q *TreeQueue) deleteNode(childPath QueuePath) bool {
	if len(childPath) == 0 {
		// node cannot delete itself
		return false
	}

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
		q.currentChildQueueIndex++
	}
	if q.currentChildQueueIndex >= len(q.childQueueOrder) {
		q.currentChildQueueIndex = localQueueIndex
	}
}
