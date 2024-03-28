// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"container/list"
)

type QueuePath []string //nolint:revive // disallows types beginning with package name
type QueueIndex int     //nolint:revive // disallows types beginning with package name

const localQueueIndex = -1

// TreeQueue is a hierarchical queue implementation with an arbitrary amount of child queues.
//
// TreeQueue uses a queueingAlgorithm to handle queuing across all of its queue dimensions.
// Each queuing dimension is modeled as a node in the tree, internally reachable through a QueuePath.
//
// The QueuePath is an ordered array of strings describing the path through the tree to the node,
// which contains the FIFO local queue of all items enqueued for that queuing dimension.
//
// When dequeuing from a given node, the node will use its queueingAlgorithm to determine whether it should dequeue
// from its own local queue or from its list of child TreeQueues. Each node is defined with its own queueingAlgorithm,
// so dequeuing from a child TreeQueue may not follow the same logic as dequeuing from the parent.
// No queue at a given level of the tree is dequeued from consecutively unless all others
// at the same level of the tree are empty down to the leaf node.
type TreeQueue struct {
	// name of the tree node will be set to its segment of the queue path
	name                   string
	localQueue             *list.List
	currentChildQueueIndex int
	childQueueOrder        []string
	childQueueMap          map[string]*TreeQueue
	queueingAlgorithm      queueingAlgorithm
}

func NewTreeQueue(name string, algorithm queueingAlgorithm) *TreeQueue {
	return &TreeQueue{
		name:                   name,
		localQueue:             nil,
		currentChildQueueIndex: localQueueIndex,
		childQueueMap:          nil,
		childQueueOrder:        nil,
		queueingAlgorithm:      algorithm,
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

// NodeCount counts the TreeQueue node and all its children, recursively.
func (q *TreeQueue) NodeCount() int {
	count := 1 // count self
	for _, childQueue := range q.childQueueMap {
		count += childQueue.NodeCount()
	}
	return count
}

// ItemCount counts the queue items in the TreeQueue node and in all its children, recursively.
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

	if q.childQueueMap == nil {
		q.childQueueMap = make(map[string]*TreeQueue, 1)
	}

	var childQueue *TreeQueue
	var ok bool
	if childQueue, ok = q.childQueueMap[childPath[0]]; !ok {
		// no child node matches next path segment
		// create next child before recurring
		// TODO (casie): Need to add support for passing queueingAlgorithms for each child in childPath.
		childQueue = NewTreeQueue(childPath[0], roundRobin)

		addNode(q.queueingAlgorithm)(q, childQueue)

		// attach new child queue to lookup map
		q.childQueueMap[childPath[0]] = childQueue
	}

	return childQueue.getOrAddNode(childPath[1:])
}

func (q *TreeQueue) getNode(childPath QueuePath) *TreeQueue {
	if len(childPath) == 0 {
		return q
	}

	if q.childQueueMap == nil {
		return nil
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
// # Dequeuing from a node follows a dequeueAlgorithm, which either dequeues from the node's own queue,
// or selects a child node to dequeue from, according to that child node's own dequeueAlgorithm, until
// a non-empty queue is found.
//
// In practice, the nodes at the same depth in the query scheduler tree queue will execute the same
// dequeuing algorithm, since query components will be at depth=1, and tenant queues will be at depth=2.
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
			setNextIndex(q.queueingAlgorithm)(q)
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
				setNextIndex(q.queueingAlgorithm)(q)
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
			parentNode.wrapIndex()
			break
		}
	}
	return true
}

func (q *TreeQueue) wrapIndex() {
	if q.currentChildQueueIndex >= len(q.childQueueOrder) {
		q.currentChildQueueIndex = localQueueIndex
	}
}
