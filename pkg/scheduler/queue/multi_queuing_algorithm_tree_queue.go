// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"container/list"
	"fmt"
)

type QueuePath []string //nolint:revive // disallows types beginning with package name
type QueueIndex int     //nolint:revive // disallows types beginning with package name

const localQueueIndex = -1

type Tree interface {
	EnqueueFrontByPath(QueuePath, any) error
	EnqueueBackByPath(QueuePath, any) error
	Dequeue() (QueuePath, any)
	ItemCount() int
	IsEmpty() bool
}

// MultiQueuingAlgorithmTreeQueue holds metadata and a pointer to the root node of a hierarchical queue implementation.
// The root Node maintains a localQueue and an arbitrary number of child nodes (which themselves
// may have local queues and children). Each Node in MultiQueuingAlgorithmTreeQueue uses a QueuingAlgorithm (determined by
// node depth) to determine dequeue order of that Node's subtree.
//
// Each queuing dimension is modeled as a node in the tree, internally reachable through a QueuePath.
//
// The QueuePath is an ordered array of strings describing the path from the tree root to a Node.
// In addition to child Nodes, each Node contains a local queue (FIFO) of items.
//
// When dequeuing from a given node, a Node will use its QueuingAlgorithm to choose either itself
// or a child node to dequeue from recursively (i.e., a child Node will use its own QueuingAlgorithm
// to determine how to proceed). MultiQueuingAlgorithmTreeQueue will not dequeue from two different Nodes at the same depth
// consecutively, unless the previously-checked Node was empty down to the leaf node.
type MultiQueuingAlgorithmTreeQueue struct {
	rootNode     *Node
	algosByDepth []QueuingAlgorithm
}

func NewTree(queuingAlgorithms ...QueuingAlgorithm) (*MultiQueuingAlgorithmTreeQueue, error) {
	if len(queuingAlgorithms) == 0 {
		return nil, fmt.Errorf("cannot create a tree without defined QueuingAlgorithm")
	}
	root, err := newNode("root", len(queuingAlgorithms)-1, queuingAlgorithms[0])
	if err != nil {
		return nil, err
	}
	return &MultiQueuingAlgorithmTreeQueue{
		rootNode:     root,
		algosByDepth: queuingAlgorithms,
	}, nil
}

func (t *MultiQueuingAlgorithmTreeQueue) ItemCount() int {
	return t.rootNode.ItemCount()
}

func (t *MultiQueuingAlgorithmTreeQueue) IsEmpty() bool {
	return t.rootNode.IsEmpty()
}

// Dequeue removes and returns an item from the front of the next appropriate Node in the MultiQueuingAlgorithmTreeQueue, as
// well as the path to the Node which that item was dequeued from.
//
// Either the root/self node or a child node is chosen according to the Node's QueuingAlgorithm. If
// the root node is chosen, an item will be dequeued from the front of its localQueue. If a child
// node is chosen, it is recursively dequeued from until a node selects its localQueue.
//
// Nodes that empty down to the leaf after being dequeued from (or which are found to be empty leaf
// nodes during the dequeue operation) are deleted as the recursion returns up the stack. This
// maintains structural guarantees relied upon to make IsEmpty() non-recursive.
func (t *MultiQueuingAlgorithmTreeQueue) Dequeue() (QueuePath, any) {
	path, v := t.rootNode.dequeue()
	// The returned node dequeue path includes the root node; exclude
	// this so that the return path can be used if needed to enqueue.
	return path[1:], v
}

// EnqueueBackByPath enqueues an item in the back of the local queue of the node
// located at a given path through the tree; nodes for the path are created as needed.
//
// path is relative to the root node; providing a QueuePath beginning with "root"
// will create a child node of the root node which is also named "root."
func (t *MultiQueuingAlgorithmTreeQueue) EnqueueBackByPath(path QueuePath, v any) error {
	if len(path) != len(t.algosByDepth)-1 {
		return fmt.Errorf("can't enqueue object to non-leaf node")
	}
	return t.rootNode.enqueueBackByPath(t, path, v)
}

// EnqueueFrontByPath enqueues an item in the front of the local queue of the Node
// located at a given path through the MultiQueuingAlgorithmTreeQueue; nodes for the path are created as needed.
//
// Enqueueing to the front is intended only for items which were first enqueued to the back
// and then dequeued after reaching the front.
//
// Re-enqueueing to the front is only intended for use in cases where a queue consumer
// fails to complete operations on the dequeued item, but failure is not yet final, and the
// operations should be retried by a subsequent queue consumer. A concrete example is when
// a queue consumer fails or disconnects for unrelated reasons while we are in the process
// of dequeuing a request for it.
//
// path must be relative to the root node; providing a QueuePath beginning with "root"
// will create a child node of root which is also named "root."
func (t *MultiQueuingAlgorithmTreeQueue) EnqueueFrontByPath(path QueuePath, v any) error {
	return t.rootNode.enqueueFrontByPath(t, path, v)
}

func (t *MultiQueuingAlgorithmTreeQueue) GetNode(path QueuePath) *Node {
	return t.rootNode.getNode(path)
}

// Node maintains node-specific information used to enqueue and dequeue to itself, such as a local
// queue, node height, references to its children, and position in queue.
// Note that the tenantQuerierAssignments QueuingAlgorithm largely disregards Node's queueOrder and
// queuePosition, managing analogous state instead, because shuffle-sharding + fairness  requirements
// necessitate input from the querier.
type Node struct {
	name             string
	localQueue       *list.List
	queuePosition    int      // next index in queueOrder to dequeue from
	queueOrder       []string // order for dequeuing from self/children
	queueMap         map[string]*Node
	height           int
	queuingAlgorithm QueuingAlgorithm
	childrenChecked  int
}

func newNode(name string, height int, da QueuingAlgorithm) (*Node, error) {
	if height < 0 {
		return nil, fmt.Errorf("cannot create a node at negative height")
	}

	if da == nil {
		return nil, fmt.Errorf("cannot create a node without a defined QueuingAlgorithm")
	}
	n := &Node{
		name:             name,
		localQueue:       nil,
		queuePosition:    localQueueIndex,
		height:           height,
		queuingAlgorithm: da,
	}
	// if the node is a leaf node, it gets memory allocated towards a local queue and cannot have child queues
	if height == 0 {
		n.localQueue = list.New()
	} else {
		// any other kind of node is allowed to have children, but not a local queue
		n.queueMap = make(map[string]*Node, 1)
		n.queueOrder = make([]string, 0)
	}
	return n, nil
}

func (n *Node) IsEmpty() bool {
	// avoid recursion to make this a cheap operation
	//
	// Because we dereference empty child nodes during dequeuing,
	// we assume that emptiness means there are no child nodes
	// and nothing in this node's local queue.
	//
	// We also assume that leaf nodes have no child nodes, since leaf nodes sit at
	// the tree's max-height, and that non-leaf nodes have no local queues
	//
	// In reality a package member could attach empty child queues with getOrAddNode
	// in order to get a functionally-empty tree that would report false for IsEmpty.
	// We assume this does not occur or is not relevant during normal operation.
	if n.isLeaf() {
		return n.localQueue.Len() == 0
	}
	return len(n.queueMap) == 0
}

func (n *Node) isLeaf() bool {
	return n.height == 0
}

// ItemCount counts the queue items in the Node and in all its children, recursively.
func (n *Node) ItemCount() int {
	if n.isLeaf() {
		return n.localQueue.Len()
	}
	items := 0
	for _, child := range n.queueMap {
		items += child.ItemCount()
	}
	return items
}

func (n *Node) Name() string {
	return n.name
}

func (n *Node) getLocalQueue() *list.List {
	return n.localQueue
}

func (n *Node) enqueueFrontByPath(tree *MultiQueuingAlgorithmTreeQueue, pathFromNode QueuePath, v any) error {
	childNode, err := n.getOrAddNode(pathFromNode, tree)
	if err != nil {
		return err
	}
	childNode.localQueue.PushFront(v)
	return nil
}

func (n *Node) enqueueBackByPath(tree *MultiQueuingAlgorithmTreeQueue, pathFromNode QueuePath, v any) error {
	childNode, err := n.getOrAddNode(pathFromNode, tree)
	if err != nil {
		return err
	}
	childNode.localQueue.PushBack(v)
	return nil
}

func (n *Node) dequeue() (QueuePath, any) {
	var v any
	var childPath QueuePath

	path := QueuePath{n.name}

	var checkedAllNodes bool
	var dequeueNode *Node
	// continue until we've found a value or checked all nodes that need checking
	for v == nil && !checkedAllNodes {
		if n.IsEmpty() {
			// we can't dequeue a value from an empty node; return early
			return path, nil
		}
		if n.isLeaf() {
			checkedAllNodes = n.childrenChecked == 1
		} else {
			checkedAllNodes = n.childrenChecked == len(n.queueMap)
		}

		dequeueNode = n.queuingAlgorithm.dequeueSelectNode(n)
		switch dequeueNode {
		case n:
			if n.isLeaf() {
				// dequeue node is self, and is a leaf node; dequeue from local queue
				if n.localQueue.Len() > 0 {
					if elt := n.localQueue.Front(); elt != nil {
						n.localQueue.Remove(elt)
						v = elt.Value
					}
				}
			}
		// no dequeue-able child found; break out of the loop,
		// since we won't find anything to dequeue if we don't
		// have a node to dequeue from now
		case nil:
			checkedAllNodes = true
		// dequeue from a child
		default:
			childPath, v = dequeueNode.dequeue()
		}

		if v == nil {
			n.childrenChecked++
		}

		n.queuingAlgorithm.dequeueUpdateState(n, dequeueNode)
	}
	// reset childrenChecked to 0 before completing this dequeue
	n.childrenChecked = 0
	return append(path, childPath...), v
}

func (n *Node) getNode(pathFromNode QueuePath) *Node {
	if len(pathFromNode) == 0 {
		return n
	}

	if n.queueMap == nil {
		return nil
	}

	if childQueue, ok := n.queueMap[pathFromNode[0]]; ok {
		return childQueue.getNode(pathFromNode[1:])
	}

	// no child node matches next path segment
	return nil
}

// getOrAddNode recursively gets or adds tree queue nodes based on given relative child path. It
// checks whether the first node in pathFromNode exists in the Node's children; if no node exists,
// one is created and added to the Node's queueOrder, according to the Node's QueuingAlgorithm.
//
// pathFromNode must be relative to the receiver node; providing a QueuePath beginning with
// the receiver/parent node name will create a child node of the same name as the parent.
func (n *Node) getOrAddNode(pathFromNode QueuePath, tree *MultiQueuingAlgorithmTreeQueue) (*Node, error) {
	if len(pathFromNode) == 0 {
		return n, nil
	}

	var childNode *Node
	var ok bool
	var err error
	if childNode, ok = n.queueMap[pathFromNode[0]]; !ok {
		// child does not exist, create it
		childHeight := n.height - 1
		// calculate child depth to properly assign queuing algorithm
		childDepth := len(tree.algosByDepth) - childHeight - 1
		childNode, err = newNode(pathFromNode[0], childHeight, tree.algosByDepth[childDepth])
		if err != nil {
			return nil, err
		}
		// add the newly created child to the node
		n.queuingAlgorithm.addChildNode(n, childNode)

	}
	return childNode.getOrAddNode(pathFromNode[1:], tree)
}
