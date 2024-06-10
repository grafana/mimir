// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"container/list"
	"fmt"
)

type QueuePath []string //nolint:revive // disallows types beginning with package name
type QueueIndex int     //nolint:revive // disallows types beginning with package name

const localQueueIndex = -1

// TreeQueue holds metadata and a pointer to the root node of a hierarchical queue implementation.
// The root Node maintains a localQueue and an arbitrary number of child nodes (which themselves
// may have local queues and children). Each Node in TreeQueue uses a DequeueAlgorithm (determined by
// node depth) to determine dequeue order of that Node's subtree.
//
// Each queuing dimension is modeled as a node in the tree, internally reachable through a QueuePath.
//
// The QueuePath is an ordered array of strings describing the path from the tree root to a Node.
// In addition to child Nodes, each Node contains a local queue (FIFO) of items.
//
// When dequeuing from a given node, a Node will use its DequeueAlgorithm to choose either itself
// or a child node to dequeue from recursively (i.e., a child Node will use its own DequeueAlgorithm
// to determine how to proceed). TreeQueue will not dequeue from two different Nodes at the same depth
// consecutively, unless the previously-checked Node was empty down to the leaf node.
type TreeQueue struct {
	rootNode     *Node
	algosByDepth []DequeueAlgorithm
}

func NewTree(dequeueAlgorithms ...DequeueAlgorithm) (*TreeQueue, error) {
	if len(dequeueAlgorithms) == 0 {
		return nil, fmt.Errorf("cannot create a tree without defined DequeueAlgorithm")
	}
	root, err := newNode("root", 0, dequeueAlgorithms[0])
	if err != nil {
		return nil, err
	}
	root.depth = 0
	return &TreeQueue{
		rootNode:     root,
		algosByDepth: dequeueAlgorithms,
	}, nil
}

func (t *TreeQueue) IsEmpty() bool {
	return t.rootNode.IsEmpty()
}

// Dequeue removes and returns an item from the front of the next appropriate Node in the TreeQueue, as
// well as the path to the Node which that item was dequeued from.
//
// Either the root/self node or a child node is chosen according to the Node's DequeueAlgorithm. If
// the root node is chosen, an item will be dequeued from the front of its localQueue. If a child
// node is chosen, it is recursively dequeued from until a node selects its localQueue.
//
// Nodes that empty down to the leaf after being dequeued from (or which are found to be empty leaf
// nodes during the dequeue operation) are deleted as the recursion returns up the stack. This
// maintains structural guarantees relied upon to make IsEmpty() non-recursive.
func (t *TreeQueue) Dequeue() (QueuePath, any) {
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
func (t *TreeQueue) EnqueueBackByPath(path QueuePath, v any) error {
	return t.rootNode.enqueueBackByPath(t, path, v)
}

// EnqueueFrontByPath enqueues an item in the front of the local queue of the Node
// located at a given path through the TreeQueue; nodes for the path are created as needed.
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
func (t *TreeQueue) EnqueueFrontByPath(path QueuePath, v any) error {
	return t.rootNode.enqueueFrontByPath(t, path, v)
}

func (t *TreeQueue) GetNode(path QueuePath) *Node {
	return t.rootNode.getNode(path)
}

// Node maintains node-specific information used to enqueue and dequeue to itself, such as a local
// queue, node depth, references to its children, and position in queue.
// Note that the tenantQuerierAssignments DequeueAlgorithm largely disregards Node's queueOrder and
// queuePosition, managing analogous state instead, because shuffle-sharding + fairness  requirements
// necessitate input from the querier.
type Node struct {
	name             string
	localQueue       *list.List
	queuePosition    int      // next index in queueOrder to dequeue from
	queueOrder       []string // order for dequeuing from self/children
	queueMap         map[string]*Node
	depth            int
	dequeueAlgorithm DequeueAlgorithm
	childrenChecked  int
}

func newNode(name string, depth int, da DequeueAlgorithm) (*Node, error) {
	if da == nil {
		return nil, fmt.Errorf("cannot create a node without a defined DequeueAlgorithm")
	}
	switch da.(type) {
	case *tenantQuerierAssignments:
		tqa := da.(*tenantQuerierAssignments)
		tqa.tenantOrderIndex = localQueueIndex - 1 // start from -2 so that we first check local queue
		if tqa.tenantNodes == nil {
			tqa.tenantNodes = map[string][]*Node{}
		}
	}
	return &Node{
		name:             name,
		localQueue:       list.New(),
		queuePosition:    localQueueIndex,
		queueOrder:       make([]string, 0),
		queueMap:         make(map[string]*Node, 1),
		depth:            depth,
		dequeueAlgorithm: da,
	}, nil
}

func (n *Node) IsEmpty() bool {
	// avoid recursion to make this a cheap operation
	//
	// Because we dereference empty child nodes during dequeuing,
	// we assume that emptiness means there are no child nodes
	// and nothing in this tree node's local queue.
	//
	// In reality a package member could attach empty child queues with getOrAddNode
	// in order to get a functionally-empty tree that would report false for IsEmpty.
	// We assume this does not occur or is not relevant during normal operation.
	return n.localQueue.Len() == 0 && len(n.queueMap) == 0
}

// ItemCount counts the queue items in the Node and in all its children, recursively.
func (n *Node) ItemCount() int {
	items := n.localQueue.Len()
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

func (n *Node) enqueueFrontByPath(tree *TreeQueue, pathFromNode QueuePath, v any) error {
	childNode, err := n.getOrAddNode(pathFromNode, tree)
	if err != nil {
		return err
	}
	childNode.localQueue.PushFront(v)
	return nil
}

func (n *Node) enqueueBackByPath(tree *TreeQueue, pathFromNode QueuePath, v any) error {
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

	if n.IsEmpty() {
		return path, nil
	}

	var checkedAllNodes bool
	var dequeueNode *Node
	// continue until we've found a value or checked all nodes that need checking
	for v == nil && !checkedAllNodes {
		dequeueNode, checkedAllNodes = n.dequeueAlgorithm.dequeueGetNode(n)
		var deletedNode bool
		// dequeuing from local queue
		if dequeueNode == n {
			if n.localQueue.Len() > 0 {
				// dequeueNode is self, local queue non-empty
				if elt := n.localQueue.Front(); elt != nil {
					n.localQueue.Remove(elt)
					v = elt.Value
				}
			}
		} else if dequeueNode == nil {
			// no dequeue-able child found; reset checked children to 0,
			// as we won't update state before moving on
			n.childrenChecked = 0
			return path, v
		} else {
			// dequeue from a child
			childPath, v = dequeueNode.dequeue()
			// if the dequeue node is empty _after_ dequeuing, delete it from children
			if dequeueNode.IsEmpty() {
				// removing an element sets our position one step forward;
				// tell state to reset it to original queuePosition
				delete(n.queueMap, dequeueNode.Name())
				deletedNode = n.dequeueAlgorithm.deleteChildNode(n, dequeueNode)
			}
		}
		n.dequeueAlgorithm.dequeueUpdateState(n, v, deletedNode)
	}
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
// one is created and added to the Node's queueOrder, according to the Node's DequeueAlgorithm.
//
// pathFromNode must be relative to the receiver node; providing a QueuePath beginning with
// the receiver/parent node name will create a child node of the same name as the parent.
func (n *Node) getOrAddNode(pathFromNode QueuePath, tree *TreeQueue) (*Node, error) {
	if len(pathFromNode) == 0 {
		return n, nil
	}

	var childNode *Node
	var ok bool
	var err error
	if childNode, ok = n.queueMap[pathFromNode[0]]; !ok {
		// child does not exist, create it
		if n.depth+1 >= len(tree.algosByDepth) {
			return nil, fmt.Errorf("cannot add a node beyond max tree depth: %v", len(tree.algosByDepth))
		}
		childNode, err = newNode(pathFromNode[0], n.depth+1, tree.algosByDepth[n.depth+1])
		if err != nil {
			return nil, err
		}
		// add the newly created child to the node
		n.dequeueAlgorithm.addChildNode(n, childNode)

	}
	return childNode.getOrAddNode(pathFromNode[1:], tree)
}
