package queue

import (
	"container/list"
	"fmt"
)

// TODO (casie): We might not need this interface anymore.
type TreeNodeIFace interface {
	//EnqueueFrontByPath(QueuePath, any) error
	enqueueBackByPath(*Tree, QueuePath, any) error
	dequeue() (QueuePath, any) // Dequeue returns the dequeued value, and the QueuePath (starting with root) to the node which was dequeued from
	IsEmpty() bool
	Name() string
	getLocalQueue() *list.List
	getOrAddNode(QueuePath, *Tree) (*Node, error)
}

type Tree struct {
	rootNode     *Node
	algosByDepth []DequeueAlgorithm
}

// Nodes with *shuffleShardState dequeueAlgorithms disregard individual
// Nodes' queueOrder and queuePosition
// TODO (casie): Implement maxQueueSize to mirror qb.maxTenantQueueSize?
type Node struct {
	name             string
	localQueue       *list.List
	queuePosition    int      // last queue position in queueOrder that was dequeued from
	queueOrder       []string // order for dequeueing from self/children
	queueMap         map[string]*Node
	depth            int
	dequeueAlgorithm DequeueAlgorithm
	childrenChecked  int
}

func NewTree(dequeueAlgorithms ...DequeueAlgorithm) *Tree {
	if len(dequeueAlgorithms) == 0 {
		return nil
	}
	root, err := NewNode("root", 0, dequeueAlgorithms[0])
	if err != nil {
		// TODO (casie): Make NewNode private, remove err return
		fmt.Print(err)
	}
	root.depth = 0
	return &Tree{
		rootNode:     root,
		algosByDepth: dequeueAlgorithms,
	}
}

func NewNode(name string, depth int, da DequeueAlgorithm) (*Node, error) {
	// TODO (casie): write a unit test
	if da == nil {
		return nil, fmt.Errorf("cannot create a node without a defined dequeueing algorithm")
	}
	switch da.(type) {
	case *shuffleShardState:
		sss := da.(*shuffleShardState)
		sss.sharedQueuePosition = localQueueIndex - 1
		if sss.tenantNodes == nil {
			sss.tenantNodes = map[string][]*Node{}
		}
		if sss.tenantQuerierMap == nil {
			sss.tenantQuerierMap = map[TenantID]map[QuerierID]struct{}{}
		}
	}
	return &Node{
		name:          name,
		localQueue:    list.New(),
		queuePosition: localQueueIndex - 1, // start from -2 so that on first dequeue, localQueueIndex is dequeued from
		//queueOrder:       make([]string, 0),
		queueMap:         make(map[string]*Node, 1),
		depth:            depth,
		dequeueAlgorithm: da,
	}, nil
}

func (n *Node) IsEmpty() bool {
	// node has nothing in local queue, and no child nodes
	return n.localQueue.Len() == 0 && len(n.queueMap) == 0
}

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

func (n *Node) enqueueFrontByPath(tree *Tree, pathFromNode QueuePath, v any) error {
	childNode, err := n.getOrAddNode(pathFromNode, tree)
	if err != nil {
		return err
	}
	childNode.getLocalQueue().PushFront(v)
	return nil
}

func (n *Node) enqueueBackByPath(tree *Tree, pathFromNode QueuePath, v any) error {
	childNode, err := n.getOrAddNode(pathFromNode, tree)
	if err != nil {
		return err
	}
	// TODO (casie): Create localQueue on node creation; why not?
	childNode.getLocalQueue().PushBack(v)
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
		dequeueNode, checkedAllNodes = n.dequeueAlgorithm.dequeueOps().getNode(n)
		var deletedNode bool
		// dequeueing from local queue
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
			// if the dequeue node is empty _after_ dequeueing, delete it from children
			if dequeueNode.IsEmpty() {
				// removing an element sets our position one step forward;
				// tell state to reset it to original queuePosition
				delete(n.queueMap, dequeueNode.Name())
				deletedNode = n.dequeueAlgorithm.deleteChildNode(n, dequeueNode)
			}
		}
		n.dequeueAlgorithm.dequeueOps().updateState(n, v, deletedNode)
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

// getOrAddNode checks whether the first node name in pathFromNode exists in a node's children;
// if no node exists, one is created and added to the "end" of the node's children, from the perspective
// of node.queuePosition (e.g., if node.queuePosition is 4, the new node will be created at position 3).
// This follows queue behavior, but if, for some reason, we ever want to create nodes in a way that violates
// queueing behavior, this will require a refactor, as will, well, the entire "tree queue" data structure.
func (n *Node) getOrAddNode(pathFromNode QueuePath, tree *Tree) (*Node, error) {
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
		childNode, err = NewNode(pathFromNode[0], n.depth+1, tree.algosByDepth[n.depth+1])
		if err != nil {
			return nil, err
		}

		n.dequeueAlgorithm.addChildNode(n, childNode)

	}
	// TODO (casie): How to determine what dequeueAlgo to pass?
	return childNode.getOrAddNode(pathFromNode[1:], tree)
}
