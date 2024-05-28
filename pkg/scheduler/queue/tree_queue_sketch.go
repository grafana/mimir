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

type DequeueAlgorithm interface {
	dequeueOps() dequeueOps
}

type OpsPath []struct {
	name             string
	dequeueAlgorithm DequeueAlgorithm
}

type dequeueGetNodeFunc func(*Node) (TreeNodeIFace, bool)
type dequeueUpdateStateFunc func(*Node, any, string, bool)

type dequeueOps struct {
	getNode     dequeueGetNodeFunc
	updateState dequeueUpdateStateFunc
}

type Tree struct {
	rootNode     *Node
	algosByDepth []DequeueAlgorithm
}

// TODO (casie): Implement maxQueueSize to mirror qb.maxTenantQueueSize?
type Node struct {
	name             string
	localQueue       *list.List
	queuePosition    int      // last queue position in queueOrder that was dequeued from
	queueOrder       []string // order for dequeueing from self/children
	queueMap         map[string]*Node
	depth            int
	dequeueAlgorithm DequeueAlgorithm
}

// TODO (casie): Instead of passing in actual state objects, just pass a string that maps to state
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
		if da.(*shuffleShardState).tenantQuerierMap == nil {
			return nil, fmt.Errorf("cannot create a shufffle-shard node with nil tenant-querier map")
		}
	}
	return &Node{
		name:             name,
		localQueue:       list.New(),
		queuePosition:    localQueueIndex - 1, // start from -2 so that on first dequeue, localQueueIndex is dequeued from
		queueOrder:       make([]string, 0),
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
	var dequeueNode TreeNodeIFace
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
			// no dequeue-able node found
			return path, v
		} else {
			// dequeue from a child
			childPath, v = dequeueNode.dequeue()
			// if the dequeue node is empty _after_ dequeueing, delete it from children
			if dequeueNode.IsEmpty() {
				deleteNodeName := dequeueNode.Name()
				delete(n.queueMap, deleteNodeName)
				for idx, name := range n.queueOrder {
					if name == deleteNodeName {
						n.queueOrder = append(n.queueOrder[:idx], n.queueOrder[idx+1:]...)
					}
				}
				// removing an element sets our position one step forward;
				// tell state to reset it to original queuePosition
				deletedNode = true
			}
		}
		n.dequeueAlgorithm.dequeueOps().updateState(n, v, dequeueNode.Name(), deletedNode)
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

func (n *Node) deleteNode(pathFromNode QueuePath) bool {
	if len(pathFromNode) == 0 {
		// node cannot delete itself
		return false
	}

	// parentPath is everything except the last node, childNode is the last in path
	parentPath, deleteNodeName := pathFromNode[:len(pathFromNode)-1], pathFromNode[len(pathFromNode)-1]

	parentNode := n.getNode(parentPath)
	if parentNode == nil {
		// not found
		return false
	}

	delete(parentNode.queueMap, deleteNodeName)
	for i, name := range parentNode.queueOrder {
		if name == deleteNodeName {
			parentNode.queueOrder = append(n.queueOrder[:i], n.queueOrder[i+1:]...)
			if parentNode.queuePosition >= len(parentNode.queueOrder) {
				// set _last_ dequeue position to last elt in order
				parentNode.queuePosition = len(parentNode.queueOrder) - 1
			}
			break
		}
	}
	return true
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

		// add childNode to n.queueMap
		n.queueMap[childNode.Name()] = childNode

		// add childNode to n.queueOrder before the current position, update n.queuePosition to current element
		if n.queuePosition <= localQueueIndex {
			n.queueOrder = append(n.queueOrder, childNode.Name())
		} else {
			n.queueOrder = append(n.queueOrder[:n.queuePosition], append([]string{childNode.Name()}, n.queueOrder[n.queuePosition:]...)...)
			n.queuePosition++ // keep position pointed at same element
		}

	}
	// TODO (casie): How to determine what dequeueAlgo to pass?
	return childNode.getOrAddNode(pathFromNode[1:], tree)
}

type roundRobinState struct {
	subtreeNodesChecked int
}

func (rrs *roundRobinState) dequeueOps() dequeueOps {
	getNode := func(n *Node) (TreeNodeIFace, bool) {
		// advance the queue position for this dequeue
		n.queuePosition++
		if n.queuePosition >= len(n.queueOrder) {
			n.queuePosition = localQueueIndex
		}

		checkedAllNodes := rrs.subtreeNodesChecked == len(n.queueOrder)+1
		if n.queuePosition == localQueueIndex {
			return n, checkedAllNodes
		}

		currentNodeName := n.queueOrder[n.queuePosition]
		if node, ok := n.queueMap[currentNodeName]; ok {
			return node, checkedAllNodes
		}
		return nil, checkedAllNodes
	}

	// TODO (casie): Ignoring nodeName here because it doesn't matter...ugly
	updateState := func(n *Node, v any, _ string, deletedNode bool) {
		if v != nil {
			rrs.subtreeNodesChecked = 0
		} else {
			rrs.subtreeNodesChecked++
		}
		if deletedNode {
			n.queuePosition--
			// if we try to go beyond something that would increment to
			// the localQueueIndex or in queueOrder, we should wrap around to the
			// back of queueOrder.
			if n.queuePosition < localQueueIndex-1 {
				n.queuePosition = len(n.queueOrder) - 1
			}
		}
	}

	return dequeueOps{
		getNode:     getNode,
		updateState: updateState,
	}
}

type shuffleShardState struct {
	tenantQuerierMap map[TenantID]map[QuerierID]struct{}
	currentQuerier   *QuerierID
	nodesChecked     int
}

func (sss *shuffleShardState) dequeueOps() dequeueOps {
	// start from ssn.queuePosition
	// check that tenant for querierID against availabilityMap
	// if exists, move element to "back" of queue
	// if doesn't exist, check next child
	// nothing here to dequeue
	getNode := func(n *Node) (TreeNodeIFace, bool) {
		// can't get a tenant if no querier set
		if sss.currentQuerier == nil {
			return nil, true
		}

		checkedAllNodes := sss.nodesChecked == len(n.queueOrder)+1 // must check local queue as well

		// advance queue position for dequeue
		n.queuePosition++
		if n.queuePosition >= len(n.queueOrder) {
			n.queuePosition = 0
		}

		// no children
		if len(n.queueOrder) == 0 || n.queuePosition == localQueueIndex {
			return n, checkedAllNodes
		}

		checkIndex := n.queuePosition

		for iters := 0; iters < len(n.queueOrder); iters++ {
			if checkIndex >= len(n.queueOrder) {
				checkIndex = 0
			}
			tenantName := n.queueOrder[checkIndex]
			// increment nodes checked even if not in tenant-querier map
			sss.nodesChecked++
			checkedAllNodes = sss.nodesChecked == len(n.queueOrder)
			// if the tenant-querier set is nil, any querier can serve this tenant
			if sss.tenantQuerierMap[TenantID(tenantName)] == nil {
				return n.queueMap[tenantName], checkedAllNodes
			}
			if tenantQuerierSet, ok := sss.tenantQuerierMap[TenantID(tenantName)]; ok {
				if _, ok := tenantQuerierSet[*sss.currentQuerier]; ok {
					return n.queueMap[tenantName], checkedAllNodes
				}
			}
			checkIndex++
		}
		return nil, checkedAllNodes
	}

	updateState := func(n *Node, v any, nodeName string, deletedNode bool) {
		if deletedNode {
			// when we delete a child node, we want to step back in the queue so the
			// previously-next tenant will still be next (e.g., if queueOrder is
			// [childA, childB, childC] and childB (position 1) is deleted, we want to
			// step back to position 0 so that in the next dequeue, position 1 (childC)
			// is dequeued
			n.queuePosition--
		}
		if v != nil {
			sss.nodesChecked = 0
			return
		}

		sss.nodesChecked++

		// if dequeueNode was self, advance queue position no matter what, and return
		if n.queuePosition == localQueueIndex {
			return
		}

		// otherwise, don't change queuePosition, only move element to back
		for i, child := range n.queueOrder {
			if child == nodeName {
				n.queueOrder = append(n.queueOrder[:i], append(n.queueOrder[i+1:], child)...) // ugly
				n.queuePosition--
				// wrap around to back of order if we go past the first element in n.queueOrder.
				// In the next dequeue, we will step forward again.
				if n.queuePosition <= localQueueIndex {
					n.queuePosition = len(n.queueOrder) - 1
				}
			}
		}
	}

	return dequeueOps{
		getNode:     getNode,
		updateState: updateState,
	}

}
