package queue

import (
	"container/list"
	"fmt"
)

// TODO (casie): We might not need this interface anymore.
type TreeNodeIFace interface {
	//EnqueueFrontByPath(QueuePath, any) error
	enqueueBackByPath(OpsPath, any) error
	dequeue() (QueuePath, any) // Dequeue returns the dequeued value, and the QueuePath (starting with root) to the node which was dequeued from
	IsEmpty() bool
	Name() string
	getLocalQueue() *list.List
	getOrAddNode(OpsPath) (TreeNodeIFace, error)
}

type DequeueAlgorithm interface {
	ops() dequeueOps
}

type OpsPath []struct {
	name             string
	dequeueAlgorithm DequeueAlgorithm
}

type dequeueGetNodeFunc func(*Node) (TreeNodeIFace, bool)
type dequeueUpdateStateFunc func(*Node, any, string)

type dequeueOps struct {
	getNode     dequeueGetNodeFunc
	updateState dequeueUpdateStateFunc
}

type Node struct {
	name             string
	localQueue       *list.List
	queuePosition    int      // position for dequeueing from queueOrder
	queueOrder       []string // order for dequeueing from self/children
	queueMap         map[string]TreeNodeIFace
	depth            int
	dequeueAlgorithm DequeueAlgorithm
}

// TODO (casie): Do we want some concept of a max-depth?

func NewNode(name string, da DequeueAlgorithm) (TreeNodeIFace, error) {
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
		queuePosition:    localQueueIndex,
		queueOrder:       make([]string, 0),
		queueMap:         make(map[string]TreeNodeIFace, 1),
		dequeueAlgorithm: da,
	}, nil
}

func (n *Node) IsEmpty() bool {
	// node has nothing in local queue, and no child nodes
	return n.localQueue.Len() == 0 && len(n.queueMap) == 0
}

func (n *Node) Name() string {
	return n.name
}

func (n *Node) getLocalQueue() *list.List {
	return n.localQueue
}

func (n *Node) enqueueBackByPath(pathFromNode OpsPath, v any) error {
	childNode, err := n.getOrAddNode(pathFromNode)
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
		dequeueNode, checkedAllNodes = n.dequeueAlgorithm.ops().getNode(n)
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
				// removing an element sets our position one step forward; reset it to original queuePosition
				n.queuePosition--
			}
		}
		n.dequeueAlgorithm.ops().updateState(n, v, dequeueNode.Name())
	}
	return append(path, childPath...), v
}

// TODO (casie): Ugly -- if a matching node name is found, the dequeueAlgorithm will be ignored, which
//
//	could lead to some unexpected behavior.
//
// getOrAddNode checks whether the first node name in pathFromNode exists in a node's children;
// if no node exists, one is created and added to the "end" of the node's children, from the perspective
// of node.queuePosition (e.g., if node.queuePosition is 4, the new node will be created at position 3).
// This follows queue behavior, but if, for some reason, we ever want to create nodes in a way that violates
// queueing behavior, this will require a refactor, as will, well, the entire "tree queue" data structure.
func (n *Node) getOrAddNode(pathFromNode OpsPath) (TreeNodeIFace, error) {
	if len(pathFromNode) == 0 {
		return n, nil
	}

	var childNode TreeNodeIFace
	var ok bool
	var err error
	if childNode, ok = n.queueMap[pathFromNode[0].name]; !ok {
		// child does not exist, create it
		childNode, err = NewNode(pathFromNode[0].name, pathFromNode[0].dequeueAlgorithm)
		if err != nil {
			return nil, err
		}

		// add childNode to rrn.queueMap
		n.queueMap[childNode.Name()] = childNode
		// add childNode to rrn.queueOrder before the current position, update rrn.queuePosition to current element
		if n.queuePosition == localQueueIndex {
			n.queueOrder = append(n.queueOrder, childNode.Name())
		} else {
			n.queueOrder = append(n.queueOrder[:n.queuePosition], append([]string{childNode.Name()}, n.queueOrder[n.queuePosition:]...)...)
			n.queuePosition++
		}

	}
	// TODO (casie): How to determine what dequeueAlgo to pass?
	return childNode.getOrAddNode(pathFromNode[1:])
}

type roundRobinState struct {
	subtreeNodesChecked int
}

func (rrs *roundRobinState) ops() dequeueOps {
	getNode := func(n *Node) (TreeNodeIFace, bool) {
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
	updateState := func(n *Node, v any, _ string) {
		if v != nil {
			rrs.subtreeNodesChecked = 0
		} else {
			rrs.subtreeNodesChecked++
		}

		n.queuePosition++
		if n.queuePosition >= len(n.queueOrder) {
			n.queuePosition = localQueueIndex
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

func (sss *shuffleShardState) ops() dequeueOps {
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
			if tenantQuerierSet, ok := sss.tenantQuerierMap[TenantID(tenantName)]; ok {
				if _, ok := tenantQuerierSet[*sss.currentQuerier]; ok {
					return n.queueMap[tenantName], checkedAllNodes
				}
			}
			checkIndex++
		}
		return nil, checkedAllNodes
	}

	updateState := func(n *Node, v any, nodeName string) {
		if v != nil {
			sss.nodesChecked = 0
			n.queuePosition++
			return
		}

		sss.nodesChecked++

		// if dequeueNode was self, advance queue position no matter what, and return
		if n.queuePosition == localQueueIndex {
			n.queuePosition++
			return
		}

		// otherwise, don't change queuePosition, only move element to back
		for i, child := range n.queueOrder {
			if child == nodeName {
				n.queueOrder = append(n.queueOrder[:i], append(n.queueOrder[i+1:], child)...) // ugly
			}
		}
	}

	return dequeueOps{
		getNode:     getNode,
		updateState: updateState,
	}

}
