package queue

import (
	"container/list"
	"fmt"
)

type TreeNodeIFace interface {
	//EnqueueFrontByPath(QueuePath, any) error
	enqueueBackByPath(*Tree, QueuePath, any) error
	dequeue() (QueuePath, any) // Dequeue returns the dequeued value, and the QueuePath (starting with root) to the node which was dequeued from
	IsEmpty() bool
	Name() string
	getOrAddNode(*Tree, QueuePath) (TreeNodeIFace, error)
	getLocalQueue() *list.List
}

type NodeType string

const roundRobin = NodeType("round-robin")

// shuffleShard node uses an externally maintained shuffle sharding mapping to select the next item to dequeue
const shuffleShard = NodeType("shuffle-shard")

type Tree struct {
	nodeTypesByDepth []NodeType
	maxDepth         int
	rootNode         TreeNodeIFace
	*ShuffleShardState
}

type ShuffleShardState struct {
	tenantQuerierMap map[TenantID]map[QuerierID]struct{}
	currentQuerier   *QuerierID
	nodesChecked     int
}

func NewTree(nodeTypeByDepth []NodeType, shuffleShardState *ShuffleShardState) (*Tree, error) {
	if len(nodeTypeByDepth) <= 0 {
		return nil, fmt.Errorf("no node types provided")
	}

	shuffleShardCount := 0
	for _, nt := range nodeTypeByDepth {
		if nt == shuffleShard {
			shuffleShardCount++
		}
	}
	if shuffleShardCount > 1 {
		return nil, fmt.Errorf("more than one tree layer with type shuffle-shard not currently supported")
	}

	if shuffleShardCount > 0 && shuffleShardState.tenantQuerierMap == nil {
		return nil, fmt.Errorf("cannot create tree with shuffle-shard nodes, without reference to tenant-querier map")
	}

	t := &Tree{
		nodeTypesByDepth:  nodeTypeByDepth,
		maxDepth:          len(nodeTypeByDepth) - 1,
		rootNode:          nil,
		ShuffleShardState: shuffleShardState,
	}
	t.rootNode = t.newNode("root", 0)
	return t, nil
}

// EnqueueBackByPath takes a QueuePath (excluding the root node), and
func (t *Tree) EnqueueBackByPath(path QueuePath, v any) error {
	// nodes need *Tree in order to know what type of node to create if necessary, and
	if len(path) > t.maxDepth {
		return fmt.Errorf("cannot create path with depth: %v; greater than max depth %v", len(path), t.maxDepth)
	}
	return t.rootNode.enqueueBackByPath(t, path, v)
}

func (t *Tree) Dequeue() (QueuePath, any) {
	return t.rootNode.dequeue()
}

func (t *Tree) newNode(name string, depth int) TreeNodeIFace {
	switch t.nodeTypesByDepth[depth] {
	case roundRobin:
		return &RoundRobinNode{
			name:          name,
			localQueue:    list.New(),
			queuePosition: localQueueIndex,
			queueOrder:    make([]string, 0),
			queueMap:      make(map[string]TreeNodeIFace, 1),
			depth:         depth,
		}
	case shuffleShard:
		return &ShuffleShardNode{
			name:              name,
			localQueue:        list.New(),
			queueOrder:        make([]string, 0),
			queueMap:          make(map[string]TreeNodeIFace, 1),
			depth:             depth,
			ShuffleShardState: t.ShuffleShardState,
		}
	default:
		panic("no defined node type at provided depth")
	}
}

type RoundRobinNode struct {
	name          string
	localQueue    *list.List
	queuePosition int      // position for dequeueing from queueOrder
	queueOrder    []string // order for dequeueing from self/children
	queueMap      map[string]TreeNodeIFace
	depth         int
	roundRobinState
}

type roundRobinState struct {
	nodesChecked int
}

func (rrn *RoundRobinNode) enqueueBackByPath(t *Tree, pathFromNode QueuePath, v any) error {
	childNode, err := rrn.getOrAddNode(t, pathFromNode)
	if err != nil {
		return err
	}
	// TODO (casie): Create localQueue on node creation; why not?
	childNode.getLocalQueue().PushBack(v)
	return nil
}

func (rrn *RoundRobinNode) getLocalQueue() *list.List {
	return rrn.localQueue
}

func (rrn *RoundRobinNode) getOrAddNode(t *Tree, pathFromNode QueuePath) (TreeNodeIFace, error) {
	if len(pathFromNode) == 0 {
		return rrn, nil
	}

	var childNode TreeNodeIFace
	var ok bool
	if childNode, ok = rrn.queueMap[pathFromNode[0]]; !ok {
		if rrn.depth+1 > t.maxDepth {
			return nil, fmt.Errorf("cannot create a node at depth %v; greater than max depth %v", rrn.depth+1, len(t.nodeTypesByDepth)-1)
		}
		// child does not exist, create it
		childNode = t.newNode(pathFromNode[0], rrn.depth+1)

		// add childNode to rrn.queueMap
		rrn.queueMap[childNode.Name()] = childNode
		// add childNode to rrn.queueOrder before the current position, update rrn.queuePosition to current element
		if rrn.queuePosition == localQueueIndex {
			rrn.queueOrder = append(rrn.queueOrder, childNode.Name())
		} else {
			rrn.queueOrder = append(rrn.queueOrder[:rrn.queuePosition], append([]string{childNode.Name()}, rrn.queueOrder[rrn.queuePosition:]...)...)
			rrn.queuePosition++
		}

	}
	return childNode.getOrAddNode(t, pathFromNode[1:])
}

func (rrn *RoundRobinNode) Name() string {
	return rrn.name
}

func (rrn *RoundRobinNode) dequeue() (QueuePath, any) {
	var v any
	var childPath QueuePath

	path := QueuePath{rrn.name}

	if rrn.IsEmpty() {
		return path, nil
	}

	var checkedAllNodes bool
	var dequeueNode TreeNodeIFace
	// continue until we've found a value or checked all nodes that need checking
	for v == nil && !checkedAllNodes {
		dequeueNode, checkedAllNodes = rrn.dequeueGetNode()
		// dequeueing from local queue
		if dequeueNode == rrn {
			if rrn.localQueue.Len() > 0 {
				// dequeueNode is self, local queue non-empty
				if elt := rrn.localQueue.Front(); elt != nil {
					rrn.localQueue.Remove(elt)
					v = elt.Value
				}
			}
		} else if dequeueNode == nil {
			// no dequeue-able node found
			return path, v
		} else {
			// dequeue from a child
			childPath, v = dequeueNode.dequeue()
			if dequeueNode.IsEmpty() {
				deleteNodeName := dequeueNode.Name()
				delete(rrn.queueMap, deleteNodeName)
				for idx, name := range rrn.queueOrder {
					if name == deleteNodeName {
						rrn.queueOrder = append(rrn.queueOrder[:idx], rrn.queueOrder[idx+1:]...)
					}
				}
				// removing an element sets our position one step forward; reset it to original queuePosition
				rrn.queuePosition--
			}
		}
		rrn.dequeueUpdateState(v)
	}
	return append(path, childPath...), v
}

func (rrn *RoundRobinNode) dequeueGetNode() (TreeNodeIFace, bool) {
	checkedAllNodes := rrn.nodesChecked == len(rrn.queueOrder)+1 // must check local queue as well
	if rrn.queuePosition == localQueueIndex {
		return rrn, checkedAllNodes
	}
	currentNodeName := rrn.queueOrder[rrn.queuePosition]
	// if the node is in queueMap, return it
	if node, ok := rrn.queueMap[currentNodeName]; ok {
		return node, checkedAllNodes
	}
	// if not in queueMap, return nil
	return nil, checkedAllNodes
}

func (rrn *RoundRobinNode) dequeueUpdateState(v any) {
	// non-nil value dequeued; reset node check counter
	if v != nil {
		rrn.nodesChecked = 0
	} else {
		// v is still nil, increment node check counter
		rrn.nodesChecked++
	}

	// either way, advance queue position
	rrn.queuePosition++
	if rrn.queuePosition >= len(rrn.queueOrder) {
		rrn.queuePosition = localQueueIndex
	}
}

func (rrn *RoundRobinNode) IsEmpty() bool {
	// node has nothing in local queue, and no child nodes
	return rrn.localQueue.Len() == 0 && len(rrn.queueMap) == 0
}

type ShuffleShardNode struct {
	name          string
	localQueue    *list.List // should never be populated
	queuePosition int
	queueOrder    []string // will be a slice of tenants (+ self?)
	queueMap      map[string]TreeNodeIFace
	depth         int
	*ShuffleShardState
}

func (ssn *ShuffleShardNode) Name() string {
	return ssn.name
}

// start from ssn.queuePosition
// check that tenant for querierID against availabilityMap
// if exists, move element to "back" of queue
// if doesn't exist, check next child
// nothing here to dequeue
func (ssn *ShuffleShardNode) dequeue() (QueuePath, any) {
	var v any
	var childPath QueuePath

	path := QueuePath{ssn.name}

	if ssn.IsEmpty() {
		return path, nil
	}

	var checkedAllNodes bool
	var dequeueNode TreeNodeIFace

	for v == nil && !checkedAllNodes {
		dequeueNode, checkedAllNodes = ssn.dequeueGetNode()
		if dequeueNode == ssn {
			if ssn.localQueue.Len() > 0 {
				if elt := ssn.localQueue.Front(); elt != nil {
					ssn.localQueue.Remove(elt)
					v = elt.Value
				}
			}
		} else if dequeueNode == nil {
			// no node found for querier
			return path, v
		} else {
			childPath, v = dequeueNode.dequeue()
			if dequeueNode.IsEmpty() {
				deleteNodeName := dequeueNode.Name()
				delete(ssn.queueMap, deleteNodeName)
				for idx, name := range ssn.queueOrder {
					if name == deleteNodeName {
						ssn.queueOrder = append(ssn.queueOrder[:idx], ssn.queueOrder[idx+1:]...)
					}
				}
				// removing an element sets our position one step forward; reset to original
				ssn.queuePosition--
			}
		}
		ssn.dequeueUpdateState(v, dequeueNode.Name())
	}
	return append(path, childPath...), v
}

// move dequeueNode to the back of queueOrder, regardless of whether a value was found?
func (ssn *ShuffleShardNode) dequeueUpdateState(v any, nodeName string) {
	if v != nil {
		ssn.nodesChecked = 0
		ssn.queuePosition++
		return
	}

	ssn.nodesChecked++

	// if dequeueNode was self, advance queue position no matter what, and return
	if ssn.queuePosition == localQueueIndex {
		ssn.queuePosition++
		return
	}

	// otherwise, don't change queuePosition, only move element to back
	for i, child := range ssn.queueOrder {
		if child == nodeName {
			ssn.queueOrder = append(ssn.queueOrder[:i], append(ssn.queueOrder[i+1:], child)...) // ugly
		}
	}
}

// return a function which selects the tree node from which to dequeue; return fn passed to tree
func (ssn *ShuffleShardNode) dequeueGetNode() (TreeNodeIFace, bool) {
	// can't get a tenant if no querier set
	if ssn.currentQuerier == nil {
		return nil, true
	}

	checkedAllNodes := ssn.nodesChecked == len(ssn.queueOrder)+1 // must check local queue as well
	// no children
	if len(ssn.queueOrder) == 0 || ssn.queuePosition == localQueueIndex {
		return ssn, checkedAllNodes
	}

	checkIndex := ssn.queuePosition

	for iters := 0; iters < len(ssn.queueOrder); iters++ {
		if checkIndex >= len(ssn.queueOrder) {
			checkIndex = 0
		}
		tenantName := ssn.queueOrder[checkIndex]
		// increment nodes checked even if not in tenant-querier map
		ssn.nodesChecked++
		checkedAllNodes = ssn.nodesChecked == len(ssn.queueOrder)
		if tenantQuerierSet, ok := ssn.tenantQuerierMap[TenantID(tenantName)]; ok {
			if _, ok := tenantQuerierSet[*ssn.currentQuerier]; ok {
				return ssn.queueMap[tenantName], checkedAllNodes
			}
		}
		checkIndex++
	}
	return nil, checkedAllNodes
}

func (ssn *ShuffleShardNode) dequeueMoveToBack(name string) {
	for i, child := range ssn.queueOrder {
		if child == name {
			ssn.queueOrder = append(ssn.queueOrder[:i], append(ssn.queueOrder[i+1:], child)...) // ugly
		}
	}
}

func (ssn *ShuffleShardNode) enqueueBackByPath(t *Tree, path QueuePath, v any) error {
	nodeToQueueTo, err := ssn.getOrAddNode(t, path)
	if err != nil {
		return err
	}
	nodeToQueueTo.getLocalQueue().PushBack(v)
	return nil
}

func (ssn *ShuffleShardNode) IsEmpty() bool {
	return ssn.localQueue.Len() == 0 && len(ssn.queueOrder) == 0
}

func (ssn *ShuffleShardNode) getOrAddNode(t *Tree, path QueuePath) (TreeNodeIFace, error) {
	if len(path) == 0 {
		return ssn, nil
	}

	var childNode TreeNodeIFace
	var ok bool
	if childNode, ok = ssn.queueMap[path[0]]; !ok {
		if ssn.depth+1 > t.maxDepth {
			return nil, fmt.Errorf("cannot create a node at depth %v; greater than max depth %v", ssn.depth+1, len(t.nodeTypesByDepth)-1)
		}
		childNode = t.newNode(path[0], ssn.depth+1)
		// add childNode to ssn.queueMap
		ssn.queueMap[childNode.Name()] = childNode
		// update ssn.queueOrder to place childNode behind rrn.queuePosition
		if ssn.queuePosition <= 0 {
			ssn.queueOrder = append(ssn.queueOrder, childNode.Name())
		} else {
			ssn.queueOrder = append(ssn.queueOrder[:ssn.queuePosition], append([]string{childNode.Name()}, ssn.queueOrder[ssn.queuePosition:]...)...)
			ssn.queuePosition++
		}
	}
	return childNode.getOrAddNode(t, path[1:])
}

func (ssn *ShuffleShardNode) getLocalQueue() *list.List {
	return ssn.localQueue
}
