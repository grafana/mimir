package queue

import (
	"container/list"
	"fmt"

	"github.com/pkg/errors"
)

//	type QueueAlgorithmStateA interface {
//		//GetState() any
//		//SetState(any) error
//	}
type EnqueueStateUpdateFunc func(nodeName string, nodeCreated bool)

type EnqueueLevelOps struct {
	nodeName    string
	UpdateState EnqueueStateUpdateFunc
}

type DequeueNodeSelectFunc func() (nodeName string, stop bool)
type DequeueUpdateStateFunc func(dequeuedNodeName string, nodeDeleted bool)

type DequeueLevelOps struct {
	Select      DequeueNodeSelectFunc
	UpdateState DequeueUpdateStateFunc
}

type TreeQueueInterface interface {
	EnqueueBackByPath(v any, ops []EnqueueLevelOps) error
	Dequeue(ops []*DequeueLevelOps) (any, error)
}

const NodeLocalQueueName = "_local"

type RoundRobinState struct {
	currentChildQueueIndex int
	queueNodeCounts        map[string]int
	queueNodeOrder         []string
	currentDequeueAttempts int
}

func NewRoundRobinState() *RoundRobinState {
	return &RoundRobinState{
		currentChildQueueIndex: -1,
		queueNodeCounts:        map[string]int{},
		queueNodeOrder:         nil,
		currentDequeueAttempts: 0,
	}
}

func (rrs *RoundRobinState) wrapIndex(increment bool) {
	if increment {
		rrs.currentChildQueueIndex++
	}
	if rrs.currentChildQueueIndex >= len(rrs.queueNodeOrder) {
		rrs.currentChildQueueIndex = localQueueIndex
	}
}

func (rrs *RoundRobinState) MakeEnqueueStateUpdateFunc() EnqueueStateUpdateFunc {
	return func(nodeName string, nodeCreated bool) {
		// we just need to do is add the node to the rotation if it is not already tracked
		if _, ok := rrs.queueNodeCounts[nodeName]; !ok {
			if rrs.currentChildQueueIndex == localQueueIndex {
				// special case; cannot slice into queueNodeOrder with index -1
				// place at end of slice, which is the last slot before the local queue slot
				rrs.queueNodeOrder = append(rrs.queueNodeOrder, nodeName)
			} else {
				// insert into order behind current child queue index
				rrs.queueNodeOrder = append(
					rrs.queueNodeOrder[:rrs.currentChildQueueIndex],
					append(
						[]string{nodeName},
						rrs.queueNodeOrder[rrs.currentChildQueueIndex:]...,
					)...,
				)
				// update current child queue index to its new place in the expanded slice
				rrs.currentChildQueueIndex++
			}
			rrs.queueNodeCounts[nodeName] = 1
		} else {
			// node is already in the rotation;
			// update the node count only if it was just created
			rrs.queueNodeCounts[nodeName] += 1
		}
	}
}

func (rrs *RoundRobinState) MakeDequeueNodeSelectFunc() *DequeueLevelOps {
	initialChildQueueOrderLen := len(rrs.queueNodeOrder)

	selectFunc := func() (nodeName string, stop bool) {
		stop = rrs.currentDequeueAttempts == initialChildQueueOrderLen
		if rrs.currentChildQueueIndex == localQueueIndex {
			// dequeuing from local queue; either we have:
			//  1. reached a leaf node, or
			//  2. reached an inner node when it is the local queue's turn
			return NodeLocalQueueName, stop
		}
		// else; dequeuing from child queue node;
		// pick the child node whose turn it is
		return rrs.queueNodeOrder[rrs.currentChildQueueIndex], stop
	}

	stateUpdateFunc := func(dequeuedNodeName string, nodeDeleted bool) {
		dequeueSuccess := dequeuedNodeName != ""
		if dequeueSuccess {
			rrs.currentDequeueAttempts = 0
		} else {
			// nothing was found at the current node in the rotation;
			rrs.currentDequeueAttempts++
		}

		if dequeueSuccess && nodeDeleted {
			rrs.queueNodeCounts[dequeuedNodeName] -= 1
		}

		if dequeueSuccess && rrs.queueNodeCounts[dequeuedNodeName] == 0 {
			// at the level in the tree governed by this queue algorithm,
			// there are no nodes left with this node name;
			// delete it from the map and remove it from the rotation
			delete(rrs.queueNodeCounts, dequeuedNodeName)
			for i, name := range rrs.queueNodeOrder {
				if name == dequeuedNodeName {
					rrs.queueNodeOrder = append(rrs.queueNodeOrder[:i], rrs.queueNodeOrder[i+1:]...)
					rrs.wrapIndex(false)
					break
				}
			}
		} else {
			rrs.wrapIndex(true)
		}
	}

	return &DequeueLevelOps{selectFunc, stateUpdateFunc}
}

type TreeQueueImplA struct {
	name string
	//maxDepth     int
	localQueue   *list.List
	childNodeMap map[string]*TreeQueueImplA
}

func (tqa *TreeQueueImplA) EnqueueBackByPath(v any, ops []EnqueueLevelOps) error {
	// error for ops longer than maxDepth ?

	// currently ignores the possibility of enqueueing into root node (len(ops) == 0)

	currentNode := tqa

	for opDepth, op := range ops {

		if currentNode.childNodeMap == nil {
			currentNode.childNodeMap = map[string]*TreeQueueImplA{}
		}
		childNode, exists := currentNode.childNodeMap[op.nodeName]
		if !exists {
			childNode = &TreeQueueImplA{
				name:         op.nodeName,
				localQueue:   nil,
				childNodeMap: nil,
			}
			currentNode.childNodeMap[op.nodeName] = childNode
		}

		if opDepth+1 == len(ops) {
			// reached the end; enqueue into local queue
			if childNode.localQueue == nil {
				childNode.localQueue = list.New()
			}
			childNode.localQueue.PushBack(v)

			// update state and return
			op.UpdateState(childNode.name, !exists)
			return nil
		} else {
			// enqueuing will occur deeper in tree; update state and continue
			currentNode = childNode
			op.UpdateState(childNode.name, !exists)
			continue
		}
	}

	return nil
}
func (tqa *TreeQueueImplA) Dequeue(ops []*DequeueLevelOps) (any, error) {
	var v any
	// error for ops longer than maxDepth ?

	//stop := false
	currentNode := tqa

	// walk down tree selecting nodes until we dequeue
	for opDepth := 0; opDepth < len(ops) && v == nil; opDepth++ {
		op := ops[opDepth]

		for {
			childNodeName, stop := op.Select()
			if stop {
				// we are done; queue algorithm has exhausted its options
				// before we found a non-empty node to dequeue from
				op.UpdateState("", false)
				return nil, nil
			}

			if childNodeName == NodeLocalQueueName {
				// dequeue operations selected the local queue for the current node level;
				// no need to go any deeper. dequeue from the current node's local queue.
				v = currentNode.deqeueLocal()
				if v != nil {
					// we are done;
					// inform the queue algorithm of successful node selection
					op.UpdateState(childNodeName, false)
					break
				} else {
					// there was nothing to dequeue in the node-local queue;
					// inform the queue algorithm to select the next node and loop
					op.UpdateState("", false)
					continue
				}

			}

			// else; dequeue operations selected a child node for the current node level
			childNode, exists := currentNode.childNodeMap[childNodeName]
			if !exists {
				return nil, MakeNodeDoesNotExistErr(currentNode.name, childNodeName, opDepth)
			}

			if opDepth+1 == len(ops) {
				// reached the end; dequeue from selected child node-local queue
				v = childNode.deqeueLocal()

				if childNode.IsEmpty() {
					delete(currentNode.childNodeMap, childNodeName)
					op.UpdateState(childNodeName, true)
				}
			} else {
				// still need to go deeper;
				// but inform the queue algorithm of successful node selection
				op.UpdateState(childNodeName, false)
			}

			// update current node to continue walking down the tree
			currentNode = childNode
			break
		}
	}

	return v, nil
}

func MakeNodeDoesNotExistErr(nodeName, childNodeName string, depth int) error {
	msg := fmt.Sprintf(
		"child node %s selected from node %s at tree depth %d does not exist",
		childNodeName, nodeName, depth,
	)
	return errors.New(msg)
}

func (tqa *TreeQueueImplA) deqeueLocal() any {
	if tqa.localQueue != nil {
		if elem := tqa.localQueue.Front(); elem != nil {
			tqa.localQueue.Remove(elem)
			return elem.Value
		}
	}
	return nil
}

func (tqa *TreeQueueImplA) IsEmpty() bool {
	// avoid recursion to make this a cheap operation
	//
	// Because we dereference empty child nodes during dequeuing,
	// we assume that emptiness means there are no child nodes
	// and nothing in this tree node's local queue.
	//
	// In reality a package member could attach empty child queues with getOrAddNode
	// in order to get a functionally-empty tree that would report false for IsEmpty.
	// We assume this does not occur or is not relevant during normal operation.
	return tqa.LocalQueueLen() == 0 && len(tqa.childNodeMap) == 0
}

//// NodeCount counts the TreeQueue node and all its children, recursively.
// There's a bug - I am double-counting when I recur for reasons I understand
// but haven't figured out how to solve  elegantly yet
//func (tqa *TreeQueueImplA) NodeCount() int {
//	count := 1 // count self
//	for _, childNode := range tqa.childNodeMap {
//		count += childNode.NodeCount()
//	}
//	return count
//}

// ItemCount counts the queue items in the TreeQueue node and in all its children, recursively.
func (tqa *TreeQueueImplA) ItemCount() int {
	count := tqa.LocalQueueLen() // count self
	for _, childNode := range tqa.childNodeMap {
		count += childNode.ItemCount()
	}
	return count
}

func (tqa *TreeQueueImplA) LocalQueueLen() int {
	localQueueLen := 0
	if tqa.localQueue != nil {
		localQueueLen = tqa.localQueue.Len()
	}
	return localQueueLen
}
