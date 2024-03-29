package queue

import (
	"container/list"
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
	Dequeue(ops []DequeueLevelOps) (any, error)
}

const NodeLocalQueueName = "_local"

type RoundRobinState struct {
	currentChildQueueIndex int
	childQueueOrder        []string
	currentDequeueAttempts int
}

func (rrs *RoundRobinState) wrapIndex(increment bool) {
	if increment {
		rrs.currentChildQueueIndex++
	}
	if rrs.currentChildQueueIndex >= len(rrs.childQueueOrder) {
		rrs.currentChildQueueIndex = localQueueIndex
	}
}

func (rrs *RoundRobinState) MakeEnqueueStateUpdateFunc() EnqueueStateUpdateFunc {
	return func(nodeName string, _ bool) {
		if rrs.currentChildQueueIndex == localQueueIndex {
			// special case; cannot slice into childQueueOrder with index -1
			// place at end of slice, which is the last slot before the local queue slot
			rrs.childQueueOrder = append(rrs.childQueueOrder, nodeName)
		} else {
			// insert into order behind current child queue index
			rrs.childQueueOrder = append(
				rrs.childQueueOrder[:rrs.currentChildQueueIndex],
				append(
					[]string{nodeName},
					rrs.childQueueOrder[rrs.currentChildQueueIndex:]...,
				)...,
			)
			// update current child queue index to its new place in the expanded slice
			rrs.currentChildQueueIndex++
		}
	}
}

func (rrs *RoundRobinState) MakeDequeueNodeSelectFunc() DequeueLevelOps {
	initialChildQueueOrderLen := len(rrs.childQueueOrder)

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
		return rrs.childQueueOrder[rrs.currentChildQueueIndex], stop
	}

	stateUpdateFunc := func(dequeuedNodeName string, nodeDeleted bool) {
		dequeueSuccess := dequeuedNodeName != ""
		if dequeueSuccess {
			rrs.currentDequeueAttempts = 0
		} else {
			rrs.currentDequeueAttempts++
		}

		if nodeDeleted {
			for i, name := range rrs.childQueueOrder {
				if name == dequeuedNodeName {
					rrs.childQueueOrder = append(rrs.childQueueOrder[:i], rrs.childQueueOrder[i+1:]...)
					rrs.wrapIndex(false)
					break
				}
			}
		} else {
			rrs.wrapIndex(true)
		}
	}

	return DequeueLevelOps{selectFunc, stateUpdateFunc}
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

		childNode, exists := currentNode.childNodeMap[op.nodeName]
		if !exists {
			childNode = &TreeQueueImplA{
				name:         op.nodeName,
				localQueue:   nil,
				childNodeMap: nil,
			}
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
func (tqa *TreeQueueImplA) Dequeue(ops []DequeueLevelOps) (any, error) {
	var v any
	// error for ops longer than maxDepth ?

	// essentially for each level of the tree we call op.Select to select the child node;
	// we also want to call op.Update state for each level we traverse.

	// if we are at the end of the ops which should be <= max tree depth we dequeue from the local queue
	// or if the op.Select result == NodeLocalQueueName, we do the same

	// we have to tell op.Update state for each level we traversed, but we can't call it until we know if the node
	// gets deleted for being empty yet; this is possible but I'm currently searching for the least-ugly way to do it

	// TODO: un-screw this

	return v, nil
}
