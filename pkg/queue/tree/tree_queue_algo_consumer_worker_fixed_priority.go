// SPDX-License-Identifier: AGPL-3.0-only

package tree

import (
	"slices"
)

// ConsumerWorkerFixedPriorityAlgo implements QueuingAlgorithm with a fixed priority node per consumer-worker.
// The integer WorkerID % len(priorityNodes) selects the priority node of a consumer-worker.
// The mapping does not change when the tree deletes an empty node or creates it again.
//
// A consumer-worker dequeues from its priority node first.
// When its priority node does not exist, or has nothing to dequeue for it, the consumer-worker walks over the other nodes that exist.
// The walk takes them in nodeNames order, from position WorkerID % the number of those nodes.
// The walk depends only on the WorkerID and on the nodes that exist, so consumer-workers share no walk state.
//
// A node that is not in priorityNodes is the priority node of no consumer-worker.
// Consumer-workers dequeue from it only in a walk.
type ConsumerWorkerFixedPriorityAlgo struct {
	priorityNodes []string
	// nodeNames holds priorityNodes, then each other node name in the order of its first enqueue.
	nodeNames []string

	currentWorkerID int
	// dequeueOrder holds the node names for the current dequeue: the priority node, then the walk.
	dequeueOrder []string
	// walk is a buffer for the other nodes that exist.
	walk []string
}

func NewConsumerWorkerFixedPriorityAlgo(priorityNodes []string) *ConsumerWorkerFixedPriorityAlgo {
	return &ConsumerWorkerFixedPriorityAlgo{
		priorityNodes: priorityNodes,
		nodeNames:     slices.Clone(priorityNodes),
	}
}

func (qa *ConsumerWorkerFixedPriorityAlgo) setup(dequeueArgs *DequeueArgs) {
	qa.currentWorkerID = dequeueArgs.WorkerID
}

func (qa *ConsumerWorkerFixedPriorityAlgo) addChildNode(parent, child *Node) {
	parent.queueMap[child.Name()] = child
	if !slices.Contains(qa.nodeNames, child.Name()) {
		qa.nodeNames = append(qa.nodeNames, child.Name())
	}
}

// dequeueSelectNode returns the node at position node.childrenChecked in dequeueOrder.
// The tree increments node.childrenChecked after each node that has nothing to dequeue.
func (qa *ConsumerWorkerFixedPriorityAlgo) dequeueSelectNode(node *Node) *Node {
	if node.childrenChecked == 0 {
		qa.setDequeueOrder(node)
	}
	if node.childrenChecked < len(qa.dequeueOrder) {
		return node.queueMap[qa.dequeueOrder[node.childrenChecked]]
	}
	return nil
}

// setDequeueOrder sets dequeueOrder to the priority node, if it exists, then the walk.
func (qa *ConsumerWorkerFixedPriorityAlgo) setDequeueOrder(node *Node) {
	priorityNode := qa.priorityNodes[qa.currentWorkerID%len(qa.priorityNodes)]
	qa.walk = qa.walk[:0]
	for _, name := range qa.nodeNames {
		if _, ok := node.queueMap[name]; ok && name != priorityNode {
			qa.walk = append(qa.walk, name)
		}
	}
	qa.dequeueOrder = qa.dequeueOrder[:0]
	if _, ok := node.queueMap[priorityNode]; ok {
		qa.dequeueOrder = append(qa.dequeueOrder, priorityNode)
	}
	for i := range qa.walk {
		qa.dequeueOrder = append(qa.dequeueOrder, qa.walk[(qa.currentWorkerID+i)%len(qa.walk)])
	}
}

// dequeueUpdateState deletes the dequeued-from node if it is empty.
func (qa *ConsumerWorkerFixedPriorityAlgo) dequeueUpdateState(node *Node, dequeuedFrom *Node) {
	if dequeuedFrom != nil && dequeuedFrom.IsEmpty() {
		delete(node.queueMap, dequeuedFrom.Name())
	}
}
