// SPDX-License-Identifier: AGPL-3.0-only

package queue

// DequeueAlgorithm represents the set of operations specific to different approaches to dequeuing. It is applied
// at the layer-level -- every Node at the same depth in a TreeQueue shares the same DequeueAlgorithm, including state
// that may be stored in a struct that implements DequeueAlgorithm.
type DequeueAlgorithm interface {
	addChildNode(*Node, *Node)
	deleteChildNode(*Node, *Node) bool
	dequeueGetNode(*Node) (*Node, bool)
	dequeueUpdateState(*Node, any, bool)
}

// roundRobinState is the simplest type of DequeueAlgorithm; nodes which use this DequeueAlgorithm and are at
// the same depth in a TreeQueue do not share any state. When children are added to these nodes, they are placed at
// the "end" of the order from the perspective of the node's current queuePosition (e.g., if queuePosition is 3,
// a new child will be placed at index 2). Children are dequeued from using a simple round-robin ordering;
// queuePosition is incremented on every dequeue.
type roundRobinState struct {
}

// addChildNode creates a new child Node and adds it to the "end" of the parent's queueOrder from the perspective
// of the parent's queuePosition. Thus, If queuePosition is localQueueIndex, the new child is added to the end of
// queueOrder. Otherwise, the new child is added at queuePosition - 1, and queuePosition is incremented to keep
// it pointed to the same child.
func (rrs *roundRobinState) addChildNode(parent, child *Node) {
	// add childNode to n.queueMap
	parent.queueMap[child.Name()] = child

	// add childNode to n.queueOrder before the next-to-dequeue position, update n.queuePosition to current element
	if parent.queuePosition <= localQueueIndex {
		parent.queueOrder = append(parent.queueOrder, child.Name())
	} else {
		parent.queueOrder = append(parent.queueOrder[:parent.queuePosition], append([]string{child.Name()}, parent.queueOrder[parent.queuePosition:]...)...)
		parent.queuePosition++ // keep position pointed at same element
	}
}

// deleteChildNode removes a child node from a parent's children, and returns whether a child was found in the
// parent's queueOrder. This allows us to adjust the parent's queuePosition if necessary.
func (rrs *roundRobinState) deleteChildNode(parent, child *Node) bool {
	var childFound bool
	childName := child.Name()

	for idx, name := range parent.queueOrder {
		if name == childName {
			parent.queueOrder = append(parent.queueOrder[:idx], parent.queueOrder[idx+1:]...)
			childFound = true
		}
	}
	return childFound
}

// dequeueGetNode returns the node at the node's queuePosition. queuePosition represents the position of
// the next node to dequeue from, and is incremented in dequeueUpdateState.
func (rrs *roundRobinState) dequeueGetNode(n *Node) (*Node, bool) {
	checkedAllNodes := n.childrenChecked == len(n.queueOrder)+1
	if n.queuePosition == localQueueIndex {
		return n, checkedAllNodes
	}

	currentNodeName := n.queueOrder[n.queuePosition]
	if node, ok := n.queueMap[currentNodeName]; ok {
		return node, checkedAllNodes
	}
	return nil, checkedAllNodes
}

// dequeueUpdateState increments queuePosition based on whether a child node was deleted during dequeue,
// and updates childrenChecked to reflect the number of children checked so that the dequeueGetNode operation
// will stop once it has checked all possible nodes.
func (rrs *roundRobinState) dequeueUpdateState(n *Node, v any, deletedNode bool) {
	if v != nil {
		n.childrenChecked = 0
	} else {
		n.childrenChecked++
	}
	if !deletedNode {
		n.queuePosition++
	}
	if n.queuePosition >= len(n.queueOrder) {
		n.queuePosition = localQueueIndex
	}
}
