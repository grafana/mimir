// SPDX-License-Identifier: AGPL-3.0-only

package queue

// DequeueAlgorithm represents the set of operations specific to different approaches to dequeuing. It is applied
// at the layer-level -- every Node at the same depth in a TreeQueue shares the same DequeueAlgorithm, including state
// that may be stored in a struct that implements DequeueAlgorithm.
type DequeueAlgorithm interface {
	addChildNode(parent, child *Node)
	dequeueSelectNode(dequeueFrom *Node) (*Node, bool)
	dequeueUpdateState(dequeueParent *Node, dequeuedFrom *Node)
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

// dequeueGetNode returns the node at the node's queuePosition. queuePosition represents the position of
// the next node to dequeue from, and is incremented in dequeueUpdateState.
func (rrs *roundRobinState) dequeueSelectNode(dequeueFrom *Node) (*Node, bool) {
	checkedAllNodes := dequeueFrom.childrenChecked == len(dequeueFrom.queueOrder)+1
	if dequeueFrom.queuePosition == localQueueIndex {
		return dequeueFrom, checkedAllNodes
	}

	currentNodeName := dequeueFrom.queueOrder[dequeueFrom.queuePosition]
	if node, ok := dequeueFrom.queueMap[currentNodeName]; ok {
		return node, checkedAllNodes
	}
	return nil, checkedAllNodes
}

// dequeueUpdateState does the following:
//   - deletes the dequeued-from child node if it is empty after the dequeue operation
//   - increments queuePosition if no child was deleted
//   - updates childrenChecked to reflect the number of children checked so that the dequeueGetNode operation
//     will stop once it has checked all possible nodes.
func (rrs *roundRobinState) dequeueUpdateState(parent *Node, dequeuedFrom *Node) {
	// if the child node is nil, we haven't done anything to the tree; return early
	if dequeuedFrom == nil {
		return
	}

	// corner case: if parent == dequeuedFrom and is empty, we will try to delete a "child" (no-op),
	// and won't increment position. This is fine, because if the parent is empty, there's nothing to
	// increment to.
	childIsEmpty := dequeuedFrom.IsEmpty()

	// if the child is empty, we should delete it, but not increment queue position, since removing an element
	// from queueOrder sets our position to the next element already.
	if childIsEmpty {
		childName := dequeuedFrom.Name()
		delete(parent.queueMap, childName)
		for idx, name := range parent.queueOrder {
			if name == childName {
				parent.queueOrder = append(parent.queueOrder[:idx], parent.queueOrder[idx+1:]...)
			}
		}

	} else {
		parent.queuePosition++
	}
	if parent.queuePosition >= len(parent.queueOrder) {
		parent.queuePosition = localQueueIndex
	}
}
