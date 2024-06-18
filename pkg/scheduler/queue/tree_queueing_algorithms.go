// SPDX-License-Identifier: AGPL-3.0-only

package queue

// QueuingAlgorithm represents the set of operations specific to different approaches to queuing/dequeuing. It is
// applied at the layer-level -- every Node at the same depth in a TreeQueue shares the same QueuingAlgorithm,
// including any state in structs that implement QueuingAlgorithm.
type QueuingAlgorithm interface {
	// addChildNode creates a child Node of parent. QueuePaths passed to enqueue functions are allowed to contain
	// node names for nodes which do not yet exist; in those cases, we create the missing nodes. Implementers are
	// responsible for adding the newly-created node to whatever state(s) keep track of nodes in the subtree, as well
	// as updating any positional pointers that have been shifted by the addition of a new node.
	addChildNode(parent, child *Node)

	// dequeueSelectNode, given a node, returns the node which an element will be dequeued from next, as well
	// as a bool representing whether the passed node and all its children have been checked for dequeue-ability.
	// The returned node may be the same as the node passed in; this triggers the base case for the recursive
	// Node.dequeue, and results in dequeuing from the node's local queue. dequeueSelectNode does *not*
	// update any Node fields.
	dequeueSelectNode(node *Node) (*Node, bool)

	// dequeueUpdateState is called after we have finished dequeuing from a node. When a child is left empty after the
	// dequeue operation, dequeueUpdateState performs cleanup by deleting that child from the node and all other
	// necessary state, and updates the relevant position to point to the next node to dequeue from.
	dequeueUpdateState(node *Node, dequeuedFrom *Node)
}

// roundRobinState is the simplest type of QueuingAlgorithm; nodes which use this QueuingAlgorithm and are at
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

// dequeueSelectNode returns the node at the node's queuePosition. queuePosition represents the position of
// the next node to dequeue from, and is incremented in dequeueUpdateState.
func (rrs *roundRobinState) dequeueSelectNode(node *Node) (*Node, bool) {
	checkedAllNodes := node.childrenChecked == len(node.queueOrder)+1 // must check local queue as well
	if node.queuePosition == localQueueIndex {
		return node, checkedAllNodes
	}

	currentNodeName := node.queueOrder[node.queuePosition]
	if node, ok := node.queueMap[currentNodeName]; ok {
		return node, checkedAllNodes
	}
	return nil, checkedAllNodes
}

// dequeueUpdateState does the following:
//   - deletes the dequeued-from child node if it is empty after the dequeue operation
//   - increments queuePosition if no child was deleted
func (rrs *roundRobinState) dequeueUpdateState(node *Node, dequeuedFrom *Node) {
	// if the child node is nil, we haven't done anything to the tree; return early
	if dequeuedFrom == nil {
		return
	}

	// corner case: if node == dequeuedFrom and is empty, we will try to delete a "child" (no-op),
	// and won't increment position. This is fine, because if the node is empty, there's nothing to
	// increment to.
	childIsEmpty := dequeuedFrom.IsEmpty()

	// if the child is empty, we should delete it, but not increment queue position, since removing an element
	// from queueOrder sets our position to the next element already.
	if childIsEmpty {
		childName := dequeuedFrom.Name()
		delete(node.queueMap, childName)
		for idx, name := range node.queueOrder {
			if name == childName {
				node.queueOrder = append(node.queueOrder[:idx], node.queueOrder[idx+1:]...)
			}
		}

	} else {
		node.queuePosition++
	}
	if node.queuePosition >= len(node.queueOrder) {
		node.queuePosition = localQueueIndex
	}
}
