// SPDX-License-Identifier: AGPL-3.0-only

package queue

import "slices"

// QueuingAlgorithm represents the set of operations specific to different approaches to queuing/dequeuing. It is
// applied at the layer-level -- every Node at the same depth in a MultiQueuingAlgorithmTreeQueue shares the same QueuingAlgorithm,
// including any state in structs that implement QueuingAlgorithm.
type QueuingAlgorithm interface {
	// setup is called by MultiQueuingAlgorithmTreeQueue once before beginning to dequeue. The tree calls setup on
	// each QueuingAlgorithm in the tree, to update any algorithm state that requires outside information.
	setup(dequeueReq *DequeueArgs)

	// addChildNode updates a parent's queueMap and other child-tracking structures with a new child Node. QueuePaths
	// passed to enqueue functions are allowed to contain node names for nodes which do not yet exist; in those cases,
	// we create the missing nodes. Implementers are responsible for adding the newly-created node to whatever state(s)
	// keep track of nodes in the subtree, as well as updating any positional pointers that have been shifted by the
	// addition of a new node.
	addChildNode(parent, child *Node)

	// dequeueSelectNode uses information in the given node and/or the QueuingAlgorithm state to select and return
	// the node which an element will be dequeued from next, as well as a bool representing whether the passed node
	// and all its children have been checked for dequeue-ability. The returned node may be one of the following:
	//	- the given node: this triggers the base case for the recursive Node.dequeue, and results in dequeuing from
	//    the node's local queue.
	//	- a direct child of the given node: this continues the dequeue recursion.
	// The Tree uses the bool returned to determine when no dequeue-able value can be found at this child. If all
	// children in the subtree have been checked, but no dequeue-able value is found, the Tree traverses to the next
	// child in the given node's order. dequeueSelectNode does *not* update any Node fields.
	dequeueSelectNode(node *Node) *Node

	// dequeueUpdateState is called after we have finished dequeuing from a node. When a child is left empty after the
	// dequeue operation, dequeueUpdateState should perform cleanup by deleting that child from the Node and update
	// any other necessary state to reflect the child's removal. It should also update the relevant position to point
	// to the next node to dequeue from.
	dequeueUpdateState(node *Node, dequeuedFrom *Node)
}

// roundRobinState is the simplest type of QueuingAlgorithm; nodes which use this QueuingAlgorithm and are at
// the same depth in a MultiQueuingAlgorithmTreeQueue do not share any state. When children are added to these nodes,
// they are placed at the "end" of the order from the perspective of the node's current queuePosition (e.g., if
// queuePosition is 3, a new child will be placed at index 2). Children are dequeued from using a simple round-robin
// ordering; queuePosition is incremented on every dequeue.
type roundRobinState struct {
}

// setup for roundRobinState doesn't need to do any state updates, because roundRobinState doesn't maintain any state.
func (rrs *roundRobinState) setup(_ *DequeueArgs) {
}

func (rrs *roundRobinState) wrapQueuePositionIndex(node *Node, increment bool) {
	if increment {
		node.queuePosition++
	}

	if node.queuePosition >= len(node.queueOrder) {
		node.queuePosition = 0
	}
}

// addChildNode adds a child Node to the "end" of the parent's queueOrder
// relative to perspective of the parent Node's current queuePosition.
func (rrs *roundRobinState) addChildNode(parent, child *Node) {
	// add childNode to n.queueMap
	parent.queueMap[child.Name()] = child

	if parent.queuePosition == 0 {
		// special case; since we are at the beginning of the order,
		// only a simple append is needed to add the new node to the end,
		// which also creates a more intuitive initial order for tests
		parent.queueOrder = append(parent.queueOrder, child.Name())
	} else {
		// insert into the order behind current child queue index
		// to prevent the possibility of new nodes continually jumping the line
		parent.queueOrder = slices.Insert(parent.queueOrder, parent.queuePosition, child.Name())
		// since the new node was inserted into the order behind the current node,
		// the queuePosition must be pushed forward to remain pointing at the same node
		rrs.wrapQueuePositionIndex(parent, true)
	}
}

// dequeueSelectNode returns the node at the node's queuePosition. queuePosition represents the position of
// the next node to dequeue from, and is incremented in dequeueUpdateState.
func (rrs *roundRobinState) dequeueSelectNode(node *Node) *Node {
	if node.isLeaf() || len(node.queueOrder) == 0 {
		return node
	}
	currentNodeName := node.queueOrder[node.queuePosition]
	if child, ok := node.queueMap[currentNodeName]; ok {
		return child
	}
	return nil
}

// dequeueUpdateState does the following:
//   - deletes the dequeued-from child node if it is empty after the dequeue operation
//   - increments queuePosition if no child was deleted
func (rrs *roundRobinState) dequeueUpdateState(node *Node, dequeuedFrom *Node) {
	// if the child node is nil, we haven't done anything to the tree; return early
	if dequeuedFrom == nil {
		return
	}

	// if the child is empty, we should delete it
	if dequeuedFrom != node && dequeuedFrom.IsEmpty() {
		childName := dequeuedFrom.Name()

		childIndex := slices.Index(node.queueOrder, childName)
		if childIndex != -1 {
			node.queueOrder = slices.Delete(node.queueOrder, childIndex, childIndex+1)
			// we do not need to increment queuePosition
			// the node removed is always the node pointed to by queuePosition
			// so removing it sets our queuePosition to the next node already
			// we will wrap if needed, as queuePosition may be pointing past the end of the slice now
			rrs.wrapQueuePositionIndex(node, false)
		}

		// delete child node from its parent's queueMap
		delete(node.queueMap, childName)
	} else {
		rrs.wrapQueuePositionIndex(node, true)
	}
}
