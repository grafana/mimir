package queue

type DequeueAlgorithm interface {
	addChildNode(*Node, *Node)
	deleteChildNode(*Node, *Node) bool
	dequeueGetNode(*Node) (*Node, bool)
	dequeueUpdateState(*Node, any, bool)
}

type dequeueGetNodeFunc func(*Node) (*Node, bool)
type dequeueUpdateStateFunc func(*Node, any, bool)

type dequeueOps struct {
	getNode     dequeueGetNodeFunc
	updateState dequeueUpdateStateFunc
}

type roundRobinState struct {
}

func (rrs *roundRobinState) addChildNode(parent, child *Node) {
	// add childNode to n.queueMap
	parent.queueMap[child.Name()] = child

	// add childNode to n.queueOrder before the current position, update n.queuePosition to current element
	if parent.queuePosition <= localQueueIndex {
		parent.queueOrder = append(parent.queueOrder, child.Name())
	} else {
		parent.queueOrder = append(parent.queueOrder[:parent.queuePosition], append([]string{child.Name()}, parent.queueOrder[parent.queuePosition:]...)...)
		parent.queuePosition++ // keep position pointed at same element
	}
}

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

func (rrs *roundRobinState) dequeueGetNode(n *Node) (*Node, bool) {
	// advance the queue position for this dequeue
	n.queuePosition++
	if n.queuePosition >= len(n.queueOrder) {
		n.queuePosition = localQueueIndex
	}

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

func (rrs *roundRobinState) dequeueUpdateState(n *Node, v any, deletedNode bool) {
	if v != nil {
		n.childrenChecked = 0
	} else {
		n.childrenChecked++
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
