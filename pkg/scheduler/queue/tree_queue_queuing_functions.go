package queue

// A queueingAlgorithm determines the order that nodes are inserted into the tree,
// and sets q.currentChildQueueIndex to the index of the next queue which will be dequeued from.
type queueingAlgorithm string

const (
	roundRobin = queueingAlgorithm("round-robin")
)

// addNode returns a function for use in getOrAddNode. The returned function should place the child node into
// the parent node's childQueueOrder according to the queueingAlgorithm in use.
func addNode(e queueingAlgorithm) func(parent *TreeQueue, child *TreeQueue) {
	switch e {
	case roundRobin:
		return addRoundRobinNode
	default:
		// We should never be able to hit this; nodes should only be created with implemented dequeue algorithms
		panic("unrecognized queueing algorithm")

	}
}

// TODO (casie): These enqueue/dequeue functions feel like they should be pointer receivers that take a child argument...
func addRoundRobinNode(parent *TreeQueue, child *TreeQueue) {
	// add new child queue to ordered list for round-robining;
	// in order to maintain round-robin order as nodes are created and deleted,
	// the new child queue should be inserted directly before the current child
	// queue index, essentially placing the new node at the end of the line
	if parent.currentChildQueueIndex == localQueueIndex {
		// special case; cannot slice into childQueueOrder with index -1
		// place at end of slice, which is the last slot before the local queue slot
		parent.childQueueOrder = append(parent.childQueueOrder, child.name)
	} else {
		// insert into order behind current child queue index
		parent.childQueueOrder = append(
			parent.childQueueOrder[:parent.currentChildQueueIndex],
			append(
				[]string{child.name},
				parent.childQueueOrder[parent.currentChildQueueIndex:]...,
			)...,
		)
		// update current child queue index to its new place in the expanded slice
		parent.currentChildQueueIndex++
	}
}

// setNextIndex returns a function for use in Dequeue. The returned function should
// set q.currentChildQueueIndex to the appropriate next index to be dequeued.
func setNextIndex(d queueingAlgorithm) func(q *TreeQueue) {
	switch d {
	case roundRobin:
		return roundRobinNextIndex
	default:
		// We should never be able to hit this; nodes should only be created with implemented dequeue algorithms
		panic("unrecognized queueing algorithm")

	}
}

// TODO (casie): Again, passing the node as an argument feels a little silly...
func roundRobinNextIndex(q *TreeQueue) {
	q.currentChildQueueIndex++
	q.wrapIndex()
}
