// SPDX-License-Identifier: AGPL-3.0-only

package queue

type QuerierWorkerQueuePriorityAlgo struct {
	currentQuerierWorker  int
	currentNodeOrderIndex int
	nodeOrder             []string
	nodeCounts            map[string]int
	nodesChecked          int
}

func NewQuerierWorkerQueuePriorityAlgo() *QuerierWorkerQueuePriorityAlgo {
	return &QuerierWorkerQueuePriorityAlgo{
		nodeCounts: make(map[string]int),
	}
}

func (qa *QuerierWorkerQueuePriorityAlgo) SetCurrentQuerierWorker(workerID int) {
	qa.currentQuerierWorker = workerID
	if len(qa.nodeOrder) == 0 {
		qa.currentNodeOrderIndex = 0
	} else {
		qa.currentNodeOrderIndex = int(workerID) % len(qa.nodeOrder)
	}
}

func (qa *QuerierWorkerQueuePriorityAlgo) wrapCurrentNodeOrderIndex(increment bool) {
	if increment {
		qa.currentNodeOrderIndex++
	}

	if qa.currentNodeOrderIndex >= len(qa.nodeOrder) {
		qa.currentNodeOrderIndex = 0
	}
}

func (qa *QuerierWorkerQueuePriorityAlgo) checkedAllNodes() bool {
	return qa.nodesChecked == len(qa.nodeOrder)
}

func (qa *QuerierWorkerQueuePriorityAlgo) addChildNode(parent, child *Node) {
	// add child node to its parent's queueMap
	parent.queueMap[child.Name()] = child

	// add child node to the global node order if it did not already exist
	if qa.nodeCounts[child.Name()] == 0 {
		if qa.currentNodeOrderIndex == 0 {
			// special case; since we are at the beginning of the order,
			// only a simple append is needed to add the new node to the end,
			// which also creates a more intuitive initial order for tests
			qa.nodeOrder = append(qa.nodeOrder, child.Name())
		} else {
			// insert into the order behind current child queue index
			// to prevent the possibility of new nodes continually jumping the line
			qa.nodeOrder = append(
				qa.nodeOrder[:qa.currentNodeOrderIndex],
				append(
					[]string{child.Name()},
					qa.nodeOrder[qa.currentNodeOrderIndex:]...,
				)...,
			)
			// since the new node was inserted into the order behind the current node,
			// the currentNodeOrderIndex must be pushed forward to remain pointing at the same node
			qa.wrapCurrentNodeOrderIndex(true)
		}
	}

	// add child node to global nodeCounts
	qa.nodeCounts[child.Name()]++
}

func (qa *QuerierWorkerQueuePriorityAlgo) dequeueSelectNode(node *Node) (*Node, bool) {
	currentNodeName := qa.nodeOrder[qa.currentNodeOrderIndex]
	if node, ok := node.queueMap[currentNodeName]; ok {
		qa.nodesChecked++
		return node, qa.checkedAllNodes()
	}
	return nil, qa.checkedAllNodes()
}

func (qa *QuerierWorkerQueuePriorityAlgo) dequeueUpdateState(node *Node, dequeuedFrom *Node) {
	// if the child node is nil, we haven't done anything to the tree; return early
	if dequeuedFrom == nil {
		return
	}

	// if the child is empty, we should delete it
	if dequeuedFrom != node && dequeuedFrom.IsEmpty() {
		childName := dequeuedFrom.Name()

		// decrement the global nodeCounts
		qa.nodeCounts[childName]--

		// only delete from global nodeOrder if the global nodeCount is now zero
		// meaning there are no nodes with this name remaining in the tree
		if qa.nodeCounts[childName] == 0 {
			for idx, name := range qa.nodeOrder {
				if name == childName {
					qa.nodeOrder = append(qa.nodeOrder[:idx], qa.nodeOrder[idx+1:]...)
					break
				}
			}
			// we do not need to increment currentNodeOrderIndex
			// the node removed is always the node pointed to by currentNodeOrderIndex
			// so removing it sets our currentNodeOrderIndex to the next node already
			// we will wrap if needed, as currentNodeOrderIndex may be pointing past the end of the slice now
			qa.wrapCurrentNodeOrderIndex(false)
		}

		// delete child node from its parent's queueMap
		delete(node.queueMap, childName)
	}

	// reset state after successful dequeue
	qa.nodesChecked = 0
}
