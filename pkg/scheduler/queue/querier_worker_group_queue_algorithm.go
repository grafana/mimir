// SPDX-License-Identifier: AGPL-3.0-only

package queue

type querierWorkerPrioritizationQueueAlgo struct {
	currentQuerierWorker  int32
	currentNodeOrderIndex int
	nodeOrder             []string
	nodesChecked          int
}

func (qa *querierWorkerPrioritizationQueueAlgo) SetCurrentQuerierWorker(workerID int32) {
	qa.currentQuerierWorker = workerID
	if len(qa.nodeOrder) == 0 {
		qa.currentNodeOrderIndex = 0
	} else {
		qa.currentNodeOrderIndex = int(workerID) % len(qa.nodeOrder)
	}
}

func (qa *querierWorkerPrioritizationQueueAlgo) incrementWrapCurrentNodeOrderIndex() {
	qa.currentNodeOrderIndex++
	if qa.currentNodeOrderIndex >= len(qa.nodeOrder) {
		qa.currentNodeOrderIndex = 0
	}
}

func (qa *querierWorkerPrioritizationQueueAlgo) checkedAllNodes() bool {
	return qa.nodesChecked == len(qa.nodeOrder)
}

func (qa *querierWorkerPrioritizationQueueAlgo) addChildNode(parent, child *Node) {
	// add childNode to n.queueMap
	parent.queueMap[child.Name()] = child

	if qa.currentNodeOrderIndex == localQueueIndex {
		// special case; cannot slice into nodeOrder with index -1
		// place at end of slice, which is the last slot before the local queue slot
		qa.nodeOrder = append(qa.nodeOrder, child.Name())
	} else if qa.currentNodeOrderIndex == 0 {
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
		// update current child queue index to its new place in the expanded slice
		qa.incrementWrapCurrentNodeOrderIndex()
	}
}

func (qa *querierWorkerPrioritizationQueueAlgo) dequeueSelectNode(node *Node) (*Node, bool) {
	currentNodeName := qa.nodeOrder[qa.currentNodeOrderIndex]
	if node, ok := node.queueMap[currentNodeName]; ok {
		qa.nodesChecked++
		return node, qa.checkedAllNodes()
	}
	return nil, qa.checkedAllNodes()
}

func (qa *querierWorkerPrioritizationQueueAlgo) dequeueUpdateState(node *Node, dequeuedFrom *Node) {
	// if the child node is nil, we haven't done anything to the tree; return early
	if dequeuedFrom == nil {
		return
	}

	// if the child is empty, we should delete it, but not increment queue position, since removing an element
	// from queueOrder sets our position to the next element already.
	if dequeuedFrom != node && dequeuedFrom.IsEmpty() {
		childName := dequeuedFrom.Name()
		delete(node.queueMap, childName)
		for idx, name := range qa.nodeOrder {
			if name == childName {
				qa.nodeOrder = append(qa.nodeOrder[:idx], qa.nodeOrder[idx+1:]...)
				break
			}
		}
		if qa.currentNodeOrderIndex >= len(qa.nodeOrder) {
			qa.currentNodeOrderIndex = 0
		}
	}

	qa.nodesChecked = 0
}
