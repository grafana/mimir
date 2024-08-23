// SPDX-License-Identifier: AGPL-3.0-only

package queue

// QuerierWorkerQueuePriorityAlgo implements QueuingAlgorithm by mapping worker IDs to a queue node to prioritize.
// Querier-workers' prioritized queue nodes are calculated by the integer workerID % len(nodeOrder).
//
// Purpose:
// This algorithm is intended to ensure that querier-workers are balanced across nodes in the queue selection tree.
// While it can serve to balance consumers across request queue nodes which have been partitioned by any criteria,
// its original purpose is to balance querier-workers across queues partitioned a query's expected query component`.
//
// There are four possible query components: "ingester", "store-gateway", "ingester-and-store-gateway", and "unknown".
// With all four possible query component queue nodes active, the modulo operation on the worker IDs
// will distribute approximately 1 / 4 of the querier-workers to each queue node.
// A querier-worker ID which is mapped to prioritize the "ingester" queue node will always start there
// and attempt first to work on a query which only requires the ingesters to complete.
//
// By splitting the queues by query component we can ensure that the approximately 1 / 4 of querier-workers are
// "reserved" for a query component even when the other query component is experiencing high latency.
// This reservation ensures that the querier-worker connections can continue to process queries
// which do not need to utilize the query component experiencing high latency.
//
// Performance:
// This significantly outperforms the previous round-robin algorithm which simply rotated through the node order
// (see TestMultiDimensionalQueueAlgorithmSlowConsumerEffects benchmark outputs for comparison).
// Although a vanilla round-robin algorithm will select a given query-component node 1 / 4 of the time,
// in situations of high latency on a query component that one slow query component will still grow asymptotically
// to dominate the utilization the querier-worker connections, as measured by inflight query processing time.
//
// Implementation Details & Assumptions:
// The MultiAlgorithmTreeQueue which utilizes this and other QueuingAlgorithm implementations always deletes
// nodes for paths through the tree which lead to an empty leaf node after a dequeue operation.
// Nodes for paths through the tree are then re-created when a new request is enqueued which requires that path.
// This means that a tree will not always have all 4 node types for the 4 possible query component assignments.
//
// This has two implications for the distribution of workers across queue nodes:
//  1. The modulo operation may modulo the worker ID by 1, 2, 3, or 4 depending on the number of node types
//     currently present in the node order, which can change which node a worker ID is prioritized for.
//  2. The node order changes as queues are deleted and re-created, so the worker ID to node mapping will change
//     as the essentially random enqueue order places query component nodes in different positions in the order.
//
// We consider this a desirable property, as it ensures that a number of querier-workers which is not evenly
// divisible by the number of query component nodes will, through the randomized changes in nodeOrder over time,
// be distributed more evenly across the nodes than if length and order of the nodeOrder were fixed.
//
// Minimizing Idle Querier-Worker Capacity:
// We say the queue nodes are "prioritized" for a worker rather than "assigned" to a worker
// because the same worker ID is mapped to *start* at a certain queue node, but will move on to other nodes
// if it cannot dequeue a request from any of the child queue nodes of its first prioritized queue node.
// This is possible when this queue algorithm is placed at the highest layer of the tree and
// the tenant-querier-shuffle-shard queue algorithm is placed at the second, leaf layer of the tree.
// Ex:
//
//  1. The QuerierWorkerQueuePriorityAlgo begins with a nodeOrder of:
//     ["ingester", "store-gateway", "ingester-and-store-gateway", "unknown"].
//
//  2. A querier-worker with workerID 0 requests to dequeue and is mapped to start with the "ingester" queue node.
//
//  3. The tree traversal algorithm recurs down to select child queue nodes of the "ingester" node,
//     where each child queue node is a non-empty tenant-specific queue of ingester-only queries.
//     The tenant-querier-shuffle-shard queue algorithm checks each tenant node for if it is sharded to this querier.
//
//  4. (a) The first tenant queue node found which is sharded to this querier will be dequeued from, and we are done.
//
//  4. (b) Otherwise, if none of those tenants are sharded to this querier, the tree traversal algorithm will return
//     back up to the parent level and ask the QuerierWorkerQueuePriorityAlgo to select its next node.
//
//  5. The QuerierWorkerQueuePriorityAlgo will select the next node in the nodeOrder, "store-gateway".
//     Then steps 3 and 4 repeat as necessary until a request is dequeued for the querier-worker.
//
// This process of continuing to search for requests to dequeue helps prevent querier-worker capacity from sitting idle
// when there are no requests to dequeue for the query component node that the querier-worker was originally mapped to.
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
		qa.currentNodeOrderIndex = workerID % len(qa.nodeOrder)
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
