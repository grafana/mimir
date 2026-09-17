// SPDX-License-Identifier: AGPL-3.0-only

package tree

import (
	"slices"
)

// ConsumerWorkerQueuePriorityAlgo implements QueuingAlgorithm by mapping worker IDs to a queue node to prioritize.
// Consumer-workers' prioritized queue nodes are calculated by the integer WorkerID % len(nodeOrder).
// This distribution of workers across query component subtrees ensures that when one query component is experiencing
// high latency about 25% of consumer-workers continue prioritizing queries for unaffected components.
//
// This significantly outperforms the previous round-robin approach which simply rotated through the node order.
// Although a vanilla round-robin algorithm will select a given query-component node 1 / 4 of the time,
// in situations of high latency on a query component, the utilization of the consumer-worker connections
// as measured by inflight query processing time will grow asymptotically to be dominated by the slow query component.
//
// There are 4 possible query components: "ingester", "store-gateway", "ingester-and-store-gateway", and "unknown".
// When all 4 queue nodes exist, approximately 1 / 4 of the consumer-workers are prioritized to each queue node.
// This algorithm requires a minimum of 4 consumer-workers per consumer to prevent queue starvation.
// The minimum is enforced in the consumers by overriding -querier.max-concurrent if necessary.
//
// MultiAlgorithmTreeQueue always deletes empty leaf nodes and nodes with no children after a dequeue operation,
// and only recreates the queue nodes when a new query request is enqueued which requires that path through the tree.
// ConsumerWorkerQueuePriorityAlgo responds by removing or re-adding the query component nodes to the nodeOrder.
// This has two implications for the distribution of workers across queue nodes:
//  1. The modulo operation may modulo the worker ID by 1, 2, 3, or 4 depending on the number of node types
//     currently present in the nodeOrder, which can change which node a worker ID is prioritized for.
//  2. The nodeOrder changes as queues are deleted and re-created, so the worker ID-to-node mapping changes
//     as the random enqueue order places query component nodes in different positions in the order.
//
// These changes in nodeOrder guarantee that when the number of consumer-workers is not evenly divisible
// by the number of query component nodes, through the randomized changes in nodeOrder over time, the workers
// are more evenly distributed across query component nodes than if length and order of the nodes were fixed.
//
// A given worker ID is prioritized to *start* at a given queue node, but is not assigned strictly to that node.
// During any period without change to the nodeOrder, the same worker ID consistently starts at the same queue node,
// but moves on to other nodes if it cannot dequeue a request from the subtree of its first prioritized queue node.
// Continuing to search through other query-component nodes and their subtrees minimizes idle consumer-worker capacity.
//
// A consumer-worker can process queries for nodes it has not prioritized when this QueuingAlgorithm is applied at the
// highest layer of the tree and the tenant-consumer-shuffle-shard QueuingAlgorithm applied at the second layer of the
// tree. If shuffle-sharding is enabled, a consumer-worker that prioritizes ingester-only queries may not find
// ingester-only queries for any tenant it is assigned to, and move on to the next query component subtree. E.g.:
//
//  1. This algorithm has nodeOrder: ["ingester", "store-gateway", "ingester-and-store-gateway", "unknown"].
//
//  2. A consumer-worker with WorkerID 0 requests to dequeue; it prioritizes the "ingester" queue node.
//
//  3. The dequeue operation attempts to dequeue first from the child nodes of the "ingester" node,
//     where each child node is a tenant-specific queue of ingester-only queries. The tenantConsumerAssignments
//     QueuingAlgorithm checks if any of its tenant queue nodes is sharded to this consumer, and finds none.
//
//  4. The dequeue operation walks back up to the ConsumerWorkerQueuePriorityAlgo level, not having dequeued anything.
//     The ConsumerWorkerQueuePriorityAlgo moves on and selects the next query-component node in the nodeOrder,
//     and recurs again to search that next subtree for tenant queue nodes sharded to this consumer, from step 3, etc.,
//     until a dequeue-able tenant queue node is found, or every query component node subtree has been exhausted.
type ConsumerWorkerQueuePriorityAlgo struct {
	currentConsumerWorker int
	currentNodeOrderIndex int
	nodeOrder             []string
	nodeCounts            map[string]int
}

func NewConsumerWorkerQueuePriorityAlgo() *ConsumerWorkerQueuePriorityAlgo {
	return &ConsumerWorkerQueuePriorityAlgo{
		nodeCounts: make(map[string]int),
	}
}

func (qa *ConsumerWorkerQueuePriorityAlgo) setup(dequeueArgs *DequeueArgs) {
	qa.currentConsumerWorker = dequeueArgs.WorkerID
	if len(qa.nodeOrder) == 0 {
		qa.currentNodeOrderIndex = 0
	} else {
		qa.currentNodeOrderIndex = qa.currentConsumerWorker % len(qa.nodeOrder)
	}
}

func (qa *ConsumerWorkerQueuePriorityAlgo) wrapCurrentNodeOrderIndex(increment bool) {
	if increment {
		qa.currentNodeOrderIndex++
	}

	if qa.currentNodeOrderIndex >= len(qa.nodeOrder) {
		qa.currentNodeOrderIndex = 0
	}
}

func (qa *ConsumerWorkerQueuePriorityAlgo) addChildNode(parent, child *Node) {
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
			qa.nodeOrder = slices.Insert(qa.nodeOrder, qa.currentNodeOrderIndex, child.Name())
			// since the new node was inserted into the order behind the current node,
			// the currentNodeOrderIndex must be pushed forward to remain pointing at the same node
			qa.wrapCurrentNodeOrderIndex(true)
		}
	}

	// add child node to global nodeCounts
	qa.nodeCounts[child.Name()]++
}

func (qa *ConsumerWorkerQueuePriorityAlgo) dequeueSelectNode(node *Node) *Node {
	currentNodeName := qa.nodeOrder[qa.currentNodeOrderIndex]
	if childNode, ok := node.queueMap[currentNodeName]; ok {
		return childNode
	}
	return nil
}

func (qa *ConsumerWorkerQueuePriorityAlgo) dequeueUpdateState(node *Node, dequeuedFrom *Node) {
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
			childIndex := slices.Index(qa.nodeOrder, childName)
			if childIndex != -1 {
				qa.nodeOrder = slices.Delete(qa.nodeOrder, childIndex, childIndex+1)
				// we do not need to increment currentNodeOrderIndex
				// the node removed is always the node pointed to by currentNodeOrderIndex
				// so removing it sets our currentNodeOrderIndex to the next node already
				// we will wrap if needed, as currentNodeOrderIndex may be pointing past the end of the slice now
				qa.wrapCurrentNodeOrderIndex(false)
			}
		}

		// delete child node from its parent's queueMap
		delete(node.queueMap, childName)
	}
}
