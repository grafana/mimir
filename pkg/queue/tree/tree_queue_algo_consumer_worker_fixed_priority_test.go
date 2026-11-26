// SPDX-License-Identifier: AGPL-3.0-only

package tree

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	ingesterNode                = "ingester"
	storeGatewayNode            = "store-gateway"
	ingesterAndStoreGatewayNode = "ingester-and-store-gateway"
	unknownNode                 = "unknown"
)

func newConsumerWorkerFixedPriorityAlgoForTest() QueuingAlgorithm {
	return NewConsumerWorkerFixedPriorityAlgo([]string{ingesterNode, storeGatewayNode, ingesterAndStoreGatewayNode, unknownNode})
}

// An ingester request completes fast.
// While the store-gateways are slow, a store-gateway request keeps its consumer-worker for many seconds.
// A consumer-worker that prioritizes the ingester node must keep dequeuing from it while ingester requests wait.
// Its priority node must not change when the tree deletes the empty ingester node and creates it again.
func TestConsumerWorkerFixedPriority_IngesterWorkerStaysOnIngesterNode(t *testing.T) {
	tree, err := NewTree(newConsumerWorkerFixedPriorityAlgoForTest())
	require.NoError(t, err)

	require.NoError(t, tree.EnqueueBackByPath(QueuePath{ingesterNode}, "ing-1"))
	for _, obj := range []string{"sg-1", "sg-2", "sg-3"} {
		require.NoError(t, tree.EnqueueBackByPath(QueuePath{storeGatewayNode}, obj))
	}

	// Worker 0 prioritizes the ingester node.
	path, obj := tree.Dequeue(&DequeueArgs{WorkerID: 0})
	require.Equal(t, QueuePath{ingesterNode}, path)
	require.Equal(t, "ing-1", obj)

	// The tree deleted the empty ingester node.
	// The next ingester request creates it again.
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{ingesterNode}, "ing-2"))

	path, obj = tree.Dequeue(&DequeueArgs{WorkerID: 0})
	require.Equal(t, QueuePath{ingesterNode}, path, "worker 0 dequeued %v while an ingester request waits", obj)
	require.Equal(t, "ing-2", obj)

	// Worker 1 prioritizes the store-gateway node.
	path, obj = tree.Dequeue(&DequeueArgs{WorkerID: 1})
	require.Equal(t, QueuePath{storeGatewayNode}, path)
	require.Equal(t, "sg-1", obj)
}

// A consumer-worker whose priority node does not exist walks over the other nodes.
// The walk starts at WorkerID % the number of other nodes, and does not depend on the dequeues of other workers.
func TestConsumerWorkerFixedPriority_WalkStartsAtWorkerID(t *testing.T) {
	tree, err := NewTree(newConsumerWorkerFixedPriorityAlgoForTest())
	require.NoError(t, err)

	for _, obj := range []string{"ing-1", "ing-2"} {
		require.NoError(t, tree.EnqueueBackByPath(QueuePath{ingesterNode}, obj))
	}
	for _, obj := range []string{"sg-1", "sg-2"} {
		require.NoError(t, tree.EnqueueBackByPath(QueuePath{storeGatewayNode}, obj))
	}

	// Workers 2 and 3 prioritize the ingester-and-store-gateway node and the unknown node.
	// Neither node exists, so 2 other nodes exist.
	for i, tc := range []struct {
		workerID     int
		expectedPath QueuePath
	}{
		{workerID: 2, expectedPath: QueuePath{ingesterNode}},
		{workerID: 2, expectedPath: QueuePath{ingesterNode}},
		{workerID: 3, expectedPath: QueuePath{storeGatewayNode}},
		{workerID: 3, expectedPath: QueuePath{storeGatewayNode}},
	} {
		path, _ := tree.Dequeue(&DequeueArgs{WorkerID: tc.workerID})
		require.Equal(t, tc.expectedPath, path, "dequeue %d", i)
	}
}

// skipNodeAlgo is a RoundRobinState that dequeues nothing from the children of one node.
// It acts like a tenant algorithm that has no tenant for the consumer.
type skipNodeAlgo struct {
	RoundRobinState
	skip string
}

func (a *skipNodeAlgo) dequeueSelectNode(node *Node) *Node {
	if node.Name() == a.skip {
		return nil
	}
	return a.RoundRobinState.dequeueSelectNode(node)
}

// A consumer-worker whose priority node has nothing to dequeue for it walks to the other nodes.
func TestConsumerWorkerFixedPriority_WalkAfterPriorityNodeHasNothing(t *testing.T) {
	tree, err := NewTree(newConsumerWorkerFixedPriorityAlgoForTest(), &skipNodeAlgo{skip: ingesterNode})
	require.NoError(t, err)

	require.NoError(t, tree.EnqueueBackByPath(QueuePath{ingesterNode, "tenant-1"}, "ing-1"))
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{storeGatewayNode, "tenant-1"}, "sg-1"))

	// Worker 0 prioritizes the ingester node, which has nothing to dequeue for it.
	path, obj := tree.Dequeue(&DequeueArgs{WorkerID: 0})
	require.Equal(t, QueuePath{storeGatewayNode, "tenant-1"}, path)
	require.Equal(t, "sg-1", obj)
}

// visitAlgo records the name of each node that it selects from, and dequeues nothing.
type visitAlgo struct {
	RoundRobinState
	visits []string
}

func (a *visitAlgo) dequeueSelectNode(node *Node) *Node {
	a.visits = append(a.visits, node.Name())
	return nil
}

// The dequeue order is the priority node, if it exists, then the other nodes that exist.
// The walk takes them in the order of the fixed list, then in the order of their first enqueue.
// It starts at WorkerID % the number of other nodes.
func TestConsumerWorkerFixedPriority_DequeueOrder(t *testing.T) {
	const otherNode = "other"
	for _, tc := range []struct {
		nodes    []string
		workerID int
		expected []string
	}{
		{nodes: []string{ingesterNode, storeGatewayNode, ingesterAndStoreGatewayNode, unknownNode}, workerID: 0, expected: []string{ingesterNode, storeGatewayNode, ingesterAndStoreGatewayNode, unknownNode}},
		{nodes: []string{ingesterNode, storeGatewayNode, ingesterAndStoreGatewayNode, unknownNode}, workerID: 1, expected: []string{storeGatewayNode, ingesterAndStoreGatewayNode, unknownNode, ingesterNode}},
		{nodes: []string{ingesterNode, storeGatewayNode, ingesterAndStoreGatewayNode, unknownNode}, workerID: 2, expected: []string{ingesterAndStoreGatewayNode, unknownNode, ingesterNode, storeGatewayNode}},
		{nodes: []string{ingesterNode, storeGatewayNode, ingesterAndStoreGatewayNode, unknownNode}, workerID: 3, expected: []string{unknownNode, ingesterNode, storeGatewayNode, ingesterAndStoreGatewayNode}},
		// Worker 4 has the priority node of worker 0, but its walk starts at 4 % 3 = 1.
		{nodes: []string{ingesterNode, storeGatewayNode, ingesterAndStoreGatewayNode, unknownNode}, workerID: 4, expected: []string{ingesterNode, ingesterAndStoreGatewayNode, unknownNode, storeGatewayNode}},
		{nodes: []string{storeGatewayNode, ingesterAndStoreGatewayNode, ingesterNode}, workerID: 3, expected: []string{ingesterNode, storeGatewayNode, ingesterAndStoreGatewayNode}},
		{nodes: []string{storeGatewayNode, ingesterAndStoreGatewayNode, ingesterNode}, workerID: 7, expected: []string{storeGatewayNode, ingesterAndStoreGatewayNode, ingesterNode}},
		{nodes: []string{storeGatewayNode, ingesterAndStoreGatewayNode}, workerID: 0, expected: []string{storeGatewayNode, ingesterAndStoreGatewayNode}},
		{nodes: []string{storeGatewayNode, ingesterAndStoreGatewayNode}, workerID: 3, expected: []string{ingesterAndStoreGatewayNode, storeGatewayNode}},
		{nodes: []string{storeGatewayNode}, workerID: 0, expected: []string{storeGatewayNode}},
		{nodes: []string{otherNode, ingesterNode}, workerID: 0, expected: []string{ingesterNode, otherNode}},
		{nodes: []string{otherNode, ingesterNode}, workerID: 3, expected: []string{otherNode, ingesterNode}},
	} {
		visits := &visitAlgo{}
		tree, err := NewTree(newConsumerWorkerFixedPriorityAlgoForTest(), visits)
		require.NoError(t, err)
		for _, node := range tc.nodes {
			require.NoError(t, tree.EnqueueBackByPath(QueuePath{node, "tenant-1"}, node))
		}

		_, obj := tree.Dequeue(&DequeueArgs{WorkerID: tc.workerID})
		require.Nil(t, obj)
		require.Equal(t, tc.expected, visits.visits, "nodes %v, worker %d", tc.nodes, tc.workerID)
	}
}

// With querier shuffle sharding, the priority node can hold only tenants of other consumers.
// The consumer-worker then walks to a node that has a tenant for its consumer.
func TestConsumerWorkerFixedPriority_ShuffleSharding(t *testing.T) {
	tenants := NewTenantConsumerQueuingAlgorithm()
	tree, err := NewTree(newConsumerWorkerFixedPriorityAlgoForTest(), tenants)
	require.NoError(t, err)

	require.NoError(t, tree.EnqueueBackByPath(QueuePath{ingesterNode, "tenant-a"}, "ing-a"))
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{storeGatewayNode, "tenant-b"}, "sg-b"))
	tenants.SetConsumersForTenant("tenant-a", map[ConsumerID]struct{}{"consumer-2": {}})
	tenants.SetConsumersForTenant("tenant-b", map[ConsumerID]struct{}{"consumer-1": {}})

	// Worker 0 of consumer-1 prioritizes the ingester node, which has only a tenant of consumer-2.
	path, obj := tree.Dequeue(&DequeueArgs{ConsumerID: "consumer-1", WorkerID: 0, LastTenantIndex: -1})
	require.Equal(t, QueuePath{storeGatewayNode, "tenant-b"}, path)
	require.Equal(t, "sg-b", obj)
}

// BenchmarkConsumerWorkerFixedPriorityAlgo_Dequeue compares a dequeue with today's root algorithm and with fixed priority.
// Each dequeue is for worker 0 of consumer-1.
// Worker 0 has the ingester node as its fixed priority node.
func BenchmarkConsumerWorkerFixedPriorityAlgo_Dequeue(b *testing.B) {
	roots := []struct {
		name    string
		newAlgo func() QueuingAlgorithm
	}{
		{name: "today", newAlgo: func() QueuingAlgorithm { return NewConsumerWorkerQueuePriorityAlgo() }},
		{name: "fixed", newAlgo: newConsumerWorkerFixedPriorityAlgoForTest},
	}
	dequeueArgs := &DequeueArgs{ConsumerID: "consumer-1", WorkerID: 0, LastTenantIndex: newConsumerTenantIndex}

	for _, root := range roots {
		// The ingester queue is empty, so the tree has no ingester node.
		b.Run(root.name+"/priority_node_empty", func(b *testing.B) {
			tree, err := NewTree(root.newAlgo(), NewRoundRobinState())
			require.NoError(b, err)
			for i := range 100 {
				require.NoError(b, tree.EnqueueBackByPath(QueuePath{storeGatewayNode, "tenant-1"}, i))
			}
			benchmarkDequeue(b, tree, dequeueArgs)
		})

		b.Run(root.name+"/priority_node_has_requests", func(b *testing.B) {
			tree, err := NewTree(root.newAlgo(), NewRoundRobinState())
			require.NoError(b, err)
			for i := range 100 {
				require.NoError(b, tree.EnqueueBackByPath(QueuePath{ingesterNode, "tenant-1"}, i))
				require.NoError(b, tree.EnqueueBackByPath(QueuePath{storeGatewayNode, "tenant-1"}, i))
			}
			benchmarkDequeue(b, tree, dequeueArgs)
		})

		// With querier shuffle sharding, the ingester node holds only tenants of consumer-2.
		// The only tenant of consumer-1 is in the store-gateway node.
		// Today's root algorithm does not walk, so its dequeue finds nothing.
		for _, tenants := range []int{10, 100, 1000} {
			b.Run(fmt.Sprintf("%s/priority_node_other_consumers_%d_tenants", root.name, tenants), func(b *testing.B) {
				tenantAlgo := NewTenantConsumerQueuingAlgorithm()
				tree, err := NewTree(root.newAlgo(), tenantAlgo)
				require.NoError(b, err)
				for i := range tenants {
					tenantID := fmt.Sprintf("other-%d", i)
					require.NoError(b, tree.EnqueueBackByPath(QueuePath{ingesterNode, tenantID}, i))
					tenantAlgo.SetConsumersForTenant(tenantID, map[ConsumerID]struct{}{"consumer-2": {}})
				}
				for i := range 100 {
					require.NoError(b, tree.EnqueueBackByPath(QueuePath{storeGatewayNode, "tenant-1"}, i))
				}
				tenantAlgo.SetConsumersForTenant("tenant-1", map[ConsumerID]struct{}{"consumer-1": {}})
				benchmarkDequeue(b, tree, dequeueArgs)
			})
		}
	}
}

// benchmarkDequeue enqueues each dequeued item back, so the tree keeps its shape.
func benchmarkDequeue(b *testing.B, tree Tree, dequeueArgs *DequeueArgs) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path, item := tree.Dequeue(dequeueArgs)
		if item == nil {
			continue
		}
		if err := tree.EnqueueBackByPath(path, item); err != nil {
			b.Fatal(err)
		}
	}
}
