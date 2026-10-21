// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/user_queues_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queue

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/queue/tree"
)

func (qb *queueBroker) enqueueObjectsForTests(tenantID string, numObjects int) error {
	for i := 0; i < numObjects; i++ {
		req := &tenantItem{
			tenantID: tenantID,
			item:     fmt.Sprintf("%v: object-%v", tenantID, i),
		}
		path := qb.queuePath(req)
		if err := qb.tree.EnqueueBackByPath(path, req); err != nil {
			return err
		}
	}
	return nil
}

func buildExpectedObject(tenantID string, num int) *tenantItem {
	return &tenantItem{
		tenantID: tenantID,
		item:     fmt.Sprintf("%v: object-%v", tenantID, num),
	}
}

type dequeueVal struct {
	item   *tenantItem
	tenant *queueTenant
}

func assertExpectedValuesOnDequeue(t *testing.T, qb *queueBroker, lastTenantIndex int, consumerID string, expectedVals []dequeueVal) int {
	var req *tenantItem
	var tenant *queueTenant
	var err error

	for _, expected := range expectedVals {
		req, tenant, lastTenantIndex, err = qb.dequeueItemForConsumer(&ConsumerWorkerDequeueRequest{
			ConsumerWorkerConn: &ConsumerWorkerConn{ConsumerID: consumerID},
			lastTenantIndex:    TenantIndex{last: lastTenantIndex},
		})
		assert.Equal(t, expected.item, req)
		assert.Equal(t, expected.tenant, tenant)
		assert.NoError(t, err)
	}
	return lastTenantIndex
}

// TestQueues_NoShuffleSharding tests dequeueing of objects for different consumers, where any consumer can
// handle queries for any tenant, as tenant queues are added and removed

func TestQueues_NoShuffleSharding(t *testing.T) {
	qb := newQueueBroker(0, 0)
	assert.NotNil(t, qb)
	assert.NoError(t, isConsistent(qb))

	qb.addConsumerWorkerConn(NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-1"))
	qb.addConsumerWorkerConn(NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-2"))

	req, tenant, lastTenantIndexConsumerOne, err := qb.dequeueItemForConsumer(&ConsumerWorkerDequeueRequest{
		ConsumerWorkerConn: &ConsumerWorkerConn{ConsumerID: "consumer-1"},
		lastTenantIndex:    TenantIndex{-1},
	})
	assert.Nil(t, req)
	assert.Nil(t, tenant)
	assert.NoError(t, err)

	// Add tenant queues: tenant "one"
	err = qb.tenantConsumerAssignments.createOrUpdateTenant("one", 0)
	assert.NoError(t, err)

	tenantOne := qb.tenantConsumerAssignments.tenantsByID["one"]

	// Queue enough objects for tenant "one"
	err = qb.enqueueObjectsForTests(tenantOne.tenantID, 10)
	assert.NoError(t, err)
	assert.NoError(t, isConsistent(qb))

	//queuePathOne := QueuePath{"root", "one"}
	expectedDequeueVals := []dequeueVal{
		{buildExpectedObject(tenantOne.tenantID, 0), tenantOne},
		{buildExpectedObject(tenantOne.tenantID, 1), tenantOne},
	}
	lastTenantIndexConsumerOne = assertExpectedValuesOnDequeue(t, qb, lastTenantIndexConsumerOne, "consumer-1", expectedDequeueVals)

	// Add tenant two
	err = qb.tenantConsumerAssignments.createOrUpdateTenant("two", 0)
	assert.NoError(t, err)

	tenantTwo := qb.tenantConsumerAssignments.tenantsByID["two"]

	err = qb.enqueueObjectsForTests(tenantTwo.tenantID, 10)
	assert.NoError(t, err)
	assert.NoError(t, isConsistent(qb))

	expectedDequeueVals = []dequeueVal{
		{buildExpectedObject(tenantTwo.tenantID, 0), tenantTwo},
		{buildExpectedObject(tenantOne.tenantID, 2), tenantOne},
		{buildExpectedObject(tenantTwo.tenantID, 1), tenantTwo},
		{buildExpectedObject(tenantOne.tenantID, 3), tenantOne},
	}

	lastTenantIndexConsumerOne = assertExpectedValuesOnDequeue(t, qb, lastTenantIndexConsumerOne, "consumer-1", expectedDequeueVals)

	expectedDequeueVals = []dequeueVal{
		{buildExpectedObject(tenantOne.tenantID, 4), tenantOne},
		{buildExpectedObject(tenantTwo.tenantID, 2), tenantTwo},
		{buildExpectedObject(tenantOne.tenantID, 5), tenantOne},
	}
	lastTenantIndexConsumerTwo := assertExpectedValuesOnDequeue(t, qb, -1, "consumer-2", expectedDequeueVals)

	// [one two three]
	// confirm fifo by adding a third tenant queue and iterating to it
	err = qb.tenantConsumerAssignments.createOrUpdateTenant("three", 0)
	assert.NoError(t, err)

	tenantThree := qb.tenantConsumerAssignments.tenantsByID["three"]

	err = qb.enqueueObjectsForTests(tenantThree.tenantID, 10)
	assert.NoError(t, err)
	assert.NoError(t, isConsistent(qb))

	// lastTenantIndexConsumerOne was 0 (tenantOne) for consumer-1; appending
	expectedDequeueVals = []dequeueVal{
		{buildExpectedObject(tenantTwo.tenantID, 3), tenantTwo},
		{buildExpectedObject(tenantThree.tenantID, 0), tenantThree},
		{buildExpectedObject(tenantOne.tenantID, 6), tenantOne},
	}

	lastTenantIndexConsumerOne = assertExpectedValuesOnDequeue(t, qb, lastTenantIndexConsumerOne, "consumer-1", expectedDequeueVals)

	// Remove one: ["" two three]
	qb.removeTenantQueue("one")
	assert.NoError(t, isConsistent(qb))

	expectedDequeueVals = []dequeueVal{
		{buildExpectedObject(tenantTwo.tenantID, 4), tenantTwo},
		{buildExpectedObject(tenantThree.tenantID, 1), tenantThree},
		{buildExpectedObject(tenantTwo.tenantID, 5), tenantTwo},
	}
	lastTenantIndexConsumerOne = assertExpectedValuesOnDequeue(t, qb, lastTenantIndexConsumerOne, "consumer-1", expectedDequeueVals)

	// "four" is added at the beginning of the list: [four two three]
	err = qb.tenantConsumerAssignments.createOrUpdateTenant("four", 0)
	assert.NoError(t, err)

	tenantFour := qb.tenantConsumerAssignments.tenantsByID["four"]

	err = qb.enqueueObjectsForTests(tenantFour.tenantID, 10)
	assert.NoError(t, err)
	assert.NoError(t, isConsistent(qb))

	expectedDequeueVals = []dequeueVal{
		{buildExpectedObject(tenantTwo.tenantID, 6), tenantTwo},
		{buildExpectedObject(tenantThree.tenantID, 2), tenantThree},
		{buildExpectedObject(tenantFour.tenantID, 0), tenantFour},
		{buildExpectedObject(tenantTwo.tenantID, 7), tenantTwo},
	}

	_ = assertExpectedValuesOnDequeue(t, qb, lastTenantIndexConsumerTwo, "consumer-2", expectedDequeueVals)

	// Remove two: [four "" three]
	qb.removeTenantQueue("two")
	assert.NoError(t, isConsistent(qb))

	expectedDequeueVals = []dequeueVal{
		{buildExpectedObject(tenantThree.tenantID, 3), tenantThree},
		{buildExpectedObject(tenantFour.tenantID, 1), tenantFour},
		{buildExpectedObject(tenantThree.tenantID, 4), tenantThree},
	}
	lastTenantIndexConsumerOne = assertExpectedValuesOnDequeue(t, qb, lastTenantIndexConsumerOne, "consumer-1", expectedDequeueVals)

	// Remove three: [four ""] )
	qb.removeTenantQueue("three")
	assert.NoError(t, isConsistent(qb))

	// Remove four: []
	qb.removeTenantQueue("four")
	assert.NoError(t, isConsistent(qb))

	req, tenant, _, err = qb.dequeueItemForConsumer(&ConsumerWorkerDequeueRequest{
		ConsumerWorkerConn: &ConsumerWorkerConn{ConsumerID: "consumer-1"},
		lastTenantIndex:    TenantIndex{lastTenantIndexConsumerOne},
	},
	)
	assert.Nil(t, req)
	assert.Nil(t, tenant)
	assert.NoError(t, err)
}

func TestQueuesRespectMaxTenantQueueSizeWithSubQueues(t *testing.T) {
	maxTenantQueueSize := 100
	qb := newQueueBroker(maxTenantQueueSize, 0)
	additionalQueueDimensions := map[int][]string{
		0: {UnknownDimension},
		1: {ingesterQueueDimension},
		2: {storeGatewayQueueDimension},
		3: {ingesterAndStoreGatewayQueueDimension},
	}
	req := &testItem{
		UserID: "tenant-1",
	}

	// build queue evenly with either no additional queue dimension or one of 3 additional dimensions
	for i := 0; i < len(additionalQueueDimensions); i++ {
		for j := 0; j < maxTenantQueueSize/len(additionalQueueDimensions); j++ {
			req.AdditionalQueueDimensions = additionalQueueDimensions[i]
			tenantReq := &tenantItem{
				tenantID:       "tenant-1",
				queueDimension: req.ExpectedQueryComponentName(),
				item:           req,
			}
			err := qb.enqueueItemBack(tenantReq, 0)
			assert.NoError(t, err)
		}
	}

	var itemCount int
	// build paths for each queue dimension and sum all items
	for _, addlQueueDim := range additionalQueueDimensions {
		var path tree.QueuePath
		// assert item count of tenant node and its subnodes
		path = append(append(path, addlQueueDim...), "tenant-1")
		if addlQueueDim == nil {
			path = qb.makeQueuePathForTests("tenant-1")
		}
		itemCount += qb.tree.GetNode(path).ItemCount()
	}
	assert.Equal(t, maxTenantQueueSize, itemCount)

	// assert equal distribution of queue items between 4 subnodes
	for _, v := range additionalQueueDimensions {
		var checkPath tree.QueuePath
		if v == nil {
			v = []string{UnknownDimension}
		}
		checkPath = append(append(checkPath, v...), "tenant-1")

		dimensionItemCount := qb.tree.GetNode(checkPath).ItemCount()
		assert.Equal(t, maxTenantQueueSize/len(additionalQueueDimensions), dimensionItemCount)
	}

	// assert error received when hitting a tenant's enqueue limit,
	// even though most of the requests are in the subqueues
	for _, additionalQueueDimension := range additionalQueueDimensions {
		// error should be received no matter if the enqueue attempt
		// is for the tenant queue or any of its subqueues
		req.AdditionalQueueDimensions = additionalQueueDimension
		tenantReq := &tenantItem{
			tenantID:       "tenant-1",
			queueDimension: req.ExpectedQueryComponentName(),
			item:           req,
		}
		err := qb.enqueueItemBack(tenantReq, 0)
		assert.ErrorIs(t, err, ErrTooManyRequests)
	}

	// dequeue a request
	qb.addConsumerWorkerConn(NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-1"))
	dequeuedTenantReq, _, _, err := qb.dequeueItemForConsumer(&ConsumerWorkerDequeueRequest{
		ConsumerWorkerConn: &ConsumerWorkerConn{ConsumerID: "consumer-1"},
		lastTenantIndex:    TenantIndex{-1},
	})
	assert.NoError(t, err)
	assert.NotNil(t, dequeuedTenantReq)

	tenantReq := &tenantItem{
		tenantID:       "tenant-1",
		queueDimension: req.ExpectedQueryComponentName(),
		item:           req,
	}
	// assert not hitting an error when enqueueing after dequeuing to below the limit
	err = qb.enqueueItemBack(tenantReq, 0)
	assert.NoError(t, err)

	// we then hit an error again, as we are back at the limit
	err = qb.enqueueItemBack(tenantReq, 0)
	assert.ErrorIs(t, err, ErrTooManyRequests)
}

func TestQueuesOnTerminatingConsumer(t *testing.T) {
	qb := newQueueBroker(0, 0)
	assert.NotNil(t, qb)
	assert.NoError(t, isConsistent(qb))

	qb.addConsumerWorkerConn(NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-1"))
	qb.addConsumerWorkerConn(NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-2"))

	// Add queues: [one two]
	err := qb.tenantConsumerAssignments.createOrUpdateTenant("one", 0)
	assert.NoError(t, err)
	err = qb.tenantConsumerAssignments.createOrUpdateTenant("two", 0)
	assert.NoError(t, err)

	err = qb.enqueueObjectsForTests("one", 10)
	assert.NoError(t, err)
	tenantOne := qb.tenantConsumerAssignments.tenantsByID["one"]

	err = qb.enqueueObjectsForTests("two", 10)
	assert.NoError(t, err)
	tenantTwo := qb.tenantConsumerAssignments.tenantsByID["two"]

	expectedDequeueVals := []dequeueVal{
		{buildExpectedObject(tenantOne.tenantID, 0), tenantOne},
		{buildExpectedObject(tenantTwo.tenantID, 0), tenantTwo},
		{buildExpectedObject(tenantOne.tenantID, 1), tenantOne},
		{buildExpectedObject(tenantTwo.tenantID, 1), tenantTwo},
	}
	qOneLastTenantIndex := assertExpectedValuesOnDequeue(t, qb, -1, "consumer-1", expectedDequeueVals)

	expectedDequeueVals = []dequeueVal{
		{buildExpectedObject(tenantOne.tenantID, 2), tenantOne},
		{buildExpectedObject(tenantTwo.tenantID, 2), tenantTwo},
		{buildExpectedObject(tenantOne.tenantID, 3), tenantOne},
		{buildExpectedObject(tenantTwo.tenantID, 3), tenantTwo},
	}
	qTwolastTenantIndex := assertExpectedValuesOnDequeue(t, qb, -1, "consumer-2", expectedDequeueVals)

	// After notify shutdown for consumer-2, it's expected to own no queue.
	qb.notifyConsumerShutdown("consumer-2")
	req, tenant, qTwolastTenantIndex, err := qb.dequeueItemForConsumer(&ConsumerWorkerDequeueRequest{
		ConsumerWorkerConn: &ConsumerWorkerConn{ConsumerID: "consumer-2"},
		lastTenantIndex:    TenantIndex{qTwolastTenantIndex},
	})
	assert.Nil(t, req)
	assert.Nil(t, tenant)
	assert.Equal(t, ErrConsumerShuttingDown, err)

	// However, consumer-1 still get queues because it's still running.
	expectedDequeueVals = []dequeueVal{
		{buildExpectedObject(tenantOne.tenantID, 4), tenantOne},
		{buildExpectedObject(tenantTwo.tenantID, 4), tenantTwo},
		{buildExpectedObject(tenantOne.tenantID, 5), tenantOne},
		{buildExpectedObject(tenantTwo.tenantID, 5), tenantTwo},
	}

	for _, expected := range expectedDequeueVals {
		req, tenant, qOneLastTenantIndex, err = qb.dequeueItemForConsumer(&ConsumerWorkerDequeueRequest{
			ConsumerWorkerConn: &ConsumerWorkerConn{ConsumerID: "consumer-1"},
			lastTenantIndex:    TenantIndex{qOneLastTenantIndex},
		})
		assert.Equal(t, expected.item, req)
		assert.Equal(t, expected.tenant, tenant)
		assert.NoError(t, err)
	}

	// After disconnecting consumer-2, it's expected to own no queue.
	qb.tenantConsumerAssignments.removeConsumers("consumer-2")
	req, tenant, _, err = qb.dequeueItemForConsumer(&ConsumerWorkerDequeueRequest{
		ConsumerWorkerConn: &ConsumerWorkerConn{ConsumerID: "consumer-2"},
		lastTenantIndex:    TenantIndex{qTwolastTenantIndex},
	})
	assert.Nil(t, req)
	assert.Nil(t, tenant)
	assert.Equal(t, ErrConsumerShuttingDown, err)
}

func TestQueues_ConsumerDistribution(t *testing.T) {
	qb := newQueueBroker(0, 0)
	assert.NotNil(t, qb)
	assert.NoError(t, isConsistent(qb))

	consumers := 30
	numTenants := 1000
	maxConsumersPerTenant := 5

	// Add some consumers.
	for ix := 0; ix < consumers; ix++ {
		qid := fmt.Sprintf("consumer-%d", ix)
		qb.addConsumerWorkerConn(NewUnregisteredConsumerWorkerConn(context.Background(), qid))

		// No consumer has any queues yet.
		req, tenant, _, err := qb.dequeueItemForConsumer(&ConsumerWorkerDequeueRequest{
			ConsumerWorkerConn: &ConsumerWorkerConn{ConsumerID: qid},
			lastTenantIndex:    TenantIndex{-1},
		})
		assert.Nil(t, req)
		assert.Nil(t, tenant)
		assert.NoError(t, err)
	}

	assert.NoError(t, isConsistent(qb))

	// Add tenant queues.
	for i := 0; i < numTenants; i++ {
		uid := fmt.Sprintf("tenant-%d", i)
		err := qb.tenantConsumerAssignments.createOrUpdateTenant(uid, maxConsumersPerTenant)
		assert.NoError(t, err)

		//Enqueue some stuff so that the tree queue node exists
		err = qb.enqueueObjectsForTests(uid, 1)
		assert.NoError(t, err)

		// Verify it has maxConsumersPerTenant consumers assigned now.
		qs := qb.tenantConsumerAssignments.consumersForTenant(uid)
		assert.Equal(t, maxConsumersPerTenant, len(qs))
	}

	// After adding all tenants, verify results. For each consumer, find out how many different tenants it handles,
	// and compute mean and stdDev.
	consumersMap := make(map[tree.ConsumerID]int)

	for tenantID := range qb.tenantConsumerAssignments.tenantsByID {
		consumerSet := qb.tenantConsumerAssignments.consumersForTenant(tenantID)
		for consumerID := range consumerSet {
			consumersMap[consumerID]++
		}
	}

	mean := float64(0)
	for _, c := range consumersMap {
		mean += float64(c)
	}
	mean = mean / float64(len(consumersMap))

	stdDev := float64(0)
	for _, c := range consumersMap {
		d := float64(c) - mean
		stdDev += (d * d)
	}
	stdDev = math.Sqrt(stdDev / float64(len(consumersMap)))
	t.Log("mean:", mean, "stddev:", stdDev)

	assert.InDelta(t, numTenants*maxConsumersPerTenant/consumers, mean, 1)
	assert.InDelta(t, stdDev, 0, mean*0.2)
}

func TestQueuesConsistency(t *testing.T) {
	tests := map[string]struct {
		forgetDelay time.Duration
	}{
		"without forget delay": {},
		"with forget delay":    {forgetDelay: time.Minute},
	}
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			qb := newQueueBroker(0, testData.forgetDelay)
			assert.NotNil(t, qb)
			assert.NoError(t, isConsistent(qb))

			r := rand.New(rand.NewSource(time.Now().Unix()))

			// maps consumer IDs to tenant indexes for that consumer.
			lastTenantIndexes := map[string]int{}

			// track active consumer-worker connections to use worker IDs for removal
			conns := map[string][]*ConsumerWorkerConn{}

			for i := 0; i < 100; i++ {
				switch r.Int() % 6 {
				case 0:
					err := getOrAddTenantQueue(qb, generateTenant(r), 3)
					assert.Nil(t, err)
				case 1:
					consumerID := generateConsumer(r)
					tenantIndex := getNextTenantForConsumer(qb, lastTenantIndexes[consumerID], consumerID)
					lastTenantIndexes[consumerID] = tenantIndex
				case 2:
					qb.removeTenantQueue(generateTenant(r))
				case 3:
					consumerID := generateConsumer(r)
					conn := NewUnregisteredConsumerWorkerConn(context.Background(), consumerID)
					qb.addConsumerWorkerConn(conn)
					conns[consumerID] = append(conns[consumerID], conn)
				case 4:
					consumerID := generateConsumer(r)
					if len(conns[consumerID]) > 0 {
						// does not matter which connection is removed; just choose the last one in the list
						conn := conns[consumerID][len(conns[consumerID])-1]
						qb.removeConsumerWorkerConn(conn, time.Now())
						// slice removed connection off end of tracking list
						conns[consumerID] = conns[consumerID][:len(conns[consumerID])-1]
					}
				case 5:
					q := generateConsumer(r)
					qb.notifyConsumerShutdown(q)
				}

				assert.NoErrorf(t, isConsistent(qb), "last action %d, rand: %d", i, r.Int()%6)
			}
		})
	}
}

func TestQueues_ForgetDelay(t *testing.T) {
	const (
		forgetDelay           = 1 * time.Minute
		maxConsumersPerTenant = 1
		numTenants            = 10
	)

	now := time.Now()
	qb := newQueueBroker(0, forgetDelay)
	assert.NotNil(t, qb)
	assert.NoError(t, isConsistent(qb))

	// 3 consumers open 2 connections each.
	consumer1Conn1 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-1")
	qb.addConsumerWorkerConn(consumer1Conn1)
	consumer1Conn2 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-1")
	qb.addConsumerWorkerConn(consumer1Conn2)

	consumer2Conn1 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-2")
	qb.addConsumerWorkerConn(consumer2Conn1)
	consumer2Conn2 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-2")
	qb.addConsumerWorkerConn(consumer2Conn2)

	consumer3Conn1 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-3")
	qb.addConsumerWorkerConn(consumer3Conn1)
	consumer3Conn2 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-3")
	qb.addConsumerWorkerConn(consumer3Conn2)

	// Add tenant queues.
	for i := 0; i < numTenants; i++ {
		tenantID := fmt.Sprintf("tenant-%d", i)
		err := qb.tenantConsumerAssignments.createOrUpdateTenant(tenantID, maxConsumersPerTenant)
		assert.NoError(t, err)

		//Enqueue some stuff so that the tree queue node exists
		err = qb.enqueueObjectsForTests(tenantID, 1)
		assert.NoError(t, err)
	}

	// We expect consumer-1 to have some tenants.
	consumer1Tenants := getTenantsByConsumer(qb, "consumer-1")
	require.NotEmpty(t, consumer1Tenants)

	// Gracefully shutdown consumer-1.
	qb.removeConsumerWorkerConn(consumer1Conn1, now.Add(20*time.Second))
	qb.removeConsumerWorkerConn(consumer1Conn2, now.Add(21*time.Second))
	qb.notifyConsumerShutdown("consumer-1")

	// We expect consumer-1 has been removed.
	assert.NotContains(t, qb.consumerConnections.consumersByID, "consumer-1")
	assert.NoError(t, isConsistent(qb))

	// We expect consumer-1 tenants have been shuffled to other consumers.
	for _, tenantID := range consumer1Tenants {
		assert.Contains(t, append(getTenantsByConsumer(qb, "consumer-2"), getTenantsByConsumer(qb, "consumer-3")...), tenantID)
	}

	// Consumer-1 reconnects.
	qb.addConsumerWorkerConn(consumer1Conn1)
	qb.addConsumerWorkerConn(consumer1Conn2)

	// We expect the initial consumer-1 tenants have got back to consumer-1.
	for _, tenantID := range consumer1Tenants {
		assert.Contains(t, getTenantsByConsumer(qb, "consumer-1"), tenantID)
		assert.NotContains(t, getTenantsByConsumer(qb, "consumer-2"), tenantID)
		assert.NotContains(t, getTenantsByConsumer(qb, "consumer-3"), tenantID)
	}

	// Consumer-1 abruptly terminates (no shutdown notification received).
	qb.removeConsumerWorkerConn(consumer1Conn1, now.Add(40*time.Second))
	qb.removeConsumerWorkerConn(consumer1Conn2, now.Add(41*time.Second))

	// We expect consumer-1 has NOT been removed.
	assert.Contains(t, qb.consumerConnections.consumersByID, "consumer-1")
	assert.NoError(t, isConsistent(qb))

	// We expect the consumer-1 tenants have not been shuffled to other consumers.
	for _, tenantID := range consumer1Tenants {
		assert.Contains(t, getTenantsByConsumer(qb, "consumer-1"), tenantID)
		assert.NotContains(t, getTenantsByConsumer(qb, "consumer-2"), tenantID)
		assert.NotContains(t, getTenantsByConsumer(qb, "consumer-3"), tenantID)
	}

	// Try to forget disconnected consumers, but consumer-1 forget delay hasn't passed yet.
	qb.forgetDisconnectedConsumers(now.Add(90 * time.Second))

	assert.Contains(t, qb.consumerConnections.consumersByID, "consumer-1")
	assert.NoError(t, isConsistent(qb))

	for _, tenantID := range consumer1Tenants {
		assert.Contains(t, getTenantsByConsumer(qb, "consumer-1"), tenantID)
		assert.NotContains(t, getTenantsByConsumer(qb, "consumer-2"), tenantID)
		assert.NotContains(t, getTenantsByConsumer(qb, "consumer-3"), tenantID)
	}

	// Try to forget disconnected consumers. This time consumer-1 forget delay has passed.
	qb.forgetDisconnectedConsumers(now.Add(105 * time.Second))

	assert.NotContains(t, qb.consumerConnections.consumersByID, "consumer-1")
	assert.NoError(t, isConsistent(qb))

	// We expect consumer-1 tenants have been shuffled to other consumers.
	for _, tenantID := range consumer1Tenants {
		assert.Contains(t, append(getTenantsByConsumer(qb, "consumer-2"), getTenantsByConsumer(qb, "consumer-3")...), tenantID)
	}
}

func TestQueues_ForgetDelay_ShouldCorrectlyHandleConsumerReconnectingBeforeForgetDelayIsPassed(t *testing.T) {
	const (
		forgetDelay           = time.Minute
		maxConsumersPerTenant = 1
		numTenants            = 100
	)

	now := time.Now()
	qb := newQueueBroker(0, forgetDelay)
	assert.NotNil(t, qb)
	assert.NoError(t, isConsistent(qb))

	// 3 consumers open 2 connections each.
	consumer1Conn1 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-1")
	qb.addConsumerWorkerConn(consumer1Conn1)
	consumer1Conn2 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-1")
	qb.addConsumerWorkerConn(consumer1Conn2)

	consumer2Conn1 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-2")
	qb.addConsumerWorkerConn(consumer2Conn1)
	consumer2Conn2 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-2")
	qb.addConsumerWorkerConn(consumer2Conn2)

	consumer3Conn1 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-3")
	qb.addConsumerWorkerConn(consumer3Conn1)
	consumer3Conn2 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-3")
	qb.addConsumerWorkerConn(consumer3Conn2)

	// Add tenant queues.
	for i := 0; i < numTenants; i++ {
		tenantID := fmt.Sprintf("tenant-%d", i)
		err := qb.tenantConsumerAssignments.createOrUpdateTenant(tenantID, maxConsumersPerTenant)
		assert.NoError(t, err)

		//Enqueue some stuff so that the tree queue node exists
		err = qb.enqueueObjectsForTests(tenantID, 1)
		assert.NoError(t, err)
	}

	// We expect consumer-1 to have some tenants.
	consumer1Tenants := getTenantsByConsumer(qb, "consumer-1")
	require.NotEmpty(t, consumer1Tenants)

	// Consumer-1 abruptly terminates (no shutdown notification received).
	qb.removeConsumerWorkerConn(consumer1Conn1, now.Add(40*time.Second))
	qb.removeConsumerWorkerConn(consumer1Conn2, now.Add(41*time.Second))

	// We expect consumer-1 has NOT been removed.
	assert.Contains(t, qb.consumerConnections.consumersByID, "consumer-1")
	assert.NoError(t, isConsistent(qb))

	// We expect the consumer-1 tenants have not been shuffled to other consumers.
	for _, tenantID := range consumer1Tenants {
		assert.Contains(t, getTenantsByConsumer(qb, "consumer-1"), tenantID)
		assert.NotContains(t, getTenantsByConsumer(qb, "consumer-2"), tenantID)
		assert.NotContains(t, getTenantsByConsumer(qb, "consumer-3"), tenantID)
	}

	// Try to forget disconnected consumers, but consumer-1 forget delay hasn't passed yet.
	qb.forgetDisconnectedConsumers(now.Add(90 * time.Second))

	// Consumer-1 reconnects.
	qb.addConsumerWorkerConn(consumer1Conn1)
	qb.addConsumerWorkerConn(consumer1Conn2)

	assert.Contains(t, qb.consumerConnections.consumersByID, "consumer-1")
	assert.NoError(t, isConsistent(qb))

	// We expect the consumer-1 tenants have not been shuffled to other consumers.
	for _, tenantID := range consumer1Tenants {
		assert.Contains(t, getTenantsByConsumer(qb, "consumer-1"), tenantID)
		assert.NotContains(t, getTenantsByConsumer(qb, "consumer-2"), tenantID)
		assert.NotContains(t, getTenantsByConsumer(qb, "consumer-3"), tenantID)
	}

	// Try to forget disconnected consumers far in the future, but there's no disconnected consumer.
	qb.forgetDisconnectedConsumers(now.Add(200 * time.Second))

	assert.Contains(t, qb.consumerConnections.consumersByID, "consumer-1")
	assert.NoError(t, isConsistent(qb))

	for _, tenantID := range consumer1Tenants {
		assert.Contains(t, getTenantsByConsumer(qb, "consumer-1"), tenantID)
		assert.NotContains(t, getTenantsByConsumer(qb, "consumer-2"), tenantID)
		assert.NotContains(t, getTenantsByConsumer(qb, "consumer-3"), tenantID)
	}
}

func generateTenant(r *rand.Rand) string {
	return fmt.Sprint("tenant-", r.Int()%5)
}

func generateConsumer(r *rand.Rand) string {
	return fmt.Sprint("consumer-", r.Int()%5)
}

// getTenantsByConsumer returns the list of tenants handled by the provided ConsumerID.

func getTenantsByConsumer(broker *queueBroker, consumerID string) []string {
	var tenantIDs []string
	for _, tenantID := range broker.tenantConsumerAssignments.queuingAlgorithm.TenantIDOrder() {
		consumerSet := broker.tenantConsumerAssignments.consumersForTenant(tenantID)
		if consumerSet == nil {
			// If it's nil then all consumers can handle this tenant.
			tenantIDs = append(tenantIDs, tenantID)
			continue
		}
		if _, ok := consumerSet[tree.ConsumerID(consumerID)]; ok {
			tenantIDs = append(tenantIDs, tenantID)
		}
	}
	return tenantIDs
}

func getOrAddTenantQueue(qb *queueBroker, tenantID string, maxConsumers int) error {
	err := qb.tenantConsumerAssignments.createOrUpdateTenant(tenantID, maxConsumers)
	if err != nil {
		return err
	}

	queuePath := qb.makeQueuePathForTests(tenantID)
	return tree.GetOrAddNode(queuePath, qb.tree)
}

// removeTenantQueue is a test utility, not intended for use by consumers of queueBroker
func (qb *queueBroker) removeTenantQueue(tenantID string) bool {
	qb.tenantConsumerAssignments.removeTenant(tenantID)
	queuePath := qb.makeQueuePathForTests(tenantID)

	tree.DeleteNode(tree.RootNode(qb.tree.(*tree.MultiAlgorithmTreeQueue)), queuePath)

	return false
}

func (qb *queueBroker) makeQueuePathForTests(tenantID string) tree.QueuePath {
	return tree.QueuePath{UnknownDimension, tenantID}
}

func isConsistent(qb *queueBroker) error {
	if len(qb.tenantConsumerAssignments.consumerIDsSorted) != len(qb.consumerConnections.consumersByID) {
		return fmt.Errorf("inconsistent number of sorted consumers and consumer connections")
	}

	tenantCount := 0
	existingTenants := make(map[string]bool)

	for ix, tenantID := range qb.tenantConsumerAssignments.queuingAlgorithm.TenantIDOrder() {
		path := qb.makeQueuePathForTests(tenantID)

		node := qb.tree.GetNode(path)
		if tenantID != "" && node == nil {
			return fmt.Errorf("tenant %s doesn't have queue", tenantID)
		}

		if tenantID == "" && node != nil {
			return fmt.Errorf("tenant %s shouldn't have queue", tenantID)
		}

		if tenantID == "" {
			continue
		}

		if tenantID != "" {
			tenantCount++
			existingTenants[tenantID] = true
		}

		tenant := qb.tenantConsumerAssignments.tenantsByID[tenantID]
		consumerSet := qb.tenantConsumerAssignments.consumersForTenant(tenantID)

		if tenant.orderIndex != ix {
			return fmt.Errorf("invalid tenant's index, expected=%d, got=%d", ix, tenant.orderIndex)
		}

		if tenant.maxConsumers == 0 && consumerSet != nil {
			return fmt.Errorf("tenant %s has consumers, but maxConsumers=0", tenantID)
		}

		if tenant.maxConsumers > 0 && len(qb.tenantConsumerAssignments.consumerIDsSorted) <= tenant.maxConsumers && consumerSet != nil {
			return fmt.Errorf("tenant %s has consumers set despite not enough consumers available", tenantID)
		}

		if tenant.maxConsumers > 0 && len(qb.tenantConsumerAssignments.consumerIDsSorted) > tenant.maxConsumers && len(consumerSet) != tenant.maxConsumers {
			return fmt.Errorf("tenant %s has incorrect number of consumers, expected=%d, got=%d", tenantID, len(consumerSet), tenant.maxConsumers)
		}
	}

	var tenantQueueCount int
	if itq, ok := qb.tree.(*tree.MultiAlgorithmTreeQueue); ok {
		tenantQueueCount = tree.TenantQueueCount(itq)
	}
	if tenantQueueCount != tenantCount {
		return fmt.Errorf("inconsistent number of tenants list and tenant queues")
	}

	return nil
}

// The next tenant for the consumer is obtained by rotating through the global tenant order
// starting just after the last tenant the consumer received a request for, until a tenant
// is found that is assigned to the given consumer according to the consumer shuffle sharding.
// A newly connected consumer provides lastTenantIndex of -1 in order to start at the beginning.
func getNextTenantForConsumer(qb *queueBroker, lastTenantIndex int, consumerID string) int {
	if !qb.consumerConnections.consumerIsAvailable(consumerID) {
		return lastTenantIndex
	}
	tenantOrderIndex := lastTenantIndex
	tenantIDOrder := qb.tenantConsumerAssignments.queuingAlgorithm.TenantIDOrder()
	for iters := 0; iters < len(tenantIDOrder); iters++ {
		tenantOrderIndex++
		if tenantOrderIndex >= len(tenantIDOrder) {
			tenantOrderIndex = 0
		}
		tenantID := tenantIDOrder[tenantOrderIndex]
		if tenantID == "" {
			continue
		}

		tenantConsumerSet := qb.tenantConsumerAssignments.consumersForTenant(tenantID)
		if tenantConsumerSet == nil {
			// tenant can use all consumers
			return tenantOrderIndex
		} else if _, ok := tenantConsumerSet[tree.ConsumerID(consumerID)]; ok {
			// tenant is assigned this consumer
			return tenantOrderIndex
		}
	}
	return lastTenantIndex
}

func newTestQueueBroker(maxTenantQueueSize int) *queueBroker {
	return newQueueBroker(maxTenantQueueSize, 0)
}

func TestQueueBroker_ReserveForTenant(t *testing.T) {
	testCases := map[string]struct {
		maxTenantQueueSize int
		queuedItems        int
		reserved           int
		count              int
		expectedErr        error
		expectedReserved   int
	}{
		"reservation that fills the cap consumes it immediately": {
			maxTenantQueueSize: 5,
			reserved:           5,
			count:              1,
			expectedErr:        ErrTooManyRequests,
			expectedReserved:   5,
		},
		"queued items count against the cap": {
			maxTenantQueueSize: 5,
			queuedItems:        1,
			reserved:           4,
			count:              1,
			expectedErr:        ErrTooManyRequests,
			expectedReserved:   4,
		},
		// A non-positive count is a caller bug, not a full queue, so it must not report
		// ErrTooManyRequests.
		"zero count": {
			maxTenantQueueSize: 5,
			count:              0,
			expectedErr:        errInvalidReservationCount,
		},
		"negative count": {
			maxTenantQueueSize: 5,
			count:              -1,
			expectedErr:        errInvalidReservationCount,
		},
		// The occupancy is already 1, so occupancy+count wraps negative at count == math.MaxInt. A
		// check that adds instead of subtracts would then admit the reservation.
		"count above the cap does not overflow": {
			maxTenantQueueSize: 5,
			queuedItems:        1,
			count:              math.MaxInt,
			expectedErr:        ErrTooManyRequests,
		},
		// The occupancy is math.MaxInt, so occupancy+1 wraps negative.
		"cap of math.MaxInt does not overflow": {
			maxTenantQueueSize: math.MaxInt,
			reserved:           math.MaxInt,
			count:              1,
			expectedErr:        ErrTooManyRequests,
			expectedReserved:   math.MaxInt,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			qb := newTestQueueBroker(testCase.maxTenantQueueSize)
			for i := 0; i < testCase.queuedItems; i++ {
				require.NoError(t, qb.enqueueItemBack(&tenantItem{tenantID: "tenant-a", item: i}, 0))
			}
			if testCase.reserved > 0 {
				require.NoError(t, qb.reserveForTenant("tenant-a", testCase.reserved))
			}

			require.ErrorIs(t, qb.reserveForTenant("tenant-a", testCase.count), testCase.expectedErr)

			// A rejected reservation must not change the reserved count.
			if testCase.expectedReserved == 0 {
				require.NotContains(t, qb.reservedByTenant, "tenant-a")
			} else {
				require.Equal(t, testCase.expectedReserved, qb.reservedByTenant["tenant-a"])
			}
			// A reservation does not create the tenant. Only a queued item does.
			require.Equal(t, testCase.queuedItems > 0, qb.tenantConsumerAssignments.tenantsByID["tenant-a"] != nil)
		})
	}
}

func TestQueueBroker_ReserveForTenant_RejectionReportsOccupancy(t *testing.T) {
	qb := newTestQueueBroker(6)
	require.NoError(t, qb.enqueueItemBack(&tenantItem{tenantID: "tenant-a", item: "plain-1"}, 0))
	require.NoError(t, qb.enqueueItemBack(&tenantItem{tenantID: "tenant-a", item: "plain-2"}, 0))
	require.NoError(t, qb.reserveForTenant("tenant-a", 3))

	err := qb.reserveForTenant("tenant-a", 2)

	var rejected *ReservationRejectedError
	require.ErrorAs(t, err, &rejected)
	require.Equal(t, ReservationRejectedError{Requested: 2, Queued: 2, Reserved: 3, MaxTenantQueueSize: 6}, *rejected)
	require.ErrorIs(t, err, ErrTooManyRequests)
}

func TestQueueBroker_ReleaseForTenant(t *testing.T) {
	const maxTenantQueueSize = 5

	testCases := map[string]struct {
		reserved          int
		release           int
		expectedLeaked    int
		expectedRemaining int
	}{
		"exact release returns the occupancy to its prior value": {
			reserved: 5,
			release:  5,
		},
		"partial release keeps the remaining slots": {
			reserved:          5,
			release:           2,
			expectedRemaining: 3,
		},
		// The tenant's reservation is then 0, not negative.
		"over-release clamps at zero and reports the leak": {
			reserved:       2,
			release:        5,
			expectedLeaked: 3,
		},
		"release for an unknown tenant reports a full leak": {
			release:        3,
			expectedLeaked: 3,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			qb := newTestQueueBroker(maxTenantQueueSize)
			if testCase.reserved > 0 {
				require.NoError(t, qb.reserveForTenant("tenant-a", testCase.reserved))
			}

			require.Equal(t, testCase.expectedLeaked, qb.releaseForTenant("tenant-a", testCase.release))

			if testCase.expectedRemaining == 0 {
				require.NotContains(t, qb.reservedByTenant, "tenant-a")
			} else {
				require.Equal(t, testCase.expectedRemaining, qb.reservedByTenant["tenant-a"])
			}
			// The tenant has no queued item, so no tenant state stays after the release.
			require.Nil(t, qb.tenantConsumerAssignments.tenantsByID["tenant-a"])
			require.NotContains(t, qb.tenantConsumerAssignments.queuingAlgorithm.TenantIDOrder(), "tenant-a")

			// The release frees the cap that the remaining slots do not hold.
			require.NoError(t, qb.reserveForTenant("tenant-a", maxTenantQueueSize-testCase.expectedRemaining))
		})
	}
}

func TestQueueBroker_EnqueueItemBack_RejectsWhenReservationsFillCap(t *testing.T) {
	testCases := map[string]struct {
		maxTenantQueueSize int
	}{
		"reservations fill the cap": {maxTenantQueueSize: 2},
		// The occupancy is math.MaxInt, so occupancy+1 wraps negative.
		"cap of math.MaxInt does not overflow": {maxTenantQueueSize: math.MaxInt},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			qb := newTestQueueBroker(testCase.maxTenantQueueSize)
			require.NoError(t, qb.reserveForTenant("tenant-a", testCase.maxTenantQueueSize))

			err := qb.enqueueItemBack(&tenantItem{tenantID: "tenant-a", item: "plain-1"}, 0)
			require.ErrorIs(t, err, ErrTooManyRequests)
			require.Equal(t, 0, qb.itemCount())

			// A rejected enqueue adds no item, so it must not register the tenant either.
			require.Nil(t, qb.tenantConsumerAssignments.tenantsByID["tenant-a"])
			require.NotContains(t, qb.tenantConsumerAssignments.queuingAlgorithm.TenantIDOrder(), "tenant-a")
		})
	}
}

func TestQueueBroker_EnqueueItemFront_DoesNotTouchReservations(t *testing.T) {
	// cap=2 makes 1 queued item plus 1 reserved slot fill the cap. So the second reservation fails
	// only if enqueueItemFront keeps the reservation.
	qb := newTestQueueBroker(2)
	require.NoError(t, qb.reserveForTenant("tenant-a", 1))

	// enqueueItemFront is the re-enqueue path for a dequeued-but-undeliverable item; it must not
	// consume or otherwise alter reservedByTenant.
	require.NoError(t, qb.enqueueItemFront(&tenantItem{tenantID: "tenant-a", item: "item-1"}, 0))

	err := qb.reserveForTenant("tenant-a", 1)
	require.ErrorIs(t, err, ErrTooManyRequests)
}

func dequeueForTest(t *testing.T, qb *queueBroker, conn *ConsumerWorkerConn) *tenantItem {
	t.Helper()
	dequeued, _, _, err := qb.dequeueItemForConsumer(NewConsumerWorkerDequeueRequest(conn, FirstTenant()))
	require.NoError(t, err)
	return dequeued
}

func TestQueueBroker_ReservedSlotsSurviveTenantQueueDraining(t *testing.T) {
	// cap=4 is the only cap for which both checks pass on correct code. A dropped reservation then
	// makes the first check fail: reserving 3 would succeed on an occupancy of 0.
	qb := newTestQueueBroker(4)
	conn := NewUnregisteredConsumerWorkerConn(t.Context(), "consumer-1")
	qb.addConsumerWorkerConn(conn)

	require.NoError(t, qb.reserveForTenant("tenant-a", 3))
	require.NoError(t, qb.enqueueReservedItemBack(&tenantItem{tenantID: "tenant-a", item: "item-1"}, 0))
	require.Equal(t, "item-1", dequeueForTest(t, qb, conn).item)

	// The queue is now empty, but 2 reserved slots are still unused. A further reservation must
	// still count them, which proves that the dequeue did not drop them.
	require.ErrorIs(t, qb.reserveForTenant("tenant-a", 3), ErrTooManyRequests)
	require.NoError(t, qb.reserveForTenant("tenant-a", 2))

	require.NoError(t, qb.enqueueReservedItemBack(&tenantItem{tenantID: "tenant-a", item: "item-2"}, 0))
	dequeued := dequeueForTest(t, qb, conn)
	require.NotNil(t, dequeued, "an item inserted after the tenant's queue drained must stay dispatchable")
	require.Equal(t, "item-2", dequeued.item)
}

func TestQueueBroker_RejectedPlainEnqueueDoesNotDuplicateTenantOrderEntry(t *testing.T) {
	qb := newTestQueueBroker(2)
	conn := NewUnregisteredConsumerWorkerConn(t.Context(), "consumer-1")
	qb.addConsumerWorkerConn(conn)

	require.NoError(t, qb.enqueueItemBack(&tenantItem{tenantID: "tenant-x", item: "x-1"}, 0))
	require.NoError(t, qb.reserveForTenant("tenant-a", 2))
	require.ErrorIs(t, qb.enqueueItemBack(&tenantItem{tenantID: "tenant-a", item: "plain-1"}, 0), ErrTooManyRequests)
	require.Equal(t, "x-1", dequeueForTest(t, qb, conn).item)
	require.NoError(t, qb.enqueueReservedItemBack(&tenantItem{tenantID: "tenant-a", item: "a-1"}, 0))

	count := 0
	for _, id := range qb.tenantConsumerAssignments.queuingAlgorithm.TenantIDOrder() {
		if id == "tenant-a" {
			count++
		}
	}
	require.Equal(t, 1, count, "tenant-a must appear in the tenant order exactly once")
}
