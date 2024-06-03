// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/user_queues_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queue

import (
	"context"
	"fmt"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math"
	"math/rand"
	"testing"
	"time"
)

// TODO (casie): Add a test case where, if a tenant is queued in the queueTree, but doesn't exist from the queueBroker perspective,
//  it should never be dequeued.

// TODO (casie): maybe implement a way to fetch an entire node that _would_ be dequeued from, rather than just one elt

func (qb *queueBroker) enqueueObjectsForTests(tenantID TenantID, numObjects int) error {
	for i := 0; i < numObjects; i++ {
		err := qb.queueTree.rootNode.enqueueBackByPath(qb.queueTree, QueuePath{string(tenantID)}, &tenantRequest{
			tenantID: tenantID,
			req:      fmt.Sprintf("%v: object-%v", tenantID, i),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func buildExpectedObject(tenantID TenantID, num int) *tenantRequest {
	return &tenantRequest{
		tenantID: tenantID,
		req:      fmt.Sprintf("%v: object-%v", tenantID, num),
	}
}

type dequeueVal struct {
	req    *tenantRequest
	tenant *queueTenant
}

func assertExpectedValuesOnDequeue(t *testing.T, qb *queueBroker, lastTenantIndex int, querierID QuerierID, expectedVals []dequeueVal) int {
	var req *tenantRequest
	var tenant *queueTenant
	var err error

	for _, expected := range expectedVals {
		req, tenant, lastTenantIndex, err = qb.dequeueRequestForQuerier(lastTenantIndex, querierID)
		assert.Equal(t, expected.req, req)
		assert.Equal(t, expected.tenant, tenant)
		assert.NoError(t, err)
	}
	return lastTenantIndex
}

func TestQueues(t *testing.T) {
	qb := newQueueBroker(0, true, 0)
	assert.NotNil(t, qb)
	assert.NoError(t, isConsistent(qb))

	qb.addQuerierConnection("querier-1")
	qb.addQuerierConnection("querier-2")

	req, tenant, lastTenantIndex, err := qb.dequeueRequestForQuerier(-1, "querier-1")
	assert.Nil(t, req)
	assert.Nil(t, tenant)
	assert.NoError(t, err)

	// Add tenant queues: tenant "one"
	err = qb.tenantQuerierAssignments.createOrUpdateTenant("one", 0)
	assert.NoError(t, err)

	tenantOne := qb.tenantQuerierAssignments.tenantsByID["one"]

	// Queue enough objects for tenant "one"
	err = qb.enqueueObjectsForTests(tenantOne.tenantID, 10)
	assert.NoError(t, err)
	assert.NoError(t, isConsistent(qb))

	//queuePathOne := QueuePath{"root", "one"}
	expectedDequeueVals := []dequeueVal{
		{buildExpectedObject(tenantOne.tenantID, 0), tenantOne},
		{buildExpectedObject(tenantOne.tenantID, 1), tenantOne},
	}
	lastTenantIndex = assertExpectedValuesOnDequeue(t, qb, lastTenantIndex, "querier-1", expectedDequeueVals)

	// Add tenant two
	err = qb.tenantQuerierAssignments.createOrUpdateTenant("two", 0)
	assert.NoError(t, err)

	tenantTwo := qb.tenantQuerierAssignments.tenantsByID["two"]

	err = qb.enqueueObjectsForTests(tenantTwo.tenantID, 10)
	assert.NoError(t, err)
	assert.NoError(t, isConsistent(qb))

	expectedDequeueVals = []dequeueVal{
		{buildExpectedObject(tenantTwo.tenantID, 0), tenantTwo},
		{buildExpectedObject(tenantOne.tenantID, 2), tenantOne},
		{buildExpectedObject(tenantTwo.tenantID, 1), tenantTwo},
		{buildExpectedObject(tenantOne.tenantID, 3), tenantOne},
	}

	lastTenantIndex = assertExpectedValuesOnDequeue(t, qb, lastTenantIndex, "querier-1", expectedDequeueVals)

	expectedDequeueVals = []dequeueVal{
		{buildExpectedObject(tenantOne.tenantID, 4), tenantOne},
		{buildExpectedObject(tenantTwo.tenantID, 2), tenantTwo},
		{buildExpectedObject(tenantOne.tenantID, 5), tenantOne},
	}
	lastTenantIndex = assertExpectedValuesOnDequeue(t, qb, -1, "querier-2", expectedDequeueVals)

	//[one two three]
	// confirm fifo by adding a third tenant queue and iterating to it
	err = qb.tenantQuerierAssignments.createOrUpdateTenant("three", 0)
	assert.NoError(t, err)

	tenantThree := qb.tenantQuerierAssignments.tenantsByID["three"]

	err = qb.enqueueObjectsForTests(tenantThree.tenantID, 10)
	assert.NoError(t, err)
	assert.NoError(t, isConsistent(qb))

	expectedDequeueVals = []dequeueVal{
		{buildExpectedObject(tenantTwo.tenantID, 3), tenantTwo},
		{buildExpectedObject(tenantThree.tenantID, 0), tenantThree},
		{buildExpectedObject(tenantOne.tenantID, 6), tenantOne},
	}

	lastTenantIndex = assertExpectedValuesOnDequeue(t, qb, lastTenantIndex, "querier-1", expectedDequeueVals)

	// Remove one: ["" two three]
	qb.removeTenantQueue("one")
	assert.NoError(t, isConsistent(qb))

	expectedDequeueVals = []dequeueVal{
		{buildExpectedObject(tenantTwo.tenantID, 4), tenantTwo},
		{buildExpectedObject(tenantThree.tenantID, 1), tenantThree},
		{buildExpectedObject(tenantTwo.tenantID, 5), tenantTwo},
	}
	lastTenantIndex = assertExpectedValuesOnDequeue(t, qb, lastTenantIndex, "querier-1", expectedDequeueVals)

	// "four" is added at the beginning of the list: [four two three]
	err = qb.tenantQuerierAssignments.createOrUpdateTenant("four", 0)
	assert.NoError(t, err)

	tenantFour := qb.tenantQuerierAssignments.tenantsByID["four"]

	err = qb.enqueueObjectsForTests(tenantFour.tenantID, 10)
	assert.NoError(t, err)
	assert.NoError(t, isConsistent(qb))

	expectedDequeueVals = []dequeueVal{
		{buildExpectedObject(tenantThree.tenantID, 2), tenantThree},
		{buildExpectedObject(tenantFour.tenantID, 0), tenantFour},
		{buildExpectedObject(tenantTwo.tenantID, 6), tenantTwo},
		{buildExpectedObject(tenantThree.tenantID, 3), tenantThree},
	}
	lastTenantIndex = assertExpectedValuesOnDequeue(t, qb, lastTenantIndex, "querier-1", expectedDequeueVals)

	// Remove two: [four "" three]
	qb.removeTenantQueue("two")
	assert.NoError(t, isConsistent(qb))

	expectedDequeueVals = []dequeueVal{
		{buildExpectedObject(tenantFour.tenantID, 1), tenantFour},
		{buildExpectedObject(tenantThree.tenantID, 4), tenantThree},
		{buildExpectedObject(tenantFour.tenantID, 2), tenantFour},
	}
	lastTenantIndex = assertExpectedValuesOnDequeue(t, qb, lastTenantIndex, "querier-1", expectedDequeueVals)

	// Remove three: [four ""] )
	qb.removeTenantQueue("three")
	assert.NoError(t, isConsistent(qb))

	// Remove four: []
	qb.removeTenantQueue("four")
	assert.NoError(t, isConsistent(qb))

	req, tenant, _, err = qb.dequeueRequestForQuerier(lastTenantIndex, "querier-1")
	assert.Nil(t, req)
	assert.Nil(t, tenant)
	assert.NoError(t, err)
}

func TestQueuesRespectMaxTenantQueueSizeWithSubQueues(t *testing.T) {
	maxTenantQueueSize := 100
	qb := newQueueBroker(maxTenantQueueSize, true, 0)
	additionalQueueDimensions := map[int][]string{
		0: nil,
		1: {"ingester"},
		2: {"store-gateway"},
		3: {"ingester-and-store-gateway"},
	}
	req := &SchedulerRequest{
		Ctx:          context.Background(),
		FrontendAddr: "http://query-frontend:8007",
		UserID:       "tenant-1",
		Request:      &httpgrpc.HTTPRequest{},
	}

	// build queue evenly with either no additional queue dimension or one of 3 additional dimensions
	for i := 0; i < len(additionalQueueDimensions); i++ {
		for j := 0; j < maxTenantQueueSize/len(additionalQueueDimensions); j++ {
			req.AdditionalQueueDimensions = additionalQueueDimensions[i]
			tenantReq := &tenantRequest{tenantID: "tenant-1", req: req}
			err := qb.enqueueRequestBack(tenantReq, 0)
			assert.NoError(t, err)
		}
	}
	// assert item count of tenant node and its subnodes
	queuePath := QueuePath{"tenant-1"}
	assert.Equal(t, maxTenantQueueSize, qb.queueTree.rootNode.getNode(queuePath).ItemCount())

	// assert equal distribution of queue items between tenant node and 3 subnodes
	for _, v := range additionalQueueDimensions {
		queuePath := append(QueuePath{"tenant-1"}, v...)
		assert.Equal(t, maxTenantQueueSize/len(additionalQueueDimensions), qb.queueTree.rootNode.getNode(queuePath).getLocalQueue().Len())
	}

	// assert error received when hitting a tenant's enqueue limit,
	// even though most of the requests are in the subqueues
	for _, additionalQueueDimension := range additionalQueueDimensions {
		// error should be received no matter if the enqueue attempt
		// is for the tenant queue or any of its subqueues
		req.AdditionalQueueDimensions = additionalQueueDimension
		tenantReq := &tenantRequest{tenantID: "tenant-1", req: req}
		err := qb.enqueueRequestBack(tenantReq, 0)
		assert.ErrorIs(t, err, ErrTooManyRequests)
	}

	// dequeue a request
	qb.addQuerierConnection("querier-1")
	dequeuedTenantReq, _, _, err := qb.dequeueRequestForQuerier(-1, "querier-1")
	assert.NoError(t, err)
	assert.NotNil(t, dequeuedTenantReq)

	tenantReq := &tenantRequest{tenantID: "tenant-1", req: req}
	// assert not hitting an error when enqueueing after dequeuing to below the limit
	err = qb.enqueueRequestBack(tenantReq, 0)
	assert.NoError(t, err)

	// we then hit an error again, as we are back at the limit
	err = qb.enqueueRequestBack(tenantReq, 0)
	assert.ErrorIs(t, err, ErrTooManyRequests)
}

func TestQueuesOnTerminatingQuerier(t *testing.T) {
	qb := newQueueBroker(0, true, 0)
	assert.NotNil(t, qb)
	assert.NoError(t, isConsistent(qb))

	qb.addQuerierConnection("querier-1")
	qb.addQuerierConnection("querier-2")

	// Add queues: [one two]
	err := qb.tenantQuerierAssignments.createOrUpdateTenant("one", 0)
	assert.NoError(t, err)
	err = qb.tenantQuerierAssignments.createOrUpdateTenant("two", 0)
	assert.NoError(t, err)

	err = qb.enqueueObjectsForTests("one", 10)
	assert.NoError(t, err)
	tenantOne := qb.tenantQuerierAssignments.tenantsByID["one"]

	err = qb.enqueueObjectsForTests("two", 10)
	assert.NoError(t, err)
	tenantTwo := qb.tenantQuerierAssignments.tenantsByID["two"]

	expectedDequeueVals := []dequeueVal{
		{buildExpectedObject(tenantOne.tenantID, 0), tenantOne},
		{buildExpectedObject(tenantTwo.tenantID, 0), tenantTwo},
		{buildExpectedObject(tenantOne.tenantID, 1), tenantOne},
		{buildExpectedObject(tenantTwo.tenantID, 1), tenantTwo},
	}
	qOneLastTenantIndex := assertExpectedValuesOnDequeue(t, qb, -1, "querier-1", expectedDequeueVals)

	// TODO (casie): This is consistent with the previous test only because the previous test
	//  checked n % numTenants == 0 dequeues.
	expectedDequeueVals = []dequeueVal{
		{buildExpectedObject(tenantOne.tenantID, 2), tenantOne},
		{buildExpectedObject(tenantTwo.tenantID, 2), tenantTwo},
		{buildExpectedObject(tenantOne.tenantID, 3), tenantOne},
		{buildExpectedObject(tenantTwo.tenantID, 3), tenantTwo},
	}
	qTwolastTenantIndex := assertExpectedValuesOnDequeue(t, qb, -1, "querier-2", expectedDequeueVals)

	// After notify shutdown for querier-2, it's expected to own no queue.
	qb.notifyQuerierShutdown("querier-2")
	req, tenant, qTwolastTenantIndex, err := qb.dequeueRequestForQuerier(qTwolastTenantIndex, "querier-2")
	assert.Nil(t, req)
	assert.Nil(t, tenant)
	assert.Equal(t, ErrQuerierShuttingDown, err)

	// However, querier-1 still get queues because it's still running.
	expectedDequeueVals = []dequeueVal{
		{buildExpectedObject(tenantOne.tenantID, 4), tenantOne},
		{buildExpectedObject(tenantTwo.tenantID, 4), tenantTwo},
		{buildExpectedObject(tenantOne.tenantID, 5), tenantOne},
		{buildExpectedObject(tenantTwo.tenantID, 5), tenantTwo},
	}

	for _, expected := range expectedDequeueVals {
		req, tenant, qOneLastTenantIndex, err = qb.dequeueRequestForQuerier(qOneLastTenantIndex, "querier-1")
		assert.Equal(t, expected.req, req)
		assert.Equal(t, expected.tenant, tenant)
		assert.NoError(t, err)
	}

	// After disconnecting querier-2, it's expected to own no queue.
	qb.tenantQuerierAssignments.removeQuerier("querier-2")
	req, tenant, qTwolastTenantIndex, err = qb.dequeueRequestForQuerier(qTwolastTenantIndex, "querier-2")
	assert.Nil(t, req)
	assert.Nil(t, tenant)
	assert.Equal(t, ErrQuerierShuttingDown, err)

}

func TestQueuesWithQueriers(t *testing.T) {
	qb := newQueueBroker(0, true, 0)
	assert.NotNil(t, qb)
	assert.NoError(t, isConsistent(qb))

	queriers := 30
	numTenants := 1000
	maxQueriersPerTenant := 5

	// Add some queriers.
	for ix := 0; ix < queriers; ix++ {
		qid := QuerierID(fmt.Sprintf("querier-%d", ix))
		qb.addQuerierConnection(qid)

		// No querier has any queues yet.
		req, tenant, _, err := qb.dequeueRequestForQuerier(-1, qid)
		assert.Nil(t, req)
		assert.Nil(t, tenant)
		assert.NoError(t, err)
	}

	assert.NoError(t, isConsistent(qb))

	// Add tenant queues.
	for i := 0; i < numTenants; i++ {
		uid := TenantID(fmt.Sprintf("tenant-%d", i))
		//getOrAdd(t, qb, uid, maxQueriersPerTenant)
		err := qb.tenantQuerierAssignments.createOrUpdateTenant(uid, maxQueriersPerTenant)
		assert.NoError(t, err)

		//Enqueue some stuff so that the tree queue node exists
		err = qb.enqueueObjectsForTests(uid, 1)
		assert.NoError(t, err)

		// Verify it has maxQueriersPerTenant queriers assigned now.
		qs := qb.tenantQuerierAssignments.tenantQuerierIDs[uid]
		assert.Equal(t, maxQueriersPerTenant, len(qs))
	}

	// After adding all tenants, verify results. For each querier, find out how many different tenants it handles,
	// and compute mean and stdDev.
	queriersMap := make(map[QuerierID]int)

	for _, querierSet := range qb.tenantQuerierAssignments.tenantQuerierIDs {
		for querierID := range querierSet {
			queriersMap[querierID]++
		}
	}

	mean := float64(0)
	for _, c := range queriersMap {
		mean += float64(c)
	}
	mean = mean / float64(len(queriersMap))

	stdDev := float64(0)
	for _, c := range queriersMap {
		d := float64(c) - mean
		stdDev += (d * d)
	}
	stdDev = math.Sqrt(stdDev / float64(len(queriersMap)))
	t.Log("mean:", mean, "stddev:", stdDev)

	assert.InDelta(t, numTenants*maxQueriersPerTenant/queriers, mean, 1)
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
			qb := newQueueBroker(0, true, testData.forgetDelay)
			assert.NotNil(t, qb)
			assert.NoError(t, isConsistent(qb))

			r := rand.New(rand.NewSource(time.Now().Unix()))

			lastTenantIndexes := map[QuerierID]int{}

			conns := map[QuerierID]int{}

			for i := 0; i < 10000; i++ {
				switch r.Int() % 6 {
				case 0:
					queue, err := qb.getOrAddTenantQueue(generateTenant(r), 3)
					assert.Nil(t, err)
					assert.NotNil(t, queue)
				case 1:
					querierID := generateQuerier(r)
					_, tenantIndex, _ := qb.tenantQuerierAssignments.getNextTenantForQuerier(lastTenantIndexes[querierID], querierID)
					lastTenantIndexes[querierID] = tenantIndex
				case 2:
					qb.removeTenantQueue(generateTenant(r))
				case 3:
					q := generateQuerier(r)
					qb.addQuerierConnection(q)
					conns[q]++
				case 4:
					q := generateQuerier(r)
					if conns[q] > 0 {
						qb.removeQuerierConnection(q, time.Now())
						conns[q]--
					}
				case 5:
					q := generateQuerier(r)
					qb.notifyQuerierShutdown(q)
				}

				assert.NoErrorf(t, isConsistent(qb), "last action %d, rand: %d", i, r.Int()%6)
			}
		})
	}
}

func TestQueues_ForgetDelay(t *testing.T) {
	const (
		forgetDelay          = 1 * time.Minute
		maxQueriersPerTenant = 1
		numTenants           = 10
	)

	now := time.Now()
	qb := newQueueBroker(0, true, forgetDelay)
	assert.NotNil(t, qb)
	assert.NoError(t, isConsistent(qb))

	// 3 queriers open 2 connections each.
	for i := 1; i <= 3; i++ {
		qb.addQuerierConnection(QuerierID(fmt.Sprintf("querier-%d", i)))
		qb.addQuerierConnection(QuerierID(fmt.Sprintf("querier-%d", i)))
	}

	// Add tenant queues.
	for i := 0; i < numTenants; i++ {
		tenantID := TenantID(fmt.Sprintf("tenant-%d", i))
		err := qb.tenantQuerierAssignments.createOrUpdateTenant(tenantID, maxQueriersPerTenant)
		assert.NoError(t, err)

		//Enqueue some stuff so that the tree queue node exists
		err = qb.enqueueObjectsForTests(tenantID, 1)
		assert.NoError(t, err)
	}

	// We expect querier-1 to have some tenants.
	querier1Tenants := getTenantsByQuerier(qb, "querier-1")
	require.NotEmpty(t, querier1Tenants)

	// Gracefully shutdown querier-1.
	qb.removeQuerierConnection("querier-1", now.Add(20*time.Second))
	qb.removeQuerierConnection("querier-1", now.Add(21*time.Second))
	qb.notifyQuerierShutdown("querier-1")

	// We expect querier-1 has been removed.
	assert.NotContains(t, qb.tenantQuerierAssignments.queriersByID, "querier-1")
	assert.NoError(t, isConsistent(qb))

	// We expect querier-1 tenants have been shuffled to other queriers.
	for _, tenantID := range querier1Tenants {
		assert.Contains(t, append(getTenantsByQuerier(qb, "querier-2"), getTenantsByQuerier(qb, "querier-3")...), tenantID)
	}

	// Querier-1 reconnects.
	qb.addQuerierConnection("querier-1")
	qb.addQuerierConnection("querier-1")

	// We expect the initial querier-1 tenants have got back to querier-1.
	for _, tenantID := range querier1Tenants {
		assert.Contains(t, getTenantsByQuerier(qb, "querier-1"), tenantID)
		assert.NotContains(t, getTenantsByQuerier(qb, "querier-2"), tenantID)
		assert.NotContains(t, getTenantsByQuerier(qb, "querier-3"), tenantID)
	}

	// Querier-1 abruptly terminates (no shutdown notification received).
	qb.removeQuerierConnection("querier-1", now.Add(40*time.Second))
	qb.removeQuerierConnection("querier-1", now.Add(41*time.Second))

	// We expect querier-1 has NOT been removed.
	assert.Contains(t, qb.tenantQuerierAssignments.queriersByID, QuerierID("querier-1"))
	assert.NoError(t, isConsistent(qb))

	// We expect the querier-1 tenants have not been shuffled to other queriers.
	for _, tenantID := range querier1Tenants {
		assert.Contains(t, getTenantsByQuerier(qb, "querier-1"), tenantID)
		assert.NotContains(t, getTenantsByQuerier(qb, "querier-2"), tenantID)
		assert.NotContains(t, getTenantsByQuerier(qb, "querier-3"), tenantID)
	}

	// Try to forget disconnected queriers, but querier-1 forget delay hasn't passed yet.
	qb.forgetDisconnectedQueriers(now.Add(90 * time.Second))

	assert.Contains(t, qb.tenantQuerierAssignments.queriersByID, QuerierID("querier-1"))
	assert.NoError(t, isConsistent(qb))

	for _, tenantID := range querier1Tenants {
		assert.Contains(t, getTenantsByQuerier(qb, "querier-1"), tenantID)
		assert.NotContains(t, getTenantsByQuerier(qb, "querier-2"), tenantID)
		assert.NotContains(t, getTenantsByQuerier(qb, "querier-3"), tenantID)
	}

	// Try to forget disconnected queriers. This time querier-1 forget delay has passed.
	qb.forgetDisconnectedQueriers(now.Add(105 * time.Second))

	assert.NotContains(t, qb.tenantQuerierAssignments.queriersByID, QuerierID("querier-1"))
	assert.NoError(t, isConsistent(qb))

	// We expect querier-1 tenants have been shuffled to other queriers.
	for _, tenantID := range querier1Tenants {
		assert.Contains(t, append(getTenantsByQuerier(qb, "querier-2"), getTenantsByQuerier(qb, "querier-3")...), tenantID)
	}
}

func TestQueues_ForgetDelay_ShouldCorrectlyHandleQuerierReconnectingBeforeForgetDelayIsPassed(t *testing.T) {
	const (
		forgetDelay          = time.Minute
		maxQueriersPerTenant = 1
		numTenants           = 100
	)

	now := time.Now()
	qb := newQueueBroker(0, true, forgetDelay)
	assert.NotNil(t, qb)
	assert.NoError(t, isConsistent(qb))

	// 3 queriers open 2 connections each.
	for i := 1; i <= 3; i++ {
		qb.addQuerierConnection(QuerierID(fmt.Sprintf("querier-%d", i)))
		qb.addQuerierConnection(QuerierID(fmt.Sprintf("querier-%d", i)))
	}

	// Add tenant queues.
	for i := 0; i < numTenants; i++ {
		tenantID := TenantID(fmt.Sprintf("tenant-%d", i))
		err := qb.tenantQuerierAssignments.createOrUpdateTenant(tenantID, maxQueriersPerTenant)
		assert.NoError(t, err)

		//Enqueue some stuff so that the tree queue node exists
		err = qb.enqueueObjectsForTests(tenantID, 1)
		assert.NoError(t, err)
		//getOrAdd(t, qb, tenantID, maxQueriersPerTenant)
	}

	// We expect querier-1 to have some tenants.
	querier1Tenants := getTenantsByQuerier(qb, "querier-1")
	require.NotEmpty(t, querier1Tenants)

	// Querier-1 abruptly terminates (no shutdown notification received).
	qb.removeQuerierConnection("querier-1", now.Add(40*time.Second))
	qb.removeQuerierConnection("querier-1", now.Add(41*time.Second))

	// We expect querier-1 has NOT been removed.
	assert.Contains(t, qb.tenantQuerierAssignments.queriersByID, QuerierID("querier-1"))
	assert.NoError(t, isConsistent(qb))

	// We expect the querier-1 tenants have not been shuffled to other queriers.
	for _, tenantID := range querier1Tenants {
		assert.Contains(t, getTenantsByQuerier(qb, "querier-1"), tenantID)
		assert.NotContains(t, getTenantsByQuerier(qb, "querier-2"), tenantID)
		assert.NotContains(t, getTenantsByQuerier(qb, "querier-3"), tenantID)
	}

	// Try to forget disconnected queriers, but querier-1 forget delay hasn't passed yet.
	qb.forgetDisconnectedQueriers(now.Add(90 * time.Second))

	// Querier-1 reconnects.
	qb.addQuerierConnection("querier-1")
	qb.addQuerierConnection("querier-1")

	assert.Contains(t, qb.tenantQuerierAssignments.queriersByID, QuerierID("querier-1"))
	assert.NoError(t, isConsistent(qb))

	// We expect the querier-1 tenants have not been shuffled to other queriers.
	for _, tenantID := range querier1Tenants {
		assert.Contains(t, getTenantsByQuerier(qb, "querier-1"), tenantID)
		assert.NotContains(t, getTenantsByQuerier(qb, "querier-2"), tenantID)
		assert.NotContains(t, getTenantsByQuerier(qb, "querier-3"), tenantID)
	}

	// Try to forget disconnected queriers far in the future, but there's no disconnected querier.
	qb.forgetDisconnectedQueriers(now.Add(200 * time.Second))

	assert.Contains(t, qb.tenantQuerierAssignments.queriersByID, QuerierID("querier-1"))
	assert.NoError(t, isConsistent(qb))

	for _, tenantID := range querier1Tenants {
		assert.Contains(t, getTenantsByQuerier(qb, "querier-1"), tenantID)
		assert.NotContains(t, getTenantsByQuerier(qb, "querier-2"), tenantID)
		assert.NotContains(t, getTenantsByQuerier(qb, "querier-3"), tenantID)
	}
}
func generateTenant(r *rand.Rand) TenantID {
	return TenantID(fmt.Sprint("tenant-", r.Int()%5))
}

func generateQuerier(r *rand.Rand) QuerierID {
	return QuerierID(fmt.Sprint("querier-", r.Int()%5))
}

// getTenantsByQuerier returns the list of tenants handled by the provided QuerierID.
func getTenantsByQuerier(broker *queueBroker, querierID QuerierID) []TenantID {
	var tenantIDs []TenantID
	for _, tenantID := range broker.tenantQuerierAssignments.tenantIDOrder {
		querierSet := broker.tenantQuerierAssignments.tenantQuerierIDs[tenantID]
		if querierSet == nil {
			// If it's nil then all queriers can handle this tenant.
			tenantIDs = append(tenantIDs, tenantID)
			continue
		}
		if _, ok := querierSet[querierID]; ok {
			tenantIDs = append(tenantIDs, tenantID)
		}
	}
	return tenantIDs
}

// getOrAddTenantQueue is a test utility, not intended for use by consumers of queueBroker
func (qb *queueBroker) getOrAddTenantQueue(tenantID TenantID, maxQueriers int) (*Node, error) {
	err := qb.tenantQuerierAssignments.createOrUpdateTenant(tenantID, maxQueriers)
	if err != nil {
		return nil, err
	}

	node, err := qb.queueTree.rootNode.getOrAddNode(QueuePath{string(tenantID)}, qb.queueTree)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// removeTenantQueue is a test utility, not intended for use by consumers of queueBroker
func (qb *queueBroker) removeTenantQueue(tenantID TenantID) bool {
	qb.tenantQuerierAssignments.removeTenant(tenantID)
	queuePath := QueuePath{string(tenantID)}
	//return qb.tenantQueuesTree.deleteNode(queuePath)
	return qb.queueTree.rootNode.deleteNode(queuePath)
}

func (n *Node) deleteNode(pathFromNode QueuePath) bool {
	if len(pathFromNode) == 0 {
		// node cannot delete itself
		return false
	}

	// parentPath is everything except the last node, childNode is the last in path
	parentPath, deleteNodeName := pathFromNode[:len(pathFromNode)-1], pathFromNode[len(pathFromNode)-1]

	parentNode := n.getNode(parentPath)
	if parentNode == nil {
		// not found
		return false
	}
	if deleteNode := parentNode.getNode(QueuePath{deleteNodeName}); deleteNode != nil {
		childDeleted := parentNode.dequeueAlgorithm.deleteChildNode(parentNode, deleteNode)
		parentNode.dequeueAlgorithm.dequeueOps().updateState(parentNode, nil, childDeleted)
		return true
	}
	return false
}

func (qb *queueBroker) makeQueuePathForTests(tenantID TenantID) QueuePath {
	return QueuePath{string(tenantID)}
}

func isConsistent(qb *queueBroker) error {
	if len(qb.tenantQuerierAssignments.querierIDsSorted) != len(qb.tenantQuerierAssignments.queriersByID) {
		return fmt.Errorf("inconsistent number of sorted queriers and querier connections")
	}

	tenantCount := 0

	for ix, tenantID := range qb.tenantQuerierAssignments.tenantIDOrder {
		path := qb.makeQueuePathForTests(tenantID)
		node := qb.queueTree.rootNode.getNode(path)

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
		}

		tenant := qb.tenantQuerierAssignments.tenantsByID[tenantID]
		querierSet := qb.tenantQuerierAssignments.tenantQuerierIDs[tenantID]

		if tenant.orderIndex != ix {
			return fmt.Errorf("invalid tenant's index, expected=%d, got=%d", ix, tenant.orderIndex)
		}

		if tenant.maxQueriers == 0 && querierSet != nil {
			return fmt.Errorf("tenant %s has queriers, but maxQueriers=0", tenantID)
		}

		if tenant.maxQueriers > 0 && len(qb.tenantQuerierAssignments.querierIDsSorted) <= tenant.maxQueriers && querierSet != nil {
			return fmt.Errorf("tenant %s has queriers set despite not enough queriers available", tenantID)
		}

		if tenant.maxQueriers > 0 && len(qb.tenantQuerierAssignments.querierIDsSorted) > tenant.maxQueriers && len(querierSet) != tenant.maxQueriers {
			return fmt.Errorf("tenant %s has incorrect number of queriers, expected=%d, got=%d", tenantID, len(querierSet), tenant.maxQueriers)
		}
	}

	tenantQueueCount := qb.queueTree.rootNode.nodeCount() - 1
	if tenantQueueCount != tenantCount {
		//fmt.Printf("tenantIDOrder: %v\n", qb.tenantQuerierAssignments.tenantIDOrder)
		//fmt.Printf("rootNode children: %v\n", qb.queueTree.rootNode.dequeueAlgorithm.getQueueOrder())
		fmt.Println("tenant count check: ", tenantQueueCount, tenantCount)
		return fmt.Errorf("inconsistent number of tenants list and tenant queues")
	}

	return nil
}

func (n *Node) nodeCount() int {
	count := 1
	for _, child := range n.queueMap {
		count += child.nodeCount()
	}
	return count
}
