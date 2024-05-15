// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/user_queues_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queue

import (
	"container/list"
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	// Add queues: [one]
	qOne := getOrAdd(t, qb, "one", 0)
	lastTenantIndex = confirmOrderForQuerier(t, qb, "querier-1", lastTenantIndex, qOne, qOne)

	// [one two]
	qTwo := getOrAdd(t, qb, "two", 0)

	lastTenantIndex = confirmOrderForQuerier(t, qb, "querier-1", lastTenantIndex, qTwo, qOne, qTwo, qOne)
	confirmOrderForQuerier(t, qb, "querier-2", -1, qOne, qTwo, qOne)

	// [one two three]
	// confirm fifo by adding a third queue and iterating to it
	qThree := getOrAdd(t, qb, "three", 0)

	lastTenantIndex = confirmOrderForQuerier(t, qb, "querier-1", lastTenantIndex, qTwo, qThree, qOne)

	// Remove one: ["" two three]
	qb.removeTenantQueue("one")
	assert.NoError(t, isConsistent(qb))

	lastTenantIndex = confirmOrderForQuerier(t, qb, "querier-1", lastTenantIndex, qTwo, qThree, qTwo)

	// "four" is added at the beginning of the list: [four two three]
	qFour := getOrAdd(t, qb, "four", 0)

	lastTenantIndex = confirmOrderForQuerier(t, qb, "querier-1", lastTenantIndex, qThree, qFour, qTwo, qThree)

	// Remove two: [four "" three]
	qb.removeTenantQueue("two")
	assert.NoError(t, isConsistent(qb))

	lastTenantIndex = confirmOrderForQuerier(t, qb, "querier-1", lastTenantIndex, qFour, qThree, qFour)

	// Remove three: [four]
	qb.removeTenantQueue("three")
	assert.NoError(t, isConsistent(qb))

	// Remove four: []
	qb.removeTenantQueue("four")
	assert.NoError(t, isConsistent(qb))

	req, _, _, err = qb.dequeueRequestForQuerier(lastTenantIndex, "querier-1")
	assert.Nil(t, req)
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
		Ctx:             context.Background(),
		FrontendAddress: "http://query-frontend:8007",
		UserID:          "tenant-1",
		Request:         &httpgrpc.HTTPRequest{},
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
	assert.Equal(t, maxTenantQueueSize, qb.tenantQueuesTree.getNode(queuePath).ItemCount())

	// assert equal distribution of queue items between tenant node and 3 subnodes
	for _, v := range additionalQueueDimensions {
		queuePath := append(QueuePath{"tenant-1"}, v...)
		assert.Equal(t, maxTenantQueueSize/len(additionalQueueDimensions), qb.tenantQueuesTree.getNode(queuePath).LocalQueueLen())
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

	// Add queues: [one, two]
	qOne := getOrAdd(t, qb, "one", 0)
	qTwo := getOrAdd(t, qb, "two", 0)
	confirmOrderForQuerier(t, qb, "querier-1", -1, qOne, qTwo, qOne, qTwo)
	confirmOrderForQuerier(t, qb, "querier-2", -1, qOne, qTwo, qOne, qTwo)

	// After notify shutdown for querier-2, it's expected to own no queue.
	qb.notifyQuerierShutdown("querier-2")
	tenant, _, err := qb.tenantQuerierAssignments.getNextTenantForQuerier(-1, "querier-2")
	assert.Nil(t, tenant)
	assert.Equal(t, ErrQuerierShuttingDown, err)

	// However, querier-1 still get queues because it's still running.
	confirmOrderForQuerier(t, qb, "querier-1", -1, qOne, qTwo, qOne, qTwo)

	// After disconnecting querier-2, it's expected to own no queue.
	qb.tenantQuerierAssignments.removeQuerier("querier-2")
	tenant, _, err = qb.tenantQuerierAssignments.getNextTenantForQuerier(-1, "querier-2")
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
		getOrAdd(t, qb, uid, maxQueriersPerTenant)

		// Verify it has maxQueriersPerTenant queriers assigned now.
		qs := qb.tenantQuerierAssignments.tenantQuerierIDs[uid]
		assert.Equal(t, maxQueriersPerTenant, len(qs))
	}

	// After adding all tenants, verify results. For each querier, find out how many different tenants it handles,
	// and compute mean and stdDev.
	queriersMap := make(map[QuerierID]int)

	for q := 0; q < queriers; q++ {
		qid := QuerierID(fmt.Sprintf("querier-%d", q))

		lastTenantIndex := -1
		for {
			_, newIx, err := qb.tenantQuerierAssignments.getNextTenantForQuerier(lastTenantIndex, qid)
			assert.NoError(t, err)
			if newIx < lastTenantIndex {
				break
			}
			lastTenantIndex = newIx
			queriersMap[qid]++
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

				assert.NoErrorf(t, isConsistent(qb), "last action %d", i)
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
		getOrAdd(t, qb, tenantID, maxQueriersPerTenant)
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
		getOrAdd(t, qb, tenantID, maxQueriersPerTenant)
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

func getOrAdd(t *testing.T, qb *queueBroker, tenantID TenantID, maxQueriers int) *list.List {
	addedQueue, err := qb.getOrAddTenantQueue(tenantID, maxQueriers)
	assert.Nil(t, err)
	assert.NotNil(t, addedQueue)
	assert.NoError(t, isConsistent(qb))
	reAddedQueue, err := qb.getOrAddTenantQueue(tenantID, maxQueriers)
	assert.Nil(t, err)
	assert.Equal(t, addedQueue, reAddedQueue)
	return addedQueue.localQueue
}

// getOrAddTenantQueue is a test utility, not intended for use by consumers of queueBroker
func (qb *queueBroker) getOrAddTenantQueue(tenantID TenantID, maxQueriers int) (*TreeQueue, error) {
	err := qb.tenantQuerierAssignments.createOrUpdateTenant(tenantID, maxQueriers)
	if err != nil {
		return nil, err
	}

	queuePath := QueuePath{string(tenantID)}
	return qb.tenantQueuesTree.getOrAddNode(queuePath)
}

// getQueue is a test utility, not intended for use by consumers of queueBroker
func (qb *queueBroker) getQueue(tenantID TenantID) *TreeQueue {
	tenant, err := qb.tenantQuerierAssignments.getTenant(tenantID)
	if tenant == nil || err != nil {
		return nil
	}

	queuePath := QueuePath{string(tenantID)}
	tenantQueue := qb.tenantQueuesTree.getNode(queuePath)
	return tenantQueue
}

// removeTenantQueue is a test utility, not intended for use by consumers of queueBroker
func (qb *queueBroker) removeTenantQueue(tenantID TenantID) bool {
	qb.tenantQuerierAssignments.removeTenant(tenantID)
	queuePath := QueuePath{string(tenantID)}
	return qb.tenantQueuesTree.deleteNode(queuePath)
}

func confirmOrderForQuerier(t *testing.T, qb *queueBroker, querier QuerierID, lastTenantIndex int, queues ...*list.List) int {
	for _, queue := range queues {
		var err error
		tenant, _, err := qb.tenantQuerierAssignments.getNextTenantForQuerier(lastTenantIndex, querier)
		tenantQueue := qb.getQueue(tenant.tenantID)
		assert.Equal(t, queue, tenantQueue.localQueue)
		assert.NoError(t, isConsistent(qb))
		assert.NoError(t, err)
	}
	return lastTenantIndex
}

func isConsistent(qb *queueBroker) error {
	if len(qb.tenantQuerierAssignments.querierIDsSorted) != len(qb.tenantQuerierAssignments.queriersByID) {
		return fmt.Errorf("inconsistent number of sorted queriers and querier connections")
	}

	tenantCount := 0
	for ix, tenantID := range qb.tenantQuerierAssignments.tenantIDOrder {
		if tenantID != "" && qb.getQueue(tenantID) == nil {
			return fmt.Errorf("tenant %s doesn't have queue", tenantID)
		}
		if tenantID == "" && qb.getQueue(tenantID) != nil {
			return fmt.Errorf("tenant %s shouldn't have queue", tenantID)
		}
		if tenantID == "" {
			continue
		}
		tenantCount++

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

	tenantQueueCount := qb.tenantQueuesTree.NodeCount() - 1 // exclude root node
	if tenantCount != tenantQueueCount {
		return fmt.Errorf("inconsistent number of tenants list and tenant queues")
	}

	return nil
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
