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

	"github.com/grafana/dskit/httpgrpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/scheduler/queue/tree"
)

func (qb *queueBroker) enqueueObjectsForTests(tenantID string, numObjects int) error {
	for i := 0; i < numObjects; i++ {
		req := &tenantRequest{
			tenantID: tenantID,
			req:      fmt.Sprintf("%v: object-%v", tenantID, i),
		}
		var path tree.QueuePath
		var err error
		if _, ok := qb.tree.(*tree.MultiQueuingAlgorithmTreeQueue); ok {
			if qb.prioritizeQueryComponents {
				path = tree.QueuePath{unknownQueueDimension, tenantID}
			} else {
				path = tree.QueuePath{tenantID, unknownQueueDimension}
			}

		} else {
			path, err = qb.makeQueuePath(req)
			if err != nil {
				return err
			}
		}

		err = qb.tree.EnqueueBackByPath(path, req)
		if err != nil {
			return err
		}
	}
	return nil
}

func buildExpectedObject(tenantID string, num int) *tenantRequest {
	return &tenantRequest{
		tenantID: tenantID,
		req:      fmt.Sprintf("%v: object-%v", tenantID, num),
	}
}

type dequeueVal struct {
	req    *tenantRequest
	tenant *queueTenant
}

func assertExpectedValuesOnDequeue(t *testing.T, qb *queueBroker, lastTenantIndex int, querierID string, expectedVals []dequeueVal) int {
	var req *tenantRequest
	var tenant *queueTenant
	var err error

	for _, expected := range expectedVals {
		req, tenant, lastTenantIndex, err = qb.dequeueRequestForQuerier(&QuerierWorkerDequeueRequest{
			QuerierWorkerConn: &QuerierWorkerConn{QuerierID: querierID},
			lastTenantIndex:   TenantIndex{last: lastTenantIndex},
		})
		assert.Equal(t, expected.req, req)
		assert.Equal(t, expected.tenant, tenant)
		assert.NoError(t, err)
	}
	return lastTenantIndex
}

// TestQueues_NoShuffleSharding tests dequeueing of objects for different queriers, where any querier can
// handle queries for any tenant, as tenant queues are added and removed

func TestQueues_NoShuffleSharding(t *testing.T) {
	treeTypes := buildTreeTestsStruct()
	for _, tt := range treeTypes {
		t.Run(tt.name, func(t *testing.T) {
			qb := newQueueBroker(0, tt.prioritizeQueryComponents, 0)
			assert.NotNil(t, qb)
			assert.NoError(t, isConsistent(qb))

			qb.addQuerierWorkerConn(NewUnregisteredQuerierWorkerConn(context.Background(), "querier-1"))
			qb.addQuerierWorkerConn(NewUnregisteredQuerierWorkerConn(context.Background(), "querier-2"))

			req, tenant, lastTenantIndexQuerierOne, err := qb.dequeueRequestForQuerier(&QuerierWorkerDequeueRequest{
				QuerierWorkerConn: &QuerierWorkerConn{QuerierID: "querier-1"},
				lastTenantIndex:   TenantIndex{-1},
			})
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
			lastTenantIndexQuerierOne = assertExpectedValuesOnDequeue(t, qb, lastTenantIndexQuerierOne, "querier-1", expectedDequeueVals)

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

			lastTenantIndexQuerierOne = assertExpectedValuesOnDequeue(t, qb, lastTenantIndexQuerierOne, "querier-1", expectedDequeueVals)

			expectedDequeueVals = []dequeueVal{
				{buildExpectedObject(tenantOne.tenantID, 4), tenantOne},
				{buildExpectedObject(tenantTwo.tenantID, 2), tenantTwo},
				{buildExpectedObject(tenantOne.tenantID, 5), tenantOne},
			}
			lastTenantIndexQuerierTwo := assertExpectedValuesOnDequeue(t, qb, -1, "querier-2", expectedDequeueVals)

			// [one two three]
			// confirm fifo by adding a third tenant queue and iterating to it
			err = qb.tenantQuerierAssignments.createOrUpdateTenant("three", 0)
			assert.NoError(t, err)

			tenantThree := qb.tenantQuerierAssignments.tenantsByID["three"]

			err = qb.enqueueObjectsForTests(tenantThree.tenantID, 10)
			assert.NoError(t, err)
			assert.NoError(t, isConsistent(qb))

			// lastTenantIndexQuerierOne was 0 (tenantOne) for querier-1; appending
			expectedDequeueVals = []dequeueVal{
				{buildExpectedObject(tenantTwo.tenantID, 3), tenantTwo},
				{buildExpectedObject(tenantThree.tenantID, 0), tenantThree},
				{buildExpectedObject(tenantOne.tenantID, 6), tenantOne},
			}

			lastTenantIndexQuerierOne = assertExpectedValuesOnDequeue(t, qb, lastTenantIndexQuerierOne, "querier-1", expectedDequeueVals)

			// Remove one: ["" two three]
			qb.removeTenantQueue("one")
			assert.NoError(t, isConsistent(qb))

			expectedDequeueVals = []dequeueVal{
				{buildExpectedObject(tenantTwo.tenantID, 4), tenantTwo},
				{buildExpectedObject(tenantThree.tenantID, 1), tenantThree},
				{buildExpectedObject(tenantTwo.tenantID, 5), tenantTwo},
			}
			lastTenantIndexQuerierOne = assertExpectedValuesOnDequeue(t, qb, lastTenantIndexQuerierOne, "querier-1", expectedDequeueVals)

			// "four" is added at the beginning of the list: [four two three]
			err = qb.tenantQuerierAssignments.createOrUpdateTenant("four", 0)
			assert.NoError(t, err)

			tenantFour := qb.tenantQuerierAssignments.tenantsByID["four"]

			err = qb.enqueueObjectsForTests(tenantFour.tenantID, 10)
			assert.NoError(t, err)
			assert.NoError(t, isConsistent(qb))

			expectedDequeueVals = []dequeueVal{
				{buildExpectedObject(tenantTwo.tenantID, 6), tenantTwo},
				{buildExpectedObject(tenantThree.tenantID, 2), tenantThree},
				{buildExpectedObject(tenantFour.tenantID, 0), tenantFour},
				{buildExpectedObject(tenantTwo.tenantID, 7), tenantTwo},
			}

			_ = assertExpectedValuesOnDequeue(t, qb, lastTenantIndexQuerierTwo, "querier-2", expectedDequeueVals)

			// Remove two: [four "" three]
			qb.removeTenantQueue("two")
			assert.NoError(t, isConsistent(qb))

			expectedDequeueVals = []dequeueVal{
				{buildExpectedObject(tenantThree.tenantID, 3), tenantThree},
				{buildExpectedObject(tenantFour.tenantID, 1), tenantFour},
				{buildExpectedObject(tenantThree.tenantID, 4), tenantThree},
			}
			lastTenantIndexQuerierOne = assertExpectedValuesOnDequeue(t, qb, lastTenantIndexQuerierOne, "querier-1", expectedDequeueVals)

			// Remove three: [four ""] )
			qb.removeTenantQueue("three")
			assert.NoError(t, isConsistent(qb))

			// Remove four: []
			qb.removeTenantQueue("four")
			assert.NoError(t, isConsistent(qb))

			req, tenant, _, err = qb.dequeueRequestForQuerier(&QuerierWorkerDequeueRequest{
				QuerierWorkerConn: &QuerierWorkerConn{QuerierID: "querier-1"},
				lastTenantIndex:   TenantIndex{lastTenantIndexQuerierOne},
			},
			)
			assert.Nil(t, req)
			assert.Nil(t, tenant)
			assert.NoError(t, err)

		})
	}
}

func TestQueuesRespectMaxTenantQueueSizeWithSubQueues(t *testing.T) {
	treeTypes := buildTreeTestsStruct()

	for _, tt := range treeTypes {
		t.Run(tt.name, func(t *testing.T) {
			maxTenantQueueSize := 100
			qb := newQueueBroker(maxTenantQueueSize, tt.prioritizeQueryComponents, 0)
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
			queuePath := tree.QueuePath{"tenant-1"}

			var itemCount int
			// if prioritizeQueryComponents, we need to build paths for each queue dimension
			// and sum all items
			if qb.prioritizeQueryComponents {
				for _, addlQueueDim := range additionalQueueDimensions {
					var path tree.QueuePath
					path = append(append(path, addlQueueDim...), "tenant-1")
					if addlQueueDim == nil {
						path = qb.makeQueuePathForTests("tenant-1")
					}
					itemCount += qb.tree.GetNode(path).ItemCount()
				}
				assert.Equal(t, maxTenantQueueSize, itemCount)

			} else {
				assert.Equal(t, maxTenantQueueSize, qb.tree.GetNode(queuePath).ItemCount())
			}

			// assert equal distribution of queue items between 4 subnodes
			for _, v := range additionalQueueDimensions {
				var checkPath tree.QueuePath
				if v == nil {
					v = []string{unknownQueueDimension}
				}
				if qb.prioritizeQueryComponents {
					checkPath = append(append(checkPath, v...), "tenant-1")
				} else {
					checkPath = append(tree.QueuePath{"tenant-1"}, v...)
				}

				dimensionItemCount := qb.tree.GetNode(checkPath).ItemCount()
				assert.Equal(t, maxTenantQueueSize/len(additionalQueueDimensions), dimensionItemCount)
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
			qb.addQuerierWorkerConn(NewUnregisteredQuerierWorkerConn(context.Background(), "querier-1"))
			dequeuedTenantReq, _, _, err := qb.dequeueRequestForQuerier(&QuerierWorkerDequeueRequest{
				QuerierWorkerConn: &QuerierWorkerConn{QuerierID: "querier-1"},
				lastTenantIndex:   TenantIndex{-1},
			})
			assert.NoError(t, err)
			assert.NotNil(t, dequeuedTenantReq)

			tenantReq := &tenantRequest{tenantID: "tenant-1", req: req}
			// assert not hitting an error when enqueueing after dequeuing to below the limit
			err = qb.enqueueRequestBack(tenantReq, 0)
			assert.NoError(t, err)

			// we then hit an error again, as we are back at the limit
			err = qb.enqueueRequestBack(tenantReq, 0)
			assert.ErrorIs(t, err, ErrTooManyRequests)
		})
	}
}

func TestQueuesOnTerminatingQuerier(t *testing.T) {
	treeTypes := buildTreeTestsStruct()
	for _, tt := range treeTypes {
		t.Run(tt.name, func(t *testing.T) {
			qb := newQueueBroker(0, tt.prioritizeQueryComponents, 0)
			assert.NotNil(t, qb)
			assert.NoError(t, isConsistent(qb))

			qb.addQuerierWorkerConn(NewUnregisteredQuerierWorkerConn(context.Background(), "querier-1"))
			qb.addQuerierWorkerConn(NewUnregisteredQuerierWorkerConn(context.Background(), "querier-2"))

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

			expectedDequeueVals = []dequeueVal{
				{buildExpectedObject(tenantOne.tenantID, 2), tenantOne},
				{buildExpectedObject(tenantTwo.tenantID, 2), tenantTwo},
				{buildExpectedObject(tenantOne.tenantID, 3), tenantOne},
				{buildExpectedObject(tenantTwo.tenantID, 3), tenantTwo},
			}
			qTwolastTenantIndex := assertExpectedValuesOnDequeue(t, qb, -1, "querier-2", expectedDequeueVals)

			// After notify shutdown for querier-2, it's expected to own no queue.
			qb.notifyQuerierShutdown("querier-2")
			req, tenant, qTwolastTenantIndex, err := qb.dequeueRequestForQuerier(&QuerierWorkerDequeueRequest{
				QuerierWorkerConn: &QuerierWorkerConn{QuerierID: "querier-2"},
				lastTenantIndex:   TenantIndex{qTwolastTenantIndex},
			})
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
				req, tenant, qOneLastTenantIndex, err = qb.dequeueRequestForQuerier(&QuerierWorkerDequeueRequest{
					QuerierWorkerConn: &QuerierWorkerConn{QuerierID: "querier-1"},
					lastTenantIndex:   TenantIndex{qOneLastTenantIndex},
				})
				assert.Equal(t, expected.req, req)
				assert.Equal(t, expected.tenant, tenant)
				assert.NoError(t, err)
			}

			// After disconnecting querier-2, it's expected to own no queue.
			qb.tenantQuerierAssignments.removeQueriers("querier-2")
			req, tenant, _, err = qb.dequeueRequestForQuerier(&QuerierWorkerDequeueRequest{
				QuerierWorkerConn: &QuerierWorkerConn{QuerierID: "querier-2"},
				lastTenantIndex:   TenantIndex{qTwolastTenantIndex},
			})
			assert.Nil(t, req)
			assert.Nil(t, tenant)
			assert.Equal(t, ErrQuerierShuttingDown, err)

		})
	}
}

func TestQueues_QuerierDistribution(t *testing.T) {
	treeTypes := buildTreeTestsStruct()
	for _, tt := range treeTypes {
		t.Run(tt.name, func(t *testing.T) {
			qb := newQueueBroker(0, tt.prioritizeQueryComponents, 0)
			assert.NotNil(t, qb)
			assert.NoError(t, isConsistent(qb))

			queriers := 30
			numTenants := 1000
			maxQueriersPerTenant := 5

			// Add some queriers.
			for ix := 0; ix < queriers; ix++ {
				qid := fmt.Sprintf("querier-%d", ix)
				qb.addQuerierWorkerConn(NewUnregisteredQuerierWorkerConn(context.Background(), qid))

				// No querier has any queues yet.
				req, tenant, _, err := qb.dequeueRequestForQuerier(&QuerierWorkerDequeueRequest{
					QuerierWorkerConn: &QuerierWorkerConn{QuerierID: qid},
					lastTenantIndex:   TenantIndex{-1},
				})
				assert.Nil(t, req)
				assert.Nil(t, tenant)
				assert.NoError(t, err)
			}

			assert.NoError(t, isConsistent(qb))

			// Add tenant queues.
			for i := 0; i < numTenants; i++ {
				uid := fmt.Sprintf("tenant-%d", i)
				err := qb.tenantQuerierAssignments.createOrUpdateTenant(uid, maxQueriersPerTenant)
				assert.NoError(t, err)

				//Enqueue some stuff so that the tree queue node exists
				err = qb.enqueueObjectsForTests(uid, 1)
				assert.NoError(t, err)

				// Verify it has maxQueriersPerTenant queriers assigned now.
				qs := qb.tenantQuerierAssignments.queriersForTenant(uid)
				assert.Equal(t, maxQueriersPerTenant, len(qs))
			}

			// After adding all tenants, verify results. For each querier, find out how many different tenants it handles,
			// and compute mean and stdDev.
			queriersMap := make(map[tree.QuerierID]int)

			for tenantID := range qb.tenantQuerierAssignments.tenantsByID {
				querierSet := qb.tenantQuerierAssignments.queriersForTenant(tenantID)
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
		})
	}
}

func TestQueuesConsistency(t *testing.T) {
	treeTypes := buildTreeTestsStruct()
	tests := map[string]struct {
		forgetDelay time.Duration
	}{
		"without forget delay": {},
		"with forget delay":    {forgetDelay: time.Minute},
	}

	for _, tt := range treeTypes {
		t.Run(tt.name, func(t *testing.T) {
			for testName, testData := range tests {
				t.Run(testName, func(t *testing.T) {
					qb := newQueueBroker(0, tt.prioritizeQueryComponents, testData.forgetDelay)
					assert.NotNil(t, qb)
					assert.NoError(t, isConsistent(qb))

					r := rand.New(rand.NewSource(time.Now().Unix()))

					// maps querier IDs to tenant indexes for that querier.
					lastTenantIndexes := map[string]int{}

					// track active querier-worker connections to use worker IDs for removal
					conns := map[string][]*QuerierWorkerConn{}

					for i := 0; i < 100; i++ {
						switch r.Int() % 6 {
						case 0:
							err := getOrAddTenantQueue(qb, generateTenant(r), 3)
							assert.Nil(t, err)
						case 1:
							querierID := generateQuerier(r)
							tenantIndex := getNextTenantForQuerier(qb, lastTenantIndexes[querierID], querierID)
							lastTenantIndexes[querierID] = tenantIndex
						case 2:
							qb.removeTenantQueue(generateTenant(r))
						case 3:
							querierID := generateQuerier(r)
							conn := NewUnregisteredQuerierWorkerConn(context.Background(), querierID)
							qb.addQuerierWorkerConn(conn)
							conns[querierID] = append(conns[querierID], conn)
						case 4:
							querierID := generateQuerier(r)
							if len(conns[querierID]) > 0 {
								// does not matter which connection is removed; just choose the last one in the list
								conn := conns[querierID][len(conns[querierID])-1]
								qb.removeQuerierWorkerConn(conn, time.Now())
								// slice removed connection off end of tracking list
								conns[querierID] = conns[querierID][:len(conns[querierID])-1]
							}
						case 5:
							q := generateQuerier(r)
							qb.notifyQuerierShutdown(q)
						}

						assert.NoErrorf(t, isConsistent(qb), "last action %d, rand: %d", i, r.Int()%6)
					}
				})
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

	treeTypes := buildTreeTestsStruct()

	for _, tt := range treeTypes {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now()
			qb := newQueueBroker(0, tt.prioritizeQueryComponents, forgetDelay)
			assert.NotNil(t, qb)
			assert.NoError(t, isConsistent(qb))

			// 3 queriers open 2 connections each.
			querier1Conn1 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-1")
			qb.addQuerierWorkerConn(querier1Conn1)
			querier1Conn2 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-1")
			qb.addQuerierWorkerConn(querier1Conn2)

			querier2Conn1 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-2")
			qb.addQuerierWorkerConn(querier2Conn1)
			querier2Conn2 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-2")
			qb.addQuerierWorkerConn(querier2Conn2)

			querier3Conn1 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-3")
			qb.addQuerierWorkerConn(querier3Conn1)
			querier3Conn2 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-3")
			qb.addQuerierWorkerConn(querier3Conn2)

			// Add tenant queues.
			for i := 0; i < numTenants; i++ {
				tenantID := fmt.Sprintf("tenant-%d", i)
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
			qb.removeQuerierWorkerConn(querier1Conn1, now.Add(20*time.Second))
			qb.removeQuerierWorkerConn(querier1Conn2, now.Add(21*time.Second))
			qb.notifyQuerierShutdown("querier-1")

			// We expect querier-1 has been removed.
			assert.NotContains(t, qb.querierConnections.queriersByID, "querier-1")
			assert.NoError(t, isConsistent(qb))

			// We expect querier-1 tenants have been shuffled to other queriers.
			for _, tenantID := range querier1Tenants {
				assert.Contains(t, append(getTenantsByQuerier(qb, "querier-2"), getTenantsByQuerier(qb, "querier-3")...), tenantID)
			}

			// Querier-1 reconnects.
			qb.addQuerierWorkerConn(querier1Conn1)
			qb.addQuerierWorkerConn(querier1Conn2)

			// We expect the initial querier-1 tenants have got back to querier-1.
			for _, tenantID := range querier1Tenants {
				assert.Contains(t, getTenantsByQuerier(qb, "querier-1"), tenantID)
				assert.NotContains(t, getTenantsByQuerier(qb, "querier-2"), tenantID)
				assert.NotContains(t, getTenantsByQuerier(qb, "querier-3"), tenantID)
			}

			// Querier-1 abruptly terminates (no shutdown notification received).
			qb.removeQuerierWorkerConn(querier1Conn1, now.Add(40*time.Second))
			qb.removeQuerierWorkerConn(querier1Conn2, now.Add(41*time.Second))

			// We expect querier-1 has NOT been removed.
			assert.Contains(t, qb.querierConnections.queriersByID, "querier-1")
			assert.NoError(t, isConsistent(qb))

			// We expect the querier-1 tenants have not been shuffled to other queriers.
			for _, tenantID := range querier1Tenants {
				assert.Contains(t, getTenantsByQuerier(qb, "querier-1"), tenantID)
				assert.NotContains(t, getTenantsByQuerier(qb, "querier-2"), tenantID)
				assert.NotContains(t, getTenantsByQuerier(qb, "querier-3"), tenantID)
			}

			// Try to forget disconnected queriers, but querier-1 forget delay hasn't passed yet.
			qb.forgetDisconnectedQueriers(now.Add(90 * time.Second))

			assert.Contains(t, qb.querierConnections.queriersByID, "querier-1")
			assert.NoError(t, isConsistent(qb))

			for _, tenantID := range querier1Tenants {
				assert.Contains(t, getTenantsByQuerier(qb, "querier-1"), tenantID)
				assert.NotContains(t, getTenantsByQuerier(qb, "querier-2"), tenantID)
				assert.NotContains(t, getTenantsByQuerier(qb, "querier-3"), tenantID)
			}

			// Try to forget disconnected queriers. This time querier-1 forget delay has passed.
			qb.forgetDisconnectedQueriers(now.Add(105 * time.Second))

			assert.NotContains(t, qb.querierConnections.queriersByID, "querier-1")
			assert.NoError(t, isConsistent(qb))

			// We expect querier-1 tenants have been shuffled to other queriers.
			for _, tenantID := range querier1Tenants {
				assert.Contains(t, append(getTenantsByQuerier(qb, "querier-2"), getTenantsByQuerier(qb, "querier-3")...), tenantID)
			}
		})
	}
}

func TestQueues_ForgetDelay_ShouldCorrectlyHandleQuerierReconnectingBeforeForgetDelayIsPassed(t *testing.T) {
	const (
		forgetDelay          = time.Minute
		maxQueriersPerTenant = 1
		numTenants           = 100
	)

	treeTypes := buildTreeTestsStruct()

	for _, tt := range treeTypes {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now()
			qb := newQueueBroker(0, tt.prioritizeQueryComponents, forgetDelay)
			assert.NotNil(t, qb)
			assert.NoError(t, isConsistent(qb))

			// 3 queriers open 2 connections each.
			querier1Conn1 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-1")
			qb.addQuerierWorkerConn(querier1Conn1)
			querier1Conn2 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-1")
			qb.addQuerierWorkerConn(querier1Conn2)

			querier2Conn1 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-2")
			qb.addQuerierWorkerConn(querier2Conn1)
			querier2Conn2 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-2")
			qb.addQuerierWorkerConn(querier2Conn2)

			querier3Conn1 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-3")
			qb.addQuerierWorkerConn(querier3Conn1)
			querier3Conn2 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-3")
			qb.addQuerierWorkerConn(querier3Conn2)

			// Add tenant queues.
			for i := 0; i < numTenants; i++ {
				tenantID := fmt.Sprintf("tenant-%d", i)
				err := qb.tenantQuerierAssignments.createOrUpdateTenant(tenantID, maxQueriersPerTenant)
				assert.NoError(t, err)

				//Enqueue some stuff so that the tree queue node exists
				err = qb.enqueueObjectsForTests(tenantID, 1)
				assert.NoError(t, err)
			}

			// We expect querier-1 to have some tenants.
			querier1Tenants := getTenantsByQuerier(qb, "querier-1")
			require.NotEmpty(t, querier1Tenants)

			// Querier-1 abruptly terminates (no shutdown notification received).
			qb.removeQuerierWorkerConn(querier1Conn1, now.Add(40*time.Second))
			qb.removeQuerierWorkerConn(querier1Conn2, now.Add(41*time.Second))

			// We expect querier-1 has NOT been removed.
			assert.Contains(t, qb.querierConnections.queriersByID, "querier-1")
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
			qb.addQuerierWorkerConn(querier1Conn1)
			qb.addQuerierWorkerConn(querier1Conn2)

			assert.Contains(t, qb.querierConnections.queriersByID, "querier-1")
			assert.NoError(t, isConsistent(qb))

			// We expect the querier-1 tenants have not been shuffled to other queriers.
			for _, tenantID := range querier1Tenants {
				assert.Contains(t, getTenantsByQuerier(qb, "querier-1"), tenantID)
				assert.NotContains(t, getTenantsByQuerier(qb, "querier-2"), tenantID)
				assert.NotContains(t, getTenantsByQuerier(qb, "querier-3"), tenantID)
			}

			// Try to forget disconnected queriers far in the future, but there's no disconnected querier.
			qb.forgetDisconnectedQueriers(now.Add(200 * time.Second))

			assert.Contains(t, qb.querierConnections.queriersByID, "querier-1")
			assert.NoError(t, isConsistent(qb))

			for _, tenantID := range querier1Tenants {
				assert.Contains(t, getTenantsByQuerier(qb, "querier-1"), tenantID)
				assert.NotContains(t, getTenantsByQuerier(qb, "querier-2"), tenantID)
				assert.NotContains(t, getTenantsByQuerier(qb, "querier-3"), tenantID)
			}

		})
	}
}

func generateTenant(r *rand.Rand) string {
	return fmt.Sprint("tenant-", r.Int()%5)
}

func generateQuerier(r *rand.Rand) string {
	return fmt.Sprint("querier-", r.Int()%5)
}

// getTenantsByQuerier returns the list of tenants handled by the provided QuerierID.

func getTenantsByQuerier(broker *queueBroker, querierID string) []string {
	var tenantIDs []string
	for _, tenantID := range broker.tenantQuerierAssignments.queuingAlgorithm.TenantIDOrder() {
		querierSet := broker.tenantQuerierAssignments.queriersForTenant(tenantID)
		if querierSet == nil {
			// If it's nil then all queriers can handle this tenant.
			tenantIDs = append(tenantIDs, tenantID)
			continue
		}
		if _, ok := querierSet[tree.QuerierID(querierID)]; ok {
			tenantIDs = append(tenantIDs, tenantID)
		}
	}
	return tenantIDs
}

func getOrAddTenantQueue(qb *queueBroker, tenantID string, maxQueriers int) error {
	err := qb.tenantQuerierAssignments.createOrUpdateTenant(tenantID, maxQueriers)
	if err != nil {
		return err
	}

	queuePath := qb.makeQueuePathForTests(tenantID)
	return tree.GetOrAddNode(queuePath, qb.tree)
}

// removeTenantQueue is a test utility, not intended for use by consumers of queueBroker
func (qb *queueBroker) removeTenantQueue(tenantID string) bool {
	qb.tenantQuerierAssignments.removeTenant(tenantID)
	queuePath := qb.makeQueuePathForTests(tenantID)

	tree.DeleteNode(tree.RootNode(qb.tree.(*tree.MultiQueuingAlgorithmTreeQueue)), queuePath)

	return false
}

func (qb *queueBroker) makeQueuePathForTests(tenantID string) tree.QueuePath {
	if qb.prioritizeQueryComponents {
		return tree.QueuePath{unknownQueueDimension, tenantID}
	}
	return tree.QueuePath{tenantID}
}

func isConsistent(qb *queueBroker) error {
	if len(qb.tenantQuerierAssignments.querierIDsSorted) != len(qb.querierConnections.queriersByID) {
		return fmt.Errorf("inconsistent number of sorted queriers and querier connections")
	}

	tenantCount := 0
	existingTenants := make(map[string]bool)

	for ix, tenantID := range qb.tenantQuerierAssignments.queuingAlgorithm.TenantIDOrder() {
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

		tenant := qb.tenantQuerierAssignments.tenantsByID[tenantID]
		querierSet := qb.tenantQuerierAssignments.queriersForTenant(tenantID)

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

	var tenantQueueCount int
	if itq, ok := qb.tree.(*tree.MultiQueuingAlgorithmTreeQueue); ok {
		tenantQueueCount = tree.TenantQueueCount(itq)
	}
	if tenantQueueCount != tenantCount {
		return fmt.Errorf("inconsistent number of tenants list and tenant queues")
	}

	return nil
}

// The next tenant for the querier is obtained by rotating through the global tenant order
// starting just after the last tenant the querier received a request for, until a tenant
// is found that is assigned to the given querier according to the querier shuffle sharding.
// A newly connected querier provides lastTenantIndex of -1 in order to start at the beginning.
func getNextTenantForQuerier(qb *queueBroker, lastTenantIndex int, querierID string) int {
	if !qb.querierConnections.querierIsAvailable(querierID) {
		return lastTenantIndex
	}
	tenantOrderIndex := lastTenantIndex
	tenantIDOrder := qb.tenantQuerierAssignments.queuingAlgorithm.TenantIDOrder()
	for iters := 0; iters < len(tenantIDOrder); iters++ {
		tenantOrderIndex++
		if tenantOrderIndex >= len(tenantIDOrder) {
			tenantOrderIndex = 0
		}
		tenantID := tenantIDOrder[tenantOrderIndex]
		if tenantID == "" {
			continue
		}

		tenantQuerierSet := qb.tenantQuerierAssignments.queriersForTenant(tenantID)
		if tenantQuerierSet == nil {
			// tenant can use all queriers
			return tenantOrderIndex
		} else if _, ok := tenantQuerierSet[tree.QuerierID(querierID)]; ok {
			// tenant is assigned this querier
			return tenantOrderIndex
		}
	}
	return lastTenantIndex
}
