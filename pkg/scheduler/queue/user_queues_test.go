// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/user_queues_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queue

import (
	"container/list"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueues(t *testing.T) {
	uq := newQueueBroker(0, 0)
	assert.NotNil(t, uq)
	assert.NoError(t, isConsistent(uq))

	uq.addQuerierConnection("querier-1")
	uq.addQuerierConnection("querier-2")

	q, u, lastUserIndex, err := uq.getNextQueueForQuerier(-1, "querier-1")
	assert.Nil(t, q)
	assert.Equal(t, emptyTenantID, u)
	assert.NoError(t, err)

	// Add queues: [one]
	qOne := getOrAdd(t, uq, "one", 0)
	lastUserIndex = confirmOrderForQuerier(t, uq, "querier-1", lastUserIndex, qOne, qOne)

	// [one two]
	qTwo := getOrAdd(t, uq, "two", 0)
	assert.NotSame(t, qOne, qTwo)

	lastUserIndex = confirmOrderForQuerier(t, uq, "querier-1", lastUserIndex, qTwo, qOne, qTwo, qOne)
	confirmOrderForQuerier(t, uq, "querier-2", -1, qOne, qTwo, qOne)

	// [one two three]
	// confirm fifo by adding a third queue and iterating to it
	qThree := getOrAdd(t, uq, "three", 0)

	lastUserIndex = confirmOrderForQuerier(t, uq, "querier-1", lastUserIndex, qTwo, qThree, qOne)

	// Remove one: ["" two three]
	uq.deleteQueue("one")
	assert.NoError(t, isConsistent(uq))

	lastUserIndex = confirmOrderForQuerier(t, uq, "querier-1", lastUserIndex, qTwo, qThree, qTwo)

	// "four" is added at the beginning of the list: [four two three]
	qFour := getOrAdd(t, uq, "four", 0)

	lastUserIndex = confirmOrderForQuerier(t, uq, "querier-1", lastUserIndex, qThree, qFour, qTwo, qThree)

	// Remove two: [four "" three]
	uq.deleteQueue("two")
	assert.NoError(t, isConsistent(uq))

	lastUserIndex = confirmOrderForQuerier(t, uq, "querier-1", lastUserIndex, qFour, qThree, qFour)

	// Remove three: [four]
	uq.deleteQueue("three")
	assert.NoError(t, isConsistent(uq))

	// Remove four: []
	uq.deleteQueue("four")
	assert.NoError(t, isConsistent(uq))

	q, _, _, err = uq.getNextQueueForQuerier(lastUserIndex, "querier-1")
	assert.Nil(t, q)
	assert.NoError(t, err)
}

func TestQueuesOnTerminatingQuerier(t *testing.T) {
	uq := newQueueBroker(0, 0)
	assert.NotNil(t, uq)
	assert.NoError(t, isConsistent(uq))

	uq.addQuerierConnection("querier-1")
	uq.addQuerierConnection("querier-2")

	// Add queues: [one, two]
	qOne := getOrAdd(t, uq, "one", 0)
	qTwo := getOrAdd(t, uq, "two", 0)
	confirmOrderForQuerier(t, uq, "querier-1", -1, qOne, qTwo, qOne, qTwo)
	confirmOrderForQuerier(t, uq, "querier-2", -1, qOne, qTwo, qOne, qTwo)

	// After notify shutdown for querier-2, it's expected to own no queue.
	uq.notifyQuerierShutdown("querier-2")
	q, u, _, err := uq.getNextQueueForQuerier(-1, "querier-2")
	assert.Nil(t, q)
	assert.Equal(t, emptyTenantID, u)
	assert.Equal(t, ErrQuerierShuttingDown, err)

	// However, querier-1 still get queues because it's still running.
	confirmOrderForQuerier(t, uq, "querier-1", -1, qOne, qTwo, qOne, qTwo)

	// After disconnecting querier-2, it's expected to own no queue.
	uq.tenantQuerierAssignments.removeQuerier("querier-2")
	q, u, _, err = uq.getNextQueueForQuerier(-1, "querier-2")
	assert.Nil(t, q)
	assert.Equal(t, emptyTenantID, u)
	assert.Equal(t, ErrQuerierShuttingDown, err)
}

func TestQueuesWithQueriers(t *testing.T) {
	uq := newQueueBroker(0, 0)
	assert.NotNil(t, uq)
	assert.NoError(t, isConsistent(uq))

	queriers := 30
	users := 1000
	maxQueriersPerUser := 5

	// Add some queriers.
	for ix := 0; ix < queriers; ix++ {
		qid := QuerierID(fmt.Sprintf("querier-%d", ix))
		uq.addQuerierConnection(qid)

		// No querier has any queues yet.
		q, u, _, err := uq.getNextQueueForQuerier(-1, qid)
		assert.Nil(t, q)
		assert.Equal(t, emptyTenantID, u)
		assert.NoError(t, err)
	}

	assert.NoError(t, isConsistent(uq))

	// Add user queues.
	for u := 0; u < users; u++ {
		uid := TenantID(fmt.Sprintf("user-%d", u))
		getOrAdd(t, uq, uid, maxQueriersPerUser)

		// Verify it has maxQueriersPerUser queriers assigned now.
		qs := uq.tenantQuerierAssignments.tenantQuerierIDs[uid]
		assert.Equal(t, maxQueriersPerUser, len(qs))
	}

	// After adding all users, verify results. For each querier, find out how many different users it handles,
	// and compute mean and stdDev.
	queriersMap := make(map[QuerierID]int)

	for q := 0; q < queriers; q++ {
		qid := QuerierID(fmt.Sprintf("querier-%d", q))

		lastUserIndex := -1
		for {
			_, _, newIx, err := uq.getNextQueueForQuerier(lastUserIndex, qid)
			assert.NoError(t, err)
			if newIx < lastUserIndex {
				break
			}
			lastUserIndex = newIx
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

	assert.InDelta(t, users*maxQueriersPerUser/queriers, mean, 1)
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
			uq := newQueueBroker(0, testData.forgetDelay)
			assert.NotNil(t, uq)
			assert.NoError(t, isConsistent(uq))

			r := rand.New(rand.NewSource(time.Now().Unix()))

			lastUserIndexes := map[QuerierID]int{}

			conns := map[QuerierID]int{}

			for i := 0; i < 10000; i++ {
				switch r.Int() % 6 {
				case 0:
					assert.NotNil(t, uq.getOrAddTenantQueue(generateTenant(r), 3))
				case 1:
					qid := generateQuerier(r)
					_, _, luid, _ := uq.getNextQueueForQuerier(lastUserIndexes[qid], qid)
					lastUserIndexes[qid] = luid
				case 2:
					uq.deleteQueue(generateTenant(r))
				case 3:
					q := generateQuerier(r)
					uq.addQuerierConnection(q)
					conns[q]++
				case 4:
					q := generateQuerier(r)
					if conns[q] > 0 {
						uq.removeQuerierConnection(q, time.Now())
						conns[q]--
					}
				case 5:
					q := generateQuerier(r)
					uq.notifyQuerierShutdown(q)
				}

				assert.NoErrorf(t, isConsistent(uq), "last action %d", i)
			}
		})
	}
}

func TestQueues_ForgetDelay(t *testing.T) {
	const (
		forgetDelay        = time.Minute
		maxQueriersPerUser = 1
		numUsers           = 100
	)

	now := time.Now()
	uq := newQueueBroker(0, forgetDelay)
	assert.NotNil(t, uq)
	assert.NoError(t, isConsistent(uq))

	// 3 queriers open 2 connections each.
	for i := 1; i <= 3; i++ {
		uq.addQuerierConnection(QuerierID(fmt.Sprintf("querier-%d", i)))
		uq.addQuerierConnection(QuerierID(fmt.Sprintf("querier-%d", i)))
	}

	// Add user queues.
	for i := 0; i < numUsers; i++ {
		tenantID := TenantID(fmt.Sprintf("user-%d", i))
		getOrAdd(t, uq, tenantID, maxQueriersPerUser)
	}

	// We expect querier-1 to have some users.
	querier1Users := getTenantsByQuerier(uq, "querier-1")
	require.NotEmpty(t, querier1Users)

	// Gracefully shutdown querier-1.
	uq.removeQuerierConnection("querier-1", now.Add(20*time.Second))
	uq.removeQuerierConnection("querier-1", now.Add(21*time.Second))
	uq.notifyQuerierShutdown("querier-1")

	// We expect querier-1 has been removed.
	assert.NotContains(t, uq.tenantQuerierAssignments.queriersByID, "querier-1")
	assert.NoError(t, isConsistent(uq))

	// We expect querier-1 users have been shuffled to other queriers.
	for _, userID := range querier1Users {
		assert.Contains(t, append(getTenantsByQuerier(uq, "querier-2"), getTenantsByQuerier(uq, "querier-3")...), userID)
	}

	// Querier-1 reconnects.
	uq.addQuerierConnection("querier-1")
	uq.addQuerierConnection("querier-1")

	// We expect the initial querier-1 users have got back to querier-1.
	for _, userID := range querier1Users {
		assert.Contains(t, getTenantsByQuerier(uq, "querier-1"), userID)
		assert.NotContains(t, getTenantsByQuerier(uq, "querier-2"), userID)
		assert.NotContains(t, getTenantsByQuerier(uq, "querier-3"), userID)
	}

	// Querier-1 abruptly terminates (no shutdown notification received).
	uq.removeQuerierConnection("querier-1", now.Add(40*time.Second))
	uq.removeQuerierConnection("querier-1", now.Add(41*time.Second))

	// We expect querier-1 has NOT been removed.
	assert.Contains(t, uq.tenantQuerierAssignments.queriersByID, QuerierID("querier-1"))
	assert.NoError(t, isConsistent(uq))

	// We expect the querier-1 users have not been shuffled to other queriers.
	for _, userID := range querier1Users {
		assert.Contains(t, getTenantsByQuerier(uq, "querier-1"), userID)
		assert.NotContains(t, getTenantsByQuerier(uq, "querier-2"), userID)
		assert.NotContains(t, getTenantsByQuerier(uq, "querier-3"), userID)
	}

	// Try to forget disconnected queriers, but querier-1 forget delay hasn't passed yet.
	uq.forgetDisconnectedQueriers(now.Add(90 * time.Second))

	assert.Contains(t, uq.tenantQuerierAssignments.queriersByID, QuerierID("querier-1"))
	assert.NoError(t, isConsistent(uq))

	for _, userID := range querier1Users {
		assert.Contains(t, getTenantsByQuerier(uq, "querier-1"), userID)
		assert.NotContains(t, getTenantsByQuerier(uq, "querier-2"), userID)
		assert.NotContains(t, getTenantsByQuerier(uq, "querier-3"), userID)
	}

	// Try to forget disconnected queriers. This time querier-1 forget delay has passed.
	uq.forgetDisconnectedQueriers(now.Add(105 * time.Second))

	assert.NotContains(t, uq.tenantQuerierAssignments.queriersByID, QuerierID("querier-1"))
	assert.NoError(t, isConsistent(uq))

	// We expect querier-1 users have been shuffled to other queriers.
	for _, userID := range querier1Users {
		assert.Contains(t, append(getTenantsByQuerier(uq, "querier-2"), getTenantsByQuerier(uq, "querier-3")...), userID)
	}
}

func TestQueues_ForgetDelay_ShouldCorrectlyHandleQuerierReconnectingBeforeForgetDelayIsPassed(t *testing.T) {
	const (
		forgetDelay        = time.Minute
		maxQueriersPerUser = 1
		numUsers           = 100
	)

	now := time.Now()
	uq := newQueueBroker(0, forgetDelay)
	assert.NotNil(t, uq)
	assert.NoError(t, isConsistent(uq))

	// 3 queriers open 2 connections each.
	for i := 1; i <= 3; i++ {
		uq.addQuerierConnection(QuerierID(fmt.Sprintf("querier-%d", i)))
		uq.addQuerierConnection(QuerierID(fmt.Sprintf("querier-%d", i)))
	}

	// Add user queues.
	for i := 0; i < numUsers; i++ {
		tenantID := TenantID(fmt.Sprintf("user-%d", i))
		getOrAdd(t, uq, tenantID, maxQueriersPerUser)
	}

	// We expect querier-1 to have some users.
	querier1Users := getTenantsByQuerier(uq, "querier-1")
	require.NotEmpty(t, querier1Users)

	// Querier-1 abruptly terminates (no shutdown notification received).
	uq.removeQuerierConnection("querier-1", now.Add(40*time.Second))
	uq.removeQuerierConnection("querier-1", now.Add(41*time.Second))

	// We expect querier-1 has NOT been removed.
	assert.Contains(t, uq.tenantQuerierAssignments.queriersByID, QuerierID("querier-1"))
	assert.NoError(t, isConsistent(uq))

	// We expect the querier-1 users have not been shuffled to other queriers.
	for _, userID := range querier1Users {
		assert.Contains(t, getTenantsByQuerier(uq, "querier-1"), userID)
		assert.NotContains(t, getTenantsByQuerier(uq, "querier-2"), userID)
		assert.NotContains(t, getTenantsByQuerier(uq, "querier-3"), userID)
	}

	// Try to forget disconnected queriers, but querier-1 forget delay hasn't passed yet.
	uq.forgetDisconnectedQueriers(now.Add(90 * time.Second))

	// Querier-1 reconnects.
	uq.addQuerierConnection("querier-1")
	uq.addQuerierConnection("querier-1")

	assert.Contains(t, uq.tenantQuerierAssignments.queriersByID, QuerierID("querier-1"))
	assert.NoError(t, isConsistent(uq))

	// We expect the querier-1 users have not been shuffled to other queriers.
	for _, userID := range querier1Users {
		assert.Contains(t, getTenantsByQuerier(uq, "querier-1"), userID)
		assert.NotContains(t, getTenantsByQuerier(uq, "querier-2"), userID)
		assert.NotContains(t, getTenantsByQuerier(uq, "querier-3"), userID)
	}

	// Try to forget disconnected queriers far in the future, but there's no disconnected querier.
	uq.forgetDisconnectedQueriers(now.Add(200 * time.Second))

	assert.Contains(t, uq.tenantQuerierAssignments.queriersByID, QuerierID("querier-1"))
	assert.NoError(t, isConsistent(uq))

	for _, userID := range querier1Users {
		assert.Contains(t, getTenantsByQuerier(uq, "querier-1"), userID)
		assert.NotContains(t, getTenantsByQuerier(uq, "querier-2"), userID)
		assert.NotContains(t, getTenantsByQuerier(uq, "querier-3"), userID)
	}
}

func generateTenant(r *rand.Rand) TenantID {
	return TenantID(fmt.Sprint("tenant-", r.Int()%5))
}

func generateQuerier(r *rand.Rand) QuerierID {
	return QuerierID(fmt.Sprint("querier-", r.Int()%5))
}

func getOrAdd(t *testing.T, qb *queueBroker, tenantID TenantID, maxQueriers int) *list.List {
	q := qb.getOrAddTenantQueue(tenantID, maxQueriers)
	assert.NotNil(t, q)
	assert.NoError(t, isConsistent(qb))
	assert.Equal(t, q, qb.getOrAddTenantQueue(tenantID, maxQueriers))
	return q
}

func confirmOrderForQuerier(t *testing.T, qb *queueBroker, querier QuerierID, lastUserIndex int, qs ...*list.List) int {
	var n *list.List
	for _, q := range qs {
		var err error
		n, _, lastUserIndex, err = qb.getNextQueueForQuerier(lastUserIndex, querier)
		assert.Equal(t, q, n)
		assert.NoError(t, isConsistent(qb))
		assert.NoError(t, err)
	}
	return lastUserIndex
}

func isConsistent(qb *queueBroker) error {
	if len(qb.tenantQuerierAssignments.querierIDsSorted) != len(qb.tenantQuerierAssignments.queriersByID) {
		return fmt.Errorf("inconsistent number of sorted queriers and querier connections")
	}

	userCount := 0
	for ix, userID := range qb.tenantQuerierAssignments.tenantIDOrder {
		uq := qb.tenantQueues[userID]
		if userID != "" && uq == nil {
			return fmt.Errorf("user %s doesn't have queue", userID)
		}
		if userID == "" && uq != nil {
			return fmt.Errorf("user %s shouldn't have queue", userID)
		}
		if userID == "" {
			continue
		}
		userCount++

		user := qb.tenantQuerierAssignments.tenantsByID[userID]
		querierSet := qb.tenantQuerierAssignments.tenantQuerierIDs[userID]

		if user.orderIndex != ix {
			return fmt.Errorf("invalid user's index, expected=%d, got=%d", ix, user.orderIndex)
		}

		if user.maxQueriers == 0 && querierSet != nil {
			return fmt.Errorf("user %s has queriers, but maxQueriers=0", userID)
		}

		if user.maxQueriers > 0 && len(qb.tenantQuerierAssignments.querierIDsSorted) <= user.maxQueriers && querierSet != nil {
			return fmt.Errorf("user %s has queriers set despite not enough queriers available", userID)
		}

		if user.maxQueriers > 0 && len(qb.tenantQuerierAssignments.querierIDsSorted) > user.maxQueriers && len(querierSet) != user.maxQueriers {
			return fmt.Errorf("user %s has incorrect number of queriers, expected=%d, got=%d", userID, len(querierSet), user.maxQueriers)
		}
	}

	if userCount != len(qb.tenantQueues) {
		return fmt.Errorf("inconsistent number of users list and user queues")
	}

	return nil
}

// getTenantsByQuerier returns the list of users handled by the provided QuerierID.
func getTenantsByQuerier(broker *queueBroker, querierID QuerierID) []TenantID {
	var tenantIDs []TenantID
	for userID := range broker.tenantQueues {
		querierSet := broker.tenantQuerierAssignments.tenantQuerierIDs[userID]
		if querierSet == nil {
			// If it's nil then all queriers can handle this user.
			tenantIDs = append(tenantIDs, userID)
			continue
		}
		if _, ok := querierSet[querierID]; ok {
			tenantIDs = append(tenantIDs, userID)
		}
	}
	return tenantIDs
}

func TestShuffleQueriers(t *testing.T) {
	allQueriers := querierIDSlice{"a", "b", "c", "d", "e"}
	tqs := tenantQuerierAssignments{
		querierIDsSorted: allQueriers,
		tenantsByID: map[TenantID]*queueTenant{
			"team-a": {
				shuffleShardSeed: 12345,
			},
		},
		tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{},
	}

	// maxQueriers is 0, so sharding is off
	tqs.shuffleTenantQueriers("team-a", nil)
	require.Nil(t, tqs.tenantQuerierIDs["team-a"])

	// maxQueriers is equal to the total queriers, so the sharding calculation is unnecessary
	tqs.tenantsByID["team-a"].maxQueriers = len(allQueriers)
	tqs.shuffleTenantQueriers("team-a", nil)
	require.Nil(t, tqs.tenantQuerierIDs["team-a"])

	// maxQueriers is greater than the total queriers, so the sharding calculation is unnecessary
	tqs.tenantsByID["team-a"].maxQueriers = len(allQueriers) + 1
	tqs.shuffleTenantQueriers("team-a", nil)
	require.Nil(t, tqs.tenantQuerierIDs["team-a"])

	// now maxQueriers is nonzero and less than the total queriers, so we shuffle shard and assign
	tqs.tenantsByID["team-a"].maxQueriers = 3
	tqs.shuffleTenantQueriers("team-a", nil)
	r1 := tqs.tenantQuerierIDs["team-a"]
	require.Equal(t, 3, len(r1))

	// Same input produces same output.
	tqs.shuffleTenantQueriers("team-a", nil)
	r2 := tqs.tenantQuerierIDs["team-a"]

	require.Equal(t, 3, len(r2))
	require.Equal(t, r1, r2)
}

func TestShuffleQueriersCorrectness(t *testing.T) {
	const queriersCount = 100

	var allSortedQueriers querierIDSlice
	for i := 0; i < queriersCount; i++ {
		allSortedQueriers = append(allSortedQueriers, QuerierID(fmt.Sprintf("%d", i)))
	}
	slices.Sort(allSortedQueriers)

	tqs := tenantQuerierAssignments{
		querierIDsSorted: allSortedQueriers,
		tenantsByID: map[TenantID]*queueTenant{
			"team-a": {
				shuffleShardSeed: 12345,
			},
		},
		tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{},
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	const tests = 1000
	for i := 0; i < tests; i++ {
		toSelect := r.Intn(queriersCount)
		if toSelect == 0 {
			toSelect = 3
		}

		tqs.tenantsByID["team-a"].maxQueriers = toSelect
		tqs.tenantsByID["team-a"].shuffleShardSeed = r.Int63()

		tqs.shuffleTenantQueriers("team-a", nil)
		selectedQueriers := tqs.tenantQuerierIDs["team-a"]
		require.Equal(t, toSelect, len(selectedQueriers))

		slices.Sort(allSortedQueriers)
		prevQuerier := QuerierID("")
		for _, q := range allSortedQueriers {
			require.True(t, prevQuerier < q, "non-unique querier")
			prevQuerier = q

			ix := allSortedQueriers.Search(q)
			require.True(t, ix < len(allSortedQueriers) && allSortedQueriers[ix] == q, "selected querier is not between all queriers")
		}
	}
}
