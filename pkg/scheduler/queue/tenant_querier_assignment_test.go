// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/user_queues_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queue

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestShuffleQueriers(t *testing.T) {
	allQueriers := querierIDSlice{"a", "b", "c", "d", "e"}
	tqs := tenantQuerierAssignments[any]{
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

	tqs := tenantQuerierAssignments[any]{
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

func TestQueues_QuerierWorkerIDAssignment(t *testing.T) {
	tqa := newTenantQuerierAssignments[any](0)

	// 2 queriers open 3 connections each.
	// Each querier's worker IDs should be assigned in order,
	// assuming no disconnects between registering connections
	querier1Conn1 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-1")
	tqa.addQuerierWorkerConn(querier1Conn1)
	require.Equal(t, 0, querier1Conn1.WorkerID)
	querier1Conn2 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-1")
	tqa.addQuerierWorkerConn(querier1Conn2)
	require.Equal(t, 1, querier1Conn2.WorkerID)
	querier1Conn3 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-1")
	tqa.addQuerierWorkerConn(querier1Conn3)
	require.Equal(t, 2, querier1Conn3.WorkerID)

	// The second querier's worker IDs again start at 0 and are independent of any other queriers
	querier2Conn1 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-2")
	tqa.addQuerierWorkerConn(querier2Conn1)
	require.Equal(t, 0, querier2Conn1.WorkerID)
	querier2Conn2 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-2")
	tqa.addQuerierWorkerConn(querier2Conn2)
	require.Equal(t, 1, querier2Conn2.WorkerID)
	querier2Conn3 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-2")
	tqa.addQuerierWorkerConn(querier2Conn3)
	require.Equal(t, 2, querier2Conn3.WorkerID)

	// Unregister two of querier-1's connection
	tqa.removeQuerierWorkerConn(querier1Conn2, time.Now())
	require.False(t, querier1Conn2.IsRegistered())
	tqa.removeQuerierWorkerConn(querier1Conn1, time.Now())
	require.False(t, querier1Conn1.IsRegistered())
	// Remaining connections should keep their worker IDs
	require.Equal(t, 2, querier1Conn3.WorkerID)

	// The next connection should get the next available worker ID;
	// since worker ID 0 was deregistered, the next connection will get worker ID 0.
	querier1Conn4 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-1")
	tqa.addQuerierWorkerConn(querier1Conn4)
	require.Equal(t, 0, querier1Conn4.WorkerID)

	// A previous connection can re-register and get a different worker ID
	// This does not happen in practice, as the de-registration only happens
	// when a QuerierLoop exits, after which the conn falls out of scope.
	// Regardless, once the connection is deregistered it acts as a new unregistered connection.
	tqa.addQuerierWorkerConn(querier1Conn1)
	require.Equal(t, 1, querier1Conn1.WorkerID)
}

func TestQueues_QuerierWorkerIDAssignment_Panics(t *testing.T) {
	// test with forget delay so the querier is not removed when its last connection is deregistered;
	// a registered querier with no registered connections allows the test to reach some panic cases
	tqa := newTenantQuerierAssignments[any](10 * time.Second)

	querierConn := NewUnregisteredQuerierWorkerConn(context.Background(), "")

	// the querier has never had any connections registered, so it is not tracked by TenantQuerierAssignments
	// de-registering any querier connection should panic with a message about the querier
	require.PanicsWithValue(
		t, "unexpected number of connections for querier",
		func() { tqa.removeQuerierWorkerConn(querierConn, time.Now()) },
	)
	// now register the first worker for the querier
	tqa.addQuerierWorkerConn(querierConn)

	// double-registering a querier connection should panic
	require.PanicsWithValue(
		t, "received request to register a querier-worker which was already registered",
		func() { tqa.addQuerierWorkerConn(querierConn) },
	)

	// the querier has active connections, so it is tracked by TenantQuerierAssignments
	// de-registering any unregistered querier connection should panic with a message about the querier-worker
	require.PanicsWithValue(
		t, "received request to deregister a querier-worker which was not already registered",
		func() {
			tqa.removeQuerierWorkerConn(
				NewUnregisteredQuerierWorkerConn(context.Background(), ""), time.Now(),
			)
		},
	)
}
