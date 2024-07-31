// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/user_queues_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queue

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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

func TestQueues_QuerierWorkerIDAssignment(t *testing.T) {
	treeTypes := buildTreeTestsStruct()
	for _, tt := range treeTypes {
		t.Run(tt.name, func(t *testing.T) {
			tqas := newTenantQuerierAssignments(0)

			// 2 queriers open 3 connections each.
			// Each querier's worker IDs should be assigned in order,
			// assuming no disconnects between registering connections
			querier1Conn1 := NewUnregisteredQuerierWorkerConn("querier-1")
			tqas.addQuerierWorkerConn(querier1Conn1)
			require.Equal(t, 0, querier1Conn1.WorkerID)
			querier1Conn2 := NewUnregisteredQuerierWorkerConn("querier-1")
			tqas.addQuerierWorkerConn(querier1Conn2)
			require.Equal(t, 1, querier1Conn2.WorkerID)
			querier1Conn3 := NewUnregisteredQuerierWorkerConn("querier-1")
			tqas.addQuerierWorkerConn(querier1Conn3)
			require.Equal(t, 2, querier1Conn3.WorkerID)

			// The second querier's worker IDs again start at 0 and are independent of any other queriers
			querier2Conn1 := NewUnregisteredQuerierWorkerConn("querier-2")
			tqas.addQuerierWorkerConn(querier2Conn1)
			require.Equal(t, 0, querier2Conn1.WorkerID)
			querier2Conn2 := NewUnregisteredQuerierWorkerConn("querier-2")
			tqas.addQuerierWorkerConn(querier2Conn2)
			require.Equal(t, 1, querier2Conn2.WorkerID)
			querier2Conn3 := NewUnregisteredQuerierWorkerConn("querier-2")
			tqas.addQuerierWorkerConn(querier2Conn3)
			require.Equal(t, 2, querier2Conn3.WorkerID)

			// Unregister two of querier-1's connection
			tqas.removeQuerierWorkerConn(querier1Conn2, time.Now())
			require.False(t, querier1Conn2.IsRegistered())
			tqas.removeQuerierWorkerConn(querier1Conn1, time.Now())
			require.False(t, querier1Conn1.IsRegistered())
			// Remaining connections should keep their worker IDs
			require.Equal(t, 2, querier1Conn3.WorkerID)

			// The next connection should get the next available worker ID;
			// since worker ID 1 was deregistered, the next connection will get worker ID 1.
			querier1Conn4 := NewUnregisteredQuerierWorkerConn("querier-1")
			tqas.addQuerierWorkerConn(querier1Conn4)
			require.Equal(t, 0, querier1Conn4.WorkerID)

			// A previous connection can re-register and get a different worker ID
			// This does not happen in practice, as the de-registration only happens
			// when a QuerierLoop exits, after which the querierWorkerConn falls out of scope.
			// Regardless, once the connection is deregistered it acts as a new unregistered connection.
			tqas.addQuerierWorkerConn(querier1Conn1)
			require.Equal(t, 1, querier1Conn1.WorkerID)
		})
	}
}
