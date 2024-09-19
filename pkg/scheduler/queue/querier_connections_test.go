// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_QuerierConnections(t *testing.T) {
	querierConns := newQuerierConnections(0)

	// 2 queriers open 3 connections each.
	// Each querier's worker IDs should be assigned in order,
	// assuming no disconnects between registering connections
	querier1Conn1 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-1")
	querierConns.addQuerierWorkerConn(querier1Conn1)
	require.Equal(t, 0, querier1Conn1.WorkerID)
	querier1Conn2 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-1")
	querierConns.addQuerierWorkerConn(querier1Conn2)
	require.Equal(t, 1, querier1Conn2.WorkerID)
	querier1Conn3 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-1")
	querierConns.addQuerierWorkerConn(querier1Conn3)
	require.Equal(t, 2, querier1Conn3.WorkerID)

	// The second querier's worker IDs again start at 0 and are independent of any other queriers
	querier2Conn1 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-2")
	querierConns.addQuerierWorkerConn(querier2Conn1)
	require.Equal(t, 0, querier2Conn1.WorkerID)
	querier2Conn2 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-2")
	querierConns.addQuerierWorkerConn(querier2Conn2)
	require.Equal(t, 1, querier2Conn2.WorkerID)
	querier2Conn3 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-2")
	querierConns.addQuerierWorkerConn(querier2Conn3)
	require.Equal(t, 2, querier2Conn3.WorkerID)

	// Unregister two of querier-1's connection
	querierConns.removeQuerierWorkerConn(querier1Conn2, time.Now())
	require.False(t, querier1Conn2.IsRegistered())
	querierConns.removeQuerierWorkerConn(querier1Conn1, time.Now())
	require.False(t, querier1Conn1.IsRegistered())
	// Remaining connections should keep their worker IDs
	require.Equal(t, 2, querier1Conn3.WorkerID)

	// The next connection should get the next available worker ID;
	// since worker ID 0 was deregistered, the next connection will get worker ID 0.
	querier1Conn4 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-1")
	querierConns.addQuerierWorkerConn(querier1Conn4)
	require.Equal(t, 0, querier1Conn4.WorkerID)

	// A previous connection can re-register and get a different worker ID
	// This does not happen in practice, as the de-registration only happens
	// when a QuerierLoop exits, after which the conn falls out of scope.
	// Regardless, once the connection is deregistered it acts as a new unregistered connection.
	querierConns.addQuerierWorkerConn(querier1Conn1)
	require.Equal(t, 1, querier1Conn1.WorkerID)
}

func Test_QuerierConnectionsPanics(t *testing.T) {
	// test with forget delay so the querier is not removed when its last connection is deregistered;
	// a registered querier with no registered connections allows the test to reach some panic cases
	querierConns := newQuerierConnections(10 * time.Second)

	querierConn := NewUnregisteredQuerierWorkerConn(context.Background(), "")

	// the querier has never had any connections registered, so it is not tracked by TenantQuerierAssignments
	// de-registering any querier connection should panic with a message about the querier
	require.PanicsWithValue(
		t, "unexpected number of connections for querier",
		func() { querierConns.removeQuerierWorkerConn(querierConn, time.Now()) },
	)
	// now register the first worker for the querier
	//tqa.addQuerierWorkerConn(querierConn)
	querierConns.addQuerierWorkerConn(querierConn)

	// double-registering a querier connection should panic
	require.PanicsWithValue(
		t, "received request to register a querier-worker which was already registered",
		func() { querierConns.addQuerierWorkerConn(querierConn) },
	)

	// the querier has active connections, so it is tracked by TenantQuerierAssignments
	// de-registering any unregistered querier connection should panic with a message about the querier-worker
	require.PanicsWithValue(
		t, "received request to deregister a querier-worker which was not already registered",
		func() {
			querierConns.removeQuerierWorkerConn(
				NewUnregisteredQuerierWorkerConn(context.Background(), ""), time.Now(),
			)
		},
	)
}
