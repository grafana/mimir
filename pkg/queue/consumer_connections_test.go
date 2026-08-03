// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_ConsumerConnections(t *testing.T) {
	consumerConns := newConsumerConnections(0)

	// 2 consumers open 3 connections each.
	// Each consumer's worker IDs should be assigned in order,
	// assuming no disconnects between registering connections
	consumer1Conn1 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-1")
	consumerConns.addConsumerWorkerConn(consumer1Conn1)
	require.Equal(t, 0, consumer1Conn1.WorkerID)
	consumer1Conn2 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-1")
	consumerConns.addConsumerWorkerConn(consumer1Conn2)
	require.Equal(t, 1, consumer1Conn2.WorkerID)
	consumer1Conn3 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-1")
	consumerConns.addConsumerWorkerConn(consumer1Conn3)
	require.Equal(t, 2, consumer1Conn3.WorkerID)

	// The second consumer's worker IDs again start at 0 and are independent of any other consumers
	consumer2Conn1 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-2")
	consumerConns.addConsumerWorkerConn(consumer2Conn1)
	require.Equal(t, 0, consumer2Conn1.WorkerID)
	consumer2Conn2 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-2")
	consumerConns.addConsumerWorkerConn(consumer2Conn2)
	require.Equal(t, 1, consumer2Conn2.WorkerID)
	consumer2Conn3 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-2")
	consumerConns.addConsumerWorkerConn(consumer2Conn3)
	require.Equal(t, 2, consumer2Conn3.WorkerID)

	// Unregister two of consumer-1's connection
	consumerConns.removeConsumerWorkerConn(consumer1Conn2, time.Now())
	require.False(t, consumer1Conn2.IsRegistered())
	consumerConns.removeConsumerWorkerConn(consumer1Conn1, time.Now())
	require.False(t, consumer1Conn1.IsRegistered())
	// Remaining connections should keep their worker IDs
	require.Equal(t, 2, consumer1Conn3.WorkerID)

	// The next connection should get the next available worker ID;
	// since worker ID 0 was deregistered, the next connection will get worker ID 0.
	consumer1Conn4 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-1")
	consumerConns.addConsumerWorkerConn(consumer1Conn4)
	require.Equal(t, 0, consumer1Conn4.WorkerID)

	// A previous connection can re-register and get a different worker ID
	// This does not happen in practice, as the de-registration only happens
	// when a ConsumerLoop exits, after which the conn falls out of scope.
	// Regardless, once the connection is deregistered it acts as a new unregistered connection.
	consumerConns.addConsumerWorkerConn(consumer1Conn1)
	require.Equal(t, 1, consumer1Conn1.WorkerID)
}

func Test_ConsumerConnectionsPanics(t *testing.T) {
	// test with forget delay so the consumer is not removed when its last connection is deregistered;
	// a registered consumer with no registered connections allows the test to reach some panic cases
	consumerConns := newConsumerConnections(10 * time.Second)

	consumerConn := NewUnregisteredConsumerWorkerConn(context.Background(), "")

	// the consumer has never had any connections registered, so it is not tracked by TenantConsumerAssignments
	// de-registering any consumer connection should panic with a message about the consumer
	require.PanicsWithValue(
		t, "unexpected number of connections for consumer",
		func() { consumerConns.removeConsumerWorkerConn(consumerConn, time.Now()) },
	)
	// now register the first worker for the consumer
	//tqa.addConsumerWorkerConn(consumerConn)
	consumerConns.addConsumerWorkerConn(consumerConn)

	// double-registering a consumer connection should panic
	require.PanicsWithValue(
		t, "received request to register a consumer-worker which was already registered",
		func() { consumerConns.addConsumerWorkerConn(consumerConn) },
	)

	// the consumer has active connections, so it is tracked by TenantConsumerAssignments
	// de-registering any unregistered consumer connection should panic with a message about the consumer-worker
	require.PanicsWithValue(
		t, "received request to deregister a consumer-worker which was not already registered",
		func() {
			consumerConns.removeConsumerWorkerConn(
				NewUnregisteredConsumerWorkerConn(context.Background(), ""), time.Now(),
			)
		},
	)
}
