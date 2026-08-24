// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"context"
	"time"
)

const unregisteredWorkerID = -1

// consumerConnections manages information about consumers connected to the request queue. The queueBroker
// receives information about consumer connections via Queue's consumerWorkerOperations channel.
type consumerConnections struct {
	consumersByID map[string]*consumerState

	// How long to wait before removing a consumer which has got disconnected
	// but hasn't notified about a graceful shutdown.
	consumerForgetDelay time.Duration
}

func newConsumerConnections(forgetDelay time.Duration) *consumerConnections {
	return &consumerConnections{
		consumersByID:       map[string]*consumerState{},
		consumerForgetDelay: forgetDelay,
	}
}

// consumerIsAvailable returns true if the consumer is registered to the consumerConnections and is not shutting down.
func (qc *consumerConnections) consumerIsAvailable(consumerID string) bool {
	q := qc.consumersByID[consumerID]
	return q != nil && !q.shuttingDown
}

// addConsumerWorkerConn is called when the queueBroker processes a consumerWorkerOperation; it adds the consumer-worker
// connection, creating a new consumer connection if we've never seen this consumer before.
func (qc *consumerConnections) addConsumerWorkerConn(conn *ConsumerWorkerConn) (addConsumer bool) {
	if conn.IsRegistered() {
		panic("received request to register a consumer-worker which was already registered")
	}

	consumer := qc.consumersByID[conn.ConsumerID]
	if consumer != nil {
		consumer.AddWorkerConn(conn)

		// Reset in case the consumer re-connected while it was in the forget waiting period.
		consumer.shuttingDown = false
		consumer.disconnectedAt = time.Time{}

		return false
	}

	// First connection from this consumer.
	newConsumerConns := &consumerState{}
	newConsumerConns.AddWorkerConn(conn)
	qc.consumersByID[conn.ConsumerID] = newConsumerConns

	return true
}

// removeConsumerWorkerConn removes a registered ConsumerWorkerConn from an active consumer. If the removed consumer-worker
// connection is the last active worker connection for the consumer, it also deletes the consumer connection, or records
// the disconnection time so the consumer can be forgotten if it does not establish any new consumer-worker connections
// before consumerForgetDelay time has passed.
func (qc *consumerConnections) removeConsumerWorkerConn(conn *ConsumerWorkerConn, now time.Time) (removedConsumer bool) {
	consumer := qc.consumersByID[conn.ConsumerID]
	if consumer == nil || !consumer.IsActive() {
		panic("unexpected number of connections for consumer")
	}

	if !conn.IsRegistered() {
		panic("received request to deregister a consumer-worker which was not already registered")
	}
	consumer.RemoveWorkerConn(conn)
	if consumer.IsActive() {
		// Consumer still has active connections; it will not be removed, so no reshard occurs.
		return false
	}

	// No more active connections. We can remove the consumer only if
	// the consumer has sent a shutdown signal or if no forget delay is enabled.
	if consumer.shuttingDown || qc.consumerForgetDelay == 0 {
		delete(qc.consumersByID, conn.ConsumerID)
		return true
	}

	// No graceful shutdown has been notified yet, so we should track the current time
	// so that we'll remove the consumer as soon as we receive the graceful shutdown
	// notification (if any) or once the threshold expires.
	consumer.disconnectedAt = now
	return false
}

// shutdownConsumer handles a graceful shutdown notification from a consumer. It updates the consumer state to shuttingDown
// if applicable, and returns true if the consumer is inactive; the consumer can be removed from consumersByID in this case.
func (qc *consumerConnections) shutdownConsumer(consumerID string) (canRemoveConsumer bool) {
	consumer := qc.consumersByID[consumerID]
	if consumer == nil {
		// The consumer may have already been removed, so we just ignore it.
		return false
	}

	// We don't check the delay on shutdown notifications, so it's safe to remove the consumer as long as
	// there are no more connections.
	if !consumer.IsActive() {
		delete(qc.consumersByID, consumerID)
		return true
	}

	// place in graceful shutdown state; any queued requests to dispatch queries
	// to this consumer will receive error responses until all consumer workers disconnect
	consumer.shuttingDown = true
	return false
}

// removeForgettableConsumers removes all consumer connections which no longer have any consumer-worker connections and for whom
// consumerForgetDelay time has passed since the consumer disconnected. It returns a slice of all consumer IDs which were
// removed.
func (qc *consumerConnections) removeForgettableConsumers(now time.Time) []string {
	// if forget delay is disabled, removal is done immediately on consumer disconnect or shutdown; do nothing
	if qc.consumerForgetDelay == 0 {
		return nil
	}

	removableConsumers := make([]string, 0)
	// Remove all consumers with no connections that have gone since at least the forget delay.
	threshold := now.Add(-qc.consumerForgetDelay)
	for consumerID, consumer := range qc.consumersByID {
		if consumer.activeWorkerConns == 0 && consumer.disconnectedAt.Before(threshold) {
			removableConsumers = append(removableConsumers, consumerID)
			delete(qc.consumersByID, consumerID)
		}
	}

	return removableConsumers
}

// ConsumerWorkerConn is a connection from the consumer-worker to the request queue.
//
// WorkerID is unique only per consumer; consumer-1 and consumer-2 will both have a WorkerID=0.
// WorkerID is derived internally in order to distribute worker connections across queue dimensions.
// Unregistered consumer-worker connections are assigned a sentinel unregisteredWorkerID.
//
// ConsumerWorkerConn is also used when passing consumerWorkerOperation messages to update consumer connection statuses.
// The consumerWorkerOperations can be specific to a consumer, but not a particular worker connection (notifyShutdown),
// or may apply to all consumers instead of any particular consumer (forgetDisconnected).
// In these cases the relevant ID fields are ignored and should be left as their unregistered or zero values.
type ConsumerWorkerConn struct {
	ctx        context.Context
	ConsumerID string
	WorkerID   int
}

func NewUnregisteredConsumerWorkerConn(ctx context.Context, consumerID string) *ConsumerWorkerConn {
	return &ConsumerWorkerConn{
		ctx:        ctx,
		ConsumerID: consumerID,
		WorkerID:   unregisteredWorkerID,
	}
}

func (qwc *ConsumerWorkerConn) IsRegistered() bool {
	return qwc.WorkerID != unregisteredWorkerID
}

// consumerState contains information which is relevant to the request queue about a consumer and its worker connections.
type consumerState struct {
	// active worker connections from this consumer
	workerConns       []*ConsumerWorkerConn
	activeWorkerConns int

	// True if the consumer notified it's gracefully shutting down.
	shuttingDown bool

	// When the last connection has been unregistered.
	disconnectedAt time.Time
}

func (qc *consumerState) IsActive() bool {
	return qc.activeWorkerConns > 0
}

func (qc *consumerState) AddWorkerConn(conn *ConsumerWorkerConn) {
	// first look for a previously de-registered connection placeholder in the list
	for i, workerConn := range qc.workerConns {
		if workerConn == nil {
			// take the place and ID of the previously de-registered worker
			conn.WorkerID = i
			qc.workerConns[i] = conn
			qc.activeWorkerConns++
			return
		}
	}
	// no de-registered placeholders to replace; we append the new worker ID
	nextWorkerID := len(qc.workerConns)
	conn.WorkerID = nextWorkerID
	qc.workerConns = append(qc.workerConns, conn)
	qc.activeWorkerConns++
}

func (qc *consumerState) RemoveWorkerConn(conn *ConsumerWorkerConn) {
	// Remove the worker ID from the consumer's list of worker connections
	for i, workerConn := range qc.workerConns {
		if workerConn != nil && workerConn.WorkerID == conn.WorkerID {
			if i == len(qc.workerConns)-1 {
				// shrink list only if at end
				qc.workerConns = qc.workerConns[:i]
			} else {
				// otherwise insert placeholder to avoid too many list append operations
				qc.workerConns[i] = nil
			}
			conn.WorkerID = unregisteredWorkerID
			qc.activeWorkerConns--
			break
		}
	}
}
