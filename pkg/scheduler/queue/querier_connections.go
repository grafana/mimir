package queue

import (
	"context"
	"time"
)

const unregisteredWorkerID = -1

type querierConnections struct {
	queriersByID map[QuerierID]*querierState

	// How long to wait before removing a querier which has got disconnected
	// but hasn't notified about a graceful shutdown.
	querierForgetDelay time.Duration
}

func newQuerierConnections(forgetDelay time.Duration) *querierConnections {
	return &querierConnections{
		queriersByID:       map[QuerierID]*querierState{},
		querierForgetDelay: forgetDelay,
	}
}

// querierIsAvailable returns true if the querier is registered to the querierConnections and is not shutting down.
func (qcm *querierConnections) querierIsAvailable(querierID QuerierID) bool {
	q := qcm.queriersByID[querierID]
	return q != nil && !q.shuttingDown
}

// does not manage updating querierIDsSorted; that should be managed by the caller/whatever cares about it getting updated.
func (qcm *querierConnections) addQuerierWorkerConn(conn *QuerierWorkerConn) (addQuerier bool) {
	if conn.IsRegistered() {
		panic("received request to register a querier-worker which was already registered")
	}

	querier := qcm.queriersByID[conn.QuerierID]
	if querier != nil {
		querier.AddWorkerConn(conn)

		// Reset in case the querier re-connected while it was in the forget waiting period.
		querier.shuttingDown = false
		querier.disconnectedAt = time.Time{}

		return false
	}

	// First connection from this querier.
	newQuerierConns := &querierState{}
	newQuerierConns.AddWorkerConn(conn)
	qcm.queriersByID[conn.QuerierID] = newQuerierConns

	return true
}

func (qcm *querierConnections) removeQuerierWorkerConn(conn *QuerierWorkerConn, now time.Time) (removedQuerier bool) {
	querier := qcm.queriersByID[conn.QuerierID]
	if querier == nil || !querier.IsActive() {
		panic("unexpected number of connections for querier")
	}

	if !conn.IsRegistered() {
		panic("received request to deregister a querier-worker which was not already registered")
	}
	querier.RemoveWorkerConn(conn)
	if querier.IsActive() {
		// Querier still has active connections; it will not be removed, so no reshard occurs.
		return false
	}

	// No more active connections. We can remove the querier only if
	// the querier has sent a shutdown signal or if no forget delay is enabled.
	if querier.shuttingDown || qcm.querierForgetDelay == 0 {
		delete(qcm.queriersByID, conn.QuerierID)
		return true
	}

	// No graceful shutdown has been notified yet, so we should track the current time
	// so that we'll remove the querier as soon as we receive the graceful shutdown
	// notification (if any) or once the threshold expires.
	querier.disconnectedAt = now
	return false
}

// shutdownQuerier handles a graceful shutdown notification from a querier. Updates the querier state to shuttingDown if
// applicable, and returns true if the querier is inactive; the querier can be removed from queriersByID in this case.
func (qcm *querierConnections) shutdownQuerier(querierID QuerierID) (canRemoveQuerier bool) {
	querier := qcm.queriersByID[querierID]
	if querier == nil {
		// The querier may have already been removed, so we just ignore it.
		return false
	}

	// We don't check the delay on shutdown notifications, so it's safe to remove the querier as long as
	// there are no more connections.
	if !querier.IsActive() {
		delete(qcm.queriersByID, querierID)
		return true
	}

	// place in graceful shutdown state; any queued requests to dispatch queries
	// to this querier will receive error responses until all querier workers disconnect
	querier.shuttingDown = true
	return false
}

// forgettableQueriers returns a slice of all queriers which have had zero connections for longer than the forget delay.
func (qcm *querierConnections) forgettableQueriers(now time.Time) []QuerierID {
	// if forget delay is disabled, removal is done immediately on querier disconnect or shutdown; do nothing
	if qcm.querierForgetDelay == 0 {
		return nil
	}

	removableQueriers := make([]QuerierID, 0)
	// Remove all queriers with no connections that have gone since at least the forget delay.
	threshold := now.Add(-qcm.querierForgetDelay)
	for querierID, querier := range qcm.queriersByID {
		if querier.activeWorkerConns == 0 && querier.disconnectedAt.Before(threshold) {
			removableQueriers = append(removableQueriers, querierID)
			delete(qcm.queriersByID, querierID)
		}
	}

	return removableQueriers
}

// QuerierWorkerConn is a connection from the querier-worker to the request queue.
//
// WorkerID is unique only per querier; querier-1 and querier-2 will both have a WorkerID=0.
// WorkerID is derived internally in order to distribute worker connections across queue dimensions.
// Unregistered querier-worker connections are assigned a sentinel unregisteredWorkerID.
//
// QuerierWorkerConn is also used when passing querierWorkerOperation messages to update querier connection statuses.
// The querierWorkerOperations can be specific to a querier, but not a particular worker connection (notifyShutdown),
// or may apply to all queriers instead of any particular querier (forgetDisconnected).
// In these cases the relevant ID fields are ignored and should be left as their unregistered or zero values.
type QuerierWorkerConn struct {
	ctx       context.Context
	QuerierID QuerierID
	WorkerID  int
}

func NewUnregisteredQuerierWorkerConn(ctx context.Context, querierID QuerierID) *QuerierWorkerConn {
	return &QuerierWorkerConn{
		ctx:       ctx,
		QuerierID: querierID,
		WorkerID:  unregisteredWorkerID,
	}
}

func (qwc *QuerierWorkerConn) IsRegistered() bool {
	return qwc.WorkerID != unregisteredWorkerID
}

// querierState contains information about a querier and its worker connections which is relevant to the request queue.
type querierState struct {
	// active worker connections from this querier
	workerConns       []*QuerierWorkerConn
	activeWorkerConns int

	// True if the querier notified it's gracefully shutting down.
	shuttingDown bool

	// When the last connection has been unregistered.
	disconnectedAt time.Time
}

func (qc *querierState) IsActive() bool {
	return qc.activeWorkerConns > 0
}

func (qc *querierState) AddWorkerConn(conn *QuerierWorkerConn) {
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

func (qc *querierState) RemoveWorkerConn(conn *QuerierWorkerConn) {
	// Remove the worker ID from the querier's list of worker connections
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
