// SPDX-License-Identifier: AGPL-3.0-only

package writetee

import (
	"fmt"
	"hash/fnv"
	"net/http"
	"sync"
	"time"

	"github.com/grafana/dskit/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	connectionHeaderKey                     = "Connection"
	connectionHeaderCloseValue              = "close"
	totalClosedConnectionsReasonLabel       = "reason"
	totalClosedConnectionsReasonLimit       = "limit"
	totalClosedConnectionsReasonIdleTimeout = "idle timeout"
)

var (
	errIdleConnectionCheckFrequencyMustBePositive = fmt.Errorf("idle connection check frequency must be positive")
	errMinLessOrEqualThanMax                      = fmt.Errorf("minimum TTL of TCP connections must be less or equal to the maximum TTL of TCP connections")
)

type connectionState struct {
	ttl      time.Duration
	created  time.Time
	lastSeen time.Time
}

func (c *connectionState) isExpired() bool {
	return time.Since(c.created) > c.ttl
}

func (c *connectionState) isIdleExpired(timeout time.Duration) bool {
	return time.Since(c.lastSeen) > timeout
}

type connectionTTLMiddleware struct {
	minTTL time.Duration
	maxTTL time.Duration

	connectionsMu sync.Mutex
	connections   map[string]*connectionState

	totalOpenConnections   prometheus.Counter
	totalClosedConnections *prometheus.CounterVec

	// Shutdown mechanism for background goroutine
	done chan struct{}
	wg   sync.WaitGroup
}

// newConnectionTTLMiddleware returns an HTTP middleware that limits the maximum lifetime, TTL,
// of TCP connections. Once the limit is reached, the middleware sends the 'Connection: close'
// response header to the client, as a signal to close the connection.
// For each connection, the TTL is between the given minTTL and maxTTL. If minTTL and maxTTL are <= 0,
// no TTL is assumed.
func newConnectionTTLMiddleware(minTTL, maxTTL, idleConnectionCheckFrequency time.Duration, reg prometheus.Registerer) (middleware.Interface, error) {
	if minTTL < 0 {
		minTTL = 0
	}

	if maxTTL < 0 {
		maxTTL = 0
	}

	if minTTL > maxTTL {
		return nil, errMinLessOrEqualThanMax
	}

	m := &connectionTTLMiddleware{
		minTTL:        minTTL,
		maxTTL:        maxTTL,
		connectionsMu: sync.Mutex{},
		connections:   make(map[string]*connectionState),
		totalOpenConnections: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: writeTeeMetricsNamespace,
			Name:      "open_connections_with_ttl_total",
			Help:      "Number of connections that connection TTL middleware started tracking",
		}),
		totalClosedConnections: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: writeTeeMetricsNamespace,
			Name:      "closed_connections_with_ttl_total",
			Help:      "Number of connections that connection TTL middleware closed or stopped tracking",
		}, []string{totalClosedConnectionsReasonLabel}),
		done: make(chan struct{}),
	}

	if maxTTL > 0 {
		if idleConnectionCheckFrequency <= 0 {
			return nil, errIdleConnectionCheckFrequencyMustBePositive
		}
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			idleConnectionCheckTicker := time.NewTicker(idleConnectionCheckFrequency)
			defer idleConnectionCheckTicker.Stop()
			for {
				select {
				case <-idleConnectionCheckTicker.C:
					m.removeIdleExpiredConnections()
				case <-m.done:
					return
				}
			}
		}()
	}

	return m, nil
}

// Stop signals the middleware to stop the background goroutine.
// Call Await() after Stop() to wait for the goroutine to complete.
func (m *connectionTTLMiddleware) Stop() {
	close(m.done)
}

// Await waits for the background goroutine to complete.
// Call this after Stop() to ensure graceful shutdown.
func (m *connectionTTLMiddleware) Await() {
	m.wg.Wait()
}

// removeIdleExpiredConnections removes all expired idle cached connections, i.e.,
// the ones exceeding their own TTL.
func (m *connectionTTLMiddleware) removeIdleExpiredConnections() {
	count := 0
	m.connectionsMu.Lock()
	for conn, state := range m.connections {
		if state.isIdleExpired(m.maxTTL) {
			delete(m.connections, conn)
			count++
		}
	}
	m.connectionsMu.Unlock()
	m.totalClosedConnections.WithLabelValues(totalClosedConnectionsReasonIdleTimeout).Add(float64(count))
}

// removeExpiredConnection checks if the given connection expired, and in that case removes it from the cache.
// Returns a boolean indicating if the given connection has been removed.
func (m *connectionTTLMiddleware) removeExpiredConnection(conn string) bool {
	state := m.connectionState(conn)
	// It is safe to call state.isExpired() without locking m.connectionsMu,
	// because connectionState.ttl and connectionState.created are never modified.
	if !state.isExpired() {
		return false
	}
	m.connectionsMu.Lock()
	delete(m.connections, conn)
	m.connectionsMu.Unlock()
	m.totalClosedConnections.WithLabelValues(totalClosedConnectionsReasonLimit).Inc()
	return true
}

// calculateTTL calculates the TTL for the given connection. This value is from the range between minTTL and maxTTL,
// and depends on the connection's hash.
func (m *connectionTTLMiddleware) calculateTTL(conn string) time.Duration {
	h := fnv.New64()
	h.Write([]byte(conn))
	hash := h.Sum64()
	ttlRange := uint64(m.maxTTL.Milliseconds() + 1 - m.minTTL.Milliseconds())
	ttlInMs := m.minTTL.Milliseconds() + int64(hash%ttlRange)
	return time.Duration(ttlInMs) * time.Millisecond
}

// connectionState returns the current state of the given connection.
func (m *connectionTTLMiddleware) connectionState(conn string) *connectionState {
	var now = time.Now()
	m.connectionsMu.Lock()
	state, cachedConn := m.connections[conn]
	if cachedConn {
		state.lastSeen = now
	} else {
		ttl := m.calculateTTL(conn)
		state = &connectionState{
			ttl:      ttl,
			created:  now,
			lastSeen: now,
		}
		m.connections[conn] = state
	}
	m.connectionsMu.Unlock()
	if !cachedConn {
		m.totalOpenConnections.Inc()
	}
	return state
}

// Wrap implements middleware.Interface, and returns a http.Handler that first
// calculates the TTL of the connection associated with a given http.Request,
// and then checks connection's current lifetime against the calculated TTL.
// If the former exceeds the latter, the resulting http.Handler marks the
// connection as closed in the response header.
func (m *connectionTTLMiddleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m.maxTTL <= 0 {
			next.ServeHTTP(w, r)
			return
		}

		conn := r.RemoteAddr
		if m.removeExpiredConnection(conn) {
			w.Header().Set(connectionHeaderKey, connectionHeaderCloseValue)
		}
		next.ServeHTTP(w, r)
	})
}
