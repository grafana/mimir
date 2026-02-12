// SPDX-License-Identifier: AGPL-3.0-only

package writetee

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConnectionTTLMiddleware_Validation(t *testing.T) {
	tests := []struct {
		name        string
		minTTL      time.Duration
		maxTTL      time.Duration
		idleFreq    time.Duration
		expectError error
	}{
		{
			name:        "minTTL greater than maxTTL",
			minTTL:      90 * time.Second,
			maxTTL:      30 * time.Second,
			idleFreq:    time.Minute,
			expectError: errMinLessOrEqualThanMax,
		},
		{
			name:        "idle frequency must be positive when maxTTL > 0",
			minTTL:      30 * time.Second,
			maxTTL:      90 * time.Second,
			idleFreq:    0,
			expectError: errIdleConnectionCheckFrequencyMustBePositive,
		},
		{
			name:        "disabled when maxTTL <= 0",
			minTTL:      0,
			maxTTL:      0,
			idleFreq:    0, // doesn't matter when disabled
			expectError: nil,
		},
		{
			name:        "valid configuration",
			minTTL:      30 * time.Second,
			maxTTL:      90 * time.Second,
			idleFreq:    time.Minute,
			expectError: nil,
		},
		{
			name:        "negative minTTL treated as 0",
			minTTL:      -1 * time.Second,
			maxTTL:      90 * time.Second,
			idleFreq:    time.Minute,
			expectError: nil,
		},
		{
			name:        "equal minTTL and maxTTL",
			minTTL:      60 * time.Second,
			maxTTL:      60 * time.Second,
			idleFreq:    time.Minute,
			expectError: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			m, err := newConnectionTTLMiddleware(tc.minTTL, tc.maxTTL, tc.idleFreq, reg)
			if tc.expectError != nil {
				require.ErrorIs(t, err, tc.expectError)
				require.Nil(t, m)
			} else {
				require.NoError(t, err)
				require.NotNil(t, m)
			}
		})
	}
}

func TestConnectionTTLMiddleware_CalculateTTL(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := newConnectionTTLMiddleware(30*time.Second, 90*time.Second, time.Minute, reg)
	require.NoError(t, err)

	middleware := m.(*connectionTTLMiddleware)

	// Test that TTL is within range
	for i := 0; i < 100; i++ {
		conn := fmt.Sprintf("192.168.1.%d:%d", i/10, 10000+i)
		ttl := middleware.calculateTTL(conn)
		assert.GreaterOrEqual(t, ttl, 30*time.Second, "TTL should be >= minTTL for conn %s", conn)
		assert.LessOrEqual(t, ttl, 90*time.Second, "TTL should be <= maxTTL for conn %s", conn)
	}

	// Test that TTL is deterministic for the same connection
	conn := "192.168.1.100:54321"
	ttl1 := middleware.calculateTTL(conn)
	ttl2 := middleware.calculateTTL(conn)
	assert.Equal(t, ttl1, ttl2, "TTL should be deterministic for the same connection")

	// Test that different connections get different TTLs (probabilistically)
	differentTTLs := make(map[time.Duration]bool)
	for i := 0; i < 100; i++ {
		conn := fmt.Sprintf("192.168.%d.%d:%d", i/100, i%100, 10000+i)
		ttl := middleware.calculateTTL(conn)
		differentTTLs[ttl] = true
	}
	// With 100 connections and a 60 second range (60001 possible values in ms),
	// we should see more than just 1 unique TTL value.
	assert.Greater(t, len(differentTTLs), 1, "Different connections should have different TTLs")
}

func TestConnectionTTLMiddleware_ConnectionCloseHeader(t *testing.T) {
	reg := prometheus.NewRegistry()
	// Use equal minTTL and maxTTL for deterministic behavior in tests
	m, err := newConnectionTTLMiddleware(50*time.Millisecond, 50*time.Millisecond, time.Minute, reg)
	require.NoError(t, err)

	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := m.Wrap(nextHandler)

	// First request - connection should be tracked, no close header
	req1 := httptest.NewRequest("POST", "/api/v1/push", nil)
	req1.RemoteAddr = "192.168.1.1:12345"
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	assert.Empty(t, rec1.Header().Get(connectionHeaderKey), "First request should not have Connection: close")

	// Wait for TTL to expire
	time.Sleep(60 * time.Millisecond)

	// Second request from same connection after TTL expired - should get close header
	req2 := httptest.NewRequest("POST", "/api/v1/push", nil)
	req2.RemoteAddr = "192.168.1.1:12345"
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	assert.Equal(t, connectionHeaderCloseValue, rec2.Header().Get(connectionHeaderKey), "Request after TTL expired should have Connection: close")

	// Request from a new connection (immediately after, before any TTL could expire) - should not have close header
	// We need a new registry to ensure a fresh middleware
	reg2 := prometheus.NewRegistry()
	m2, err := newConnectionTTLMiddleware(50*time.Millisecond, 50*time.Millisecond, time.Minute, reg2)
	require.NoError(t, err)
	handler2 := m2.Wrap(nextHandler)

	req3 := httptest.NewRequest("POST", "/api/v1/push", nil)
	req3.RemoteAddr = "192.168.1.1:54321"
	rec3 := httptest.NewRecorder()
	handler2.ServeHTTP(rec3, req3)
	assert.Empty(t, rec3.Header().Get(connectionHeaderKey), "New connection should not have Connection: close")
}

func TestConnectionTTLMiddleware_Disabled(t *testing.T) {
	reg := prometheus.NewRegistry()
	// maxTTL <= 0 means disabled
	m, err := newConnectionTTLMiddleware(0, 0, 0, reg)
	require.NoError(t, err)

	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := m.Wrap(nextHandler)

	// Request should pass through without any connection tracking
	req := httptest.NewRequest("POST", "/api/v1/push", nil)
	req.RemoteAddr = "192.168.1.1:12345"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Empty(t, rec.Header().Get(connectionHeaderKey), "Disabled middleware should not set Connection header")
}

func TestConnectionTTLMiddleware_IdleCleanup(t *testing.T) {
	reg := prometheus.NewRegistry()
	// Use a very short idle check frequency for testing
	m, err := newConnectionTTLMiddleware(100*time.Millisecond, 100*time.Millisecond, 50*time.Millisecond, reg)
	require.NoError(t, err)

	middleware := m.(*connectionTTLMiddleware)

	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := m.Wrap(nextHandler)

	// Make a request to track a connection
	req := httptest.NewRequest("POST", "/api/v1/push", nil)
	req.RemoteAddr = "192.168.1.1:12345"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// Verify connection is tracked
	middleware.connectionsMu.Lock()
	_, exists := middleware.connections["192.168.1.1:12345"]
	middleware.connectionsMu.Unlock()
	assert.True(t, exists, "Connection should be tracked")

	// Wait for idle cleanup to run (maxTTL is used as idle timeout)
	time.Sleep(200 * time.Millisecond)

	// Verify connection was cleaned up
	middleware.connectionsMu.Lock()
	_, exists = middleware.connections["192.168.1.1:12345"]
	middleware.connectionsMu.Unlock()
	assert.False(t, exists, "Idle connection should be cleaned up")
}
