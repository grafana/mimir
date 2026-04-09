// SPDX-License-Identifier: AGPL-3.0-only

package remotewrite

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	m := NewManager(t.TempDir(), 5*time.Second, log.NewNopLogger(), prometheus.NewRegistry())
	t.Cleanup(m.Stop)
	return m
}

func testConfig(t *testing.T, name, url string) Config {
	t.Helper()
	cfg := Config{Name: name}
	require.NoError(t, cfg.URL.Set(url))
	return cfg
}

func TestManager_AppendablesForTenant_CreatesStorage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	m := newTestManager(t)
	cfgs := []Config{testConfig(t, "remote1", srv.URL)}

	appendables, err := m.AppendablesForTenant("user1", cfgs)
	require.NoError(t, err)
	assert.Len(t, appendables, 1)
}

func TestManager_AppendablesForTenant_ReusesExistingStorage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	m := newTestManager(t)
	cfg := testConfig(t, "remote1", srv.URL)

	a1, err := m.AppendablesForTenant("user1", []Config{cfg})
	require.NoError(t, err)

	a2, err := m.AppendablesForTenant("user1", []Config{cfg})
	require.NoError(t, err)

	// Same storage instance returned both times.
	assert.Equal(t, a1[0], a2[0])
}

func TestManager_AppendablesForTenant_RemovesDroppedConfig(t *testing.T) {
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv1.Close)
	t.Cleanup(srv2.Close)

	m := newTestManager(t)

	// Start with two configs.
	_, err := m.AppendablesForTenant("user1", []Config{
		testConfig(t, "remote1", srv1.URL),
		testConfig(t, "remote2", srv2.URL),
	})
	require.NoError(t, err)

	// Drop remote2 — only remote1 should remain.
	appendables, err := m.AppendablesForTenant("user1", []Config{
		testConfig(t, "remote1", srv1.URL),
	})
	require.NoError(t, err)
	assert.Len(t, appendables, 1)
}

func TestManager_AppendablesForTenant_EmptyConfigStopsAll(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	m := newTestManager(t)

	_, err := m.AppendablesForTenant("user1", []Config{testConfig(t, "remote1", srv.URL)})
	require.NoError(t, err)

	// Passing nil config list stops all storages for the tenant.
	appendables, err := m.AppendablesForTenant("user1", nil)
	require.NoError(t, err)
	assert.Empty(t, appendables)

	// Tenant should be gone from internal map.
	m.mu.Lock()
	_, tenantExists := m.storages["user1"]
	m.mu.Unlock()
	assert.False(t, tenantExists)
}

func TestManager_TenantIsolation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	m := newTestManager(t)
	cfg := testConfig(t, "remote1", srv.URL)

	a1, err := m.AppendablesForTenant("user1", []Config{cfg})
	require.NoError(t, err)

	a2, err := m.AppendablesForTenant("user2", []Config{cfg})
	require.NoError(t, err)

	// Different tenants must get different Storage instances.
	assert.NotEqual(t, a1[0], a2[0], "different tenants must not share a Storage")
}

func TestManager_InvalidConfigReturnsError(t *testing.T) {
	m := newTestManager(t)

	// Config with no URL should fail validation.
	_, err := m.AppendablesForTenant("user1", []Config{{Name: "bad"}})
	assert.Error(t, err)
}

func TestManager_Stop(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	m := NewManager(t.TempDir(), time.Second, log.NewNopLogger(), prometheus.NewRegistry())
	_, err := m.AppendablesForTenant("user1", []Config{testConfig(t, "remote1", srv.URL)})
	require.NoError(t, err)

	// Stop should not panic and should clean up all storages.
	m.Stop()

	m.mu.Lock()
	storageCount := len(m.storages)
	m.mu.Unlock()
	assert.Equal(t, 0, storageCount)
}
