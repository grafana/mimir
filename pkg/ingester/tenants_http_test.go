// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/stretchr/testify/require"
)

func TestIngester_TenantsHandlers(t *testing.T) {
	ctx := context.Background()
	cfg := defaultIngesterTestConfig(t)

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, i))
	defer services.StopAndAwaitTerminated(ctx, i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	pushSingleSampleAtTime(t, i, time.Now().UnixMilli())

	t.Run("tenants list", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req, err := http.NewRequest("GET", "/tenants", nil)
		require.NoError(t, err)

		i.TenantsHandler(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		// Check if link to user's TSDB was generated
		require.Contains(t, rec.Body.String(), fmt.Sprintf(`<a href="tsdb/%s">%s</a>`, userID, userID))
	})

	t.Run("tenant TSDB for valid tenant", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req, err := http.NewRequest("GET", "/tsdb", nil)
		require.NoError(t, err)

		req = mux.SetURLVars(req, map[string]string{"tenant": userID})
		i.TenantTSDBHandler(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), "<li>Number of series: 1</li>")
	})

	t.Run("tenant TSDB for unknown tenant", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req, err := http.NewRequest("GET", "/tsdb", nil)
		require.NoError(t, err)

		req = mux.SetURLVars(req, map[string]string{"tenant": "unknown"})
		i.TenantTSDBHandler(rec, req)

		require.Equal(t, http.StatusNotFound, rec.Code)
		require.Contains(t, rec.Body.String(), "TSDB not found for tenant unknown")
	})
}
