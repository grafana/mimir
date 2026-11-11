// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestIngester_TenantsHandlers(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)

	// Create ingester
	i, r, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)

	pushSingleSampleAtTime(t, i, time.Now().UnixMilli())

	const tenantWithMoreSeries = "tenant-with-more-series"
	for _, metricName := range []string{"first", "second"} {
		req := mockWriteRequest(t, labels.FromStrings(model.MetricNameLabel, metricName), 0, time.Now().UnixMilli())
		_, err := i.Push(user.InjectOrgID(context.Background(), tenantWithMoreSeries), req)
		require.NoError(t, err)
	}

	t.Run("tenants list", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req, err := http.NewRequest("GET", "/tenants", nil)
		require.NoError(t, err)

		i.TenantsHandler(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		// Check if link to user's TSDB was generated
		require.Contains(t, rec.Body.String(), fmt.Sprintf(`<a href="tsdb/%s">%s</a>`, userID, userID))
		// Check if grace interval is present
		require.Contains(t, rec.Body.String(), `<td>(-0s, +10m0s)</td>`)
	})

	t.Run("tenants list is sorted by the number of head series", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req, err := http.NewRequest("GET", "/tenants", nil)
		require.NoError(t, err)
		req.Header.Set("Accept", "application/json")

		i.TenantsHandler(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		var content tenantsPageContent
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &content))
		require.Len(t, content.Tenants, 2)
		require.Equal(t, tenantWithMoreSeries, content.Tenants[0].Tenant)
		require.Equal(t, uint64(2), content.Tenants[0].Series)
		require.Equal(t, userID, content.Tenants[1].Tenant)
		require.Equal(t, uint64(1), content.Tenants[1].Series)
	})

	t.Run("tenant TSDB for valid tenant", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req, err := http.NewRequest("GET", "/tsdb", nil)
		require.NoError(t, err)

		req = mux.SetURLVars(req, map[string]string{"tenant": userID})
		i.TenantTSDBHandler(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), "<li>Number of series: 1</li>")
		require.NotContains(t, rec.Body.String(), "label_churn")
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

func TestIngester_TenantTSDBHandler_LabelChurnLink(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.LabelChurnEnabled = true

	i, r, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)

	pushSingleSampleAtTime(t, i, time.Now().UnixMilli())

	rec := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/tsdb", nil)
	require.NoError(t, err)

	req = mux.SetURLVars(req, map[string]string{"tenant": userID})
	i.TenantTSDBHandler(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), fmt.Sprintf(`<a href="../label_churn/%s">Label churn</a>`, userID))
}
