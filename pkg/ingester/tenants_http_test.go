// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/grafana/dskit/user"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestIngester_TenantsHandlers(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.LabelChurnEnabled = true

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
		// Check that the tenant with more head series comes first
		require.Regexp(t, fmt.Sprintf(`(?s)href="tsdb/%s".*href="tsdb/%s"`, tenantWithMoreSeries, userID), rec.Body.String())
	})

	t.Run("tenant TSDB for valid tenant", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req, err := http.NewRequest("GET", "/tsdb", nil)
		require.NoError(t, err)

		req = mux.SetURLVars(req, map[string]string{"tenant": userID})
		i.TenantTSDBHandler(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), "<li>Number of series: 1</li>")
		require.Contains(t, rec.Body.String(), fmt.Sprintf(`<a href="%s/label_churn">Label churn</a>`, userID))
	})

	t.Run("tenant TSDB shows block diagnostics", func(t *testing.T) {
		db := i.getTSDB(userID)
		require.NoError(t, db.compactHead(math.MaxInt64))
		blocks := db.Blocks()
		require.Len(t, blocks, 1)
		id := blocks[0].Meta().ULID

		uploaded := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		require.NoError(t, writeShipperMetaFile(nil, db.prometheusDB().Dir(), shipperMeta{
			Version: shipperMetaVersion1,
			Shipped: map[ulid.ULID]model.Time{id: model.TimeFromUnixNano(uploaded.UnixNano())},
		}))
		require.NoError(t, db.prometheus().updateCachedShippedBlocks())

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/tsdb", nil)
		i.TenantTSDBHandler(rec, mux.SetURLVars(req, map[string]string{"tenant": userID}))

		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), "<td>"+id.String()+"</td>")
		require.Contains(t, rec.Body.String(), "<td>"+uploaded.Format(time.RFC3339)+"</td>")
		rec = httptest.NewRecorder()
		req.Header.Set("Accept", "application/json")
		i.TenantTSDBHandler(rec, mux.SetURLVars(req, map[string]string{"tenant": userID}))

		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), fmt.Sprintf(`"Blocks":[{"ID":%q`, id.String()))
		require.Contains(t, rec.Body.String(), fmt.Sprintf(`"UploadedOn":%q`, uploaded.Format(time.RFC3339)))
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
