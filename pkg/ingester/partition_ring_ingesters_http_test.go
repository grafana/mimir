// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/require"
)

func TestPartitionRingIngestersPageHandler(t *testing.T) {
	now := time.Now()
	desc := ring.NewPartitionRingDesc()
	desc.AddPartition(1, ring.PartitionActive, now)
	desc.AddPartition(0, ring.PartitionPending, now)
	desc.AddOrUpdateOwner("ingester-zone-b-1", ring.OwnerActive, 1, now)
	desc.AddOrUpdateOwner("ingester-zone-a-1", ring.OwnerActive, 1, now)
	desc.AddOrUpdateOwner("ingester-zone-a-0", ring.OwnerActive, 0, now)

	partitionRing, err := ring.NewPartitionRing(*desc)
	require.NoError(t, err)
	handler := NewPartitionRingIngestersPageHandler(&partitionRingHolder{pr: partitionRing})

	t.Run("HTML", func(t *testing.T) {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ingester/partition-ring/ingesters", nil))

		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), `<a href="ingester-zone-a-1/ingester/tenants">ingester-zone-a-1</a>`)
	})

	t.Run("JSON", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/ingester/partition-ring/ingesters", nil)
		req.Header.Set("Accept", "application/json")
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		var content partitionRingIngestersPageContent
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &content))
		require.Equal(t, []partitionRingIngester{
			{IngesterID: "ingester-zone-a-0", PartitionID: 0, PartitionState: ring.PartitionPending.String()},
			{IngesterID: "ingester-zone-a-1", PartitionID: 1, PartitionState: ring.PartitionActive.String()},
			{IngesterID: "ingester-zone-b-1", PartitionID: 1, PartitionState: ring.PartitionActive.String()},
		}, content.Ingesters)
	})
}
