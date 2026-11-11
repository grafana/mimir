// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/require"
)

type instanceRingMock map[string]ring.InstanceDesc

func (m instanceRingMock) GetInstance(id string) (ring.InstanceDesc, error) {
	instance, ok := m[id]
	if !ok {
		return ring.InstanceDesc{}, fmt.Errorf("instance %s not found", id)
	}
	return instance, nil
}

func (m instanceRingMock) InstancesCount() int {
	return len(m)
}

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

	// ingester-zone-a-0 is not in the instance ring.
	instanceRing := instanceRingMock{
		"ingester-zone-a-1": {Id: "ingester-zone-a-1", Addr: "10.0.0.1:9095", Zone: "zone-a"},
		"ingester-zone-b-1": {Id: "ingester-zone-b-1", Addr: "10.0.0.2:9095", Zone: "zone-b"},
	}
	partitionInstanceRing := ring.NewPartitionInstanceRing(&partitionRingHolder{pr: partitionRing}, instanceRing, time.Minute)

	newRequest := func(accept string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/ingester/partition-ring/ingesters", nil)
		req.Header.Set("Accept", accept)
		return req
	}

	t.Run("HTML with the default link template", func(t *testing.T) {
		var cfg PartitionRingConfig
		flagext.DefaultValues(&cfg)
		handler := NewPartitionRingIngestersPageHandler(partitionInstanceRing, cfg.IngestersPageLinkTemplate)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, newRequest("text/html"))

		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), `<a href="http://10.0.0.1:8080/ingester/tenants">ingester-zone-a-1</a>`)
	})

	t.Run("HTML with a link template for a proxy", func(t *testing.T) {
		handler := NewPartitionRingIngestersPageHandler(partitionInstanceRing, "{ingester}/ingester/tenants")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, newRequest("text/html"))

		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), `<a href="ingester-zone-a-1/ingester/tenants">ingester-zone-a-1</a>`)
	})

	t.Run("JSON with a link template with the zone", func(t *testing.T) {
		handler := NewPartitionRingIngestersPageHandler(partitionInstanceRing, "http://{ingester}.ingester-{zone}:8080/ingester/tenants")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, newRequest("application/json"))

		require.Equal(t, http.StatusOK, rec.Code)
		var content partitionRingIngestersPageContent
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &content))
		require.Equal(t, []partitionRingIngester{
			{IngesterID: "ingester-zone-a-0", Zone: "", Link: "http://ingester-zone-a-0.ingester-:8080/ingester/tenants", PartitionID: 0, PartitionState: ring.PartitionPending.String()},
			{IngesterID: "ingester-zone-a-1", Zone: "zone-a", Link: "http://ingester-zone-a-1.ingester-zone-a:8080/ingester/tenants", PartitionID: 1, PartitionState: ring.PartitionActive.String()},
			{IngesterID: "ingester-zone-b-1", Zone: "zone-b", Link: "http://ingester-zone-b-1.ingester-zone-b:8080/ingester/tenants", PartitionID: 1, PartitionState: ring.PartitionActive.String()},
		}, content.Ingesters)
	})
}
