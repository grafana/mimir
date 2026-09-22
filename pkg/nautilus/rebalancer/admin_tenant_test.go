// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

func TestAdminHTML_RendersTenantRangeSelector(t *testing.T) {
	r, _ := tenantInspectorTestRebalancer(t)

	rec := httptest.NewRecorder()
	r.serveAdminHTML(rec)
	body := rec.Body.String()

	assert.Contains(t, body, "Tenant hash ranges")
	assert.Contains(t, body, `id="tenantSelect"`)
	assert.Contains(t, body, `data-endpoint="`+adminPathPrefix+`/tenant-ranges"`)
	assert.Contains(t, body, `<option value="tenant-a">tenant-a</option>`)
	assert.Contains(t, body, `<option value="tenant-b">tenant-b</option>`)
	assert.Less(t, strings.Index(body, `value="tenant-a"`), strings.Index(body, `value="tenant-b"`))
	assert.Contains(t, body, "<th>Lo</th>")
	assert.Contains(t, body, "<th>Hi</th>")
	assert.Contains(t, body, "<th class=\"numeric\">Partition</th>")
	assert.Contains(t, body, "<th class=\"numeric\">Head series</th>")
	assert.Contains(t, body, "<th class=\"numeric\">Samples/s</th>")
}

func TestServeTenantRanges_ReturnsCurrentTenantAssignmentAndLoad(t *testing.T) {
	r, firstRange := tenantInspectorTestRebalancer(t)

	req := httptest.NewRequest(http.MethodGet, adminPathPrefix+"/tenant-ranges?tenant=tenant-a", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))

	var response struct {
		TenantID string            `json:"tenant_id"`
		Ranges   []tenantRangeView `json:"ranges"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	assert.Equal(t, "tenant-a", response.TenantID)
	require.Len(t, response.Ranges, 2)

	assert.Equal(t, firstRange.Lo, response.Ranges[0].Lo)
	assert.Equal(t, firstRange.Hi, response.Ranges[0].Hi)
	assert.Equal(t, int32(10), response.Ranges[0].PartitionID)
	assert.Equal(t, int64(1234), response.Ranges[0].HeadSeries)
	assert.Equal(t, 456.75, response.Ranges[0].SampleRate)
	assert.True(t, response.Ranges[0].LoadAvailable)

	assert.Equal(t, int32(11), response.Ranges[1].PartitionID)
	assert.False(t, response.Ranges[1].LoadAvailable)
	for _, item := range response.Ranges {
		assert.NotEqual(t, int32(20), item.PartitionID, "tenant-b ranges must not leak into tenant-a")
	}
}

func TestServeTenantRanges_RequiresTenant(t *testing.T) {
	r, _ := tenantInspectorTestRebalancer(t)

	req := httptest.NewRequest(http.MethodGet, adminPathPrefix+"/tenant-ranges", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func tenantInspectorTestRebalancer(t *testing.T) (*Rebalancer, assignment.HashRange) {
	t.Helper()

	r := newTestRebalancerForReset(t, 21, nil)
	now := time.Now()
	tenantA := assignment.EvenSplitForTenant("tenant-a", []int32{10, 11})
	tenantB := assignment.EvenSplitForTenant("tenant-b", []int32{20})
	current := &assignment.Assignment{
		Entries: append(append([]assignment.Entry(nil), tenantA.Entries...), tenantB.Entries...),
	}
	require.True(t, r.store.apply(now, current, r.cfg.LeaseDuration, r.hashLeaseLookahead(), r.cfg.EntryRetention))

	first := tenantA.Entries[0]
	lm := buildLoadMap([]rangeRate{{
		tenantID:    first.TenantID,
		partitionID: first.PartitionID,
		hr:          first.Range,
		series:      1234,
		sampleRate:  456.75,
	}})
	r.admin.setLastStats(lm, map[int32]int64{10: 1234}, map[int32]float64{10: 456.75}, []int32{10, 11, 20})

	return r, first.Range
}
