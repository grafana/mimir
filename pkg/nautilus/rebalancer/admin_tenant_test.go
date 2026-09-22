// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
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
	assert.Contains(t, body, `method="GET" action="`+adminPathPrefix+`"`)
	assert.Contains(t, body, `name="tenant" onchange="this.form.submit()"`)
	assert.NotContains(t, body, "fetch(", "tenant selection must use a normal page reload, not AJAX")
	assert.Contains(t, body, `<option value="tenant-a">tenant-a</option>`)
	assert.Contains(t, body, `<option value="tenant-b">tenant-b</option>`)
	assert.Less(t, strings.Index(body, `value="tenant-a"`), strings.Index(body, `value="tenant-b"`))
	assert.Contains(t, body, "Choose a tenant to inspect its current assignment.")
}

func TestBuildTenantRangeViews_ReturnsCurrentTenantAssignmentAndLoad(t *testing.T) {
	r, firstRange := tenantInspectorTestRebalancer(t)

	_, lastStats, _, _ := r.admin.snapshot()
	current := r.store.latestActiveAssignment(r.now())
	ranges := buildTenantRangeViews(current, lastStats, "tenant-a")
	require.Len(t, ranges, 2)

	assert.Equal(t, firstRange.Lo, ranges[0].Lo)
	assert.Equal(t, firstRange.Hi, ranges[0].Hi)
	assert.Equal(t, int32(10), ranges[0].PartitionID)
	assert.Equal(t, int64(1234), ranges[0].HeadSeries)
	assert.Equal(t, 456.75, ranges[0].SampleRate)
	assert.True(t, ranges[0].LoadAvailable)

	assert.Equal(t, int32(11), ranges[1].PartitionID)
	assert.False(t, ranges[1].LoadAvailable)
	for _, item := range ranges {
		assert.NotEqual(t, int32(20), item.PartitionID, "tenant-b ranges must not leak into tenant-a")
	}
}

func TestAdminHTML_TenantQueryRendersSelectedRanges(t *testing.T) {
	r, _ := tenantInspectorTestRebalancer(t)

	req := httptest.NewRequest(http.MethodGet, adminPathPrefix+"?tenant=tenant-a", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, `<option value="tenant-a" selected>tenant-a</option>`)
	assert.Contains(t, body, "<th>Lo</th>")
	assert.Contains(t, body, "<th>Hi</th>")
	assert.Contains(t, body, "<th class=\"numeric\">Partition</th>")
	assert.Contains(t, body, "<th class=\"numeric\">Head series</th>")
	assert.Contains(t, body, "<th class=\"numeric\">Samples/s</th>")
	assert.Contains(t, body, `<td class="hash-bound">0x00000000</td>`)
	assert.Contains(t, body, `<td class="hash-bound">0x7fffffff</td>`)
	assert.Contains(t, body, `<td class="numeric">P10</td>`)
	assert.Contains(t, body, "2 current hash ranges.")
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
