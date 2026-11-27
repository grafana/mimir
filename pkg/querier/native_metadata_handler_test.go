// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/distributor"
	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

// fakeNativeMetadataDistributor answers with its results, or its error, and records what it was asked.
type fakeNativeMetadataDistributor struct {
	resources   distributor.NativeResourcesResult
	metadata    distributor.NativeMetricMetadataResult
	err         error
	matcherSets [][]*labels.Matcher
	start, end  int64
	limit       int
}

func (f *fakeNativeMetadataDistributor) NativeMetricMetadata(_ context.Context, matcherSets [][]*labels.Matcher, limit int) (distributor.NativeMetricMetadataResult, error) {
	f.matcherSets, f.limit = matcherSets, limit
	return f.metadata, f.err
}

func (f *fakeNativeMetadataDistributor) NativeResources(_ context.Context, matcherSets [][]*labels.Matcher, start, end int64, limit int, _ bool) (distributor.NativeResourcesResult, error) {
	f.matcherSets, f.start, f.end, f.limit = matcherSets, start, end, limit
	return f.resources, f.err
}

func (f *fakeNativeMetadataDistributor) NativeSeriesResources(context.Context, [][]*labels.Matcher, int64, int64) (distributor.NativeResourcesResult, func(), error) {
	return distributor.NativeResourcesResult{}, func() {}, nil
}

func serveNativeLookup(t *testing.T, handler http.Handler, target string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, target, nil).WithContext(user.InjectOrgID(t.Context(), "tenant"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestNativeResourcesHandler(t *testing.T) {
	d := &fakeNativeMetadataDistributor{resources: distributor.NativeResourcesResult{
		Series: []ingester_client.NativeResourcesSeries{{
			Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "up"}, {Name: "job", Value: "api"}},
			Versions: []ingester_client.NativeResourceVersion{
				{MinTimeMs: 1_000, MaxTimeMs: 1_999, Identifying: map[string]string{"service.name": "api"}, Descriptive: map[string]string{"host.name": "node-1"}},
				{MinTimeMs: 2_000, MaxTimeMs: 3_000, Identifying: map[string]string{"service.name": "api"}, Descriptive: map[string]string{"host.name": "node-2"}},
			},
			Coalesced: true,
		}},
		Limited:       true,
		CoverageStart: 500,
	}}
	handler := NativeResourcesHandler(d)

	rec := serveNativeLookup(t, handler, `/api/v1/resources?match[]=up{job="api"}&start=1&end=4`)
	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"status": "success", "data": {
		"results": [{
			"labels": {"__name__": "up", "job": "api"},
			"versions": [
				{"resource_attributes": {"identifying": {"service.name": "api"}, "descriptive": {"host.name": "node-1"}}, "min_time_ms": 1000, "max_time_ms": 1999},
				{"resource_attributes": {"identifying": {"service.name": "api"}, "descriptive": {"host.name": "node-2"}}, "min_time_ms": 2000, "max_time_ms": 3000}
			],
			"coalesced": true
		}],
		"limited": true,
		"coverage_start_ms": 500
	}}`, rec.Body.String())
	require.Len(t, d.matcherSets, 1)
	require.Equal(t, int64(1_000), d.start)
	require.Equal(t, int64(4_000), d.end)
	require.Equal(t, nativeLookupDefaultLimit, d.limit)

	// With latest, only each series' last version; without start and end, all of them.
	rec = serveNativeLookup(t, handler, `/api/v1/resources?match[]=up{job="api"}&latest=true&limit=10`)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"node-2"`)
	require.NotContains(t, rec.Body.String(), `"node-1"`)
	require.Equal(t, int64(math.MinInt64), d.start)
	require.Equal(t, int64(math.MaxInt64), d.end)
	require.Equal(t, 10, d.limit)
}

func TestNativeMetricMetadataHandler(t *testing.T) {
	d := &fakeNativeMetadataDistributor{metadata: distributor.NativeMetricMetadataResult{
		Series: []ingester_client.NativeMetricMetadataSeries{{
			Labels:    []mimirpb.LabelAdapter{{Name: "__name__", Value: "requests_total"}},
			Versions:  []ingester_client.NativeMetricMetadataVersion{{MinTimeMs: 1_000, MaxTimeMs: 2_000, Type: mimirpb.COUNTER, Unit: "requests", Help: "Requests."}},
			Truncated: true,
		}},
		CoverageStart: 1_000,
	}}
	rec := serveNativeLookup(t, NativeMetricMetadataHandler(d), `/api/v1/native_metric_metadata?match[]=requests_total`)
	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"status": "success", "data": {
		"results": [{
			"labels": {"__name__": "requests_total"},
			"versions": [{"type": "counter", "unit": "requests", "help": "Requests.", "min_time_ms": 1000, "max_time_ms": 2000}],
			"truncated": true,
			"coalesced": false
		}],
		"limited": false,
		"coverage_start_ms": 1000
	}}`, rec.Body.String())
}

// The endpoints refuse lookups that would go through every series of a tenant, or more series than the maximum limit,
// and tell a disabled tenant and a lookup over an ingester's cap apart from failures.
func TestNativeLookupHandlersRefuse(t *testing.T) {
	for name, tc := range map[string]struct {
		target   string
		err      error
		wantCode int
	}{
		"without match[]":                     {target: `/api/v1/resources`, wantCode: http.StatusBadRequest},
		"a match[] that matches every series": {target: `/api/v1/resources?match[]={job=~".*"}`, wantCode: http.StatusBadRequest},
		"a limit over the maximum":            {target: `/api/v1/resources?match[]=up&limit=10001`, wantCode: http.StatusBadRequest},
		"a disabled tenant":                   {target: `/api/v1/resources?match[]=up`, err: ingester_client.ErrNativeMetadataDisabled, wantCode: http.StatusBadRequest},
		"a lookup over an ingester's cap":     {target: `/api/v1/resources?match[]=up`, err: ingester_client.NativeLookupTooManySeriesError(10), wantCode: http.StatusUnprocessableEntity},
	} {
		t.Run(name, func(t *testing.T) {
			rec := serveNativeLookup(t, NativeResourcesHandler(&fakeNativeMetadataDistributor{err: tc.err}), tc.target)
			require.Equal(t, tc.wantCode, rec.Code)
			require.Contains(t, rec.Body.String(), `"status":"error"`)
		})
	}
}
