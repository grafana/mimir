// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestResourceAttributesHandler_RequiresMatcher(t *testing.T) {
	distributor := &mockDistributor{}
	handler := NewResourceAttributesHandler(distributor, nil, ResourceAttributesHandlerConfig{})

	ctx := user.InjectOrgID(context.Background(), "test-tenant")
	request, err := http.NewRequestWithContext(ctx, "GET", "/api/v1/resources", nil)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	// Should return error for missing matcher
	assert.Equal(t, http.StatusOK, recorder.Result().StatusCode)

	var resp ResourceAttributesResponse
	err = json.Unmarshal(recorder.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Equal(t, statusError, resp.Status)
	assert.Contains(t, resp.Error, "at least one matcher is required")
}

func TestResourceAttributesHandler_Success(t *testing.T) {
	distributor := &mockDistributor{}

	// Mock the distributor to return test data
	testItems := []*client.SeriesResourceAttributes{
		{
			Labels: []mimirpb.LabelAdapter{
				{Name: "__name__", Value: "test_metric"},
				{Name: "job", Value: "test"},
			},
			Versions: []*client.ResourceVersionData{
				{
					Identifying: map[string]string{"service.name": "my-service"},
					Descriptive: map[string]string{"service.version": "1.0.0"},
					MinTimeMs:   1000,
					MaxTimeMs:   2000,
				},
			},
		},
	}

	distributor.On("ResourceAttributes", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(testItems, nil)

	handler := NewResourceAttributesHandler(distributor, nil, ResourceAttributesHandlerConfig{})

	ctx := user.InjectOrgID(context.Background(), "test-tenant")
	request, err := http.NewRequestWithContext(ctx, "GET", "/api/v1/resources?match[]={__name__=~\".+\"}", nil)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	// Should return success
	assert.Equal(t, http.StatusOK, recorder.Result().StatusCode)

	responseBody, err := io.ReadAll(recorder.Result().Body)
	require.NoError(t, err)

	var resp ResourceAttributesResponse
	err = json.Unmarshal(responseBody, &resp)
	require.NoError(t, err)

	assert.Equal(t, statusSuccess, resp.Status)
	assert.Len(t, resp.Data.Series, 1)
	assert.Equal(t, "test_metric", resp.Data.Series[0].Labels["__name__"])
	assert.Len(t, resp.Data.Series[0].Versions, 1)
	assert.Equal(t, "my-service", resp.Data.Series[0].Versions[0].Identifying["service.name"])
}

func TestResourceAttributesHandler_RequiresTenant(t *testing.T) {
	distributor := &mockDistributor{}
	handler := NewResourceAttributesHandler(distributor, nil, ResourceAttributesHandlerConfig{})

	// No tenant ID in context
	request, err := http.NewRequest("GET", "/api/v1/resources?match[]={__name__=~\".+\"}", nil)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	// Should return 400 for missing tenant
	assert.Equal(t, http.StatusBadRequest, recorder.Result().StatusCode)
}

func TestMergeResourceAttributesSeries(t *testing.T) {
	// Test case: Merging series from ingesters and store-gateways
	ingesterSeries := []*SeriesResourceAttributesData{
		{
			Labels: map[string]string{"__name__": "metric1", "job": "test"},
			Versions: []*ResourceVersionData{
				{MinTimeMs: 5000, MaxTimeMs: 6000, Identifying: map[string]string{"service.name": "svc1"}},
			},
		},
		{
			Labels: map[string]string{"__name__": "metric2", "job": "test"},
			Versions: []*ResourceVersionData{
				{MinTimeMs: 7000, MaxTimeMs: 8000, Identifying: map[string]string{"service.name": "svc2"}},
			},
		},
	}

	storeSeries := []*SeriesResourceAttributesData{
		{
			Labels: map[string]string{"__name__": "metric1", "job": "test"}, // Same as ingester
			Versions: []*ResourceVersionData{
				{MinTimeMs: 1000, MaxTimeMs: 2000, Identifying: map[string]string{"service.name": "svc1-old"}},
				{MinTimeMs: 3000, MaxTimeMs: 4000, Identifying: map[string]string{"service.name": "svc1"}},
			},
		},
		{
			Labels: map[string]string{"__name__": "metric3", "job": "test"}, // Only in store
			Versions: []*ResourceVersionData{
				{MinTimeMs: 1000, MaxTimeMs: 2000, Identifying: map[string]string{"service.name": "svc3"}},
			},
		},
	}

	result := mergeResourceAttributesSeries(ingesterSeries, storeSeries)

	// Should have 3 series (metric1, metric2, metric3)
	assert.Len(t, result, 3)

	// Find metric1 - should have merged versions from both sources
	var metric1 *SeriesResourceAttributesData
	for _, s := range result {
		if s.Labels["__name__"] == "metric1" {
			metric1 = s
			break
		}
	}
	require.NotNil(t, metric1)
	// Should have 3 versions (2 from store + 1 from ingester, sorted by time)
	assert.Len(t, metric1.Versions, 3)
	assert.Equal(t, int64(1000), metric1.Versions[0].MinTimeMs)
	assert.Equal(t, int64(3000), metric1.Versions[1].MinTimeMs)
	assert.Equal(t, int64(5000), metric1.Versions[2].MinTimeMs)
}

func TestMergeResourceVersions(t *testing.T) {
	// Test deduplication of overlapping time ranges
	a := []*ResourceVersionData{
		{MinTimeMs: 1000, MaxTimeMs: 2000, Identifying: map[string]string{"a": "1"}},
		{MinTimeMs: 3000, MaxTimeMs: 4000, Identifying: map[string]string{"a": "2"}},
	}

	b := []*ResourceVersionData{
		{MinTimeMs: 1000, MaxTimeMs: 2000, Identifying: map[string]string{"b": "1"}}, // Same time range as a[0]
		{MinTimeMs: 5000, MaxTimeMs: 6000, Identifying: map[string]string{"b": "2"}},
	}

	result := mergeResourceVersions(a, b)

	// Should have 3 versions (deduplicating the ones with same time range)
	assert.Len(t, result, 3)
	assert.Equal(t, int64(1000), result[0].MinTimeMs)
	assert.Equal(t, int64(3000), result[1].MinTimeMs)
	assert.Equal(t, int64(5000), result[2].MinTimeMs)

	// The first one should be from 'a' (ingesters take precedence)
	assert.Equal(t, "1", result[0].Identifying["a"])
}

func TestLabelsMapToKey(t *testing.T) {
	labels1 := map[string]string{"__name__": "metric", "job": "test", "instance": "localhost"}
	labels2 := map[string]string{"instance": "localhost", "__name__": "metric", "job": "test"}

	// Same labels in different order should produce the same key
	assert.Equal(t, labelsMapToKey(labels1), labelsMapToKey(labels2))

	// Different labels should produce different keys
	labels3 := map[string]string{"__name__": "metric", "job": "other"}
	assert.NotEqual(t, labelsMapToKey(labels1), labelsMapToKey(labels3))
}
