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

	// Should return 400 for missing matcher
	assert.Equal(t, http.StatusBadRequest, recorder.Result().StatusCode)
	assert.Contains(t, recorder.Body.String(), "at least one matcher is required")
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

	distributor.On("ResourceAttributes", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
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
	// Test deduplication: same time range AND same identifying attrs → deduplicated.
	a := []*ResourceVersionData{
		{MinTimeMs: 1000, MaxTimeMs: 2000, Identifying: map[string]string{"a": "1"}},
		{MinTimeMs: 3000, MaxTimeMs: 4000, Identifying: map[string]string{"a": "2"}},
	}

	b := []*ResourceVersionData{
		{MinTimeMs: 1000, MaxTimeMs: 2000, Identifying: map[string]string{"a": "1"}}, // Duplicate of a[0]
		{MinTimeMs: 5000, MaxTimeMs: 6000, Identifying: map[string]string{"b": "2"}},
	}

	result := mergeResourceVersions(a, b)

	// Should have 3 versions (deduplicating the one with same time range and identifying attrs)
	assert.Len(t, result, 3)
	assert.Equal(t, int64(1000), result[0].MinTimeMs)
	assert.Equal(t, int64(3000), result[1].MinTimeMs)
	assert.Equal(t, int64(5000), result[2].MinTimeMs)

	// The first one should be from 'a' (ingesters take precedence)
	assert.Equal(t, "1", result[0].Identifying["a"])
}

func TestResourceAttributesSeriesHandler_RequiresFilter(t *testing.T) {
	distributor := &mockDistributor{}
	handler := NewResourceAttributesSeriesHandler(distributor, nil, ResourceAttributesHandlerConfig{})

	ctx := user.InjectOrgID(context.Background(), "test-tenant")
	request, err := http.NewRequestWithContext(ctx, "GET", "/api/v1/resources/series", nil)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	assert.Equal(t, http.StatusBadRequest, recorder.Result().StatusCode)
	assert.Contains(t, recorder.Body.String(), "at least one resource.attr parameter is required")
}

func TestResourceAttributesSeriesHandler_InvalidFilter(t *testing.T) {
	distributor := &mockDistributor{}
	handler := NewResourceAttributesSeriesHandler(distributor, nil, ResourceAttributesHandlerConfig{})

	ctx := user.InjectOrgID(context.Background(), "test-tenant")
	request, err := http.NewRequestWithContext(ctx, "GET", "/api/v1/resources/series?resource.attr=nocolon", nil)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	assert.Equal(t, http.StatusBadRequest, recorder.Result().StatusCode)
	assert.Contains(t, recorder.Body.String(), "invalid resource.attr format")
}

func TestResourceAttributesSeriesHandler_Success(t *testing.T) {
	distributor := &mockDistributor{}

	testItems := []*client.SeriesResourceAttributes{
		{
			Labels: []mimirpb.LabelAdapter{
				{Name: "__name__", Value: "test_metric"},
				{Name: "job", Value: "test"},
			},
			Versions: []*client.ResourceVersionData{
				{
					Identifying: map[string]string{"service.name": "my-service"},
					MinTimeMs:   1000,
					MaxTimeMs:   2000,
				},
			},
		},
	}

	distributor.On("ResourceAttributes", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(testItems, nil)

	handler := NewResourceAttributesSeriesHandler(distributor, nil, ResourceAttributesHandlerConfig{})

	ctx := user.InjectOrgID(context.Background(), "test-tenant")
	request, err := http.NewRequestWithContext(ctx, "GET", "/api/v1/resources/series?resource.attr=service.name:my-service", nil)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	assert.Equal(t, http.StatusOK, recorder.Result().StatusCode)

	responseBody, err := io.ReadAll(recorder.Result().Body)
	require.NoError(t, err)

	var resp ResourceAttributesResponse
	err = json.Unmarshal(responseBody, &resp)
	require.NoError(t, err)

	assert.Equal(t, statusSuccess, resp.Status)
	require.NotNil(t, resp.Data)
	assert.Len(t, resp.Data.Series, 1)
	assert.Equal(t, "my-service", resp.Data.Series[0].Versions[0].Identifying["service.name"])
}

func TestParseResourceAttrFilters(t *testing.T) {
	tests := []struct {
		name          string
		input         []string
		expectedKey   string
		expectedValue string
		expectErr     bool
	}{
		{
			name:          "simple key:value",
			input:         []string{"service.name:test"},
			expectedKey:   "service.name",
			expectedValue: "test",
		},
		{
			name:          "value with colons",
			input:         []string{"key:value:with:colons"},
			expectedKey:   "key",
			expectedValue: "value:with:colons",
		},
		{
			name:      "empty key (colon at start)",
			input:     []string{":value"},
			expectErr: true,
		},
		{
			name:      "no colon",
			input:     []string{"nocolon"},
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ingesterFilters, storeFilters, err := parseResourceAttrFilters(tc.input)
			if tc.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, ingesterFilters, 1)
			require.Len(t, storeFilters, 1)
			assert.Equal(t, tc.expectedKey, ingesterFilters[0].Key)
			assert.Equal(t, tc.expectedValue, ingesterFilters[0].Value)
			assert.Equal(t, tc.expectedKey, storeFilters[0].Key)
			assert.Equal(t, tc.expectedValue, storeFilters[0].Value)
		})
	}
}

func TestResourceAttributesHandler_InvalidLimit(t *testing.T) {
	// The forward lookup handler validates limit and returns 400 for non-numeric values.
	// This test serves as a reference for the shard middleware, which should also reject invalid limits.
	distributor := &mockDistributor{}
	handler := NewResourceAttributesHandler(distributor, nil, ResourceAttributesHandlerConfig{})

	ctx := user.InjectOrgID(context.Background(), "test-tenant")
	request, err := http.NewRequestWithContext(ctx, "GET", "/api/v1/resources?match[]={__name__=~\".+\"}&limit=abc", nil)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	assert.Equal(t, http.StatusBadRequest, recorder.Result().StatusCode)
	assert.Contains(t, recorder.Body.String(), "invalid limit parameter")
}

func TestResourceAttributesSeriesHandler_InvalidLimit(t *testing.T) {
	// The series handler should also reject non-numeric limit values.
	distributor := &mockDistributor{}
	handler := NewResourceAttributesSeriesHandler(distributor, nil, ResourceAttributesHandlerConfig{})

	ctx := user.InjectOrgID(context.Background(), "test-tenant")
	request, err := http.NewRequestWithContext(ctx, "GET", "/api/v1/resources/series?resource.attr=service.name:test&limit=abc", nil)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	assert.Equal(t, http.StatusBadRequest, recorder.Result().StatusCode)
	assert.Contains(t, recorder.Body.String(), "invalid limit parameter")
}

func TestResourceAttributesHandler_LimitClamping(t *testing.T) {
	distributor := &mockDistributor{}
	distributor.On("ResourceAttributes", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return([]*client.SeriesResourceAttributes{}, nil)

	tests := []struct {
		name          string
		userLimit     string
		maxLimit      int
		expectedLimit int64 // the limit passed to distributor
	}{
		{
			name:          "user limit exceeds max → clamped",
			userLimit:     "50000",
			maxLimit:      10000,
			expectedLimit: 10000,
		},
		{
			name:          "user limit 0 → uses max",
			userLimit:     "",
			maxLimit:      10000,
			expectedLimit: 10000,
		},
		{
			name:          "user limit within max → unchanged",
			userLimit:     "500",
			maxLimit:      10000,
			expectedLimit: 500,
		},
		{
			name:          "max limit 0 → no clamping",
			userLimit:     "50000",
			maxLimit:      0,
			expectedLimit: 50000,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			distributor.Calls = nil // reset calls
			distributor.ExpectedCalls = nil
			distributor.On("ResourceAttributes", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return([]*client.SeriesResourceAttributes{}, nil)

			maxLimit := tc.maxLimit
			handler := NewResourceAttributesHandler(distributor, nil, ResourceAttributesHandlerConfig{
				MaxResourceAttributesQueryLimit: func(_ string) int { return maxLimit },
			})

			url := "/api/v1/resources?match[]={__name__=~\".+\"}"
			if tc.userLimit != "" {
				url += "&limit=" + tc.userLimit
			}
			ctx := user.InjectOrgID(context.Background(), "test-tenant")
			request, err := http.NewRequestWithContext(ctx, "GET", url, nil)
			require.NoError(t, err)

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)

			assert.Equal(t, http.StatusOK, recorder.Result().StatusCode)

			// Verify the limit passed to distributor
			calls := distributor.Calls
			require.NotEmpty(t, calls)
			passedLimit := calls[0].Arguments.Get(4).(int64)
			assert.Equal(t, tc.expectedLimit, passedLimit)
		})
	}
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
