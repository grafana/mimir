// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mimirtest "github.com/grafana/mimir/pkg/util/test"
)

func TestCardinalityQueryCache_RoundTrip_WithTenantFederation(t *testing.T) {
	tests := map[string]struct {
		tenantIDs        []string
		limits           map[string]mockLimits
		expectedCacheTTL time.Duration
	}{
		"should disable the cache if there's 1 tenant with TTL = 0": {
			tenantIDs: []string{"user-1", "user-2"},
			limits: map[string]mockLimits{
				"user-1": {resultsCacheTTLForCardinalityQuery: time.Minute},
				"user-2": {resultsCacheTTLForCardinalityQuery: 0},
			},
			expectedCacheTTL: 0,
		},
		"should use lowest TTL among request tenants if all tenants have cache enabled (TTL > 0)": {
			tenantIDs: []string{"user-1", "user-2"},
			limits: map[string]mockLimits{
				"user-1": {resultsCacheTTLForCardinalityQuery: time.Hour},
				"user-2": {resultsCacheTTLForCardinalityQuery: time.Minute},
			},
			expectedCacheTTL: time.Minute,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Mock the downstream.
			downstream := RoundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(strings.NewReader("{}")),
					Header:     http.Header{"Content-Type": []string{"application/json"}},
				}, nil
			})

			// Create the request.
			reqURL := mustParseURL(t, `/prometheus/api/v1/cardinality/label_names?selector={job="test"}&limit=100`)
			reqCacheKey := tenant.JoinTenantIDs(testData.tenantIDs) + ":job=\"test\"\x00inmemory\x00100"
			reqHashedCacheKey := cardinalityLabelNamesQueryCachePrefix + cacheHashKey(reqCacheKey)

			req := &http.Request{URL: reqURL}
			req = req.WithContext(user.InjectOrgID(context.Background(), tenant.JoinTenantIDs(testData.tenantIDs)))

			// Init the RoundTripper.
			cacheBackend := cache.NewInstrumentedMockCache()
			limits := multiTenantMockLimits{byTenant: testData.limits}

			rt := newCardinalityQueryCacheRoundTripper(cacheBackend, DefaultCacheKeyGenerator{}, limits, downstream, mimirtest.NewTestingLogger(t), nil)
			res, err := rt.RoundTrip(req)
			require.NoError(t, err)

			// Assert on the response received.
			assert.Equal(t, 200, res.StatusCode)

			actualBody, err := io.ReadAll(res.Body)
			require.NoError(t, err)
			assert.Equal(t, []byte("{}"), actualBody)

			// Assert on the state of the cache.
			if testData.expectedCacheTTL > 0 {
				assert.Equal(t, 1, cacheBackend.CountStoreCalls())

				items := cacheBackend.GetItems()
				require.Len(t, items, 1)
				require.NotZero(t, items[reqHashedCacheKey])

				cached := CachedHTTPResponse{}
				require.NoError(t, cached.Unmarshal(items[reqHashedCacheKey].Data))
				assert.Equal(t, 200, int(cached.StatusCode))
				assert.Equal(t, []byte("{}"), cached.Body)
				assert.Equal(t, reqCacheKey, cached.CacheKey)
				assert.WithinDuration(t, time.Now().Add(testData.expectedCacheTTL), items[reqHashedCacheKey].ExpiresAt, 5*time.Second)
			} else {
				assert.Equal(t, 0, cacheBackend.CountStoreCalls())
			}
		})
	}
}

func TestCardinalityQueryCache_RoundTrip(t *testing.T) {
	testGenericQueryCacheRoundTrip(t, newCardinalityQueryCacheRoundTripper, "cardinality", map[string]testGenericQueryCacheRequestType{
		"label names request": {
			reqPath:        "/prometheus/api/v1/cardinality/label_names",
			reqData:        url.Values{"selector": []string{`{job="test"}`}, "limit": []string{"100"}, "count_method": []string{"active"}},
			cacheKey:       "user-1:job=\"test\"\x00active\x00100",
			hashedCacheKey: cardinalityLabelNamesQueryCachePrefix + cacheHashKey("user-1:job=\"test\"\x00active\x00100"),
		},
		"label values request": {
			reqPath:        "/prometheus/api/v1/cardinality/label_values",
			reqData:        url.Values{"selector": []string{`{job="test"}`}, "label_names[]": []string{"metric_1", "metric_2"}, "limit": []string{"100"}},
			cacheKey:       "user-1:metric_1\x01metric_2\x00job=\"test\"\x00inmemory\x00100",
			hashedCacheKey: cardinalityLabelValuesQueryCachePrefix + cacheHashKey("user-1:metric_1\x01metric_2\x00job=\"test\"\x00inmemory\x00100"),
		},
	})
}

func mustParseURL(t *testing.T, rawURL string) *url.URL {
	parsed, err := url.Parse(rawURL)
	require.NoError(t, err)
	return parsed
}
