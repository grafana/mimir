// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
)

type newGenericQueryCacheFunc func(cache cache.Cache, limits Limits, next http.RoundTripper, logger log.Logger, reg prometheus.Registerer) http.RoundTripper

type testGenericQueryCacheRequestType struct {
	url            *url.URL
	cacheKey       string
	hashedCacheKey string
}

func testGenericQueryCacheRoundTrip(t *testing.T, newRoundTripper newGenericQueryCacheFunc, requestTypeLabelValue string, requestTypes map[string]testGenericQueryCacheRequestType) {
	const (
		userID = "user-1"
	)

	// We need to create a new response each time because (a) it could be manipulated
	// and (b) the Body can only be consumed once.
	downstreamRes := func(statusCode int, body []byte) func() *http.Response {
		return func() *http.Response {
			return &http.Response{
				StatusCode: statusCode,
				Body:       io.NopCloser(bytes.NewReader(body)),
				Header:     http.Header{"Content-Type": []string{"application/json"}},
			}
		}
	}

	tests := map[string]struct {
		init                     func(t *testing.T, cacheBackend cache.Cache, reqCacheKey, reqHashedCacheKey string)
		cacheTTL                 time.Duration
		reqHeader                http.Header
		downstreamRes            func() *http.Response
		downstreamErr            error
		expectedStatusCode       int
		expectedHeader           http.Header
		expectedBody             []byte
		expectedDownstreamCalled bool
		expectedLookupFromCache  bool
		expectedStoredToCache    bool
	}{
		"should fetch the response from the downstream and store it the cache if the downstream request succeed": {
			cacheTTL:                 time.Minute,
			downstreamRes:            downstreamRes(200, []byte(`{content:"fresh"}`)),
			expectedStatusCode:       200,
			expectedHeader:           http.Header{"Content-Type": []string{"application/json"}},
			expectedBody:             []byte(`{content:"fresh"}`),
			expectedDownstreamCalled: true,
			expectedLookupFromCache:  true,
			expectedStoredToCache:    true,
		},
		"should not store the response in the cache if disabled for the tenant": {
			cacheTTL:                 0,
			downstreamRes:            downstreamRes(200, []byte(`{content:"fresh"}`)),
			expectedStatusCode:       200,
			expectedHeader:           http.Header{"Content-Type": []string{"application/json"}},
			expectedBody:             []byte(`{content:"fresh"}`),
			expectedDownstreamCalled: true,
			expectedLookupFromCache:  false,
			expectedStoredToCache:    false,
		},
		"should not store the response in the cache if disabled for the request": {
			cacheTTL:                 time.Minute,
			reqHeader:                http.Header{"Cache-Control": []string{"no-store"}},
			downstreamRes:            downstreamRes(200, []byte(`{content:"fresh"}`)),
			expectedStatusCode:       200,
			expectedHeader:           http.Header{"Content-Type": []string{"application/json"}},
			expectedBody:             []byte(`{content:"fresh"}`),
			expectedDownstreamCalled: true,
			expectedLookupFromCache:  false,
			expectedStoredToCache:    false,
		},
		"should not store the response in the cache if the downstream returned a 4xx status code": {
			cacheTTL:                 time.Minute,
			downstreamRes:            downstreamRes(400, []byte(`{error:"400"}`)),
			expectedStatusCode:       400,
			expectedHeader:           http.Header{"Content-Type": []string{"application/json"}},
			expectedBody:             []byte(`{error:"400"}`),
			expectedDownstreamCalled: true,
			expectedLookupFromCache:  true,
			expectedStoredToCache:    false,
		},
		"should not store the response in the cache if the downstream returned a 5xx status code": {
			cacheTTL:                 time.Minute,
			downstreamRes:            downstreamRes(500, []byte(`{error:"500"}`)),
			expectedStatusCode:       500,
			expectedHeader:           http.Header{"Content-Type": []string{"application/json"}},
			expectedBody:             []byte(`{error:"500"}`),
			expectedDownstreamCalled: true,
			expectedLookupFromCache:  true,
			expectedStoredToCache:    false,
		},
		"should fetch the response from the cache if the cached response is not expired": {
			init: func(t *testing.T, c cache.Cache, reqCacheKey, reqHashedCacheKey string) {
				res := CachedHTTPResponse{CacheKey: reqCacheKey, StatusCode: 200, Body: []byte(`{content:"cached"}`), Headers: []*CachedHTTPHeader{{Name: "Content-Type", Value: "application/json"}}}
				data, err := res.Marshal()
				require.NoError(t, err)

				c.StoreAsync(map[string][]byte{reqHashedCacheKey: data}, time.Minute)
			},
			cacheTTL:                 time.Minute,
			downstreamRes:            downstreamRes(200, []byte(`{content:"fresh"}`)),
			expectedStatusCode:       200,
			expectedHeader:           http.Header{"Content-Type": []string{"application/json"}},
			expectedBody:             []byte(`{content:"cached"}`),
			expectedDownstreamCalled: false, // Should not call the downstream.
			expectedLookupFromCache:  true,
			expectedStoredToCache:    false, // Should not store anything to the cache.
		},
		"should fetch the response from the downstream and overwrite the cached response if corrupted": {
			init: func(t *testing.T, c cache.Cache, _, reqHashedCacheKey string) {
				c.StoreAsync(map[string][]byte{reqHashedCacheKey: []byte("corrupted")}, time.Minute)
			},
			cacheTTL:                 time.Minute,
			downstreamRes:            downstreamRes(200, []byte(`{content:"fresh"}`)),
			expectedStatusCode:       200,
			expectedHeader:           http.Header{"Content-Type": []string{"application/json"}},
			expectedBody:             []byte(`{content:"fresh"}`),
			expectedDownstreamCalled: true,
			expectedLookupFromCache:  true,
			expectedStoredToCache:    true,
		},
		"should fetch the response from the downstream and overwrite the cached response if a key collision was detected": {
			init: func(t *testing.T, c cache.Cache, _, reqHashedCacheKey string) {
				res := CachedHTTPResponse{CacheKey: "another-key", StatusCode: 200, Body: []byte(`{content:"cached"}`), Headers: []*CachedHTTPHeader{{Name: "Content-Type", Value: "application/json"}}}
				data, err := res.Marshal()
				require.NoError(t, err)

				c.StoreAsync(map[string][]byte{reqHashedCacheKey: data}, time.Minute)
			},
			cacheTTL:                 time.Minute,
			downstreamRes:            downstreamRes(200, []byte(`{content:"fresh"}`)),
			expectedStatusCode:       200,
			expectedHeader:           http.Header{"Content-Type": []string{"application/json"}},
			expectedBody:             []byte(`{content:"fresh"}`),
			expectedDownstreamCalled: true,
			expectedLookupFromCache:  true,
			expectedStoredToCache:    true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for reqName, reqData := range requestTypes {
				t.Run(reqName, func(t *testing.T) {
					// Mock the limits.
					limits := multiTenantMockLimits{
						byTenant: map[string]mockLimits{
							userID: {resultsCacheTTLForCardinalityQuery: testData.cacheTTL},
						},
					}

					// Mock the downstream.
					downstreamCalled := false
					downstream := RoundTripFunc(func(request *http.Request) (*http.Response, error) {
						downstreamCalled = true
						return testData.downstreamRes(), testData.downstreamErr
					})

					// Create the request.
					req := &http.Request{URL: reqData.url, Header: testData.reqHeader}
					req = req.WithContext(user.InjectOrgID(context.Background(), userID))

					// Init the cache.
					cacheBackend := cache.NewInstrumentedMockCache()
					if testData.init != nil {
						testData.init(t, cacheBackend, reqData.cacheKey, reqData.hashedCacheKey)
					}
					initialStoreCallsCount := cacheBackend.CountStoreCalls()

					reg := prometheus.NewPedanticRegistry()
					rt := newRoundTripper(cacheBackend, limits, downstream, testutil.NewLogger(t), reg)
					res, err := rt.RoundTrip(req)
					require.NoError(t, err)

					// Assert on the downstream.
					assert.Equal(t, testData.expectedDownstreamCalled, downstreamCalled)

					// Assert on the response received.
					assert.Equal(t, testData.expectedStatusCode, res.StatusCode)
					assert.Equal(t, testData.expectedHeader, res.Header)

					actualBody, err := io.ReadAll(res.Body)
					require.NoError(t, err)
					assert.Equal(t, testData.expectedBody, actualBody)

					// Assert on the state of the cache.
					if testData.expectedStoredToCache {
						assert.Equal(t, initialStoreCallsCount+1, cacheBackend.CountStoreCalls())

						items := cacheBackend.GetItems()
						require.Len(t, items, 1)
						require.NotZero(t, items[reqData.hashedCacheKey])

						cached := CachedHTTPResponse{}
						require.NoError(t, cached.Unmarshal(items[reqData.hashedCacheKey].Data))
						assert.Equal(t, testData.expectedStatusCode, int(cached.StatusCode))
						assert.Equal(t, testData.expectedHeader, DecodeCachedHTTPResponse(&cached).Header)
						assert.Equal(t, testData.expectedBody, cached.Body)
						assert.Equal(t, reqData.cacheKey, cached.CacheKey)
						assert.WithinDuration(t, time.Now().Add(testData.cacheTTL), items[reqData.hashedCacheKey].ExpiresAt, 5*time.Second)
					} else {
						assert.Equal(t, initialStoreCallsCount, cacheBackend.CountStoreCalls())
					}

					// Assert on metrics.
					expectedRequestsCount := 0
					expectedHitsCount := 0
					if testData.expectedLookupFromCache {
						expectedRequestsCount = 1
						if !testData.expectedDownstreamCalled {
							expectedHitsCount = 1
						}
					}

					assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
                        # HELP cortex_frontend_query_result_cache_requests_total Total number of requests (or partial requests) looked up in the results cache.
                        # TYPE cortex_frontend_query_result_cache_requests_total counter
                        cortex_frontend_query_result_cache_requests_total{request_type="%s"} %d

                        # HELP cortex_frontend_query_result_cache_hits_total Total number of requests (or partial requests) fetched from the results cache.
                        # TYPE cortex_frontend_query_result_cache_hits_total counter
                        cortex_frontend_query_result_cache_hits_total{request_type="%s"} %d
					`, requestTypeLabelValue, expectedRequestsCount, requestTypeLabelValue, expectedHitsCount)),
						"cortex_frontend_query_result_cache_requests_total",
						"cortex_frontend_query_result_cache_hits_total",
					))
				})
			}
		})
	}
}

func TestCardinalityQueryCache_RoundTrip_WithTenantFederation(t *testing.T) {
	// Enable tenant ID resolve used when tenant federation is enabled.
	tenant.WithDefaultResolver(tenant.NewMultiResolver())
	t.Cleanup(func() {
		tenant.WithDefaultResolver(tenant.NewSingleResolver())
	})

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
			downstream := RoundTripFunc(func(request *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(strings.NewReader("{}")),
					Header:     http.Header{"Content-Type": []string{"application/json"}},
				}, nil
			})

			// Create the request.
			reqURL := mustParseURL(t, `/prometheus/api/v1/cardinality/label_names?selector={job="test"}&limit=100`)
			reqCacheKey := tenant.JoinTenantIDs(testData.tenantIDs) + ":job=\"test\"\x00100"
			reqHashedCacheKey := cardinalityLabelNamesQueryCachePrefix + cacheHashKey(reqCacheKey)

			req := &http.Request{URL: reqURL}
			req = req.WithContext(user.InjectOrgID(context.Background(), tenant.JoinTenantIDs(testData.tenantIDs)))

			// Init the RoundTripper.
			cacheBackend := cache.NewInstrumentedMockCache()
			limits := multiTenantMockLimits{byTenant: testData.limits}

			rt := newCardinalityQueryCacheRoundTripper(cacheBackend, limits, downstream, testutil.NewLogger(t), nil)
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
			url:            mustParseURL(t, `/prometheus/api/v1/cardinality/label_names?selector={job="test"}&limit=100`),
			cacheKey:       "user-1:job=\"test\"\x00100",
			hashedCacheKey: cardinalityLabelNamesQueryCachePrefix + cacheHashKey("user-1:job=\"test\"\x00100"),
		},
		"label values request": {
			url:            mustParseURL(t, `/prometheus/api/v1/cardinality/label_values?selector={job="test"}&label_names[]=metric_1&label_names[]=metric_2&limit=100`),
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
