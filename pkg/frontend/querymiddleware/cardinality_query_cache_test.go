// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/grafana/dskit/cache"
	"github.com/prometheus/prometheus/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
)

func TestCardinalityQueryCache_RoundTrip(t *testing.T) {
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
		expectedStoredToCache    bool
	}{
		"should fetch the response from the downstream and store it the cache if the downstream request succeed": {
			cacheTTL:                 time.Minute,
			downstreamRes:            downstreamRes(200, []byte(`{content:"fresh"}`)),
			expectedStatusCode:       200,
			expectedHeader:           http.Header{"Content-Type": []string{"application/json"}},
			expectedBody:             []byte(`{content:"fresh"}`),
			expectedDownstreamCalled: true,
			expectedStoredToCache:    true,
		},
		"should not store the response in the cache if disabled for the tenant": {
			cacheTTL:                 0,
			downstreamRes:            downstreamRes(200, []byte(`{content:"fresh"}`)),
			expectedStatusCode:       200,
			expectedHeader:           http.Header{"Content-Type": []string{"application/json"}},
			expectedBody:             []byte(`{content:"fresh"}`),
			expectedDownstreamCalled: true,
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
			expectedStoredToCache:    false,
		},
		"should not store the response in the cache if the downstream returned a 4xx status code": {
			cacheTTL:                 0,
			downstreamRes:            downstreamRes(400, []byte(`{error:"400"}`)),
			expectedStatusCode:       400,
			expectedHeader:           http.Header{"Content-Type": []string{"application/json"}},
			expectedBody:             []byte(`{error:"400"}`),
			expectedDownstreamCalled: true,
			expectedStoredToCache:    false,
		},
		"should not store the response in the cache if the downstream returned a 5xx status code": {
			cacheTTL:                 0,
			downstreamRes:            downstreamRes(500, []byte(`{error:"500"}`)),
			expectedStatusCode:       500,
			expectedHeader:           http.Header{"Content-Type": []string{"application/json"}},
			expectedBody:             []byte(`{error:"500"}`),
			expectedDownstreamCalled: true,
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
			expectedStoredToCache:    true,
		},
	}

	requests := map[string]struct {
		url            *url.URL
		cacheKey       string
		hashedCacheKey string
	}{
		"label names request": {
			url:            mustParseURL(t, `/prometheus/api/v1/cardinality/label_names?selector={job="test"}&limit=100`),
			cacheKey:       "user-1:job=\"test\"\x00100",
			hashedCacheKey: cardinalityLabelNamesQueryCachePrefix + cacheHashKey("user-1:job=\"test\"\x00100"),
		},
		"label values request": {
			url:            mustParseURL(t, `/prometheus/api/v1/cardinality/label_values?selector={job="test"}&label_names[]=metric_1&label_names[]=metric_2&limit=100`),
			cacheKey:       "user-1:metric_1\x01metric_2\x00job=\"test\"\x00100",
			hashedCacheKey: cardinalityLabelValuesQueryCachePrefix + cacheHashKey("user-1:metric_1\x01metric_2\x00job=\"test\"\x00100"),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for reqName, reqData := range requests {
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

					rt := newCardinalityQueryCacheRoundTripper(cacheBackend, limits, downstream, testutil.NewLogger(t))
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

						hits := cacheBackend.Fetch(context.Background(), []string{reqData.hashedCacheKey})
						require.Len(t, hits, 1)

						cached := CachedHTTPResponse{}
						require.NoError(t, cached.Unmarshal(hits[reqData.hashedCacheKey]))
						assert.Equal(t, testData.expectedStatusCode, int(cached.StatusCode))
						assert.Equal(t, testData.expectedHeader, DecodeCachedHTTPResponse(&cached).Header)
						assert.Equal(t, testData.expectedBody, cached.Body)
						assert.Equal(t, reqData.cacheKey, cached.CacheKey)
					} else {
						assert.Equal(t, initialStoreCallsCount, cacheBackend.CountStoreCalls())
					}
				})
			}
		})
	}
}

func mustParseURL(t *testing.T, rawURL string) *url.URL {
	parsed, err := url.Parse(rawURL)
	require.NoError(t, err)
	return parsed
}
