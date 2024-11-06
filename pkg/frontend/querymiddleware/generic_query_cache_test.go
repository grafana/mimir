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
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mimirtest "github.com/grafana/mimir/pkg/util/test"
)

type newGenericQueryCacheFunc func(cache cache.Cache, splitter CacheKeyGenerator, limits Limits, next http.RoundTripper, logger log.Logger, reg prometheus.Registerer) http.RoundTripper

type testGenericQueryCacheRequestType struct {
	reqPath        string
	reqData        url.Values
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

				c.SetMultiAsync(map[string][]byte{reqHashedCacheKey: data}, time.Minute)
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
			init: func(_ *testing.T, c cache.Cache, _, reqHashedCacheKey string) {
				c.SetMultiAsync(map[string][]byte{reqHashedCacheKey: []byte("corrupted")}, time.Minute)
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

				c.SetMultiAsync(map[string][]byte{reqHashedCacheKey: data}, time.Minute)
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
				for _, reqMethod := range []string{http.MethodGet, http.MethodPost} {
					t.Run(fmt.Sprintf("%s (%s)", reqName, reqMethod), func(t *testing.T) {
						// Mock the limits.
						limits := multiTenantMockLimits{
							byTenant: map[string]mockLimits{
								userID: {
									resultsCacheTTLForCardinalityQuery: testData.cacheTTL,
									resultsCacheTTLForLabelsQuery:      testData.cacheTTL,
								},
							},
						}

						var (
							req                 *http.Request
							downstreamCalled    = false
							downstreamReqParams url.Values
							err                 error
						)

						// Mock the downstream and capture the request.
						downstream := RoundTripFunc(func(req *http.Request) (*http.Response, error) {
							downstreamCalled = true

							// Parse the request body.
							require.NoError(t, req.ParseForm())
							downstreamReqParams = req.Form

							return testData.downstreamRes(), testData.downstreamErr
						})

						// Create the request.
						switch reqMethod {
						case http.MethodGet:
							req, err = http.NewRequest(reqMethod, reqData.reqPath+"?"+reqData.reqData.Encode(), nil)
							require.NoError(t, err)
						case http.MethodPost:
							req, err = http.NewRequest(reqMethod, reqData.reqPath, strings.NewReader(reqData.reqData.Encode()))
							req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
							require.NoError(t, err)
						default:
							t.Fatalf("unsupported HTTP method %q", reqMethod)
						}

						for name, values := range testData.reqHeader {
							for _, value := range values {
								req.Header.Set(name, value)
							}
						}

						// Inject the tenant ID in the request.
						queryDetails, ctx := ContextWithEmptyDetails(context.Background())
						req = req.WithContext(user.InjectOrgID(ctx, userID))

						// Init the cache.
						cacheBackend := cache.NewInstrumentedMockCache()
						if testData.init != nil {
							testData.init(t, cacheBackend, reqData.cacheKey, reqData.hashedCacheKey)
						}
						initialStoreCallsCount := cacheBackend.CountStoreCalls()

						reg := prometheus.NewPedanticRegistry()
						rt := newRoundTripper(cacheBackend, DefaultCacheKeyGenerator{codec: NewPrometheusCodec(reg, 0*time.Minute, formatJSON, nil)}, limits, downstream, mimirtest.NewTestingLogger(t), reg)
						res, err := rt.RoundTrip(req)
						require.NoError(t, err)

						// Assert on the downstream.
						assert.Equal(t, testData.expectedDownstreamCalled, downstreamCalled)
						if testData.expectedDownstreamCalled {
							assert.Equal(t, reqData.reqData, downstreamReqParams)
						}

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
							assert.Equal(t, cached.Size(), queryDetails.ResultsCacheMissBytes)
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
			}
		})
	}
}
