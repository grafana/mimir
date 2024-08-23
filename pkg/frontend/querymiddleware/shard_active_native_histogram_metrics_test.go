// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/go-kit/log"
	"github.com/golang/snappy"
	"github.com/grafana/dskit/user"
	"github.com/klauspost/compress/s2"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/storage/sharding"
)

func Test_shardActiveNativeHistogramMetricsMiddleware_RoundTrip(t *testing.T) {
	const tenantShardCount = 4
	const tenantMaxShardCount = 128

	validReq := func() *http.Request {
		r := httptest.NewRequest("POST", "/active_native_histogram_metrics", strings.NewReader(`selector={__name__=~".+"}`))
		r.Header.Add("X-Scope-OrgID", "test")
		r.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		return r
	}

	validReqWithShardHeader := func(shardCount int) func() *http.Request {
		return func() *http.Request {
			r := validReq()
			r.Header.Add(totalShardsControlHeader, strconv.Itoa(shardCount))
			return r
		}
	}

	noError := func(t *testing.T, err error) bool {
		return assert.NoError(t, err)
	}

	tests := map[string]struct {
		// Request parameters
		request func() *http.Request

		// Possible responses from upstream
		validResponses []cardinality.ActiveNativeHistogramMetricsResponse
		responseStatus int
		responseBody   string
		errorResponse  error

		// Error expectations
		checkResponseErr func(t *testing.T, err error) (continueTest bool)

		// Expected result
		expect                resultActiveNativeHistogramMetrics
		expectedShardCount    int
		expectContentEncoding string
	}{
		"no selector": {
			request: func() *http.Request {
				request := httptest.NewRequest("GET", "/active_native_histogram_metrics", nil)
				return request
			},
			checkResponseErr: func(t *testing.T, err error) (cont bool) {
				assert.True(t, apierror.IsNonRetryableAPIError(err))
				return false
			},
		},
		"invalid selector": {
			request: func() *http.Request {
				v := &url.Values{}
				v.Set("selector", "{invalid}")
				r := httptest.NewRequest("GET", "/active_native_histogram_metrics", nil)
				r.URL.RawQuery = v.Encode()
				return r
			},
			checkResponseErr: func(t *testing.T, err error) (cont bool) {
				assert.True(t, apierror.IsNonRetryableAPIError(err))
				return false
			},
		},
		"shard count bounded by limits": {
			request: validReqWithShardHeader(tenantMaxShardCount + 1),
			checkResponseErr: func(t *testing.T, err error) (continueTest bool) {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), fmt.Sprintf("shard count %d exceeds allowed maximum (%d)", tenantMaxShardCount+1, tenantMaxShardCount))
				return false
			},
		},
		"upstream response: invalid type for data field": {
			request:        validReq,
			responseStatus: http.StatusInternalServerError,
			responseBody:   `{"data": "unexpected"}`,

			checkResponseErr: func(t *testing.T, err error) (continueTest bool) {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "received unexpected response from upstream")
				return false
			},
			expectedShardCount: tenantShardCount,
			expect:             resultActiveNativeHistogramMetrics{Status: "error", Error: "expected data field to contain an array"},
		},
		"upstream response: no data field": {
			request:        validReq,
			responseStatus: http.StatusInternalServerError,
			responseBody:   `{unexpected: "response"}`,

			checkResponseErr: func(t *testing.T, err error) (continueTest bool) {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "received unexpected response from upstream")
				return false
			},
			expectedShardCount: tenantShardCount,
			expect:             resultActiveNativeHistogramMetrics{Status: "error", Error: "expected data field at top level"},
		},
		"upstream response: response too large": {
			request:        validReq,
			responseStatus: http.StatusRequestEntityTooLarge,
			responseBody:   "",

			checkResponseErr: func(t *testing.T, err error) (continueTest bool) {
				assert.Contains(t, err.Error(), errShardCountTooLow.Error())
				return false
			},
		},
		"upstream response: error": {
			request: validReq,

			errorResponse: errors.New("upstream error"),
			checkResponseErr: func(t *testing.T, err error) (continueTest bool) {
				assert.Error(t, err)
				return false
			},
		},
		"honours shard count from request header": {
			request: validReqWithShardHeader(3),

			validResponses: []cardinality.ActiveNativeHistogramMetricsResponse{
				{
					Data: []cardinality.ActiveMetricWithBucketCount{
						{
							Metric:         "metric_1",
							SeriesCount:    1,
							BucketCount:    5,
							AvgBucketCount: 5,
							MinBucketCount: 5,
							MaxBucketCount: 5,
						},
						{
							Metric:         "metric_2",
							SeriesCount:    1,
							BucketCount:    10,
							AvgBucketCount: 10,
							MinBucketCount: 10,
							MaxBucketCount: 10,
						},
					},
				},
				{
					Data: []cardinality.ActiveMetricWithBucketCount{
						{
							Metric:         "metric_1",
							SeriesCount:    1,
							BucketCount:    10,
							AvgBucketCount: 10,
							MinBucketCount: 10,
							MaxBucketCount: 10,
						},
					},
				},
				{
					Data: []cardinality.ActiveMetricWithBucketCount{
						{
							Metric:         "metric_3",
							SeriesCount:    10,
							BucketCount:    100,
							AvgBucketCount: 10,
							MinBucketCount: 10,
							MaxBucketCount: 10,
						},
					},
				},
			},

			checkResponseErr: noError,
			expect: resultActiveNativeHistogramMetrics{
				Data: []cardinality.ActiveMetricWithBucketCount{
					{
						Metric:         "metric_1",
						SeriesCount:    2,
						BucketCount:    15,
						AvgBucketCount: 7.5,
						MinBucketCount: 5,
						MaxBucketCount: 10,
					},
					{
						Metric:         "metric_2",
						SeriesCount:    1,
						BucketCount:    10,
						AvgBucketCount: 10,
						MinBucketCount: 10,
						MaxBucketCount: 10,
					},
					{
						Metric:         "metric_3",
						SeriesCount:    10,
						BucketCount:    100,
						AvgBucketCount: 10,
						MinBucketCount: 10,
						MaxBucketCount: 10,
					},
				},
			},
			expectedShardCount: 3,
		},
		"uses tenant's default shard count if none is specified in the request header": {
			request: validReq,
			validResponses: []cardinality.ActiveNativeHistogramMetricsResponse{
				{
					Data: []cardinality.ActiveMetricWithBucketCount{
						{
							Metric:         "metric_1",
							SeriesCount:    1,
							BucketCount:    5,
							AvgBucketCount: 5,
							MinBucketCount: 5,
							MaxBucketCount: 5,
						},
						{
							Metric:         "metric_2",
							SeriesCount:    1,
							BucketCount:    10,
							AvgBucketCount: 10,
							MinBucketCount: 10,
							MaxBucketCount: 10,
						},
					},
				},
				{
					Data: []cardinality.ActiveMetricWithBucketCount{
						{
							Metric:         "metric_1",
							SeriesCount:    1,
							BucketCount:    10,
							AvgBucketCount: 10,
							MinBucketCount: 10,
							MaxBucketCount: 10,
						},
					},
				},
				{
					Data: []cardinality.ActiveMetricWithBucketCount{
						{
							Metric:         "metric_3",
							SeriesCount:    5,
							BucketCount:    50,
							AvgBucketCount: 10,
							MinBucketCount: 10,
							MaxBucketCount: 10,
						},
					},
				},
				{
					Data: []cardinality.ActiveMetricWithBucketCount{
						{
							Metric:         "metric_3",
							SeriesCount:    5,
							BucketCount:    50,
							AvgBucketCount: 10,
							MinBucketCount: 10,
							MaxBucketCount: 10,
						},
					},
				},
			},
			checkResponseErr: noError,
			expect: resultActiveNativeHistogramMetrics{
				Data: []cardinality.ActiveMetricWithBucketCount{
					{
						Metric:         "metric_1",
						SeriesCount:    2,
						BucketCount:    15,
						AvgBucketCount: 7.5,
						MinBucketCount: 5,
						MaxBucketCount: 10,
					},
					{
						Metric:         "metric_2",
						SeriesCount:    1,
						BucketCount:    10,
						AvgBucketCount: 10,
						MinBucketCount: 10,
						MaxBucketCount: 10,
					},
					{
						Metric:         "metric_3",
						SeriesCount:    10,
						BucketCount:    100,
						AvgBucketCount: 10,
						MinBucketCount: 10,
						MaxBucketCount: 10,
					},
				},
			},
			expectedShardCount: tenantShardCount,
		},
		"no sharding, request passed through": {
			request: validReqWithShardHeader(1),

			validResponses: []cardinality.ActiveNativeHistogramMetricsResponse{
				{
					Data: []cardinality.ActiveMetricWithBucketCount{
						{
							Metric:         "metric_1",
							SeriesCount:    1,
							BucketCount:    5,
							AvgBucketCount: 5,
							MinBucketCount: 5,
							MaxBucketCount: 5,
						},
					},
				},
			},
			checkResponseErr: noError,
			expect: resultActiveNativeHistogramMetrics{
				Data: []cardinality.ActiveMetricWithBucketCount{
					{
						Metric:         "metric_1",
						SeriesCount:    1,
						BucketCount:    5,
						AvgBucketCount: 5,
						MinBucketCount: 5,
						MaxBucketCount: 5,
					},
				},
			},
		},
		"handles empty shards": {
			request: validReqWithShardHeader(6),
			validResponses: []cardinality.ActiveNativeHistogramMetricsResponse{
				{
					Data: []cardinality.ActiveMetricWithBucketCount{
						{
							Metric:         "metric_1",
							SeriesCount:    1,
							BucketCount:    5,
							AvgBucketCount: 5,
							MinBucketCount: 5,
							MaxBucketCount: 5,
						},
					},
				},
				{Data: []cardinality.ActiveMetricWithBucketCount{}},
				{Data: []cardinality.ActiveMetricWithBucketCount{}},
				{Data: []cardinality.ActiveMetricWithBucketCount{}},
				{Data: []cardinality.ActiveMetricWithBucketCount{}},
				{Data: []cardinality.ActiveMetricWithBucketCount{}},
			},
			checkResponseErr: noError,
			expect: resultActiveNativeHistogramMetrics{
				Data: []cardinality.ActiveMetricWithBucketCount{
					{
						Metric:         "metric_1",
						SeriesCount:    1,
						BucketCount:    5,
						AvgBucketCount: 5,
						MinBucketCount: 5,
						MaxBucketCount: 5,
					},
				},
			},
			expectedShardCount: 6,
		},
		"honours Accept-Encoding header": {
			request: func() *http.Request {
				r := validReq()
				r.Header.Add("Accept-Encoding", "snappy")
				r.Header.Add(totalShardsControlHeader, "2")
				return r
			},
			validResponses: []cardinality.ActiveNativeHistogramMetricsResponse{
				{
					Data: []cardinality.ActiveMetricWithBucketCount{
						{
							Metric:         "metric_1",
							SeriesCount:    1,
							BucketCount:    5,
							AvgBucketCount: 5,
							MinBucketCount: 5,
							MaxBucketCount: 5,
						},
						{
							Metric:         "metric_2",
							SeriesCount:    1,
							BucketCount:    10,
							AvgBucketCount: 10,
							MinBucketCount: 10,
							MaxBucketCount: 10,
						},
					},
				},
				{
					Data: []cardinality.ActiveMetricWithBucketCount{
						{
							Metric:         "metric_1",
							SeriesCount:    1,
							BucketCount:    10,
							AvgBucketCount: 10,
							MinBucketCount: 10,
							MaxBucketCount: 10,
						},
					},
				},
			},

			checkResponseErr: noError,
			expect: resultActiveNativeHistogramMetrics{
				Data: []cardinality.ActiveMetricWithBucketCount{
					{
						Metric:         "metric_1",
						SeriesCount:    2,
						BucketCount:    15,
						AvgBucketCount: 7.5,
						MinBucketCount: 5,
						MaxBucketCount: 10,
					},
					{
						Metric:         "metric_2",
						SeriesCount:    1,
						BucketCount:    10,
						AvgBucketCount: 10,
						MinBucketCount: 10,
						MaxBucketCount: 10,
					},
				},
			},
			expectedShardCount:    2,
			expectContentEncoding: "snappy",
		},
		"builds correct request shards for GET requests": {
			request: func() *http.Request {
				q := url.Values{}
				q.Set("selector", "{__name__=~\".+\"}")
				req, _ := http.NewRequest(http.MethodGet, "/active_native_histogram_metrics", nil)
				req.URL.RawQuery = q.Encode()
				req.Header.Add(totalShardsControlHeader, "2")
				return req
			},
			validResponses: []cardinality.ActiveNativeHistogramMetricsResponse{
				{
					Data: []cardinality.ActiveMetricWithBucketCount{
						{
							Metric:         "metric_1",
							SeriesCount:    1,
							BucketCount:    5,
							AvgBucketCount: 5,
							MinBucketCount: 5,
							MaxBucketCount: 5,
						},
						{
							Metric:         "metric_2",
							SeriesCount:    1,
							BucketCount:    10,
							AvgBucketCount: 10,
							MinBucketCount: 10,
							MaxBucketCount: 10,
						},
					},
				},
				{
					Data: []cardinality.ActiveMetricWithBucketCount{
						{
							Metric:         "metric_1",
							SeriesCount:    1,
							BucketCount:    10,
							AvgBucketCount: 10,
							MinBucketCount: 10,
							MaxBucketCount: 10,
						},
					},
				},
			},

			checkResponseErr: noError,
			expect: resultActiveNativeHistogramMetrics{
				Data: []cardinality.ActiveMetricWithBucketCount{
					{
						Metric:         "metric_1",
						SeriesCount:    2,
						BucketCount:    15,
						AvgBucketCount: 7.5,
						MinBucketCount: 5,
						MaxBucketCount: 10,
					},
					{
						Metric:         "metric_2",
						SeriesCount:    1,
						BucketCount:    10,
						AvgBucketCount: 10,
						MinBucketCount: 10,
						MaxBucketCount: 10,
					},
				},
			},
			expectedShardCount: 2,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			// Stub upstream with valid or invalid responses.
			var requestCount atomic.Int32
			upstream := RoundTripFunc(func(r *http.Request) (*http.Response, error) {
				_, _, err := user.ExtractOrgIDFromHTTPRequest(r)
				require.NoError(t, err)
				_, err = user.ExtractOrgID(r.Context())
				require.NoError(t, err)

				requestCount.Inc()

				if tt.errorResponse != nil {
					return nil, tt.errorResponse
				}

				if tt.validResponses == nil {
					return &http.Response{StatusCode: tt.responseStatus, Body: io.NopCloser(strings.NewReader(tt.responseBody))}, nil
				}

				require.NoError(t, r.ParseForm())

				req, err := cardinality.DecodeActiveSeriesRequestFromValues(r.Form)
				require.NoError(t, err)

				// Return requested shard.
				shard, _, err := sharding.ShardFromMatchers(req.Matchers)
				require.NoError(t, err)
				// If no shard is requested, return the first response element
				if shard == nil {
					shard = &sharding.ShardSelector{ShardIndex: 0}
				}
				require.NotNil(t, tt.validResponses)
				require.Greater(t, len(tt.validResponses), int(shard.ShardIndex))

				resp, err := json.Marshal(resultActiveNativeHistogramMetrics{Data: tt.validResponses[shard.ShardIndex].Data})
				require.NoError(t, err)

				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(resp))}, nil
			})

			// Run the request through the middleware.
			s := newShardActiveNativeHistogramMetricsMiddleware(
				upstream,
				mockLimits{maxShardedQueries: tenantMaxShardCount, totalShards: tenantShardCount},
				log.NewNopLogger(),
			)
			resp, err := s.RoundTrip(tt.request().WithContext(user.InjectOrgID(tt.request().Context(), "test")))
			if !tt.checkResponseErr(t, err) {
				return
			}

			assert.GreaterOrEqual(t, 200, resp.StatusCode)

			defer func(Body io.ReadCloser) {
				_ = Body.Close()
			}(resp.Body)

			var br io.Reader = resp.Body
			assert.Equal(t, tt.expectContentEncoding, resp.Header.Get("Content-Encoding"))
			body, err := io.ReadAll(br)
			assert.NoError(t, err)

			if resp.Header.Get("Content-Encoding") == "snappy" {
				body, err = snappy.Decode(nil, body)
				assert.NoError(t, err)
			}

			var res resultActiveNativeHistogramMetrics
			err = json.Unmarshal(body, &res)
			require.NoError(t, err)

			// Check that the response contains the expected data.
			assert.ElementsMatch(t, tt.expect.Data, res.Data)
			assert.Equal(t, tt.expect.Status, res.Status)
			assert.Contains(t, res.Error, tt.expect.Error)

			// Check that the request was split into the expected number of shards.
			var expectedCount int
			if tt.expectedShardCount > 0 {
				expectedCount = tt.expectedShardCount
			} else {
				expectedCount = len(tt.expect.Data)
			}
			assert.Equal(t, int32(expectedCount), requestCount.Load())
		})
	}
}

func TestShardActiveNativeHistogramMetricsMiddlewareRoundTripConcurrent(t *testing.T) {
	const shardCount = 4

	upstream := RoundTripFunc(func(r *http.Request) (*http.Response, error) {
		require.NoError(t, r.ParseForm())
		req, err := cardinality.DecodeActiveSeriesRequestFromValues(r.Form)
		require.NoError(t, err)
		shard, _, err := sharding.ShardFromMatchers(req.Matchers)
		require.NoError(t, err)
		require.NotNil(t, shard)

		resp, err := json.Marshal(resultActiveNativeHistogramMetrics{Data: []cardinality.ActiveMetricWithBucketCount{
			{
				Metric:         fmt.Sprintf("metric-%d", shard.ShardIndex),
				SeriesCount:    1,
				BucketCount:    5,
				AvgBucketCount: 5,
				MinBucketCount: 5,
				MaxBucketCount: 5,
			},
		}})
		require.NoError(t, err)

		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(resp))}, nil
	})

	s := newShardActiveNativeHistogramMetricsMiddleware(
		upstream,
		mockLimits{maxShardedQueries: shardCount, totalShards: shardCount},
		log.NewNopLogger(),
	)

	assertRoundTrip := func(t *testing.T, trip http.RoundTripper, req *http.Request) {
		resp, err := trip.RoundTrip(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var body io.Reader = resp.Body
		if resp.Header.Get("Content-Encoding") == encodingTypeSnappyFramed {
			body = s2.NewReader(resp.Body)
		}

		// For this test, if we can decode the response, it is enough to guaranty it worked. We proof actual validity
		// of all kinds of responses in the tests above.
		var res resultActiveNativeHistogramMetrics
		err = json.NewDecoder(body).Decode(&res)
		require.NoError(t, err)
		require.Len(t, res.Data, shardCount)
	}

	const reqCount = 20

	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(reqCount)

	for n := reqCount; n > 0; n-- {
		go func(n int) {
			defer wg.Done()

			req := httptest.NewRequest("POST", "/active_native_histogram_metrics", strings.NewReader(`selector={__name__=~"metric-.*"}`))
			req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

			// Send every other request as snappy to proof the middleware doesn't mess up body encoders
			if n%2 == 0 {
				req.Header.Add("Accept-Encoding", encodingTypeSnappyFramed)
			}

			req = req.WithContext(user.InjectOrgID(req.Context(), "test"))

			assertRoundTrip(t, s, req)
		}(n)
	}
}

func TestShardActiveNativeHistogramMetricsMiddlewareMergeResponseContextCancellation(t *testing.T) {
	s := newShardActiveNativeHistogramMetricsMiddleware(nil, mockLimits{}, log.NewNopLogger()).(*shardActiveNativeHistogramMetricsMiddleware)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(fmt.Errorf("test ran to completion"))

	rawResp := resultActiveNativeHistogramMetrics{}
	for i := 0; i < 10000; i++ { // Bump this number if the test is flaky.
		rawResp.Data = append(rawResp.Data, cardinality.ActiveMetricWithBucketCount{
			Metric:         fmt.Sprintf("metric-%d", i),
			SeriesCount:    1,
			BucketCount:    5,
			AvgBucketCount: 5,
			MinBucketCount: 5,
			MaxBucketCount: 5,
		})
	}

	body, err := json.Marshal(rawResp)
	require.NoError(t, err)

	responses := []*http.Response{
		{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body))},
		{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body))},
	}
	var resp *http.Response

	g := sync.WaitGroup{}
	g.Add(1)
	go func() {
		defer g.Done()
		resp = s.mergeResponses(ctx, responses, "")
	}()

	cancelCause := "request canceled while streaming response"
	cancel(errors.New(cancelCause))

	g.Wait()

	var buf bytes.Buffer
	_, err = io.Copy(&buf, resp.Body)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), cancelCause)
}

type resultActiveNativeHistogramMetrics struct {
	Data   []cardinality.ActiveMetricWithBucketCount `json:"data"`
	Status string                                    `json:"status,omitempty"`
	Error  string                                    `json:"error,omitempty"`
}
