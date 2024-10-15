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
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/klauspost/compress/s2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/storage/sharding"
)

func Test_shardActiveSeriesMiddleware_RoundTrip(t *testing.T) {
	for _, useZeroAllocationDecoder := range []bool{false, true} {
		t.Run(fmt.Sprintf("useZeroAllocationDecoder=%t", useZeroAllocationDecoder), func(t *testing.T) {
			runTestShardActiveSeriesMiddlewareRoundTrip(t, useZeroAllocationDecoder)
		})
	}
}

func runTestShardActiveSeriesMiddlewareRoundTrip(t *testing.T, useZeroAllocationDecoder bool) {
	const tenantShardCount = 4
	const tenantMaxShardCount = 128

	validReq := func() *http.Request {
		r := httptest.NewRequest("POST", "/active_series", strings.NewReader(`selector={__name__="metric"}`))
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

	tests := []struct {
		// Request parameters
		name    string
		request func() *http.Request

		// Possible responses from upstream
		validResponses [][]labels.Labels
		responseStatus int
		responseBody   string
		errorResponse  error

		// Error expectations
		checkResponseErr func(t *testing.T, err error) (continueTest bool)

		// Expected result
		expect                result
		expectedShardCount    int
		expectContentEncoding string
	}{
		{
			name: "no selector",

			request: func() *http.Request {
				request := httptest.NewRequest("GET", "/active_series", nil)
				return request
			},
			checkResponseErr: func(t *testing.T, err error) (cont bool) {
				assert.True(t, apierror.IsNonRetryableAPIError(err))
				return false
			},
		},
		{
			name: "invalid selector",

			request: func() *http.Request {
				v := &url.Values{}
				v.Set("selector", "{invalid}")
				r := httptest.NewRequest("GET", "/active_series", nil)
				r.URL.RawQuery = v.Encode()
				return r
			},
			checkResponseErr: func(t *testing.T, err error) (cont bool) {
				assert.True(t, apierror.IsNonRetryableAPIError(err))
				return false
			},
		},
		{
			name:    "shard count bounded by limits",
			request: validReqWithShardHeader(tenantMaxShardCount + 1),
			checkResponseErr: func(t *testing.T, err error) (continueTest bool) {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), fmt.Sprintf("shard count %d exceeds allowed maximum (%d)", tenantMaxShardCount+1, tenantMaxShardCount))
				return false
			},
		},
		{
			name:           "upstream response: invalid type for data field",
			request:        validReq,
			responseStatus: http.StatusOK,
			responseBody:   `{"data": "unexpected"}`,

			// We don't expect an error here because it only occurs later as the response is
			// being streamed.
			checkResponseErr: func(t *testing.T, err error) (continueTest bool) {
				return assert.NoError(t, err)
			},
			expectedShardCount: tenantShardCount,
			expect:             result{Status: "error", Error: "expected data field to contain an array"},
		},
		{
			name:           "upstream response: no data field",
			request:        validReq,
			responseStatus: http.StatusOK,
			responseBody:   `{unexpected: "response"}`,

			// We don't expect an error here because it only occurs later as the response is
			// being streamed.
			checkResponseErr: func(t *testing.T, err error) (continueTest bool) {
				return assert.NoError(t, err)
			},
			expectedShardCount: tenantShardCount,
			expect:             result{Status: "error", Error: "expected data field at top level"},
		},
		{
			name:           "upstream response: response too large",
			request:        validReq,
			responseStatus: http.StatusRequestEntityTooLarge,
			responseBody:   "",

			checkResponseErr: func(t *testing.T, err error) (continueTest bool) {
				assert.Contains(t, err.Error(), errShardCountTooLow.Error())
				resp, ok := apierror.HTTPResponseFromError(err)
				require.True(t, ok)
				assert.Equal(t, int(resp.Code), http.StatusRequestEntityTooLarge)
				return false
			},
		},
		{
			name:    "upstream response: error",
			request: validReq,

			errorResponse: errors.New("upstream error"),
			checkResponseErr: func(t *testing.T, err error) (continueTest bool) {
				assert.Error(t, err)
				return false
			},
		},
		{
			name:    "honours shard count from request header",
			request: validReqWithShardHeader(3),

			validResponses: [][]labels.Labels{
				{labels.FromStrings(labels.MetricName, "metric", "shard", "1")},
				{labels.FromStrings(labels.MetricName, "metric", "shard", "2")},
				{labels.FromStrings(labels.MetricName, "metric", "shard", "3")},
			},
			checkResponseErr: noError,
			expect: result{
				Data: []labels.Labels{
					labels.FromStrings(labels.MetricName, "metric", "shard", "1"),
					labels.FromStrings(labels.MetricName, "metric", "shard", "2"),
					labels.FromStrings(labels.MetricName, "metric", "shard", "3"),
				},
			},
		},
		{
			name:    "uses tenant's default shard count if none is specified in the request header",
			request: validReq,
			validResponses: [][]labels.Labels{
				{labels.FromStrings(labels.MetricName, "metric", "shard", "1")},
				{labels.FromStrings(labels.MetricName, "metric", "shard", "2")},
				{labels.FromStrings(labels.MetricName, "metric", "shard", "3")},
				{labels.FromStrings(labels.MetricName, "metric", "shard", "4")},
			},
			checkResponseErr: noError,
			expect: result{
				Data: []labels.Labels{
					labels.FromStrings(labels.MetricName, "metric", "shard", "1"),
					labels.FromStrings(labels.MetricName, "metric", "shard", "2"),
					labels.FromStrings(labels.MetricName, "metric", "shard", "3"),
					labels.FromStrings(labels.MetricName, "metric", "shard", "4"),
				},
			},
			expectedShardCount: tenantShardCount,
		},
		{
			name:    "no sharding, request passed through",
			request: validReqWithShardHeader(1),

			validResponses:   [][]labels.Labels{{labels.FromStrings(labels.MetricName, "metric")}},
			checkResponseErr: noError,
			expect:           result{Data: []labels.Labels{labels.FromStrings(labels.MetricName, "metric")}},
		},
		{
			name:    "handles empty shards",
			request: validReqWithShardHeader(6),
			validResponses: [][]labels.Labels{
				{labels.FromStrings(labels.MetricName, "metric", "shard", "1")},
				{},
				{},
				{},
				{},
				{},
			},
			checkResponseErr: noError,
			expect: result{
				Data: []labels.Labels{
					labels.FromStrings(labels.MetricName, "metric", "shard", "1"),
				},
			},
			expectedShardCount: 6,
		},
		{
			name: "honours Accept-Encoding header",
			request: func() *http.Request {
				r := validReq()
				r.Header.Add("Accept-Encoding", encodingTypeSnappyFramed)
				r.Header.Add(totalShardsControlHeader, "2")
				return r
			},
			validResponses: [][]labels.Labels{
				{labels.FromStrings(labels.MetricName, "metric", "shard", "1")},
				{labels.FromStrings(labels.MetricName, "metric", "shard", "2")},
			},
			checkResponseErr: noError,
			expect: result{
				Data: []labels.Labels{
					labels.FromStrings(labels.MetricName, "metric", "shard", "1"),
					labels.FromStrings(labels.MetricName, "metric", "shard", "2"),
				},
			},
			expectContentEncoding: encodingTypeSnappyFramed,
		},
		{
			name: "builds correct request shards for GET requests",
			request: func() *http.Request {
				q := url.Values{}
				q.Set("selector", "metric")
				req, _ := http.NewRequest(http.MethodGet, "/active_series", nil)
				req.URL.RawQuery = q.Encode()
				req.Header.Add(totalShardsControlHeader, "2")
				return req
			},
			validResponses: [][]labels.Labels{
				{labels.FromStrings(labels.MetricName, "metric", "shard", "1")},
				{labels.FromStrings(labels.MetricName, "metric", "shard", "2")},
			},
			checkResponseErr: noError,
			expect: result{
				Data: []labels.Labels{
					labels.FromStrings(labels.MetricName, "metric", "shard", "1"),
					labels.FromStrings(labels.MetricName, "metric", "shard", "2"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

				resp, err := json.Marshal(result{Data: tt.validResponses[shard.ShardIndex]})
				require.NoError(t, err)

				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(resp))}, nil
			})

			// Run the request through the middleware.
			s := newShardActiveSeriesMiddleware(
				upstream,
				useZeroAllocationDecoder,
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
			if resp.Header.Get("Content-Encoding") == encodingTypeSnappyFramed {
				br = s2.NewReader(br)
			}
			body, err := io.ReadAll(br)
			assert.NoError(t, err)

			var res result
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

func Test_shardActiveSeriesMiddleware_RoundTrip_concurrent(t *testing.T) {
	for _, useZeroAllocationDecoder := range []bool{false, true} {
		t.Run(fmt.Sprintf("useZeroAllocationDecoder=%t", useZeroAllocationDecoder), func(t *testing.T) {
			runTestShardActiveSeriesMiddlewareRoundTripConcurrent(t, useZeroAllocationDecoder)
		})
	}
}

func runTestShardActiveSeriesMiddlewareRoundTripConcurrent(t *testing.T, useZeroAllocationDecoder bool) {
	const shardCount = 4

	upstream := RoundTripFunc(func(r *http.Request) (*http.Response, error) {
		require.NoError(t, r.ParseForm())
		req, err := cardinality.DecodeActiveSeriesRequestFromValues(r.Form)
		require.NoError(t, err)
		shard, _, err := sharding.ShardFromMatchers(req.Matchers)
		require.NoError(t, err)
		require.NotNil(t, shard)

		resp := fmt.Sprintf(`{"data": [{"__name__": "metric-%d"}]}`, shard.ShardIndex)

		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(resp))}, nil
	})

	s := newShardActiveSeriesMiddleware(
		upstream,
		useZeroAllocationDecoder,
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
		var res result
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

			req := httptest.NewRequest("POST", "/active_series", strings.NewReader(`selector={__name__=~"metric-.*"}`))
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

func Test_shardActiveSeriesMiddleware_mergeResponse_contextCancellation(t *testing.T) {
	for _, useZeroAllocationDecoder := range []bool{false, true} {
		t.Run(fmt.Sprintf("useZeroAllocationDecoder=%t", useZeroAllocationDecoder), func(t *testing.T) {
			runTestShardActiveSeriesMiddlewareMergeResponseContextCancellation(t, useZeroAllocationDecoder)
		})
	}
}

func runTestShardActiveSeriesMiddlewareMergeResponseContextCancellation(t *testing.T, useZeroAllocationDecoder bool) {
	s := newShardActiveSeriesMiddleware(nil, true, mockLimits{}, log.NewNopLogger()).(*shardActiveSeriesMiddleware)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(fmt.Errorf("test ran to completion"))

	body, err := json.Marshal(&activeSeriesResponse{Data: []labels.Labels{
		// Make this large enough to ensure the whole response isn't buffered.
		labels.FromStrings("lbl1", strings.Repeat("a", os.Getpagesize())),
		labels.FromStrings("lbl2", "val2"),
		labels.FromStrings("lbl3", "val3"),
	}})
	require.NoError(t, err)

	responses := []*http.Response{
		{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body))},
		{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body))},
	}
	var resp *http.Response

	if useZeroAllocationDecoder {
		resp = s.mergeResponsesWithZeroAllocationDecoder(ctx, responses, "")
	} else {
		defer func() {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}()

		resp = s.mergeResponses(ctx, responses, "")
	}

	var buf bytes.Buffer
	_, err = io.CopyN(&buf, resp.Body, int64(os.Getpagesize()))
	require.NoError(t, err)

	cancelCause := "request canceled while streaming response"
	cancel(errors.New(cancelCause))

	_, err = io.Copy(&buf, resp.Body)
	require.NoError(t, err)
	assert.Contains(t, buf.String(), cancelCause)
}

func BenchmarkActiveSeriesMiddlewareMergeResponses(b *testing.B) {
	b.Run("encoding=none", func(b *testing.B) {
		benchmarkActiveSeriesMiddlewareMergeResponses(b, "", 1)
	})

	b.Run("encoding=snappy", func(b *testing.B) {
		benchmarkActiveSeriesMiddlewareMergeResponses(b, encodingTypeSnappyFramed, 1)
	})

	b.Run("seriesCount=1_000", func(b *testing.B) {
		benchmarkActiveSeriesMiddlewareMergeResponses(b, "", 1_000)
	})

	b.Run("seriesCount=10_000", func(b *testing.B) {
		benchmarkActiveSeriesMiddlewareMergeResponses(b, "", 10_000)
	})
}

type activeSeriesResponse struct {
	Data []labels.Labels `json:"data"`
}

func benchmarkActiveSeriesMiddlewareMergeResponses(b *testing.B, encoding string, numSeries int) {

	bcs := []int{4, 16, 64, 128}

	for _, numResponses := range bcs {
		b.Run(fmt.Sprintf("num-responses-%d", numResponses), func(b *testing.B) {
			benchResponses := make([][]*http.Response, b.N)

			for i := 0; i < b.N; i++ {
				var responses []*http.Response
				for i := 0; i < numResponses; i++ {

					var apiResp activeSeriesResponse
					for k := 0; k < numSeries; k++ {
						apiResp.Data = append(apiResp.Data, labels.FromStrings(
							"__name__", "m_"+fmt.Sprint(i),
							"job", "prometheus"+fmt.Sprint(i),
							"instance", "instance"+fmt.Sprint(i),
							"series", fmt.Sprintf("series_%d", k),
						))
					}
					body, _ := json.Marshal(&apiResp)

					responses = append(responses, &http.Response{
						StatusCode: http.StatusOK,
						Header:     http.Header{},
						Body:       io.NopCloser(bytes.NewReader(body)),
					})
				}
				benchResponses[i] = responses
			}

			s := newShardActiveSeriesMiddleware(nil, true, mockLimits{}, log.NewNopLogger()).(*shardActiveSeriesMiddleware)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				resp := s.mergeResponsesWithZeroAllocationDecoder(context.Background(), benchResponses[i], encoding)

				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		})
	}
}

type result struct {
	Data   []labels.Labels `json:"data"`
	Status string          `json:"status,omitempty"`
	Error  string          `json:"error,omitempty"`
}
