// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
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
	const tenantShardCount = 4
	const tenantMaxShardCount = 128

	validReq := func() *http.Request {
		r := httptest.NewRequest("POST", "/active_series", strings.NewReader(`selector={__name__="metric"}`))
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
				defer func(body io.ReadCloser) {
					if body != nil {
						_ = body.Close()
					}
				}(r.Body)

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

func Test_shardActiveSeriesMiddleware_RoundTrip_ResponseBodyStreamed(t *testing.T) {
	// This value needs to be set at least as large as the buffer size used by the
	// implementation for this test to make sense.
	const bufferSize = 512
	const shardCount = 2

	// Stub upstream with two responses that are larger than the buffer size and
	// retain a reference to the response bodies. The responses use a custom body
	// type that counts the number of bytes read, so we can assert on that later in
	// the test.
	var upstreamResponseBodies [shardCount]*bodyReadBytesCounter
	var responseSize [shardCount]int
	upstream := RoundTripFunc(func(r *http.Request) (*http.Response, error) {
		// Extract requested shard index
		require.NoError(t, r.ParseForm())
		req, err := cardinality.DecodeActiveSeriesRequestFromValues(r.Form)
		require.NoError(t, err)
		shard, _, err := sharding.ShardFromMatchers(req.Matchers)
		require.NoError(t, err)
		require.NotNil(t, shard, "this test requires a shard to be requested")

		// Make sure the response body is big enough to not be buffered entirely.
		response := fmt.Sprintf(fmt.Sprintf(`{"data": [{"__name__": "metric-%%0%dd"}]}`, bufferSize), shard.ShardIndex)
		body := &bodyReadBytesCounter{body: io.NopCloser(strings.NewReader(response))}
		upstreamResponseBodies[shard.ShardIndex] = body
		responseSize[shard.ShardIndex] = len(response)

		return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
	})

	s := newShardActiveSeriesMiddleware(
		upstream,
		mockLimits{maxShardedQueries: shardCount, totalShards: shardCount},
		log.NewNopLogger(),
	)

	r := httptest.NewRequest("POST", "/active_series", strings.NewReader(`selector={__name__=~"metric-.*"}`))
	r.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := s.RoundTrip(r.WithContext(user.InjectOrgID(r.Context(), "test")))
	require.NoError(t, err)
	defer func(body io.ReadCloser) {
		_, _ = io.ReadAll(body)
		_ = body.Close()
	}(resp.Body)

	// Check that upstream responses have been read only up to a max of the buffer size.
	for _, body := range upstreamResponseBodies {
		bytesRead := int(body.BytesRead())
		require.GreaterOrEqual(t, bufferSize, bytesRead)
	}

	// Read and close the response body.
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	// Check that upstream responses have been fully read now.
	for i, body := range upstreamResponseBodies {
		bytesRead := int(body.BytesRead())
		assert.Equal(t, responseSize[i], bytesRead)
	}
}

// bodyReadBytesCounter is a wrapper around a response body that counts the number of bytes read from it.
type bodyReadBytesCounter struct {
	body      io.ReadCloser
	bytesRead atomic.Uint64
}

func (b *bodyReadBytesCounter) Read(p []byte) (n int, err error) {
	read, err := b.body.Read(p)
	b.bytesRead.Add(uint64(read))
	return read, err
}

func (b *bodyReadBytesCounter) Close() error {
	return b.body.Close()
}

func (b *bodyReadBytesCounter) BytesRead() uint64 {
	return b.bytesRead.Load()
}

type result struct {
	Data   []labels.Labels `json:"data"`
	Status string          `json:"status,omitempty"`
	Error  string          `json:"error,omitempty"`
}
