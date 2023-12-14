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
		validResponses  [][]labels.Labels
		invalidResponse []byte
		errorResponse   error

		// Error expectations
		checkResponseErr func(t *testing.T, err error) (continueTest bool)

		// Expected result
		expect             result
		expectedShardCount int
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
			name:            "upstream response: invalid type for data field",
			invalidResponse: []byte(`{"data": "unexpected"}`),
			request:         validReq,

			// We don't expect an error here because it only occurs later as the response is
			// being streamed.
			checkResponseErr: func(t *testing.T, err error) (continueTest bool) {
				return assert.NoError(t, err)
			},
			expectedShardCount: tenantShardCount,
			expect:             result{Status: "error", Error: "expected data field to contain an array"},
		},
		{
			name:            "upstream response: no data field",
			invalidResponse: []byte(`{"unexpected": "response"}`),
			request:         validReq,

			// We don't expect an error here because it only occurs later as the response is
			// being streamed.
			checkResponseErr: func(t *testing.T, err error) (continueTest bool) {
				return assert.NoError(t, err)
			},
			expectedShardCount: tenantShardCount,
			expect:             result{Status: "error", Error: "expected data field at top level"},
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// Stub upstream with valid or invalid responses.
			var requestCount atomic.Int32
			upstream := RoundTripFunc(func(r *http.Request) (*http.Response, error) {
				defer func(Body io.ReadCloser) {
					_ = Body.Close()
				}(r.Body)

				requestCount.Inc()

				if tt.errorResponse != nil {
					return nil, tt.errorResponse
				}

				if len(tt.invalidResponse) > 0 {
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(tt.invalidResponse))}, nil
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

			body, err := io.ReadAll(resp.Body)
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

type result struct {
	Data   []labels.Labels `json:"data"`
	Status string          `json:"status,omitempty"`
	Error  string          `json:"error,omitempty"`
}
