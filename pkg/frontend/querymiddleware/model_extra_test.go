// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestEncodeAndDecodeCachedHTTPResponse(t *testing.T) {
	tests := map[string]struct {
		httpRes      *http.Response
		expectedBody []byte
	}{
		"HTTP response with 1 header value per name": {
			httpRes: &http.Response{
				StatusCode: 200,
				Header: http.Header{
					"first":  []string{"1"},
					"second": []string{"2"},
				},
				Body: io.NopCloser(strings.NewReader("hello world")),
			},
			expectedBody: []byte("hello world"),
		},
		"HTTP response with multiple header values per name": {
			httpRes: &http.Response{
				StatusCode: 200,
				Header: http.Header{
					"first":  []string{"1a", "1b"},
					"second": []string{"2a", "2b"},
				},
				Body: io.NopCloser(strings.NewReader("hello world")),
			},
			expectedBody: []byte("hello world"),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cachedRes, err := EncodeCachedHTTPResponse("test-cache-key", testData.httpRes)
			require.NoError(t, err)

			decodedRes := DecodeCachedHTTPResponse(cachedRes)

			assert.Equal(t, testData.httpRes.StatusCode, decodedRes.StatusCode)
			assert.Equal(t, testData.httpRes.Header, decodedRes.Header)
			assert.Equal(t, int64(len(testData.expectedBody)), decodedRes.ContentLength)

			// Read the response body from the decoded response.
			actualBody, err := io.ReadAll(decodedRes.Body)
			require.NoError(t, err)
			assert.Equal(t, testData.expectedBody, actualBody)

			// The response from the original response should be readable too.
			actualBody, err = io.ReadAll(testData.httpRes.Body)
			require.NoError(t, err)
			assert.Equal(t, testData.expectedBody, actualBody)
		})
	}
}

func TestMetricQueryRequestCloneHeaders(t *testing.T) {
	validateClonedHeaders := func(t *testing.T, cloned, original []*PrometheusHeader) {
		// Check header elements
		headersMap := make(map[string]string)
		for _, header := range cloned {
			headersMap[header.Name] = header.Values[0]
		}
		require.Equal(t, "test-value", headersMap["X-Test-Header"])
		require.Equal(t, "application/x-www-form-urlencoded", headersMap["Content-Type"])

		// Check that the elements are equal but not the same instances
		for i := range original {
			require.NotSame(t, cloned[i], original[i])

			require.Equalf(t, original[i].Name, cloned[i].Name, "expected element %d to have Name %s, got %s", i, original[i].Name, cloned[i].Name)

			require.True(t, slices.Equal(original[i].Values, cloned[i].Values), "expected values to be equal")
			require.NotSame(t, unsafe.SliceData(original[i].Values), unsafe.SliceData(cloned[i].Values), "expected values to be different instances")
		}
	}

	for _, asRangeQuery := range []bool{true, false} {
		t.Run("asRangeQuery="+strconv.FormatBool(asRangeQuery), func(t *testing.T) {
			var (
				urlPath string
				params  = url.Values{}
			)

			params.Add("query", "up")
			if asRangeQuery {
				params.Add("start", "0")
				params.Add("end", "1")
				params.Add("step", "1")

				urlPath = "/api/v1/query_range"
			} else {
				urlPath = "/api/v1/query"
			}

			httpReq, err := http.NewRequest(http.MethodPost, urlPath, strings.NewReader(params.Encode()))
			require.NoError(t, err)

			httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			httpReq.Header.Set("X-Test-Header", "test-value")

			c := NewPrometheusCodec(prometheus.NewPedanticRegistry(), time.Minute*5, "json")
			originalReq, err := c.DecodeMetricsQueryRequest(context.Background(), httpReq)
			require.NoError(t, err)

			t.Run("WithID", func(t *testing.T) {
				r, err := originalReq.WithID(1234)
				require.NoError(t, err)
				validateClonedHeaders(t, r.GetHeaders(), originalReq.GetHeaders())
			})
			t.Run("WithHeaders", func(t *testing.T) {
				newHeaders := []*PrometheusHeader{
					{Name: "Content-Type", Values: []string{"application/x-www-form-urlencoded"}},
					{Name: "X-Test-Header", Values: []string{"test-value"}},
				}

				r, err := originalReq.WithHeaders(newHeaders)
				require.NoError(t, err)
				validateClonedHeaders(t, r.GetHeaders(), newHeaders)
			})
			t.Run("WithStartEnd", func(t *testing.T) {
				r, err := originalReq.WithStartEnd(100, 200)
				require.NoError(t, err)

				validateClonedHeaders(t, r.GetHeaders(), originalReq.GetHeaders())
			})
			t.Run("WithQuery", func(t *testing.T) {
				r, err := originalReq.WithQuery("count")
				require.NoError(t, err)

				validateClonedHeaders(t, r.GetHeaders(), originalReq.GetHeaders())
			})
			t.Run("WithTotalQueriesHint", func(t *testing.T) {
				r, err := originalReq.WithTotalQueriesHint(10)
				require.NoError(t, err)
				validateClonedHeaders(t, r.GetHeaders(), originalReq.GetHeaders())
			})
			t.Run("WithExpr", func(t *testing.T) {
				r, err := originalReq.WithExpr(nil)
				require.NoError(t, err)
				validateClonedHeaders(t, r.GetHeaders(), originalReq.GetHeaders())
			})
			t.Run("WithEstimatedSeriesCountHint", func(t *testing.T) {
				r, err := originalReq.WithEstimatedSeriesCountHint(10)
				require.NoError(t, err)
				validateClonedHeaders(t, r.GetHeaders(), originalReq.GetHeaders())
			})
		})
	}
}
