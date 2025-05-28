// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apierror "github.com/grafana/mimir/pkg/api/error"
)

// TestPrometheusCodec_ResponseParsingEdgeCases tests various edge cases for response parsing
// to ensure the codec handles malformed, unexpected, or non-Prometheus responses gracefully.
func TestPrometheusCodec_ResponseParsingEdgeCases(t *testing.T) {
	for _, tc := range []struct {
		name            string
		contentType     string
		responseBody    string
		expectedError   string
		shouldParseData bool // whether we expect to get a valid PrometheusResponse with nil data
	}{
		{
			name:        "arbitrary JSON object",
			contentType: "application/json",
			responseBody: `{
				"someField": "someValue",
				"anotherField": 123,
				"nested": {
					"key": "value"
				}
			}`,
			expectedError:   "",
			shouldParseData: true, // Should parse but result in PrometheusResponse with Data=nil
		},
		{
			name:            "empty JSON object",
			contentType:     "application/json",
			responseBody:    `{}`,
			expectedError:   "",
			shouldParseData: true,
		},
		{
			name:          "JSON array instead of object",
			contentType:   "application/json",
			responseBody:  `["item1", "item2", "item3"]`,
			expectedError: "readObjectStart: expect { or n, but found [",
		},
		{
			name:          "JSON string instead of object",
			contentType:   "application/json",
			responseBody:  `"just a string"`,
			expectedError: "readObjectStart: expect { or n, but found \"",
		},
		{
			name:          "JSON number instead of object",
			contentType:   "application/json",
			responseBody:  `42`,
			expectedError: "readObjectStart: expect { or n, but found 4",
		},
		{
			name:          "malformed JSON - missing closing brace",
			contentType:   "application/json",
			responseBody:  `{"status": "success", "data": {"resultType": "matrix"`,
			expectedError: "object not ended with }",
		},
		{
			name:          "malformed JSON - trailing comma",
			contentType:   "application/json",
			responseBody:  `{"status": "success", "data": {},}`,
			expectedError: "unsupported value type",
		},
		{
			name:          "malformed JSON - unescaped quotes",
			contentType:   "application/json",
			responseBody:  `{"message": "this contains "unescaped" quotes"}`,
			expectedError: "expect }, but found u",
		},
		{
			name:        "valid JSON with null data field",
			contentType: "application/json",
			responseBody: `{
				"status": "success",
				"data": null
			}`,
			expectedError:   "",
			shouldParseData: true,
		},
		{
			name:        "HTTP error response with arbitrary JSON content",
			contentType: "application/json",
			responseBody: `{
				"message": "Network Authentication Required",
				"code": 511,
				"details": "Cluster validation failed"
			}`,
			expectedError:   "",
			shouldParseData: true,
		},
		{
			name:        "text/plain content type with JSON body",
			contentType: "text/plain",
			responseBody: `{
				"status": "success",
				"data": {
					"resultType": "matrix",
					"result": []
				}
			}`,
			expectedError: "unknown response content type 'text/plain'",
		},
		{
			name:        "text/html content type with HTML body",
			contentType: "text/html",
			responseBody: `<!DOCTYPE html>
			<html>
			<head><title>Error 500</title></head>
			<body><h1>Internal Server Error</h1></body>
			</html>`,
			expectedError: "unknown response content type 'text/html'",
		},
		{
			name:        "application/xml content type with XML body",
			contentType: "application/xml",
			responseBody: `<?xml version="1.0" encoding="UTF-8"?>
			<error>
				<code>500</code>
				<message>Internal Server Error</message>
			</error>`,
			expectedError: "unknown response content type 'application/xml'",
		},
		{
			name:          "text/plain content type with plain text body",
			contentType:   "text/plain",
			responseBody:  "Something went wrong. Please try again later.",
			expectedError: "unknown response content type 'text/plain'",
		},
		{
			name:          "empty content type with JSON body",
			contentType:   "",
			responseBody:  `{"status": "success", "data": null}`,
			expectedError: "unknown response content type ''",
		},
		{
			name:        "custom content type with JSON body",
			contentType: "application/vnd.api+json",
			responseBody: `{
				"jsonapi": {"version": "1.0"},
				"errors": [
					{
						"status": "500",
						"title": "Internal Server Error"
					}
				]
			}`,
			expectedError: "unknown response content type 'application/vnd.api+json'",
		},
		{
			name:        "content type with charset parameter",
			contentType: "application/json; charset=utf-8",
			responseBody: `{
				"status": "success",
				"data": null
			}`,
			expectedError: "unknown response content type 'application/json; charset=utf-8'",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			codec := NewPrometheusCodec(reg, 0*time.Minute, formatJSON, nil)

			responseBody := []byte(tc.responseBody)
			httpResponse := &http.Response{
				StatusCode:    200,
				Header:        http.Header{"Content-Type": []string{tc.contentType}},
				Body:          io.NopCloser(bytes.NewBuffer(responseBody)),
				ContentLength: int64(len(responseBody)),
			}

			decoded, err := codec.DecodeMetricsQueryResponse(context.Background(), httpResponse, nil, log.NewNopLogger())

			if tc.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedError)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, decoded)

			if tc.shouldParseData {
				// For arbitrary JSON, we expect to get a PrometheusResponse but with Data=nil
				promResp, ok := decoded.GetPrometheusResponse()
				require.True(t, ok)
				require.NotNil(t, promResp)

				// Test that ResponseToSamples handles this gracefully
				_, respErr := ResponseToSamples(promResp)
				require.Error(t, respErr)
				assert.Contains(t, respErr.Error(), "response data is nil")
			}
		})
	}
}

// TestPrometheusCodec_ResponseParsingLargePayloads tests handling of unusually large responses
func TestPrometheusCodec_ResponseParsingLargePayloads(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	codec := NewPrometheusCodec(reg, 0*time.Minute, formatJSON, nil)

	t.Run("very large arbitrary JSON", func(t *testing.T) {
		// Create a large JSON object that's not a valid Prometheus response
		var buf strings.Builder
		buf.WriteString(`{"largeData": [`)
		for i := 0; i < 10000; i++ {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(`{"index":`)
			buf.WriteString(strings.Repeat("1234567890", 10)) // 100 chars per item
			buf.WriteString("}")
		}
		buf.WriteString(`]}`)

		responseBody := []byte(buf.String())
		httpResponse := &http.Response{
			StatusCode:    200,
			Header:        http.Header{"Content-Type": []string{"application/json"}},
			Body:          io.NopCloser(bytes.NewBuffer(responseBody)),
			ContentLength: int64(len(responseBody)),
		}

		decoded, err := codec.DecodeMetricsQueryResponse(context.Background(), httpResponse, nil, log.NewNopLogger())
		require.NoError(t, err)
		require.NotNil(t, decoded)

		// Should parse but result in PrometheusResponse with Data=nil
		promResp, ok := decoded.GetPrometheusResponse()
		require.True(t, ok)
		require.NotNil(t, promResp)

		// ResponseToSamples should handle this gracefully
		_, respErr := ResponseToSamples(promResp)
		require.Error(t, respErr)
		assert.Contains(t, respErr.Error(), "response data is nil")
	})
}

// TestPrometheusCodec_ResponseParsingSpecialCharacters tests responses with special characters
func TestPrometheusCodec_ResponseParsingSpecialCharacters(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	codec := NewPrometheusCodec(reg, 0*time.Minute, formatJSON, nil)

	t.Run("unicode characters in arbitrary JSON", func(t *testing.T) {
		responseBody := []byte(`{
			"message": "こんにちは世界",
			"emoji": "🚀🔥💻",
			"special": "quotes: \"'` + "`" + `",
			"unicode_escape": "\u0048\u0065\u006C\u006C\u006F"
		}`)

		httpResponse := &http.Response{
			StatusCode:    200,
			Header:        http.Header{"Content-Type": []string{"application/json"}},
			Body:          io.NopCloser(bytes.NewBuffer(responseBody)),
			ContentLength: int64(len(responseBody)),
		}

		decoded, err := codec.DecodeMetricsQueryResponse(context.Background(), httpResponse, nil, log.NewNopLogger())
		require.NoError(t, err)
		require.NotNil(t, decoded)

		// Should parse but result in PrometheusResponse with Data=nil
		promResp, ok := decoded.GetPrometheusResponse()
		require.True(t, ok)
		require.NotNil(t, promResp)

		// ResponseToSamples should handle this gracefully
		_, respErr := ResponseToSamples(promResp)
		require.Error(t, respErr)
		assert.Contains(t, respErr.Error(), "response data is nil")
	})
}

// TestPrometheusCodec_ResponseParsingErrorStatusCodes tests handling of various HTTP error status codes
func TestPrometheusCodec_ResponseParsingErrorStatusCodes(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	codec := NewPrometheusCodec(reg, 0*time.Minute, formatJSON, nil)

	// Test error status codes WITHOUT content type (these should generate errors)
	for _, statusCode := range []int{500, 503, 429, 413} {
		t.Run(fmt.Sprintf("status_%d_no_content_type", statusCode), func(t *testing.T) {
			responseBody := []byte("Something went wrong")

			httpResponse := &http.Response{
				StatusCode:    statusCode,
				Header:        http.Header{}, // No content type
				Body:          io.NopCloser(bytes.NewBuffer(responseBody)),
				ContentLength: int64(len(responseBody)),
			}

			_, err := codec.DecodeMetricsQueryResponse(context.Background(), httpResponse, nil, log.NewNopLogger())
			require.Error(t, err)

			// Should be an API error with the corresponding status code
			require.True(t, apierror.IsAPIError(err))
			resp, ok := apierror.HTTPResponseFromError(err)
			require.True(t, ok)
			require.Equal(t, int32(statusCode), resp.Code)
		})
	}

	// Test error status codes WITH application/json content type (these get parsed as JSON)
	for _, statusCode := range []int{500, 503} {
		t.Run(fmt.Sprintf("status_%d_with_json_content_type", statusCode), func(t *testing.T) {
			responseBody := []byte(`{
				"message": "This is arbitrary JSON with error status",
				"code": ` + fmt.Sprintf("%d", statusCode) + `
			}`)

			httpResponse := &http.Response{
				StatusCode:    statusCode,
				Header:        http.Header{"Content-Type": []string{"application/json"}},
				Body:          io.NopCloser(bytes.NewBuffer(responseBody)),
				ContentLength: int64(len(responseBody)),
			}

			// With content type, it tries to parse as JSON regardless of status code
			decoded, err := codec.DecodeMetricsQueryResponse(context.Background(), httpResponse, nil, log.NewNopLogger())
			require.NoError(t, err)
			require.NotNil(t, decoded)

			// Should parse but result in PrometheusResponse with Data=nil
			promResp, ok := decoded.GetPrometheusResponse()
			require.True(t, ok)
			require.NotNil(t, promResp)

			// ResponseToSamples should handle this gracefully
			_, respErr := ResponseToSamples(promResp)
			require.Error(t, respErr)
			assert.Contains(t, respErr.Error(), "response data is nil")
		})
	}

	// Test that 200 status with arbitrary JSON doesn't generate an error
	t.Run("status_200_with_arbitrary_json", func(t *testing.T) {
		responseBody := []byte(`{
			"message": "This is not a Prometheus response",
			"someField": "someValue"
		}`)

		httpResponse := &http.Response{
			StatusCode:    200,
			Header:        http.Header{"Content-Type": []string{"application/json"}},
			Body:          io.NopCloser(bytes.NewBuffer(responseBody)),
			ContentLength: int64(len(responseBody)),
		}

		decoded, err := codec.DecodeMetricsQueryResponse(context.Background(), httpResponse, nil, log.NewNopLogger())
		require.NoError(t, err)
		require.NotNil(t, decoded)

		// Should parse but result in PrometheusResponse with Data=nil
		promResp, ok := decoded.GetPrometheusResponse()
		require.True(t, ok)
		require.NotNil(t, promResp)

		// ResponseToSamples should handle this gracefully
		_, respErr := ResponseToSamples(promResp)
		require.Error(t, respErr)
		assert.Contains(t, respErr.Error(), "response data is nil")
	})
}
