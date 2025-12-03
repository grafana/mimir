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

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apierror "github.com/grafana/mimir/pkg/api/error"
)

// TestCodec_ResponseParsingEdgeCases tests various edge cases for response parsing
// to ensure the codec handles malformed, unexpected, or non-Prometheus responses gracefully.
func TestCodec_ResponseParsingEdgeCases(t *testing.T) {
	for _, tc := range []struct {
		name            string
		contentType     string
		responseBody    string
		statusCodes     []int // test with multiple status codes
		expectedError   string
		shouldParseData bool // whether we expect to get a valid PrometheusResponse with nil data
	}{
		{
			name:        "arbitrary JSON object",
			contentType: "application/json",
			statusCodes: []int{200, 500, 503},
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
			statusCodes:     []int{200, 404, 500},
			responseBody:    `{}`,
			expectedError:   "",
			shouldParseData: true,
		},
		{
			name:          "JSON array instead of object",
			contentType:   "application/json",
			statusCodes:   []int{200, 400, 500},
			responseBody:  `["item1", "item2", "item3"]`,
			expectedError: "readObjectStart: expect { or n, but found [",
		},
		{
			name:          "JSON string instead of object",
			contentType:   "application/json",
			statusCodes:   []int{200, 500},
			responseBody:  `"just a string"`,
			expectedError: "readObjectStart: expect { or n, but found \"",
		},
		{
			name:          "JSON number instead of object",
			contentType:   "application/json",
			statusCodes:   []int{200, 503},
			responseBody:  `42`,
			expectedError: "readObjectStart: expect { or n, but found 4",
		},
		{
			name:          "malformed JSON - missing closing brace",
			contentType:   "application/json",
			statusCodes:   []int{200, 500, 503},
			responseBody:  `{"status": "success", "data": {"resultType": "matrix"`,
			expectedError: "expect }, but found",
		},
		{
			name:          "malformed JSON - trailing comma",
			contentType:   "application/json",
			statusCodes:   []int{200},
			responseBody:  `{"status": "success", "data": {},}`,
			expectedError: "unsupported value type",
		},
		{
			name:          "malformed JSON - unescaped quotes",
			contentType:   "application/json",
			statusCodes:   []int{200, 500},
			responseBody:  `{"message": "this contains "unescaped" quotes"}`,
			expectedError: "expect }, but found u",
		},
		{
			name:        "valid JSON with null data field",
			contentType: "application/json",
			statusCodes: []int{200, 511},
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
			statusCodes: []int{200, 511, 503, 500},
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
			statusCodes: []int{200, 500, 503},
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
			statusCodes: []int{200, 500},
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
			statusCodes: []int{200, 500, 503},
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
			statusCodes:   []int{200, 500, 503, 511},
			responseBody:  "Something went wrong. Please try again later.",
			expectedError: "unknown response content type 'text/plain'",
		},
		{
			name:          "empty content type with JSON body",
			contentType:   "",
			statusCodes:   []int{200, 500, 503, 429, 413},
			responseBody:  `{"status": "success", "data": null}`,
			expectedError: "unknown response content type ''",
		},
		{
			name:        "custom content type with JSON body",
			contentType: "application/vnd.api+json",
			statusCodes: []int{200, 500},
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
			statusCodes: []int{200, 500, 503},
			responseBody: `{
				"status": "success",
				"data": null
			}`,
			expectedError: "unknown response content type 'application/json; charset=utf-8'",
		},
		{
			name:        "unicode characters in arbitrary JSON",
			contentType: "application/json",
			statusCodes: []int{200, 500, 503},
			responseBody: func() string {
				return `{
					"message": "こんにちは世界",
					"emoji": "🚀🔥💻",
					"special": "quotes: \"'` + "`" + `",
					"unicode_escape": "\u0048\u0065\u006C\u006C\u006F",
					"nested": {
						"key": "value with ñáñá and 中文"
					}
				}`
			}(),
			expectedError:   "",
			shouldParseData: true,
		},
		{
			name:        "very large arbitrary JSON",
			contentType: "application/json",
			statusCodes: []int{200, 500},
			responseBody: func() string {
				var buf strings.Builder
				buf.WriteString(`{"largeData": [`)
				for i := 0; i < 10000; i++ {
					if i > 0 {
						buf.WriteString(",")
					}
					buf.WriteString(`{"index":`)
					buf.WriteString(fmt.Sprintf("%d", i))
					buf.WriteString(`,"data":"`)
					buf.WriteString(strings.Repeat("1234567890", 10)) // 100 chars per item
					buf.WriteString(`"}`)
				}
				buf.WriteString(`]}`)
				return buf.String()
			}(),
			expectedError:   "",
			shouldParseData: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, statusCode := range tc.statusCodes {
				t.Run(fmt.Sprintf("status_%d", statusCode), func(t *testing.T) {
					codec := newTestCodec()

					responseBody := []byte(tc.responseBody)
					headers := http.Header{}
					if tc.contentType != "" {
						headers["Content-Type"] = []string{tc.contentType}
					}

					httpResponse := &http.Response{
						StatusCode:    statusCode,
						Header:        headers,
						Body:          io.NopCloser(bytes.NewBuffer(responseBody)),
						ContentLength: int64(len(responseBody)),
					}

					decoded, err := codec.DecodeMetricsQueryResponse(context.Background(), httpResponse, nil, log.NewNopLogger())

					// For empty content type with error status codes, expect specific error handling
					if tc.contentType == "" && (statusCode == 500 || statusCode == 503 || statusCode == 429 || statusCode == 413) {
						require.Error(t, err)
						require.True(t, apierror.IsAPIError(err))
						resp, ok := apierror.HTTPResponseFromError(err)
						require.True(t, ok)
						require.Equal(t, int32(statusCode), resp.Code)
						return
					}

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
		})
	}
}
