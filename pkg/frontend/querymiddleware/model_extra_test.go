// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
