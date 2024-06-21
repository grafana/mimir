// SPDX-License-Identifier: AGPL-3.0-only

package compat

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEngineFallbackInjector(t *testing.T) {
	testCases := map[string]struct {
		headers http.Header

		expectFallback bool
		expectedError  string
	}{
		"no headers": {
			headers:        http.Header{},
			expectFallback: false,
		},
		"unrelated header": {
			headers: http.Header{
				"Content-Type": []string{"application/blah"},
			},
			expectFallback: false,
		},
		"force fallback header is present, but does not have expected value": {
			headers: http.Header{
				"X-Mimir-Force-Prometheus-Engine": []string{"blah"},
			},
			expectedError: "invalid value 'blah' for 'X-Mimir-Force-Prometheus-Engine' header, must be exactly 'true' or not set",
		},
		"force fallback header is present, and does have expected value": {
			headers: http.Header{
				"X-Mimir-Force-Prometheus-Engine": []string{"true"},
			},
			expectFallback: true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			injector := EngineFallbackInjector{}
			handlerCalled := false
			handler := injector.Wrap(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				handlerCalled = true
				require.Equal(t, testCase.expectFallback, isForceFallbackEnabled(req.Context()))
				w.WriteHeader(http.StatusOK)
			}))

			req, err := http.NewRequest(http.MethodGet, "/blah", nil)
			require.NoError(t, err)
			req.Header = testCase.headers

			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)

			if testCase.expectedError == "" {
				require.True(t, handlerCalled)
				require.Equal(t, http.StatusOK, resp.Code)
			} else {
				require.False(t, handlerCalled)
				require.Equal(t, http.StatusBadRequest, resp.Code)
				require.Equal(t, "application/json", resp.Header().Get("Content-Type"))

				body := resp.Body.String()
				expectedBody := `{"status": "error", "errorType": "bad_data", "error": "` + testCase.expectedError + `"}`
				require.JSONEq(t, expectedBody, body)
			}
		})
	}
}
