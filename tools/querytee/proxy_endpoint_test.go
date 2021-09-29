// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/tools/querytee/proxy_endpoint_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querytee

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ProxyEndpoint_waitBackendResponseForDownstream(t *testing.T) {
	backendURL1, err := url.Parse("http://backend-1/")
	require.NoError(t, err)
	backendURL2, err := url.Parse("http://backend-2/")
	require.NoError(t, err)
	backendURL3, err := url.Parse("http://backend-3/")
	require.NoError(t, err)

	backendPref := NewProxyBackend("backend-1", backendURL1, time.Second, true)
	backendOther1 := NewProxyBackend("backend-2", backendURL2, time.Second, false)
	backendOther2 := NewProxyBackend("backend-3", backendURL3, time.Second, false)

	tests := map[string]struct {
		backends  []*ProxyBackend
		responses []*backendResponse
		expected  *ProxyBackend
	}{
		"the preferred backend is the 1st response received": {
			backends: []*ProxyBackend{backendPref, backendOther1},
			responses: []*backendResponse{
				{backend: backendPref, status: 200},
			},
			expected: backendPref,
		},
		"the preferred backend is the last response received": {
			backends: []*ProxyBackend{backendPref, backendOther1},
			responses: []*backendResponse{
				{backend: backendOther1, status: 200},
				{backend: backendPref, status: 200},
			},
			expected: backendPref,
		},
		"the preferred backend is the last response received but it's not successful": {
			backends: []*ProxyBackend{backendPref, backendOther1},
			responses: []*backendResponse{
				{backend: backendOther1, status: 200},
				{backend: backendPref, status: 500},
			},
			expected: backendOther1,
		},
		"the preferred backend is the 2nd response received but only the last one is successful": {
			backends: []*ProxyBackend{backendPref, backendOther1, backendOther2},
			responses: []*backendResponse{
				{backend: backendOther1, status: 500},
				{backend: backendPref, status: 500},
				{backend: backendOther2, status: 200},
			},
			expected: backendOther2,
		},
		"there's no preferred backend configured and the 1st response is successful": {
			backends: []*ProxyBackend{backendOther1, backendOther2},
			responses: []*backendResponse{
				{backend: backendOther1, status: 200},
			},
			expected: backendOther1,
		},
		"there's no preferred backend configured and the last response is successful": {
			backends: []*ProxyBackend{backendOther1, backendOther2},
			responses: []*backendResponse{
				{backend: backendOther1, status: 500},
				{backend: backendOther2, status: 200},
			},
			expected: backendOther2,
		},
		"no received response is successful": {
			backends: []*ProxyBackend{backendPref, backendOther1},
			responses: []*backendResponse{
				{backend: backendOther1, status: 500},
				{backend: backendPref, status: 500},
			},
			expected: backendOther1,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			endpoint := NewProxyEndpoint(testData.backends, "test", NewProxyMetrics(nil), log.NewNopLogger(), nil)

			// Send the responses from a dedicated goroutine.
			resCh := make(chan *backendResponse)
			go func() {
				for _, res := range testData.responses {
					resCh <- res
				}
				close(resCh)
			}()

			// Wait for the selected backend response.
			actual := endpoint.waitBackendResponseForDownstream(resCh)
			assert.Equal(t, testData.expected, actual.backend)
		})
	}
}

func Test_ProxyEndpoint_Post(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			body, err := io.ReadAll(r.Body)
			require.Equal(t, "this-is-some-payload", string(body))
			require.NoError(t, err)
		}
		_, _ = w.Write([]byte("ok"))
	})
	backend1 := httptest.NewServer(handler)
	defer backend1.Close()
	backendURL1, err := url.Parse(backend1.URL)
	require.NoError(t, err)

	backend2 := httptest.NewServer(handler)
	defer backend2.Close()
	backendURL2, err := url.Parse(backend2.URL)
	require.NoError(t, err)

	var backends = []*ProxyBackend{
		NewProxyBackend("backend-1", backendURL1, time.Second, true),
		NewProxyBackend("backend-2", backendURL2, time.Second, false),
	}
	endpoint := NewProxyEndpoint(backends, "test", NewProxyMetrics(nil), log.NewNopLogger(), nil)

	// test GET request
	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "http://test/api/v1/test", nil)
	require.NoError(t, err)
	endpoint.ServeHTTP(w, req)
	require.Equal(t, "ok", w.Body.String())
	require.Equal(t, 200, w.Code)

	// test POST request
	strings := strings.NewReader("this-is-some-payload")
	w = httptest.NewRecorder()
	req, err = http.NewRequest("POST", "http://test/api/v1/test", strings)
	require.NoError(t, err)
	endpoint.ServeHTTP(w, req)
	require.Equal(t, "ok", w.Body.String())
	require.Equal(t, 200, w.Code)

}

func Test_backendResponse_succeeded(t *testing.T) {
	tests := map[string]struct {
		resStatus int
		resError  error
		expected  bool
	}{
		"Error while executing request": {
			resStatus: 0,
			resError:  errors.New("network error"),
			expected:  false,
		},
		"2xx response status code": {
			resStatus: 200,
			resError:  nil,
			expected:  true,
		},
		"3xx response status code": {
			resStatus: 300,
			resError:  nil,
			expected:  false,
		},
		"4xx response status code": {
			resStatus: 400,
			resError:  nil,
			expected:  true,
		},
		"5xx response status code": {
			resStatus: 500,
			resError:  nil,
			expected:  false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			res := &backendResponse{
				status: testData.resStatus,
				err:    testData.resError,
			}

			assert.Equal(t, testData.expected, res.succeeded())
		})
	}
}

func Test_backendResponse_statusCode(t *testing.T) {
	tests := map[string]struct {
		resStatus int
		resError  error
		expected  int
	}{
		"Error while executing request": {
			resStatus: 0,
			resError:  errors.New("network error"),
			expected:  500,
		},
		"200 response status code": {
			resStatus: 200,
			resError:  nil,
			expected:  200,
		},
		"503 response status code": {
			resStatus: 503,
			resError:  nil,
			expected:  503,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			res := &backendResponse{
				status: testData.resStatus,
				err:    testData.resError,
			}

			assert.Equal(t, testData.expected, res.statusCode())
		})
	}
}
