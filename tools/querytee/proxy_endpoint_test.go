// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/tools/querytee/proxy_endpoint_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querytee

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
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

func Test_ProxyEndpoint_Requests(t *testing.T) {
	var (
		requestCount atomic.Uint64
		wg           sync.WaitGroup
		testHandler  http.HandlerFunc
	)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer wg.Done()
		defer requestCount.Add(1)
		testHandler(w, r)
	})
	backend1 := httptest.NewServer(handler)
	defer backend1.Close()
	backendURL1, err := url.Parse(backend1.URL)
	require.NoError(t, err)

	backend2 := httptest.NewServer(handler)
	defer backend2.Close()
	backendURL2, err := url.Parse(backend2.URL)
	require.NoError(t, err)

	backends := []*ProxyBackend{
		NewProxyBackend("backend-1", backendURL1, time.Second, true),
		NewProxyBackend("backend-2", backendURL2, time.Second, false),
	}
	endpoint := NewProxyEndpoint(backends, "test", NewProxyMetrics(nil), log.NewNopLogger(), nil)

	for _, tc := range []struct {
		name    string
		request func(*testing.T) *http.Request
		handler func(*testing.T) http.HandlerFunc
	}{
		{
			name: "GET-request",
			request: func(t *testing.T) *http.Request {
				r, err := http.NewRequest("GET", "http://test/api/v1/test", nil)
				r.Header["test-X"] = []string{"test-X-value"}
				require.NoError(t, err)
				return r
			},
			handler: func(t *testing.T) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					require.Equal(t, "test-X-value", r.Header.Get("test-X"))
					_, _ = w.Write([]byte("ok"))
				}
			},
		},
		{
			name: "GET-filter-accept-encoding",
			request: func(t *testing.T) *http.Request {
				r, err := http.NewRequest("GET", "http://test/api/v1/test", nil)
				r.Header.Set("Accept-Encoding", "gzip")
				require.NoError(t, err)
				return r
			},
			handler: func(t *testing.T) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					require.Equal(t, 0, len(r.Header.Values("Accept-Encoding")))
					_, _ = w.Write([]byte("ok"))
				}
			},
		},
		{
			name: "POST-request-with-body",
			request: func(t *testing.T) *http.Request {
				strings := strings.NewReader("this-is-some-payload")
				r, err := http.NewRequest("POST", "http://test/api/v1/test", strings)
				require.NoError(t, err)
				r.Header["test-X"] = []string{"test-X-value"}
				return r
			},
			handler: func(t *testing.T) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					body, err := io.ReadAll(r.Body)
					require.Equal(t, "this-is-some-payload", string(body))
					require.NoError(t, err)
					require.Equal(t, "test-X-value", r.Header.Get("test-X"))

					_, _ = w.Write([]byte("ok"))
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// reset request count
			requestCount.Store(0)
			wg.Add(2)

			if tc.handler == nil {
				testHandler = func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write([]byte("ok"))
				}

			} else {
				testHandler = tc.handler(t)
			}

			w := httptest.NewRecorder()
			endpoint.ServeHTTP(w, tc.request(t))
			require.Equal(t, "ok", w.Body.String())
			require.Equal(t, 200, w.Code)

			wg.Wait()
			require.Equal(t, uint64(2), requestCount.Load())
		})
	}
}

func Test_ProxyEndpoint_Comparison(t *testing.T) {
	scenarios := map[string]struct {
		preferredResponseStatusCode  int
		secondaryResponseStatusCode  int
		preferredResponseContentType string
		secondaryResponseContentType string
		comparatorResult             ComparisonResult
		comparatorError              error
		expectedComparisonResult     ComparisonResult
		expectedComparisonError      string
	}{
		"responses are the same": {
			preferredResponseStatusCode:  http.StatusOK,
			secondaryResponseStatusCode:  http.StatusOK,
			preferredResponseContentType: "application/json",
			secondaryResponseContentType: "application/json",
			comparatorResult:             ComparisonSuccess,
			expectedComparisonResult:     ComparisonSuccess,
		},
		"responses are not the same": {
			preferredResponseStatusCode:  http.StatusOK,
			secondaryResponseStatusCode:  http.StatusOK,
			preferredResponseContentType: "application/json",
			secondaryResponseContentType: "application/json",
			comparatorResult:             ComparisonFailed,
			comparatorError:              errors.New("the responses are different"),
			expectedComparisonError:      "the responses are different",
			expectedComparisonResult:     ComparisonFailed,
		},
		"responses have different status codes": {
			preferredResponseStatusCode:  http.StatusOK,
			secondaryResponseStatusCode:  http.StatusTeapot,
			preferredResponseContentType: "application/json",
			secondaryResponseContentType: "application/json",
			expectedComparisonError:      "expected status code 200 (returned by preferred backend) but got 418 from secondary backend",
			expectedComparisonResult:     ComparisonFailed,
		},
		"preferred backend response has non-JSON content type": {
			preferredResponseStatusCode:  http.StatusOK,
			secondaryResponseStatusCode:  http.StatusOK,
			preferredResponseContentType: "text/plain",
			secondaryResponseContentType: "application/json",
			expectedComparisonError:      "skipped comparison of response because the response from the preferred backend contained an unexpected content type 'text/plain', expected 'application/json'",
			expectedComparisonResult:     ComparisonSkipped,
		},
		"secondary backend response has non-JSON content type": {
			preferredResponseStatusCode:  http.StatusOK,
			secondaryResponseStatusCode:  http.StatusOK,
			preferredResponseContentType: "application/json",
			secondaryResponseContentType: "text/plain",
			expectedComparisonError:      "skipped comparison of response because the response from the secondary backend contained an unexpected content type 'text/plain', expected 'application/json'",
			expectedComparisonResult:     ComparisonSkipped,
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			preferredBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", scenario.preferredResponseContentType)
				w.WriteHeader(scenario.preferredResponseStatusCode)
				_, err := w.Write([]byte("preferred response"))
				require.NoError(t, err)
			}))

			defer preferredBackend.Close()
			preferredBackendURL, err := url.Parse(preferredBackend.URL)
			require.NoError(t, err)

			secondaryBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", scenario.secondaryResponseContentType)
				w.WriteHeader(scenario.secondaryResponseStatusCode)
				_, err := w.Write([]byte("secondary response"))
				require.NoError(t, err)
			}))

			defer secondaryBackend.Close()
			secondaryBackendURL, err := url.Parse(secondaryBackend.URL)
			require.NoError(t, err)

			backends := []*ProxyBackend{
				NewProxyBackend("preferred-backend", preferredBackendURL, time.Second, true),
				NewProxyBackend("secondary-backend", secondaryBackendURL, time.Second, false),
			}

			logger := newMockLogger()
			reg := prometheus.NewPedanticRegistry()
			comparator := &mockComparator{
				comparisonResult: scenario.comparatorResult,
				comparisonError:  scenario.comparatorError,
			}

			endpoint := NewProxyEndpoint(backends, "test", NewProxyMetrics(reg), logger, comparator)

			resp := httptest.NewRecorder()
			req, err := http.NewRequest("GET", "http://test/api/v1/test", nil)
			require.NoError(t, err)
			endpoint.ServeHTTP(resp, req)
			require.Equal(t, "preferred response", resp.Body.String())
			require.Equal(t, scenario.preferredResponseStatusCode, resp.Code)
			require.Equal(t, scenario.preferredResponseContentType, resp.Header().Get("Content-Type"))

			// The HTTP request above will return as soon as the primary response is received, but this doesn't guarantee that the response comparison has been completed.
			// Wait for the response comparison to complete before checking the logged messages.
			waitForResponseComparisonMetric(t, reg, scenario.expectedComparisonResult)

			switch scenario.expectedComparisonResult {
			case ComparisonSuccess:
				requireNoLogMessages(t, logger.messages, "response comparison failed", "response comparison skipped")

			case ComparisonFailed:
				requireLogMessageWithError(t, logger.messages, "response comparison failed", scenario.expectedComparisonError)

			case ComparisonSkipped:
				requireLogMessageWithError(t, logger.messages, "response comparison skipped", scenario.expectedComparisonError)
			}
		})
	}
}

func waitForResponseComparisonMetric(t *testing.T, g prometheus.Gatherer, expectedResult ComparisonResult) {
	started := time.Now()
	timeoutAt := started.Add(2 * time.Second)

	for {
		expected := fmt.Sprintf(`
			# HELP cortex_querytee_responses_compared_total Total number of responses compared per route name by result.
			# TYPE cortex_querytee_responses_compared_total counter
			cortex_querytee_responses_compared_total{result="%v",route="test"} 1
`, expectedResult)
		err := testutil.GatherAndCompare(g, bytes.NewBufferString(expected), "cortex_querytee_responses_compared_total")

		if err == nil {
			return
		}

		if time.Now().After(timeoutAt) {
			require.NoError(t, err, "timed out waiting for comparison result to be reported, last metrics comparison failed with error")
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func requireNoLogMessages(t *testing.T, messages []map[string]interface{}, forbiddenMessages ...string) {
	for _, m := range messages {
		msg := m["msg"]

		for _, forbiddenMessage := range forbiddenMessages {
			if msg == forbiddenMessage {
				require.Fail(t, "unexpected message logged", "expected to find no log lines with the message '%s', but these messages were logged: %v", forbiddenMessage, messages)
			}
		}
	}
}

func requireLogMessageWithError(t *testing.T, messages []map[string]interface{}, expectedMessage string, expectedError string) {
	sawMessage := false

	for _, m := range messages {
		if m["msg"] == expectedMessage {
			sawMessage = true
			require.EqualError(t, m["err"].(error), expectedError)
		}
	}

	require.True(t, sawMessage, "expected to find a '%s' message logged, but only these messages were logged: %v", expectedMessage, messages)
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

type mockComparator struct {
	comparisonResult ComparisonResult
	comparisonError  error
}

func (m *mockComparator) Compare(_, _ []byte) (ComparisonResult, error) {
	return m.comparisonResult, m.comparisonError
}

type mockLogger struct {
	messages []map[string]interface{}
	lock     sync.Mutex
}

func newMockLogger() *mockLogger {
	return &mockLogger{
		lock: sync.Mutex{},
	}
}

func (m *mockLogger) Log(keyvals ...interface{}) error {
	if len(keyvals)%2 != 0 {
		panic("invalid log message")
	}

	message := map[string]interface{}{}

	for keyIndex := 0; keyIndex < len(keyvals); keyIndex += 2 {
		message[keyvals[keyIndex].(string)] = keyvals[keyIndex+1]
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	m.messages = append(m.messages, message)

	return nil
}
