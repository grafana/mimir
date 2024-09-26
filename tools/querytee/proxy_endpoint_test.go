// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/tools/querytee/proxy_endpoint_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querytee

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func Test_ProxyEndpoint_waitBackendResponseForDownstream(t *testing.T) {
	testRoute := Route{RouteName: "test"}
	backendURL1, err := url.Parse("http://backend-1/")
	require.NoError(t, err)
	backendURL2, err := url.Parse("http://backend-2/")
	require.NoError(t, err)
	backendURL3, err := url.Parse("http://backend-3/")
	require.NoError(t, err)

	backendPref := NewProxyBackend("backend-1", backendURL1, time.Second, true, false)
	backendOther1 := NewProxyBackend("backend-2", backendURL2, time.Second, false, false)
	backendOther2 := NewProxyBackend("backend-3", backendURL3, time.Second, false, false)

	tests := map[string]struct {
		backends  []ProxyBackendInterface
		responses []*backendResponse
		expected  ProxyBackendInterface
	}{
		"the preferred backend is the 1st response received": {
			backends: []ProxyBackendInterface{backendPref, backendOther1},
			responses: []*backendResponse{
				{backend: backendPref, status: 200},
			},
			expected: backendPref,
		},
		"the preferred backend is the last response received": {
			backends: []ProxyBackendInterface{backendPref, backendOther1},
			responses: []*backendResponse{
				{backend: backendOther1, status: 200},
				{backend: backendPref, status: 200},
			},
			expected: backendPref,
		},
		"the preferred backend is the last response received but it's not successful": {
			backends: []ProxyBackendInterface{backendPref, backendOther1},
			responses: []*backendResponse{
				{backend: backendOther1, status: 200},
				{backend: backendPref, status: 500},
			},
			expected: backendPref,
		},
		"the preferred backend is the 2nd response received but only the last one is successful": {
			backends: []ProxyBackendInterface{backendPref, backendOther1, backendOther2},
			responses: []*backendResponse{
				{backend: backendOther1, status: 500},
				{backend: backendPref, status: 500},
				{backend: backendOther2, status: 200},
			},
			expected: backendPref,
		},
		"there's a preferred backend configured and no received response is successful": {
			backends: []ProxyBackendInterface{backendPref, backendOther1},
			responses: []*backendResponse{
				{backend: backendOther1, status: 500},
				{backend: backendPref, status: 500},
			},
			expected: backendPref,
		},
		"there's no preferred backend configured and the 1st response is successful": {
			backends: []ProxyBackendInterface{backendOther1, backendOther2},
			responses: []*backendResponse{
				{backend: backendOther1, status: 200},
			},
			expected: backendOther1,
		},
		"there's no preferred backend configured and the last response is successful": {
			backends: []ProxyBackendInterface{backendOther1, backendOther2},
			responses: []*backendResponse{
				{backend: backendOther1, status: 500},
				{backend: backendOther2, status: 200},
			},
			expected: backendOther2,
		},
		"there's no preferred backend configured and no received response is successful": {
			backends: []ProxyBackendInterface{backendOther1, backendOther2},
			responses: []*backendResponse{
				{backend: backendOther1, status: 500},
				{backend: backendOther2, status: 500},
			},
			expected: backendOther1,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			endpoint := NewProxyEndpoint(testData.backends, testRoute, NewProxyMetrics(nil), log.NewNopLogger(), nil, 0, 1.0)

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

	testRoute := Route{RouteName: "test"}
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

	backends := []ProxyBackendInterface{
		NewProxyBackend("backend-1", backendURL1, time.Second, true, false),
		NewProxyBackend("backend-2", backendURL2, time.Second, false, false),
	}
	endpoint := NewProxyEndpoint(backends, testRoute, NewProxyMetrics(nil), log.NewNopLogger(), nil, 0, 1.0)

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
				testHandler = func(w http.ResponseWriter, _ *http.Request) {
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
	testRoute := Route{RouteName: "test"}

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
			preferredBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", scenario.preferredResponseContentType)
				w.WriteHeader(scenario.preferredResponseStatusCode)
				_, err := w.Write([]byte("preferred response"))
				require.NoError(t, err)
			}))

			defer preferredBackend.Close()
			preferredBackendURL, err := url.Parse(preferredBackend.URL)
			require.NoError(t, err)

			secondaryBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", scenario.secondaryResponseContentType)
				w.WriteHeader(scenario.secondaryResponseStatusCode)
				_, err := w.Write([]byte("secondary response"))
				require.NoError(t, err)
			}))

			defer secondaryBackend.Close()
			secondaryBackendURL, err := url.Parse(secondaryBackend.URL)
			require.NoError(t, err)

			backends := []ProxyBackendInterface{
				NewProxyBackend("preferred-backend", preferredBackendURL, time.Second, true, false),
				NewProxyBackend("secondary-backend", secondaryBackendURL, time.Second, false, false),
			}

			logger := newMockLogger()
			reg := prometheus.NewPedanticRegistry()
			comparator := &mockComparator{
				comparisonResult: scenario.comparatorResult,
				comparisonError:  scenario.comparatorError,
			}

			endpoint := NewProxyEndpoint(backends, testRoute, NewProxyMetrics(reg), logger, comparator, 0, 1.0)

			resp := httptest.NewRecorder()
			req, err := http.NewRequest("GET", "http://test/api/v1/test", nil)
			require.NoError(t, err)
			endpoint.ServeHTTP(resp, req)
			require.Equal(t, "preferred response", resp.Body.String())
			require.Equal(t, scenario.preferredResponseStatusCode, resp.Code)
			require.Equal(t, scenario.preferredResponseContentType, resp.Header().Get("Content-Type"))

			// The HTTP request above will return as soon as the primary response is received, but this doesn't guarantee that the response comparison has been completed.
			// Wait for the response comparison to complete before checking the logged messages.
			waitForResponseComparisonMetric(t, reg, scenario.expectedComparisonResult, 1)

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

func Test_ProxyEndpoint_LogSlowQueries(t *testing.T) {
	testRoute := Route{RouteName: "test"}

	scenarios := map[string]struct {
		slowResponseThreshold         time.Duration
		preferredResponseLatency      time.Duration
		secondaryResponseLatency      time.Duration
		expectLatencyExceedsThreshold bool
		fastestBackend                string
		slowestBackend                string
	}{
		"responses are below threshold": {
			slowResponseThreshold:         100 * time.Millisecond,
			preferredResponseLatency:      1 * time.Millisecond,
			secondaryResponseLatency:      1 * time.Millisecond,
			expectLatencyExceedsThreshold: false,
		},
		"one response above threshold": {
			slowResponseThreshold:         50 * time.Millisecond,
			preferredResponseLatency:      1 * time.Millisecond,
			secondaryResponseLatency:      70 * time.Millisecond,
			expectLatencyExceedsThreshold: true,
			fastestBackend:                "preferred-backend",
			slowestBackend:                "secondary-backend",
		},
		"responses are both above threshold, but lower than threshold between themselves": {
			slowResponseThreshold:         50 * time.Millisecond,
			preferredResponseLatency:      51 * time.Millisecond,
			secondaryResponseLatency:      62 * time.Millisecond,
			expectLatencyExceedsThreshold: false,
		},
		"responses are both above threshold, and above threshold between themselves": {
			slowResponseThreshold:         10 * time.Millisecond,
			preferredResponseLatency:      11 * time.Millisecond,
			secondaryResponseLatency:      52 * time.Millisecond,
			expectLatencyExceedsThreshold: true,
			fastestBackend:                "preferred-backend",
			slowestBackend:                "secondary-backend",
		},
		"secondary latency is faster than primary, and difference is below threshold": {
			slowResponseThreshold:         50 * time.Millisecond,
			preferredResponseLatency:      10 * time.Millisecond,
			secondaryResponseLatency:      1 * time.Millisecond,
			expectLatencyExceedsThreshold: false,
		},
		"secondary latency is faster than primary, and difference is above threshold": {
			slowResponseThreshold:         50 * time.Millisecond,
			preferredResponseLatency:      71 * time.Millisecond,
			secondaryResponseLatency:      1 * time.Millisecond,
			expectLatencyExceedsThreshold: true,
			fastestBackend:                "secondary-backend",
			slowestBackend:                "preferred-backend",
		},
		"slowest response threshold is disabled (0)": {
			slowResponseThreshold:         0 * time.Millisecond,
			preferredResponseLatency:      200 * time.Millisecond,
			secondaryResponseLatency:      100 * time.Millisecond,
			expectLatencyExceedsThreshold: false,
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			backends := []ProxyBackendInterface{
				newMockProxyBackend("preferred-backend", time.Second, true, []time.Duration{scenario.preferredResponseLatency}),
				newMockProxyBackend("secondary-backend", time.Second, false, []time.Duration{scenario.secondaryResponseLatency}),
			}

			logger := newMockLogger()
			reg := prometheus.NewPedanticRegistry()
			comparator := &mockComparator{
				comparisonResult: ComparisonSuccess,
			}

			endpoint := NewProxyEndpoint(backends, testRoute, NewProxyMetrics(reg), logger, comparator, scenario.slowResponseThreshold, 1.0)

			resp := httptest.NewRecorder()
			req, err := http.NewRequest("GET", "http://test/api/v1/test", nil)
			require.NoError(t, err)
			endpoint.ServeHTTP(resp, req)

			// The HTTP request above will return as soon as the primary response is received, but this doesn't guarantee that the response comparison has been completed.
			// Wait for the response comparison to complete before checking the logged messages.
			waitForResponseComparisonMetric(t, reg, ComparisonSuccess, 1)

			if scenario.expectLatencyExceedsThreshold {
				requireLogKeyValues(t, logger.messages, map[string]string{
					"msg":             "response time difference between backends exceeded threshold",
					"slowest_backend": scenario.slowestBackend,
					"fastest_backend": scenario.fastestBackend,
				})
			} else {
				requireNoLogMessages(t, logger.messages, "response time difference between backends exceeded threshold")
			}
		})
	}
}

func Test_ProxyEndpoint_RelativeDurationMetric(t *testing.T) {
	testRoute := Route{RouteName: "test"}

	scenarios := map[string]struct {
		latencyPairs                  []latencyPair
		expectedDurationSampleSum     float64
		expectedProportionalSampleSum float64
	}{
		"secondary backend is faster than preferred": {
			latencyPairs: []latencyPair{
				{
					preferredResponseLatency: 3 * time.Second,
					secondaryResponseLatency: 1 * time.Second,
				}, {
					preferredResponseLatency: 5 * time.Second,
					secondaryResponseLatency: 2 * time.Second,
				},
			},
			expectedDurationSampleSum:     -5,
			expectedProportionalSampleSum: -2.0/3 + -3.0/5,
		},
		"preferred backend is 5 seconds faster than secondary": {
			latencyPairs: []latencyPair{{
				preferredResponseLatency: 2 * time.Second,
				secondaryResponseLatency: 7 * time.Second,
			}},
			expectedDurationSampleSum:     5,
			expectedProportionalSampleSum: 5.0 / 2,
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			preferredLatencies, secondaryLatencies := splitLatencyPairs(scenario.latencyPairs)
			backends := []ProxyBackendInterface{
				newMockProxyBackend("preferred-backend", time.Second, true, preferredLatencies),
				newMockProxyBackend("secondary-backend", time.Second, false, secondaryLatencies),
			}

			logger := newMockLogger()
			reg := prometheus.NewPedanticRegistry()
			comparator := &mockComparator{
				comparisonResult: ComparisonSuccess,
			}

			endpoint := NewProxyEndpoint(backends, testRoute, NewProxyMetrics(reg), logger, comparator, 0, 1.0)

			resp := httptest.NewRecorder()
			req, err := http.NewRequest("GET", "http://test/api/v1/test", nil)
			require.NoError(t, err)
			for i := range scenario.latencyPairs {
				// This is just in serial to keep the tests simple
				endpoint.ServeHTTP(resp, req)
				// The HTTP request above will return as soon as the primary response is received, but this doesn't guarantee that the response comparison has been completed.
				// Wait for the response comparison to complete the number of requests we have.
				// We do this for each latencyPair to avoid a race where the second request would consume a result expected for the first request.
				waitForResponseComparisonMetric(t, reg, ComparisonSuccess, uint64(i+1))
			}

			got, done, err := prometheus.ToTransactionalGatherer(reg).Gather()
			defer done()
			require.NoError(t, err, "Failed to gather metrics from registry")

			gotDuration := filterMetrics(got, []string{"cortex_querytee_backend_response_relative_duration_seconds"})
			require.Equal(t, 1, len(gotDuration), "Expect only one metric after filtering")
			require.Equal(t, uint64(len(scenario.latencyPairs)), gotDuration[0].Metric[0].Histogram.GetSampleCount())
			require.InDelta(t, scenario.expectedDurationSampleSum, gotDuration[0].Metric[0].Histogram.GetSampleSum(), 1e-9)

			gotProportional := filterMetrics(got, []string{"cortex_querytee_backend_response_relative_duration_proportional"})
			require.Equal(t, 1, len(gotProportional), "Expect only one metric after filtering")
			require.Equal(t, uint64(len(scenario.latencyPairs)), gotProportional[0].Metric[0].Histogram.GetSampleCount())
			require.InDelta(t, scenario.expectedProportionalSampleSum, gotProportional[0].Metric[0].Histogram.GetSampleSum(), 1e-9)
		})
	}
}

func filterMetrics(metrics []*dto.MetricFamily, names []string) []*dto.MetricFamily {
	var filtered []*dto.MetricFamily
	for _, m := range metrics {
		for _, name := range names {
			if m.GetName() == name {
				filtered = append(filtered, m)
				break
			}
		}
	}
	return filtered
}

func waitForResponseComparisonMetric(t *testing.T, g prometheus.Gatherer, expectedResult ComparisonResult, expectedCount uint64) {
	started := time.Now()
	timeoutAt := started.Add(2 * time.Second)

	for {
		expected := fmt.Sprintf(`
			# HELP cortex_querytee_responses_compared_total Total number of responses compared per route name by result.
			# TYPE cortex_querytee_responses_compared_total counter
			cortex_querytee_responses_compared_total{result="%v",route="test"} %d
`, expectedResult, expectedCount)
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

func requireLogKeyValues(t *testing.T, messages []map[string]interface{}, targetKeyValues map[string]string) {
	allKeyValuesMatch := false
	for _, m := range messages {
		allKeyValuesMatch = true
		for targetKey, targetValue := range targetKeyValues {
			if value, exists := m[targetKey]; !exists || value != targetValue {
				// Key does not exist or value does not match
				allKeyValuesMatch = false
				break
			}
		}
		if allKeyValuesMatch {
			break
		}
	}

	require.True(t, allKeyValuesMatch, "expected to find a message logged with specific key-values: %s, but only these messages were logged: %v", targetKeyValues, messages)
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

func (m *mockComparator) Compare(_, _ []byte, _ time.Time) (ComparisonResult, error) {
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

type latencyPair struct {
	preferredResponseLatency time.Duration
	secondaryResponseLatency time.Duration
}

func splitLatencyPairs(latencyPairs []latencyPair) (preferredLatencies []time.Duration, secondaryLatencies []time.Duration) {
	for _, pair := range latencyPairs {
		preferredLatencies = append(preferredLatencies, pair.preferredResponseLatency)
		secondaryLatencies = append(secondaryLatencies, pair.secondaryResponseLatency)
	}
	return
}

type mockProxyBackend struct {
	name                  string
	timeout               time.Duration
	preferred             bool
	fakeResponseLatencies []time.Duration
	responseIndex         int
}

func newMockProxyBackend(name string, timeout time.Duration, preferred bool, fakeResponseLatencies []time.Duration) ProxyBackendInterface {
	return &mockProxyBackend{
		name:                  name,
		timeout:               timeout,
		preferred:             preferred,
		fakeResponseLatencies: fakeResponseLatencies,
	}
}

func (b *mockProxyBackend) Name() string {
	return b.name
}

func (b *mockProxyBackend) Endpoint() *url.URL {
	return nil
}

func (b *mockProxyBackend) Preferred() bool {
	return b.preferred
}

func (b *mockProxyBackend) ForwardRequest(_ context.Context, _ *http.Request, _ io.ReadCloser) (time.Duration, int, []byte, *http.Response, error) {
	resp := &http.Response{
		StatusCode: 200,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewBufferString(`{}`)),
	}
	resp.Header.Set("Content-Type", "application/json")
	if b.responseIndex >= len(b.fakeResponseLatencies) {
		resp.StatusCode = 500
		return 0, 500, []byte("{}"), resp, errors.New("no more latencies available")
	}
	latency := b.fakeResponseLatencies[b.responseIndex]
	b.responseIndex++
	return latency, 200, []byte("{}"), resp, nil
}

func TestProxyEndpoint_BackendSelection(t *testing.T) {
	const runCount = 1000

	testCases := map[string]struct {
		backends                            []ProxyBackendInterface
		secondaryBackendRequestProportion   float64
		expectedPreferredOnlySelectionCount int // Out of 1000 runs
	}{
		"single preferred backend, secondary request proportion 0.0": {
			backends:                            []ProxyBackendInterface{newMockProxyBackend("preferred-backend", 0, true, nil)},
			secondaryBackendRequestProportion:   0.0,
			expectedPreferredOnlySelectionCount: runCount,
		},
		"single preferred backend, secondary request proportion 1.0": {
			backends:                            []ProxyBackendInterface{newMockProxyBackend("preferred-backend", 0, true, nil)},
			secondaryBackendRequestProportion:   1.0,
			expectedPreferredOnlySelectionCount: runCount,
		},
		"single non-preferred backend, secondary request proportion 0.0": {
			backends:                            []ProxyBackendInterface{newMockProxyBackend("preferred-backend", 0, false, nil)},
			secondaryBackendRequestProportion:   0.0,
			expectedPreferredOnlySelectionCount: runCount,
		},
		"single non-preferred backend, secondary request proportion 1.0": {
			backends:                            []ProxyBackendInterface{newMockProxyBackend("preferred-backend", 0, false, nil)},
			secondaryBackendRequestProportion:   1.0,
			expectedPreferredOnlySelectionCount: runCount,
		},
		"multiple backends, secondary request proportion 0.0": {
			backends:                            []ProxyBackendInterface{newMockProxyBackend("preferred-backend", 0, true, nil), newMockProxyBackend("non-preferred-backend", 0, false, nil)},
			secondaryBackendRequestProportion:   0.0,
			expectedPreferredOnlySelectionCount: runCount,
		},
		"multiple backends, secondary request proportion 0.2": {
			backends:                            []ProxyBackendInterface{newMockProxyBackend("preferred-backend", 0, true, nil), newMockProxyBackend("non-preferred-backend", 0, false, nil)},
			secondaryBackendRequestProportion:   0.2,
			expectedPreferredOnlySelectionCount: 800,
		},
		"multiple backends, secondary request proportion 1.0": {
			backends:                            []ProxyBackendInterface{newMockProxyBackend("preferred-backend", 0, true, nil), newMockProxyBackend("non-preferred-backend", 0, false, nil)},
			secondaryBackendRequestProportion:   1.0,
			expectedPreferredOnlySelectionCount: 0,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			proxyEndpoint := NewProxyEndpoint(testCase.backends, Route{}, nil, nil, nil, 0, testCase.secondaryBackendRequestProportion)
			preferredOnlySelectionCount := 0

			for i := 0; i < runCount; i++ {
				backends := proxyEndpoint.selectBackends()
				require.GreaterOrEqual(t, len(backends), 1)

				if len(backends) == 1 {
					preferredOnlySelectionCount++
					require.Equal(t, "preferred-backend", backends[0].Name())
				}
			}

			if testCase.expectedPreferredOnlySelectionCount == 0 || testCase.expectedPreferredOnlySelectionCount == runCount {
				// We expect to have selected only the preferred backend either every time or never.
				require.Equal(t, testCase.expectedPreferredOnlySelectionCount, preferredOnlySelectionCount)
			} else {
				// We expect to have selected only the preferred backend just some of the time.
				// Allow for some variation due to randomness.
				require.InEpsilonf(t, testCase.expectedPreferredOnlySelectionCount, preferredOnlySelectionCount, 0.2, "expected to choose only the preferred backend %v times, but chose it %v times", testCase.expectedPreferredOnlySelectionCount, preferredOnlySelectionCount)
			}
		})
	}
}
