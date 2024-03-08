// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/frontend/transport/handler_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package transport

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/util/activitytracker"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestWriteError(t *testing.T) {
	for _, test := range []struct {
		status int
		err    error
	}{
		{http.StatusInternalServerError, errors.New("unknown")},
		{http.StatusGatewayTimeout, context.DeadlineExceeded},
		{StatusClientClosedRequest, context.Canceled},
		{http.StatusBadRequest, httpgrpc.Errorf(http.StatusBadRequest, "")},
	} {
		t.Run(test.err.Error(), func(t *testing.T) {
			w := httptest.NewRecorder()
			writeError(w, test.err)
			require.Equal(t, test.status, w.Result().StatusCode)
		})
	}
}

func TestHandler_ServeHTTP(t *testing.T) {
	for _, tt := range []struct {
		name                    string
		cfg                     HandlerConfig
		request                 func() *http.Request
		expectedParams          url.Values
		expectedMetrics         int
		expectedActivity        string
		expectedReadConsistency string
	}{
		{
			name: "handler with stats enabled, POST request with params",
			cfg:  HandlerConfig{QueryStatsEnabled: true, MaxBodySize: 1024},
			request: func() *http.Request {
				form := url.Values{
					"query": []string{"some_metric"},
					"time":  []string{"42"},
				}
				r := httptest.NewRequest("POST", "/api/v1/query", strings.NewReader(form.Encode()))
				r.Header.Add("Content-Type", "application/x-www-form-urlencoded")
				r.Header.Add("User-Agent", "test-user-agent")
				return r
			},
			expectedParams: url.Values{
				"query": []string{"some_metric"},
				"time":  []string{"42"},
			},
			expectedMetrics:         5,
			expectedActivity:        "user:12345 UA:test-user-agent req:POST /api/v1/query query=some_metric&time=42",
			expectedReadConsistency: "",
		},
		{
			name: "handler with stats enabled, GET request with params",
			cfg:  HandlerConfig{QueryStatsEnabled: true},
			request: func() *http.Request {
				r := httptest.NewRequest("GET", "/api/v1/query?query=some_metric&time=42", nil)
				r.Header.Add("User-Agent", "test-user-agent")
				return r
			},
			expectedParams: url.Values{
				"query": []string{"some_metric"},
				"time":  []string{"42"},
			},
			expectedMetrics:         5,
			expectedActivity:        "user:12345 UA:test-user-agent req:GET /api/v1/query query=some_metric&time=42",
			expectedReadConsistency: "",
		},
		{
			name: "handler with stats enabled, GET request with params and read consistency specified",
			cfg:  HandlerConfig{QueryStatsEnabled: true},
			request: func() *http.Request {
				r := httptest.NewRequest("GET", "/api/v1/query?query=some_metric&time=42", nil)
				r.Header.Add("User-Agent", "test-user-agent")
				return r.WithContext(api.ContextWithReadConsistency(context.Background(), api.ReadConsistencyStrong))
			},
			expectedParams: url.Values{
				"query": []string{"some_metric"},
				"time":  []string{"42"},
			},
			expectedMetrics:         5,
			expectedActivity:        "user:12345 UA:test-user-agent req:GET /api/v1/query query=some_metric&time=42",
			expectedReadConsistency: api.ReadConsistencyStrong,
		},
		{
			name: "handler with stats enabled, GET request without params",
			cfg:  HandlerConfig{QueryStatsEnabled: true},
			request: func() *http.Request {
				r := httptest.NewRequest("GET", "/api/v1/query", nil)
				r.Header.Add("User-Agent", "test-user-agent")
				return r
			},
			expectedParams:          url.Values{},
			expectedMetrics:         5,
			expectedActivity:        "user:12345 UA:test-user-agent req:GET /api/v1/query (no params)",
			expectedReadConsistency: "",
		},
		{
			name: "handler with stats disabled, GET request with params",
			cfg:  HandlerConfig{QueryStatsEnabled: false},
			request: func() *http.Request {
				r := httptest.NewRequest("GET", "/api/v1/query?query=some_metric&time=42", nil)
				r.Header.Add("User-Agent", "test-user-agent")
				return r
			},
			expectedParams: url.Values{
				"query": []string{"some_metric"},
				"time":  []string{"42"},
			},
			expectedMetrics:         0,
			expectedActivity:        "user:12345 UA:test-user-agent req:GET /api/v1/query query=some_metric&time=42",
			expectedReadConsistency: "",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			activityFile := filepath.Join(t.TempDir(), "activity-tracker")

			roundTripper := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				activities, err := activitytracker.LoadUnfinishedEntries(activityFile)
				assert.NoError(t, err)
				assert.Len(t, activities, 1)
				assert.Equal(t, tt.expectedActivity, activities[0].Activity)

				assert.NoError(t, req.ParseForm())
				assert.Equal(t, tt.expectedParams, req.Form)

				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("{}")),
				}, nil
			})

			reg := prometheus.NewPedanticRegistry()

			at, err := activitytracker.NewActivityTracker(activitytracker.Config{Filepath: activityFile, MaxEntries: 1024}, reg)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, at.Close()) })

			logger := &testLogger{}
			handler := NewHandler(tt.cfg, roundTripper, logger, reg, at)

			req := tt.request()
			req = req.WithContext(user.InjectOrgID(req.Context(), "12345"))
			resp := httptest.NewRecorder()

			handler.ServeHTTP(resp, req)
			responseData, _ := io.ReadAll(resp.Body)
			require.Equal(t, resp.Code, http.StatusOK)

			count, err := promtest.GatherAndCount(
				reg,
				"cortex_query_seconds_total",
				"cortex_query_fetched_series_total",
				"cortex_query_fetched_chunk_bytes_total",
				"cortex_query_fetched_chunks_total",
				"cortex_query_fetched_index_bytes_total",
			)

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedMetrics, count)

			activities, err := activitytracker.LoadUnfinishedEntries(activityFile)
			require.NoError(t, err)
			require.Empty(t, activities)

			if tt.cfg.QueryStatsEnabled {
				require.Len(t, logger.logMessages, 1)

				msg := logger.logMessages[0]
				require.Equal(t, level.InfoValue(), msg["level"])
				require.Equal(t, "query stats", msg["msg"])
				require.Equal(t, "query-frontend", msg["component"])
				require.Equal(t, "success", msg["status"])
				require.Equal(t, "12345", msg["user"])
				require.Equal(t, req.Method, msg["method"])
				require.Equal(t, req.URL.Path, msg["path"])
				require.Equal(t, req.UserAgent(), msg["user_agent"])
				require.Contains(t, msg, "response_time")
				require.Equal(t, int64(len(responseData)), msg["response_size_bytes"])
				require.Contains(t, msg, "query_wall_time_seconds")
				require.EqualValues(t, 0, msg["fetched_series_count"])
				require.EqualValues(t, 0, msg["fetched_chunk_bytes"])
				require.EqualValues(t, 0, msg["fetched_chunks_count"])
				require.EqualValues(t, 0, msg["fetched_index_bytes"])
				require.EqualValues(t, 0, msg["sharded_queries"])
				require.EqualValues(t, 0, msg["split_queries"])
				require.EqualValues(t, 0, msg["estimated_series_count"])
				require.EqualValues(t, 0, msg["queue_time_seconds"])

				if tt.expectedReadConsistency != "" {
					require.Equal(t, tt.expectedReadConsistency, msg["read_consistency"])
				} else {
					_, ok := msg["read_consistency"]
					require.False(t, ok)
				}
			} else {
				require.Empty(t, logger.logMessages)
			}
		})
	}
}

func TestHandler_FailedRoundTrip(t *testing.T) {
	for _, test := range []struct {
		name                string
		cfg                 HandlerConfig
		path                string
		queryResponseFunc   roundTripperFunc
		expectedStatusCode  int
		expectedMetrics     int
		expectedStatusLog   string
		expectQueryParamLog bool
	}{
		{
			name: "Failed round trip with context cancelled",
			cfg:  HandlerConfig{QueryStatsEnabled: false},
			path: "/api/v1/query?query=up&time=2015-07-01T20:10:51.781Z",
			queryResponseFunc: func(*http.Request) (*http.Response, error) {
				return nil, context.Canceled
			},
			expectedStatusCode:  StatusClientClosedRequest,
			expectedMetrics:     0,
			expectedStatusLog:   "canceled",
			expectQueryParamLog: true,
		},
		{
			name: "Failed round trip with no query params",
			cfg:  HandlerConfig{QueryStatsEnabled: true},
			path: "/api/v1/query",
			queryResponseFunc: func(*http.Request) (*http.Response, error) {
				return nil, context.Canceled
			},
			expectedStatusCode:  StatusClientClosedRequest,
			expectedMetrics:     5,
			expectedStatusLog:   "canceled",
			expectQueryParamLog: false,
		},
		{
			name: "Failed round trip with HTTP response",
			cfg:  HandlerConfig{QueryStatsEnabled: true},
			path: "/api/v1/query",
			queryResponseFunc: func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body:       io.NopCloser(strings.NewReader("{}")),
				}, nil
			},
			expectedStatusCode:  http.StatusInternalServerError,
			expectedMetrics:     5,
			expectedStatusLog:   "failed",
			expectQueryParamLog: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			logs := &concurrency.SyncBuffer{}
			logger := log.NewLogfmtLogger(logs)
			handler := NewHandler(test.cfg, test.queryResponseFunc, logger, reg, nil)

			ctx := user.InjectOrgID(context.Background(), "12345")
			req := httptest.NewRequest("GET", test.path, nil)
			req = req.WithContext(ctx)
			resp := httptest.NewRecorder()

			handler.ServeHTTP(resp, req)
			require.Equal(t, test.expectedStatusCode, resp.Code)

			count, err := promtest.GatherAndCount(
				reg,
				"cortex_query_seconds_total",
				"cortex_query_fetched_series_total",
				"cortex_query_fetched_chunk_bytes_total",
				"cortex_query_fetched_chunks_total",
				"cortex_query_fetched_index_bytes_total",
			)
			require.NoError(t, err)

			assert.Contains(t, strings.TrimSpace(logs.String()), "sharded_queries")
			assert.Contains(t, strings.TrimSpace(logs.String()), fmt.Sprintf("status=%s", test.expectedStatusLog))
			if test.expectQueryParamLog {
				assert.Contains(t, strings.TrimSpace(logs.String()), "param_query")
			}
			assert.Equal(t, test.expectedMetrics, count)
		})
	}
}

// Test Handler.Stop.
func TestHandler_Stop(t *testing.T) {
	const (
		// We want to verify that the Stop method will wait on 10 in-flight requests.
		requests = 10
	)
	inProgress := make(chan int32)
	var reqID atomic.Int32
	roundTripper := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		id := reqID.Inc()
		t.Logf("request %d sending its ID", id)
		inProgress <- id
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("{}")),
		}, nil
	})

	reg := prometheus.NewPedanticRegistry()
	cfg := HandlerConfig{MaxBodySize: 1024}
	logger := &testLogger{}
	handler := NewHandler(cfg, roundTripper, logger, reg, nil)

	var wg sync.WaitGroup
	for i := 0; i < requests; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			form := url.Values{
				"query": []string{"some_metric"},
				"time":  []string{"42"},
			}
			req := httptest.NewRequest(http.MethodPost, "/api/v1/query", strings.NewReader(form.Encode()))
			req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Add("User-Agent", "test-user-agent")
			req = req.WithContext(user.InjectOrgID(context.Background(), "12345"))
			resp := httptest.NewRecorder()

			handler.ServeHTTP(resp, req)
			_, _ = io.ReadAll(resp.Body)
			require.Equal(t, http.StatusOK, resp.Code)
		}()
	}

	// Wait for all requests to be in flight
	test.Poll(t, 1*time.Second, requests, func() interface{} {
		return int(reqID.Load())
	})
	t.Log("all requests in flight")

	var stopped atomic.Bool
	go func() {
		t.Log("waiting on handler to stop")
		handler.Stop()
		t.Log("done waiting on handler to stop")
		stopped.Store(true)
	}()

	// Wait for handler to enter stopping mode
	test.Poll(t, 1*time.Second, true, func() interface{} {
		handler.mtx.Lock()
		defer handler.mtx.Unlock()
		return handler.stopped
	})

	// Complete the requests, by consuming their messages
	for i := 0; i < requests; i++ {
		require.False(t, stopped.Load(), "handler stopped too early")

		ri := <-inProgress
		t.Logf("received message from request %d", ri)
	}

	wg.Wait()
	t.Log("waiting on handler to stop")
	test.Poll(t, 1*time.Second, true, func() interface{} {
		return stopped.Load()
	})
}

func TestHandler_LogsFormattedQueryDetails(t *testing.T) {
	t1 := time.UnixMilli(1698421429219)
	t2 := t1.Add(time.Hour)

	for _, tt := range []struct {
		name                         string
		requestFormFields            []string
		setQueryDetails              func(*querymiddleware.QueryDetails)
		expectedLoggedFields         map[string]string
		expectedMissingFields        []string
		expectedApproximateDurations map[string]time.Duration
	}{
		{
			name:              "query_range",
			requestFormFields: []string{"start", "end", "step"},
			setQueryDetails: func(d *querymiddleware.QueryDetails) {
				d.Start, d.MinT = t1, t1.Add(-time.Minute)
				d.End, d.MaxT = t2, t2
				d.Step = time.Minute
			},
			expectedLoggedFields: map[string]string{
				"param_start": t1.Format(time.RFC3339Nano),
				"param_end":   t2.Format(time.RFC3339Nano),
				"param_step":  fmt.Sprint(time.Minute.Milliseconds()),
				"length":      "1h1m0s",
			},
			expectedApproximateDurations: map[string]time.Duration{},
		},
		{
			name:              "instant",
			requestFormFields: []string{"time"},
			setQueryDetails: func(d *querymiddleware.QueryDetails) {
				d.Start = t1
				d.End = t1
			},
			expectedLoggedFields: map[string]string{
				"param_time": t1.Format(time.RFC3339Nano),
			},
			expectedApproximateDurations: map[string]time.Duration{},
		},
		{
			// the details are used to figure out the length regardless of the user setting explicit or implicit time
			name:              "instant, missing time from request",
			requestFormFields: []string{},
			setQueryDetails: func(d *querymiddleware.QueryDetails) {
				d.Start = t1
				d.End = t1
			},
			expectedLoggedFields:         map[string]string{},
			expectedApproximateDurations: map[string]time.Duration{},
			expectedMissingFields:        []string{"param_time"},
		},
		{
			// the details aren't set by the query stats middleware if the request isn't a query
			name:                         "not a query request",
			requestFormFields:            []string{},
			setQueryDetails:              func(d *querymiddleware.QueryDetails) {},
			expectedLoggedFields:         map[string]string{},
			expectedApproximateDurations: map[string]time.Duration{},
			expectedMissingFields:        []string{"length", "param_time", "time_since_param_start", "time_since_param_end"},
		},
		{
			name:              "results cache statistics",
			requestFormFields: []string{},
			setQueryDetails: func(d *querymiddleware.QueryDetails) {
				d.ResultsCacheMissBytes = 10
				d.ResultsCacheHitBytes = 200
			},
			expectedLoggedFields: map[string]string{
				"results_cache_miss_bytes": "10",
				"results_cache_hit_bytes":  "200",
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			activityFile := filepath.Join(t.TempDir(), "activity-tracker")
			roundTripper := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				tt.setQueryDetails(querymiddleware.QueryDetailsFromContext(req.Context()))
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("{}")),
				}, nil
			})

			reg := prometheus.NewPedanticRegistry()

			at, err := activitytracker.NewActivityTracker(activitytracker.Config{Filepath: activityFile, MaxEntries: 1024}, reg)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, at.Close()) })

			logger := &testLogger{}
			handler := NewHandler(HandlerConfig{QueryStatsEnabled: true, MaxBodySize: 1024}, roundTripper, logger, reg, at)

			req := httptest.NewRequest(http.MethodPost, "/api/v1/query", nil)
			req = req.WithContext(user.InjectOrgID(context.Background(), "12345"))
			req.Form = map[string][]string{}
			for _, field := range tt.requestFormFields {
				req.Form.Set(field, "1") // this value doesn't matter because our assertions should run against what's in the query details
			}

			resp := httptest.NewRecorder()

			handler.ServeHTTP(resp, req)
			responseData, _ := io.ReadAll(resp.Body)
			require.Equal(t, resp.Code, http.StatusOK)
			require.Equal(t, []byte("{}"), responseData)

			require.Len(t, logger.logMessages, 1)

			msg := logger.logMessages[0]
			for field, expectedVal := range tt.expectedLoggedFields {
				assert.EqualValues(t, expectedVal, fmt.Sprint(msg[field]))
			}
			for _, expectedMissingVal := range tt.expectedMissingFields {
				assert.NotContains(t, msg, expectedMissingVal)
			}
			for field, expectedDuration := range tt.expectedApproximateDurations {
				actualDuration, err := time.ParseDuration(msg[field].(string))
				assert.NoError(t, err)
				assert.InDelta(t, expectedDuration, actualDuration, float64(time.Second))
			}
		})
	}
}

func TestHandler_ActiveSeriesWriteTimeout(t *testing.T) {
	const serverWriteTimeout = 50 * time.Millisecond
	const activeSeriesWriteTimeout = 150 * time.Millisecond

	for _, tt := range []struct {
		name            string
		path            string
		requestDuration time.Duration
		wantError       bool
		wantCtxCancel   bool
	}{
		{
			name:            "deadline exceeded for non-streaming endpoint",
			path:            "/api/v1/query",
			requestDuration: 100 * time.Millisecond,
			wantError:       true,
		},
		{
			name:            "deadline not exceeded for active series endpoint",
			path:            "/api/v1/cardinality/active_series",
			requestDuration: 100 * time.Millisecond,
		},
		{
			name:            "deadline exceeded for active series endpoint",
			path:            "/api/v1/cardinality/active_series",
			requestDuration: 200 * time.Millisecond,
			wantError:       true,
			wantCtxCancel:   true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {

			roundTripper := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				// Simulate a request that takes longer than the server write timeout.
				select {
				case <-req.Context().Done():
					assert.EqualError(t, context.Cause(req.Context()),
						fmt.Sprintf("context canceled: write deadline exceeded (timeout: %v)", activeSeriesWriteTimeout))
					return nil, req.Context().Err()
				case <-time.After(tt.requestDuration):
					if tt.wantCtxCancel {
						assert.Fail(t, "request context should have been cancelled")
					}
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
				}
			})

			handler := NewHandler(
				HandlerConfig{ActiveSeriesWriteTimeout: activeSeriesWriteTimeout},
				roundTripper, log.NewNopLogger(), nil, nil,
			)

			server := httptest.NewUnstartedServer(handler)
			server.Config.WriteTimeout = serverWriteTimeout
			server.Start()
			defer server.Close()

			req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s%s", server.URL, tt.path), nil)
			require.NoError(t, err)
			resp, err := http.DefaultClient.Do(req)
			if tt.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			defer func() {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}()
		})
	}
}

type testLogger struct {
	logMessages []map[string]interface{}
}

func (t *testLogger) Log(keyvals ...interface{}) error {
	if len(keyvals)%2 != 0 {
		panic("received uneven number of key/value pairs for log line")
	}

	entryCount := len(keyvals) / 2
	msg := make(map[string]interface{}, entryCount)

	for i := 0; i < entryCount; i++ {
		name := keyvals[2*i].(string)
		value := keyvals[2*i+1]

		msg[name] = value
	}

	t.logMessages = append(t.logMessages, msg)
	return nil
}

func TestFormatRequestHeaders(t *testing.T) {
	h := http.Header{}
	h.Add("X-Header-To-Log", "i should be logged!")
	h.Add("X-Header-To-Not-Log", "i shouldn't be logged!")

	fields := formatRequestHeaders(&h, []string{"X-Header-To-Log", "X-Header-Not-Present"})

	expected := []interface{}{
		"header_x_header_to_log",
		"i should be logged!",
	}

	assert.Equal(t, expected, fields)
}
