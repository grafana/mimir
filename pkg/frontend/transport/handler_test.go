// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/frontend/transport/handler_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package transport

import (
	"bytes"
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
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	apierror "github.com/grafana/mimir/pkg/api/error"
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
		{http.StatusGatewayTimeout, errors.Wrap(context.DeadlineExceeded, "an error occurred")},
		{StatusClientClosedRequest, context.Canceled},
		{StatusClientClosedRequest, errors.Wrap(context.Canceled, "an error occurred")},
		{http.StatusBadRequest, httpgrpc.Errorf(http.StatusBadRequest, "")},
		{http.StatusBadRequest, errors.Wrap(httpgrpc.Errorf(http.StatusBadRequest, ""), "an error occurred")},
		{http.StatusBadRequest, apierror.New(apierror.TypeBadData, "")},
		{http.StatusBadRequest, errors.Wrap(apierror.New(apierror.TypeBadData, "invalid request"), "an error occurred")},
		{http.StatusNotFound, apierror.New(apierror.TypeNotFound, "")},
		{http.StatusNotFound, errors.Wrap(apierror.New(apierror.TypeNotFound, "invalid request"), "an error occurred")},
	} {
		t.Run(test.err.Error(), func(t *testing.T) {
			w := httptest.NewRecorder()
			require.Equal(t, test.status, writeError(w, test.err))
			require.Equal(t, test.status, w.Result().StatusCode)
		})
	}
}

func TestHandler_ServeHTTP(t *testing.T) {
	const testRouteName = "the_test_route"

	makeSuccessfulDownstreamResponse := func() *http.Response {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("{}")),
		}
	}

	for _, tt := range []struct {
		name                    string
		cfg                     HandlerConfig
		request                 func() *http.Request
		downstreamResponse      *http.Response
		downstreamErr           error
		expectedStatusCode      int
		expectedParams          url.Values
		expectedMetrics         int
		expectedActivity        string
		expectedReadConsistency string
		assertHeaders           func(t *testing.T, headers http.Header)
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
			downstreamResponse: makeSuccessfulDownstreamResponse(),
			expectedStatusCode: 200,
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
			downstreamResponse: makeSuccessfulDownstreamResponse(),
			expectedStatusCode: 200,
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
				return r.WithContext(api.ContextWithReadConsistencyLevel(context.Background(), api.ReadConsistencyStrong))
			},
			downstreamResponse: makeSuccessfulDownstreamResponse(),
			expectedStatusCode: 200,
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
			downstreamResponse:      makeSuccessfulDownstreamResponse(),
			expectedStatusCode:      200,
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
			downstreamResponse: makeSuccessfulDownstreamResponse(),
			expectedStatusCode: 200,
			expectedParams: url.Values{
				"query": []string{"some_metric"},
				"time":  []string{"42"},
			},
			expectedMetrics:         0,
			expectedActivity:        "user:12345 UA:test-user-agent req:GET /api/v1/query query=some_metric&time=42",
			expectedReadConsistency: "",
		},
		{
			name: "handler with stats enabled, serving remote read query with snappy compression",
			cfg:  HandlerConfig{QueryStatsEnabled: true, MaxBodySize: 1024},
			request: func() *http.Request {
				r := httptest.NewRequest("GET", "/api/v1/read", nil)
				r.Header.Add("User-Agent", "test-user-agent")
				r.Header.Add("Content-Type", "application/x-protobuf")
				remoteReadRequest := &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Name: "__name__", Type: prompb.LabelMatcher_EQ, Value: "some_metric"},
								{Name: "foo", Type: prompb.LabelMatcher_RE, Value: ".*bar.*"},
							},
							StartTimestampMs: 0,
							EndTimestampMs:   42,
						},
						{
							Matchers: []*prompb.LabelMatcher{
								{Name: "__name__", Type: prompb.LabelMatcher_EQ, Value: "up"},
							},
							StartTimestampMs: 10,
							EndTimestampMs:   20,
							Hints: &prompb.ReadHints{
								StepMs: 1000,
							},
						},
					},
				}
				data, _ := proto.Marshal(remoteReadRequest) // Ignore error, if this fails, the test will fail.
				r.Header.Add("Content-Encoding", "snappy")
				compressed := snappy.Encode(nil, data)
				r.Body = io.NopCloser(bytes.NewReader(compressed))
				return r
			},
			downstreamResponse: makeSuccessfulDownstreamResponse(),
			expectedActivity:   "user:12345 UA:test-user-agent req:GET /api/v1/read end_0=42&end_1=20&hints_1=%7B%22step_ms%22%3A1000%7D&matchers_0=%7B__name__%3D%22some_metric%22%2Cfoo%3D~%22.%2Abar.%2A%22%7D&matchers_1=%7B__name__%3D%22up%22%7D&start_0=0&start_1=10",
			expectedMetrics:    5,
			expectedStatusCode: 200,
			expectedParams: url.Values{
				"matchers_0": []string{"{__name__=\"some_metric\",foo=~\".*bar.*\"}"},
				"start_0":    []string{"0"},
				"end_0":      []string{"42"},
				"matchers_1": []string{"{__name__=\"up\"}"},
				"start_1":    []string{"10"},
				"end_1":      []string{"20"},
				"hints_1":    []string{"{\"step_ms\":1000}"},
			},
		},
		{
			name: "downstream returns an apierror with 4xx status code",
			cfg:  HandlerConfig{QueryStatsEnabled: true},
			request: func() *http.Request {
				return httptest.NewRequest("GET", "/api/v1/query?query=some_metric&time=42", nil)
			},
			downstreamErr:      apierror.New(apierror.TypeBadData, "invalid request"),
			expectedStatusCode: 400,
			expectedParams: url.Values{
				"query": []string{"some_metric"},
				"time":  []string{"42"},
			},
			expectedMetrics:         5,
			expectedActivity:        "user:12345 UA: req:GET /api/v1/query query=some_metric&time=42",
			expectedReadConsistency: "",
		},
		{
			name: "downstream returns a gRPC error with 4xx status code",
			cfg:  HandlerConfig{QueryStatsEnabled: true},
			request: func() *http.Request {
				return httptest.NewRequest("GET", "/api/v1/query?query=some_metric&time=42", nil)
			},
			downstreamErr:      httpgrpc.Errorf(http.StatusBadRequest, "invalid request"),
			expectedStatusCode: 400,
			expectedParams: url.Values{
				"query": []string{"some_metric"},
				"time":  []string{"42"},
			},
			expectedMetrics:         5,
			expectedActivity:        "user:12345 UA: req:GET /api/v1/query query=some_metric&time=42",
			expectedReadConsistency: "",
		},
		{
			name: "downstream returns a generic error",
			cfg:  HandlerConfig{QueryStatsEnabled: true},
			request: func() *http.Request {
				return httptest.NewRequest("GET", "/api/v1/query?query=some_metric&time=42", nil)
			},
			downstreamErr:      errors.New("something unexpected happened"),
			expectedStatusCode: 500,
			expectedParams: url.Values{
				"query": []string{"some_metric"},
				"time":  []string{"42"},
			},
			expectedMetrics:         5,
			expectedActivity:        "user:12345 UA: req:GET /api/v1/query query=some_metric&time=42",
			expectedReadConsistency: "",
		},
		{
			name: "handler with stats enabled, check ServiceTimingHeader",
			cfg:  HandlerConfig{QueryStatsEnabled: true, MaxBodySize: 1024},
			request: func() *http.Request {
				req := httptest.NewRequest(http.MethodPost, "/api/v1/query", strings.NewReader("query=some_metric&time=42"))
				req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
				return req
			},
			downstreamResponse: makeSuccessfulDownstreamResponse(),
			expectedStatusCode: 200,
			expectedParams: url.Values{
				"query": []string{"some_metric"},
				"time":  []string{"42"},
			},
			expectedMetrics:         5,
			expectedActivity:        "user:12345 UA: req:POST /api/v1/query query=some_metric&time=42",
			expectedReadConsistency: "",
			assertHeaders: func(t *testing.T, headers http.Header) {
				assert.Contains(t, headers.Get(ServiceTimingHeaderName), "bytes_processed=0")
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			activityFile := filepath.Join(t.TempDir(), "activity-tracker")

			roundTripper := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				activities, err := activitytracker.LoadUnfinishedEntries(activityFile)
				assert.NoError(t, err)
				assert.Len(t, activities, 1)
				assert.Equal(t, tt.expectedActivity, activities[0].Activity)

				if req.Header.Get("Content-Type") != "application/x-protobuf" {
					assert.NoError(t, req.ParseForm())
					assert.Equal(t, tt.expectedParams, req.Form)
				}

				return tt.downstreamResponse, tt.downstreamErr
			})

			reg := prometheus.NewPedanticRegistry()

			at, err := activitytracker.NewActivityTracker(activitytracker.Config{Filepath: activityFile, MaxEntries: 1024}, reg)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, at.Close()) })

			logger := &testLogger{}
			handler := NewHandler(tt.cfg, roundTripper, logger, reg, at)

			req := tt.request()
			req = req.WithContext(user.InjectOrgID(req.Context(), "12345"))
			req = middleware.WithRouteName(req, testRouteName)
			resp := httptest.NewRecorder()

			handler.ServeHTTP(resp, req)
			responseData, _ := io.ReadAll(resp.Body)
			require.Equal(t, tt.expectedStatusCode, resp.Code)

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
				require.EqualValues(t, tt.expectedStatusCode, msg["status_code"])
				require.Equal(t, "12345", msg["user"])
				require.Equal(t, req.Method, msg["method"])
				require.Equal(t, req.URL.Path, msg["path"])
				require.Equal(t, testRouteName, msg["route_name"])
				require.Equal(t, req.UserAgent(), msg["user_agent"])
				require.Contains(t, msg, "response_time")
				require.Contains(t, msg, "query_wall_time_seconds")
				require.EqualValues(t, 0, msg["fetched_series_count"])
				require.EqualValues(t, 0, msg["fetched_chunk_bytes"])
				require.EqualValues(t, 0, msg["fetched_chunks_count"])
				require.EqualValues(t, 0, msg["fetched_index_bytes"])
				require.EqualValues(t, 0, msg["sharded_queries"])
				require.EqualValues(t, 0, msg["split_queries"])
				require.EqualValues(t, 0, msg["estimated_series_count"])
				require.EqualValues(t, 0, msg["queue_time_seconds"])

				if tt.expectedStatusCode >= 200 && tt.expectedStatusCode < 300 {
					require.Equal(t, "success", msg["status"])
				} else {
					require.Equal(t, "failed", msg["status"])
				}

				// The response size is tracked only for successful requests.
				if tt.expectedStatusCode >= 200 && tt.expectedStatusCode < 300 {
					require.Equal(t, int64(len(responseData)), msg["response_size_bytes"])
				}
				if tt.assertHeaders != nil {
					tt.assertHeaders(t, resp.Header())
				}

				// Check that the HTTP or Protobuf request parameters are logged.
				paramsLogged := 0
				for key := range msg {
					if strings.HasPrefix(key, "param_") {
						paramsLogged++
					}
				}
				require.Equal(t, len(tt.expectedParams), paramsLogged)
				for key, value := range tt.expectedParams {
					require.Equal(t, value[0], msg["param_"+key])
				}

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
	roundTripper := roundTripperFunc(func(*http.Request) (*http.Response, error) {
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
		requestAdditionalHeaders     map[string]string
		logQueryRequestHeaders       []string
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
			setQueryDetails:              func(*querymiddleware.QueryDetails) {},
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
				"header_cache_control":     "",
			},
		},
		{
			name:              "results cache turned off on request",
			requestFormFields: []string{},
			requestAdditionalHeaders: map[string]string{
				"Cache-Control": "no-store",
			},
			setQueryDetails: func(d *querymiddleware.QueryDetails) {
				d.ResultsCacheMissBytes = 200
				d.ResultsCacheHitBytes = 0
			},
			expectedLoggedFields: map[string]string{
				"results_cache_miss_bytes": "200",
				"results_cache_hit_bytes":  "0",
				"header_cache_control":     "no-store",
			},
		},
		{
			name:              "header logging cache control header not logged twice upper case",
			requestFormFields: []string{},
			requestAdditionalHeaders: map[string]string{
				"Cache-Control": "no-store",
				"X-Form-ID":     "12345",
			},
			logQueryRequestHeaders: []string{"Cache-Control", "X-Form-ID"},
			setQueryDetails: func(d *querymiddleware.QueryDetails) {
				d.ResultsCacheMissBytes = 200
				d.ResultsCacheHitBytes = 0
			},
			expectedLoggedFields: map[string]string{
				"results_cache_miss_bytes": "200",
				"results_cache_hit_bytes":  "0",
				"header_cache_control":     "no-store",
				"header_x_form_id":         "12345",
			},
		},
		{
			name:              "header logging cache control header not logged twice - lower case",
			requestFormFields: []string{},
			requestAdditionalHeaders: map[string]string{
				"Cache-Control": "no-store",
				"x-form-id":     "12345",
			},
			logQueryRequestHeaders: []string{"cache-control", "X-Form-ID"},
			setQueryDetails: func(d *querymiddleware.QueryDetails) {
				d.ResultsCacheMissBytes = 200
				d.ResultsCacheHitBytes = 0
			},
			expectedLoggedFields: map[string]string{
				"results_cache_miss_bytes": "200",
				"results_cache_hit_bytes":  "0",
				"header_cache_control":     "no-store",
				"header_x_form_id":         "12345",
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
			handler := NewHandler(HandlerConfig{QueryStatsEnabled: true, MaxBodySize: 1024, LogQueryRequestHeaders: tt.logQueryRequestHeaders}, roundTripper, logger, reg, at)

			req := httptest.NewRequest(http.MethodPost, "/api/v1/query", nil)
			for header, value := range tt.requestAdditionalHeaders {
				req.Header.Add(header, value)
			}
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
			require.Empty(t, logger.duplicates)

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
	duplicates  []string
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
		if _, ok := msg[name]; ok {
			t.duplicates = append(t.duplicates, name)
		}
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
		"header_cache_control",
		"",
		"header_x_header_to_log",
		"i should be logged!",
	}

	assert.Equal(t, expected, fields)
}
