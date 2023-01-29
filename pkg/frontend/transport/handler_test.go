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
	"testing"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

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
		name             string
		cfg              HandlerConfig
		request          func() *http.Request
		expectedParams   url.Values
		expectedMetrics  int
		expectedActivity string
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
			expectedMetrics:  4,
			expectedActivity: "12345 POST /api/v1/query query=some_metric&time=42",
		},
		{
			name: "handler with stats enabled, GET request with params",
			cfg:  HandlerConfig{QueryStatsEnabled: true},
			request: func() *http.Request {
				return httptest.NewRequest("GET", "/api/v1/query?query=some_metric&time=42", nil)
			},
			expectedParams: url.Values{
				"query": []string{"some_metric"},
				"time":  []string{"42"},
			},
			expectedMetrics:  4,
			expectedActivity: "12345 GET /api/v1/query query=some_metric&time=42",
		},
		{
			name: "handler with stats enabled, GET request without params",
			cfg:  HandlerConfig{QueryStatsEnabled: true},
			request: func() *http.Request {
				return httptest.NewRequest("GET", "/api/v1/query", nil)
			},
			expectedParams:   url.Values{},
			expectedMetrics:  4,
			expectedActivity: "12345 GET /api/v1/query (no params)",
		},
		{
			name: "handler with stats disabled, GET request with params",
			cfg:  HandlerConfig{QueryStatsEnabled: false},
			request: func() *http.Request {
				return httptest.NewRequest("GET", "/api/v1/query?query=some_metric&time=42", nil)
			},
			expectedParams: url.Values{
				"query": []string{"some_metric"},
				"time":  []string{"42"},
			},
			expectedMetrics:  0,
			expectedActivity: "12345 GET /api/v1/query query=some_metric&time=42",
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

			req := tt.request().WithContext(user.InjectOrgID(context.Background(), "12345"))
			resp := httptest.NewRecorder()

			handler.ServeHTTP(resp, req)
			_, _ = io.ReadAll(resp.Body)
			require.Equal(t, resp.Code, http.StatusOK)

			count, err := promtest.GatherAndCount(
				reg,
				"cortex_query_seconds_total",
				"cortex_query_fetched_series_total",
				"cortex_query_fetched_chunk_bytes_total",
				"cortex_query_fetched_chunks_total",
			)

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedMetrics, count)

			activities, err := activitytracker.LoadUnfinishedEntries(activityFile)
			require.NoError(t, err)
			require.Empty(t, activities)

			if tt.cfg.QueryStatsEnabled {
				require.Len(t, logger.logMessages, 1)

				msg := logger.logMessages[0]
				require.Len(t, msg, 16+len(tt.expectedParams))
				require.Equal(t, level.InfoValue(), msg["level"])
				require.Equal(t, "query stats", msg["msg"])
				require.Equal(t, "query-frontend", msg["component"])
				require.Equal(t, "success", msg["status"])
				require.Equal(t, "12345", msg["user"])
				require.Equal(t, req.Method, msg["method"])
				require.Equal(t, req.URL.Path, msg["path"])
				require.Equal(t, req.UserAgent(), msg["user_agent"])
				require.Contains(t, msg, "response_time")
				require.Contains(t, msg, "query_wall_time_seconds")
				require.EqualValues(t, 0, msg["fetched_series_count"])
				require.EqualValues(t, 0, msg["fetched_chunk_bytes"])
				require.EqualValues(t, 0, msg["fetched_chunks_count"])
				require.EqualValues(t, 0, msg["fetched_index_bytes"])
				require.EqualValues(t, 0, msg["sharded_queries"])
				require.EqualValues(t, 0, msg["split_queries"])

				for name, values := range tt.expectedParams {
					logMessageKey := fmt.Sprintf("param_%v", name)
					expectedValues := strings.Join(values, ",")
					require.Equal(t, expectedValues, msg[logMessageKey])
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
		expectedMetrics     int
		path                string
		expectQueryParamLog bool
		queryErr            error
	}{
		{
			name:                "Failed round trip with context cancelled",
			cfg:                 HandlerConfig{QueryStatsEnabled: false},
			expectedMetrics:     0,
			path:                "/api/v1/query?query=up&time=2015-07-01T20:10:51.781Z",
			expectQueryParamLog: true,
			queryErr:            context.Canceled,
		},
		{
			name:                "Failed round trip with no query params",
			cfg:                 HandlerConfig{QueryStatsEnabled: true},
			expectedMetrics:     4,
			path:                "/api/v1/query",
			expectQueryParamLog: false,
			queryErr:            context.Canceled,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			roundTripper := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				return nil, test.queryErr
			})

			reg := prometheus.NewPedanticRegistry()
			logs := &concurrency.SyncBuffer{}
			logger := log.NewLogfmtLogger(logs)
			handler := NewHandler(test.cfg, roundTripper, logger, reg, nil)

			ctx := user.InjectOrgID(context.Background(), "12345")
			req := httptest.NewRequest("GET", test.path, nil)
			req = req.WithContext(ctx)
			resp := httptest.NewRecorder()

			handler.ServeHTTP(resp, req)
			require.Equal(t, StatusClientClosedRequest, resp.Code)

			count, err := promtest.GatherAndCount(
				reg,
				"cortex_query_seconds_total",
				"cortex_query_fetched_series_total",
				"cortex_query_fetched_chunk_bytes_total",
				"cortex_query_fetched_chunks_total",
			)

			require.NoError(t, err)

			assert.Contains(t, strings.TrimSpace(logs.String()), "sharded_queries")
			assert.Contains(t, strings.TrimSpace(logs.String()), "status=canceled")
			if test.expectQueryParamLog {
				assert.Contains(t, strings.TrimSpace(logs.String()), "param_query")
			}
			assert.Equal(t, test.expectedMetrics, count)
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
