// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gorilla/mux"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/server"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/querier/tenantfederation"
	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/propagation"
	"github.com/grafana/mimir/pkg/util/validation"
)

type costTestQueryable struct {
	storage.SampleAndChunkQueryable
	querier storage.Querier
}

func (q costTestQueryable) Querier(int64, int64) (storage.Querier, error) { return q.querier, nil }

type costTestQuerier struct {
	storage.MockQuerier
	selectFn func(context.Context, *storage.SelectHints, ...*labels.Matcher) storage.SeriesSet
	closed   bool
}

func (q *costTestQuerier) Select(ctx context.Context, _ bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return q.selectFn(ctx, hints, matchers...)
}

func (q *costTestQuerier) Close() error { q.closed = true; return nil }

func newCostTestHandler(t *testing.T, cfg querier.Config, q storage.Querier, engine promql.QueryEngine) http.Handler {
	t.Helper()
	reg := prometheus.NewRegistry()
	var defaults validation.Limits
	flagext.DefaultValues(&defaults)
	limits := validation.NewOverrides(defaults, nil)
	return NewQuerierHandler(Config{ServerPrefix: "/mimir", PrometheusHTTPPrefix: "/prometheus"}, cfg,
		costTestQueryable{querier: q}, nil, nil, engine, nil, querier.NewRequestMetrics(reg), reg, log.NewNopLogger(), limits, &propagation.NoopExtractor{})
}

func TestQueryCostAPI(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		for _, tc := range []struct {
			name, endpoint, query string
			extra                 url.Values
			samples               int
			start, end            int64
		}{
			{name: "instant", endpoint: "query_cost", query: "up", samples: 2, start: 300001, end: 600000},
			{name: "range selector", endpoint: "query_cost", query: "rate(up[5m])", samples: 22, start: 300001, end: 600000},
			{name: "range query", endpoint: "query_range_cost", query: "up", samples: 22, start: 300001, end: 1200000},
			{name: "lookback override", endpoint: "query_cost", query: "up", extra: url.Values{"lookback_delta": {"120"}}, samples: 2, start: 480001, end: 600000},
			{name: "offset", endpoint: "query_cost", query: "up offset 1m", samples: 2, start: 240001, end: 540000},
			{name: "fixed timestamp", endpoint: "query_cost", query: "up @ 300", samples: 2, start: 1, end: 300000},
			{name: "subquery default step", endpoint: "query_cost", query: "max_over_time(up[2m:])", samples: 12, start: 180001, end: 600000},
		} {
			t.Run(method+"/"+tc.name, func(t *testing.T) {
				var cfg querier.Config
				flagext.DefaultValues(&cfg)
				cfg.ExperimentalQueryCostAPIEnabled = true
				cfg.QueryCostEstimatedScrapeInterval = 30 * time.Second
				cfg.EngineConfig.DefaultEvaluationInterval = 20 * time.Second
				selected := false
				q := &costTestQuerier{selectFn: func(ctx context.Context, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
					selected = true
					require.Equal(t, "tenant-a", mustTenant(t, ctx))
					_, err := limiter.MemoryConsumptionTrackerFromContext(ctx)
					require.NoError(t, err)
					require.Equal(t, "series", hints.Func)
					require.Equal(t, tc.start, hints.Start)
					require.Equal(t, tc.end, hints.End)
					require.Len(t, matchers, 1)
					require.Equal(t, "up", matchers[0].Value)
					return series.NewConcreteSeriesSetFromSortedSeries([]storage.Series{
						storage.NewListSeries(labels.FromStrings("__name__", "up", "job", "a"), nil),
						storage.NewListSeries(labels.FromStrings("__name__", "up", "job", "b"), nil),
					})
				}}
				// A nil engine ensures that neither query construction nor execution is used.
				handler := newCostTestHandler(t, cfg, q, nil)
				params := url.Values{"query": {tc.query}, "time": {"600"}, "start": {"600"}, "end": {"1200"}, "step": {"60"}}
				for key, value := range tc.extra {
					params[key] = value
				}
				req := costTestRequest(t, method, tc.endpoint, params)
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				var response struct {
					Status   string          `json:"status"`
					Data     json.RawMessage `json:"data"`
					Warnings []string        `json:"warnings"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
				require.Equal(t, "success", response.Status)
				expected, err := json.Marshal(map[string]any{"estimate": map[string]int{"seriesTouched": 2, "samplesRead": tc.samples}})
				require.NoError(t, err)
				require.JSONEq(t, string(expected), string(response.Data))
				require.Equal(t, []string{"query cost estimate uses the configured scrape interval and float-sized points because bounded storage sampling is unavailable"}, response.Warnings)
				require.True(t, selected)
				require.True(t, q.closed)
			})
		}
	}
}

func mustTenant(t *testing.T, ctx context.Context) string {
	t.Helper()
	id, err := user.ExtractOrgID(ctx)
	require.NoError(t, err)
	return id
}

func costTestRequest(t *testing.T, method, endpoint string, params url.Values) *http.Request {
	t.Helper()
	target := "/mimir/prometheus/api/v1/" + endpoint
	var req *http.Request
	if method == http.MethodPost {
		req = httptest.NewRequest(method, target, strings.NewReader(params.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	} else {
		req = httptest.NewRequest(method, target+"?"+params.Encode(), nil)
	}
	return req.WithContext(user.InjectOrgID(t.Context(), "tenant-a"))
}

func TestQueryCostAPIValidation(t *testing.T) {
	for _, endpoint := range []string{"query_cost", "query_range_cost"} {
		for _, tc := range []struct{ name, key, value, errorText string }{
			{"syntax", "query", "sum(", "invalid parameter \"query\""},
			{"timeout", "timeout", "0", "timeout must be positive"},
			{"lookback", "lookback_delta", "bad", "error parsing lookback delta duration"},
			{"limit", "limit", "-1", "invalid parameter \"limit\""},
			{"cost", "cost", "bad", "invalid parameter \"cost\""},
			{"duration ceiling", "max_query_duration", "3m", "exceeds"},
		} {
			t.Run(endpoint+"/"+tc.name, func(t *testing.T) {
				var cfg querier.Config
				flagext.DefaultValues(&cfg)
				cfg.ExperimentalQueryCostAPIEnabled = true
				handler := newCostTestHandler(t, cfg, nil, nil)
				params := url.Values{"query": {"up"}, "start": {"600"}, "end": {"1200"}, "step": {"60"}}
				params.Set(tc.key, tc.value)
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, costTestRequest(t, http.MethodGet, endpoint, params))
				require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
				var response struct {
					ErrorType string `json:"errorType"`
					Error     string `json:"error"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
				require.Equal(t, "bad_data", response.ErrorType)
				require.Contains(t, response.Error, tc.errorText)
			})
		}
	}
}

func TestQueryCostAPIDisabled(t *testing.T) {
	var cfg querier.Config
	flagext.DefaultValues(&cfg)
	handler := newCostTestHandler(t, cfg, nil, nil)
	for _, endpoint := range []string{"query_cost", "query_range_cost"} {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, costTestRequest(t, method, endpoint, url.Values{"query": {"up"}}))
			require.Equal(t, http.StatusNotFound, rec.Code)
		}
	}
}

func TestQueryCostAPIErrors(t *testing.T) {
	for _, tc := range []struct {
		name       string
		timeout    time.Duration
		params     url.Values
		storageErr error
		code       int
		errorType  string
	}{
		{name: "configured timeout", timeout: time.Millisecond, code: 503, errorType: "timeout"},
		{name: "request timeout", timeout: time.Minute, params: url.Values{"timeout": {"0.001"}}, code: 503, errorType: "timeout"},
		{name: "duration override", timeout: time.Minute, params: url.Values{"max_query_duration": {"0.001"}}, code: 503, errorType: "timeout"},
		{name: "storage failure", timeout: time.Minute, storageErr: errors.New("storage unavailable"), code: 500, errorType: "internal"},
		{name: "canceled", timeout: time.Minute, storageErr: context.Canceled, code: 499, errorType: "canceled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg querier.Config
			flagext.DefaultValues(&cfg)
			cfg.ExperimentalQueryCostAPIEnabled = true
			cfg.EngineConfig.Timeout = tc.timeout
			q := &costTestQuerier{selectFn: func(ctx context.Context, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
				if tc.storageErr != nil {
					return storage.ErrSeriesSet(tc.storageErr)
				}
				<-ctx.Done()
				return storage.ErrSeriesSet(ctx.Err())
			}}
			handler := newCostTestHandler(t, cfg, q, nil)
			params := tc.params
			if params == nil {
				params = url.Values{}
			}
			params.Set("query", "up")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, costTestRequest(t, http.MethodGet, "query_cost", params))
			require.Equal(t, tc.code, rec.Code, rec.Body.String())
			var response struct {
				ErrorType string          `json:"errorType"`
				Data      json.RawMessage `json:"data"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
			require.Equal(t, tc.errorType, response.ErrorType)
			require.Empty(t, response.Data)
			require.True(t, q.closed)
		})
	}
}

func TestQueryCostEngineConcurrency(t *testing.T) {
	engine := &queryCostEngine{slots: make(chan struct{}, 1)}
	release, err := engine.AcquireQuerySlot(t.Context(), "up")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	noRelease, err := engine.AcquireQuerySlot(ctx, "up")
	noRelease()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	release()
	release, err = engine.AcquireQuerySlot(t.Context(), "up")
	require.NoError(t, err)
	release()
}

func TestQueryCostPublicRoutes(t *testing.T) {
	var cfg Config
	flagext.DefaultValues(&cfg)
	srv := &server.Server{HTTP: mux.NewRouter()}
	api, err := New(cfg, tenantfederation.Config{}, server.Config{}, srv, log.NewNopLogger(), nil)
	require.NoError(t, err)
	calls := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		require.Equal(t, "tenant-a", mustTenant(t, r.Context()))
		w.WriteHeader(http.StatusNoContent)
	})
	api.RegisterQueryAPI(handler, nil, 16)
	for _, endpoint := range []string{"query_cost", "query_range_cost"} {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			for _, authenticated := range []bool{false, true} {
				req := httptest.NewRequest(method, "/prometheus/api/v1/"+endpoint, nil)
				if authenticated {
					req.Header.Set("X-Scope-OrgID", "tenant-a")
				}
				rec := httptest.NewRecorder()
				srv.HTTP.ServeHTTP(rec, req)
				if authenticated {
					require.Equal(t, http.StatusNoContent, rec.Code)
				} else {
					require.Equal(t, http.StatusUnauthorized, rec.Code)
				}
			}
		}
	}
	require.Equal(t, 4, calls)
}

func TestQueryCostAPIDoesNotEnableExecutionCost(t *testing.T) {
	var cfg querier.Config
	flagext.DefaultValues(&cfg)
	cfg.ExperimentalQueryCostAPIEnabled = true
	engine := promql.NewEngine(promql.EngineOpts{Timeout: time.Minute, MaxSamples: 100, LookbackDelta: 5 * time.Minute})
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	handler := newCostTestHandler(t, cfg, &storage.MockQuerier{}, engine)
	for _, endpoint := range []string{"query", "query_range"} {
		params := url.Values{"query": {"1"}, "time": {"600"}, "start": {"600"}, "end": {"600"}, "step": {"60"}}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, costTestRequest(t, http.MethodGet, endpoint, params))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		baseline := rec.Body.String()
		params.Set("cost", "true")
		params.Set("max_series", "not-a-number")
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, costTestRequest(t, http.MethodGet, endpoint, params))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.JSONEq(t, baseline, rec.Body.String())
	}
}

func TestQueryCostAPINonpositiveTimeout(t *testing.T) {
	for _, tc := range []struct {
		name         string
		engine       string
		timeout      time.Duration
		params       url.Values
		wantTimeout  bool
		wantDeadline bool
	}{
		{name: "Prometheus negative", engine: querier.PrometheusEngine, timeout: -time.Second, wantTimeout: true},
		{name: "Prometheus zero", engine: querier.PrometheusEngine, wantTimeout: true},
		{name: "Prometheus positive", engine: querier.PrometheusEngine, timeout: time.Minute, wantDeadline: true},
		{name: "Prometheus zero with request timeout", engine: querier.PrometheusEngine, params: url.Values{"timeout": {"15s"}}, wantTimeout: true},
		{name: "Prometheus zero with duration override", engine: querier.PrometheusEngine, params: url.Values{"max_query_duration": {"15s"}}, wantTimeout: true},
		{name: "MQE negative", engine: querier.MimirEngine, timeout: -time.Second, wantTimeout: true},
		{name: "MQE zero", engine: querier.MimirEngine},
		{name: "MQE positive", engine: querier.MimirEngine, timeout: time.Minute, wantDeadline: true},
		{name: "MQE zero with request timeout", engine: querier.MimirEngine, params: url.Values{"timeout": {"15s"}}, wantDeadline: true},
		{name: "MQE zero with duration override", engine: querier.MimirEngine, params: url.Values{"max_query_duration": {"15s"}}, wantDeadline: true},
		{name: "MQE negative with request timeout", engine: querier.MimirEngine, timeout: -time.Second, params: url.Values{"timeout": {"15s"}}, wantTimeout: true},
	} {
		for _, endpoint := range []string{"query_cost", "query_range_cost"} {
			t.Run(tc.name+"/"+endpoint, func(t *testing.T) {
				var cfg querier.Config
				flagext.DefaultValues(&cfg)
				cfg.ExperimentalQueryCostAPIEnabled = true
				cfg.QueryEngine = tc.engine
				cfg.EngineConfig.Timeout = tc.timeout
				require.NoError(t, cfg.Validate())

				selected := false
				q := &costTestQuerier{selectFn: func(ctx context.Context, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
					selected = true
					_, hasDeadline := ctx.Deadline()
					require.Equal(t, tc.wantDeadline, hasDeadline)
					require.NoError(t, ctx.Err())
					return storage.EmptySeriesSet()
				}}
				handler := newCostTestHandler(t, cfg, q, nil)
				params := url.Values{"query": {"up"}, "time": {"600"}, "start": {"600"}, "end": {"1200"}, "step": {"60"}}
				for key, values := range tc.params {
					params[key] = values
				}
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, costTestRequest(t, http.MethodGet, endpoint, params))

				if tc.wantTimeout {
					require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
					require.JSONEq(t, `{"status":"error","errorType":"timeout","error":"cost estimation exceeded the query duration limit: context deadline exceeded"}`, rec.Body.String())
					require.False(t, selected, "an expired query must not enumerate storage")
				} else {
					require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
					require.JSONEq(t, `{"status":"success","data":{"estimate":{"seriesTouched":0,"samplesRead":0}}}`, rec.Body.String())
					require.True(t, selected)
					require.True(t, q.closed)
				}
			})
		}
	}
}

func TestQueryCostEngineCanceledContext(t *testing.T) {
	for _, timeoutImmediately := range []bool{false, true} {
		engine := &queryCostEngine{slots: make(chan struct{}, 1), timeoutImmediately: timeoutImmediately}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		release, err := engine.AcquireQuerySlot(ctx, "up")
		release()
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, engine.slots)
	}
}
