// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/roundtrip_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/mimirpb"
	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestTripperware_RangeQuery(t *testing.T) {
	var (
		query        = "/api/v1/query_range?end=1536716880&query=sum+by+%28namespace%29+%28container_memory_rss%29&start=1536673680&step=120"
		responseBody = `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[1536673680,"137"],[1536673780,"137"]]}]}}`
	)

	s := httptest.NewServer(
		middleware.AuthenticateUser.Wrap(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var err error
				if r.RequestURI == query {
					w.Header().Set("Content-Type", jsonMimeType)
					_, err = w.Write([]byte(responseBody))
				} else {
					_, err = w.Write([]byte("bar"))
				}
				if err != nil {
					t.Fatal(err)
				}
			}),
		),
	)
	defer s.Close()

	u, err := url.Parse(s.URL)
	require.NoError(t, err)

	downstream := singleHostRoundTripper{
		host: u.Host,
		next: http.DefaultTransport,
	}

	tw, err := NewTripperware(
		Config{},
		log.NewNopLogger(),
		mockLimits{},
		newTestPrometheusCodec(),
		nil,
		promql.EngineOpts{
			Logger:     promslog.NewNopLogger(),
			Reg:        nil,
			MaxSamples: 1000,
			Timeout:    time.Minute,
		},
		true,
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	for i, tc := range []struct {
		path, expectedBody string
	}{
		{"/foo", "bar"},
		{query, responseBody},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			req, err := http.NewRequest("GET", tc.path, http.NoBody)
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), "user-1")
			req = req.WithContext(ctx)
			require.NoError(t, user.InjectOrgIDIntoHTTPRequest(ctx, req))

			resp, err := tw(downstream).RoundTrip(req)
			require.NoError(t, err)
			require.Equal(t, 200, resp.StatusCode)

			bs, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, tc.expectedBody, string(bs))
		})
	}
}

func TestTripperware_InstantQuery(t *testing.T) {
	const totalShards = 8

	ctx := user.InjectOrgID(context.Background(), "user-1")
	codec := newTestPrometheusCodec()

	tw, err := NewTripperware(
		makeTestConfig(func(cfg *Config) {
			cfg.ShardedQueries = true
		}),
		log.NewNopLogger(),
		mockLimits{totalShards: totalShards},
		codec,
		nil,
		promql.EngineOpts{
			Logger:     promslog.NewNopLogger(),
			Reg:        nil,
			MaxSamples: 1000,
			Timeout:    time.Minute,
		},
		true,
		nil,
		nil,
	)
	require.NoError(t, err)

	rt := RoundTripFunc(func(r *http.Request) (*http.Response, error) {
		// We will provide a sample exactly for the requested time,
		// this way we'll also be able to tell which time was requested.
		reqTime, err := strconv.ParseFloat(r.URL.Query().Get("time"), 64)
		if err != nil {
			return nil, err
		}

		return codec.EncodeMetricsQueryResponse(r.Context(), r, &PrometheusResponse{
			Status: "success",
			Data: &PrometheusData{
				ResultType: "vector",
				Result: []SampleStream{
					{
						Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar"}},
						Samples: []mimirpb.Sample{
							{TimestampMs: int64(reqTime * 1000), Value: 1},
						},
					},
				},
			},
		})
	})

	tripper := tw(rt)

	t.Run("specific time happy case", func(t *testing.T) {
		queryClient, err := api.NewClient(api.Config{Address: "http://localhost", RoundTripper: tripper})
		require.NoError(t, err)
		api := v1.NewAPI(queryClient)

		ts := time.Date(2021, 1, 2, 3, 4, 5, 0, time.UTC)
		res, _, err := api.Query(ctx, `sum(increase(we_dont_care_about_this[1h])) by (foo)`, ts)
		require.NoError(t, err)
		require.Equal(t, model.Vector{
			{Metric: model.Metric{"foo": "bar"}, Timestamp: model.TimeFromUnixNano(ts.UnixNano()), Value: totalShards},
		}, res)
	})

	t.Run("default time param happy case", func(t *testing.T) {
		queryClient, err := api.NewClient(api.Config{Address: "http://localhost", RoundTripper: tripper})
		require.NoError(t, err)
		api := v1.NewAPI(queryClient)

		res, _, err := api.Query(ctx, `sum(increase(we_dont_care_about_this[1h])) by (foo)`, time.Time{})
		require.NoError(t, err)
		require.IsType(t, model.Vector{}, res)
		require.NotEmpty(t, res.(model.Vector))

		resultTime := res.(model.Vector)[0].Timestamp.Time()
		require.InDelta(t, time.Now().Unix(), resultTime.Unix(), 1)
	})

	t.Run("post form time param takes precedence over query time param ", func(t *testing.T) {
		postFormTimeParam := time.Date(2021, 1, 2, 3, 4, 5, 0, time.UTC)

		addQueryTimeParam := RoundTripFunc(func(r *http.Request) (*http.Response, error) {
			query := r.URL.Query()
			// Set query's "time" param to something wrong, so that it would fail to be parsed if it's used.
			query.Set("time", "query-time-param-should-not-be-used")
			r.URL.RawQuery = query.Encode()
			return tripper.RoundTrip(r)
		})
		queryClient, err := api.NewClient(api.Config{Address: "http://localhost", RoundTripper: addQueryTimeParam})
		require.NoError(t, err)
		api := v1.NewAPI(queryClient)

		res, _, err := api.Query(ctx, `sum(increase(we_dont_care_about_this[1h])) by (foo)`, postFormTimeParam)
		require.NoError(t, err)
		require.IsType(t, model.Vector{}, res)
		require.NotEmpty(t, res.(model.Vector))

		resultTime := res.(model.Vector)[0].Timestamp.Time()
		require.Equal(t, postFormTimeParam.Unix(), resultTime.Unix())
	})
}

func TestTripperware_Metrics(t *testing.T) {
	tests := map[string]struct {
		request          *http.Request
		stepAlignEnabled bool
		expectedMetrics  string
	}{
		"range query, start/end is aligned to step": {
			request: httptest.NewRequest("GET", "/api/v1/query_range?query=up&start=1536673680&end=1536716880&step=120", http.NoBody),
			expectedMetrics: `
				# HELP cortex_query_frontend_non_step_aligned_queries_total Total queries sent that are not step aligned.
				# TYPE cortex_query_frontend_non_step_aligned_queries_total counter
				cortex_query_frontend_non_step_aligned_queries_total 0

				# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
				# TYPE cortex_query_frontend_queries_total counter
				cortex_query_frontend_queries_total{op="query_range",user="user-1"} 1
			`,
		},
		"range query, start/end is not aligned to step, aligning disabled": {
			request: httptest.NewRequest("GET", "/api/v1/query_range?query=up&start=1536673680&end=1536716880&step=7", http.NoBody),
			expectedMetrics: `
				# HELP cortex_query_frontend_non_step_aligned_queries_total Total queries sent that are not step aligned.
				# TYPE cortex_query_frontend_non_step_aligned_queries_total counter
				cortex_query_frontend_non_step_aligned_queries_total 1

				# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
				# TYPE cortex_query_frontend_queries_total counter
				cortex_query_frontend_queries_total{op="query_range",user="user-1"} 1
			`,
		},
		"range query, start/end is not aligned to step, aligning enabled": {
			request:          httptest.NewRequest("GET", "/api/v1/query_range?query=up&start=1536673680&end=1536716880&step=7", http.NoBody),
			stepAlignEnabled: true,
			expectedMetrics: `
				# HELP cortex_query_frontend_non_step_aligned_queries_total Total queries sent that are not step aligned.
				# TYPE cortex_query_frontend_non_step_aligned_queries_total counter
				cortex_query_frontend_non_step_aligned_queries_total 1

				# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
				# TYPE cortex_query_frontend_queries_total counter
				cortex_query_frontend_queries_total{op="query_range",user="user-1"} 1
			`,
		},
		"instant query": {
			request: httptest.NewRequest("GET", "/api/v1/query?query=up&time=1536673680", http.NoBody),
			expectedMetrics: `
				# HELP cortex_query_frontend_non_step_aligned_queries_total Total queries sent that are not step aligned.
				# TYPE cortex_query_frontend_non_step_aligned_queries_total counter
				cortex_query_frontend_non_step_aligned_queries_total 0

				# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
				# TYPE cortex_query_frontend_queries_total counter
				cortex_query_frontend_queries_total{op="query",user="user-1"} 1
			`,
		},
		"remote read, start/end is aligned to step": {
			request: func() *http.Request {
				return makeTestHTTPRequestFromRemoteRead(&prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers:         []*prompb.LabelMatcher{{Name: "__name__", Type: prompb.LabelMatcher_EQ, Value: "some_metric"}},
							StartTimestampMs: 1536673680,
							EndTimestampMs:   1536716880,
							Hints: &prompb.ReadHints{
								StepMs:  120,
								StartMs: 1536673680,
								EndMs:   1536716880,
							},
						},
					},
				})
			}(),
			expectedMetrics: `
				# HELP cortex_query_frontend_non_step_aligned_queries_total Total queries sent that are not step aligned.
				# TYPE cortex_query_frontend_non_step_aligned_queries_total counter
				cortex_query_frontend_non_step_aligned_queries_total 0

				# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
				# TYPE cortex_query_frontend_queries_total counter
				cortex_query_frontend_queries_total{op="remote_read",user="user-1"} 1
			`,
		},
		"remote read, start/end is not aligned to step, aligning disabled": {
			request: func() *http.Request {
				return makeTestHTTPRequestFromRemoteRead(&prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers:         []*prompb.LabelMatcher{{Name: "__name__", Type: prompb.LabelMatcher_EQ, Value: "some_metric"}},
							StartTimestampMs: 1536673680,
							EndTimestampMs:   1536716880,
							Hints: &prompb.ReadHints{
								StepMs:  7,
								StartMs: 1536673680,
								EndMs:   1536716880,
							},
						},
					},
				})
			}(),
			expectedMetrics: `
				# HELP cortex_query_frontend_non_step_aligned_queries_total Total queries sent that are not step aligned.
				# TYPE cortex_query_frontend_non_step_aligned_queries_total counter
				cortex_query_frontend_non_step_aligned_queries_total 0

				# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
				# TYPE cortex_query_frontend_queries_total counter
				cortex_query_frontend_queries_total{op="remote_read",user="user-1"} 1
			`,
		},
		"remote read, start/end is not aligned to step, aligning enabled": {
			request: func() *http.Request {
				return makeTestHTTPRequestFromRemoteRead(&prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers:         []*prompb.LabelMatcher{{Name: "__name__", Type: prompb.LabelMatcher_EQ, Value: "some_metric"}},
							StartTimestampMs: 1536673680,
							EndTimestampMs:   1536716880,
							Hints: &prompb.ReadHints{
								StepMs:  7,
								StartMs: 1536673680,
								EndMs:   1536716880,
							},
						},
					},
				})
			}(),
			stepAlignEnabled: true,
			// Even if forced aligning is enabled, the step will not be aligned because in Mimir we don't take
			// in account the step when processing remote read requests.
			expectedMetrics: `
				# HELP cortex_query_frontend_non_step_aligned_queries_total Total queries sent that are not step aligned.
				# TYPE cortex_query_frontend_non_step_aligned_queries_total counter
				cortex_query_frontend_non_step_aligned_queries_total 0

				# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
				# TYPE cortex_query_frontend_queries_total counter
				cortex_query_frontend_queries_total{op="remote_read",user="user-1"} 1
			`,
		},
		"label names": {
			request: httptest.NewRequest("GET", "/api/v1/labels?start=1536673680&end=1536716880", http.NoBody),
			expectedMetrics: `
				# HELP cortex_query_frontend_non_step_aligned_queries_total Total queries sent that are not step aligned.
				# TYPE cortex_query_frontend_non_step_aligned_queries_total counter
				cortex_query_frontend_non_step_aligned_queries_total 0

				# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
				# TYPE cortex_query_frontend_queries_total counter
				cortex_query_frontend_queries_total{op="label_names_and_values",user="user-1"} 1
			`,
		},
		"label values": {
			request: httptest.NewRequest("GET", "/api/v1/label/test/values?start=1536673680&end=1536716880", http.NoBody),
			expectedMetrics: `
				# HELP cortex_query_frontend_non_step_aligned_queries_total Total queries sent that are not step aligned.
				# TYPE cortex_query_frontend_non_step_aligned_queries_total counter
				cortex_query_frontend_non_step_aligned_queries_total 0

				# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
				# TYPE cortex_query_frontend_queries_total counter
				cortex_query_frontend_queries_total{op="label_names_and_values",user="user-1"} 1
			`,
		},
		"cardinality label names": {
			request: httptest.NewRequest("GET", "/api/v1/cardinality/label_names?selector=%7Bjob%3D%22test%22%7D", http.NoBody),
			expectedMetrics: `
				# HELP cortex_query_frontend_non_step_aligned_queries_total Total queries sent that are not step aligned.
				# TYPE cortex_query_frontend_non_step_aligned_queries_total counter
				cortex_query_frontend_non_step_aligned_queries_total 0

				# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
				# TYPE cortex_query_frontend_queries_total counter
				cortex_query_frontend_queries_total{op="cardinality",user="user-1"} 1
			`,
		},
		"cardinality active series": {
			request: httptest.NewRequest("GET", "/api/v1/cardinality/active_series?selector=%7Bjob%3D%22test%22%7D", http.NoBody),
			expectedMetrics: `
				# HELP cortex_query_frontend_non_step_aligned_queries_total Total queries sent that are not step aligned.
				# TYPE cortex_query_frontend_non_step_aligned_queries_total counter
				cortex_query_frontend_non_step_aligned_queries_total 0

				# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
				# TYPE cortex_query_frontend_queries_total counter
				cortex_query_frontend_queries_total{op="active_series",user="user-1"} 1
			`,
		},
		"cardinality active native histogram metrics": {
			request: httptest.NewRequest("GET", "/api/v1/cardinality/active_native_histogram_metrics?selector=%7Bjob%3D%22test%22%7D", http.NoBody),
			expectedMetrics: `
			# HELP cortex_query_frontend_non_step_aligned_queries_total Total queries sent that are not step aligned.
			# TYPE cortex_query_frontend_non_step_aligned_queries_total counter
			cortex_query_frontend_non_step_aligned_queries_total 0

			# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
			# TYPE cortex_query_frontend_queries_total counter
			cortex_query_frontend_queries_total{op="active_native_histogram_metrics",user="user-1"} 1
			`,
		},
		"unknown query type": {
			request: httptest.NewRequest("GET", "/api/v1/unknown", http.NoBody),
			expectedMetrics: `
				# HELP cortex_query_frontend_non_step_aligned_queries_total Total queries sent that are not step aligned.
				# TYPE cortex_query_frontend_non_step_aligned_queries_total counter
				cortex_query_frontend_non_step_aligned_queries_total 0

				# HELP cortex_query_frontend_queries_total Total queries sent per tenant.
				# TYPE cortex_query_frontend_queries_total counter
				cortex_query_frontend_queries_total{op="other",user="user-1"} 1
			`,
		},
	}

	s := httptest.NewServer(
		middleware.AuthenticateUser.Wrap(
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", jsonMimeType)
				_, err := w.Write([]byte("{}"))
				require.NoError(t, err)
			}),
		),
	)
	defer s.Close()

	u, err := url.Parse(s.URL)
	require.NoError(t, err)

	downstream := singleHostRoundTripper{
		host: u.Host,
		next: http.DefaultTransport,
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			tw, err := NewTripperware(
				Config{},
				log.NewNopLogger(),
				mockLimits{
					alignQueriesWithStep: testData.stepAlignEnabled,
				},
				newTestPrometheusCodec(),
				nil,
				promql.EngineOpts{
					Logger:     promslog.NewNopLogger(),
					Reg:        nil,
					MaxSamples: 1000,
					Timeout:    time.Minute,
				},
				true,
				nil,
				reg,
			)
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), "user-1")
			req := testData.request.WithContext(ctx)
			require.NoError(t, user.InjectOrgIDIntoHTTPRequest(ctx, req))

			resp, err := tw(downstream).RoundTrip(req)
			require.NoError(t, err)
			require.Equal(t, 200, resp.StatusCode)

			assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(testData.expectedMetrics),
				"cortex_query_frontend_non_step_aligned_queries_total",
				"cortex_query_frontend_queries_total",
			))
		})
	}
}

// TestMiddlewaresConsistency ensures that we don't forget to add a middleware to a given type of request
// (e.g. range query, remote read, ...) when a new middleware is added. By default, it expects that a middleware
// is added to each type of request, and then it allows to define exceptions when we intentionally don't
// want a given middleware to be used for a specific request.
func TestMiddlewaresConsistency(t *testing.T) {
	cfg := makeTestConfig()
	cfg.CacheResults = true
	cfg.ShardedQueries = true
	cfg.PrunedQueries = true
	cfg.BlockPromQLExperimentalFunctions = true

	// Ensure all features are enabled, so that we assert on all middlewares.
	require.NotZero(t, cfg.CacheResults)
	require.NotZero(t, cfg.ShardedQueries)
	require.NotZero(t, cfg.PrunedQueries)
	require.NotZero(t, cfg.BlockPromQLExperimentalFunctions)
	require.NotZero(t, cfg.SplitQueriesByInterval)
	require.NotZero(t, cfg.MaxRetries)

	queryRangeMiddlewares, queryInstantMiddlewares, remoteReadMiddlewares := newQueryMiddlewares(
		cfg,
		log.NewNopLogger(),
		mockLimits{
			alignQueriesWithStep: true,
		},
		newTestPrometheusCodec(),
		nil,
		nil,
		nil,
		promql.NewEngine(promql.EngineOpts{}),
		nil,
		nil,
	)

	middlewaresByRequestType := map[string]struct {
		instances  []MetricsQueryMiddleware
		exceptions []string
	}{
		"instant query": {
			instances:  queryInstantMiddlewares,
			exceptions: []string{"splitAndCacheMiddleware", "stepAlignMiddleware"},
		},
		"range query": {
			instances:  queryRangeMiddlewares,
			exceptions: []string{"splitInstantQueryByIntervalMiddleware"},
		},
		"remote read": {
			instances: remoteReadMiddlewares,
			exceptions: []string{
				"instrumentMiddleware",
				"querySharding", // No query sharding support.
				"retry",
				"splitAndCacheMiddleware",               // No time splitting and results cache support.
				"splitInstantQueryByIntervalMiddleware", // Not applicable because specific to instant queries.
				"stepAlignMiddleware",                   // Not applicable because remote read requests don't take step in account when running in Mimir.
				"pruneMiddleware",                       // No query pruning support.
				"experimentalFunctionsMiddleware",       // No blocking for PromQL experimental functions as it is executed remotely.
			},
		},
	}

	// Utility to get the name of the struct.
	getName := func(i interface{}) string {
		t := reflect.TypeOf(i)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		return t.Name()
	}

	// Utility to get the names of middlewares.
	getMiddlewareNames := func(middlewares []MetricsQueryMiddleware) (names []string) {
		for _, middleware := range middlewares {
			handler := middleware.Wrap(&mockHandler{})
			name := getName(handler)

			names = append(names, name)
		}

		// Unique names.
		slices.Sort(names)
		names = slices.Compact(names)

		return
	}

	// Get the (unique) names of all middlewares.
	var allNames []string
	for _, middlewares := range middlewaresByRequestType {
		allNames = append(allNames, getMiddlewareNames(middlewares.instances)...)
	}
	slices.Sort(allNames)
	allNames = slices.Compact(allNames)

	// Ensure that all request types implements all middlewares, except exclusions.
	for requestType, middlewares := range middlewaresByRequestType {
		t.Run(requestType, func(t *testing.T) {
			actualNames := getMiddlewareNames(middlewares.instances)
			expectedNames := slices.DeleteFunc(slices.Clone(allNames), func(s string) bool {
				return slices.Contains(middlewares.exceptions, s)
			})

			assert.ElementsMatch(t, expectedNames, actualNames)
		})
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		config        Config
		expectedError error
	}{
		"happy path": {
			config:        Config{QueryResultResponseFormat: formatJSON},
			expectedError: nil,
		},
		"unknown query result payload format": {
			config:        Config{QueryResultResponseFormat: "something-else"},
			expectedError: errors.New("unknown query result response format 'something-else'. Supported values: json, protobuf"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := test.config.Validate()
			require.Equal(t, test.expectedError, err)
		})
	}
}

func TestIsLabelsQuery(t *testing.T) {
	tests := []struct {
		path     string
		expected bool
	}{
		{
			path:     "/api/v1/labels/unknown",
			expected: false,
		}, {
			path:     "/api/v1/",
			expected: false,
		}, {
			path:     "/api/v1/labels",
			expected: true,
		}, {
			path:     "/labels",
			expected: false,
		}, {
			path:     "/prometheus/api/v1/labels",
			expected: true,
		}, {
			path:     "/api/v1/label/test/values",
			expected: true,
		}, {
			path:     "/values",
			expected: false,
		}, {
			path:     "/prometheus/api/v1/label/test/values",
			expected: true,
		}, {
			path:     "/prometheus/api/v1/label/test/values/unknown",
			expected: false,
		}, {
			path:     "/prometheus/api/v1/label/test/unknown/values",
			expected: false,
		},
	}

	for _, testData := range tests {
		t.Run(testData.path, func(t *testing.T) {
			assert.Equal(t, testData.expected, IsLabelsQuery(testData.path))
		})
	}
}

func TestTripperware_RemoteRead(t *testing.T) {
	testCases := map[string]struct {
		makeRequest         func() *http.Request
		limits              mockLimits
		expectError         bool
		expectAPIError      bool
		expectErrorContains string
	}{
		"valid query": {
			makeRequest: func() *http.Request {
				return makeTestHTTPRequestFromRemoteRead(makeTestRemoteReadRequest())
			},
			limits: mockLimits{},
		},
		"max total query length limit exceeded": {
			makeRequest: func() *http.Request {
				return makeTestHTTPRequestFromRemoteRead(&prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers:         []*prompb.LabelMatcher{{Name: "__name__", Type: prompb.LabelMatcher_EQ, Value: "some_metric"}},
							StartTimestampMs: time.Now().Add(-60 * 24 * time.Hour).UnixMilli(),
							EndTimestampMs:   time.Now().UnixMilli(),
						},
					},
				})
			},
			limits: mockLimits{
				maxTotalQueryLength: 30 * 24 * time.Hour,
			},
			expectError:         true,
			expectAPIError:      true,
			expectErrorContains: "the total query time range exceeds the limit",
		},
	}

	s := httptest.NewServer(
		middleware.AuthenticateUser.Wrap(
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", jsonMimeType)
				_, err := w.Write([]byte("{}"))
				require.NoError(t, err)
			}),
		),
	)
	defer s.Close()

	u, err := url.Parse(s.URL)
	require.NoError(t, err)

	downstream := singleHostRoundTripper{
		host: u.Host,
		next: http.DefaultTransport,
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			tw, err := NewTripperware(
				makeTestConfig(),
				log.NewNopLogger(),
				tc.limits,
				newTestPrometheusCodec(),
				nil,
				promql.EngineOpts{
					Logger:     promslog.NewNopLogger(),
					Reg:        nil,
					MaxSamples: 1000,
					Timeout:    time.Minute,
				},
				true,
				nil,
				reg,
			)
			require.NoError(t, err)

			req := tc.makeRequest()
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), "user-1")
			req = req.WithContext(ctx)
			require.NoError(t, user.InjectOrgIDIntoHTTPRequest(ctx, req))

			resp, err := tw(downstream).RoundTrip(req)
			if tc.expectError {
				require.Error(t, err)
				if tc.expectAPIError {
					require.True(t, apierror.IsAPIError(err))
				}
				if tc.expectErrorContains != "" {
					require.Contains(t, err.Error(), tc.expectErrorContains)
				}
			} else {
				require.NoError(t, err)
				_, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
			}
		})
	}
}

func TestTripperware_ShouldSupportReadConsistencyOffsetsInjection(t *testing.T) {
	const (
		topic         = "test"
		numPartitions = 10
		tenantID      = "user-1"
	)

	tests := map[string]struct {
		makeRequest func() *http.Request
	}{
		"range query": {
			makeRequest: func() *http.Request {
				return httptest.NewRequest("GET", queryRangePathSuffix+"?start=1536673680&end=1536716880&step=120&query=up", nil)
			},
		},
		"instant query": {
			makeRequest: func() *http.Request {
				return httptest.NewRequest("GET", instantQueryPathSuffix+"?time=1536673680&query=up", nil)
			},
		},
		"cardinality label names": {
			makeRequest: func() *http.Request {
				return httptest.NewRequest("GET", cardinalityLabelNamesPathSuffix, nil)
			},
		},
		"cardinality label values": {
			makeRequest: func() *http.Request {
				return httptest.NewRequest("GET", cardinalityLabelValuesPathSuffix+"?label_names[]=foo", nil)
			},
		},
		"cardinality active series": {
			makeRequest: func() *http.Request {
				return httptest.NewRequest("GET", cardinalityActiveSeriesPathSuffix, nil)
			},
		},
		"cardinality active native histograms": {
			makeRequest: func() *http.Request {
				return httptest.NewRequest("GET", cardinalityActiveNativeHistogramMetricsPathSuffix, nil)
			},
		},
		"label names": {
			makeRequest: func() *http.Request {
				return httptest.NewRequest("GET", labelNamesPathSuffix, nil)
			},
		},
		"remote read": {
			makeRequest: func() *http.Request {
				return makeTestHTTPRequestFromRemoteRead(&prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers:         []*prompb.LabelMatcher{{Name: "__name__", Type: prompb.LabelMatcher_EQ, Value: "some_metric"}},
							StartTimestampMs: time.Now().Add(-60 * 24 * time.Hour).UnixMilli(),
							EndTimestampMs:   time.Now().UnixMilli(),
						},
					},
				})
			},
		},
	}

	ctx := context.Background()
	logger := log.NewNopLogger()

	// Setup a fake Kafka cluster.
	_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topic)

	// Write some records to different partitions.
	expectedOffsets := produceKafkaRecords(t, clusterAddr, topic,
		&kgo.Record{Partition: 0},
		&kgo.Record{Partition: 0},
		&kgo.Record{Partition: 0},
		&kgo.Record{Partition: 1},
		&kgo.Record{Partition: 1},
		&kgo.Record{Partition: 2},
	)

	// Create the topic offsets reader.
	readClient, err := ingest.NewKafkaReaderClient(createKafkaConfig(clusterAddr, topic), nil, logger)
	require.NoError(t, err)
	t.Cleanup(readClient.Close)

	offsetsReader := ingest.NewTopicOffsetsReaderForAllPartitions(readClient, topic, 100*time.Millisecond, nil, logger)
	require.NoError(t, services.StartAndAwaitRunning(ctx, offsetsReader))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, offsetsReader))
	})

	// Create the tripperware.
	tw, err := NewTripperware(
		makeTestConfig(func(cfg *Config) {
			cfg.ShardedQueries = false
			cfg.SplitQueriesByInterval = 0
			cfg.CacheResults = false
		}),
		log.NewNopLogger(),
		mockLimits{},
		NewPrometheusCodec(nil, 0, formatJSON, nil),
		nil,
		promql.EngineOpts{
			Logger:     promslog.NewNopLogger(),
			Reg:        nil,
			MaxSamples: 1000,
			Timeout:    time.Minute,
		},
		true,
		map[string]*ingest.TopicOffsetsReader{querierapi.ReadConsistencyOffsetsHeader: offsetsReader},
		nil,
	)
	require.NoError(t, err)

	// Test it against all routes.
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			for _, consistencyLevel := range []string{querierapi.ReadConsistencyEventual, querierapi.ReadConsistencyStrong} {
				t.Run(fmt.Sprintf("consistency level: %s", consistencyLevel), func(t *testing.T) {
					t.Parallel()

					// Create a roundtripper that captures the downstream HTTP request.
					var downstreamReq *http.Request

					tripper := tw(RoundTripFunc(func(req *http.Request) (*http.Response, error) {
						downstreamReq = req

						return &http.Response{
							StatusCode: 200,
							Body:       io.NopCloser(strings.NewReader("{}")),
							Header:     http.Header{"Content-Type": []string{"application/json"}},
						}, nil
					}))

					// Send an HTTP request through the roundtripper.
					req := testData.makeRequest()
					req = req.WithContext(user.InjectOrgID(req.Context(), tenantID))
					req = req.WithContext(querierapi.ContextWithReadConsistencyLevel(req.Context(), consistencyLevel))

					res, err := tripper.RoundTrip(req)
					require.NoError(t, err)
					require.NotNil(t, res)
					require.NotNil(t, downstreamReq)

					if consistencyLevel == querierapi.ReadConsistencyStrong {
						offsets := querierapi.EncodedOffsets(downstreamReq.Header.Get(querierapi.ReadConsistencyOffsetsHeader))

						for partitionID, expectedOffset := range expectedOffsets {
							actual, ok := offsets.Lookup(partitionID)
							assert.True(t, ok)
							assert.Equal(t, expectedOffset, actual)
						}
					} else {
						assert.Empty(t, downstreamReq.Header.Get(querierapi.ReadConsistencyOffsetsHeader))
					}
				})
			}
		})
	}
}

type singleHostRoundTripper struct {
	host string
	next http.RoundTripper
}

func (s singleHostRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	r.URL.Scheme = "http"
	r.URL.Host = s.host
	return s.next.RoundTrip(r)
}

func makeTestConfig(overrides ...func(*Config)) Config {
	cfg := Config{}
	flagext.DefaultValues(&cfg)

	for _, override := range overrides {
		override(&cfg)
	}

	return cfg
}
