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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestRangeTripperware(t *testing.T) {
	var (
		query        = "/api/v1/query_range?end=1536716880&query=sum%28container_memory_rss%29+by+%28namespace%29&start=1536673680&step=120"
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
			Logger:     log.NewNopLogger(),
			Reg:        nil,
			MaxSamples: 1000,
			Timeout:    time.Minute,
		},
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

func TestInstantTripperware(t *testing.T) {
	const totalShards = 8

	ctx := user.InjectOrgID(context.Background(), "user-1")
	codec := newTestPrometheusCodec()

	tw, err := NewTripperware(
		Config{
			ShardedQueries: true,
		},
		log.NewNopLogger(),
		mockLimits{totalShards: totalShards},
		codec,
		nil,
		promql.EngineOpts{
			Logger:     log.NewNopLogger(),
			Reg:        nil,
			MaxSamples: 1000,
			Timeout:    time.Minute,
		},
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

		return codec.EncodeResponse(r.Context(), r, &PrometheusResponse{
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

	t.Run("specific time param with form being already parsed", func(t *testing.T) {
		ts := time.Date(2021, 1, 2, 3, 4, 5, 0, time.UTC)

		formParserRoundTripper := RoundTripFunc(func(r *http.Request) (*http.Response, error) {
			assert.NoError(t, r.ParseForm())
			return tripper.RoundTrip(r)
		})
		queryClient, err := api.NewClient(api.Config{Address: "http://localhost", RoundTripper: formParserRoundTripper})
		require.NoError(t, err)
		api := v1.NewAPI(queryClient)

		res, _, err := api.Query(ctx, `sum(increase(we_dont_care_about_this[1h])) by (foo)`, ts)
		require.NoError(t, err)
		require.IsType(t, model.Vector{}, res)
		require.NotEmpty(t, res.(model.Vector))

		resultTime := res.(model.Vector)[0].Timestamp.Time()
		require.Equal(t, ts.Unix(), resultTime.Unix())
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

	t.Run("default time param with form being already parsed", func(t *testing.T) {
		formParserRoundTripper := RoundTripFunc(func(r *http.Request) (*http.Response, error) {
			assert.NoError(t, r.ParseForm())
			return tripper.RoundTrip(r)
		})
		queryClient, err := api.NewClient(api.Config{Address: "http://localhost", RoundTripper: formParserRoundTripper})
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
		path                    string
		expectedNotAlignedCount int
		stepAlignEnabled        bool
	}{
		"start/end is aligned to step": {
			path:                    "/api/v1/query_range?query=up&start=1536673680&end=1536716880&step=120",
			expectedNotAlignedCount: 0,
		},
		"start/end is not aligned to step, aligning disabled": {
			path:                    "/api/v1/query_range?query=up&start=1536673680&end=1536716880&step=7",
			expectedNotAlignedCount: 1,
		},
		"start/end is not aligned to step, aligning enabled": {
			path:                    "/api/v1/query_range?query=up&start=1536673680&end=1536716880&step=7",
			expectedNotAlignedCount: 1,
			stepAlignEnabled:        true,
		},
	}

	s := httptest.NewServer(
		middleware.AuthenticateUser.Wrap(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
				Config{DeprecatedAlignQueriesWithStep: testData.stepAlignEnabled},
				log.NewNopLogger(),
				mockLimits{},
				newTestPrometheusCodec(),
				nil,
				promql.EngineOpts{
					Logger:     log.NewNopLogger(),
					Reg:        nil,
					MaxSamples: 1000,
					Timeout:    time.Minute,
				},
				reg,
			)
			require.NoError(t, err)

			req, err := http.NewRequest("GET", testData.path, http.NoBody)
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), "user-1")
			req = req.WithContext(ctx)
			require.NoError(t, user.InjectOrgIDIntoHTTPRequest(ctx, req))

			resp, err := tw(downstream).RoundTrip(req)
			require.NoError(t, err)
			require.Equal(t, 200, resp.StatusCode)

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, `{"status":""}`, string(body))

			assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
				# HELP cortex_query_frontend_non_step_aligned_queries_total Total queries sent that are not step aligned.
				# TYPE cortex_query_frontend_non_step_aligned_queries_total counter
				cortex_query_frontend_non_step_aligned_queries_total %d
			`, testData.expectedNotAlignedCount)),
				"cortex_query_frontend_non_step_aligned_queries_total",
			))
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

type singleHostRoundTripper struct {
	host string
	next http.RoundTripper
}

func (s singleHostRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	r.URL.Scheme = "http"
	r.URL.Host = s.host
	return s.next.RoundTrip(r)
}
