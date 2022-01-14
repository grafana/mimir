// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/roundtrip_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queryrange

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/chunk/storage"
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

	tw, _, err := NewTripperware(Config{},
		log.NewNopLogger(),
		mockLimits{},
		PrometheusCodec,
		nil,
		storage.StorageEngineBlocks,
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

			bs, err := ioutil.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, tc.expectedBody, string(bs))
		})
	}
}

func TestInstantTripperware(t *testing.T) {
	const totalShards = 8

	ctx := user.InjectOrgID(context.Background(), "user-1")

	tw, _, err := NewTripperware(
		Config{
			ShardedQueries: true,
		},
		log.NewNopLogger(),
		mockLimits{totalShards: totalShards},
		PrometheusCodec,
		nil,
		storage.StorageEngineBlocks,
		promql.EngineOpts{
			Logger:     log.NewNopLogger(),
			Reg:        nil,
			MaxSamples: 1000,
			Timeout:    time.Minute,
		},
		nil,
	)
	require.NoError(t, err)

	ts := time.Date(2021, 1, 2, 3, 4, 5, 0, time.UTC)
	rt := RoundTripFunc(func(r *http.Request) (*http.Response, error) {
		// We will provide a sample exactly for the requested time,
		// this way we'll also be able to tell which time was requested.
		reqTime, err := strconv.ParseFloat(r.URL.Query().Get("time"), 10)
		if err != nil {
			return nil, err
		}

		return PrometheusCodec.EncodeResponse(r.Context(), &PrometheusResponse{
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

	queryClient, err := api.NewClient(api.Config{Address: "http://localhost", RoundTripper: tw(rt)})
	require.NoError(t, err)
	api := v1.NewAPI(queryClient)

	t.Run("happy case roundtrip", func(t *testing.T) {
		res, _, err := api.Query(ctx, `sum(increase(we_dont_care_about_this[1h])) by (foo)`, ts)
		require.NoError(t, err)
		require.Equal(t, model.Vector{
			{Metric: model.Metric{"foo": "bar"}, Timestamp: model.TimeFromUnixNano(ts.UnixNano()), Value: totalShards},
		}, res)
	})

	t.Run("default time param", func(t *testing.T) {
		res, _, err := api.Query(ctx, `sum(increase(we_dont_care_about_this[1h])) by (foo)`, time.Now())
		require.NoError(t, err)
		require.IsType(t, model.Vector{}, res)
		require.NotEmpty(t, res.(model.Vector))

		resultTime := res.(model.Vector)[0].Timestamp.Time()
		require.InDelta(t, time.Now().Unix(), resultTime.Unix(), 1)
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
			tw, _, err := NewTripperware(Config{AlignQueriesWithStep: testData.stepAlignEnabled},
				log.NewNopLogger(),
				mockLimits{},
				PrometheusCodec,
				nil,
				storage.StorageEngineBlocks,
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

			body, err := ioutil.ReadAll(resp.Body)
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

type singleHostRoundTripper struct {
	host string
	next http.RoundTripper
}

func (s singleHostRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	r.URL.Scheme = "http"
	r.URL.Host = s.host
	return s.next.RoundTrip(r)
}

func Test_ShardingConfigError(t *testing.T) {
	_, _, err := NewTripperware(
		Config{ShardedQueries: true},
		log.NewNopLogger(),
		nil,
		nil,
		nil,
		storage.StorageEngineChunks,
		promql.EngineOpts{},
		nil,
	)

	require.EqualError(t, err, errInvalidShardingStorage.Error())
}
