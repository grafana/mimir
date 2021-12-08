// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/split_by_interval_test.go
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
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"
	"go.uber.org/atomic"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/util"
)

const seconds = 1e3 // 1e3 milliseconds per second.

func TestNextIntervalBoundary(t *testing.T) {
	for i, tc := range []struct {
		in, step, out int64
		interval      time.Duration
	}{
		// Smallest possible period is 1 millisecond
		{0, 1, toMs(day) - 1, day},
		{0, 1, toMs(time.Hour) - 1, time.Hour},
		// A more standard example
		{0, 15 * seconds, toMs(day) - 15*seconds, day},
		{0, 15 * seconds, toMs(time.Hour) - 15*seconds, time.Hour},
		// Move start time forward 1 second; end time moves the same
		{1 * seconds, 15 * seconds, toMs(day) - (15-1)*seconds, day},
		{1 * seconds, 15 * seconds, toMs(time.Hour) - (15-1)*seconds, time.Hour},
		// Move start time forward 14 seconds; end time moves the same
		{14 * seconds, 15 * seconds, toMs(day) - (15-14)*seconds, day},
		{14 * seconds, 15 * seconds, toMs(time.Hour) - (15-14)*seconds, time.Hour},
		// Now some examples where the period does not divide evenly into a day:
		// 1 day modulus 35 seconds = 20 seconds
		{0, 35 * seconds, toMs(day) - 20*seconds, day},
		// 1 hour modulus 35 sec = 30  (3600 mod 35 = 30)
		{0, 35 * seconds, toMs(time.Hour) - 30*seconds, time.Hour},
		// Move start time forward 1 second; end time moves the same
		{1 * seconds, 35 * seconds, toMs(day) - (20-1)*seconds, day},
		{1 * seconds, 35 * seconds, toMs(time.Hour) - (30-1)*seconds, time.Hour},
		// If the end time lands exactly on midnight we stop one period before that
		{20 * seconds, 35 * seconds, toMs(day) - 35*seconds, day},
		{30 * seconds, 35 * seconds, toMs(time.Hour) - 35*seconds, time.Hour},
		// This example starts 35 seconds after the 5th one ends
		{toMs(day) + 15*seconds, 35 * seconds, 2*toMs(day) - 5*seconds, day},
		{toMs(time.Hour) + 15*seconds, 35 * seconds, 2*toMs(time.Hour) - 15*seconds, time.Hour},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require.Equal(t, tc.out, nextIntervalBoundary(tc.in, tc.step, tc.interval))
		})
	}
}

func TestSplitQueryByInterval(t *testing.T) {
	for i, tc := range []struct {
		input    Request
		expected []Request
		interval time.Duration
	}{
		{
			input: &PrometheusRequest{Start: 0, End: 60 * 60 * seconds, Step: 15 * seconds, Query: "foo"},
			expected: []Request{
				&PrometheusRequest{Start: 0, End: 60 * 60 * seconds, Step: 15 * seconds, Query: "foo"},
			},
			interval: day,
		},
		{
			input: &PrometheusRequest{Start: 0, End: 60 * 60 * seconds, Step: 15 * seconds, Query: "foo"},
			expected: []Request{
				&PrometheusRequest{Start: 0, End: 60 * 60 * seconds, Step: 15 * seconds, Query: "foo"},
			},
			interval: 3 * time.Hour,
		},
		{
			input: &PrometheusRequest{Start: 0, End: 24 * 3600 * seconds, Step: 15 * seconds, Query: "foo"},
			expected: []Request{
				&PrometheusRequest{Start: 0, End: 24 * 3600 * seconds, Step: 15 * seconds, Query: "foo"},
			},
			interval: day,
		},
		{
			input: &PrometheusRequest{Start: 0, End: 3 * 3600 * seconds, Step: 15 * seconds, Query: "foo"},
			expected: []Request{
				&PrometheusRequest{Start: 0, End: 3 * 3600 * seconds, Step: 15 * seconds, Query: "foo"},
			},
			interval: 3 * time.Hour,
		},
		{
			input: &PrometheusRequest{Start: 0, End: 2 * 24 * 3600 * seconds, Step: 15 * seconds, Query: "foo @ start()"},
			expected: []Request{
				&PrometheusRequest{Start: 0, End: (24 * 3600 * seconds) - (15 * seconds), Step: 15 * seconds, Query: "foo @ 0.000"},
				&PrometheusRequest{Start: 24 * 3600 * seconds, End: 2 * 24 * 3600 * seconds, Step: 15 * seconds, Query: "foo @ 0.000"},
			},
			interval: day,
		},
		{
			input: &PrometheusRequest{Start: 0, End: 2 * 3 * 3600 * seconds, Step: 15 * seconds, Query: "foo"},
			expected: []Request{
				&PrometheusRequest{Start: 0, End: (3 * 3600 * seconds) - (15 * seconds), Step: 15 * seconds, Query: "foo"},
				&PrometheusRequest{Start: 3 * 3600 * seconds, End: 2 * 3 * 3600 * seconds, Step: 15 * seconds, Query: "foo"},
			},
			interval: 3 * time.Hour,
		},
		{
			input: &PrometheusRequest{Start: 3 * 3600 * seconds, End: 3 * 24 * 3600 * seconds, Step: 15 * seconds, Query: "foo"},
			expected: []Request{
				&PrometheusRequest{Start: 3 * 3600 * seconds, End: (24 * 3600 * seconds) - (15 * seconds), Step: 15 * seconds, Query: "foo"},
				&PrometheusRequest{Start: 24 * 3600 * seconds, End: (2 * 24 * 3600 * seconds) - (15 * seconds), Step: 15 * seconds, Query: "foo"},
				&PrometheusRequest{Start: 2 * 24 * 3600 * seconds, End: 3 * 24 * 3600 * seconds, Step: 15 * seconds, Query: "foo"},
			},
			interval: day,
		},
		{
			input: &PrometheusRequest{Start: 2 * 3600 * seconds, End: 3 * 3 * 3600 * seconds, Step: 15 * seconds, Query: "foo"},
			expected: []Request{
				&PrometheusRequest{Start: 2 * 3600 * seconds, End: (3 * 3600 * seconds) - (15 * seconds), Step: 15 * seconds, Query: "foo"},
				&PrometheusRequest{Start: 3 * 3600 * seconds, End: (2 * 3 * 3600 * seconds) - (15 * seconds), Step: 15 * seconds, Query: "foo"},
				&PrometheusRequest{Start: 2 * 3 * 3600 * seconds, End: 3 * 3 * 3600 * seconds, Step: 15 * seconds, Query: "foo"},
			},
			interval: 3 * time.Hour,
		},
		{
			input: &PrometheusRequest{Start: timeToMillis(t, "2021-10-14T23:48:00Z"), End: timeToMillis(t, "2021-10-15T00:03:00Z"), Step: 5 * time.Minute.Milliseconds(), Query: "foo"},
			expected: []Request{
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-14T23:48:00Z"), End: timeToMillis(t, "2021-10-15T00:03:00Z"), Step: 5 * time.Minute.Milliseconds(), Query: "foo"},
			},
			interval: day,
		},
		{
			input: &PrometheusRequest{Start: timeToMillis(t, "2021-10-14T23:48:00Z"), End: timeToMillis(t, "2021-10-15T00:00:00Z"), Step: 6 * time.Minute.Milliseconds(), Query: "foo"},
			expected: []Request{
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-14T23:48:00Z"), End: timeToMillis(t, "2021-10-14T23:54:00Z"), Step: 6 * time.Minute.Milliseconds(), Query: "foo"},
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-15T00:00:00Z"), End: timeToMillis(t, "2021-10-15T00:00:00Z"), Step: 6 * time.Minute.Milliseconds(), Query: "foo"},
			},
			interval: day,
		},
		{
			input: &PrometheusRequest{Start: timeToMillis(t, "2021-10-14T22:00:00Z"), End: timeToMillis(t, "2021-10-17T22:00:00Z"), Step: 24 * time.Hour.Milliseconds(), Query: "foo"},
			expected: []Request{
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-14T22:00:00Z"), End: timeToMillis(t, "2021-10-14T22:00:00Z"), Step: 24 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-15T22:00:00Z"), End: timeToMillis(t, "2021-10-15T22:00:00Z"), Step: 24 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-16T22:00:00Z"), End: timeToMillis(t, "2021-10-16T22:00:00Z"), Step: 24 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-17T22:00:00Z"), End: timeToMillis(t, "2021-10-17T22:00:00Z"), Step: 24 * time.Hour.Milliseconds(), Query: "foo"},
			},
			interval: day,
		},
		{
			input: &PrometheusRequest{Start: timeToMillis(t, "2021-10-15T00:00:00Z"), End: timeToMillis(t, "2021-10-18T00:00:00Z"), Step: 24 * time.Hour.Milliseconds(), Query: "foo"},
			expected: []Request{
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-15T00:00:00Z"), End: timeToMillis(t, "2021-10-15T00:00:00Z"), Step: 24 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-16T00:00:00Z"), End: timeToMillis(t, "2021-10-16T00:00:00Z"), Step: 24 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-17T00:00:00Z"), End: timeToMillis(t, "2021-10-17T00:00:00Z"), Step: 24 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-18T00:00:00Z"), End: timeToMillis(t, "2021-10-18T00:00:00Z"), Step: 24 * time.Hour.Milliseconds(), Query: "foo"},
			},
			interval: day,
		},
		{
			input: &PrometheusRequest{Start: timeToMillis(t, "2021-10-15T22:00:00Z"), End: timeToMillis(t, "2021-10-22T04:00:00Z"), Step: 30 * time.Hour.Milliseconds(), Query: "foo"},
			expected: []Request{
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-15T22:00:00Z"), End: timeToMillis(t, "2021-10-15T22:00:00Z"), Step: 30 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-17T04:00:00Z"), End: timeToMillis(t, "2021-10-17T04:00:00Z"), Step: 30 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-18T10:00:00Z"), End: timeToMillis(t, "2021-10-18T10:00:00Z"), Step: 30 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-19T16:00:00Z"), End: timeToMillis(t, "2021-10-19T16:00:00Z"), Step: 30 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-20T22:00:00Z"), End: timeToMillis(t, "2021-10-20T22:00:00Z"), Step: 30 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-22T04:00:00Z"), End: timeToMillis(t, "2021-10-22T04:00:00Z"), Step: 30 * time.Hour.Milliseconds(), Query: "foo"},
			},
			interval: day,
		},
		{
			input: &PrometheusRequest{Start: timeToMillis(t, "2021-10-15T06:00:00Z"), End: timeToMillis(t, "2021-10-17T14:00:00Z"), Step: 12 * time.Hour.Milliseconds(), Query: "foo"},
			expected: []Request{
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-15T06:00:00Z"), End: timeToMillis(t, "2021-10-15T18:00:00Z"), Step: 12 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-16T06:00:00Z"), End: timeToMillis(t, "2021-10-16T18:00:00Z"), Step: 12 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-17T06:00:00Z"), End: timeToMillis(t, "2021-10-17T14:00:00Z"), Step: 12 * time.Hour.Milliseconds(), Query: "foo"},
			},
			interval: day,
		},
		{
			input: &PrometheusRequest{Start: timeToMillis(t, "2021-10-15T06:00:00Z"), End: timeToMillis(t, "2021-10-17T18:00:00Z"), Step: 12 * time.Hour.Milliseconds(), Query: "foo"},
			expected: []Request{
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-15T06:00:00Z"), End: timeToMillis(t, "2021-10-15T18:00:00Z"), Step: 12 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-16T06:00:00Z"), End: timeToMillis(t, "2021-10-16T18:00:00Z"), Step: 12 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-17T06:00:00Z"), End: timeToMillis(t, "2021-10-17T18:00:00Z"), Step: 12 * time.Hour.Milliseconds(), Query: "foo"},
			},
			interval: day,
		},
		{
			input: &PrometheusRequest{Start: timeToMillis(t, "2021-10-15T06:00:00Z"), End: timeToMillis(t, "2021-10-17T18:00:00Z"), Step: 10 * time.Hour.Milliseconds(), Query: "foo"},
			expected: []Request{
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-15T06:00:00Z"), End: timeToMillis(t, "2021-10-15T16:00:00Z"), Step: 10 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-16T02:00:00Z"), End: timeToMillis(t, "2021-10-16T22:00:00Z"), Step: 10 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-17T08:00:00Z"), End: timeToMillis(t, "2021-10-17T18:00:00Z"), Step: 10 * time.Hour.Milliseconds(), Query: "foo"},
			},
			interval: day,
		},
		{
			input: &PrometheusRequest{Start: timeToMillis(t, "2021-10-15T06:00:00Z"), End: timeToMillis(t, "2021-10-17T08:00:00Z"), Step: 10 * time.Hour.Milliseconds(), Query: "foo"},
			expected: []Request{
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-15T06:00:00Z"), End: timeToMillis(t, "2021-10-15T16:00:00Z"), Step: 10 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-16T02:00:00Z"), End: timeToMillis(t, "2021-10-16T22:00:00Z"), Step: 10 * time.Hour.Milliseconds(), Query: "foo"},
				&PrometheusRequest{Start: timeToMillis(t, "2021-10-17T08:00:00Z"), End: timeToMillis(t, "2021-10-17T08:00:00Z"), Step: 10 * time.Hour.Milliseconds(), Query: "foo"},
			},
			interval: day,
		},
	} {
		t.Run(fmt.Sprintf("%d: start: %v, end: %v, step: %v", i, tc.input.GetStart(), tc.input.GetEnd(), tc.input.GetStep()), func(t *testing.T) {
			days, err := splitQueryByInterval(tc.input, tc.interval)
			require.NoError(t, err)
			require.Equal(t, tc.expected, days)
		})
	}
}

func timeToMillis(t *testing.T, input string) int64 {
	r, err := time.Parse(time.RFC3339, input)
	require.NoError(t, err)
	return util.TimeToMillis(r)
}

func TestSplitByDay(t *testing.T) {
	mergedResponse, err := PrometheusCodec.MergeResponse(parsedResponse, parsedResponse)
	require.NoError(t, err)

	mergedHTTPResponse, err := PrometheusCodec.EncodeResponse(context.Background(), mergedResponse)
	require.NoError(t, err)

	mergedHTTPResponseBody, err := ioutil.ReadAll(mergedHTTPResponse.Body)
	require.NoError(t, err)

	for i, tc := range []struct {
		path, expectedBody string
		expectedQueryCount int32
	}{
		{query, string(mergedHTTPResponseBody), 2},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var actualCount atomic.Int32
			s := httptest.NewServer(
				middleware.AuthenticateUser.Wrap(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						actualCount.Inc()
						_, _ = w.Write([]byte(responseBody))
					}),
				),
			)
			defer s.Close()

			u, err := url.Parse(s.URL)
			require.NoError(t, err)

			interval := func(_ Request) time.Duration { return 24 * time.Hour }
			roundtripper := NewRoundTripper(singleHostRoundTripper{
				host: u.Host,
				next: http.DefaultTransport,
			}, PrometheusCodec, log.NewNopLogger(), NewLimitsMiddleware(mockLimits{}, log.NewNopLogger()), SplitByIntervalMiddleware(interval, mockLimits{}, PrometheusCodec, nil))

			req, err := http.NewRequest("GET", tc.path, http.NoBody)
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), "1")
			req = req.WithContext(ctx)

			resp, err := roundtripper.RoundTrip(req)
			require.NoError(t, err)
			require.Equal(t, 200, resp.StatusCode)

			bs, err := ioutil.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, tc.expectedBody, string(bs))
			require.Equal(t, tc.expectedQueryCount, actualCount.Load())
		})
	}
}

func Test_evaluateAtModifier(t *testing.T) {
	const (
		start, end = int64(1546300800), int64(1646300800)
	)
	for _, tt := range []struct {
		in, expected string
		err          error
	}{
		{"topk(5, rate(http_requests_total[1h] @ start()))", "topk(5, rate(http_requests_total[1h] @ 1546300.800))", nil},
		{"topk(5, rate(http_requests_total[1h] @ 0))", "topk(5, rate(http_requests_total[1h] @ 0.000))", nil},
		{"http_requests_total[1h] @ 10.001", "http_requests_total[1h] @ 10.001", nil},
		{
			`min_over_time(
				sum by(cluster) (
					rate(http_requests_total[5m] @ end())
				)[10m:]
			)
			or
			max_over_time(
				stddev_over_time(
					deriv(
						rate(http_requests_total[10m] @ start())
					[5m:1m])
				[2m:])
			[10m:])`,
			`min_over_time(
				sum by(cluster) (
					rate(http_requests_total[5m] @ 1646300.800)
				)[10m:]
			)
			or
			max_over_time(
				stddev_over_time(
					deriv(
						rate(http_requests_total[10m] @ 1546300.800)
					[5m:1m])
				[2m:])
			[10m:])`, nil,
		},
		{"sum by (foo) (bar[buzz])", "foo{}", apierror.New(apierror.TypeBadData, `1:19: parse error: bad duration syntax: ""`)},
	} {
		tt := tt
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			expectedExpr, err := parser.ParseExpr(tt.expected)
			require.NoError(t, err)
			out, err := evaluateAtModifierFunction(tt.in, start, end)
			if tt.err != nil {
				require.Equal(t, tt.err, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, expectedExpr.String(), out)
		})
	}
}

func TestSplitByInterval_WrapMultipleTimes(t *testing.T) {
	interval := func(_ Request) time.Duration { return 24 * time.Hour }
	m := SplitByIntervalMiddleware(interval, mockLimits{}, PrometheusCodec, prometheus.NewRegistry())
	require.NotPanics(t, func() {
		m.Wrap(mockHandlerWith(nil, nil))
		m.Wrap(mockHandlerWith(nil, nil))
	})
}
