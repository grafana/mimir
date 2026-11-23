// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/retry_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"errors"
	fmt "fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/querier/stats"
)

func TestRetry(t *testing.T) {
	var try atomic.Int32

	errBadRequest := httpgrpc.ErrorFromHTTPResponse(&httpgrpc.HTTPResponse{
		Code: http.StatusBadRequest,
		Body: []byte("Bad Request"),
	})
	errUnprocessable := apierror.New(apierror.TypeBadData, "invalid expression type \"range vector\" for range query, must be Scalar or instant Vector\"")
	errInternal := httpgrpc.ErrorFromHTTPResponse(&httpgrpc.HTTPResponse{
		Code: http.StatusInternalServerError,
		Body: []byte("Internal Server Error"),
	})

	for _, tc := range []struct {
		name            string
		handler         MetricsQueryHandler
		resp            Response
		err             error
		expectedRetries int
	}{
		{
			name:            "retry failures",
			expectedRetries: 4,
			handler: HandlerFunc(func(context.Context, MetricsQueryRequest) (Response, error) {
				if try.Inc() == 5 {
					return &PrometheusResponse{Status: "Hello World"}, nil
				}
				return nil, fmt.Errorf("fail")
			}),
			resp: &PrometheusResponse{Status: "Hello World"},
		},
		{
			name:            "don't retry 400s",
			expectedRetries: 0,
			handler: HandlerFunc(func(context.Context, MetricsQueryRequest) (Response, error) {
				return nil, errBadRequest
			}),
			err: errBadRequest,
		},
		{
			name:            "don't retry bad-data",
			expectedRetries: 0,
			handler: HandlerFunc(func(context.Context, MetricsQueryRequest) (Response, error) {
				return nil, errUnprocessable
			}),
			err: errUnprocessable,
		},
		{
			name:            "retry 500s",
			expectedRetries: 5,
			handler: HandlerFunc(func(context.Context, MetricsQueryRequest) (Response, error) {
				return nil, errInternal
			}),
			err: errInternal,
		},
		{
			name:            "last error",
			expectedRetries: 4,
			handler: HandlerFunc(func(context.Context, MetricsQueryRequest) (Response, error) {
				if try.Inc() == 5 {
					return nil, errBadRequest
				}
				return nil, errInternal
			}),
			err: errBadRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			try.Store(0)
			mockMetrics := mockRetryMetrics{}
			h := newRetryMiddleware(log.NewNopLogger(), 5, &mockMetrics).Wrap(tc.handler)
			stats, ctx := stats.ContextWithEmptyStats(context.Background())
			resp, err := h.Do(ctx, nil)
			require.Equal(t, tc.err, err)
			require.Equal(t, tc.resp, resp)
			require.Equal(t, float64(tc.expectedRetries), mockMetrics.retries)
			require.Equal(t, uint32(tc.expectedRetries), stats.LoadRetries())
		})
	}
}

type mockRetryMetrics struct {
	retries float64
}

func (c *mockRetryMetrics) Observe(v float64) {
	c.retries = v
}

func Test_RetryMiddlewareCancel(t *testing.T) {
	var try atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := newRetryMiddleware(log.NewNopLogger(), 5, nil).Wrap(
		HandlerFunc(func(context.Context, MetricsQueryRequest) (Response, error) {
			try.Inc()
			return nil, ctx.Err()
		}),
	).Do(ctx, nil)
	require.Equal(t, int32(0), try.Load())
	require.Equal(t, ctx.Err(), err)

	ctx, cancel = context.WithCancel(context.Background())
	_, err = newRetryMiddleware(log.NewNopLogger(), 5, nil).Wrap(
		HandlerFunc(func(context.Context, MetricsQueryRequest) (Response, error) {
			try.Inc()
			cancel()
			return nil, errors.New("failed")
		}),
	).Do(ctx, nil)
	require.Equal(t, int32(1), try.Load())
	require.Equal(t, ctx.Err(), err)
}

func Test_RetryRoundTripper(t *testing.T) {
	var try atomic.Int32

	errBadRequest := httpgrpc.ErrorFromHTTPResponse(&httpgrpc.HTTPResponse{
		Code: http.StatusBadRequest,
		Body: []byte("Bad Request"),
	})

	errInternal := httpgrpc.ErrorFromHTTPResponse(&httpgrpc.HTTPResponse{
		Code: http.StatusInternalServerError,
		Body: []byte("Internal Server Error"),
	})

	for _, tc := range []struct {
		name            string
		handler         http.RoundTripper
		resp            *http.Response
		err             error
		expectedRetries int
	}{
		{
			name:            "retry failures",
			expectedRetries: 4,
			handler: RoundTripFunc(func(*http.Request) (*http.Response, error) {
				if try.Inc() == 5 {
					return &http.Response{StatusCode: http.StatusOK}, nil
				}
				return nil, fmt.Errorf("fail")
			}),
			resp: &http.Response{StatusCode: http.StatusOK},
		},
		{
			name:            "don't retry 400s",
			expectedRetries: 0,
			handler: RoundTripFunc(func(*http.Request) (*http.Response, error) {
				return nil, errBadRequest
			}),
			err: errBadRequest,
		},
		{
			name:            "retry 500s",
			expectedRetries: 5,
			handler: RoundTripFunc(func(*http.Request) (*http.Response, error) {
				return nil, errInternal
			}),
			err: errInternal,
		},
		{
			name:            "last error",
			expectedRetries: 4,
			handler: RoundTripFunc(func(*http.Request) (*http.Response, error) {
				if try.Inc() == 5 {
					return nil, errBadRequest
				}
				return nil, errInternal
			}),
			err: errBadRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			try.Store(0)
			mockMetrics := mockRetryMetrics{}
			rt := newRetryRoundTripper(tc.handler, log.NewNopLogger(), 5, &mockMetrics)
			stats, ctx := stats.ContextWithEmptyStats(context.Background())
			resp, err := rt.RoundTrip((&http.Request{}).WithContext(ctx))
			require.Equal(t, tc.err, err)
			require.Equal(t, tc.resp, resp)
			require.Equal(t, float64(tc.expectedRetries), mockMetrics.retries)
			require.Equal(t, uint32(tc.expectedRetries), stats.LoadRetries())
		})
	}
}

func TestRetryRoundTripper_ResendsRequestBody(t *testing.T) {
	const body = "match[]=up&start=0&end=3600"
	errInternal := httpgrpc.ErrorFromHTTPResponse(&httpgrpc.HTTPResponse{Code: http.StatusInternalServerError})

	var tries int
	next := RoundTripFunc(func(r *http.Request) (*http.Response, error) {
		tries++
		// Forwarding the request to the querier reads its body, so do the same.
		got, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, body, string(got), "try %d", tries)
		if tries < 3 {
			return nil, errInternal
		}
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	})

	req := httptest.NewRequest(http.MethodPost, "/prometheus/api/v1/labels", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := newRetryRoundTripper(next, log.NewNopLogger(), 5, nil).RoundTrip(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, 3, tries)
}
