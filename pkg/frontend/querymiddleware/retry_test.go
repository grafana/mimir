// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/retry_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"errors"
	fmt "fmt"
	"net/http"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/services"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/util"
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

	errNotRunningNew := util.NotRunningError{Component: "frontend", State: services.New}
	errNotRunningStarting := util.NotRunningError{Component: "frontend", State: services.Starting}
	errNotRunningStopping := util.NotRunningError{Component: "frontend", State: services.Stopping}
	errNotRunningTerminated := util.NotRunningError{Component: "frontend", State: services.Terminated}
	errNotRunningFailed := util.NotRunningError{Component: "frontend", State: services.Failed}

	for _, tc := range []struct {
		name            string
		handler         Handler
		resp            Response
		err             error
		expectedRetries int
		expectBackoff   bool
	}{
		{
			name:            "retry failures",
			expectedRetries: 4,
			handler: HandlerFunc(func(_ context.Context, req Request) (Response, error) {
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
			handler: HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				return nil, errBadRequest
			}),
			err: errBadRequest,
		},
		{
			name:            "don't retry bad-data",
			expectedRetries: 0,
			handler: HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				return nil, errUnprocessable
			}),
			err: errUnprocessable,
		},
		{
			name:            "retry 500s",
			expectedRetries: 5,
			handler: HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				return nil, errInternal
			}),
			err: errInternal,
		},
		{
			name:            "last error",
			expectedRetries: 4,
			handler: HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				if try.Inc() == 5 {
					return nil, errBadRequest
				}
				return nil, errInternal
			}),
			err: errBadRequest,
		},
		{
			name:            "retry 'not running: new' errors with backoff",
			expectedRetries: 5,
			expectBackoff:   true,
			handler: HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				return nil, errNotRunningNew
			}),
			err: errNotRunningNew,
		},
		{
			name:            "retry 'not running: starting' errors with backoff",
			expectedRetries: 5,
			expectBackoff:   true,
			handler: HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				return nil, errNotRunningStarting
			}),
			err: errNotRunningStarting,
		},
		{
			name:            "don't retry 'not running: stopping' errors",
			expectedRetries: 0,
			handler: HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				return nil, errNotRunningStopping
			}),
			err: errNotRunningStopping,
		},
		{
			name:            "don't retry 'not running: terminated' errors",
			expectedRetries: 0,
			handler: HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				return nil, errNotRunningTerminated
			}),
			err: errNotRunningTerminated,
		},
		{
			name:            "don't retry 'not running: failed' errors",
			expectedRetries: 0,
			handler: HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				return nil, errNotRunningFailed
			}),
			err: errNotRunningFailed,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			try.Store(0)

			maxRetries := 5
			localErrorBackoff := 100 * time.Millisecond
			lastCall := time.UnixMilli(0)
			mockMetrics := mockRetryMetrics{}
			handler := tc.handler

			if tc.expectBackoff {
				handler = HandlerFunc(func(ctx context.Context, req Request) (Response, error) {
					require.Greater(t, time.Since(lastCall), localErrorBackoff, "expected to be called with backoff")
					lastCall = time.Now()
					return tc.handler.Do(ctx, req)
				})
			}

			startTime := time.Now()
			h := newRetryMiddleware(log.NewNopLogger(), maxRetries, localErrorBackoff, &mockMetrics).Wrap(handler)
			resp, err := h.Do(context.Background(), nil)
			duration := time.Since(startTime)

			require.Equal(t, tc.err, err)
			require.Equal(t, tc.resp, resp)
			require.Equal(t, float64(tc.expectedRetries), mockMetrics.retries)

			if tc.expectBackoff {
				require.Less(t, duration, localErrorBackoff*time.Duration(maxRetries), "expected to wait four times, once between each attempt, but waited five times or more")
			}
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
	_, err := newRetryMiddleware(log.NewNopLogger(), 5, 0, nil).Wrap(
		HandlerFunc(func(c context.Context, r Request) (Response, error) {
			try.Inc()
			return nil, ctx.Err()
		}),
	).Do(ctx, nil)
	require.Equal(t, int32(0), try.Load())
	require.Equal(t, ctx.Err(), err)

	ctx, cancel = context.WithCancel(context.Background())
	_, err = newRetryMiddleware(log.NewNopLogger(), 5, 0, nil).Wrap(
		HandlerFunc(func(c context.Context, r Request) (Response, error) {
			try.Inc()
			cancel()
			return nil, errors.New("failed")
		}),
	).Do(ctx, nil)
	require.Equal(t, int32(1), try.Load())
	require.Equal(t, ctx.Err(), err)
}
