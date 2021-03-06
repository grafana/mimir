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

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"go.uber.org/atomic"
)

func TestRetry(t *testing.T) {
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
		name    string
		handler Handler
		resp    Response
		err     error
	}{
		{
			name: "retry failures",
			handler: HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				if try.Inc() == 5 {
					return &PrometheusResponse{Status: "Hello World"}, nil
				}
				return nil, fmt.Errorf("fail")
			}),
			resp: &PrometheusResponse{Status: "Hello World"},
		},
		{
			name: "don't retry 400s",
			handler: HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				return nil, errBadRequest
			}),
			err: errBadRequest,
		},
		{
			name: "retry 500s",
			handler: HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				return nil, errInternal
			}),
			err: errInternal,
		},
		{
			name: "last error",
			handler: HandlerFunc(func(_ context.Context, req Request) (Response, error) {
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
			h := newRetryMiddleware(log.NewNopLogger(), 5, nil).Wrap(tc.handler)
			resp, err := h.Do(context.Background(), nil)
			require.Equal(t, tc.err, err)
			require.Equal(t, tc.resp, resp)
		})
	}
}

func Test_RetryMiddlewareCancel(t *testing.T) {
	var try atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := newRetryMiddleware(log.NewNopLogger(), 5, nil).Wrap(
		HandlerFunc(func(c context.Context, r Request) (Response, error) {
			try.Inc()
			return nil, ctx.Err()
		}),
	).Do(ctx, nil)
	require.Equal(t, int32(0), try.Load())
	require.Equal(t, ctx.Err(), err)

	ctx, cancel = context.WithCancel(context.Background())
	_, err = newRetryMiddleware(log.NewNopLogger(), 5, nil).Wrap(
		HandlerFunc(func(c context.Context, r Request) (Response, error) {
			try.Inc()
			cancel()
			return nil, errors.New("failed")
		}),
	).Do(ctx, nil)
	require.Equal(t, int32(1), try.Load())
	require.Equal(t, ctx.Err(), err)
}
