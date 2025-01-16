// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestProm2RangeCompat_Do(t *testing.T) {
	newRangeRequest := func(q string) *PrometheusRangeQueryRequest {
		return &PrometheusRangeQueryRequest{
			queryExpr: parseQuery(t, q),
			start:     100,
			end:       200,
			step:      10,
		}
	}

	runHandler := func(ctx context.Context, inner MetricsQueryHandler, limits Limits, req MetricsQueryRequest) (Response, error) {
		middleware := newProm2RangeCompatMiddleware(limits, log.NewNopLogger())
		handler := middleware.Wrap(inner)
		return handler.Do(ctx, req)
	}

	t.Run("no user set", func(t *testing.T) {
		ctx := context.Background()
		req := newRangeRequest("sum(rate(some_series[1m:1m]))")

		innerRes := newEmptyPrometheusResponse()
		inner := &mockHandler{}
		inner.On("Do", mock.Anything, req).Return(innerRes, nil)

		limits := &mockLimits{prom2RangeCompat: true}
		res, err := runHandler(ctx, inner, limits, req)

		require.NoError(t, err)
		require.Equal(t, innerRes, res)
	})

	t.Run("disabled by config", func(t *testing.T) {
		ctx := user.InjectOrgID(context.Background(), "1234")
		req := newRangeRequest("sum(rate(some_series[1m:1m]))")

		innerRes := newEmptyPrometheusResponse()
		inner := &mockHandler{}
		inner.On("Do", mock.Anything, req).Return(innerRes, nil)

		limits := &mockLimits{prom2RangeCompat: false}
		res, err := runHandler(ctx, inner, limits, req)

		require.NoError(t, err)
		require.Equal(t, innerRes, res)
	})

	t.Run("no resolution", func(t *testing.T) {
		ctx := user.InjectOrgID(context.Background(), "1234")
		req := newRangeRequest("sum(rate(some_series[1m]))")

		innerRes := newEmptyPrometheusResponse()
		inner := &mockHandler{}
		inner.On("Do", mock.Anything, req).Return(innerRes, nil)

		limits := &mockLimits{prom2RangeCompat: true}
		res, err := runHandler(ctx, inner, limits, req)

		require.NoError(t, err)
		require.Equal(t, innerRes, res)
	})

	t.Run("query rewritten", func(t *testing.T) {
		ctx := user.InjectOrgID(context.Background(), "1234")
		orig := newRangeRequest("sum(rate(some_series[1m:1m]))")
		rewritten := newRangeRequest("sum(rate(some_series[1m1ms:1m]))")

		innerRes := newEmptyPrometheusResponse()
		inner := &mockHandler{}
		inner.On("Do", mock.Anything, mock.MatchedBy(func(req MetricsQueryRequest) bool {
			return req.GetQuery() == rewritten.GetQuery()
		})).Return(innerRes, nil)

		limits := &mockLimits{prom2RangeCompat: true}
		res, err := runHandler(ctx, inner, limits, orig)

		require.NoError(t, err)
		require.Equal(t, innerRes, res)
	})
}
