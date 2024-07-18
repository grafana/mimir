// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestErrorCachingHandler_Do(t *testing.T) {
	newDefaultRequest := func() *PrometheusRangeQueryRequest {
		return &PrometheusRangeQueryRequest{
			queryExpr: parseQuery(t, "up"),
			start:     100,
			end:       200,
			step:      10,
		}
	}

	runHandler := func(ctx context.Context, inner MetricsQueryHandler, c cache.Cache, req MetricsQueryRequest) (Response, error) {
		limits := &mockLimits{resultsCacheTTLForErrors: time.Minute}
		middleware := newErrorCachingMiddleware(c, limits, resultsCacheEnabledByOption, test.NewTestingLogger(t), prometheus.NewPedanticRegistry())
		handler := middleware.Wrap(inner)
		return handler.Do(ctx, req)
	}

	t.Run("no user set", func(t *testing.T) {
		c := cache.NewInstrumentedMockCache()

		innerRes := newEmptyPrometheusResponse()
		inner := &mockHandler{}
		inner.On("Do", mock.Anything, mock.Anything).Return(innerRes, nil)

		ctx := context.Background()
		req := newDefaultRequest()
		res, err := runHandler(ctx, inner, c, req)

		require.NoError(t, err)
		require.Equal(t, innerRes, res)
		require.Equal(t, 0, c.CountFetchCalls())
		require.Equal(t, 0, c.CountStoreCalls())
	})

	t.Run("disabled by option", func(t *testing.T) {
		c := cache.NewInstrumentedMockCache()

		innerRes := newEmptyPrometheusResponse()
		inner := &mockHandler{}
		inner.On("Do", mock.Anything, mock.Anything).Return(innerRes, nil)

		ctx := user.InjectOrgID(context.Background(), "1234")
		req := newDefaultRequest()
		req.options = Options{
			CacheDisabled: true,
		}
		res, err := runHandler(ctx, inner, c, req)

		require.NoError(t, err)
		require.Equal(t, innerRes, res)
		require.Equal(t, 0, c.CountFetchCalls())
		require.Equal(t, 0, c.CountStoreCalls())
	})

	t.Run("cache hit", func(t *testing.T) {
		c := cache.NewInstrumentedMockCache()

		innerRes := newEmptyPrometheusResponse()
		inner := &mockHandler{}
		inner.On("Do", mock.Anything, mock.Anything).Return(innerRes, nil)

		ctx := user.InjectOrgID(context.Background(), "1234")
		req := newDefaultRequest()
		key := errorCachingKey("1234", req)
		bytes, err := proto.Marshal(&CachedError{
			Key:          key,
			ErrorType:    string(apierror.TypeExec),
			ErrorMessage: "limits error",
		})
		require.NoError(t, err)

		// NOTE: We rely on this mock cache being synchronous
		c.SetAsync(cacheHashKey(key), bytes, time.Minute)
		res, err := runHandler(ctx, inner, c, req)

		require.Error(t, err)
		require.Nil(t, res)
		require.Equal(t, 1, c.CountFetchCalls())
		require.Equal(t, 1, c.CountStoreCalls())
	})

	t.Run("cache hit key collision", func(t *testing.T) {
		c := cache.NewInstrumentedMockCache()

		innerRes := newEmptyPrometheusResponse()
		inner := &mockHandler{}
		inner.On("Do", mock.Anything, mock.Anything).Return(innerRes, nil)

		ctx := user.InjectOrgID(context.Background(), "1234")
		req := newDefaultRequest()

		key := errorCachingKey("1234", req)
		bytes, err := proto.Marshal(&CachedError{
			Key:          "different key that is stored under the same hashed key",
			ErrorType:    string(apierror.TypeExec),
			ErrorMessage: "limits error",
		})
		require.NoError(t, err)

		// NOTE: We rely on this mock cache being synchronous
		c.SetAsync(cacheHashKey(key), bytes, time.Minute)
		res, err := runHandler(ctx, inner, c, req)

		require.NoError(t, err)
		require.Equal(t, innerRes, res)
		require.Equal(t, 1, c.CountFetchCalls())
		require.Equal(t, 1, c.CountStoreCalls())
	})

	t.Run("corrupt cache data", func(t *testing.T) {
		c := cache.NewInstrumentedMockCache()

		innerRes := newEmptyPrometheusResponse()
		inner := &mockHandler{}
		inner.On("Do", mock.Anything, mock.Anything).Return(innerRes, nil)

		ctx := user.InjectOrgID(context.Background(), "1234")
		req := newDefaultRequest()

		key := errorCachingKey("1234", req)
		bytes := []byte{0x0, 0x0, 0x0, 0x0}

		// NOTE: We rely on this mock cache being synchronous
		c.SetAsync(cacheHashKey(key), bytes, time.Minute)
		res, err := runHandler(ctx, inner, c, req)

		require.NoError(t, err)
		require.Equal(t, innerRes, res)
		require.Equal(t, 1, c.CountFetchCalls())
		require.Equal(t, 1, c.CountStoreCalls())
	})

	t.Run("cache miss no error", func(t *testing.T) {
		c := cache.NewInstrumentedMockCache()

		innerRes := newEmptyPrometheusResponse()
		inner := &mockHandler{}
		inner.On("Do", mock.Anything, mock.Anything).Return(innerRes, nil)

		ctx := user.InjectOrgID(context.Background(), "1234")
		req := newDefaultRequest()
		res, err := runHandler(ctx, inner, c, req)

		require.NoError(t, err)
		require.Equal(t, innerRes, res)
		require.Equal(t, 1, c.CountFetchCalls())
		require.Equal(t, 0, c.CountStoreCalls())
	})

	t.Run("non-api error", func(t *testing.T) {
		c := cache.NewInstrumentedMockCache()

		innerErr := errors.New("something unexpected happened")
		inner := &mockHandler{}
		inner.On("Do", mock.Anything, mock.Anything).Return((*PrometheusResponse)(nil), innerErr)

		ctx := user.InjectOrgID(context.Background(), "1234")
		req := newDefaultRequest()
		res, err := runHandler(ctx, inner, c, req)

		require.Error(t, err)
		require.Nil(t, res)
		require.Equal(t, 1, c.CountFetchCalls())
		require.Equal(t, 0, c.CountStoreCalls())
	})

	t.Run("api error not cachable", func(t *testing.T) {
		c := cache.NewInstrumentedMockCache()

		innerErr := apierror.New(apierror.TypeUnavailable, "service unavailable")
		inner := &mockHandler{}
		inner.On("Do", mock.Anything, mock.Anything).Return((*PrometheusResponse)(nil), innerErr)

		ctx := user.InjectOrgID(context.Background(), "1234")
		req := newDefaultRequest()
		res, err := runHandler(ctx, inner, c, req)

		require.Error(t, err)
		require.Nil(t, res)
		require.Equal(t, 1, c.CountFetchCalls())
		require.Equal(t, 0, c.CountStoreCalls())
	})

	t.Run("api error cachable", func(t *testing.T) {
		c := cache.NewInstrumentedMockCache()

		innerErr := apierror.New(apierror.TypeTooLargeEntry, "response is too big")
		inner := &mockHandler{}
		inner.On("Do", mock.Anything, mock.Anything).Return((*PrometheusResponse)(nil), innerErr)

		ctx := user.InjectOrgID(context.Background(), "1234")
		req := newDefaultRequest()
		res, err := runHandler(ctx, inner, c, req)

		require.Error(t, err)
		require.Nil(t, res)
		require.Equal(t, 1, c.CountFetchCalls())
		require.Equal(t, 1, c.CountStoreCalls())
	})
}
