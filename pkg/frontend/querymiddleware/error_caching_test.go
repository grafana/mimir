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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestErrorCachingHandler_Do(t *testing.T) {
	keyGen := NewDefaultCacheKeyGenerator(newTestCodec(), time.Second)

	runHandler := func(ctx context.Context, inner MetricsQueryHandler, c cache.Cache, req MetricsQueryRequest) (Response, error) {
		limits := &mockLimits{resultsCacheTTLForErrors: time.Minute}
		middleware := newErrorCachingMiddleware(c, limits, resultsCacheEnabledByOption, keyGen, test.NewTestingLogger(t), prometheus.NewPedanticRegistry())
		handler := middleware.Wrap(inner)
		return handler.Do(ctx, req)
	}

	testCases := map[string]struct {
		newQueryRequest func(Options) MetricsQueryRequest
	}{
		"range query": {
			newQueryRequest: func(o Options) MetricsQueryRequest {
				return &PrometheusRangeQueryRequest{
					queryExpr: parseQuery(t, "up"),
					start:     100,
					end:       200,
					step:      10,
					options:   o,
				}
			},
		},
		"instant query": {
			newQueryRequest: func(o Options) MetricsQueryRequest {
				return &PrometheusInstantQueryRequest{
					queryExpr: parseQuery(t, "up"),
					time:      100,
					options:   o,
				}
			},
		},
	}

	for testCaseName, tc := range testCases {
		t.Run(testCaseName, func(t *testing.T) {
			t.Run("no user set", func(t *testing.T) {
				c := cache.NewInstrumentedMockCache()

				innerRes := NewEmptyPrometheusResponse()
				inner := &mockHandler{}
				inner.On("Do", mock.Anything, mock.Anything).Return(innerRes, nil)

				ctx := context.Background()
				req := tc.newQueryRequest(Options{})
				res, err := runHandler(ctx, inner, c, req)

				require.NoError(t, err)
				require.Equal(t, innerRes, res)
				require.Equal(t, 0, c.CountFetchCalls())
				require.Equal(t, 0, c.CountStoreCalls())
			})

			t.Run("disabled by option", func(t *testing.T) {
				c := cache.NewInstrumentedMockCache()

				innerRes := NewEmptyPrometheusResponse()
				inner := &mockHandler{}
				inner.On("Do", mock.Anything, mock.Anything).Return(innerRes, nil)

				ctx := user.InjectOrgID(context.Background(), "1234")
				req := tc.newQueryRequest(Options{
					CacheDisabled: true,
				})
				res, err := runHandler(ctx, inner, c, req)

				require.NoError(t, err)
				require.Equal(t, innerRes, res)
				require.Equal(t, 0, c.CountFetchCalls())
				require.Equal(t, 0, c.CountStoreCalls())
			})

			t.Run("cache hit", func(t *testing.T) {
				c := cache.NewInstrumentedMockCache()

				inner := &mockHandler{}

				ctx := user.InjectOrgID(context.Background(), "1234")
				req := tc.newQueryRequest(Options{})
				key := keyGen.QueryRequestError(ctx, "1234", req)
				bytes, err := proto.Marshal(&CachedError{
					Key:          key,
					ErrorType:    string(apierror.TypeExec),
					ErrorMessage: "limits error",
				})
				require.NoError(t, err)

				require.NoError(t, c.Set(ctx, hashCacheKey(key), bytes, time.Minute))

				res, err := runHandler(ctx, inner, c, req)

				require.Error(t, err)
				require.Nil(t, res)
				require.Equal(t, 1, c.CountFetchCalls())
				require.Equal(t, 1, c.CountStoreCalls())
			})

			t.Run("cache hit key collision", func(t *testing.T) {
				c := cache.NewInstrumentedMockCache()

				innerRes := NewEmptyPrometheusResponse()
				inner := &mockHandler{}
				inner.On("Do", mock.Anything, mock.Anything).Return(innerRes, nil)

				ctx := user.InjectOrgID(context.Background(), "1234")
				req := tc.newQueryRequest(Options{})

				key := keyGen.QueryRequestError(ctx, "1234", req)
				bytes, err := proto.Marshal(&CachedError{
					Key:          "different key that is stored under the same hashed key",
					ErrorType:    string(apierror.TypeExec),
					ErrorMessage: "limits error",
				})
				require.NoError(t, err)

				require.NoError(t, c.Set(ctx, hashCacheKey(key), bytes, time.Minute))

				res, err := runHandler(ctx, inner, c, req)

				require.NoError(t, err)
				require.Equal(t, innerRes, res)
				require.Equal(t, 1, c.CountFetchCalls())
				require.Equal(t, 1, c.CountStoreCalls())
			})

			t.Run("corrupt cache data", func(t *testing.T) {
				c := cache.NewInstrumentedMockCache()

				innerRes := NewEmptyPrometheusResponse()
				inner := &mockHandler{}
				inner.On("Do", mock.Anything, mock.Anything).Return(innerRes, nil)

				ctx := user.InjectOrgID(context.Background(), "1234")
				req := tc.newQueryRequest(Options{})

				key := keyGen.QueryRequestError(ctx, "1234", req)
				bytes := []byte{0x0, 0x0, 0x0, 0x0}
				require.NoError(t, c.Set(ctx, hashCacheKey(key), bytes, time.Minute))

				res, err := runHandler(ctx, inner, c, req)

				require.NoError(t, err)
				require.Equal(t, innerRes, res)
				require.Equal(t, 1, c.CountFetchCalls())
				require.Equal(t, 1, c.CountStoreCalls())
			})

			t.Run("cache miss no error", func(t *testing.T) {
				c := cache.NewInstrumentedMockCache()

				innerRes := NewEmptyPrometheusResponse()
				inner := &mockHandler{}
				inner.On("Do", mock.Anything, mock.Anything).Return(innerRes, nil)

				ctx := user.InjectOrgID(context.Background(), "1234")
				req := tc.newQueryRequest(Options{})
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
				req := tc.newQueryRequest(Options{})
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
				req := tc.newQueryRequest(Options{})
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
				req := tc.newQueryRequest(Options{})
				res, err := runHandler(ctx, inner, c, req)

				require.Error(t, err)
				require.Nil(t, res)
				require.Equal(t, 1, c.CountFetchCalls())
				require.Equal(t, 1, c.CountStoreCalls())
			})
		})
	}
}

func TestErrorCachingHandler_Do_subsequentInstantQueries(t *testing.T) {
	keyGen := NewDefaultCacheKeyGenerator(newTestCodec(), 0)

	testHandler := func(ctx context.Context, inner MetricsQueryHandler, c cache.Cache, req MetricsQueryRequest) (Response, error) {
		limits := &mockLimits{resultsCacheTTLForErrors: time.Minute}
		middleware := newErrorCachingMiddleware(c, limits, resultsCacheEnabledByOption, keyGen, test.NewTestingLogger(t), prometheus.NewPedanticRegistry())
		handler := middleware.Wrap(inner)
		return handler.Do(ctx, req)
	}

	newInstantQueryRequest := func(t *testing.T, ts int64) *PrometheusInstantQueryRequest {
		return &PrometheusInstantQueryRequest{
			queryExpr: parseQuery(t, "up"),
			time:      ts,
		}
	}

	c := cache.NewInstrumentedMockCache()

	innerErr := apierror.New(apierror.TypeTooLargeEntry, "response is too big")
	inner := &mockHandler{}
	inner.On("Do", mock.Anything, mock.Anything).
		Once(). // The handler is queried once; the rest of the requests are served from the cache.
		Return((*PrometheusResponse)(nil), innerErr)

	ctx := user.InjectOrgID(context.Background(), "1234")

	// First query.
	req1 := newInstantQueryRequest(t, 100)
	res, err := testHandler(ctx, inner, c, req1)
	require.ErrorIs(t, err, innerErr) // The first query gets the error from the inner handler.
	require.Nil(t, res)
	require.Equal(t, 1, c.CountFetchCalls())
	require.Equal(t, 1, c.CountStoreCalls())

	// Second query with the updated timestamp.
	req2 := newInstantQueryRequest(t, 200)
	res, err = testHandler(ctx, inner, c, req2)
	require.Error(t, err)
	// Test that for the second query, the error we got from the cache is of the expected type.
	if wantErr := new(apierror.APIError); assert.ErrorAs(t, err, &wantErr) {
		require.Equal(t, apierror.TypeTooLargeEntry, wantErr.Type)
	}

	require.Nil(t, res)
	require.Equal(t, 2, c.CountFetchCalls())
	require.Equal(t, 1, c.CountStoreCalls())
}
