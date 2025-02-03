// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import (
	"context"
	"errors"
	"testing"

	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/api"
)

func TestGrpcInflightMethodLimiter(t *testing.T) {
	t.Run("nil ingester and distributor receiver", func(t *testing.T) {
		l := newGrpcInflightMethodLimiter(func() ingesterReceiver { return nil }, func() pushReceiver { return nil })

		ctx, err := l.RPCCallStarting(context.Background(), "test", nil)
		require.NoError(t, err)
		require.NotPanics(t, func() {
			l.RPCCallFinished(ctx)
		})

		ctx, err = l.RPCCallStarting(context.Background(), ingesterPushMethod, nil)
		require.ErrorIs(t, err, errNoIngester)

		ctx, err = l.RPCCallStarting(context.Background(), httpgrpcHandleMethod, metadata.Pairs(httpgrpc.MetadataMethod, "POST", httpgrpc.MetadataURL, api.PrometheusPushEndpoint))
		require.ErrorIs(t, err, errNoDistributor)

		require.Panics(t, func() {
			// In practice, this will not be called, since l.RPCCallStarting() for ingester push returns error if there's no ingester.
			l.RPCCallFinished(context.WithValue(ctx, rpcCallCtxKey, rpcCallIngesterPush))
		})

		require.Panics(t, func() {
			// In practice, this will not be called, since l.RPCCallStarting() distributor push returns error if there's no distributor.
			l.RPCCallFinished(context.WithValue(ctx, rpcCallCtxKey, rpcCallDistributorPush))
		})
	})

	t.Run("ingester push receiver, wrong method name", func(t *testing.T) {
		m := &mockIngesterReceiver{}
		l := newGrpcInflightMethodLimiter(func() ingesterReceiver { return m }, nil)

		ctx, err := l.RPCCallStarting(context.Background(), "test", nil)
		require.NoError(t, err)
		require.NotPanics(t, func() {
			l.RPCCallFinished(ctx)
		})
		require.Equal(t, 0, m.startPushCalls)
		require.Equal(t, 0, m.preparePushCalls)
		require.Equal(t, 0, m.finishPushCalls)
	})

	t.Run("ingester push receiver, check returns error", func(t *testing.T) {
		m := &mockIngesterReceiver{}
		l := newGrpcInflightMethodLimiter(func() ingesterReceiver { return m }, nil)

		m.returnError = errors.New("hello there")
		ctx, err := l.RPCCallStarting(context.Background(), ingesterPushMethod, nil)
		require.Error(t, err)
		_, ok := grpcutil.ErrorToStatus(err)
		require.True(t, ok)
		require.Nil(t, ctx.Value(rpcCallCtxKey)) // Original context expected in case of errors.
	})

	t.Run("ingester push receiver, without size", func(t *testing.T) {
		m := &mockIngesterReceiver{}
		l := newGrpcInflightMethodLimiter(func() ingesterReceiver { return m }, nil)

		ctx, err := l.RPCCallStarting(context.Background(), ingesterPushMethod, nil)
		require.NoError(t, err)
		require.Equal(t, 1, m.startPushCalls)
		require.Equal(t, int64(0), m.startPushBytes)
		require.Equal(t, 0, m.preparePushCalls)
		require.Equal(t, 0, m.finishPushCalls)
		require.Equal(t, int64(0), m.finishPushBytes)
		_, err = l.RPCCallProcessing(ctx, ingesterPushMethod)
		require.NoError(t, err)
		require.Equal(t, 1, m.preparePushCalls)
		require.NotPanics(t, func() {
			l.RPCCallFinished(ctx)
		})
		require.Equal(t, 1, m.startPushCalls)
		require.Equal(t, int64(0), m.startPushBytes)
		require.Equal(t, 1, m.finishPushCalls)
		require.Equal(t, int64(0), m.finishPushBytes)
	})

	t.Run("ingester push receiver, with size provided", func(t *testing.T) {
		m := &mockIngesterReceiver{}
		l := newGrpcInflightMethodLimiter(func() ingesterReceiver { return m }, nil)

		ctx, err := l.RPCCallStarting(context.Background(), ingesterPushMethod, metadata.New(map[string]string{
			grpcutil.MetadataMessageSize: "123456",
		}))
		require.NoError(t, err)
		require.Equal(t, 1, m.startPushCalls)
		require.Equal(t, int64(123456), m.startPushBytes)
		require.Equal(t, 0, m.preparePushCalls)
		require.Equal(t, 0, m.finishPushCalls)
		require.Equal(t, int64(0), m.finishPushBytes)
		_, err = l.RPCCallProcessing(ctx, ingesterPushMethod)
		require.NoError(t, err)
		require.Equal(t, 1, m.preparePushCalls)
		require.NotPanics(t, func() {
			l.RPCCallFinished(ctx)
		})
		require.Equal(t, 1, m.startPushCalls)
		require.Equal(t, int64(123456), m.startPushBytes)
		require.Equal(t, 1, m.finishPushCalls)
		require.Equal(t, int64(123456), m.finishPushBytes)
	})

	t.Run("ingester push receiver, with wrong size", func(t *testing.T) {
		m := &mockIngesterReceiver{}
		l := newGrpcInflightMethodLimiter(func() ingesterReceiver { return m }, nil)

		ctx, err := l.RPCCallStarting(context.Background(), ingesterPushMethod, metadata.New(map[string]string{
			grpcutil.MetadataMessageSize: "wrong",
		}))
		require.NoError(t, err)
		require.Equal(t, 1, m.startPushCalls)
		require.Equal(t, int64(0), m.startPushBytes)
		require.Equal(t, 0, m.preparePushCalls)
		require.Equal(t, 0, m.finishPushCalls)
		require.Equal(t, int64(0), m.finishPushBytes)
		_, err = l.RPCCallProcessing(ctx, ingesterPushMethod)
		require.NoError(t, err)
		require.Equal(t, 1, m.preparePushCalls)
		require.NotPanics(t, func() {
			l.RPCCallFinished(ctx)
		})
		require.Equal(t, 1, m.startPushCalls)
		require.Equal(t, int64(0), m.startPushBytes)
		require.Equal(t, 1, m.finishPushCalls)
		require.Equal(t, int64(0), m.finishPushBytes)
	})

	t.Run("ingester read receiver, check returns error", func(t *testing.T) {
		m := &mockIngesterReceiver{}
		l := newGrpcInflightMethodLimiter(func() ingesterReceiver { return m }, nil)

		m.returnError = errors.New("hello there")
		ctx, err := l.RPCCallStarting(context.Background(), ingesterMethod, nil)
		require.Error(t, err)
		_, ok := grpcutil.ErrorToStatus(err)
		require.True(t, ok)
		require.Nil(t, ctx.Value(rpcCallCtxKey))
	})

	t.Run("ingester read receiver, without size", func(t *testing.T) {
		m := &mockIngesterReceiver{}
		l := newGrpcInflightMethodLimiter(func() ingesterReceiver { return m }, nil)

		ctx, err := l.RPCCallStarting(context.Background(), ingesterMethod, nil)
		require.NoError(t, err)
		require.Equal(t, 1, m.startReadCalls)
		require.Equal(t, 0, m.prepareReadCalls)
		require.Equal(t, 0, m.finishReadCalls)
		_, err = l.RPCCallProcessing(ctx, ingesterMethod)
		require.NoError(t, err)
		require.Equal(t, 1, m.prepareReadCalls)
		require.NotPanics(t, func() {
			l.RPCCallFinished(ctx)
		})
		require.Equal(t, 1, m.finishReadCalls)
	})

	t.Run("distributor push via httpgrpc", func(t *testing.T) {
		m := &mockDistributorReceiver{}

		l := newGrpcInflightMethodLimiter(nil, func() pushReceiver { return m })

		ctx, err := l.RPCCallStarting(context.Background(), "test", nil)
		require.NoError(t, err)
		l.RPCCallFinished(ctx)
		require.Equal(t, 0, m.startCalls)
		require.Equal(t, 0, m.finishCalls)

		ctx, err = l.RPCCallStarting(context.Background(), httpgrpcHandleMethod, metadata.New(map[string]string{
			httpgrpc.MetadataMethod:      "POST",
			httpgrpc.MetadataURL:         api.PrometheusPushEndpoint, // no prefix
			grpcutil.MetadataMessageSize: "123456",
		}))
		require.NoError(t, err)
		require.Equal(t, 1, m.startCalls)
		require.Equal(t, 0, m.finishCalls)
		require.Equal(t, int64(123456), m.lastRequestSize)

		// calling finish with empty context does not do any Finish calls.
		l.RPCCallFinished(context.Background())
		require.Equal(t, 1, m.startCalls)
		require.Equal(t, 0, m.finishCalls)

		l.RPCCallFinished(ctx)
		require.Equal(t, 1, m.startCalls)
		require.Equal(t, 1, m.finishCalls)

		// Now return error from distributor receiver.
		m.returnError = errors.New("hello there")
		ctx, err = l.RPCCallStarting(context.Background(), httpgrpcHandleMethod, metadata.New(map[string]string{
			httpgrpc.MetadataMethod:      "POST",
			httpgrpc.MetadataURL:         api.PrometheusPushEndpoint,
			grpcutil.MetadataMessageSize: "123456",
		}))
		require.Error(t, err)
		_, ok := grpcutil.ErrorToStatus(err)
		require.True(t, ok)
		require.Nil(t, ctx.Value(rpcCallCtxKey)) // Original context expected in case of errors.
	})

	t.Run("distributor push via httpgrpc, GET", func(t *testing.T) {
		m := &mockDistributorReceiver{}

		l := newGrpcInflightMethodLimiter(nil, func() pushReceiver { return m })

		_, err := l.RPCCallStarting(context.Background(), httpgrpcHandleMethod, metadata.New(map[string]string{
			httpgrpc.MetadataMethod:      "GET",
			httpgrpc.MetadataURL:         "prefix" + api.PrometheusPushEndpoint,
			grpcutil.MetadataMessageSize: "123456",
		}))
		require.NoError(t, err)
		require.Equal(t, 0, m.startCalls)
		require.Equal(t, 0, m.finishCalls)
		require.Equal(t, int64(0), m.lastRequestSize)
	})

	t.Run("distributor push via httpgrpc, /hello", func(t *testing.T) {
		m := &mockDistributorReceiver{}
		l := newGrpcInflightMethodLimiter(nil, func() pushReceiver { return m })

		_, err := l.RPCCallStarting(context.Background(), httpgrpcHandleMethod, metadata.New(map[string]string{
			httpgrpc.MetadataMethod:      "POST",
			httpgrpc.MetadataURL:         "/hello",
			grpcutil.MetadataMessageSize: "123456",
		}))
		require.NoError(t, err)
		require.Equal(t, 0, m.startCalls)
		require.Equal(t, 0, m.finishCalls)
		require.Equal(t, int64(0), m.lastRequestSize)
	})

	t.Run("distributor push via httpgrpc, wrong message size", func(t *testing.T) {
		m := &mockDistributorReceiver{}
		l := newGrpcInflightMethodLimiter(nil, func() pushReceiver { return m })

		_, err := l.RPCCallStarting(context.Background(), httpgrpcHandleMethod, metadata.New(map[string]string{
			httpgrpc.MetadataMethod:      "POST",
			httpgrpc.MetadataURL:         "prefix" + api.OTLPPushEndpoint,
			grpcutil.MetadataMessageSize: "one-two-three",
		}))
		require.NoError(t, err)
		require.Equal(t, 1, m.startCalls)
		require.Equal(t, 0, m.finishCalls)
		require.Equal(t, int64(0), m.lastRequestSize)
	})
}

type mockIngesterReceiver struct {
	lastRequestSize int64

	startPushCalls   int
	startPushBytes   int64
	preparePushCalls int
	finishPushCalls  int
	finishPushBytes  int64
	returnError      error

	startReadCalls   int
	prepareReadCalls int
	finishReadCalls  int
}

func (i *mockIngesterReceiver) StartPushRequest(ctx context.Context, size int64) (context.Context, error) {
	i.lastRequestSize = size
	i.startPushCalls++
	i.startPushBytes += size
	return ctx, i.returnError
}

func (i *mockIngesterReceiver) PreparePushRequest(_ context.Context) (func(error), error) {
	i.preparePushCalls++
	return nil, i.returnError
}

func (i *mockIngesterReceiver) FinishPushRequest(_ context.Context) {
	i.finishPushCalls++
	i.finishPushBytes += i.lastRequestSize
}

func (i *mockIngesterReceiver) StartReadRequest(ctx context.Context) (context.Context, error) {
	i.startReadCalls++
	return ctx, i.returnError
}

func (i *mockIngesterReceiver) PrepareReadRequest(_ context.Context) (func(error), error) {
	i.prepareReadCalls++
	return nil, i.returnError
}

func (i *mockIngesterReceiver) FinishReadRequest(_ context.Context) {
	i.finishReadCalls++
}

type mockDistributorReceiver struct {
	startCalls      int
	prepareCalls    int
	finishCalls     int
	lastRequestSize int64
	returnError     error
}

func (i *mockDistributorReceiver) StartPushRequest(ctx context.Context, requestSize int64) (context.Context, error) {
	i.startCalls++
	i.lastRequestSize = requestSize
	return ctx, i.returnError
}

func (i *mockDistributorReceiver) PreparePushRequest(_ context.Context) (func(error), error) {
	i.prepareCalls++
	return nil, i.returnError
}

func (i *mockDistributorReceiver) FinishPushRequest(_ context.Context) {
	i.finishCalls++
}
