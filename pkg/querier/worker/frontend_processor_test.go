// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/worker/frontend_processor_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package worker

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/frontend/v1/frontendv1pb"
	"github.com/grafana/mimir/pkg/querier/stats"
)

func TestFrontendProcessor_processQueriesOnSingleStream(t *testing.T) {
	t.Run("should immediately return if worker context is canceled and there's no inflight query", func(t *testing.T) {
		fp, processClient, requestHandler := prepareFrontendProcessor()

		workerCtx, workerCancel := context.WithCancel(context.Background())

		processClient.On("Recv").Return(func() (*frontendv1pb.FrontendToClient, error) {
			// Simulate the querier received a SIGTERM while waiting for a query to execute.
			workerCancel()

			// No query to execute, so wait until terminated.
			<-processClient.Context().Done()
			return nil, toRPCErr(processClient.Context().Err())
		})

		requestHandler.On("Handle", mock.Anything, mock.Anything).Return(&httpgrpc.HTTPResponse{}, nil)

		fp.processQueriesOnSingleStream(workerCtx, nil, "127.0.0.1")

		// We expect at this point, the execution context has been canceled too.
		require.Error(t, processClient.Context().Err())

		// We expect Send() has not been called, because no query has been executed.
		processClient.AssertNumberOfCalls(t, "Send", 0)
	})

	t.Run("should wait until inflight query execution is completed before returning when worker context is canceled", func(t *testing.T) {
		fp, processClient, requestHandler := prepareFrontendProcessor()

		recvCount := atomic.NewInt64(0)

		processClient.On("Recv").Return(func() (*frontendv1pb.FrontendToClient, error) {
			switch recvCount.Inc() {
			case 1:
				return &frontendv1pb.FrontendToClient{
					Type:        frontendv1pb.HTTP_REQUEST,
					HttpRequest: nil,
				}, nil
			default:
				// No more messages to process, so waiting until terminated.
				<-processClient.Context().Done()
				return nil, toRPCErr(processClient.Context().Err())
			}
		})

		workerCtx, workerCancel := context.WithCancel(context.Background())

		requestHandler.On("Handle", mock.Anything, mock.Anything).Run(func(mock.Arguments) {
			// Cancel the worker context while the query execution is in progress.
			workerCancel()

			// Ensure the execution context hasn't been canceled yet.
			require.NoError(t, processClient.Context().Err())

			// Intentionally slow down the query execution, to double check the worker waits until done.
			time.Sleep(time.Second)
		}).Return(&httpgrpc.HTTPResponse{}, nil)

		startTime := time.Now()
		fp.processQueriesOnSingleStream(workerCtx, nil, "127.0.0.1")
		assert.GreaterOrEqual(t, time.Since(startTime), time.Second)

		// We expect at this point, the execution context has been canceled too.
		require.Error(t, processClient.Context().Err())

		// We expect Send() to be called once, to send the query result.
		processClient.AssertNumberOfCalls(t, "Send", 1)
	})
}

func TestFrontendProcessor_QueryTime(t *testing.T) {
	runTest := func(t *testing.T, statsEnabled bool) {
		fp, processClient, requestHandler := prepareFrontendProcessor()

		recvCount := atomic.NewInt64(0)
		queueTime := 3 * time.Second

		processClient.On("Recv").Return(func() (*frontendv1pb.FrontendToClient, error) {
			switch recvCount.Inc() {
			case 1:
				return &frontendv1pb.FrontendToClient{
					Type:           frontendv1pb.HTTP_REQUEST,
					HttpRequest:    nil,
					QueueTimeNanos: queueTime.Nanoseconds(),
					StatsEnabled:   statsEnabled,
				}, nil
			default:
				// No more messages to process, so waiting until terminated.
				<-processClient.Context().Done()
				return nil, toRPCErr(processClient.Context().Err())
			}
		})

		workerCtx, workerCancel := context.WithCancel(context.Background())

		requestHandler.On("Handle", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			workerCancel()

			stat := stats.FromContext(args.Get(0).(context.Context))

			if statsEnabled {
				require.Equal(t, queueTime, stat.LoadQueueTime())
			} else {
				require.Equal(t, time.Duration(0), stat.LoadQueueTime())
			}
		}).Return(&httpgrpc.HTTPResponse{}, nil)

		fp.processQueriesOnSingleStream(workerCtx, nil, "127.0.0.1")

		// We expect Send() to be called once, to send the query result.
		processClient.AssertNumberOfCalls(t, "Send", 1)
	}

	t.Run("query stats enabled should record query time", func(t *testing.T) {
		runTest(t, true)
	})

	t.Run("query stats disabled will not record query time", func(t *testing.T) {
		runTest(t, false)
	})
}

func TestRecvFailDoesntCancelProcess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// We use random port here, hopefully without any gRPC server.
	// nolint:staticcheck // grpc.DialContext() has been deprecated; we'll address it before upgrading to gRPC 2.
	cc, err := grpc.DialContext(ctx, "localhost:999", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	cfg := Config{}
	mgr := newFrontendProcessor(cfg, nil, log.NewNopLogger())
	running := atomic.NewBool(false)
	go func() {
		running.Store(true)
		defer running.Store(false)

		mgr.processQueriesOnSingleStream(ctx, cc, "test:12345")
	}()

	test.Poll(t, time.Second, true, func() interface{} {
		return running.Load()
	})

	// Wait a bit, and verify that processQueriesOnSingleStream is still running, and hasn't stopped
	// just because it cannot contact frontend.
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, true, running.Load())

	cancel()
	test.Poll(t, time.Second, false, func() interface{} {
		return running.Load()
	})
}

func TestContextCancelStopsProcess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// We use random port here, hopefully without any gRPC server.
	// nolint:staticcheck // grpc.DialContext() has been deprecated; we'll address it before upgrading to gRPC 2.
	cc, err := grpc.DialContext(ctx, "localhost:999", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	pm := newProcessorManager(ctx, &mockProcessor{}, cc, "test")
	pm.concurrency(1, "starting")

	test.Poll(t, time.Second, 1, func() interface{} {
		return int(pm.currentProcessors.Load())
	})

	cancel()

	test.Poll(t, time.Second, 0, func() interface{} {
		return int(pm.currentProcessors.Load())
	})

	pm.stop("stopping")
	test.Poll(t, time.Second, 0, func() interface{} {
		return int(pm.currentProcessors.Load())
	})
}

func prepareFrontendProcessor() (*frontendProcessor, *frontendProcessClientMock, *requestHandlerMock) {
	var processCtx context.Context

	processClient := &frontendProcessClientMock{}
	processClient.On("Send", mock.Anything).Return(nil)
	processClient.On("Context").Return(func() context.Context {
		return processCtx
	})

	frontendClient := &frontendClientMock{}
	frontendClient.On("Process", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		processCtx = args.Get(0).(context.Context)
	}).Return(processClient, nil)

	requestHandler := &requestHandlerMock{}

	fp := newFrontendProcessor(Config{QuerierID: "test-querier-id"}, requestHandler, log.NewNopLogger())
	fp.frontendClientFactory = func(_ *grpc.ClientConn) frontendv1pb.FrontendClient {
		return frontendClient
	}

	return fp, processClient, requestHandler
}

type frontendClientMock struct {
	mock.Mock
}

func (m *frontendClientMock) Process(ctx context.Context, opts ...grpc.CallOption) (frontendv1pb.Frontend_ProcessClient, error) {
	args := m.Called(ctx, opts)
	return args.Get(0).(frontendv1pb.Frontend_ProcessClient), args.Error(1)
}

func (m *frontendClientMock) NotifyClientShutdown(ctx context.Context, in *frontendv1pb.NotifyClientShutdownRequest, opts ...grpc.CallOption) (*frontendv1pb.NotifyClientShutdownResponse, error) {
	args := m.Called(ctx, in, opts)
	return args.Get(0).(*frontendv1pb.NotifyClientShutdownResponse), args.Error(1)
}

type frontendProcessClientMock struct {
	mock.Mock
}

func (m *frontendProcessClientMock) Send(msg *frontendv1pb.ClientToFrontend) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *frontendProcessClientMock) Recv() (*frontendv1pb.FrontendToClient, error) {
	args := m.Called()

	// Allow to mock the Recv() with a function which is called each time.
	if fn, ok := args.Get(0).(func() (*frontendv1pb.FrontendToClient, error)); ok {
		return fn()
	}

	return args.Get(0).(*frontendv1pb.FrontendToClient), args.Error(1)
}

func (m *frontendProcessClientMock) Header() (metadata.MD, error) {
	args := m.Called()
	return args.Get(0).(metadata.MD), args.Error(1)
}

func (m *frontendProcessClientMock) Trailer() metadata.MD {
	args := m.Called()
	return args.Get(0).(metadata.MD)
}

func (m *frontendProcessClientMock) CloseSend() error {
	args := m.Called()
	return args.Error(0)
}

func (m *frontendProcessClientMock) Context() context.Context {
	args := m.Called()

	// Allow to mock the Context() with a function which is called each time.
	if fn, ok := args.Get(0).(func() context.Context); ok {
		return fn()
	}

	return args.Get(0).(context.Context)
}

func (m *frontendProcessClientMock) SendMsg(msg interface{}) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *frontendProcessClientMock) RecvMsg(msg interface{}) error {
	args := m.Called(msg)
	return args.Error(0)
}
