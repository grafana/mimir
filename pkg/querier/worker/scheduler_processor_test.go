// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"context"
	"errors"
	"math"
	"net"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/status"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
)

func TestSchedulerProcessor_processQueriesOnSingleStream(t *testing.T) {
	t.Run("should immediately return if worker context is canceled and there's no inflight query", func(t *testing.T) {
		sp, loopClient, requestHandler, _ := prepareSchedulerProcessor(t)

		workerCtx, workerCancel := context.WithCancel(context.Background())

		loopClient.On("Recv").Return(func() (*schedulerpb.SchedulerToQuerier, error) {
			// Simulate the querier received a SIGTERM while waiting for a query to execute.
			workerCancel()

			// No query to execute, so wait until terminated.
			<-loopClient.Context().Done()
			return nil, toRPCErr(loopClient.Context().Err())
		})

		requestHandler.On("Handle", mock.Anything, mock.Anything).Return(&httpgrpc.HTTPResponse{}, nil)

		sp.processQueriesOnSingleStream(workerCtx, nil, "127.0.0.1")

		// We expect at this point, the execution context has been canceled too.
		require.Error(t, loopClient.Context().Err())

		// We expect Send() has been called only once, to send the querier ID to scheduler.
		loopClient.AssertNumberOfCalls(t, "Send", 1)
		loopClient.AssertCalled(t, "Send", &schedulerpb.QuerierToScheduler{QuerierID: "test-querier-id"})
	})

	t.Run("should wait until inflight query execution is completed before returning when worker context is canceled", func(t *testing.T) {
		sp, loopClient, requestHandler, frontend := prepareSchedulerProcessor(t)

		recvCount := atomic.NewInt64(0)

		loopClient.On("Recv").Return(func() (*schedulerpb.SchedulerToQuerier, error) {
			switch recvCount.Inc() {
			case 1:
				return &schedulerpb.SchedulerToQuerier{
					QueryID:         1,
					HttpRequest:     nil,
					FrontendAddress: frontend.addr,
					UserID:          "user-1",
				}, nil
			default:
				// No more messages to process, so waiting until terminated.
				<-loopClient.Context().Done()
				return nil, toRPCErr(loopClient.Context().Err())
			}
		})

		workerCtx, workerCancel := context.WithCancel(context.Background())

		requestHandler.On("Handle", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			// Cancel the worker context while the query execution is in progress.
			workerCancel()

			// Ensure the execution context hasn't been canceled yet.
			require.Nil(t, loopClient.Context().Err())

			// Intentionally slow down the query execution, to double check the worker waits until done.
			time.Sleep(time.Second)
		}).Return(&httpgrpc.HTTPResponse{}, nil)

		startTime := time.Now()
		sp.processQueriesOnSingleStream(workerCtx, nil, "127.0.0.1")
		assert.GreaterOrEqual(t, time.Since(startTime), time.Second)

		// We expect at this point, the execution context has been canceled too.
		require.Error(t, loopClient.Context().Err())

		// We expect Send() to be called twice: first to send the querier ID to scheduler
		// and then to send the query result.
		loopClient.AssertNumberOfCalls(t, "Send", 2)
		loopClient.AssertCalled(t, "Send", &schedulerpb.QuerierToScheduler{QuerierID: "test-querier-id"})

		require.Equal(t, 1, int(frontend.queryResultCalls.Load()), "expected frontend to be informed of query result exactly once")
	})

	t.Run("should abort query if query is cancelled", func(t *testing.T) {
		sp, loopClient, requestHandler, frontend := prepareSchedulerProcessor(t)

		recvCount := atomic.NewInt64(0)
		queryEvaluationBegun := make(chan struct{})

		loopClient.On("Recv").Return(func() (*schedulerpb.SchedulerToQuerier, error) {
			switch recvCount.Inc() {
			case 1:
				return &schedulerpb.SchedulerToQuerier{
					QueryID:         1,
					HttpRequest:     nil,
					FrontendAddress: frontend.addr,
					UserID:          "user-1",
				}, nil
			default:
				// Wait until query execution has begun, then simulate the scheduler shutting down.
				<-queryEvaluationBegun

				// Emulate the behaviour of the gRPC client: if the server returns an error, the context returned from the stream's Context() should be cancelled.
				loopClient.cancelCtx()

				return nil, toRPCErr(context.Canceled)
			}
		})

		requestHandler.On("Handle", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			ctx := args.Get(0).(context.Context)

			// Trigger the shutdown of the scheduler.
			close(queryEvaluationBegun)

			// Wait for our context to be cancelled before continuing.
			select {
			case <-ctx.Done():
				// Nothing more to do.
			case <-time.After(time.Second):
				require.Fail(t, "expected query execution context to be cancelled when scheduler shut down")
			}
		}).Return(&httpgrpc.HTTPResponse{}, nil)

		workerCtx, workerCancel := context.WithCancel(context.Background())

		// processQueriesOnSingleStream() blocks and retries until its context is cancelled, so run it in the background.
		go func() {
			sp.processQueriesOnSingleStream(workerCtx, nil, "127.0.0.1")
		}()

		// It isn't strictly necessary that we report the query result to the frontend if the query is cancelled, as
		// the scheduler should do that, but it guards against the possibility that the scheduler didn't, and helps
		// ensure there are no paths where the querier doesn't report back a result when it should.
		require.Eventually(t, func() bool {
			return frontend.queryResultCalls.Load() == 1
		}, time.Second, 10*time.Millisecond, "expected frontend to be informed of query result exactly once")

		workerCancel()
	})

	t.Run("should not log an error when the query-scheduler is terminated while waiting for the next query to run", func(t *testing.T) {
		sp, loopClient, requestHandler, _ := prepareSchedulerProcessor(t)

		// Override the logger to capture the logs.
		logs := &concurrency.SyncBuffer{}
		sp.log = log.NewLogfmtLogger(logs)

		workerCtx, workerCancel := context.WithCancel(context.Background())

		// As soon as the Recv() is called for the first time, we cancel the worker context and
		// return the "scheduler not running" error. The reason why we cancel the worker context
		// is to let processQueriesOnSingleStream() terminate.
		loopClient.On("Recv").Return(func() (*schedulerpb.SchedulerToQuerier, error) {
			workerCancel()

			// Emulate the behaviour of the gRPC client: if the server returns an error, the context returned from the stream's Context() should be cancelled.
			loopClient.cancelCtx()

			return nil, status.Error(codes.Unknown, schedulerpb.ErrSchedulerIsNotRunning.Error())
		})

		requestHandler.On("Handle", mock.Anything, mock.Anything).Return(&httpgrpc.HTTPResponse{}, nil)

		sp.processQueriesOnSingleStream(workerCtx, nil, "127.0.0.1")

		// We expect no error in the log.
		assert.NotContains(t, logs.String(), "error")
		assert.NotContains(t, logs.String(), schedulerpb.ErrSchedulerIsNotRunning)
	})

	t.Run("should not cancel query execution if scheduler client returns a non-cancellation error", func(t *testing.T) {
		sp, loopClient, requestHandler, frontend := prepareSchedulerProcessor(t)

		recvCount := atomic.NewInt64(0)
		executionStarted := make(chan struct{})

		loopClient.On("Recv").Return(func() (*schedulerpb.SchedulerToQuerier, error) {
			switch recvCount.Inc() {
			case 1:
				return &schedulerpb.SchedulerToQuerier{
					QueryID:         1,
					HttpRequest:     nil,
					FrontendAddress: frontend.addr,
					UserID:          "user-1",
				}, nil
			default:
				// No more requests to process: wait until execution of first query starts, then return an error that should trigger aborting execution
				<-executionStarted

				// Emulate the behaviour of the gRPC client: if the server returns an error, the context returned from the stream's Context() should be cancelled.
				loopClient.cancelCtx()

				return nil, status.Error(codes.Unknown, "something went wrong")
			}
		})

		workerCtx, workerCancel := context.WithCancel(context.Background())
		defer workerCancel()

		requestHandler.On("Handle", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			// Ensure the execution context hasn't been canceled yet.
			ctx := args.Get(0).(context.Context)
			require.NoError(t, ctx.Err())

			// Trigger Recv() returning an error.
			close(executionStarted)

			// Wait for the querier loop to observe the error, and make sure the query context has not been cancelled.
			time.Sleep(500 * time.Millisecond)
			require.NoError(t, ctx.Err())
		}).Return(&httpgrpc.HTTPResponse{}, nil)

		go func() {
			sp.processQueriesOnSingleStream(workerCtx, nil, "127.0.0.1")
		}()

		// Wait for query execution to terminate.
		require.Eventually(t, func() bool {
			return frontend.queryResultCalls.Load() == 1
		}, 2*time.Second, 10*time.Millisecond, "expected frontend to be informed of query result exactly once")

		// We expect Send() to be called twice: first to send the querier ID to scheduler
		// and then to send the query result.
		loopClient.AssertNumberOfCalls(t, "Send", 2)
		loopClient.AssertCalled(t, "Send", &schedulerpb.QuerierToScheduler{QuerierID: "test-querier-id"})
	})
}

func TestSchedulerProcessor_QueryTime(t *testing.T) {
	runTest := func(t *testing.T, statsEnabled bool) {
		fp, processClient, requestHandler, frontend := prepareSchedulerProcessor(t)

		recvCount := atomic.NewInt64(0)
		queueTime := 3 * time.Second

		processClient.On("Recv").Return(func() (*schedulerpb.SchedulerToQuerier, error) {
			switch recvCount.Inc() {
			case 1:
				return &schedulerpb.SchedulerToQuerier{
					QueryID:         1,
					HttpRequest:     nil,
					FrontendAddress: frontend.addr,
					UserID:          "user-1",
					StatsEnabled:    statsEnabled,
					QueueTimeNanos:  queueTime.Nanoseconds(),
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

		// We expect Send() to be called twice: first to send the querier ID to scheduler
		// and then to send the query result.
		processClient.AssertNumberOfCalls(t, "Send", 2)

		require.Equal(t, 1, int(frontend.queryResultCalls.Load()), "expected frontend to be informed of query result exactly once")
	}

	t.Run("query stats enabled should record queue time", func(t *testing.T) {
		runTest(t, true)
	})

	t.Run("query stats disabled will not record queue time", func(t *testing.T) {
		runTest(t, false)
	})
}

func TestCreateSchedulerProcessor(t *testing.T) {
	conf := grpcclient.Config{}
	flagext.DefaultValues(&conf)
	conf.MaxSendMsgSize = 1 * 1024 * 1024

	sp, _ := newSchedulerProcessor(Config{
		SchedulerAddress:               "sched:12345",
		QuerierID:                      "test",
		QueryFrontendGRPCClientConfig:  conf,
		QuerySchedulerGRPCClientConfig: grpcclient.Config{MaxSendMsgSize: 5 * 1024}, // schedulerProcessor should ignore this.
		MaxConcurrentRequests:          5,
	}, nil, nil, nil)

	assert.Equal(t, 1*1024*1024, sp.maxMessageSize)
	assert.Equal(t, conf, sp.grpcConfig)
}

func prepareSchedulerProcessor(t *testing.T) (*schedulerProcessor, *querierLoopClientMock, *requestHandlerMock, *frontendForQuerierMockServer) {
	loopClient := &querierLoopClientMock{}
	loopClient.On("Send", mock.Anything).Return(nil)
	loopClient.On("Context").Return(func() context.Context {
		return loopClient.ctx
	})

	schedulerClient := &schedulerForQuerierClientMock{}
	schedulerClient.On("QuerierLoop", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		ctx := args.Get(0).(context.Context)

		loopClient.ctx, loopClient.cancelCtx = context.WithCancel(ctx)
	}).Return(loopClient, nil)

	requestHandler := &requestHandlerMock{}

	sp, _ := newSchedulerProcessor(Config{QuerierID: "test-querier-id"}, requestHandler, log.NewNopLogger(), nil)
	sp.schedulerClientFactory = func(_ *grpc.ClientConn) schedulerpb.SchedulerForQuerierClient {
		return schedulerClient
	}
	sp.grpcConfig.MaxSendMsgSize = math.MaxInt

	frontendForQuerierMock := &frontendForQuerierMockServer{}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	frontendForQuerierMock.addr = lis.Addr().String()

	grpcServer := grpc.NewServer()
	frontendv2pb.RegisterFrontendForQuerierServer(grpcServer, frontendForQuerierMock)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)

		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			require.NoError(t, err)
		}
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		<-stopped // Wait for shutdown to complete.
	})

	return sp, loopClient, requestHandler, frontendForQuerierMock
}

type schedulerForQuerierClientMock struct {
	mock.Mock
}

func (m *schedulerForQuerierClientMock) QuerierLoop(ctx context.Context, opts ...grpc.CallOption) (schedulerpb.SchedulerForQuerier_QuerierLoopClient, error) {
	args := m.Called(ctx, opts)
	return args.Get(0).(schedulerpb.SchedulerForQuerier_QuerierLoopClient), args.Error(1)
}

func (m *schedulerForQuerierClientMock) NotifyQuerierShutdown(ctx context.Context, in *schedulerpb.NotifyQuerierShutdownRequest, opts ...grpc.CallOption) (*schedulerpb.NotifyQuerierShutdownResponse, error) {
	args := m.Called(ctx, in, opts)
	return args.Get(0).(*schedulerpb.NotifyQuerierShutdownResponse), args.Error(1)
}

type querierLoopClientMock struct {
	mock.Mock

	ctx       context.Context
	cancelCtx context.CancelFunc
}

func (m *querierLoopClientMock) Send(msg *schedulerpb.QuerierToScheduler) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *querierLoopClientMock) Recv() (*schedulerpb.SchedulerToQuerier, error) {
	args := m.Called()

	// Allow to mock the Recv() with a function which is called each time.
	if fn, ok := args.Get(0).(func() (*schedulerpb.SchedulerToQuerier, error)); ok {
		return fn()
	}

	return args.Get(0).(*schedulerpb.SchedulerToQuerier), args.Error(1)
}

func (m *querierLoopClientMock) Header() (metadata.MD, error) {
	args := m.Called()
	return args.Get(0).(metadata.MD), args.Error(1)
}

func (m *querierLoopClientMock) Trailer() metadata.MD {
	args := m.Called()
	return args.Get(0).(metadata.MD)
}

func (m *querierLoopClientMock) CloseSend() error {
	args := m.Called()
	return args.Error(0)
}

func (m *querierLoopClientMock) Context() context.Context {
	args := m.Called()

	// Allow to mock the Context() with a function which is called each time.
	if fn, ok := args.Get(0).(func() context.Context); ok {
		return fn()
	}

	return args.Get(0).(context.Context)
}

func (m *querierLoopClientMock) SendMsg(msg interface{}) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *querierLoopClientMock) RecvMsg(msg interface{}) error {
	args := m.Called(msg)
	return args.Error(0)
}

type requestHandlerMock struct {
	mock.Mock
}

func (m *requestHandlerMock) Handle(ctx context.Context, req *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(*httpgrpc.HTTPResponse), args.Error(1)
}

type frontendForQuerierMockServer struct {
	addr             string
	queryResultCalls atomic.Int64
}

func (f *frontendForQuerierMockServer) QueryResult(context.Context, *frontendv2pb.QueryResultRequest) (*frontendv2pb.QueryResultResponse, error) {
	f.queryResultCalls.Inc()

	return &frontendv2pb.QueryResultResponse{}, nil
}
