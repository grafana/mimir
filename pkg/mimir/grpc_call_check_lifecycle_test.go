// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/server"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/ingester/client"
)

// TestGrpcInflightMethodLimiter_FinishesReadRequestsThatEndBeforeProcessing runs a real gRPC server with the real
// grpcInflightMethodLimiter, and checks that every read request started by RPCCallStarting is finished by
// RPCCallFinished, including the ones that end before the gRPC interceptor calls RPCCallProcessing. The ingester
// only releases the circuit breaker permit of a read request from PrepareReadRequest's finish function or from
// FinishReadRequest, so a request that is not finished leaks its permit.
func TestGrpcInflightMethodLimiter_FinishesReadRequestsThatEndBeforeProcessing(t *testing.T) {
	const maxMessageSize = 1024 // Set by getServerConfig.

	// cancelAfterHeaders opens a stream, so that the server runs RPCCallStarting, and cancels it before sending any
	// message. The unary handler of the server only gets to the interceptors after receiving the message.
	cancelAfterHeaders := func(t *testing.T, conn *grpc.ClientConn, ing *lifecycleTrackingIngester, method string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, method)
		require.NoError(t, err)
		require.Eventually(t, func() bool { return ing.startedReads.Load() == 1 }, 5*time.Second, time.Millisecond)
		cancel()
	}

	testCases := map[string]struct {
		// run sends a single read request to the server and returns after the client is done with it.
		run                  func(t *testing.T, conn *grpc.ClientConn, ing *lifecycleTrackingIngester)
		expectedPreparedRead int64
	}{
		"unary request processed by its handler": {
			run: func(t *testing.T, conn *grpc.ClientConn, _ *lifecycleTrackingIngester) {
				_, err := client.NewIngesterClient(conn).LabelValues(context.Background(), &client.LabelValuesRequest{LabelName: "job"})
				require.NoError(t, err)
			},
			expectedPreparedRead: 1,
		},
		"unary request rejected because the message is too large": {
			run: func(t *testing.T, conn *grpc.ClientConn, _ *lifecycleTrackingIngester) {
				_, err := client.NewIngesterClient(conn).LabelValues(context.Background(), &client.LabelValuesRequest{LabelName: strings.Repeat("x", 4*maxMessageSize)})
				require.Equal(t, codes.ResourceExhausted, status.Code(err))
			},
			expectedPreparedRead: 0,
		},
		"unary request cancelled by the client before sending the message": {
			run: func(t *testing.T, conn *grpc.ClientConn, ing *lifecycleTrackingIngester) {
				cancelAfterHeaders(t, conn, ing, "/cortex.Ingester/LabelValues")
			},
			expectedPreparedRead: 0,
		},
		"streaming request cancelled by the client before sending the message": {
			run: func(t *testing.T, conn *grpc.ClientConn, ing *lifecycleTrackingIngester) {
				cancelAfterHeaders(t, conn, ing, "/cortex.Ingester/QueryStream")
			},
			expectedPreparedRead: 1,
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			ing := &lifecycleTrackingIngester{}

			cfg := getServerConfig(t, "logfmt", "warn")
			cfg.GrpcMethodLimiter = newGrpcInflightMethodLimiter(func() ingesterReceiver { return ing }, nil)
			srv, err := server.New(cfg)
			require.NoError(t, err)
			client.RegisterIngesterServer(srv.GRPC, &noopIngesterServer{})
			go func() { _ = srv.Run() }()
			t.Cleanup(srv.Shutdown)

			conn, err := grpc.NewClient(srv.GRPCListenAddr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })

			testCase.run(t, conn, ing)

			// The server calls RPCCallFinished asynchronously once the RPC is over.
			require.Eventually(t, func() bool { return ing.finishedReads.Load() == 1 }, 5*time.Second, time.Millisecond,
				"RPCCallFinished did not call FinishReadRequest, so the read permit was never released")
			require.Equal(t, int64(1), ing.startedReads.Load())
			require.Equal(t, testCase.expectedPreparedRead, ing.preparedReads.Load())
		})
	}
}

// lifecycleTrackingIngester is an ingesterReceiver that counts the calls made for read requests.
// It is safe for concurrent use, because the gRPC server calls it from its own goroutines.
type lifecycleTrackingIngester struct {
	startedReads  atomic.Int64
	preparedReads atomic.Int64
	finishedReads atomic.Int64
}

func (i *lifecycleTrackingIngester) StartReadRequest(ctx context.Context) (context.Context, error) {
	i.startedReads.Inc()
	return ctx, nil
}

func (i *lifecycleTrackingIngester) PrepareReadRequest(context.Context) (func(error), error) {
	i.preparedReads.Inc()
	return nil, nil
}

func (i *lifecycleTrackingIngester) FinishReadRequest(context.Context) {
	i.finishedReads.Inc()
}

func (i *lifecycleTrackingIngester) StartPushRequest(ctx context.Context, _ int64) (context.Context, error) {
	return ctx, nil
}

func (i *lifecycleTrackingIngester) PreparePushRequest(context.Context) (func(error), error) {
	return nil, nil
}

func (i *lifecycleTrackingIngester) FinishPushRequest(context.Context) {}

// noopIngesterServer is an IngesterServer that answers the requests used by the tests above.
type noopIngesterServer struct {
	client.UnimplementedIngesterServer
}

func (*noopIngesterServer) LabelValues(context.Context, *client.LabelValuesRequest) (*client.LabelValuesResponse, error) {
	return &client.LabelValuesResponse{}, nil
}
