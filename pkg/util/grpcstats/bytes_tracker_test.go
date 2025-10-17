// SPDX-License-Identifier: AGPL-3.0-only

package grpcstats

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/grpcclient"
	dskitserver "github.com/grafana/dskit/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestDataTransferStatsHandler(t *testing.T) {
	// Create a Prometheus counter to track bytes transferred.
	reg := prometheus.NewPedanticRegistry()
	counter := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "test_grpc_bytes_transferred_total",
		Help: "Total bytes transferred in test gRPC calls.",
	})

	// Create the stats handler.
	handler := NewDataTransferStatsHandler(counter)

	// Set up a real gRPC server.
	grpcServer := grpc.NewServer()
	t.Cleanup(grpcServer.GracefulStop)

	server := &testGRPCServer{}
	dskitserver.RegisterFakeServerServer(grpcServer, server)

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	// Create a real gRPC client with the stats handler attached.
	clientCfg := grpcclient.Config{}
	flagext.DefaultValues(&clientCfg)

	opts, err := clientCfg.DialOption(nil, nil, nil)
	require.NoError(t, err)

	// Attach the stats handler to the client.
	opts = append(opts, grpc.WithStatsHandler(handler))

	// nolint:staticcheck // grpc.Dial() has been deprecated; we'll address it before upgrading to gRPC 2.
	conn, err := grpc.Dial(listener.Addr().String(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})

	client := dskitserver.NewFakeServerClient(conn)

	t.Run("unary RPC", func(t *testing.T) {
		// Get the current counter value.
		initialBytes := testutil.ToFloat64(counter)

		// Make a unary RPC call.
		_, err := client.Succeed(context.Background(), &empty.Empty{})
		require.NoError(t, err)

		// Verify that bytes were tracked (counter increased).
		finalBytes := testutil.ToFloat64(counter)
		require.Greater(t, finalBytes, initialBytes, "counter should have increased after unary RPC")
	})

	t.Run("streaming RPC", func(t *testing.T) {
		// Get the current counter value.
		initialBytes := testutil.ToFloat64(counter)

		// Make a streaming RPC call.
		stream, err := client.StreamSleep(context.Background(), &empty.Empty{})
		require.NoError(t, err)

		// Receive all messages from the stream.
		messagesReceived := 0
		for {
			_, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
			messagesReceived++
		}

		// Verify that we received the expected number of messages.
		require.Equal(t, 3, messagesReceived, "should have received 3 messages from stream")

		// Verify that bytes were tracked (counter increased).
		finalBytes := testutil.ToFloat64(counter)
		require.Greater(t, finalBytes, initialBytes, "counter should have increased after streaming RPC")
	})
}

// testGRPCServer is a mock implementation of FakeServerServer for testing.
type testGRPCServer struct {
	dskitserver.UnimplementedFakeServerServer
}

func (s *testGRPCServer) Succeed(_ context.Context, _ *empty.Empty) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

func (s *testGRPCServer) StreamSleep(_ *empty.Empty, stream dskitserver.FakeServer_StreamSleepServer) error {
	// Send 3 messages on the stream.
	for i := 0; i < 3; i++ {
		if err := stream.Send(&empty.Empty{}); err != nil {
			return err
		}
	}
	return nil
}
