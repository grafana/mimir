// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/client/cortex_util_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"context"
	"fmt"
	"math"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestSendQueryStream(t *testing.T) {
	// Create a new gRPC server with in-memory communication.
	listen := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	bufDialer := func(context.Context, string) (net.Conn, error) {
		return listen.Dial()
	}

	// nolint:staticcheck // grpc.DialContext() has been deprecated; we'll address it before upgrading to gRPC 2.
	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	// Create a cancellable context for the client.
	clientCtx, clientCancel := context.WithCancel(context.Background())

	// Create a WaitGroup used to wait until the mocked server assertions
	// complete before returning.
	wg := sync.WaitGroup{}
	wg.Add(1)

	serverMock := &IngesterServerMock{}
	serverMock.On("QueryStream", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		defer wg.Done()

		stream := args.Get(1).(grpc.ServerStream)

		// Cancel the client request.
		clientCancel()

		// Wait until the cancelling has been propagated to the server.
		test.Poll(t, time.Second, context.Canceled, func() interface{} {
			return stream.Context().Err()
		})

		// Try to send the response and assert the error we get is the context.Canceled
		// and not transport.ErrIllegalHeaderWrite. This is the assertion we care about
		// in this test.
		err := SendQueryStream(stream.(Ingester_QueryStreamServer), &QueryStreamResponse{})
		assert.Equal(t, context.Canceled, err)
	})

	RegisterIngesterServer(server, serverMock)

	go func() {
		require.NoError(t, server.Serve(listen))
	}()
	t.Cleanup(server.Stop)

	client := NewIngesterClient(conn)
	stream, err := client.QueryStream(clientCtx, &QueryRequest{})
	require.NoError(t, err)

	// Try to receive the response and assert the error we get is the context.Canceled
	// wrapped within a gRPC error.
	_, err = stream.Recv()
	s, ok := grpcutil.ErrorToStatus(err)
	require.True(t, ok)
	require.Equal(t, codes.Canceled, s.Code())

	assert.Equal(t, true, grpcutil.IsCanceled(err))

	// Wait until the assertions in the server mock have completed.
	wg.Wait()
}

func TestSendAndReceiveSeriesCountsResponse(t *testing.T) {
	listen := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	// nolint:staticcheck // grpc.DialContext() has been deprecated; we'll address it before upgrading to gRPC 2.
	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listen.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	want := &SeriesCountsResponse{SeriesWalked: 300, SeriesCounted: 200, HeadMinTimeMs: 1, HeadMaxTimeMs: 2, MinTimeMs: 1, ShardSizeUnchangedSinceMs: 5}
	for g := range 100 {
		hashes := make([]uint64, 40)
		for h := range hashes {
			hashes[h] = math.MaxUint64 - uint64(h)
		}
		want.Groups = append(want.Groups, &SeriesCountGroup{Value: fmt.Sprintf("metric_%d", g), Hashes: hashes})
	}
	var sent []int
	serverMock := &IngesterServerMock{}
	serverMock.On("SeriesCounts", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		stream := countingSeriesCountsServer{Ingester_SeriesCountsServer: args.Get(1).(Ingester_SeriesCountsServer), sent: &sent}
		// About 400 bytes a group: ten groups to a message.
		assert.NoError(t, SendSeriesCountsResponse(stream, want, 4000))
	})
	RegisterIngesterServer(server, serverMock)
	go func() {
		require.NoError(t, server.Serve(listen))
	}()
	t.Cleanup(server.Stop)

	stream, err := NewIngesterClient(conn).SeriesCounts(context.Background(), &SeriesCountsRequest{})
	require.NoError(t, err)
	got, err := ReceiveSeriesCountsResponse(stream)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Greater(t, len(sent), 5, "split into several messages")
	for _, n := range sent {
		assert.LessOrEqual(t, n, 4000)
	}

	// A response with no groups is still one message.
	sent = nil
	want = &SeriesCountsResponse{ShardSizeChanged: true}
	stream, err = NewIngesterClient(conn).SeriesCounts(context.Background(), &SeriesCountsRequest{})
	require.NoError(t, err)
	got, err = ReceiveSeriesCountsResponse(stream)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Len(t, sent, 1)
}

type countingSeriesCountsServer struct {
	Ingester_SeriesCountsServer
	sent *[]int
}

func (s countingSeriesCountsServer) Send(m *SeriesCountsResponse) error {
	*s.sent = append(*s.sent, m.Size())
	return s.Ingester_SeriesCountsServer.Send(m)
}
