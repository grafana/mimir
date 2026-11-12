// SPDX-License-Identifier: AGPL-3.0-only

package cost_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/cost"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

type samplingIngesterRPCServer struct {
	client.UnimplementedIngesterServer
	requests chan *cost.SamplingRequest
	response *cost.SamplingResponse
}

func (s *samplingIngesterRPCServer) SampleQueryCost(_ context.Context, req *cost.SamplingRequest) (*cost.SamplingResponse, error) {
	s.requests <- req
	return s.response, nil
}

type samplingStoreRPCServer struct {
	storegatewaypb.UnimplementedStoreGatewayServer
	sampler cost.Sampler
}

func (s *samplingStoreRPCServer) SampleQueryCost(ctx context.Context, req *cost.SamplingRequest) (*cost.SamplingResponse, error) {
	return s.sampler.SampleQueryCost(ctx, req)
}

func newSamplingRPCConnection(t *testing.T, ing client.IngesterServer, store storegatewaypb.StoreGatewayServer) *grpc.ClientConn {
	t.Helper()
	return newSamplingRPCConnectionWithServices(t, func(server *grpc.Server) {
		client.RegisterIngesterServer(server, ing)
		storegatewaypb.RegisterStoreGatewayServer(server, store)
	})
}

func newSamplingRPCConnectionWithServices(t *testing.T, register func(*grpc.Server)) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	register(server)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, <-serveErr)
		require.NoError(t, listener.Close())
	})
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return conn
}

func TestSamplingRPCWireRoundTrip(t *testing.T) {
	request := &cost.SamplingRequest{
		Start: 1000, End: 61000,
		Matchers: []cost.Matcher{
			{Type: cost.EQUAL, Name: "__name__", Value: "up"},
			{Type: cost.NOT_EQUAL, Name: "instance", Value: "localhost"},
			{Type: cost.REGEX_MATCH, Name: "job", Value: "api.*"},
			{Type: cost.REGEX_NO_MATCH, Name: "cluster", Value: "test.*"},
		},
		SkipHistogramBuckets: true,
		BlockIds:             []string{"assigned-block"},
	}
	response := &cost.SamplingResponse{
		Intervals: 4, IntervalMilliseconds: 60000,
		Points: 2, PointCost: 8,
		SeriesExamined: 3, ChunksExamined: 4, ChunkBytes: 1024,
		Unsupported: true, Warnings: []string{"sampling unavailable for one source"},
		AvailableBlocks: []string{"assigned-block"},
	}
	server := &samplingIngesterRPCServer{requests: make(chan *cost.SamplingRequest, 1), response: response}
	conn := newSamplingRPCConnection(t, server, &samplingStoreRPCServer{sampler: server})
	for name, sample := range map[string]func(context.Context, *cost.SamplingRequest, ...grpc.CallOption) (*cost.SamplingResponse, error){
		"ingester":             client.NewIngesterClient(conn).SampleQueryCost,
		"store-gateway":        storegatewaypb.NewStoreGatewayClient(conn).SampleQueryCost,
		"custom store-gateway": storegatewaypb.NewCustomStoreGatewayClient(conn).SampleQueryCost,
	} {
		t.Run(name, func(t *testing.T) {
			res, err := sample(t.Context(), request)
			require.NoError(t, err)
			require.Equal(t, request, <-server.requests)
			require.Equal(t, response, res)
		})
	}
}

func TestSamplingRPCOlderServer(t *testing.T) {
	conn := newSamplingRPCConnectionWithServices(t, func(server *grpc.Server) {
		// Older servers register these services without the SampleQueryCost method.
		server.RegisterService(&grpc.ServiceDesc{
			ServiceName: "cortex.Ingester",
			HandlerType: (*client.IngesterServer)(nil),
		}, &client.UnimplementedIngesterServer{})
		server.RegisterService(&grpc.ServiceDesc{
			ServiceName: "gatewaypb.StoreGateway",
			HandlerType: (*storegatewaypb.StoreGatewayServer)(nil),
		}, &storegatewaypb.UnimplementedStoreGatewayServer{})
	})
	for name, sample := range map[string]func(context.Context, *cost.SamplingRequest, ...grpc.CallOption) (*cost.SamplingResponse, error){
		"ingester":      client.NewIngesterClient(conn).SampleQueryCost,
		"store-gateway": storegatewaypb.NewCustomStoreGatewayClient(conn).SampleQueryCost,
	} {
		t.Run(name, func(t *testing.T) {
			res, err := sample(t.Context(), &cost.SamplingRequest{})
			require.Equal(t, codes.Unimplemented, status.Code(err))
			require.Contains(t, status.Convert(err).Message(), "unknown method SampleQueryCost")
			require.Nil(t, res)
		})
	}
}

func TestSamplingRPCCustomClientContextErrors(t *testing.T) {
	conn := newSamplingRPCConnection(t, &ingester.Ingester{}, &storegateway.StoreGateway{})
	for _, tc := range []struct {
		name string
		err  error
		code codes.Code
	}{
		{name: "canceled", err: context.Canceled, code: codes.Canceled},
		{name: "deadline exceeded", err: context.DeadlineExceeded, code: codes.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ctx context.Context
			if tc.code == codes.Canceled {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(t.Context())
				cancel()
			} else {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
				defer cancel()
			}
			res, err := storegatewaypb.NewCustomStoreGatewayClient(conn).SampleQueryCost(ctx, &cost.SamplingRequest{})
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, tc.code, status.Code(err))
			require.Nil(t, res)
		})
	}
}

func TestSamplingRPCUnimplemented(t *testing.T) {
	for name, ing := range map[string]client.IngesterServer{
		"ingester": &ingester.Ingester{},
		"wrapped ingester": ingester.NewIngesterProfilingWrapper(
			ingester.NewIngesterActivityTracker(&ingester.Ingester{}, nil)),
	} {
		t.Run(name, func(t *testing.T) {
			conn := newSamplingRPCConnection(t, ing, &storegateway.StoreGateway{})
			for _, sample := range []func(context.Context, *cost.SamplingRequest, ...grpc.CallOption) (*cost.SamplingResponse, error){
				client.NewIngesterClient(conn).SampleQueryCost,
				storegatewaypb.NewStoreGatewayClient(conn).SampleQueryCost,
				storegatewaypb.NewCustomStoreGatewayClient(conn).SampleQueryCost,
			} {
				res, err := sample(t.Context(), &cost.SamplingRequest{})
				require.Equal(t, codes.Unimplemented, status.Code(err))
				require.Nil(t, res)
			}
		})
	}
}
