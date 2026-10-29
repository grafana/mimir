// SPDX-License-Identifier: AGPL-3.0-only

package cost_test

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/cost"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

type samplingIngesterRPCServer struct {
	client.UnimplementedIngesterServer
}
type samplingStoreRPCServer struct {
	storegatewaypb.UnimplementedStoreGatewayServer
}

func (*samplingIngesterRPCServer) SampleQueryCost(_ context.Context, req *cost.SamplingRequest) (*cost.SamplingResponse, error) {
	return &cost.SamplingResponse{Points: 1, PointCost: 8, Intervals: 4, IntervalMilliseconds: float64(req.End - req.Start), AvailableBlocks: req.BlockIds}, nil
}
func (*samplingStoreRPCServer) SampleQueryCost(ctx context.Context, req *cost.SamplingRequest) (*cost.SamplingResponse, error) {
	return (&samplingIngesterRPCServer{}).SampleQueryCost(ctx, req)
}

func TestSamplingRPCWireRoundTrip(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	defer listener.Close()
	server := grpc.NewServer()
	client.RegisterIngesterServer(server, &samplingIngesterRPCServer{})
	storegatewaypb.RegisterStoreGatewayServer(server, &samplingStoreRPCServer{})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	for _, sample := range []func(context.Context, *cost.SamplingRequest, ...grpc.CallOption) (*cost.SamplingResponse, error){client.NewIngesterClient(conn).SampleQueryCost, storegatewaypb.NewStoreGatewayClient(conn).SampleQueryCost} {
		res, err := sample(t.Context(), &cost.SamplingRequest{Start: 0, End: 60000, BlockIds: []string{"assigned-block"}})
		require.NoError(t, err)
		require.Equal(t, &cost.SamplingResponse{Points: 1, PointCost: 8, Intervals: 4, IntervalMilliseconds: 60000, AvailableBlocks: []string{"assigned-block"}}, res)
	}
}
