// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/store_gateway_client_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"
	"net"
	"testing"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

func Test_newStoreGatewayClientFactory(t *testing.T) {
	// Create a GRPC server used to query the mocked service.
	grpcServer := grpc.NewServer()
	defer grpcServer.GracefulStop()

	srv := &mockStoreGatewayServer{}
	storegatewaypb.RegisterStoreGatewayServer(grpcServer, srv)

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		require.NoError(t, grpcServer.Serve(listener))
	}()

	// Create a client factory and query back the mocked service
	// with different clients.
	cfg := grpcclient.Config{}
	flagext.DefaultValues(&cfg)

	reg := prometheus.NewPedanticRegistry()
	factory := newStoreGatewayClientFactory(cfg, reg)

	for i := 0; i < 2; i++ {
		inst := ring.InstanceDesc{Addr: listener.Addr().String()}
		client, err := factory.FromInstance(inst)
		require.NoError(t, err)
		defer client.Close() //nolint:errcheck

		ctx := user.InjectOrgID(context.Background(), "test")
		stream, err := client.(*storeGatewayClient).Series(ctx, &storepb.SeriesRequest{})
		assert.NoError(t, err)

		// nolint:revive // Read the entire response from the stream.
		for _, err = stream.Recv(); err == nil; {
		}
	}

	// Assert on the request duration metric, but since it's a duration histogram and
	// we can't predict the exact time it took, we need to workaround it.
	metrics, err := reg.Gather()
	require.NoError(t, err)

	assert.Len(t, metrics, 1)
	assert.Equal(t, "cortex_storegateway_client_request_duration_seconds", metrics[0].GetName())
	assert.Equal(t, dto.MetricType_HISTOGRAM, metrics[0].GetType())
	assert.Len(t, metrics[0].GetMetric(), 1)
	assert.Equal(t, uint64(2), metrics[0].GetMetric()[0].GetHistogram().GetSampleCount())
}

type mockStoreGatewayServer struct {
	onSeries      func(req *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer) error
	onLabelNames  func(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error)
	onLabelValues func(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error)
}

func (m *mockStoreGatewayServer) Series(req *storepb.SeriesRequest, srv storegatewaypb.StoreGateway_SeriesServer) error {
	if m.onSeries != nil {
		return m.onSeries(req, srv)
	}

	return nil
}

func (m *mockStoreGatewayServer) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	if m.onLabelNames != nil {
		return m.onLabelNames(ctx, req)
	}

	return nil, nil
}

func (m *mockStoreGatewayServer) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	if m.onLabelValues != nil {
		return m.onLabelValues(ctx, req)
	}

	return nil, nil
}
