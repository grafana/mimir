// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/store_gateway_client.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/ring/client"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/grpcstats"
)

func newStoreGatewayClientFactory(clientCfg grpcclient.Config, reg prometheus.Registerer, logger log.Logger) client.PoolFactory {
	requestDuration := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:        "cortex_storegateway_client_request_duration_seconds",
		Help:        "Time spent executing requests to the store-gateway.",
		Buckets:     prometheus.ExponentialBuckets(0.008, 4, 7),
		ConstLabels: prometheus.Labels{"client": "querier"},
	}, []string{"operation", "status_code"})

	transferredBytes := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_storegateway_client_transferred_bytes_total",
		Help: "Total bytes transferred to/from the store-gateway.",
	}, []string{"store_gateway_zone"})

	invalidClusterValidation := util.NewRequestInvalidClusterValidationLabelsTotalCounter(reg, "store-gateway", util.GRPCProtocol)

	return client.PoolInstFunc(func(inst ring.InstanceDesc) (client.PoolClient, error) {
		return dialStoreGatewayClient(clientCfg, inst, requestDuration, invalidClusterValidation, transferredBytes, logger)
	})
}

func dialStoreGatewayClient(clientCfg grpcclient.Config, instance ring.InstanceDesc, requestDuration *prometheus.HistogramVec, invalidClusterValidation, transferredBytes *prometheus.CounterVec, logger log.Logger) (*storeGatewayClient, error) {
	unary, stream := grpcclient.Instrument(requestDuration)
	opts, err := clientCfg.DialOption(unary, stream, util.NewInvalidClusterValidationReporter(clientCfg.ClusterValidation.Label, invalidClusterValidation, logger))
	if err != nil {
		return nil, err
	}
	opts = append(opts,
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithStatsHandler(grpcstats.NewDataTransferStatsHandler(transferredBytes.WithLabelValues(instance.Zone))),
	)

	// nolint:staticcheck // grpc.Dial() has been deprecated; we'll address it before upgrading to gRPC 2.
	conn, err := grpc.Dial(instance.Addr, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to dial store-gateway %s %s", instance.Id, instance.Addr)
	}

	return &storeGatewayClient{
		StoreGatewayClient: storegatewaypb.NewCustomStoreGatewayClient(conn),
		HealthClient:       grpc_health_v1.NewHealthClient(conn),
		conn:               conn,
		instance:           instance,
	}, nil
}

type storeGatewayClient struct {
	storegatewaypb.StoreGatewayClient
	grpc_health_v1.HealthClient
	conn     *grpc.ClientConn
	instance ring.InstanceDesc
}

func (c *storeGatewayClient) Close() error {
	return c.conn.Close()
}

func (c *storeGatewayClient) String() string {
	return c.RemoteAddress()
}

func (c *storeGatewayClient) RemoteAddress() string {
	return c.conn.Target()
}

func (c *storeGatewayClient) RemoteZone() string {
	return c.instance.Zone
}

func newStoreGatewayClientPool(discovery client.PoolServiceDiscovery, clientConfig grpcclient.Config, logger log.Logger, reg prometheus.Registerer) *client.Pool {
	poolCfg := client.PoolConfig{
		CheckInterval:      10 * time.Second,
		HealthCheckEnabled: true,
		HealthCheckTimeout: 10 * time.Second,
	}

	clientsCount := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name:        "cortex_storegateway_clients",
		Help:        "The current number of store-gateway clients in the pool.",
		ConstLabels: map[string]string{"client": "querier"},
	})

	return client.NewPool("store-gateway", poolCfg, discovery, newStoreGatewayClientFactory(clientConfig, reg, logger), clientsCount, logger)
}
