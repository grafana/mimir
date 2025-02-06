// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/store_gateway_client.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"flag"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/crypto/tls"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/ring/client"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

func newStoreGatewayClientFactory(clientCfg grpcclient.Config, reg prometheus.Registerer, cluster string, logger log.Logger) client.PoolFactory {
	requestDuration := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   "cortex",
		Name:        "storegateway_client_request_duration_seconds",
		Help:        "Time spent executing requests to the store-gateway.",
		Buckets:     prometheus.ExponentialBuckets(0.008, 4, 7),
		ConstLabels: prometheus.Labels{"client": "querier"},
	}, []string{"operation", "status_code"})

	return client.PoolInstFunc(func(inst ring.InstanceDesc) (client.PoolClient, error) {
		return dialStoreGatewayClient(clientCfg, inst, requestDuration, cluster, logger)
	})
}

func dialStoreGatewayClient(clientCfg grpcclient.Config, inst ring.InstanceDesc, requestDuration *prometheus.HistogramVec, cluster string, logger log.Logger) (*storeGatewayClient, error) {
	unary, stream := grpcclient.Instrument(requestDuration)
	unary = append(unary, middleware.ClusterUnaryClientInterceptor(cluster))
	opts, err := clientCfg.DialOption(unary, stream)
	if err != nil {
		return nil, err
	}

	// nolint:staticcheck // grpc.Dial() has been deprecated; we'll address it before upgrading to gRPC 2.
	conn, err := grpc.Dial(inst.Addr, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to dial store-gateway %s %s", inst.Id, inst.Addr)
	}

	return &storeGatewayClient{
		StoreGatewayClient: storegatewaypb.NewCustomStoreGatewayClient(conn),
		HealthClient:       grpc_health_v1.NewHealthClient(conn),
		conn:               conn,
	}, nil
}

type storeGatewayClient struct {
	storegatewaypb.StoreGatewayClient
	grpc_health_v1.HealthClient
	conn *grpc.ClientConn
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

func newStoreGatewayClientPool(discovery client.PoolServiceDiscovery, clientConfig ClientConfig, logger log.Logger, reg prometheus.Registerer, cluster string) *client.Pool {
	// We prefer sane defaults instead of exposing further config options.
	clientCfg := grpcclient.Config{
		MaxRecvMsgSize:      100 << 20,
		MaxSendMsgSize:      16 << 20,
		GRPCCompression:     "",
		RateLimit:           0,
		RateLimitBurst:      0,
		BackoffOnRatelimits: false,
		TLSEnabled:          clientConfig.TLSEnabled,
		TLS:                 clientConfig.TLS,
	}
	poolCfg := client.PoolConfig{
		CheckInterval:      10 * time.Second,
		HealthCheckEnabled: true,
		HealthCheckTimeout: 10 * time.Second,
	}

	clientsCount := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Namespace:   "cortex",
		Name:        "storegateway_clients",
		Help:        "The current number of store-gateway clients in the pool.",
		ConstLabels: map[string]string{"client": "querier"},
	})

	return client.NewPool("store-gateway", poolCfg, discovery, newStoreGatewayClientFactory(clientCfg, reg, cluster, logger), clientsCount, logger)
}

type ClientConfig struct {
	TLSEnabled bool             `yaml:"tls_enabled" category:"advanced"`
	TLS        tls.ClientConfig `yaml:",inline"`
}

func (cfg *ClientConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.TLSEnabled, prefix+".tls-enabled", cfg.TLSEnabled, "Enable TLS for gRPC client connecting to store-gateway.")
	cfg.TLS.RegisterFlagsWithPrefix(prefix, f)
}
