// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/client_pool.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type ClientConfig struct {
	grpcclient.Config
	Cluster string `yaml:"-"`
}

// ClientsPool is the interface used to get the client from the pool for a specified address.
type ClientsPool interface {
	services.Service
	// GetClientForInstance returns the ruler client for the ring instance.
	GetClientForInstance(inst ring.InstanceDesc) (RulerClient, error)
}

type rulerClientsPool struct {
	*client.Pool
}

func (p *rulerClientsPool) GetClientForInstance(inst ring.InstanceDesc) (RulerClient, error) {
	c, err := p.Pool.GetClientForInstance(inst)
	if err != nil {
		return nil, err
	}
	return c.(RulerClient), nil
}

func newRulerClientPool(clientCfg ClientConfig, logger log.Logger, reg prometheus.Registerer) ClientsPool {
	// We prefer sane defaults instead of exposing further config options.
	poolCfg := client.PoolConfig{
		CheckInterval:      10 * time.Second,
		HealthCheckEnabled: true,
		HealthCheckTimeout: 10 * time.Second,
	}

	clientsCount := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_ruler_clients",
		Help: "The current number of ruler clients in the pool.",
	})

	return &rulerClientsPool{
		client.NewPool("ruler", poolCfg, nil, newRulerClientFactory(clientCfg, reg), clientsCount, logger),
	}
}

func newRulerClientFactory(clientCfg ClientConfig, reg prometheus.Registerer) client.PoolFactory {
	requestDuration := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cortex_ruler_client_request_duration_seconds",
		Help:    "Time spent executing requests to the ruler.",
		Buckets: prometheus.ExponentialBuckets(0.008, 4, 7),
	}, []string{"operation", "status_code"})

	return client.PoolInstFunc(func(inst ring.InstanceDesc) (client.PoolClient, error) {
		return dialRulerClient(clientCfg, inst, requestDuration)
	})
}

func dialRulerClient(clientCfg ClientConfig, inst ring.InstanceDesc, requestDuration *prometheus.HistogramVec) (*rulerExtendedClient, error) {
	unary, stream := grpcclient.Instrument(requestDuration)
	unary = append(unary, middleware.ClusterUnaryClientInterceptor(clientCfg.Cluster))
	opts, err := clientCfg.DialOption(unary, stream)
	if err != nil {
		return nil, err
	}

	// nolint:staticcheck // grpc.Dial() has been deprecated; we'll address it before upgrading to gRPC 2.
	conn, err := grpc.Dial(inst.Addr, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to dial ruler %s %s", inst.Id, inst.Addr)
	}

	return &rulerExtendedClient{
		RulerClient:  NewRulerClient(conn),
		HealthClient: grpc_health_v1.NewHealthClient(conn),
		conn:         conn,
	}, nil
}

type rulerExtendedClient struct {
	RulerClient
	grpc_health_v1.HealthClient
	conn *grpc.ClientConn
}

func (c *rulerExtendedClient) Close() error {
	return c.conn.Close()
}

func (c *rulerExtendedClient) String() string {
	return c.RemoteAddress()
}

func (c *rulerExtendedClient) RemoteAddress() string {
	return c.conn.Target()
}
