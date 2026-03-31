// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"io"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/ring"
	ring_client "github.com/grafana/dskit/ring/client"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/mimir/pkg/distributor/distributorpb"
)

// L2DistributorClient is the union of DistributorClient and grpc_health_v1.HealthClient.
type L2DistributorClient interface {
	distributorpb.DistributorClient
	grpc_health_v1.HealthClient
	io.Closer
}

type closableL2DistributorClient struct {
	distributorpb.DistributorClient
	grpc_health_v1.HealthClient
	conn *grpc.ClientConn
}

func (c *closableL2DistributorClient) Close() error {
	return c.conn.Close()
}

// MakeL2DistributorClient creates a new L2DistributorClient for the given instance.
// Header propagation (org ID, cluster validation, etc.) is handled by forwarding the
// incoming gRPC metadata in pushToL2, so no interceptors are needed here.
func MakeL2DistributorClient(inst ring.InstanceDesc, cfg grpcclient.Config, _ log.Logger) (L2DistributorClient, error) {
	opts, err := cfg.DialOption(nil, nil, nil)
	if err != nil {
		return nil, err
	}
	// nolint:staticcheck // grpc.Dial() has been deprecated; we'll address it before upgrading to gRPC 2.
	conn, err := grpc.Dial(inst.Addr, opts...)
	if err != nil {
		return nil, err
	}
	return &closableL2DistributorClient{
		DistributorClient: distributorpb.NewDistributorClient(conn),
		HealthClient:      grpc_health_v1.NewHealthClient(conn),
		conn:              conn,
	}, nil
}

// NewL2DistributorPool creates a pool of L2 distributor clients backed by the given ring.
// If factory is nil the default MakeL2DistributorClient is used; pass a non-nil factory to
// inject mock clients in tests.
func NewL2DistributorPool(cfg PoolConfig, distributorsRing ring.ReadRing, clientCfg grpcclient.Config, factory ring_client.PoolFactory, reg prometheus.Registerer, logger log.Logger) *ring_client.Pool {
	poolCfg := ring_client.PoolConfig{
		CheckInterval:          cfg.ClientCleanupPeriod,
		HealthCheckEnabled:     cfg.HealthCheckIngesters,
		HealthCheckTimeout:     cfg.RemoteTimeout,
		HealthCheckGracePeriod: cfg.HealthCheckGracePeriod,
	}

	clientsGauge := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_distributor_l2_clients",
		Help: "The current number of L2 distributor clients.",
	})

	if factory == nil {
		factory = ring_client.PoolInstFunc(func(inst ring.InstanceDesc) (ring_client.PoolClient, error) {
			return MakeL2DistributorClient(inst, clientCfg, logger)
		})
	}

	return ring_client.NewPool("l2-distributor", poolCfg, ring_client.NewRingServiceDiscovery(distributorsRing), factory, clientsGauge, logger)
}
