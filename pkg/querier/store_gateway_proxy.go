// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"flag"
	"slices"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

const (
	// GRPCTeeTargetAddrMetadataKey is the gRPC metadata key with the address of the store-gateway that a
	// proxied call is for. grpc-tee (tools/grpc-tee) reads this key.
	GRPCTeeTargetAddrMetadataKey = "x-grpctee-target-addr"
	// GRPCTeeTargetIDMetadataKey is the gRPC metadata key with the instance ID of the store-gateway that a
	// proxied call is for. grpc-tee (tools/grpc-tee) reads this key.
	GRPCTeeTargetIDMetadataKey = "x-grpctee-target-id"

	grpcTeeRingName = "querier-grpc-tee"
	grpcTeeRingKey  = "grpc-tee"
)

// grpcTeeRingRead is the ring operation that selects the grpc-tee instances that can receive calls.
var grpcTeeRingRead = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, nil)

// StoreGatewayProxyConfig is the configuration of the store-gateway proxy mode.
type StoreGatewayProxyConfig struct {
	Zones        flagext.StringSliceCSV `yaml:"zones" category:"experimental"`
	RingKVPrefix string                 `yaml:"ring_kvstore_prefix" category:"experimental"`
}

// RegisterFlagsWithPrefix registers the StoreGatewayProxyConfig flags.
func (cfg *StoreGatewayProxyConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.Var(&cfg.Zones, prefix+".zones", "Comma-separated list of store-gateway zones that the querier sends through grpc-tee. The querier sends each call for a store-gateway in one of these zones to a random healthy grpc-tee instance in the same zone. The querier still selects the store-gateways with the store-gateway ring. If the list is empty, the proxy mode is disabled.")
	f.StringVar(&cfg.RingKVPrefix, prefix+".ring-kvstore-prefix", "collectors/", "The KV store prefix of the grpc-tee ring. The grpc-tee ring uses the KV store of the store-gateway ring.")
}

// Enabled returns true if the proxy mode is enabled for at least one zone.
func (cfg *StoreGatewayProxyConfig) Enabled() bool {
	return len(cfg.Zones) > 0
}

// storeGatewayProxy sends the calls for the store-gateways in the proxy zones through the instances of the grpc-tee
// ring. Each grpc-tee instance has the zone of the store-gateways that it serves.
type storeGatewayProxy struct {
	services.Service

	zones       []string
	teeRing     ring.ReadRing
	teePool     *client.Pool
	directCalls prometheus.Counter

	subservices *services.Manager
}

// newStoreGatewayProxy makes a storeGatewayProxy. The grpc-tee ring client uses the KV store and the heartbeat
// timeout of the store-gateway ring, with the KV prefix of the proxy config.
func newStoreGatewayProxy(cfg StoreGatewayProxyConfig, gatewayCfg storegateway.Config, clientCfg StoreGatewayClientConfig, logger log.Logger, reg prometheus.Registerer) (*storeGatewayProxy, error) {
	ringCfg := gatewayCfg.ShardingRing.ToRingConfig()
	ringCfg.KVStore.Prefix = cfg.RingKVPrefix
	ringCfg.ReplicationFactor = 1
	ringCfg.ZoneAwarenessEnabled = false
	ringCfg.ExcludedZones = nil

	ringStore, err := kv.NewClient(ringCfg.KVStore, ring.GetCodec(), kv.RegistererWithKVName(prometheus.WrapRegistererWithPrefix("cortex_", reg), grpcTeeRingName), logger)
	if err != nil {
		return nil, err
	}
	teeRing, err := ring.NewWithStoreClientAndStrategy(ringCfg, grpcTeeRingName, grpcTeeRingKey, ringStore, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), prometheus.WrapRegistererWithPrefix("cortex_", reg), logger)
	if err != nil {
		return nil, err
	}

	teePool := newGRPCTeeClientPool(teeRing, clientCfg, logger, reg)
	p, err := newStoreGatewayProxyWithRing(cfg.Zones, teeRing, teePool, reg)
	if err != nil {
		return nil, err
	}
	if p.subservices, err = services.NewManager(teeRing, teePool); err != nil {
		return nil, err
	}
	return p, nil
}

func newStoreGatewayProxyWithRing(zones []string, teeRing ring.ReadRing, teePool *client.Pool, reg prometheus.Registerer) (*storeGatewayProxy, error) {
	p := &storeGatewayProxy{
		zones:   zones,
		teeRing: teeRing,
		teePool: teePool,
		directCalls: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_querier_store_gateway_proxy_direct_calls_total",
			Help: "Total number of store-gateway calls in a proxy zone that the querier sent direct, because no grpc-tee instance in that zone was healthy.",
		}),
	}
	p.Service = services.NewIdleService(p.starting, p.stopping)
	return p, nil
}

func (p *storeGatewayProxy) starting(ctx context.Context) error {
	if p.subservices == nil {
		return nil
	}
	return services.StartManagerAndAwaitHealthy(ctx, p.subservices)
}

func (p *storeGatewayProxy) stopping(_ error) error {
	var err error
	if p.subservices != nil {
		err = services.StopManagerAndAwaitStopped(context.Background(), p.subservices)
	}
	// The pool does not close its clients when it stops, so the proxy closes them.
	for _, addr := range p.teePool.RegisteredAddresses() {
		p.teePool.RemoveClientFor(addr)
	}
	return err
}

// clientFor returns the client for the calls to the target store-gateway. If the target is in a proxy zone,
// the client sends the calls through a random healthy grpc-tee instance in the same zone. Otherwise, clientFor
// returns direct.
func (p *storeGatewayProxy) clientFor(target ring.InstanceDesc, direct BlocksStoreClient) BlocksStoreClient {
	if !slices.Contains(p.zones, target.Zone) {
		return direct
	}

	set, err := p.teeRing.GetAllHealthy(grpcTeeRingRead)
	if err != nil {
		p.directCalls.Inc()
		return direct
	}
	// GetAllHealthy returns a new slice, so the proxy can filter it in place.
	set.Instances = slices.DeleteFunc(set.Instances, func(tee ring.InstanceDesc) bool { return tee.Zone != target.Zone })
	tee := getNonExcludedInstance(set, nil, randomLoadBalancing, nil)
	if tee == nil {
		p.directCalls.Inc()
		return direct
	}

	c, err := p.teePool.GetClientForInstance(*tee)
	if err != nil {
		p.directCalls.Inc()
		return direct
	}
	return &proxiedStoreGatewayClient{tee: c.(BlocksStoreClient), target: target}
}

// newGRPCTeeClientPool returns the pool of the clients for the grpc-tee instances.
// The pool does no health checks, because grpc-tee forwards each call to a store-gateway and a health check
// has no target. The heartbeats in the grpc-tee ring give the health of each instance.
func newGRPCTeeClientPool(teeRing ring.ReadRing, clientCfg StoreGatewayClientConfig, logger log.Logger, reg prometheus.Registerer) *client.Pool {
	poolCfg := client.PoolConfig{
		CheckInterval:      10 * time.Second,
		HealthCheckEnabled: false,
	}

	clientsCount := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_querier_store_gateway_proxy_clients",
		Help: "The current number of grpc-tee clients in the pool of the store-gateway proxy.",
	})

	// The clients of the grpc-tee instances use the same metrics as the store-gateway clients. Those metrics are
	// already registered, so the clients of the grpc-tee instances register their metrics nowhere.
	factory := newStoreGatewayClientFactory(clientCfg.Config, nil, logger)

	return client.NewPool("grpc-tee", poolCfg, client.NewRingServiceDiscovery(teeRing), factory, clientsCount, logger)
}

// proxiedStoreGatewayClient sends the calls for a target store-gateway through a grpc-tee instance.
// It adds the target to the gRPC metadata of each call, and grpc-tee forwards the call to the target.
type proxiedStoreGatewayClient struct {
	tee    BlocksStoreClient
	target ring.InstanceDesc
}

func (c *proxiedStoreGatewayClient) withTarget(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, GRPCTeeTargetAddrMetadataKey, c.target.Addr, GRPCTeeTargetIDMetadataKey, c.target.Id)
}

func (c *proxiedStoreGatewayClient) Series(ctx context.Context, in *storepb.SeriesRequest, opts ...grpc.CallOption) (storegatewaypb.StoreGateway_SeriesClient, error) {
	return c.tee.Series(c.withTarget(ctx), in, opts...)
}

func (c *proxiedStoreGatewayClient) LabelNames(ctx context.Context, in *storepb.LabelNamesRequest, opts ...grpc.CallOption) (*storepb.LabelNamesResponse, error) {
	return c.tee.LabelNames(c.withTarget(ctx), in, opts...)
}

func (c *proxiedStoreGatewayClient) LabelValues(ctx context.Context, in *storepb.LabelValuesRequest, opts ...grpc.CallOption) (*storepb.LabelValuesResponse, error) {
	return c.tee.LabelValues(c.withTarget(ctx), in, opts...)
}

func (c *proxiedStoreGatewayClient) SearchLabelNames(ctx context.Context, in *storepb.SearchLabelNamesRequest, opts ...grpc.CallOption) (storegatewaypb.StoreGateway_SearchLabelNamesClient, error) {
	return c.tee.SearchLabelNames(c.withTarget(ctx), in, opts...)
}

func (c *proxiedStoreGatewayClient) SearchLabelValues(ctx context.Context, in *storepb.SearchLabelValuesRequest, opts ...grpc.CallOption) (storegatewaypb.StoreGateway_SearchLabelValuesClient, error) {
	return c.tee.SearchLabelValues(c.withTarget(ctx), in, opts...)
}

// RemoteAddress returns the address of the target store-gateway, so the exclude list and the stats of the
// querier use the store-gateway and not the grpc-tee instance.
func (c *proxiedStoreGatewayClient) RemoteAddress() string {
	return c.target.Addr
}

// RemoteZone returns the zone of the target store-gateway.
func (c *proxiedStoreGatewayClient) RemoteZone() string {
	return c.target.Zone
}

// Close does nothing. The pool of the grpc-tee clients owns the connection.
func (c *proxiedStoreGatewayClient) Close() error {
	return nil
}

func (c *proxiedStoreGatewayClient) String() string {
	return c.target.Addr + " via grpc-tee " + c.tee.RemoteAddress()
}
