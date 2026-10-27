// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

func TestStoreGatewayProxyConfig_Enabled(t *testing.T) {
	require.False(t, (&StoreGatewayProxyConfig{}).Enabled())
	require.True(t, (&StoreGatewayProxyConfig{Zones: []string{"zone-a"}}).Enabled())
}

func TestStoreGatewayProxy_ClientFor(t *testing.T) {
	tee := startFakeGRPCTee(t)

	zoneA := ring.InstanceDesc{Id: "store-gateway-1", Addr: "10.0.0.1:9095", Zone: "zone-a"}
	zoneB := ring.InstanceDesc{Id: "store-gateway-3", Addr: "10.0.0.3:9095", Zone: "zone-b"}
	direct := &proxiedStoreGatewayClient{target: ring.InstanceDesc{Addr: "direct"}}

	t.Run("a target in the proxy zone goes through a grpc-tee instance", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		proxy := startTestStoreGatewayProxy(t, []string{"zone-a"}, reg, func(d *ring.Desc) {
			d.AddIngester("grpc-tee-1", tee.addr, "zone-a", []uint32{1}, ring.ACTIVE, time.Now(), false, time.Time{}, nil)
		})

		c := proxy.clientFor(zoneA, direct)
		require.IsType(t, &proxiedStoreGatewayClient{}, c)
		assert.Equal(t, zoneA.Addr, c.RemoteAddress())
		assert.Equal(t, zoneA.Zone, c.RemoteZone())

		_, err := c.LabelNames(user.InjectOrgID(t.Context(), "user-1"), &storepb.LabelNamesRequest{})
		require.NoError(t, err)
		md := tee.lastMetadata()
		assert.Equal(t, []string{zoneA.Addr}, md.Get(GRPCTeeTargetAddrMetadataKey))
		assert.Equal(t, []string{zoneA.Id}, md.Get(GRPCTeeTargetIDMetadataKey))

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_querier_store_gateway_proxy_direct_calls_total Total number of store-gateway calls in a proxy zone that the querier sent direct, because no grpc-tee instance in that zone was healthy.
			# TYPE cortex_querier_store_gateway_proxy_direct_calls_total counter
			cortex_querier_store_gateway_proxy_direct_calls_total 0
		`), "cortex_querier_store_gateway_proxy_direct_calls_total"))
	})

	t.Run("a target in another zone goes direct", func(t *testing.T) {
		proxy := startTestStoreGatewayProxy(t, []string{"zone-a"}, prometheus.NewPedanticRegistry(), func(d *ring.Desc) {
			d.AddIngester("grpc-tee-1", tee.addr, "zone-a", []uint32{1}, ring.ACTIVE, time.Now(), false, time.Time{}, nil)
		})

		assert.Same(t, direct, proxy.clientFor(zoneB, direct))
	})

	t.Run("a target goes through a grpc-tee instance in its own zone", func(t *testing.T) {
		teeB := startFakeGRPCTee(t)
		proxy := startTestStoreGatewayProxy(t, []string{"zone-a", "zone-b"}, prometheus.NewPedanticRegistry(), func(d *ring.Desc) {
			d.AddIngester("grpc-tee-a-1", tee.addr, "zone-a", []uint32{1}, ring.ACTIVE, time.Now(), false, time.Time{}, nil)
			d.AddIngester("grpc-tee-b-1", teeB.addr, "zone-b", []uint32{2}, ring.ACTIVE, time.Now(), false, time.Time{}, nil)
		})

		for range 10 {
			c := proxy.clientFor(zoneB, direct)
			require.IsType(t, &proxiedStoreGatewayClient{}, c)
			assert.Equal(t, teeB.addr, c.(*proxiedStoreGatewayClient).tee.RemoteAddress())
		}
	})

	t.Run("a target in a proxy zone goes direct if no grpc-tee instance in that zone is healthy", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		proxy := startTestStoreGatewayProxy(t, []string{"zone-a", "zone-b"}, reg, func(d *ring.Desc) {
			d.AddIngester("grpc-tee-a-1", tee.addr, "zone-a", []uint32{1}, ring.ACTIVE, time.Now(), false, time.Time{}, nil)
		})

		assert.Same(t, direct, proxy.clientFor(zoneB, direct))
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_querier_store_gateway_proxy_direct_calls_total Total number of store-gateway calls in a proxy zone that the querier sent direct, because no grpc-tee instance in that zone was healthy.
			# TYPE cortex_querier_store_gateway_proxy_direct_calls_total counter
			cortex_querier_store_gateway_proxy_direct_calls_total 1
		`), "cortex_querier_store_gateway_proxy_direct_calls_total"))
	})

	t.Run("a target in the proxy zone goes direct if no grpc-tee instance is healthy", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		proxy := startTestStoreGatewayProxy(t, []string{"zone-a"}, reg, func(d *ring.Desc) {
			d.AddIngester("grpc-tee-1", tee.addr, "zone-a", []uint32{1}, ring.LEAVING, time.Now(), false, time.Time{}, nil)
		})

		assert.Same(t, direct, proxy.clientFor(zoneA, direct))
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_querier_store_gateway_proxy_direct_calls_total Total number of store-gateway calls in a proxy zone that the querier sent direct, because no grpc-tee instance in that zone was healthy.
			# TYPE cortex_querier_store_gateway_proxy_direct_calls_total counter
			cortex_querier_store_gateway_proxy_direct_calls_total 1
		`), "cortex_querier_store_gateway_proxy_direct_calls_total"))
	})
}

func TestBlocksStoreReplicationSet_GetClientsFor_WithProxy(t *testing.T) {
	ctx := context.Background()
	tee := startFakeGRPCTee(t)

	block1, block2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })
	require.NoError(t, ringStore.CAS(ctx, "test", func(any) (any, bool, error) {
		d := ring.NewDesc()
		d.AddIngester("store-gateway-1", "127.0.0.1", "zone-a", []uint32{1}, ring.ACTIVE, time.Now(), false, time.Time{}, nil)
		d.AddIngester("store-gateway-3", "127.0.0.3", "zone-b", []uint32{2}, ring.ACTIVE, time.Now(), false, time.Time{}, nil)
		return d, true, nil
	}))

	ringCfg := ring.Config{}
	flagext.DefaultValues(&ringCfg)
	ringCfg.ReplicationFactor = 2
	ringCfg.ZoneAwarenessEnabled = true
	r, err := ring.NewWithStoreClientAndStrategy(ringCfg, "test", "test", ringStore, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, log.NewNopLogger())
	require.NoError(t, err)

	pool := newStoreGatewayClientPool(client.NewRingServiceDiscovery(r), StoreGatewayClientConfig{}, log.NewNopLogger(), nil)
	s, err := newBlocksStoreReplicationSet(r, pool, randomLoadBalancing, storegateway.NewNopDynamicReplication(2), nil, &blocksStoreLimitsMock{})
	require.NoError(t, err)
	s.proxy = startTestStoreGatewayProxy(t, []string{"zone-a"}, nil, func(d *ring.Desc) {
		d.AddIngester("grpc-tee-1", tee.addr, "zone-a", []uint32{1}, ring.ACTIVE, time.Now(), false, time.Time{}, nil)
	})
	require.NoError(t, services.StartAndAwaitRunning(ctx, s))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(ctx, s)) })
	require.NoError(t, services.StartAndAwaitRunning(ctx, pool))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, pool))
		// The pool does not close its clients when it stops.
		for _, addr := range pool.RegisteredAddresses() {
			pool.RemoveClientFor(addr)
		}
	})
	test.Poll(t, time.Second, 2, func() any { return r.InstancesCount() })

	blocks := bucketindex.Blocks{{ID: block1}, {ID: block2}}
	// With RF 2 and two zones, each block has one owner in each zone. Exclude zone-b for block1 and zone-a for block2.
	clients, err := s.GetClientsFor("user-1", blocks, map[ulid.ULID][]string{block1: {"127.0.0.3"}, block2: {"127.0.0.1"}})
	require.NoError(t, err)
	t.Cleanup(func() {
		for c := range clients {
			assert.NoError(t, c.(interface{ Close() error }).Close())
		}
	})

	assert.Equal(t, map[string][][]ulid.ULID{"127.0.0.1": {{block1}}, "127.0.0.3": {{block2}}}, getStoreGatewayClientAddrs(clients))
	for c := range clients {
		_, proxied := c.(*proxiedStoreGatewayClient)
		assert.Equal(t, c.RemoteZone() == "zone-a", proxied, "client for %s", c.RemoteAddress())
	}
}

// startTestStoreGatewayProxy starts a storeGatewayProxy with a grpc-tee ring that setupRing fills.
func startTestStoreGatewayProxy(t *testing.T, zones []string, reg prometheus.Registerer, setupRing func(*ring.Desc)) *storeGatewayProxy {
	ctx := context.Background()
	store, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })
	require.NoError(t, store.CAS(ctx, grpcTeeRingKey, func(any) (any, bool, error) {
		d := ring.NewDesc()
		setupRing(d)
		return d, true, nil
	}))

	ringCfg := ring.Config{}
	flagext.DefaultValues(&ringCfg)
	ringCfg.ReplicationFactor = 1
	teeRing, err := ring.NewWithStoreClientAndStrategy(ringCfg, grpcTeeRingName, grpcTeeRingKey, store, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, log.NewNopLogger())
	require.NoError(t, err)

	teePool := newGRPCTeeClientPool(teeRing, StoreGatewayClientConfig{}, log.NewNopLogger(), nil)
	proxy, err := newStoreGatewayProxyWithRing(zones, teeRing, teePool, reg)
	require.NoError(t, err)
	proxy.subservices, err = services.NewManager(teeRing, teePool)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, proxy))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(ctx, proxy)) })
	test.Poll(t, time.Second, true, func() any { return teeRing.InstancesCount() > 0 })
	return proxy
}

// fakeGRPCTee is a gRPC server that records the incoming metadata of each call.
type fakeGRPCTee struct {
	storegatewaypb.UnimplementedStoreGatewayServer

	addr     string
	metadata chan metadata.MD
}

func startFakeGRPCTee(t *testing.T) *fakeGRPCTee {
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	tee := &fakeGRPCTee{addr: lis.Addr().String(), metadata: make(chan metadata.MD, 10)}
	srv := grpc.NewServer()
	storegatewaypb.RegisterStoreGatewayServer(srv, tee)

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		require.NoError(t, <-serveErr)
	})
	return tee
}

func (f *fakeGRPCTee) LabelNames(ctx context.Context, _ *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx) //lint:ignore faillint The test records all keys.
	f.metadata <- md
	return &storepb.LabelNamesResponse{}, nil
}

func (f *fakeGRPCTee) lastMetadata() metadata.MD {
	return <-f.metadata
}
