// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/compartments"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/util/extprom"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// TestBlocksStoreQuerier_ConsistencyCheckWhileTenantShardSizeChanges reproduces the race when a tenant's shard size
// changes: queriers switch to the new tenant shard immediately, while its new owners load the blocks only at their
// next sync.
func TestBlocksStoreQuerier_ConsistencyCheckWhileTenantShardSizeChanges(t *testing.T) {
	tests := map[string]struct {
		fromShardSize int
		toShardSize   int
	}{
		"shard size increased":      {fromShardSize: 3, toShardSize: 6},
		"shard size decreased":      {fromShardSize: 6, toShardSize: 3},
		"shuffle sharding disabled": {fromShardSize: 3, toShardSize: 0},
	}

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			t.Run("without shard size transitions the query fails until the new owners sync", func(t *testing.T) {
				env := newShardSizeTransitionQueryEnv(t, testData.fromShardSize, 0)

				env.syncStoreGateways(t, env.allStoreGateways())
				require.NoError(t, env.query(t))

				// The shard size changes. Queriers immediately query the new owners, which haven't synced yet.
				env.limits.storeGatewayTenantShardSize = testData.toShardSize

				var consistencyErr *storeConsistencyCheckFailedErr
				require.ErrorAs(t, env.query(t), &consistencyErr)
				require.NotEmpty(t, consistencyErr.remainingBlocks)

				env.syncStoreGateways(t, env.allStoreGateways())
				require.NoError(t, env.query(t))
			})

			t.Run("with shard size transitions the query succeeds while the store-gateways sync", func(t *testing.T) {
				env := newShardSizeTransitionQueryEnv(t, testData.fromShardSize, time.Hour)

				env.syncStoreGateways(t, env.allStoreGateways())
				require.NoError(t, env.query(t))
				require.Len(t, env.lastQueryAttempts, 1)
				previousOwners := env.storeGatewaysWithBlocks()

				// The shard size changes, and the query runs before any store-gateway has synced. The attempt queries the
				// new owners, which don't have all the blocks, and then the previous owners.
				env.limits.storeGatewayTenantShardSize = testData.toShardSize
				require.NoError(t, env.query(t))
				require.Len(t, env.lastQueryAttempts, 2)

				// The previous owners sync first. They must keep their blocks, because the new owners haven't loaded
				// them yet.
				env.syncStoreGateways(t, previousOwners)
				require.NoError(t, env.query(t))

				env.syncStoreGateways(t, env.allStoreGateways())
				require.NoError(t, env.query(t))

				// Each block is served by an owner in the new tenant shard in a single attempt, even though the previous
				// owners still have the blocks loaded.
				require.Len(t, env.lastQueryAttempts, 1)
				env.assertBlocksQueriedFromNewOwners(t)
			})
		})
	}
}

func TestBlocksStoreQuerier_ShardSizeTransitionShouldEnd(t *testing.T) {
	const transitionPeriod = 300 * time.Millisecond

	env := newShardSizeTransitionQueryEnv(t, 6, transitionPeriod)
	env.syncStoreGateways(t, env.allStoreGateways())
	require.NoError(t, env.query(t))
	assert.Zero(t, env.querierTransitionEvents(t, "started"))

	env.limits.storeGatewayTenantShardSize = 3
	require.NoError(t, env.query(t))
	env.syncStoreGateways(t, env.allStoreGateways())
	require.NoError(t, env.query(t))

	assert.True(t, env.inShardSizeTransition(t))
	assert.Equal(t, 1.0, env.querierTransitionEvents(t, "started"))
	assert.Zero(t, env.querierTransitionEvents(t, "ended"))
	assert.Equal(t, 1.0, env.storeGatewayTransitionEvents(t, "started"))
	assert.Zero(t, env.storeGatewayTransitionEvents(t, "ended"))

	leaving := env.storeGatewaysLeavingTheTenantShard(t)
	require.NotEmpty(t, leaving)
	for _, addr := range leaving {
		assert.NotEmpty(t, env.storeGateways[addr].loaded, "store-gateway %s", addr)
	}

	// After the period, the next sync unloads the blocks of the leaving store-gateways, and queries keep using a single attempt.
	time.Sleep(transitionPeriod + 100*time.Millisecond)
	env.syncStoreGateways(t, env.allStoreGateways())
	require.NoError(t, env.query(t))

	require.Len(t, env.lastQueryAttempts, 1)
	env.assertBlocksQueriedFromNewOwners(t)
	assert.False(t, env.inShardSizeTransition(t))
	for _, addr := range leaving {
		assert.Empty(t, env.storeGateways[addr].loaded, "store-gateway %s", addr)
	}

	assert.Equal(t, 1.0, env.querierTransitionEvents(t, "started"))
	assert.Equal(t, 1.0, env.querierTransitionEvents(t, "ended"))
	assert.Equal(t, 1.0, env.storeGatewayTransitionEvents(t, "started"))
	assert.Equal(t, 1.0, env.storeGatewayTransitionEvents(t, "ended"))
}

// shardSizeTransitionQueryEnv is a querier and a set of simulated store-gateways sharing the same ring.
type shardSizeTransitionQueryEnv struct {
	userID   string
	blockIDs []ulid.ULID

	ring          *ring.Ring
	limits        *blocksStoreLimitsMock
	storeGateways map[string]*simulatedStoreGateway
	querier       *blocksStoreQuerier
	stores        *blocksStoreReplicationSet
	finder        *blocksFinderMock
	querierReg    *prometheus.Registry

	// lastQueryAttempts records, for each query to store-gateways of the last request, the store-gateway each block was sent to.
	lastQueryAttempts []map[ulid.ULID]string
}

// simulatedStoreGateway keeps track of the blocks a store-gateway has loaded, using the store-gateway sharding strategy.
type simulatedStoreGateway struct {
	id       string
	addr     string
	strategy *storegateway.ShuffleShardingStrategy
	reg      *prometheus.Registry
	loaded   map[ulid.ULID]struct{}
}

// newShardSizeTransitionQueryEnv returns an environment where the queriers and store-gateways use shard size
// transitions for the input period, or don't if the period is 0.
func newShardSizeTransitionQueryEnv(t *testing.T, shardSize int, transitionPeriod time.Duration) *shardSizeTransitionQueryEnv {
	t.Helper()

	const (
		numZones          = 3
		instancesPerZone  = 4
		replicationFactor = 3
		numBlocks         = 100
	)

	ctx := t.Context()
	logger := log.NewNopLogger()
	env := &shardSizeTransitionQueryEnv{
		userID:        "user-1",
		limits:        &blocksStoreLimitsMock{storeGatewayTenantShardSize: shardSize},
		storeGateways: map[string]*simulatedStoreGateway{},
		querierReg:    prometheus.NewPedanticRegistry(),
	}
	for i := 0; i < numBlocks; i++ {
		env.blockIDs = append(env.blockIDs, ulid.MustNew(uint64(i+1), nil))
	}

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), logger, nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Use a fixed seed for the tokens, so that the blocks ownership is the same on every run.
	tokenGenerator := ring.NewRandomTokenGeneratorWithSeed(1)
	registeredAt := time.Now().Add(-time.Hour)
	var takenTokens []uint32

	require.NoError(t, ringStore.CAS(ctx, "test", func(interface{}) (interface{}, bool, error) {
		d := ring.NewDesc()
		for z := 0; z < numZones; z++ {
			for i := 0; i < instancesPerZone; i++ {
				id := fmt.Sprintf("store-gateway-zone-%d-%d", z, i)
				addr := fmt.Sprintf("127.0.%d.%d", z, i)
				tokens := tokenGenerator.GenerateTokens(128, takenTokens)
				takenTokens = append(takenTokens, tokens...)

				d.AddIngester(id, addr, fmt.Sprintf("zone-%d", z), tokens, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				env.storeGateways[addr] = &simulatedStoreGateway{id: id, addr: addr, reg: prometheus.NewPedanticRegistry(), loaded: map[ulid.ULID]struct{}{}}
			}
		}
		return d, true, nil
	}))

	ringCfg := ring.Config{}
	flagext.DefaultValues(&ringCfg)
	ringCfg.ReplicationFactor = replicationFactor
	ringCfg.ZoneAwarenessEnabled = true

	var err error
	env.ring, err = ring.NewWithStoreClientAndStrategy(ringCfg, "test", "test", ringStore, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, logger)
	require.NoError(t, err)

	dynamicReplication := storegateway.NewNopDynamicReplication(replicationFactor)

	// Each store-gateway tracks the shard size transitions on its own, like separate processes do.
	for _, g := range env.storeGateways {
		var transitions *storegateway.ShardSizeTransitions
		if transitionPeriod > 0 {
			transitions = storegateway.NewShardSizeTransitions(transitionPeriod, "store-gateway", g.reg)
		}
		g.strategy = storegateway.NewShuffleShardingStrategy(env.ring, g.id, g.addr, dynamicReplication, env.limits, transitions, logger)
	}

	var transitions *storegateway.ShardSizeTransitions
	if transitionPeriod > 0 {
		transitions = storegateway.NewShardSizeTransitions(transitionPeriod, "querier", env.querierReg)
	}

	pool := newStoreGatewayClientPool(client.NewRingServiceDiscovery(env.ring), StoreGatewayClientConfig{}, logger, env.querierReg)
	env.stores, err = newBlocksStoreReplicationSet(env.ring, pool, randomLoadBalancing, dynamicReplication, nil, env.limits, transitions)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, env.stores))
	t.Cleanup(func() {
		// The clients are cached by the pool and never connect, so they're closed once at the end of the test.
		for _, addr := range pool.RegisteredAddresses() {
			pool.RemoveClientFor(addr)
		}
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), env.stores))
	})

	test.Poll(t, time.Second, numZones*instancesPerZone, func() interface{} {
		all, err := env.ring.GetAllHealthy(storegateway.BlocksRead)
		if err != nil {
			return 0
		}
		return len(all.Instances)
	})

	minT := time.Now().Add(-5 * time.Hour)
	maxT := minT.Add(2 * time.Hour)
	knownBlocks := make(bucketindex.Blocks, 0, len(env.blockIDs))
	for _, blockID := range env.blockIDs {
		knownBlocks = append(knownBlocks, newBlock(blockID, minT, maxT))
	}

	env.finder = &blocksFinderMock{}
	env.finder.On("GetBlocks", mock.Anything, env.userID, mock.Anything, mock.Anything).Return(knownBlocks, &bucketindex.Metadata{}, nil)

	env.querier = &blocksStoreQuerier{
		minT:               minT.UnixMilli(),
		maxT:               maxT.UnixMilli(),
		dynamicReplication: dynamicReplication,
		consistency:        NewBlocksConsistency(0, env.querierReg),
		logger:             logger,
		metrics:            newBlocksStoreQueryableMetrics(compartments.Config{}, env.querierReg),
		limits:             env.limits,
	}

	return env
}

// query runs a query through the querier consistency check, where each store-gateway returns the blocks it has loaded.
func (e *shardSizeTransitionQueryEnv) query(t *testing.T) error {
	t.Helper()

	e.lastQueryAttempts = nil
	queryF := func(_ context.Context, clients map[BlocksStoreClient][][]ulid.ULID, _, _ int64, _ *bucketindex.Metadata) ([]ulid.ULID, error) {
		var queried []ulid.ULID
		attempt := map[ulid.ULID]string{}
		e.lastQueryAttempts = append(e.lastQueryAttempts, attempt)
		for c, partitions := range clients {
			g, ok := e.storeGateways[c.RemoteAddress()]
			require.True(t, ok, "unexpected store-gateway %s", c.RemoteAddress())

			for _, partition := range partitions {
				for _, blockID := range partition {
					attempt[blockID] = c.RemoteAddress()
					if _, loaded := g.loaded[blockID]; loaded {
						queried = append(queried, blockID)
					}
				}
			}
		}
		return queried, nil
	}

	spanLog, ctx := spanlogger.New(t.Context(), log.NewNopLogger(), tracer, "test")
	_, err := e.querier.queryWithConsistencyCheck(ctx, spanLog, e.finder, e.stores, e.querier.minT, e.querier.maxT, e.userID, nil, queryF)
	return err
}

func (e *shardSizeTransitionQueryEnv) inShardSizeTransition(t *testing.T) bool {
	t.Helper()

	_, inTransition, err := e.stores.GetPreviousClientsFor(e.userID, nil, nil)
	require.NoError(t, err)
	return inTransition
}

func (e *shardSizeTransitionQueryEnv) allStoreGateways() []string {
	addrs := make([]string, 0, len(e.storeGateways))
	for addr := range e.storeGateways {
		addrs = append(addrs, addr)
	}
	return addrs
}

func (e *shardSizeTransitionQueryEnv) storeGatewaysWithBlocks() []string {
	var addrs []string
	for addr, g := range e.storeGateways {
		if len(g.loaded) > 0 {
			addrs = append(addrs, addr)
		}
	}
	return addrs
}

func (e *shardSizeTransitionQueryEnv) storeGatewaysLeavingTheTenantShard(t *testing.T) []string {
	t.Helper()

	currentRing, _ := storegateway.GetShuffleShardingSubrings(e.ring, e.userID, e.limits, nil)

	var addrs []string
	for addr, g := range e.storeGateways {
		if len(g.loaded) > 0 && !currentRing.HasInstance(g.id) {
			addrs = append(addrs, addr)
		}
	}
	return addrs
}

func (e *shardSizeTransitionQueryEnv) syncStoreGateways(t *testing.T, addrs []string) {
	t.Helper()

	for _, addr := range addrs {
		g := e.storeGateways[addr]

		users, err := g.strategy.FilterUsers(t.Context(), []string{e.userID})
		require.NoError(t, err)

		if len(users) == 0 {
			g.loaded = map[ulid.ULID]struct{}{}
			continue
		}

		metas := make(map[ulid.ULID]*block.Meta, len(e.blockIDs))
		for _, blockID := range e.blockIDs {
			metas[blockID] = &block.Meta{BlockMeta: tsdb.BlockMeta{ULID: blockID}}
		}

		synced := extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{}, []string{"state"})
		require.NoError(t, g.strategy.FilterBlocks(t.Context(), e.userID, metas, g.loaded, synced))

		g.loaded = make(map[ulid.ULID]struct{}, len(metas))
		for blockID := range metas {
			g.loaded[blockID] = struct{}{}
		}
	}
}

func (e *shardSizeTransitionQueryEnv) assertBlocksQueriedFromNewOwners(t *testing.T) {
	t.Helper()

	require.Len(t, e.lastQueryAttempts, 1)
	currentRing, _ := storegateway.GetShuffleShardingSubrings(e.ring, e.userID, e.limits, nil)

	for _, blockID := range e.blockIDs {
		set, err := currentRing.GetWithOptions(mimir_tsdb.HashBlockID(blockID), storegateway.BlocksOwnerSync)
		require.NoError(t, err)

		for _, addr := range set.GetAddresses() {
			assert.Contains(t, e.storeGateways[addr].loaded, blockID, "block %s should be loaded by its new owner %s", blockID, addr)
		}
		assert.Contains(t, set.GetAddresses(), e.lastQueryAttempts[0][blockID], "block %s should be queried from a new owner", blockID)
	}
}

func (e *shardSizeTransitionQueryEnv) querierTransitionEvents(t *testing.T, event string) float64 {
	t.Helper()
	return transitionEvents(t, e.querierReg, "querier", event)
}

// storeGatewayTransitionEvents returns the events recorded by a store-gateway, which are the same on all of them
// because they observe the same tenant at every sync.
func (e *shardSizeTransitionQueryEnv) storeGatewayTransitionEvents(t *testing.T, event string) float64 {
	t.Helper()

	for _, g := range e.storeGateways {
		return transitionEvents(t, g.reg, "store-gateway", event)
	}
	return 0
}

func transitionEvents(t *testing.T, reg prometheus.Gatherer, component, event string) float64 {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	for _, family := range families {
		if family.GetName() != "cortex_shard_size_transitions_total" {
			continue
		}
		for _, m := range family.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["component"] == component && labels["event"] == event {
				return m.GetCounter().GetValue()
			}
		}
	}

	require.FailNow(t, "shard size transitions metric not found", "component=%s event=%s", component, event)
	return 0
}
