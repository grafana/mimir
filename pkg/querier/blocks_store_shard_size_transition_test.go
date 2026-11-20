// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type shardSizeTransitionTestEnv struct {
	ring   *ring.Ring
	limits *blocksStoreLimitsMock
	set    *blocksStoreReplicationSet
	blocks bucketindex.Blocks

	// clients are the clients handed out by the pool, which are shared and closed once at the end.
	clients map[string]io.Closer
}

func newShardSizeTransitionTestEnv(t testing.TB, shardSize int, transitions *storegateway.ShardSizeTransitions) *shardSizeTransitionTestEnv {
	t.Helper()

	const (
		numInstances = 12
		numBlocks    = 100
	)
	ctx := context.Background()

	store, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	random := rand.New(rand.NewSource(1))
	generator := ring.NewRandomTokenGeneratorWithSeed(random.Int63())
	require.NoError(t, store.CAS(ctx, "test", func(interface{}) (interface{}, bool, error) {
		d := ring.NewDesc()
		for i := 0; i < numInstances; i++ {
			id := fmt.Sprintf("instance-%d", i)
			d.AddIngester(id, fmt.Sprintf("127.0.0.%d", i+1), "", generator.GenerateTokens(512, nil), ring.ACTIVE, time.Now(), false, time.Time{}, nil)
		}
		return d, true, nil
	}))

	ringCfg := ring.Config{ReplicationFactor: 3, HeartbeatTimeout: time.Minute}
	r, err := ring.NewWithStoreClientAndStrategy(ringCfg, "test", "test", store, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, log.NewNopLogger())
	require.NoError(t, err)
	limits := &blocksStoreLimitsMock{storeGatewayTenantShardSize: shardSize}
	pool := newStoreGatewayClientPool(client.NewRingServiceDiscovery(r), StoreGatewayClientConfig{}, log.NewNopLogger(), prometheus.NewPedanticRegistry())
	set, err := newBlocksStoreReplicationSet(r, pool, noLoadBalancing, storegateway.NewNopDynamicReplication(ringCfg.ReplicationFactor), nil, limits, transitions)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, set))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(ctx, set)) })

	// Wait until the ring client has initialised the state.
	test.Poll(t, time.Second, numInstances, func() interface{} {
		all, err := r.GetAllHealthy(storegateway.BlocksRead)
		if err != nil {
			return 0
		}
		return len(all.Instances)
	})

	minT := time.Now().Add(-5 * time.Hour)
	blocks := make(bucketindex.Blocks, 0, numBlocks)
	for i := 0; i < numBlocks; i++ {
		blocks = append(blocks, newBlock(ulid.MustNew(uint64(i+1), random), minT, minT.Add(time.Hour)))
	}

	env := &shardSizeTransitionTestEnv{ring: r, limits: limits, set: set, blocks: blocks, clients: map[string]io.Closer{}}
	t.Cleanup(func() {
		for _, c := range env.clients {
			assert.NoError(t, c.Close())
		}
	})
	return env
}

// getClients returns the address of the store-gateway chosen for each block.
func (e *shardSizeTransitionTestEnv) getClients(t testing.TB, userID string, exclude map[ulid.ULID][]string) map[ulid.ULID]string {
	t.Helper()

	clients, err := e.set.GetClientsFor(userID, e.blocks, exclude)
	require.NoError(t, err)

	chosen := map[ulid.ULID]string{}
	for c, partitions := range clients {
		e.clients[c.RemoteAddress()] = c.(io.Closer)
		for _, partition := range partitions {
			for _, id := range partition {
				chosen[id] = c.RemoteAddress()
			}
		}
	}
	require.Len(t, chosen, len(e.blocks))
	return chosen
}

func (e *shardSizeTransitionTestEnv) addrsInShard(userID string, shardSize int) map[string]struct{} {
	all, err := e.ring.ShuffleShard(userID, shardSize).GetAllHealthy(storegateway.BlocksRead)
	if err != nil {
		panic(err)
	}
	addrs := map[string]struct{}{}
	for _, inst := range all.Instances {
		addrs[inst.Addr] = struct{}{}
	}
	return addrs
}

func TestBlocksStoreReplicationSet_GetClientsFor_ShardSizeTransition(t *testing.T) {
	const (
		userID       = "user-1"
		oldShardSize = 4
		newShardSize = 8
	)

	oldTransitionPeriod := time.Hour
	env := newShardSizeTransitionTestEnv(t, oldShardSize, storegateway.NewShardSizeTransitions(oldTransitionPeriod, "querier", nil))
	oldAddrs := env.addrsInShard(userID, oldShardSize)
	newAddrs := env.addrsInShard(userID, newShardSize)

	// Before any change, only the tenant shard is used.
	for _, addr := range env.getClients(t, userID, nil) {
		require.Contains(t, oldAddrs, addr)
	}

	env.limits.storeGatewayTenantShardSize = newShardSize

	// Replay the retries of the querier: each attempt excludes the instances tried in previous attempts.
	exclude := map[ulid.ULID][]string{}
	var oldOnlyAttempts, newOnlyAttempts int
	for attempt := 0; attempt < 3; attempt++ {
		chosen := env.getClients(t, userID, exclude)
		for id, addr := range chosen {
			exclude[id] = append(exclude[id], addr)

			_, inOld := oldAddrs[addr]
			_, inNew := newAddrs[addr]
			require.True(t, inOld || inNew, "attempt %d queried an instance outside both shards", attempt)

			if attempt%2 == 0 {
				// The first attempt, and every other one, goes to the new tenant shard.
				require.True(t, inNew, "attempt %d should query the new shard", attempt)
				newOnlyAttempts++
			} else {
				require.True(t, inOld, "attempt %d should query the previous shard", attempt)
				oldOnlyAttempts++
			}
		}
	}
	require.NotZero(t, newOnlyAttempts)
	require.NotZero(t, oldOnlyAttempts)
}

func TestBlocksStoreReplicationSet_GetClientsFor_ShardSizeTransitionShouldFallBackWhenAShardHasNoInstanceLeft(t *testing.T) {
	const (
		userID       = "user-1"
		oldShardSize = 3
		newShardSize = 12
	)

	env := newShardSizeTransitionTestEnv(t, oldShardSize, storegateway.NewShardSizeTransitions(time.Hour, "querier", nil))
	oldAddrs := env.addrsInShard(userID, oldShardSize)

	env.getClients(t, userID, nil)
	env.limits.storeGatewayTenantShardSize = newShardSize

	// Exclude every instance of the previous shard for every block, as if they had all been tried, and
	// pretend this is an odd attempt: the previous shard is preferred but has no instance left.
	exclude := map[ulid.ULID][]string{}
	for _, b := range env.blocks {
		for addr := range oldAddrs {
			exclude[b.ID] = append(exclude[b.ID], addr)
		}
		if len(exclude[b.ID])%2 == 0 {
			// Keep the attempt odd without excluding an instance from the new shard.
			exclude[b.ID] = append(exclude[b.ID], "unrelated")
		}
	}

	for _, addr := range env.getClients(t, userID, exclude) {
		require.NotContains(t, oldAddrs, addr)
	}
}

func TestBlocksStoreReplicationSet_GetClientsFor_ShardSizeTransitionShouldEnd(t *testing.T) {
	const (
		userID       = "user-1"
		oldShardSize = 3
		newShardSize = 6
	)

	env := newShardSizeTransitionTestEnv(t, oldShardSize, storegateway.NewShardSizeTransitions(100*time.Millisecond, "querier", nil))
	newAddrs := env.addrsInShard(userID, newShardSize)

	env.getClients(t, userID, nil)
	env.limits.storeGatewayTenantShardSize = newShardSize
	env.getClients(t, userID, nil)

	time.Sleep(150 * time.Millisecond)

	// Once settled, every attempt only uses the new shard, as it does when transitions are disabled.
	exclude := map[ulid.ULID][]string{}
	for attempt := 0; attempt < 2; attempt++ {
		for id, addr := range env.getClients(t, userID, exclude) {
			exclude[id] = append(exclude[id], addr)
			require.Contains(t, newAddrs, addr)
		}
	}
}

func TestBlocksStoreReplicationSet_GetClientsFor_ShardSizeTransitionDisabled(t *testing.T) {
	const userID = "user-1"

	env := newShardSizeTransitionTestEnv(t, 3, nil)
	env.getClients(t, userID, nil)
	env.limits.storeGatewayTenantShardSize = 6
	newAddrs := env.addrsInShard(userID, 6)

	exclude := map[ulid.ULID][]string{}
	for attempt := 0; attempt < 2; attempt++ {
		for id, addr := range env.getClients(t, userID, exclude) {
			exclude[id] = append(exclude[id], addr)
			require.Contains(t, newAddrs, addr)
		}
	}
}

func BenchmarkBlocksStoreReplicationSet_GetClientsFor_ShardSizeTransitions(b *testing.B) {
	const userID = "user-1"

	tests := map[string]struct {
		enabled      bool
		inTransition bool
		retry        bool
	}{
		"disabled":                             {},
		"enabled and settled":                  {enabled: true},
		"enabled and in transition":            {enabled: true, inTransition: true},
		"enabled and in transition, 2nd tries": {enabled: true, inTransition: true, retry: true},
	}

	for name, testData := range tests {
		b.Run(name, func(b *testing.B) {
			var transitions *storegateway.ShardSizeTransitions
			if testData.enabled {
				transitions = storegateway.NewShardSizeTransitions(time.Hour, "querier", nil)
			}
			env := newShardSizeTransitionTestEnv(b, 6, transitions)
			env.getClients(b, userID, nil)
			if testData.inTransition {
				env.limits.storeGatewayTenantShardSize = 9
			}

			// The retry excludes the instances picked by the first attempt.
			var exclude map[ulid.ULID][]string
			if testData.retry {
				exclude = map[ulid.ULID][]string{}
				for id, addr := range env.getClients(b, userID, nil) {
					exclude[id] = []string{addr}
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				clients, err := env.set.GetClientsFor(userID, env.blocks, exclude)
				if err != nil {
					b.Fatal(err)
				}
				_ = clients
				_ = env.set.InShardSizeTransition(userID)
			}

			// Register the pooled clients, to close them at the end.
			b.StopTimer()
			env.getClients(b, userID, exclude)
		})
	}
}

func TestBlocksStoreQuerier_ShardSizeTransitionShouldDoubleAttempts(t *testing.T) {
	const (
		replicationFactor = 3
		minT              = int64(10)
		maxT              = int64(20)
	)

	block := ulid.MustNew(1, nil)

	// The store-gateways queried by the first 5 attempts don't have the block.
	newResponses := func() []interface{} {
		responses := make([]interface{}, 0, 2*replicationFactor)
		for i := 0; i < 2*replicationFactor; i++ {
			series := newSeriesResponseBuilder()
			if i == 2*replicationFactor-1 {
				series = series.addValue(labels.FromStrings(model.MetricNameLabel, "some_metric"), minT, 1).addBlocks(block)
			}
			responses = append(responses, map[BlocksStoreClient][][]ulid.ULID{
				&storeGatewayClientMock{remoteAddr: fmt.Sprintf("1.1.1.%d", i), mockedSeriesResponses: series.build()}: {{block}},
			})
		}
		return responses
	}

	tests := map[string]struct {
		inTransition     bool
		expectedAttempts int
		expectedErr      bool
	}{
		"outside of a transition, attempts are limited to the replication factor": {
			expectedAttempts: replicationFactor,
			expectedErr:      true,
		},
		"during a transition, attempts are doubled": {
			inTransition:     true,
			expectedAttempts: 2 * replicationFactor,
		},
	}

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			stores := &blocksStoreSetMock{mockedResponses: newResponses(), inTransition: testData.inTransition}
			finder := &blocksFinderMock{}
			finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(bucketindex.Blocks{{ID: block}}, &bucketindex.Metadata{}, nil)

			reg := prometheus.NewPedanticRegistry()
			ctx := user.InjectOrgID(t.Context(), "user-1")
			ctx = limiter.ContextWithNewUnlimitedMemoryConsumptionTracker(ctx)
			ctx = limiter.ContextWithNewSeriesLabelsDeduplicator(ctx, limiter.NewSeriesDeduplicatorMetrics(reg))
			querier := &blocksStoreQuerier{
				minT:               minT,
				maxT:               maxT,
				compartments:       []blocksStoreCompartment{{finder: finder, stores: stores}},
				dynamicReplication: storegateway.NewNopDynamicReplication(replicationFactor),
				consistency:        NewBlocksConsistency(0, reg),
				logger:             log.NewNopLogger(),
				metrics:            newBlocksStoreQueryableMetrics(compartments.Config{}, reg),
				limits:             &blocksStoreLimitsMock{},
			}

			seriesSet := querier.Select(ctx, true, &storage.SelectHints{Start: minT, End: maxT}, labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".*"))
			if testData.expectedErr {
				require.Error(t, seriesSet.Err())
				require.ErrorContains(t, seriesSet.Err(), "err-mimir-store-consistency-check-failed")
			} else {
				require.NoError(t, seriesSet.Err())
			}
			require.Equal(t, testData.expectedAttempts, stores.nextResult)
		})
	}
}

// shardSizeTransitionsVariants returns the shard size transitions settings that the replication set tests run with:
// disabled, and enabled. A tracker observing a tenant shard size that never changes is settled, so tests which don't
// change the shard size expect the same results with and without it.

// noShardSizeTransitions returns nil, which disables shard size transitions.
func noShardSizeTransitions() *storegateway.ShardSizeTransitions {
	return nil
}

// newShardSizeTransitionsForTest returns an enabled ShardSizeTransitions. A tracker observing a tenant shard size
// that never changes is settled, so tests which don't change the shard size expect the same results as when disabled.
func newShardSizeTransitionsForTest() *storegateway.ShardSizeTransitions {
	return storegateway.NewShardSizeTransitions(time.Hour, "querier", nil)
}
