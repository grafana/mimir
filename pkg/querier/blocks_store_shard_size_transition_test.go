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

func (e *shardSizeTransitionTestEnv) getClients(t testing.TB, userID string, exclude map[ulid.ULID][]string) map[ulid.ULID]string {
	t.Helper()

	clients, err := e.set.GetClientsFor(userID, e.blocks, exclude)
	return e.chosenAddrs(t, clients, err)
}

func (e *shardSizeTransitionTestEnv) getPreviousClients(t testing.TB, userID string, exclude map[ulid.ULID][]string) map[ulid.ULID]string {
	t.Helper()

	clients, inTransition, err := e.set.GetPreviousClientsFor(userID, e.blocks, exclude)
	require.True(t, inTransition)
	return e.chosenAddrs(t, clients, err)
}

func (e *shardSizeTransitionTestEnv) inTransition(t testing.TB, userID string) bool {
	t.Helper()

	clients, inTransition, err := e.set.GetPreviousClientsFor(userID, nil, nil)
	require.NoError(t, err)
	require.Empty(t, clients)
	return inTransition
}

// chosenAddrs returns the address of the store-gateway chosen for each block.
func (e *shardSizeTransitionTestEnv) chosenAddrs(t testing.TB, clients map[BlocksStoreClient][][]ulid.ULID, err error) map[ulid.ULID]string {
	t.Helper()
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

func TestBlocksStoreReplicationSet_ShardSizeTransition(t *testing.T) {
	const (
		userID       = "user-1"
		oldShardSize = 4
		newShardSize = 8
	)

	env := newShardSizeTransitionTestEnv(t, oldShardSize, storegateway.NewShardSizeTransitions(time.Hour, "querier", nil, log.NewNopLogger()))
	oldAddrs := env.addrsInShard(userID, oldShardSize)
	newAddrs := env.addrsInShard(userID, newShardSize)

	for _, addr := range env.getClients(t, userID, nil) {
		require.Contains(t, oldAddrs, addr)
	}
	require.False(t, env.inTransition(t, userID))

	env.limits.storeGatewayTenantShardSize = newShardSize

	// GetClientsFor only uses the new tenant shard, and GetPreviousClientsFor only the previous one, which doesn't
	// pick again the instance picked by the new one if it's in both shards.
	exclude := map[ulid.ULID][]string{}
	for id, addr := range env.getClients(t, userID, exclude) {
		require.Contains(t, newAddrs, addr)
		exclude[id] = append(exclude[id], addr)
	}
	require.True(t, env.inTransition(t, userID))

	for id, addr := range env.getPreviousClients(t, userID, exclude) {
		require.Contains(t, oldAddrs, addr)
		require.NotContains(t, exclude[id], addr)
	}
}

func TestBlocksStoreReplicationSet_ShardSizeTransitionShouldEnd(t *testing.T) {
	const userID = "user-1"

	env := newShardSizeTransitionTestEnv(t, 3, storegateway.NewShardSizeTransitions(100*time.Millisecond, "querier", nil, log.NewNopLogger()))
	newAddrs := env.addrsInShard(userID, 6)

	env.getClients(t, userID, nil)
	env.limits.storeGatewayTenantShardSize = 6
	env.getClients(t, userID, nil)
	require.True(t, env.inTransition(t, userID))

	time.Sleep(150 * time.Millisecond)

	for _, addr := range env.getClients(t, userID, nil) {
		require.Contains(t, newAddrs, addr)
	}
	require.False(t, env.inTransition(t, userID))
}

func TestBlocksStoreReplicationSet_ShardSizeTransitionDisabled(t *testing.T) {
	const userID = "user-1"

	env := newShardSizeTransitionTestEnv(t, 3, nil)
	env.getClients(t, userID, nil)
	env.limits.storeGatewayTenantShardSize = 6
	newAddrs := env.addrsInShard(userID, 6)

	for _, addr := range env.getClients(t, userID, nil) {
		require.Contains(t, newAddrs, addr)
	}
	require.False(t, env.inTransition(t, userID))
}

func BenchmarkBlocksStoreReplicationSet_GetClientsFor_ShardSizeTransitions(b *testing.B) {
	const userID = "user-1"

	tests := map[string]struct {
		enabled       bool
		inTransition  bool
		previousShard bool
	}{
		"disabled":                                  {},
		"disabled, previous shard":                  {previousShard: true},
		"enabled and settled":                       {enabled: true},
		"enabled and settled, previous shard":       {enabled: true, previousShard: true},
		"enabled and in transition":                 {enabled: true, inTransition: true},
		"enabled and in transition, previous shard": {enabled: true, inTransition: true, previousShard: true},
	}

	for name, testData := range tests {
		b.Run(name, func(b *testing.B) {
			var transitions *storegateway.ShardSizeTransitions
			if testData.enabled {
				transitions = storegateway.NewShardSizeTransitions(time.Hour, "querier", nil, log.NewNopLogger())
			}
			env := newShardSizeTransitionTestEnv(b, 6, transitions)
			env.getClients(b, userID, nil)
			if testData.inTransition {
				env.limits.storeGatewayTenantShardSize = 9
			}

			// The previous shard is queried after the new one, excluding the instances it picked.
			exclude := map[ulid.ULID][]string{}
			if testData.previousShard {
				for id, addr := range env.getClients(b, userID, nil) {
					exclude[id] = []string{addr}
				}
			}

			getClientsFor := env.set.GetClientsFor
			if testData.previousShard {
				getClientsFor = func(userID string, blocks bucketindex.Blocks, exclude map[ulid.ULID][]string) (map[BlocksStoreClient][][]ulid.ULID, error) {
					clients, _, err := env.set.GetPreviousClientsFor(userID, blocks, exclude)
					return clients, err
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				clients, err := getClientsFor(userID, env.blocks, exclude)
				if err != nil {
					b.Fatal(err)
				}
				_ = clients
			}

			// Register the pooled clients, to close them at the end.
			b.StopTimer()
			env.getClients(b, userID, exclude)
		})
	}
}

func TestBlocksStoreQuerier_ShardSizeTransitionShouldQueryThePreviousShardInEachAttempt(t *testing.T) {
	const (
		replicationFactor = 3
		minT              = int64(10)
		maxT              = int64(20)
	)

	block := ulid.MustNew(1, nil)

	// newResponses returns a response for each attempt, and only the one at index found has the block.
	newResponses := func(prefix string, found int) []interface{} {
		responses := make([]interface{}, 0, replicationFactor)
		for i := 0; i < replicationFactor; i++ {
			series := newSeriesResponseBuilder()
			if i == found {
				series = series.addValue(labels.FromStrings(model.MetricNameLabel, "some_metric"), minT, 1).addBlocks(block)
			}
			responses = append(responses, map[BlocksStoreClient][][]ulid.ULID{
				&storeGatewayClientMock{remoteAddr: fmt.Sprintf("%s.%d", prefix, i), mockedSeriesResponses: series.build()}: {{block}},
			})
		}
		return responses
	}

	tests := map[string]struct {
		inTransition             bool
		foundInNew               int
		foundInPrevious          int
		expectedNewQueries       int
		expectedPreviousQueries  int
		expectedConsistencyError bool
	}{
		"outside of a transition, only the new shard is queried": {
			foundInNew: -1, foundInPrevious: 0,
			expectedNewQueries:       replicationFactor,
			expectedConsistencyError: true,
		},
		"during a transition, each attempt queries the new shard and then the previous one": {
			inTransition: true,
			foundInNew:   -1, foundInPrevious: replicationFactor - 1,
			expectedNewQueries:      replicationFactor,
			expectedPreviousQueries: replicationFactor,
		},
		"during a transition, the previous shard isn't queried if the new one has all the blocks": {
			inTransition: true,
			foundInNew:   0, foundInPrevious: -1,
			expectedNewQueries: 1,
		},
		"during a transition, the query stops at the first shard with all the blocks": {
			inTransition: true,
			foundInNew:   -1, foundInPrevious: 0,
			expectedNewQueries:      1,
			expectedPreviousQueries: 1,
		},
	}

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			stores := &blocksStoreSetMock{
				mockedResponses:         newResponses("1.1.1", testData.foundInNew),
				mockedPreviousResponses: newResponses("2.2.2", testData.foundInPrevious),
				inTransition:            testData.inTransition,
			}
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
			if testData.expectedConsistencyError {
				require.ErrorContains(t, seriesSet.Err(), "err-mimir-store-consistency-check-failed")
			} else {
				require.NoError(t, seriesSet.Err())
			}
			require.Equal(t, testData.expectedNewQueries, stores.nextResult)
			require.Equal(t, testData.expectedPreviousQueries, stores.nextPreviousResult)
		})
	}
}

func noShardSizeTransitions() *storegateway.ShardSizeTransitions {
	return nil
}

// newShardSizeTransitionsForTest returns an enabled ShardSizeTransitions. A tracker observing a tenant shard size
// that never changes is settled, so tests which don't change the shard size expect the same results as when disabled.
func newShardSizeTransitionsForTest() *storegateway.ShardSizeTransitions {
	return storegateway.NewShardSizeTransitions(time.Hour, "querier", nil, log.NewNopLogger())
}
