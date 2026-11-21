// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/extprom"
)

type fakeClock struct {
	mtx sync.Mutex
	t   time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Unix(1_700_000_000, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.t = c.t.Add(d)
}

func newTestShardSizeTransitions(period time.Duration) (*ShardSizeTransitions, *fakeClock) {
	clock := newFakeClock()
	tr := NewShardSizeTransitions(period, "store-gateway", nil)
	tr.now = clock.Now
	return tr, clock
}

func TestShardSizeTransitions_Observe(t *testing.T) {
	const period = time.Hour

	type step struct {
		advance          time.Duration
		shardSize        int
		expectTransition bool
		expectPrevious   int
	}

	tests := map[string][]step{
		"first observation is never a transition": {
			{shardSize: 3},
			{advance: time.Minute, shardSize: 3},
		},
		"increase is a transition until the period elapses": {
			{shardSize: 3},
			{shardSize: 6, expectTransition: true, expectPrevious: 3},
			{advance: period - time.Nanosecond, shardSize: 6, expectTransition: true, expectPrevious: 3},
			{advance: time.Nanosecond, shardSize: 6},
			{advance: period, shardSize: 6},
		},
		"decrease is a transition": {
			{shardSize: 6},
			{shardSize: 3, expectTransition: true, expectPrevious: 6},
		},
		"disabling shuffle sharding is a transition from the previous size": {
			{shardSize: 3},
			{shardSize: 0, expectTransition: true, expectPrevious: 3},
		},
		"enabling shuffle sharding is a transition from the full ring": {
			{shardSize: 0},
			{shardSize: 3, expectTransition: true, expectPrevious: 0},
		},
		"another change restarts the period and keeps the last settled size": {
			{shardSize: 3},
			{shardSize: 6, expectTransition: true, expectPrevious: 3},
			{advance: 40 * time.Minute, shardSize: 9, expectTransition: true, expectPrevious: 3},
			{advance: 40 * time.Minute, shardSize: 9, expectTransition: true, expectPrevious: 3},
			{advance: 20 * time.Minute, shardSize: 9},
		},
		"changing back to the last settled size ends the transition": {
			{shardSize: 3},
			{shardSize: 6, expectTransition: true, expectPrevious: 3},
			{advance: time.Minute, shardSize: 3},
			{advance: time.Minute, shardSize: 3},
		},
		"a change after settling uses the settled size as previous": {
			{shardSize: 3},
			{shardSize: 6, expectTransition: true, expectPrevious: 3},
			{advance: period, shardSize: 6},
			{shardSize: 9, expectTransition: true, expectPrevious: 6},
		},
		"a change observed after the period elapsed without observations is still a transition": {
			{shardSize: 3},
			{advance: 10 * period, shardSize: 6, expectTransition: true, expectPrevious: 3},
		},
	}

	for name, steps := range tests {
		t.Run(name, func(t *testing.T) {
			tr, clock := newTestShardSizeTransitions(period)

			for i, s := range steps {
				clock.Advance(s.advance)
				previous, inTransition := tr.Observe("user-1", TenantShardSize{Total: s.shardSize})
				require.Equal(t, s.expectTransition, inTransition, "step %d", i)
				require.Equal(t, TenantShardSize{Total: s.expectPrevious}, previous, "step %d", i)
			}
		})
	}
}

func TestShardSizeTransitions_ShouldTrackTenantsIndependently(t *testing.T) {
	tr, _ := newTestShardSizeTransitions(time.Hour)

	tr.Observe("user-1", TenantShardSize{Total: 3})
	tr.Observe("user-2", TenantShardSize{Total: 3})

	_, inTransition := tr.Observe("user-1", TenantShardSize{Total: 6})
	assert.True(t, inTransition)

	_, inTransition = tr.Observe("user-2", TenantShardSize{Total: 3})
	assert.False(t, inTransition)
}

func TestShardSizeTransitions_NilShouldNeverTransition(t *testing.T) {
	var tr *ShardSizeTransitions

	previous, inTransition := tr.Observe("user-1", TenantShardSize{Total: 3})
	assert.False(t, inTransition)
	assert.Zero(t, previous)

	previous, inTransition = tr.Observe("user-1", TenantShardSize{Total: 6})
	assert.False(t, inTransition)
	assert.Zero(t, previous)
}

func TestShardSizeTransitions_ConcurrentObserve(t *testing.T) {
	tr := NewShardSizeTransitions(time.Hour, "store-gateway", nil)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				tr.Observe(fmt.Sprintf("user-%d", i%10), TenantShardSize{Total: 3 + i/100})
			}
		}()
	}
	wg.Wait()
}

func BenchmarkShardSizeTransitions_Observe(b *testing.B) {
	for name, tr := range map[string]*ShardSizeTransitions{
		"disabled": nil,
		"enabled":  NewShardSizeTransitions(time.Hour, "store-gateway", nil),
	} {
		b.Run(name, func(b *testing.B) {
			tr.Observe("user-1", TenantShardSize{Total: 3})
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					tr.Observe("user-1", TenantShardSize{Total: 3})
				}
			})
		})
	}
}

func TestShuffleShardingStrategy_ShardSizeTransition(t *testing.T) {
	const (
		numInstances = 12
		numBlocks    = 200
		period       = time.Hour
		userID       = "user-1"
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
			d.AddIngester(id, id, "", generator.GenerateTokens(ringNumTokensDefault, nil), ring.ACTIVE, time.Now(), false, time.Time{}, nil)
		}
		return d, true, nil
	}))

	r, err := ring.NewWithStoreClientAndStrategy(ring.Config{ReplicationFactor: 1, HeartbeatTimeout: time.Minute}, "test", "test", store, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(ctx, r) })
	require.NoError(t, ring.WaitInstanceState(ctx, r, "instance-0", ring.ACTIVE))

	blockIDs := make([]ulid.ULID, numBlocks)
	for i := range blockIDs {
		blockIDs[i] = ulid.MustNew(uint64(i+1), random)
	}
	newMetas := func() map[ulid.ULID]*block.Meta {
		metas := make(map[ulid.ULID]*block.Meta, numBlocks)
		for _, id := range blockIDs {
			metas[id] = &block.Meta{BlockMeta: tsdb.BlockMeta{ULID: id}}
		}
		return metas
	}

	// The shard size shrinks, so there's at least one store-gateway leaving the tenant shard.
	const oldShardSize, newShardSize = 6, 3
	oldSubring := r.ShuffleShard(userID, oldShardSize)
	newSubring := r.ShuffleShard(userID, newShardSize)
	var leaving string
	for i := 0; i < numInstances; i++ {
		id := fmt.Sprintf("instance-%d", i)
		if oldSubring.HasInstance(id) && !newSubring.HasInstance(id) {
			leaving = id
			break
		}
	}
	require.NotEmpty(t, leaving)

	limits := &shardingLimitsMock{storeGatewayTenantShardSize: oldShardSize}
	transitions, clock := newTestShardSizeTransitions(period)
	strategy := NewShuffleShardingStrategy(r, leaving, leaving, NewNopDynamicReplication(1), limits, transitions, log.NewNopLogger())
	baseline := NewShuffleShardingStrategy(r, leaving, leaving, NewNopDynamicReplication(1), limits, nil, log.NewNopLogger())

	filterBlocks := func(s *ShuffleShardingStrategy, loaded map[ulid.ULID]struct{}) map[ulid.ULID]struct{} {
		metas := newMetas()
		require.NoError(t, s.FilterBlocks(ctx, userID, metas, loaded, extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{}, []string{"state"})))
		kept := make(map[ulid.ULID]struct{}, len(metas))
		for id := range metas {
			kept[id] = struct{}{}
		}
		return kept
	}

	users, err := strategy.FilterUsers(ctx, []string{userID})
	require.NoError(t, err)
	require.Equal(t, []string{userID}, users)
	loaded := filterBlocks(strategy, nil)
	require.NotEmpty(t, loaded)

	limits.storeGatewayTenantShardSize = newShardSize

	// Without transitions, the store-gateway would unload every block.
	require.Empty(t, filterBlocks(baseline, loaded))
	users, err = baseline.FilterUsers(ctx, []string{userID})
	require.NoError(t, err)
	require.Empty(t, users)

	t.Run("loaded blocks and the tenant are kept during the transition", func(t *testing.T) {
		users, err := strategy.FilterUsers(ctx, []string{userID})
		require.NoError(t, err)
		require.Equal(t, []string{userID}, users)

		require.Equal(t, loaded, filterBlocks(strategy, loaded))

		clock.Advance(period - time.Second)
		require.Equal(t, loaded, filterBlocks(strategy, loaded))
	})

	t.Run("blocks that were not loaded are not loaded because of the previous shard", func(t *testing.T) {
		require.Empty(t, filterBlocks(strategy, nil))
	})

	t.Run("blocks are unloaded once the transition ends", func(t *testing.T) {
		clock.Advance(time.Second)

		require.Empty(t, filterBlocks(strategy, loaded))
		users, err := strategy.FilterUsers(ctx, []string{userID})
		require.NoError(t, err)
		require.Empty(t, users)
	})
}

func newBenchmarkRing(b *testing.B, numInstances int) *ring.Ring {
	b.Helper()
	ctx := context.Background()

	store, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	b.Cleanup(func() { assert.NoError(b, closer.Close()) })

	generator := ring.NewRandomTokenGeneratorWithSeed(1)
	require.NoError(b, store.CAS(ctx, "test", func(interface{}) (interface{}, bool, error) {
		d := ring.NewDesc()
		for i := 0; i < numInstances; i++ {
			id := fmt.Sprintf("instance-%d", i)
			d.AddIngester(id, id, fmt.Sprintf("zone-%d", i%3), generator.GenerateTokens(ringNumTokensDefault, nil), ring.ACTIVE, time.Now(), false, time.Time{}, nil)
		}
		return d, true, nil
	}))

	r, err := ring.NewWithStoreClientAndStrategy(ring.Config{ReplicationFactor: 3, HeartbeatTimeout: time.Minute, ZoneAwarenessEnabled: true}, "test", "test", store, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, log.NewNopLogger())
	require.NoError(b, err)
	require.NoError(b, services.StartAndAwaitRunning(ctx, r))
	b.Cleanup(func() { _ = services.StopAndAwaitTerminated(ctx, r) })
	require.NoError(b, ring.WaitInstanceState(ctx, r, "instance-0", ring.ACTIVE))
	return r
}

func BenchmarkGetShuffleShardingSubrings(b *testing.B) {
	r := newBenchmarkRing(b, 300)
	limits := &shardingLimitsMock{storeGatewayTenantShardSize: 60}

	tests := map[string]struct {
		transitions  *ShardSizeTransitions
		inTransition bool
	}{
		"disabled":                  {},
		"enabled and settled":       {transitions: NewShardSizeTransitions(time.Hour, "store-gateway", nil)},
		"enabled and in transition": {transitions: NewShardSizeTransitions(time.Hour, "store-gateway", nil), inTransition: true},
	}

	for name, testData := range tests {
		b.Run(name, func(b *testing.B) {
			GetShuffleShardingSubrings(r, "user-1", limits, testData.transitions)
			limits.storeGatewayTenantShardSize = 60
			if testData.inTransition {
				limits.storeGatewayTenantShardSize = 90
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				GetShuffleShardingSubrings(r, "user-1", limits, testData.transitions)
			}
		})
	}
}

func BenchmarkShuffleShardingStrategy_FilterBlocks_ShardSizeTransition(b *testing.B) {
	const (
		numBlocks = 10000
		userID    = "user-1"
	)
	ctx := context.Background()
	r := newBenchmarkRing(b, 300)

	// Every block of the old shard is loaded, and the shard shrinks, so most blocks of a leaving instance are
	// "loaded but not owned" and take the extra ring lookup during a transition.
	blockIDs := make([]ulid.ULID, numBlocks)
	for i := range blockIDs {
		blockIDs[i] = ulid.MustNew(uint64(i+1), rand.New(rand.NewSource(int64(i))))
	}
	limits := &shardingLimitsMock{storeGatewayTenantShardSize: 90}
	oldSub, newSub := r.ShuffleShard(userID, 90), r.ShuffleShard(userID, 60)
	var leaving string
	for i := 0; i < 300; i++ {
		id := fmt.Sprintf("instance-%d", i)
		if oldSub.HasInstance(id) && !newSub.HasInstance(id) {
			leaving = id
			break
		}
	}
	require.NotEmpty(b, leaving)

	metas := func() map[ulid.ULID]*block.Meta {
		m := make(map[ulid.ULID]*block.Meta, numBlocks)
		for _, id := range blockIDs {
			m[id] = &block.Meta{BlockMeta: tsdb.BlockMeta{ULID: id}}
		}
		return m
	}

	limits.storeGatewayTenantShardSize = 90
	loaded := map[ulid.ULID]struct{}{}
	initial := metas()
	baseline := NewShuffleShardingStrategy(r, leaving, leaving, NewNopDynamicReplication(3), limits, nil, log.NewNopLogger())
	require.NoError(b, baseline.FilterBlocks(ctx, userID, initial, nil, extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{}, []string{"state"})))
	for id := range initial {
		loaded[id] = struct{}{}
	}

	tests := map[string]struct {
		transitions *ShardSizeTransitions
		observed    int
	}{
		"disabled":      {},
		"settled":       {transitions: NewShardSizeTransitions(time.Hour, "store-gateway", nil), observed: 60},
		"in transition": {transitions: NewShardSizeTransitions(time.Hour, "store-gateway", nil), observed: 90},
	}

	for name, testData := range tests {
		b.Run(name, func(b *testing.B) {
			testData.transitions.Observe(userID, TenantShardSize{Total: testData.observed})
			limits.storeGatewayTenantShardSize = 60
			strategy := NewShuffleShardingStrategy(r, leaving, leaving, NewNopDynamicReplication(3), limits, testData.transitions, log.NewNopLogger())

			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				m := metas()
				b.StartTimer()
				require.NoError(b, strategy.FilterBlocks(ctx, userID, m, loaded, extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{}, []string{"state"})))
			}
		})
	}
}

func TestGetShuffleShardingSubrings_ShouldNotTreatZonesCountChangeAsShardSizeChange(t *testing.T) {
	const (
		userID           = "user-1"
		instancesPerZone = 8
		shardSizePerZone = 5
	)
	ctx := context.Background()

	store, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	generator := ring.NewRandomTokenGeneratorWithSeed(1)
	setZones := func(zones ...string) {
		require.NoError(t, store.CAS(ctx, "test", func(interface{}) (interface{}, bool, error) {
			d := ring.NewDesc()
			for _, zone := range zones {
				for i := 0; i < instancesPerZone; i++ {
					id := fmt.Sprintf("instance-%s-%d", zone, i)
					d.AddIngester(id, id, zone, generator.GenerateTokens(ringNumTokensDefault, nil), ring.ACTIVE, time.Now(), false, time.Time{}, nil)
				}
			}
			return d, true, nil
		}))
	}
	setZones("a", "b", "c")

	r, err := ring.NewWithStoreClientAndStrategy(ring.Config{ReplicationFactor: 3, HeartbeatTimeout: time.Minute, ZoneAwarenessEnabled: true}, "test", "test", store, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(ctx, r) })
	require.Eventually(t, func() bool { return r.ZonesCount() == 3 }, 5*time.Second, 10*time.Millisecond)

	limits := &shardingLimitsMock{storeGatewayTenantShardSizePerZone: shardSizePerZone}
	transitions := NewShardSizeTransitions(time.Hour, "store-gateway", nil)

	_, previous := GetShuffleShardingSubrings(r, userID, limits, transitions)
	require.Nil(t, previous)

	for name, zones := range map[string][]string{"zone added": {"a", "b", "c", "d"}, "zone removed": {"a", "b"}} {
		t.Run(name, func(t *testing.T) {
			setZones(zones...)
			require.Eventually(t, func() bool { return r.ZonesCount() == len(zones) }, 5*time.Second, 10*time.Millisecond)

			_, previous := GetShuffleShardingSubrings(r, userID, limits, transitions)
			require.Nil(t, previous, "the tenant's shard size per zone didn't change")
		})
	}
}

func TestShardSizeTransitions_Observe_PerZoneShardSize(t *testing.T) {
	tr, _ := newTestShardSizeTransitions(time.Hour)

	_, inTransition := tr.Observe("user-1", TenantShardSize{PerZone: 3})
	require.False(t, inTransition)

	// A per-zone shard size takes precedence, so the total is irrelevant and normalized away.
	_, inTransition = tr.Observe("user-1", TenantShardSize{PerZone: 3, Total: 9})
	require.False(t, inTransition)

	previous, inTransition := tr.Observe("user-1", TenantShardSize{PerZone: 4})
	require.True(t, inTransition)
	require.Equal(t, TenantShardSize{PerZone: 3}, previous)

	// Switching between a per-zone and a total shard size is a change, even if they resolve to the same number.
	previous, inTransition = tr.Observe("user-1", TenantShardSize{Total: 12})
	require.True(t, inTransition)
	require.Equal(t, TenantShardSize{PerZone: 3}, previous)

	// A negative total means shuffle sharding is disabled, like 0.
	tr, _ = newTestShardSizeTransitions(time.Hour)
	tr.Observe("user-2", TenantShardSize{Total: 0})
	_, inTransition = tr.Observe("user-2", TenantShardSize{Total: -1})
	require.False(t, inTransition)
}

func noShardSizeTransitions() *ShardSizeTransitions {
	return nil
}

// newShardSizeTransitionsForTest returns an enabled ShardSizeTransitions. A tracker observing a tenant shard size
// that never changes is settled, so tests which don't change the shard size expect the same results as when disabled.
func newShardSizeTransitionsForTest() *ShardSizeTransitions {
	return NewShardSizeTransitions(time.Hour, "store-gateway", nil)
}

func TestShardSizeTransitions_Metrics(t *testing.T) {
	const period = time.Hour

	reg := prometheus.NewPedanticRegistry()
	clock := newFakeClock()
	tr := NewShardSizeTransitions(period, "store-gateway", reg)
	tr.now = clock.Now

	assertMetrics := func(t *testing.T, started, ended int) {
		t.Helper()
		expected := fmt.Sprintf(`
			# HELP cortex_shard_size_transitions_total %s
			# TYPE cortex_shard_size_transitions_total counter
			cortex_shard_size_transitions_total{component="store-gateway",event="ended"} %d
			cortex_shard_size_transitions_total{component="store-gateway",event="started"} %d
		`, metricHelp(t, reg), ended, started)
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected), "cortex_shard_size_transitions_total"))
	}

	steps := []struct {
		name      string
		advance   time.Duration
		shardSize int
		started   int
		ended     int
	}{
		{name: "metrics are exported before any observation", shardSize: -1},
		{name: "first observation isn't a transition", shardSize: 3},
		{name: "a change starts a transition", shardSize: 6, started: 1},
		{name: "a change during the transition doesn't start another one", advance: 10 * time.Minute, shardSize: 9, started: 1},
		{name: "observing within the period doesn't end it", advance: 30 * time.Minute, shardSize: 9, started: 1},
		{name: "observing after the period ends it", advance: period, shardSize: 9, started: 1, ended: 1},
		{name: "a change after settling starts a new one", shardSize: 12, started: 2, ended: 1},
		{name: "changing back to the settled size ends it", advance: time.Minute, shardSize: 9, started: 2, ended: 2},
		{name: "a settled tenant doesn't change the metrics", advance: 10 * period, shardSize: 9, started: 2, ended: 2},
		{name: "another change starts a transition", shardSize: 6, started: 3, ended: 2},
		{name: "the end is recorded at the first observation after the period", advance: 5 * period, shardSize: 6, started: 3, ended: 3},
		{name: "a change after settling starts another one", shardSize: 9, started: 4, ended: 3},
		{name: "a change observed after the period records the end of the previous transition and a new start", advance: 5 * period, shardSize: 12, started: 5, ended: 4},
	}
	for _, step := range steps {
		clock.Advance(step.advance)
		if step.shardSize >= 0 {
			tr.Observe("user-1", TenantShardSize{Total: step.shardSize})
		}
		assertMetrics(t, step.started, step.ended)
	}
}

func TestShardSizeTransitions_MetricsOfQuerierAndStoreGatewayShouldRegisterInTheSameRegistry(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()

	querier := NewShardSizeTransitions(time.Hour, "querier", reg)
	storeGateway := NewShardSizeTransitions(time.Hour, "store-gateway", reg)

	querier.Observe("user-1", TenantShardSize{Total: 3})
	querier.Observe("user-1", TenantShardSize{Total: 6})
	storeGateway.Observe("user-1", TenantShardSize{Total: 3})

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_shard_size_transitions_total `+metricHelp(t, reg)+`
		# TYPE cortex_shard_size_transitions_total counter
		cortex_shard_size_transitions_total{component="querier",event="ended"} 0
		cortex_shard_size_transitions_total{component="querier",event="started"} 1
		cortex_shard_size_transitions_total{component="store-gateway",event="ended"} 0
		cortex_shard_size_transitions_total{component="store-gateway",event="started"} 0
	`), "cortex_shard_size_transitions_total"))
}

func metricHelp(t *testing.T, reg prometheus.Gatherer) string {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == "cortex_shard_size_transitions_total" {
			return family.GetHelp()
		}
	}
	require.FailNow(t, "metric not found")
	return ""
}
