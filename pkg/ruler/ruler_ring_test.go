// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util"
)

func TestRingConfig_ZoneAwareness(t *testing.T) {
	cfg := RingConfig{
		Common:               util.CommonRingConfig{InstanceID: "ruler-1", InstanceAddr: "1.2.3.4", InstancePort: 9095},
		ZoneAwarenessEnabled: true,
		InstanceZone:         "zone-a",
		ExcludedZones:        flagext.StringSliceCSV{"zone-c"},
	}

	lcCfg, err := cfg.ToLifecyclerConfig(log.NewNopLogger())
	require.NoError(t, err)
	assert.Equal(t, "zone-a", lcCfg.Zone)

	rc := cfg.toRingConfig()
	assert.True(t, rc.ZoneAwarenessEnabled)
	assert.Equal(t, cfg.ExcludedZones, rc.ExcludedZones)
	assert.Equal(t, 1, rc.ReplicationFactor)
}

func TestTenantShard(t *testing.T) {
	const userID = "user-1"

	tests := map[string]struct {
		rulersPerZone map[string]int
		shardSize     int
		expected      map[string]int
	}{
		"even zones": {
			rulersPerZone: map[string]int{"zone-a": 8, "zone-b": 8},
			shardSize:     12,
			expected:      map[string]int{"zone-a": 6, "zone-b": 6},
		},
		"odd shard size rounds up per zone": {
			rulersPerZone: map[string]int{"zone-a": 8, "zone-b": 8},
			shardSize:     5,
			expected:      map[string]int{"zone-a": 3, "zone-b": 3},
		},
		"short zone is filled from the other zone": {
			rulersPerZone: map[string]int{"zone-a": 10, "zone-b": 3},
			shardSize:     12,
			expected:      map[string]int{"zone-a": 9, "zone-b": 3},
		},
		"short zone with fewer rulers than the shard size": {
			rulersPerZone: map[string]int{"zone-a": 8, "zone-b": 3},
			shardSize:     12,
			expected:      map[string]int{"zone-a": 8, "zone-b": 3},
		},
		"single zone left": {
			rulersPerZone: map[string]int{"zone-a": 8},
			shardSize:     12,
			expected:      map[string]int{"zone-a": 8},
		},
		"three zones with one short zone": {
			rulersPerZone: map[string]int{"zone-a": 8, "zone-b": 8, "zone-c": 2},
			shardSize:     12,
			expected:      map[string]int{"zone-a": 5, "zone-b": 5, "zone-c": 2},
		},
		"rulers without a zone count as a zone": {
			rulersPerZone: map[string]int{"": 2, "zone-a": 8, "zone-b": 8},
			shardSize:     12,
			expected:      map[string]int{"": 2, "zone-a": 5, "zone-b": 5},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			r := newRulerRing(t, zonedRulersDesc(tc.rulersPerZone), true)

			shard := tenantShard(r, userID, tc.shardSize, true)
			assert.Equal(t, tc.expected, countRulersPerZone(t, shard))

			// Filling a short zone must not remove a ruler that the dskit shard picks.
			assert.Subset(t, rulerIDs(t, shard), rulerIDs(t, r.ShuffleShard(userID, tc.shardSize)))
		})
	}

	t.Run("no fill without zone awareness", func(t *testing.T) {
		r := newRulerRing(t, zonedRulersDesc(map[string]int{"zone-a": 10, "zone-b": 3}), false)

		assert.Len(t, rulerIDs(t, tenantShard(r, userID, 5, false)), 5)
	})

	t.Run("a ruler leaves the ring during the call", func(t *testing.T) {
		r := shrinkingRing{newRulerRing(t, zonedRulersDesc(map[string]int{"zone-a": 2, "zone-b": 2}), true)}

		done := make(chan ring.ReadRing, 1)
		go func() { done <- tenantShard(r, userID, 6, true) }()

		select {
		case shard := <-done:
			assert.Equal(t, map[string]int{"zone-a": 2, "zone-b": 2}, countRulersPerZone(t, shard))
		case <-time.After(5 * time.Second):
			require.FailNow(t, "tenantShard did not return")
		}
	})
}

// shrinkingRing reports one more instance than its zones hold, as if a ruler left the ring between two reads.
type shrinkingRing struct {
	ring.ReadRing
}

func (r shrinkingRing) InstancesCount() int {
	return r.ReadRing.InstancesCount() + 1
}

func TestRulerRing_ZoneAwarenessKeepsGetRulesComplete(t *testing.T) {
	desc := zonedRulersDesc(map[string]int{"zone-a": 2, "zone-b": 2})
	stale := desc.Ingesters["ruler-zone-b-0"]
	stale.Timestamp = time.Now().Add(-time.Hour).Unix()
	desc.Ingesters["ruler-zone-b-0"] = stale

	r := newRulerRing(t, desc, true)

	// GetRules needs every ruler in the shard.
	// With replication factor 1, zone awareness must not drop the zone of a stale ruler and return partial rules.
	_, err := r.GetSubringForOperationStates(RuleEvalRingOp).GetReplicationSetForOperation(RuleEvalRingOp)
	require.ErrorIs(t, err, ring.ErrTooManyUnhealthyInstances)
}

func zonedRulersDesc(rulersPerZone map[string]int) *ring.Desc {
	d := ring.NewDesc()
	for zone, n := range rulersPerZone {
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("ruler-%s-%d", zone, i)
			d.AddIngester(id, id, zone, generateSortedTokens(128), ring.ACTIVE, time.Now(), false, time.Time{}, nil)
		}
	}
	return d
}

func newRulerRing(t *testing.T, desc *ring.Desc, zoneAware bool) *ring.Ring {
	kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	require.NoError(t, kvStore.CAS(t.Context(), RulerRingKey, func(interface{}) (interface{}, bool, error) {
		return desc, true, nil
	}))

	cfg := ring.Config{}
	flagext.DefaultValues(&cfg)
	cfg.ReplicationFactor = 1
	cfg.ZoneAwarenessEnabled = zoneAware
	cfg.SubringCacheDisabled = true

	r, err := ring.NewWithStoreClientAndStrategy(cfg, "ruler", RulerRingKey, kvStore, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(t.Context(), r))
	t.Cleanup(func() { assert.NoError(t, services.StopAndAwaitTerminated(context.Background(), r)) })

	test.Poll(t, time.Second, len(desc.Ingesters), func() interface{} { return r.InstancesCount() })

	return r
}

func countRulersPerZone(t *testing.T, rr ring.ReadRing) map[string]int {
	set, err := rr.GetAllHealthy(RuleEvalRingOp)
	require.NoError(t, err)

	counts := map[string]int{}
	for _, inst := range set.Instances {
		counts[inst.Zone]++
	}
	return counts
}

func rulerIDs(t *testing.T, rr ring.ReadRing) []string {
	set, err := rr.GetAllHealthy(RuleEvalRingOp)
	require.NoError(t, err)

	return set.GetIDs()
}
