// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/validation"
)

// rulerChanges are the ways that rulers stop owning rule groups.
var rulerChanges = map[string]func(d *ring.Desc, idPrefix string){
	"leaves the ring": removeRulers,
	"is LEAVING": func(d *ring.Desc, idPrefix string) {
		updateRulers(d, idPrefix, func(i *ring.InstanceDesc) { i.State = ring.LEAVING })
	},
	"stops heartbeating": func(d *ring.Desc, idPrefix string) {
		updateRulers(d, idPrefix, func(i *ring.InstanceDesc) { i.Timestamp = time.Now().Add(-time.Hour).Unix() })
	},
}

func TestHomeZoneOwner(t *testing.T) {
	groups := testRuleGroups("user-1", 2000)

	t.Run("each zone owns an equal share of the groups", func(t *testing.T) {
		desc := zonedRulersDesc(map[string]int{"zone-a": 6, "zone-b": 2, "zone-c": 1})

		perZone := ownersPerZone(homeZoneOwners(t, newRulerRing(t, desc, true), groups, RuleEvalRingOp))
		for _, zone := range []string{"zone-a", "zone-b", "zone-c"} {
			assert.InDelta(t, len(groups)/3, perZone[zone], float64(len(groups))/20, zone)
		}
	})

	t.Run("rulers without a zone form a zone", func(t *testing.T) {
		desc := zonedRulersDesc(map[string]int{"": 1, "zone-a": 3})

		perZone := ownersPerZone(homeZoneOwners(t, newRulerRing(t, desc, true), groups, RuleEvalRingOp))
		assert.InDelta(t, len(groups)/2, perZone[""], float64(len(groups))/20)
	})

	for name, change := range rulerChanges {
		t.Run("a ruler that "+name+" gives its groups to rulers in its zone", func(t *testing.T) {
			const changed = "ruler-zone-a-0"
			desc := zonedRulersDesc(map[string]int{"zone-a": 3, "zone-b": 3})
			before := homeZoneOwners(t, newRulerRing(t, desc, true), groups, RuleEvalRingOp)

			changedDesc := cloneDesc(desc)
			change(changedDesc, changed)
			after := homeZoneOwners(t, newRulerRing(t, changedDesc, true), groups, RuleEvalRingOp)

			moved := 0
			for i, g := range groups {
				if before[i].Addr != changed {
					require.Equal(t, before[i].Addr, after[i].Addr, g.Name)
					continue
				}
				moved++
				require.NotEqual(t, changed, after[i].Addr, g.Name)
				require.Equal(t, "zone-a", after[i].Zone, g.Name)
			}
			assert.NotZero(t, moved)
		})
	}

	t.Run("a new ruler takes groups only from rulers in its zone", func(t *testing.T) {
		const added = "ruler-zone-a-new"
		desc := zonedRulersDesc(map[string]int{"zone-a": 3, "zone-b": 3})
		before := homeZoneOwners(t, newRulerRing(t, desc, true), groups, RuleEvalRingOp)

		grownDesc := cloneDesc(desc)
		grownDesc.AddIngester(added, added, "zone-a", generateSortedTokens(128), ring.ACTIVE, time.Now(), false, time.Time{}, nil)
		after := homeZoneOwners(t, newRulerRing(t, grownDesc, true), groups, RuleEvalRingOp)

		moved := 0
		for i, g := range groups {
			if after[i].Addr == before[i].Addr {
				continue
			}
			moved++
			require.Equal(t, added, after[i].Addr, g.Name)
			require.Equal(t, "zone-a", before[i].Zone, g.Name)
		}
		assert.NotZero(t, moved)
	})

	for name, change := range rulerChanges {
		t.Run("a zone that "+name+" gives its groups to the other zones", func(t *testing.T) {
			desc := zonedRulersDesc(map[string]int{"zone-a": 2, "zone-b": 2, "zone-c": 2})
			before := homeZoneOwners(t, newRulerRing(t, desc, true), groups, RuleEvalRingOp)

			changedDesc := cloneDesc(desc)
			change(changedDesc, "ruler-zone-b-")
			after := homeZoneOwners(t, newRulerRing(t, changedDesc, true), groups, RuleEvalRingOp)

			movedTo := map[string]int{}
			for i, g := range groups {
				if before[i].Zone != "zone-b" {
					require.Equal(t, before[i].Addr, after[i].Addr, g.Name)
					continue
				}
				movedTo[after[i].Zone]++
			}
			assert.Zero(t, movedTo["zone-b"])
			assert.NotZero(t, movedTo["zone-a"])
			assert.NotZero(t, movedTo["zone-c"])
		})
	}

	t.Run("a JOINING ruler owns groups at the initial sync only", func(t *testing.T) {
		const joining = "ruler-zone-a-0"
		desc := zonedRulersDesc(map[string]int{"zone-a": 2, "zone-b": 2})
		updateRulers(desc, joining, func(i *ring.InstanceDesc) { i.State = ring.JOINING })
		r := newRulerRing(t, desc, true)

		syncOwners := homeZoneOwners(t, r, groups, RuleSyncRingOp)
		evalOwners := homeZoneOwners(t, r, groups, RuleEvalRingOp)

		owned := 0
		for i, g := range groups {
			require.NotEqual(t, joining, evalOwners[i].Addr, g.Name)
			if syncOwners[i].Addr == joining {
				owned++
				require.Equal(t, "zone-a", evalOwners[i].Zone, g.Name)
			}
		}
		assert.NotZero(t, owned)
	})

	t.Run("no healthy ruler", func(t *testing.T) {
		desc := zonedRulersDesc(map[string]int{"zone-a": 2, "zone-b": 2})
		rulerChanges["stops heartbeating"](desc, "ruler-")

		_, err := homeZoneOwner(newRulerRing(t, desc, true), tokenForGroup(groups[0]), RuleEvalRingOp)
		require.ErrorContains(t, err, "at least 1 healthy replica required")
	})

	t.Run("empty ring", func(t *testing.T) {
		_, err := homeZoneOwner(newRulerRing(t, ring.NewDesc(), true), tokenForGroup(groups[0]), RuleEvalRingOp)
		require.ErrorIs(t, err, ring.ErrEmptyRing)
	})
}

func TestListRuleGroupsToSync_HomeZoneOwnership(t *testing.T) {
	const userID = "user-1"
	groups := testRuleGroups(userID, 400)

	// The test ruler is the only ruler in zone-b.
	// With home zones, it owns about half of the groups.
	// With tokens, it owns about 1/8 of the groups.
	desc := zonedRulersDesc(map[string]int{"zone-a": 7})
	desc.AddIngester("ruler-1", "1.1.1.1:9999", "zone-b", generateSortedTokens(128), ring.ACTIVE, time.Now(), false, time.Time{}, nil)

	for name, tc := range map[string]struct {
		zoneAware          bool
		homeZone           bool
		minShare, maxShare float64
	}{
		"home zone ownership":     {zoneAware: true, homeZone: true, minShare: 0.4, maxShare: 0.6},
		"limit disabled":          {zoneAware: true, homeZone: false, minShare: 0, maxShare: 0.3},
		"zone awareness disabled": {zoneAware: false, homeZone: true, minShare: 0, maxShare: 0.3},
	} {
		t.Run(name, func(t *testing.T) {
			kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })
			require.NoError(t, kvStore.CAS(t.Context(), RulerRingKey, func(interface{}) (interface{}, bool, error) {
				return desc, true, nil
			}))

			cfg := Config{
				Ring: RingConfig{
					Common: util.CommonRingConfig{
						InstanceID:       "ruler-1",
						InstanceAddr:     "1.1.1.1",
						InstancePort:     9999,
						KVStore:          kv.Config{Mock: kvStore},
						HeartbeatTimeout: time.Minute,
					},
					ZoneAwarenessEnabled: tc.zoneAware,
					InstanceZone:         "zone-b",
				},
			}
			r := prepareRuler(t, cfg, newMockRuleStore(map[string]rulespb.RuleGroupList{userID: groups}), withLimits(validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
				defaults.RulerEvaluationDelay = 0
				defaults.RulerHomeZoneOwnershipEnabled = tc.homeZone
			})))
			t.Cleanup(r.manager.Stop)

			require.NoError(t, services.StartAndAwaitRunning(t.Context(), r.ring))
			t.Cleanup(func() { assert.NoError(t, services.StopAndAwaitTerminated(context.Background(), r.ring)) })
			test.Poll(t, time.Second, len(desc.Ingesters), func() interface{} { return r.ring.InstancesCount() })

			loaded, err := r.listRuleGroupsToSyncForAllUsers(t.Context(), rulerSyncReasonPeriodic, true)
			require.NoError(t, err)

			share := float64(len(loaded[userID])) / float64(len(groups))
			assert.GreaterOrEqual(t, share, tc.minShare)
			assert.LessOrEqual(t, share, tc.maxShare)
		})
	}
}

func BenchmarkFilterRuleGroupsByOwnership(b *testing.B) {
	groups := testRuleGroups("user-1", 1000)
	desc := zonedRulersDesc(map[string]int{"zone-a": 10, "zone-b": 10, "zone-c": 10})
	leavingDesc := cloneDesc(desc)
	rulerChanges["is LEAVING"](leavingDesc, "ruler-zone-c-")
	ringCheckErrors := prometheus.NewCounter(prometheus.CounterOpts{Name: "ring_check_errors"})

	for _, bc := range []struct {
		name     string
		desc     *ring.Desc
		homeZone bool
	}{
		{name: "token ownership", desc: desc},
		{name: "home zone ownership", desc: desc, homeZone: true},
		{name: "home zone ownership with a LEAVING zone", desc: leavingDesc, homeZone: true},
	} {
		r := newRulerRing(b, bc.desc, true)
		b.Run(bc.name, func(b *testing.B) {
			for b.Loop() {
				filterRuleGroupsByOwnership("user-1", groups, r, "ruler-zone-a-0", log.NewNopLogger(), ringCheckErrors, rulerSyncReasonPeriodic, bc.homeZone)
			}
		})
	}
}

func testRuleGroups(userID string, n int) []*rulespb.RuleGroupDesc {
	groups := make([]*rulespb.RuleGroupDesc, n)
	for i := range groups {
		groups[i] = &rulespb.RuleGroupDesc{User: userID, Namespace: "namespace", Name: fmt.Sprintf("group-%d", i)}
	}
	return groups
}

func homeZoneOwners(t *testing.T, r ring.ReadRing, groups []*rulespb.RuleGroupDesc, op ring.Operation) []ring.InstanceDesc {
	owners := make([]ring.InstanceDesc, len(groups))
	for i, g := range groups {
		owner, err := homeZoneOwner(r, tokenForGroup(g), op)
		require.NoError(t, err)
		owners[i] = owner
	}
	return owners
}

func ownersPerZone(owners []ring.InstanceDesc) map[string]int {
	perZone := map[string]int{}
	for _, o := range owners {
		perZone[o.Zone]++
	}
	return perZone
}

func cloneDesc(d *ring.Desc) *ring.Desc {
	c := ring.NewDesc()
	maps.Copy(c.Ingesters, d.Ingesters)
	return c
}

func updateRulers(d *ring.Desc, idPrefix string, update func(*ring.InstanceDesc)) {
	for id, inst := range d.Ingesters {
		if strings.HasPrefix(id, idPrefix) {
			update(&inst)
			d.Ingesters[id] = inst
		}
	}
}

func removeRulers(d *ring.Desc, idPrefix string) {
	maps.DeleteFunc(d.Ingesters, func(id string, _ ring.InstanceDesc) bool {
		return strings.HasPrefix(id, idPrefix)
	})
}
