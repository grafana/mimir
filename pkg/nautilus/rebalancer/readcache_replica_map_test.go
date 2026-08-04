// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
)

// TestReplicaMapProtoRoundTrip covers the wire contract the readcache
// and the querier-embedded distributor decode: an empty map must stay
// empty (identity / RF=1) and a populated one must survive unchanged,
// including a slot whose replicas are all gone.
func TestReplicaMapProtoRoundTrip(t *testing.T) {
	t.Run("empty map encodes as identity", func(t *testing.T) {
		assert.Nil(t, ReplicaMapToProto(nil))
		assert.Nil(t, ReplicaMapToProto(readcacheassignment.ReplicaMap{}))
		assert.Nil(t, ReplicaMapFromProto(nil))
		assert.Nil(t, ReplicaMapFromProto([]ReadcacheReplicaSet{}))
	})

	t.Run("populated map survives the round trip", func(t *testing.T) {
		in := readcacheassignment.ReplicaMap{
			"readcache-0": {
				{InstanceID: "readcache-zone-a-0", Zone: "zone-a"},
				{InstanceID: "readcache-zone-b-0", Zone: "zone-b"},
			},
			"readcache-1": {
				{InstanceID: "readcache-zone-a-1", Zone: "zone-a"},
			},
			// Both mirrors of slot 2 are absent from the ring.
			"readcache-2": nil,
		}

		wire := ReplicaMapToProto(in)
		require.Len(t, wire, 3)
		// Logical IDs are emitted sorted so the encoded bytes are
		// stable across broadcasts.
		assert.Equal(t, []string{"readcache-0", "readcache-1", "readcache-2"},
			[]string{wire[0].LogicalId, wire[1].LogicalId, wire[2].LogicalId})

		out := ReplicaMapFromProto(wire)
		assert.True(t, in.Equal(out), "want %v, got %v", in, out)
		assert.Empty(t, out["readcache-2"], "a slot with no live mirror must decode as an empty replica list")
	})
}

// TestPlacementReadcacheInstances_StickyVsLegacy pins the difference
// between RF=1 placement (follows ring membership, so a departing pod
// shrinks the placement set) and RF>=2 placement (sticky logical
// slots, unaffected by a single zone mirror leaving).
func TestPlacementReadcacheInstances_StickyVsLegacy(t *testing.T) {
	newRebalancer := func(desired int, ringMembers ...string) *Rebalancer {
		instances := make([]ring.InstanceDesc, 0, len(ringMembers))
		for _, id := range ringMembers {
			instances = append(instances, ring.InstanceDesc{Id: id})
		}
		r := &Rebalancer{
			logger:              log.NewNopLogger(),
			readcacheRing:       stubReadcacheRing{set: ring.ReplicationSet{Instances: instances}},
			readcacheMembership: newReadcacheMembershipTracker(),
			readcacheStore:      newReadcacheLogStore(),
		}
		r.cfg.ReadcacheSlicer.DesiredReplicas = desired
		r.cfg.ReadcacheSlicer.LogicalIDPrefix = "readcache"
		return r
	}

	t.Run("legacy placement follows ring membership", func(t *testing.T) {
		r := newRebalancer(0, "readcache-zone-a-0", "readcache-zone-b-0")
		assert.Equal(t, []string{"readcache-zone-a-0", "readcache-zone-b-0"}, r.placementReadcacheInstances())
	})

	t.Run("sticky placement returns logical slots regardless of the ring", func(t *testing.T) {
		// Only zone-a of slot 1 is in the ring, and slot 2 is absent
		// entirely; placement must still offer all three slots.
		r := newRebalancer(3, "readcache-zone-a-0", "readcache-zone-b-0", "readcache-zone-a-1")
		assert.Equal(t, []string{"readcache-0", "readcache-1", "readcache-2"}, r.placementReadcacheInstances())
	})

	t.Run("sticky placement honours a custom prefix", func(t *testing.T) {
		r := newRebalancer(2)
		r.cfg.ReadcacheSlicer.LogicalIDPrefix = "rc"
		assert.Equal(t, []string{"rc-0", "rc-1"}, r.placementReadcacheInstances())
	})

	t.Run("placementReadcacheInstancesFrom reuses the caller's stabilized set", func(t *testing.T) {
		r := newRebalancer(0, "readcache-zone-a-0")
		assert.Equal(t, []string{"pinned-a", "pinned-b"},
			r.placementReadcacheInstancesFrom([]string{"pinned-a", "pinned-b"}),
			"under RF=1 the caller's set is passed through untouched")

		r.cfg.ReadcacheSlicer.DesiredReplicas = 1
		assert.Equal(t, []string{"readcache-0"},
			r.placementReadcacheInstancesFrom([]string{"pinned-a", "pinned-b"}),
			"under RF>=2 the stabilized set is irrelevant")
	})
}

// TestRefreshReplicaMap covers both directions of the DesiredReplicas
// switch: building the map from the ring (or the static pin list) and
// clearing it back to identity.
func TestRefreshReplicaMap(t *testing.T) {
	newRebalancer := func(desired int, ringMembers ...ring.InstanceDesc) *Rebalancer {
		r := &Rebalancer{
			logger:         log.NewNopLogger(),
			readcacheRing:  stubReadcacheRing{set: ring.ReplicationSet{Instances: ringMembers}},
			readcacheStore: newReadcacheLogStore(),
		}
		r.cfg.ReadcacheSlicer.DesiredReplicas = desired
		r.cfg.ReadcacheSlicer.LogicalIDPrefix = "readcache"
		return r
	}

	t.Run("groups ring members by logical slot", func(t *testing.T) {
		r := newRebalancer(2,
			ring.InstanceDesc{Id: "readcache-zone-b-0", Zone: "zone-b"},
			ring.InstanceDesc{Id: "readcache-zone-a-0", Zone: "zone-a"},
			ring.InstanceDesc{Id: "readcache-zone-a-1", Zone: "zone-a"},
		)
		m := r.refreshReplicaMap()
		assert.Equal(t, []readcacheassignment.Replica{
			{InstanceID: "readcache-zone-a-0", Zone: "zone-a"},
			{InstanceID: "readcache-zone-b-0", Zone: "zone-b"},
		}, m["readcache-0"], "replicas are ordered by zone for stability")
		assert.Equal(t, []readcacheassignment.Replica{
			{InstanceID: "readcache-zone-a-1", Zone: "zone-a"},
		}, m["readcache-1"])
		assert.True(t, m.Equal(r.readcacheStore.getReplicaMap()), "the map must be published on the store")
	})

	t.Run("desired slots with no live mirror still get an entry", func(t *testing.T) {
		r := newRebalancer(3, ring.InstanceDesc{Id: "readcache-zone-a-0", Zone: "zone-a"})
		m := r.refreshReplicaMap()
		require.Contains(t, m, "readcache-2")
		assert.Empty(t, m["readcache-2"])
	})

	t.Run("zone is recovered from the instance name when the ring omits it", func(t *testing.T) {
		r := newRebalancer(1, ring.InstanceDesc{Id: "readcache-zone-a-0"})
		m := r.refreshReplicaMap()
		assert.Equal(t, []readcacheassignment.Replica{
			{InstanceID: "readcache-zone-a-0", Zone: "zone-a"},
		}, m["readcache-0"])
	})

	t.Run("static pin list wins over the ring", func(t *testing.T) {
		r := newRebalancer(1, ring.InstanceDesc{Id: "readcache-zone-a-0", Zone: "zone-a"})
		r.cfg.ReadcacheSlicer.Instances = flagext.StringSliceCSV{"readcache-zone-b-0"}
		m := r.refreshReplicaMap()
		assert.Equal(t, []readcacheassignment.Replica{
			{InstanceID: "readcache-zone-b-0", Zone: "zone-b"},
		}, m["readcache-0"])
	})

	t.Run("DesiredReplicas=0 clears the map back to identity", func(t *testing.T) {
		r := newRebalancer(2,
			ring.InstanceDesc{Id: "readcache-zone-a-0", Zone: "zone-a"},
			ring.InstanceDesc{Id: "readcache-zone-b-0", Zone: "zone-b"},
		)
		require.NotEmpty(t, r.refreshReplicaMap())

		r.cfg.ReadcacheSlicer.DesiredReplicas = 0
		assert.Nil(t, r.refreshReplicaMap())
		assert.Empty(t, r.readcacheStore.getReplicaMap())
	})
}

// TestExcludeLogicalTargetsFromConcreteFailures is the RF=2 failure
// isolation rule: a stats-RPC failure on one zone mirror must not take
// the logical slot out of the placement set, because the other mirror
// still serves it. Only when every mirror is gone does the slot become
// ineligible.
func TestExcludeLogicalTargetsFromConcreteFailures(t *testing.T) {
	replicaMap := readcacheassignment.ReplicaMap{
		"readcache-0": {
			{InstanceID: "readcache-zone-a-0", Zone: "zone-a"},
			{InstanceID: "readcache-zone-b-0", Zone: "zone-b"},
		},
		"readcache-1": {
			{InstanceID: "readcache-zone-a-1", Zone: "zone-a"},
			{InstanceID: "readcache-zone-b-1", Zone: "zone-b"},
		},
	}
	healthy := map[string]struct{}{
		"readcache-zone-a-0": {}, "readcache-zone-b-0": {},
		"readcache-zone-a-1": {}, "readcache-zone-b-1": {},
	}

	t.Run("no failures excludes nothing", func(t *testing.T) {
		assert.Empty(t, excludeLogicalTargetsFromConcreteFailures(nil, replicaMap, healthy))
	})

	t.Run("one zone failing does not exclude the slot", func(t *testing.T) {
		got := excludeLogicalTargetsFromConcreteFailures(
			map[string]struct{}{"readcache-zone-a-0": {}}, replicaMap, healthy)
		assert.Empty(t, got, "zone-b still serves slot 0")
	})

	t.Run("both zones failing excludes the slot", func(t *testing.T) {
		got := excludeLogicalTargetsFromConcreteFailures(
			map[string]struct{}{"readcache-zone-a-0": {}, "readcache-zone-b-0": {}}, replicaMap, healthy)
		assert.Equal(t, map[string]struct{}{"readcache-0": {}}, got)
	})

	t.Run("a slot with no live mirror is excluded", func(t *testing.T) {
		withEmpty := replicaMap.Clone()
		withEmpty["readcache-2"] = nil
		got := excludeLogicalTargetsFromConcreteFailures(
			map[string]struct{}{"readcache-zone-a-0": {}}, withEmpty, healthy)
		assert.Equal(t, map[string]struct{}{"readcache-2": {}}, got)
	})

	t.Run("a mirror missing from the ring counts as unavailable", func(t *testing.T) {
		// zone-a of slot 1 failed its stats RPC and zone-b is no
		// longer in the ring, so nothing serves slot 1.
		partiallyHealthy := map[string]struct{}{
			"readcache-zone-a-0": {}, "readcache-zone-b-0": {}, "readcache-zone-a-1": {},
		}
		got := excludeLogicalTargetsFromConcreteFailures(
			map[string]struct{}{"readcache-zone-a-1": {}}, replicaMap, partiallyHealthy)
		assert.Equal(t, map[string]struct{}{"readcache-1": {}}, got)
	})

	t.Run("without a replica map concrete failures are logical failures", func(t *testing.T) {
		failed := map[string]struct{}{"rc-a": {}}
		assert.Equal(t, failed, excludeLogicalTargetsFromConcreteFailures(failed, nil, nil))
	})
}

// TestReadcacheLogStore_SetReplicaMapRebroadcasts pins the reason
// setReplicaMap does its own broadcast: a zone mirror joining or
// leaving changes no lease, so subscribers would otherwise keep
// dialing a stale replica set until the next lease rotation.
func TestReadcacheLogStore_SetReplicaMapRebroadcasts(t *testing.T) {
	newStoreWithLease := func(t *testing.T) (*readcacheLogStore, time.Time) {
		t.Helper()
		s := newReadcacheLogStore()
		now := time.Unix(10_000, 0)
		require.True(t, s.apply(now, &readcacheassignment.Assignment{Entries: []readcacheassignment.AssignmentEntry{
			{PartitionID: 0, InstanceID: "readcache-0"},
		}}, 5*time.Minute, time.Minute, time.Hour, 0))
		return s, now
	}

	m := readcacheassignment.ReplicaMap{"readcache-0": {
		{InstanceID: "readcache-zone-a-0", Zone: "zone-a"},
		{InstanceID: "readcache-zone-b-0", Zone: "zone-b"},
	}}

	t.Run("a map change re-primes delta subscribers with a full snapshot", func(t *testing.T) {
		s, _ := newStoreWithLease(t)
		initial, updates, unsubscribe := s.subscribe(true)
		defer unsubscribe()
		require.NotNil(t, initial)
		assert.Empty(t, initial.replicaMap)

		s.setReplicaMap(m)

		select {
		case u := <-updates:
			assert.True(t, u.reset, "a map change must be delivered as a full snapshot")
			assert.Len(t, u.entries, 1)
			assert.True(t, m.Equal(u.replicaMap))
		default:
			t.Fatal("setReplicaMap must broadcast when the map changes")
		}
	})

	t.Run("an unchanged map broadcasts nothing", func(t *testing.T) {
		s, _ := newStoreWithLease(t)
		s.setReplicaMap(m)
		_, updates, unsubscribe := s.subscribe(true)
		defer unsubscribe()

		s.setReplicaMap(m.Clone())
		select {
		case u := <-updates:
			t.Fatalf("unchanged map must not rebroadcast, got %+v", u)
		default:
		}
	})

	t.Run("subsequent lease applies carry the current map", func(t *testing.T) {
		s, now := newStoreWithLease(t)
		s.setReplicaMap(m)
		_, updates, unsubscribe := s.subscribe(true)
		defer unsubscribe()
		// Drain the initial snapshot the subscribe primed.
		select {
		case <-updates:
		default:
		}

		require.True(t, s.apply(now.Add(time.Minute), &readcacheassignment.Assignment{Entries: []readcacheassignment.AssignmentEntry{
			{PartitionID: 0, InstanceID: "readcache-1"},
		}}, 5*time.Minute, time.Minute, time.Hour, 0))

		select {
		case u := <-updates:
			assert.True(t, m.Equal(u.replicaMap), "every broadcast must carry the current map")
		default:
			t.Fatal("apply must broadcast a lease change")
		}
	})

	t.Run("the map is withheld until the store is ready", func(t *testing.T) {
		s := newReadcacheLogStore()
		_, updates, unsubscribe := s.subscribe(true)
		defer unsubscribe()

		s.setReplicaMap(m)
		select {
		case u := <-updates:
			t.Fatalf("a pre-apply store must not broadcast, got %+v", u)
		default:
		}
		assert.True(t, m.Equal(s.getReplicaMap()), "the map is still recorded for the first apply to carry")
	})
}

// TestReadcacheUpdateToProto_CarriesReplicaMap checks the last hop
// between the store and the wire.
func TestReadcacheUpdateToProto_CarriesReplicaMap(t *testing.T) {
	u := readcacheUpdate{
		entries: []readcacheassignment.LogEntry{
			{PartitionID: 0, InstanceID: "readcache-0", From: time.Unix(1, 0), To: time.Unix(300, 0)},
		},
		reset: true,
		replicaMap: readcacheassignment.ReplicaMap{"readcache-0": {
			{InstanceID: "readcache-zone-a-0", Zone: "zone-a"},
		}},
	}
	resp := readcacheUpdateToProto(u)
	require.Len(t, resp.ReplicaSets, 1)
	assert.Equal(t, "readcache-0", resp.ReplicaSets[0].LogicalId)
	assert.Equal(t, []ReadcacheReplica{{InstanceId: "readcache-zone-a-0", Zone: "zone-a"}}, resp.ReplicaSets[0].Replicas)

	assert.Nil(t, readcacheUpdateToProto(readcacheUpdate{entries: u.entries}).ReplicaSets,
		"RF=1 updates must not carry replica sets")
}
