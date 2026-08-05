// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
	"github.com/grafana/mimir/pkg/util/validation"
)

// twoZoneReplicaMap is the steady-state RF=2 expansion for two logical
// slots, each mirrored in zone-a and zone-b.
func twoZoneReplicaMap() readcacheassignment.ReplicaMap {
	return readcacheassignment.ReplicaMap{
		"readcache-0": {
			{InstanceID: "readcache-zone-a-0", Zone: "zone-a"},
			{InstanceID: "readcache-zone-b-0", Zone: "zone-b"},
		},
		"readcache-1": {
			{InstanceID: "readcache-zone-a-1", Zone: "zone-a"},
			{InstanceID: "readcache-zone-b-1", Zone: "zone-b"},
		},
	}
}

// addrsOf returns the concrete dial targets of a replication set,
// sorted for comparison.
func addrsOf(set ring.ReplicationSet) []string {
	out := make([]string, 0, len(set.Instances))
	for _, inst := range set.Instances {
		out = append(out, inst.Addr)
	}
	sort.Strings(out)
	return out
}

// TestGetReadcacheReplicationSetsForQuery_RF2 covers the read-path
// expansion: each partition resolves to one replication set holding
// both zone mirrors of its logical owner, zone-aware so a single zone
// answering is enough.
func TestGetReadcacheReplicationSetsForQuery_RF2(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	const userID = "user-1"
	partitions := []int32{0, 1, 2, 3}
	from := model.TimeFromUnixNano(now.UnixNano())
	to := model.TimeFromUnixNano(now.Add(time.Minute).UnixNano())
	anyMatcher := []*labels.Matcher{mustEqualMatcher("bar", "baz")}

	// Even partitions on logical slot 0, odd ones on slot 1.
	logicalOwnerFor := func(p int32) (string, bool) {
		if p%2 == 0 {
			return "readcache-0", true
		}
		return "readcache-1", true
	}

	t.Run("each partition gets one zone-aware set with both mirrors", func(t *testing.T) {
		d := readcacheTestDistributor(t, now, partitions, logicalOwnerFor)
		m := twoZoneReplicaMap()
		d.setReadcacheAssignment(d.GetReadcacheLog(), m)

		sets, partitionByInstance, err := d.getReadcacheReplicationSetsForQuery(userID, from, to, anyMatcher)
		require.NoError(t, err)
		require.Len(t, sets, len(partitions), "one replication set per partition, not per pod")

		seen := map[int32][]string{}
		for _, set := range sets {
			require.Len(t, set.Instances, 2, "both zone mirrors must be in the same set")
			assert.True(t, set.ZoneAwarenessEnabled)
			assert.Equal(t, 1, set.MaxUnavailableZones, "one of the two zones may be unavailable")
			assert.Zero(t, set.MaxErrors, "MaxErrors and zone awareness are mutually exclusive")

			zones := []string{}
			partIDs := map[int32]struct{}{}
			for _, inst := range set.Instances {
				zones = append(zones, inst.Zone)
				partID, ok := partitionByInstance[inst.Id]
				require.True(t, ok, "instance %q must carry a partition hint", inst.Id)
				partIDs[partID] = struct{}{}
				// Id is the synthetic routing key over the concrete
				// pod; Addr is what actually gets dialed.
				assert.Equal(t, readcacheSyntheticInstanceID(inst.Addr, partID), inst.Id)
			}
			sort.Strings(zones)
			assert.Equal(t, []string{"zone-a", "zone-b"}, zones)
			require.Len(t, partIDs, 1, "every instance in a set serves the same partition")
			for partID := range partIDs {
				seen[partID] = addrsOf(set)
			}
		}

		require.Len(t, seen, len(partitions))
		assert.Equal(t, []string{"readcache-zone-a-0", "readcache-zone-b-0"}, seen[0])
		assert.Equal(t, []string{"readcache-zone-a-1", "readcache-zone-b-1"}, seen[1])
		assert.Equal(t, []string{"readcache-zone-a-0", "readcache-zone-b-0"}, seen[2])
		assert.Equal(t, []string{"readcache-zone-a-1", "readcache-zone-b-1"}, seen[3])
	})

	t.Run("a slot with a single live mirror needs that mirror to answer", func(t *testing.T) {
		d := readcacheTestDistributor(t, now, []int32{0}, func(int32) (string, bool) { return "readcache-0", true })
		m := readcacheassignment.ReplicaMap{"readcache-0": {
			{InstanceID: "readcache-zone-a-0", Zone: "zone-a"},
		}}
		d.setReadcacheAssignment(d.GetReadcacheLog(), m)

		sets, _, err := d.getReadcacheReplicationSetsForQuery(userID, from, to, anyMatcher)
		require.NoError(t, err)
		require.Len(t, sets, 1)
		require.Len(t, sets[0].Instances, 1)
		assert.False(t, sets[0].ZoneAwarenessEnabled, "a single zone offers nothing to fail over to")
		assert.Zero(t, sets[0].MaxUnavailableZones)
		assert.Zero(t, sets[0].MaxErrors)
	})

	t.Run("replicas without zone labels tolerate a single failure", func(t *testing.T) {
		d := readcacheTestDistributor(t, now, []int32{0}, func(int32) (string, bool) { return "readcache-0", true })
		m := readcacheassignment.ReplicaMap{"readcache-0": {
			{InstanceID: "rc-a"}, {InstanceID: "rc-b"},
		}}
		d.setReadcacheAssignment(d.GetReadcacheLog(), m)

		sets, _, err := d.getReadcacheReplicationSetsForQuery(userID, from, to, anyMatcher)
		require.NoError(t, err)
		require.Len(t, sets, 1)
		require.Len(t, sets[0].Instances, 2)
		assert.False(t, sets[0].ZoneAwarenessEnabled)
		assert.Equal(t, 1, sets[0].MaxErrors, "either replica alone can serve the read")
	})

	t.Run("a logical slot with no live mirror fails routing", func(t *testing.T) {
		d := readcacheTestDistributor(t, now, []int32{0}, func(int32) (string, bool) { return "readcache-0", true })
		// The rebalancer knows the slot but both mirrors left the ring.
		m := readcacheassignment.ReplicaMap{"readcache-0": nil}
		d.setReadcacheAssignment(d.GetReadcacheLog(), m)

		_, _, err := d.getReadcacheReplicationSetsForQuery(userID, from, to, anyMatcher)
		require.Error(t, err)
		var rcErr errReadcacheRoutingUnavailable
		assert.True(t, errors.As(err, &rcErr), "want errReadcacheRoutingUnavailable, got %T", err)
	})

	t.Run("without a replica map routing is unchanged", func(t *testing.T) {
		d := readcacheTestDistributor(t, now, partitions, func(int32) (string, bool) { return "rc-a", true })

		sets, partitionByInstance, err := d.getReadcacheReplicationSetsForQuery(userID, from, to, anyMatcher)
		require.NoError(t, err)
		require.Len(t, sets, len(partitions))
		for _, set := range sets {
			require.Len(t, set.Instances, 1)
			assert.Equal(t, "rc-a", set.Instances[0].Addr)
			assert.Empty(t, set.Instances[0].Zone)
			assert.False(t, set.ZoneAwarenessEnabled)
			assert.Zero(t, set.MaxErrors)
			assert.Contains(t, partitionByInstance, set.Instances[0].Id)
		}
	})

	t.Run("ignore-replica-map-for-queries keeps dialing the logical owner", func(t *testing.T) {
		d := readcacheTestDistributor(t, now, []int32{0}, func(int32) (string, bool) { return "readcache-0", true })
		d.cfg.Readcache.IgnoreReplicaMapForQueries = true
		// Dual-fleet warm: map lists legacy + both zones, but queries
		// must stay on the logical (legacy) ID until cutover.
		m := readcacheassignment.ReplicaMap{"readcache-0": {
			{InstanceID: "readcache-0", Zone: ""},
			{InstanceID: "readcache-zone-a-0", Zone: "zone-a"},
			{InstanceID: "readcache-zone-b-0", Zone: "zone-b"},
		}}
		d.setReadcacheAssignment(d.GetReadcacheLog(), m)

		sets, _, err := d.getReadcacheReplicationSetsForQuery(userID, from, to, anyMatcher)
		require.NoError(t, err)
		require.Len(t, sets, 1)
		require.Len(t, sets[0].Instances, 1)
		assert.Equal(t, "readcache-0", sets[0].Instances[0].Addr)
		assert.Empty(t, sets[0].Instances[0].Zone)
		assert.False(t, sets[0].ZoneAwarenessEnabled)
	})

	t.Run("cutover drops non-zonal replicas from the query set", func(t *testing.T) {
		d := readcacheTestDistributor(t, now, []int32{0}, func(int32) (string, bool) { return "readcache-0", true })
		// IgnoreReplicaMapForQueries stays false (default): once the
		// map is used for queries, prefer zoned mirrors so the legacy
		// STS can keep consuming without serving.
		m := readcacheassignment.ReplicaMap{"readcache-0": {
			{InstanceID: "readcache-0", Zone: ""},
			{InstanceID: "readcache-zone-a-0", Zone: "zone-a"},
			{InstanceID: "readcache-zone-b-0", Zone: "zone-b"},
		}}
		d.setReadcacheAssignment(d.GetReadcacheLog(), m)

		sets, _, err := d.getReadcacheReplicationSetsForQuery(userID, from, to, anyMatcher)
		require.NoError(t, err)
		require.Len(t, sets, 1)
		assert.Equal(t, []string{"readcache-zone-a-0", "readcache-zone-b-0"}, addrsOf(sets[0]))
		assert.True(t, sets[0].ZoneAwarenessEnabled)
		assert.Equal(t, 1, sets[0].MaxUnavailableZones)
	})
}

// TestGetReadcacheReplicationSetsForQuery_RF2MoveWindow proves the two
// dimensions compose: a partition that changed logical owner inside the
// query window yields one set per owner, each expanded to its own zone
// mirrors. Collapsing them into a single set would lose the frozen
// slice held by the previous owner.
func TestGetReadcacheReplicationSetsForQuery_RF2MoveWindow(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	const userID = "user-1"
	full := assignment.HashRange{Lo: 0, Hi: 4294967295}

	d := &Distributor{
		now: func() time.Time { return now },
		limits: validation.NewOverrides(validation.Limits{
			QueryIngestersWithin: model.Duration(13 * time.Hour),
		}, nil),
	}
	d.nautilusLog.Store(assignment.NewLogFromEntries([]assignment.LogEntry{
		{Range: full, PartitionID: 0, From: now.Add(-2 * time.Hour), To: now.Add(5 * time.Minute)},
	}))
	// Partition 0 moved from logical slot 0 to slot 1 half an hour ago.
	m := twoZoneReplicaMap()
	d.setReadcacheAssignment(readcacheassignment.NewLogFromEntries([]readcacheassignment.LogEntry{
		{PartitionID: 0, InstanceID: "readcache-0", From: now.Add(-2 * time.Hour), To: now.Add(-30 * time.Minute)},
		{PartitionID: 0, InstanceID: "readcache-1", From: now.Add(-30 * time.Minute), To: now.Add(5 * time.Minute)},
	}), m)

	sets, partitionByInstance, err := d.getReadcacheReplicationSetsForQuery(
		userID,
		model.TimeFromUnixNano(now.Add(-time.Hour).UnixNano()),
		model.TimeFromUnixNano(now.UnixNano()),
		[]*labels.Matcher{mustEqualMatcher("bar", "baz")},
	)
	require.NoError(t, err)
	require.Len(t, sets, 2, "one replication set per logical owner during the window")

	var got [][]string
	for _, set := range sets {
		require.Len(t, set.Instances, 2)
		assert.True(t, set.ZoneAwarenessEnabled)
		for _, inst := range set.Instances {
			assert.Equal(t, int32(0), partitionByInstance[inst.Id])
		}
		got = append(got, addrsOf(set))
	}
	sort.Slice(got, func(i, j int) bool { return got[i][0] < got[j][0] })
	assert.Equal(t, [][]string{
		{"readcache-zone-a-0", "readcache-zone-b-0"},
		{"readcache-zone-a-1", "readcache-zone-b-1"},
	}, got)

	// All four synthetic IDs are distinct, so DoMultiUntilQuorum can
	// track each pod independently.
	assert.Len(t, partitionByInstance, 4)
}

// TestResolveReadcacheClientForPartition_RF2Failover covers the
// single-owner dialing path (warm-up fallback, query-load attribution):
// the logged logical owner is expanded and the replicas are tried in
// order, so an unreachable first mirror falls through to the second
// instead of failing the partition.
func TestResolveReadcacheClientForPartition_RF2Failover(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	ctx := context.Background()

	newDistributor := func(t *testing.T, addresses string, m readcacheassignment.ReplicaMap) *Distributor {
		t.Helper()
		pool, err := newReadcachePool(ReadcacheConfig{Addresses: addresses}, nil, "", nil, log.NewNopLogger())
		require.NoError(t, err)
		t.Cleanup(func() { _ = pool.Close() })

		d := &Distributor{now: func() time.Time { return now }, readcachePool: pool}
		d.setReadcacheAssignment(readcacheassignment.NewLogFromEntries([]readcacheassignment.LogEntry{
			{PartitionID: 0, InstanceID: "readcache-0", From: now.Add(-time.Minute), To: now.Add(5 * time.Minute)},
		}), m)
		return d
	}

	t.Run("prefers the first replica", func(t *testing.T) {
		d := newDistributor(t, "readcache-zone-a-0=127.0.0.1:9095,readcache-zone-b-0=127.0.0.1:9096", twoZoneReplicaMap())
		_, instanceID, ok, err := d.resolveReadcacheClientForPartition(ctx, 0)
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, "readcache-zone-a-0", instanceID)
	})

	t.Run("falls through to the next replica when the first is unresolvable", func(t *testing.T) {
		// Only zone-b has an address, so resolving zone-a fails.
		d := newDistributor(t, "readcache-zone-b-0=127.0.0.1:9096", twoZoneReplicaMap())
		_, instanceID, ok, err := d.resolveReadcacheClientForPartition(ctx, 0)
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, "readcache-zone-b-0", instanceID)
	})

	t.Run("reports the error when no replica resolves", func(t *testing.T) {
		d := newDistributor(t, "readcache-zone-a-1=127.0.0.1:9097", twoZoneReplicaMap())
		_, _, ok, err := d.resolveReadcacheClientForPartition(ctx, 0)
		assert.False(t, ok)
		assert.Error(t, err)
	})

	t.Run("without a replica map the logged owner is dialed directly", func(t *testing.T) {
		d := newDistributor(t, "readcache-0=127.0.0.1:9095", nil)
		_, instanceID, ok, err := d.resolveReadcacheClientForPartition(ctx, 0)
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, "readcache-0", instanceID)
	})
}

// TestPreviousReadcacheClientForPartition_RF2 covers the target of the
// still-warming fallback under RF≥2.
//
// Both mirrors of a logical slot adopt a partition from the same lease
// row, so a move leaves them warming together and the peer mirror has
// nothing warmer to serve. The fallback must therefore resolve to the
// *previous* logical owner's mirrors, which still hold the frozen
// pre-move head — never to the current slot's peer.
func TestPreviousReadcacheClientForPartition_RF2(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	ctx := context.Background()

	// Partition 0 just moved from logical slot 0 to slot 1.
	entries := []readcacheassignment.LogEntry{
		{PartitionID: 0, InstanceID: "readcache-0", From: now.Add(-5 * time.Minute), To: now},
		{PartitionID: 0, InstanceID: "readcache-1", From: now, To: now.Add(5 * time.Minute)},
	}

	newDistributor := func(t *testing.T, addresses string, ignoreMap bool) *Distributor {
		t.Helper()
		pool, err := newReadcachePool(ReadcacheConfig{Addresses: addresses}, nil, "", nil, log.NewNopLogger())
		require.NoError(t, err)
		t.Cleanup(func() { _ = pool.Close() })

		d := &Distributor{now: func() time.Time { return now }, readcachePool: pool}
		d.cfg.Readcache.IgnoreReplicaMapForQueries = ignoreMap
		d.setReadcacheAssignment(readcacheassignment.NewLogFromEntries(entries), twoZoneReplicaMap())
		return d
	}

	t.Run("resolves a mirror of the previous logical owner", func(t *testing.T) {
		d := newDistributor(t, "readcache-zone-a-0=127.0.0.1:9095,readcache-zone-b-0=127.0.0.1:9096", false)
		_, instanceID, ok := d.previousReadcacheClientForPartition(ctx, 0)
		require.True(t, ok)
		assert.Equal(t, "readcache-zone-a-0", instanceID,
			"must dial slot 0's mirrors (the previous owner), not slot 1's peer")
	})

	t.Run("falls through when the previous owner's first mirror is unresolvable", func(t *testing.T) {
		d := newDistributor(t, "readcache-zone-b-0=127.0.0.1:9096", false)
		_, instanceID, ok := d.previousReadcacheClientForPartition(ctx, 0)
		require.True(t, ok)
		assert.Equal(t, "readcache-zone-b-0", instanceID)
	})

	t.Run("warm stage dials the previous logical owner directly", func(t *testing.T) {
		// With ignore-replica-map-for-queries the map is bypassed, so
		// the fallback stays on the legacy pod like the primary read.
		d := newDistributor(t, "readcache-0=127.0.0.1:9095,readcache-zone-a-0=127.0.0.1:9096", true)
		_, instanceID, ok := d.previousReadcacheClientForPartition(ctx, 0)
		require.True(t, ok)
		assert.Equal(t, "readcache-0", instanceID)
	})

	t.Run("reports no fallback when no mirror resolves", func(t *testing.T) {
		d := newDistributor(t, "readcache-zone-a-1=127.0.0.1:9097", false)
		_, _, ok := d.previousReadcacheClientForPartition(ctx, 0)
		assert.False(t, ok)
	})
}

// TestExplainReadcacheQuery_RF2 keeps the debug plan in lockstep with
// production routing: the plan must enumerate the concrete pods that
// would be dialed, not the logical slots recorded in the log.
func TestExplainReadcacheQuery_RF2(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	const userID = "user-1"

	d := readcacheTestDistributor(t, now, []int32{0, 1}, func(p int32) (string, bool) {
		if p == 0 {
			return "readcache-0", true
		}
		return "readcache-1", true
	})
	d.limits = validation.NewOverrides(validation.Limits{
		ReadcacheReadRouting: validation.ReadcacheReadRoutingNautilus,
	}, nil)
	m := twoZoneReplicaMap()
	d.setReadcacheAssignment(d.GetReadcacheLog(), m)

	plan := d.ExplainReadcacheQuery(context.Background(), userID,
		model.TimeFromUnixNano(now.UnixNano()),
		model.TimeFromUnixNano(now.Add(time.Minute).UnixNano()),
		[]*labels.Matcher{mustEqualMatcher("bar", "baz")})

	require.Empty(t, plan.Unavailable)
	require.Len(t, plan.Partitions, 2)
	assert.Equal(t, 4, plan.TotalCalls, "two partitions × two zone mirrors")

	for _, pp := range plan.Partitions {
		require.Len(t, pp.Calls, 2)
		zones := []string{}
		for _, c := range pp.Calls {
			zones = append(zones, c.Zone)
			assert.NotEqual(t, c.LogicalOwner, c.Owner, "Owner must be the concrete pod")
			assert.Equal(t, readcacheSyntheticInstanceID(c.Owner, pp.PartitionID), c.InstanceID)
			assert.False(t, c.LeaseFrom.IsZero(), "lease bounds come from the logical owner's entries")
		}
		sort.Strings(zones)
		assert.Equal(t, []string{"zone-a", "zone-b"}, zones)
	}
}

// TestGetReadcacheReplicaMap covers the accessor's zero value and the
// wholesale replacement the stream relies on.
func TestGetReadcacheReplicaMap(t *testing.T) {
	d := &Distributor{}
	assert.Empty(t, d.GetReadcacheReplicaMap(), "no snapshot yet means identity")

	m := twoZoneReplicaMap()
	d.setReadcacheAssignment(readcacheassignment.NewLog(), m)
	assert.True(t, m.Equal(d.GetReadcacheReplicaMap()))

	var cleared readcacheassignment.ReplicaMap
	d.setReadcacheAssignment(readcacheassignment.NewLog(), cleared)
	assert.Empty(t, d.GetReadcacheReplicaMap(), "clearing the map must restore identity")
}
