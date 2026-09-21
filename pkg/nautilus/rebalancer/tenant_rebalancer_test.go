// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"math"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
)

func TestRunSlicer_PreservesIndependentTenantTilings(t *testing.T) {
	full := assignment.HashRange{Lo: 0, Hi: math.MaxUint32}
	current := &assignment.Assignment{Entries: []assignment.Entry{
		{TenantID: "tenant-a", Range: full, PartitionID: 0},
		{TenantID: "tenant-b", Range: full, PartitionID: 1},
	}}
	r := &Rebalancer{}

	got, actions := r.runSlicer(current, nil, nil, []int32{0, 1}, nil, time.Time{})

	require.NoError(t, got.Validate())
	assert.Equal(t, current.Entries, got.Entries)
	assert.Empty(t, actions, "identical numeric ranges from different tenants must never merge")
}

func TestMergeAdjacentCold_DoesNotMergeAcrossTenants(t *testing.T) {
	full := assignment.HashRange{Lo: 0, Hi: math.MaxUint32}
	entries := []rangeLoad{
		{entry: assignment.Entry{TenantID: "tenant-a", Range: full, PartitionID: 0}, load: 0.1},
		{entry: assignment.Entry{TenantID: "tenant-b", Range: full, PartitionID: 0}, load: 0.1},
	}

	got, actions := mergeAdjacentCold(entries, 1, math.MaxFloat64, 1, 1, 0, nil)

	assert.Equal(t, entries, got)
	assert.Empty(t, actions)
}

func TestRunSlicer_HotTenantSplitLeavesColocatedTenantUnchanged(t *testing.T) {
	ranges := []assignment.HashRange{
		{Lo: 0, Hi: 999},
		{Lo: 1000, Hi: 1999},
		{Lo: 2000, Hi: math.MaxUint32},
	}
	var entries []assignment.Entry
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		entries = append(entries,
			assignment.Entry{TenantID: tenantID, Range: ranges[0], PartitionID: 0},
			assignment.Entry{TenantID: tenantID, Range: ranges[1], PartitionID: 1},
			assignment.Entry{TenantID: tenantID, Range: ranges[2], PartitionID: 1},
		)
	}
	current := &assignment.Assignment{Entries: entries}
	rates := []rangeRate{
		{tenantID: "tenant-a", partitionID: 0, hr: ranges[0], sampleRate: 100},
		{tenantID: "tenant-a", partitionID: 1, hr: ranges[1], sampleRate: 1},
		{tenantID: "tenant-a", partitionID: 1, hr: ranges[2], sampleRate: 1},
	}
	r := &Rebalancer{}

	got, actions := r.runSlicer(current, rates, partitionLoadFromRates(rates, []int32{0, 1}), []int32{0, 1}, nil, time.Time{})

	require.NoError(t, got.Validate())
	var tenantB []assignment.Entry
	for _, entry := range got.Entries {
		if entry.TenantID == "tenant-b" {
			tenantB = append(tenantB, entry)
		}
	}
	assert.Equal(t, current.Entries[3:], tenantB)
	require.NotEmpty(t, actions)
	for _, action := range actions {
		assert.Equal(t, "tenant-a", action.TenantID)
	}
	assert.Positive(t, countActions(actions).splits)
}

func TestCollectUnknownTenants_DeduplicatesAndAcceptsOnlyPartitionZero(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	firstSeen := h.clock.Now().Add(-2 * time.Minute)
	rc0 := h.addReadcache("readcache-0")
	rc0.unknownTenants = []ingester_client.UnknownTenant{
		{TenantId: "tenant-new", PartitionId: 0, FirstSeenUnixMs: firstSeen.Add(time.Minute).UnixMilli()},
		{TenantId: "tenant-new", PartitionId: 0, FirstSeenUnixMs: firstSeen.UnixMilli()},
		{TenantId: "ignored", PartitionId: 3, FirstSeenUnixMs: firstSeen.UnixMilli()},
	}
	rc1 := h.addReadcache("readcache-1")
	rc1.unknownTenants = []ingester_client.UnknownTenant{
		{TenantId: "tenant-new", PartitionId: 0, FirstSeenUnixMs: firstSeen.Add(30 * time.Second).UnixMilli()},
	}

	_, _, _, _, _, unknowns, _, err := h.r.collectRatesFromReadcaches(h.ctx)

	require.NoError(t, err)
	require.Len(t, unknowns, 1)
	assert.Equal(t, "tenant-new", unknowns[0].tenantID)
	assert.True(t, firstSeen.Equal(unknowns[0].firstSeen))
}

func TestBootstrapUnknownTenant_PreservesAssignmentsAndHistory(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	now := h.clock.Now()
	existing := &assignment.Assignment{Entries: []assignment.Entry{{
		TenantID:    "tenant-existing",
		Range:       assignment.HashRange{Lo: 0, Hi: math.MaxUint32},
		PartitionID: 2,
	}}}
	require.True(t, h.r.store.apply(now, existing, h.cfg.LeaseDuration, h.r.hashLeaseLookahead(), h.cfg.EntryRetention))
	firstSeen := now.Add(-3 * time.Minute)

	got, seeded, err := h.r.bootstrapUnknownTenants(now, existing, []unknownTenant{
		{tenantID: "tenant-new", firstSeen: firstSeen},
		{tenantID: "tenant-new", firstSeen: firstSeen.Add(time.Minute)},
	}, []int32{0, 1, 2})

	require.NoError(t, err)
	require.Equal(t, 1, seeded)
	require.NoError(t, got.Validate())
	require.Len(t, got.Entries, 1+initialSlicesPerPartition)
	assert.Equal(t, existing.Entries[0], got.Entries[0])
	for _, entry := range got.Entries[1:] {
		assert.Equal(t, "tenant-new", entry.TenantID)
		assert.Equal(t, int32(0), entry.PartitionID)
	}

	var existingFrom, newFrom []time.Time
	for _, entry := range h.r.store.snapshot() {
		switch entry.TenantID {
		case "tenant-existing":
			existingFrom = append(existingFrom, entry.From)
		case "tenant-new":
			newFrom = append(newFrom, entry.From)
		}
	}
	require.Equal(t, []time.Time{now}, existingFrom, "bootstrap must not backdate existing tenants")
	require.Len(t, newFrom, initialSlicesPerPartition)
	for _, from := range newFrom {
		assert.True(t, firstSeen.Equal(from))
	}

	got, seeded, err = h.r.bootstrapUnknownTenants(now, got, []unknownTenant{{tenantID: "tenant-new", firstSeen: firstSeen}}, []int32{0, 1, 2})
	require.NoError(t, err)
	assert.Zero(t, seeded)
	assert.Len(t, h.r.store.snapshot(), 1+initialSlicesPerPartition, "duplicate reports must not seed duplicate history")
}

func TestBootstrapUnknownTenant_RejectsInactivePartitionZero(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	_, seeded, err := h.r.bootstrapUnknownTenants(h.clock.Now(), nil, []unknownTenant{{
		tenantID:  "tenant-new",
		firstSeen: h.clock.Now(),
	}}, []int32{1, 2})

	require.EqualError(t, err, "cannot bootstrap 1 unknown tenant(s): Kafka partition 0 is not in the active partition set")
	assert.Zero(t, seeded)
	assert.Empty(t, h.r.store.snapshot())
}

func TestRebalance_UnknownTenantSeedsAndPushesInSameRound(t *testing.T) {
	h := newHarness(t, harnessOpts{cfg: Config{
		PartitionCount: 2,
		ReadcacheSlicer: ReadcacheSlicerConfig{
			Enabled:        true,
			Alpha:          1,
			MovementBudget: 1,
		},
	}})
	rc := h.addReadcache("readcache-0")
	firstSeen := h.clock.Now().Add(-time.Minute)
	rc.unknownTenants = []ingester_client.UnknownTenant{
		{TenantId: "tenant-new", PartitionId: 0, FirstSeenUnixMs: firstSeen.UnixMilli()},
		{TenantId: "tenant-new", PartitionId: 0, FirstSeenUnixMs: firstSeen.Add(time.Second).UnixMilli()},
	}

	require.NoError(t, h.runRound())

	active := h.tier1Active()
	require.NotNil(t, active)
	require.NoError(t, active.Validate())
	require.Len(t, active.Entries, initialSlicesPerPartition)
	for _, entry := range active.Entries {
		assert.Equal(t, "tenant-new", entry.TenantID)
		assert.Equal(t, int32(0), entry.PartitionID)
	}
	require.Len(t, rc.scopedOwned[0]["tenant-new"], initialSlicesPerPartition, "bootstrap must reach partition 0's concrete owner immediately")
	require.Len(t, h.r.store.snapshot(), initialSlicesPerPartition)
	for _, entry := range h.r.store.snapshot() {
		assert.True(t, firstSeen.Equal(entry.From))
	}

	require.NoError(t, h.runRound())
	assert.Len(t, h.r.store.snapshot(), initialSlicesPerPartition, "repeated unknown reports must not duplicate placement history")
}

func TestPushAndReconstruct_PreserveTenantIDs(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	rc0 := h.addReadcache("readcache-0")
	rc1 := h.addReadcache("readcache-1")
	now := h.clock.Now()
	h.r.readcacheStore.apply(now, &readcacheassignment.Assignment{Entries: []readcacheassignment.AssignmentEntry{
		{PartitionID: 0, InstanceID: rc0.id},
		{PartitionID: 1, InstanceID: rc1.id},
	}}, h.cfg.LeaseDuration, h.r.readcacheLeaseLookahead(), h.cfg.EntryRetention, 0)

	mid := uint32(math.MaxUint32 / 2)
	want := &assignment.Assignment{Entries: []assignment.Entry{
		{TenantID: "tenant-a", Range: assignment.HashRange{Lo: 0, Hi: mid}, PartitionID: 0},
		{TenantID: "tenant-a", Range: assignment.HashRange{Lo: mid + 1, Hi: math.MaxUint32}, PartitionID: 1},
		{TenantID: "tenant-b", Range: assignment.HashRange{Lo: 0, Hi: mid}, PartitionID: 1},
		{TenantID: "tenant-b", Range: assignment.HashRange{Lo: mid + 1, Hi: math.MaxUint32}, PartitionID: 0},
	}}

	h.r.pushRangesToReadcache(h.ctx, want, now)
	require.Contains(t, rc0.scopedOwned[0], "tenant-a")
	require.Contains(t, rc0.scopedOwned[0], "tenant-b")
	require.Contains(t, rc1.scopedOwned[1], "tenant-a")
	require.Contains(t, rc1.scopedOwned[1], "tenant-b")

	got := h.r.reconstructAssignmentFromReadcache(h.ctx, []int32{0, 1})
	require.NotNil(t, got)
	require.NoError(t, got.Validate())
	sort.Slice(got.Entries, func(i, j int) bool {
		if got.Entries[i].TenantID != got.Entries[j].TenantID {
			return got.Entries[i].TenantID < got.Entries[j].TenantID
		}
		return got.Entries[i].Range.Lo < got.Entries[j].Range.Lo
	})
	assert.Equal(t, want.Entries, got.Entries)
}

func TestCooldownIndex_IsTenantScoped(t *testing.T) {
	now := time.Now()
	hr := assignment.HashRange{Lo: 100, Hi: 200}
	idx := newCooldownIndex(now, map[tenantRangeKey]time.Time{
		{tenantID: "tenant-a", hr: hr}: now.Add(time.Minute),
	})

	assert.True(t, idx.overlaps("tenant-a", hr))
	assert.False(t, idx.overlaps("tenant-b", hr))
}
