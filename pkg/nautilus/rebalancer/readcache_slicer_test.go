// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlanReadcacheAssignment_ColdStartBalancesEvenly(t *testing.T) {
	cfg := ReadcacheSlicerConfig{
		Alpha:          1.0,
		Beta:           0.0,
		MovementBudget: 1.0, // Don't gate during cold start.
	}

	in := readcachePlanInput{
		partitions:      []int32{0, 1, 2, 3},
		loadByPartition: map[int32]float64{0: 100, 1: 100, 2: 100, 3: 100},
		instances:       []string{"rc-a", "rc-b"},
		currentOwner:    map[int32]string{},
		recentlyMoved:   map[int32]struct{}{},
	}
	plan := planReadcacheAssignment(cfg, in)

	require.Len(t, plan.Assignment.Entries, 4)
	require.Len(t, plan.LoadByInstance, 2)

	// Equal load means the two instances get 2 partitions each.
	for inst, l := range plan.LoadByInstance {
		assert.Equal(t, 200.0, l, "expected 200 load on %s", inst)
	}
}

func TestPlanReadcacheAssignment_PrefersCurrentOwner(t *testing.T) {
	cfg := ReadcacheSlicerConfig{Alpha: 1.0, MovementBudget: 0.01}

	in := readcachePlanInput{
		partitions:      []int32{0, 1, 2, 3},
		loadByPartition: map[int32]float64{0: 100, 1: 100, 2: 100, 3: 100},
		instances:       []string{"rc-a", "rc-b"},
		currentOwner: map[int32]string{
			0: "rc-a", 1: "rc-a", 2: "rc-b", 3: "rc-b",
		},
		recentlyMoved: map[int32]struct{}{},
	}
	plan := planReadcacheAssignment(cfg, in)
	require.Len(t, plan.Assignment.Entries, 4)

	mapping := map[int32]string{}
	for _, e := range plan.Assignment.Entries {
		mapping[e.PartitionID] = e.InstanceID
	}
	assert.Equal(t, "rc-a", mapping[0])
	assert.Equal(t, "rc-a", mapping[1])
	assert.Equal(t, "rc-b", mapping[2])
	assert.Equal(t, "rc-b", mapping[3])
	assert.Empty(t, plan.Moves, "steady-state should produce no moves")
}

func TestPlanReadcacheAssignment_MovesOverloadedPartition(t *testing.T) {
	cfg := ReadcacheSlicerConfig{Alpha: 1.0, MovementBudget: 0.5}

	in := readcachePlanInput{
		partitions: []int32{0, 1, 2, 3},
		loadByPartition: map[int32]float64{
			0: 100,
			1: 100,
			2: 50,
			3: 50,
		},
		instances: []string{"rc-a", "rc-b"},
		// rc-a owns everything; rc-b is empty. Total load 300, target
		// per instance 150 — moving one of the partitions to rc-b
		// should bring rc-a within budget.
		currentOwner: map[int32]string{
			0: "rc-a", 1: "rc-a", 2: "rc-a", 3: "rc-a",
		},
		recentlyMoved: map[int32]struct{}{},
	}
	plan := planReadcacheAssignment(cfg, in)
	require.NotEmpty(t, plan.Moves, "expected at least one move")

	assert.Less(t, plan.LoadByInstance["rc-a"], 250.0, "rc-a should have been relieved")
	assert.Greater(t, plan.LoadByInstance["rc-b"], 0.0, "rc-b should have received load")

	// Every move must carry a non-empty Reason so the admin UI's
	// "Recent Readcache Slicer Rounds" history can explain why each
	// partition was moved. Without this, operators chasing a bad
	// move have no way to distinguish "src was over target" from
	// "cooldown collision" by inspecting the trace alone.
	for _, m := range plan.Moves {
		assert.NotEmpty(t, m.Reason, "move %+v should record a reason", m)
		assert.Contains(t, m.Reason, m.From, "reason should mention src instance")
		assert.Contains(t, m.Reason, m.To, "reason should mention dst instance")
	}
}

func TestPlanReadcacheAssignment_RespectsMoveCooldown(t *testing.T) {
	cfg := ReadcacheSlicerConfig{Alpha: 1.0, MovementBudget: 1.0}

	in := readcachePlanInput{
		partitions:      []int32{0, 1, 2, 3},
		loadByPartition: map[int32]float64{0: 100, 1: 100, 2: 100, 3: 100},
		instances:       []string{"rc-a", "rc-b"},
		currentOwner: map[int32]string{
			0: "rc-a", 1: "rc-a", 2: "rc-a", 3: "rc-a",
		},
		// All partitions are cooling — none may move this round.
		recentlyMoved: map[int32]struct{}{0: {}, 1: {}, 2: {}, 3: {}},
	}
	plan := planReadcacheAssignment(cfg, in)
	assert.Empty(t, plan.Moves, "cooldowns should suppress all moves this round")
}

// TestRunReadcacheSlicer_AlphaWeightsSampleRate confirms that the
// second-tier slicer's load function uses partitionRateByPID (the
// samples-per-second EWMA) as the Alpha-weighted term — the v5
// contract — rather than the head-series count. Two readcache
// instances should each receive ~50% of the total write rate even
// though all four partitions have the same active-series count.
func TestRunReadcacheSlicer_AlphaWeightsSampleRate(t *testing.T) {
	r := &Rebalancer{
		cfg: Config{
			LeaseDuration:  5 * time.Minute,
			LeaseLookahead: 90 * time.Second,
			EntryRetention: 24 * time.Hour,
			ReadcacheSlicer: ReadcacheSlicerConfig{
				Enabled:        true,
				Alpha:          1.0, // weight only on write rate
				Beta:           0.0,
				MovementBudget: 1.0,
			},
		},
		logger:             log.NewNopLogger(),
		readcacheStore:     newReadcacheLogStore(),
		readcacheCooldowns: make(readcacheMoveCooldowns),
		metrics:            newMetrics(nil),
	}

	partitions := []int32{0, 1, 2, 3}
	// Partition 0 is a hot writer, partitions 1-3 are cold. If the
	// slicer mistakenly balanced on series (or on a zero vector
	// because we forgot to wire partitionRateByPID through), all
	// instances would end up with the same partition count regardless
	// of write rate, and the heavy partition could pile up alongside
	// other partitions on one instance.
	partitionRate := map[int32]float64{
		0: 9000.0,
		1: 1.0,
		2: 1.0,
		3: 1.0,
	}
	instances := []string{"rc-a", "rc-b"}

	r.runReadcacheSlicer(time.Unix(1_000_000, 0), partitions, partitionRate, nil /*query*/, instances)

	// After the round, the readcache store must hold the planned
	// assignment. Whichever instance owns partition 0 should be the
	// only owner of partition 0 — the other three partitions sit on
	// the other instance to keep total load balanced.
	hot := ""
	cold := ""
	owners := map[int32]string{}
	for _, entry := range r.readcacheStore.snapshot() {
		if !entry.ActiveAt(time.Unix(1_000_000, 0)) {
			continue
		}
		owners[entry.PartitionID] = entry.InstanceID
	}
	require.Len(t, owners, len(partitions), "every partition must be assigned")

	hot = owners[0]
	require.NotEmpty(t, hot, "partition 0 (the hot writer) must have an owner")
	for _, inst := range instances {
		if inst != hot {
			cold = inst
		}
	}
	require.NotEqual(t, hot, cold, "test setup must have two distinct instances")

	// The other partitions should land on the cold side, since each
	// hot-instance assignment of an extra partition would dwarf any
	// gain from co-location.
	for pid, inst := range owners {
		if pid == 0 {
			continue
		}
		assert.Equalf(t, cold, inst,
			"cold partition %d landed on %s; expected the non-hot instance %s "+
				"(slicer must respond to per-partition write rate, not partition count)",
			pid, inst, cold)
	}
}

func TestReadcacheMoveCooldowns(t *testing.T) {
	c := make(readcacheMoveCooldowns)
	now := time.Unix(1000, 0)

	c.extendForMoves(now, time.Minute, []readcacheMove{
		{PartitionID: 1},
		{PartitionID: 2},
	})

	cooling := c.stillCooling(now)
	assert.Len(t, cooling, 2)

	// Past the cooldown window, the partitions are no longer
	// cooling, but they're still in the map until prune runs.
	later := now.Add(2 * time.Minute)
	cooling = c.stillCooling(later)
	assert.Empty(t, cooling)

	c.prune(later)
	assert.Empty(t, map[int32]time.Time(c))
}
