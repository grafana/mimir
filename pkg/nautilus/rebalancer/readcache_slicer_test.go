// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"testing"
	"time"

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
