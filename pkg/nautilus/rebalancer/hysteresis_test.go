// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

func TestRunPhase3_LoadHysteresisKeepsBalancedRoundQuiet(t *testing.T) {
	entries := []rangeLoad{
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 0}, load: 4},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 100, Hi: 199}, PartitionID: 0}, load: 100},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 200, Hi: math.MaxUint32}, PartitionID: 1}, load: 96},
	}
	r := &Rebalancer{cfg: Config{MovementBudget: 1, LoadHysteresis: 0.05}}

	actions := r.runPhase3(entries, map[int32]float64{0: 104, 1: 96}, []int32{0, 1}, nil, time.Time{})

	assert.Empty(t, actions, "loads inside the ±5% no-op band must not trigger a move")
}

func TestRunSlicer_LoadHysteresisKeepsEntireBalancedRoundQuiet(t *testing.T) {
	partitions := []int32{0, 1, 2, 3}
	current := assignment.FineEvenSplit(partitions, initialSlicesPerPartition)
	rates := make([]rangeRate, 0, len(current.Entries))
	partitionRates := map[int32]float64{}
	for _, entry := range current.Entries {
		rates = append(rates, rangeRate{hr: entry.Range, partitionID: entry.PartitionID, sampleRate: 100})
		partitionRates[entry.PartitionID] += 100
	}
	r := &Rebalancer{cfg: Config{
		MovementBudget:     1,
		LoadHysteresis:     0.05,
		StructuralCooldown: time.Minute,
	}}

	_, actions := r.runSlicer(current, rates, partitionRates, partitions, nil, time.Unix(1000, 0))

	assert.Empty(t, actions, "a balanced round must produce no moves, merges, or splits")
}

func TestRunPhase3_LoadHysteresisStillCorrectsRealImbalance(t *testing.T) {
	entries := []rangeLoad{
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 0}, load: 10},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 100, Hi: 199}, PartitionID: 0}, load: 110},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 200, Hi: math.MaxUint32}, PartitionID: 1}, load: 80},
	}
	r := &Rebalancer{cfg: Config{MovementBudget: 1, LoadHysteresis: 0.05}}

	actions := r.runPhase3(entries, map[int32]float64{0: 120, 1: 80}, []int32{0, 1}, nil, time.Time{})

	require.NotEmpty(t, actions)
	assert.Equal(t, int32(0), actions[0].FromPart)
	assert.Equal(t, int32(1), actions[0].ToPart)
}

func TestRunPhase3_PartitionRoleCooldownBlocksDirectionReversal(t *testing.T) {
	now := time.Unix(1000, 0)
	entries := []rangeLoad{
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 0}, load: 10},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 100, Hi: 199}, PartitionID: 0}, load: 110},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 200, Hi: math.MaxUint32}, PartitionID: 1}, load: 80},
	}
	r := &Rebalancer{
		cfg: Config{MovementBudget: 1, PartitionRoleCooldown: time.Minute},
		partitionRoles: partitionRoleCooldowns{
			recentDestinations: map[int32]time.Time{0: now.Add(time.Minute)},
			recentSources:      map[int32]time.Time{},
		},
	}

	actions := r.runPhase3(entries, map[int32]float64{0: 120, 1: 80}, []int32{0, 1}, nil, now)

	assert.Empty(t, actions, "a recent destination must not immediately reverse into a source")
}

func TestMergeAdjacentCold_StructuralCooldownBlocksMerge(t *testing.T) {
	entries := []rangeLoad{
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 0}, load: 0.1},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 100, Hi: 199}, PartitionID: 0}, load: 0.1},
	}
	blocked := func(hr assignment.HashRange) bool {
		return hashRangesOverlap(hr, assignment.HashRange{Lo: 0, Hi: 199})
	}

	result, actions := mergeAdjacentCold(entries, 1, math.MaxFloat64, 1, 1, 0, blocked)

	assert.Len(t, result, 2)
	assert.Empty(t, actions)
}

func TestRunSlicer_StructuralCooldownBlocksSplit(t *testing.T) {
	now := time.Unix(1000, 0)
	hotRange := assignment.HashRange{Lo: 0, Hi: 999}
	current := &assignment.Assignment{Entries: []assignment.Entry{
		{Range: hotRange, PartitionID: 0},
		{Range: assignment.HashRange{Lo: 1000, Hi: 1999}, PartitionID: 1},
		{Range: assignment.HashRange{Lo: 2000, Hi: math.MaxUint32}, PartitionID: 1},
	}}
	rates := []rangeRate{
		{hr: hotRange, partitionID: 0, sampleRate: 100},
		{hr: assignment.HashRange{Lo: 1000, Hi: 1999}, partitionID: 1, sampleRate: 1},
		{hr: assignment.HashRange{Lo: 2000, Hi: math.MaxUint32}, partitionID: 1, sampleRate: 1},
	}
	r := &Rebalancer{
		cfg:                 Config{StructuralCooldown: time.Minute},
		structuralCooldowns: map[assignment.HashRange]time.Time{hotRange: now.Add(time.Minute)},
	}

	_, actions := r.runSlicer(current, rates, map[int32]float64{0: 100, 1: 2}, []int32{0, 1}, nil, now)

	for _, action := range actions {
		assert.NotEqual(t, ActionSplit, action.Kind, "a structurally cooling range must not split")
	}
}
