// SPDX-License-Identifier: AGPL-3.0-only

package explore

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExplorer_DefaultDefinitionIsExhaustive(t *testing.T) {
	def, err := LoadDefinition("")
	require.NoError(t, err)
	e := newExplorer(1, def.NativeHistograms.dimensions())
	require.Equal(t, int64(52), e.cycleLength())

	for cycle := range int64(3) {
		seen := map[string]bool{}
		for pos := range e.cycleLength() {
			seen[strings.Join(e.combination(cycle*e.cycleLength()+pos), ",")] = true
		}
		require.Len(t, seen, 52, "cycle %d should explore every combination exactly once", cycle)
	}
}

func TestExplorer_Deterministic(t *testing.T) {
	dims := testDimensions(3, 4, 13)
	const slots = 200
	forward, backward := newExplorer(42, dims), newExplorer(42, dims)
	other := newExplorer(43, dims)

	// Querying slots in a different order, as happens across restarts, must not change the result.
	backwardCombinations := make([][]string, slots)
	for slot := int64(slots - 1); slot >= 0; slot-- {
		backwardCombinations[slot] = backward.combination(slot)
	}
	differs := false
	for slot := int64(0); slot < slots; slot++ {
		require.Equal(t, backwardCombinations[slot], forward.combination(slot))
		if !equalStrings(forward.combination(slot), other.combination(slot)) {
			differs = true
		}
	}
	require.True(t, differs, "different seeds should produce different schedules")
}

func TestExplorer_CyclesDiffer(t *testing.T) {
	e := newExplorer(1, testDimensions(2, 2, 13))
	var cycles []string
	for cycle := range int64(2) {
		var order []string
		for pos := range e.cycleLength() {
			order = append(order, strings.Join(e.combination(cycle*e.cycleLength()+pos), ","))
		}
		cycles = append(cycles, strings.Join(order, ";"))
	}
	require.NotEqual(t, cycles[0], cycles[1])
}

func TestExplorer_PairwiseCoverage(t *testing.T) {
	// 6^5 combinations is above exhaustiveLimit, so the explorer only covers pairs.
	dims := testDimensions(6, 6, 6, 6, 6)
	for _, seed := range []uint64{1, 2, 3} {
		e := newExplorer(seed, dims)
		require.Less(t, e.cycleLength(), int64(100), "pairwise coverage should need far fewer rows than the 7776 combinations")

		for cycle := range int64(2) {
			covered := map[string]bool{}
			for pos := range e.cycleLength() {
				combo := e.combination(cycle*e.cycleLength() + pos)
				for d1 := range combo {
					for d2 := d1 + 1; d2 < len(combo); d2++ {
						covered[fmt.Sprintf("%d=%s,%d=%s", d1, combo[d1], d2, combo[d2])] = true
					}
				}
			}
			require.Len(t, covered, 10*36, "seed %d cycle %d should cover all pairs", seed, cycle)
		}
	}
}

func TestExplorer_NegativeSlot(t *testing.T) {
	e := newExplorer(1, testDimensions(2, 3))
	require.Len(t, e.combination(-1), 2)
	require.Equal(t, e.combination(-1), e.combination(-1))
}

func testDimensions(sizes ...int) []dimension {
	dims := make([]dimension, len(sizes))
	for d, size := range sizes {
		dims[d].name = fmt.Sprintf("d%d", d)
		for v := range size {
			dims[d].values = append(dims[d].values, fmt.Sprintf("v%d", v))
		}
	}
	return dims
}

func equalStrings(a, b []string) bool {
	return strings.Join(a, ",") == strings.Join(b, ",")
}
