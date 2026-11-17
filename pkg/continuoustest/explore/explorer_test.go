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
	e, err := newExplorer(1, def.dimensions(), exclusions())
	require.NoError(t, err)
	// 3 protocols x 2 sample types x 2 kinds x 13 schemas, minus the OTLP combinations with float
	// samples or gauges.
	require.Equal(t, int64(156-39), e.cycleLength())

	for cycle := range int64(3) {
		seen := map[string]bool{}
		for pos := range e.cycleLength() {
			combo := e.combination(cycle*e.cycleLength() + pos)
			require.True(t, e.allowedValues(combo), "cycle %d explores the excluded combination %v", cycle, combo)
			seen[strings.Join(combo, ",")] = true
		}
		require.Len(t, seen, 117, "cycle %d should explore every allowed combination exactly once", cycle)
	}
}

func TestExplorer_Deterministic(t *testing.T) {
	dims := testDimensions(3, 4, 13)
	const slots = 200
	forward, backward := testExplorer(t, 42, dims), testExplorer(t, 42, dims)
	other := testExplorer(t, 43, dims)

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
	e := testExplorer(t, 1, testDimensions(2, 2, 13))
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
		e := testExplorer(t, seed, dims)
		require.Less(t, e.cycleLength(), int64(100), "pairwise coverage should need far fewer rows than the 7776 combinations")

		for cycle := range int64(2) {
			require.Len(t, coveredPairs(e, cycle), 10*36, "seed %d cycle %d should cover all pairs", seed, cycle)
		}
	}
}

func TestExplorer_PairwiseCoverageWithExclusions(t *testing.T) {
	dims := testDimensions(6, 6, 6, 6, 6)
	excl := []exclusion{
		{dim1: "d0", value1: "v0", dim2: "d1", value2: "v0"},
		{dim1: "d3", value1: "v5", dim2: "d0", value2: "v0"},
		{dim1: "d0", value1: "v0", dim2: "d9", value2: "v0"}, // Unknown dimensions are ignored.
	}
	for _, seed := range []uint64{1, 2, 3} {
		e, err := newExplorer(seed, dims, excl)
		require.NoError(t, err)
		for cycle := range int64(2) {
			covered := coveredPairs(e, cycle)
			require.Len(t, covered, 10*36-2, "seed %d cycle %d should cover all pairs but the excluded ones", seed, cycle)
			require.NotContains(t, covered, "0=v0,1=v0")
			require.NotContains(t, covered, "0=v0,3=v5")
		}
	}
}

func TestExplorer_EverythingExcluded(t *testing.T) {
	_, err := newExplorer(1, testDimensions(1, 2), []exclusion{
		{dim1: "d0", value1: "v0", dim2: "d1", value2: "v0"},
		{dim1: "d0", value1: "v0", dim2: "d1", value2: "v1"},
	})
	require.ErrorContains(t, err, "every combination of dimension values is excluded")
}

func TestExplorer_NegativeSlot(t *testing.T) {
	e := testExplorer(t, 1, testDimensions(2, 3))
	require.Len(t, e.combination(-1), 2)
	require.Equal(t, e.combination(-1), e.combination(-1))
}

func testExplorer(t *testing.T, seed uint64, dims []dimension) *explorer {
	e, err := newExplorer(seed, dims, nil)
	require.NoError(t, err)
	return e
}

func coveredPairs(e *explorer, cycle int64) map[string]bool {
	covered := map[string]bool{}
	for pos := range e.cycleLength() {
		combo := e.combination(cycle*e.cycleLength() + pos)
		for d1 := range combo {
			for d2 := d1 + 1; d2 < len(combo); d2++ {
				covered[fmt.Sprintf("%d=%s,%d=%s", d1, combo[d1], d2, combo[d2])] = true
			}
		}
	}
	return covered
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
