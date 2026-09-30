// SPDX-License-Identifier: AGPL-3.0-only

package cardpoc

import (
	"os"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/tools/headcount/cardgen"
	"github.com/grafana/mimir/tools/headcount/model"
)

// id and srcs build tiny, distinct ULIDs for hand-written Block fixtures,
// where the exact ID value doesn't matter, only that different blocks
// have different ones.
func id(n byte) ulid.ULID {
	var u ulid.ULID
	u[15] = n
	return u
}

func srcs(ns ...byte) []ulid.ULID {
	out := make([]ulid.ULID, len(ns))
	for i, n := range ns {
		out[i] = id(n)
	}
	return out
}

// testPopulation returns a small population where most series span
// several hours, so an unhandled union across cardgen's hourly blocks
// would visibly overcount.
func testPopulation(t *testing.T) *model.Model {
	pop, err := model.New(model.Config{
		Seed:        1,
		Start:       time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC),
		End:         time.Date(2026, 9, 20, 6, 0, 0, 0, time.UTC), // 6 hours.
		MetricNames: 20,
		SeriesZipfS: 1.2, SeriesFloor: 1, SeriesCap: 30,
		ChurnFraction: 0.2, ChurnPeriod: 2 * time.Hour,
		GapFraction: 0.1, GapDuration: time.Hour,
		StaleFraction: 0.1,
		SpikeMetric:   -1,
	})
	require.NoError(t, err)
	return pop
}

func loadL1Blocks(t *testing.T, pop *model.Model, partitions int) []Block {
	bucket := t.TempDir()
	require.NoError(t, os.MkdirAll(bucket+"/anonymous", 0o755))
	_, err := cardgen.Generate(pop, bucket+"/anonymous", cardgen.Config{Partitions: partitions, Seed: 1})
	require.NoError(t, err)

	blocks, err := LoadBlocks(bucket)
	require.NoError(t, err)
	require.NotEmpty(t, blocks)
	return blocks
}

func TestLoadBlocks_SeriesByNameSumsToTotal(t *testing.T) {
	blocks := loadL1Blocks(t, testPopulation(t), 3)
	for _, b := range blocks {
		var n int
		for _, hashes := range b.SeriesByName {
			n += len(hashes)
		}
		require.Equal(t, n, b.TotalSeries())
	}
}

func TestSumAll_OvercountsAcrossHourlyBlocks(t *testing.T) {
	pop := testPopulation(t)
	blocks := loadL1Blocks(t, pop, 3)

	truth := pop.Truth(nil, pop.Config().Start.UnixMilli(), pop.Config().End.UnixMilli())
	require.Greater(t, SumAll(blocks), truth, "most series span several of cardgen's hourly blocks, so the naive sum must overcount")
}

func TestHashUnion_ExactRegardlessOfOverlap(t *testing.T) {
	pop := testPopulation(t)
	blocks := loadL1Blocks(t, pop, 3)

	truth := pop.Truth(nil, pop.Config().Start.UnixMilli(), pop.Config().End.UnixMilli())
	require.Equal(t, truth, HashUnion(blocks))
}

func TestSumSkipSources_LevelOneBlocksNeverSkipThemselves(t *testing.T) {
	// A level-1 block's Compaction.Sources is itself, which must not be
	// read as "already superseded".
	blocks := loadL1Blocks(t, testPopulation(t), 3)
	require.Equal(t, SumAll(blocks), SumSkipSources(blocks), "no level-1 block should be skipped: only real compaction output supersedes anything")
}

func TestSumSkipSources_SkipsSupersededSources(t *testing.T) {
	blocks := []Block{
		{ID: id(1), Level: 1, Sources: srcs(1), SeriesByName: map[string][]uint64{"m": {1, 2}}},
		{ID: id(2), Level: 1, Sources: srcs(2), SeriesByName: map[string][]uint64{"m": {3}}},
		{ID: id(3), Level: 4, Sources: srcs(1, 2), SeriesByName: map[string][]uint64{"m": {1, 2, 3}}},
	}
	// Blocks 1 and 2's source-sets ({1}, {2}) are each contained in
	// block 3's ({1, 2}), so a merge that skips superseded blocks should
	// count only block 3's series.
	require.Equal(t, 3, SumSkipSources(blocks))
	require.Equal(t, 6, SumAll(blocks), "sanity: without the skip, all three blocks' series are counted")
}

// Regression test: real compactor output lists Compaction.Sources as the
// transitive set of original level-1 ancestor IDs, not each intermediate
// block's own ID, so an intermediate block's ID is never literally listed
// anywhere. A version of SumSkipSources that only checked ID membership
// (is this block's own ID in anyone's Sources) passed every hand-written
// fixture in this file, all built with a single compaction step, and
// still overcounted more than 50x against real multi-level compactor
// output before this test (and the fix) existed.
func TestSumSkipSources_SkipsIntermediateLevelsByLineage(t *testing.T) {
	blocks := []Block{
		{ID: id(1), Level: 1, Sources: srcs(1), SeriesByName: map[string][]uint64{"m": {1}}},
		{ID: id(2), Level: 1, Sources: srcs(2), SeriesByName: map[string][]uint64{"m": {2}}},
		{ID: id(3), Level: 1, Sources: srcs(3), SeriesByName: map[string][]uint64{"m": {3}}},
		{ID: id(4), Level: 1, Sources: srcs(4), SeriesByName: map[string][]uint64{"m": {4}}},
		// An intermediate merge of blocks 1 and 2: its own ID (5) is
		// never listed anywhere, only its lineage {1, 2} is, once it in
		// turn gets absorbed into block 6.
		{ID: id(5), Level: 2, Sources: srcs(1, 2), SeriesByName: map[string][]uint64{"m": {1, 2}}},
		// The final merge: its Sources is the full transitive ancestry
		// (1, 2, 3, 4), not the immediate parents (3, 4, 5).
		{ID: id(6), Level: 3, Sources: srcs(1, 2, 3, 4), SeriesByName: map[string][]uint64{"m": {1, 2, 3, 4}}},
	}
	require.Equal(t, 4, SumSkipSources(blocks), "only block 6 (the final merge) should survive")
}

// TestSumSkipSources_MatchesFullyCompactedSelection checks the property
// E4 actually relies on: source-dedup on a handover-shaped block set
// (superseded blocks still present) selects the same blocks -- and so
// gives the same sum -- as SumAll would on the equivalent snapshot with
// those superseded blocks already deleted. It deliberately does not
// check source-dedup against the population's ground truth: on a window
// spanning more than one final block, even the fully compacted sum can
// still overcount a series that lives in more than one of them (that's
// E2's finding, not E4's), so matching the compacted sum is the right
// bar here, not matching truth.
func TestSumSkipSources_MatchesFullyCompactedSelection(t *testing.T) {
	l1a := Block{ID: id(1), Level: 1, Sources: srcs(1), SeriesByName: map[string][]uint64{"m": {1}}}
	l1b := Block{ID: id(2), Level: 1, Sources: srcs(2), SeriesByName: map[string][]uint64{"m": {2}}}
	final := Block{ID: id(3), Level: 2, Sources: srcs(1, 2), SeriesByName: map[string][]uint64{"m": {1, 2}}}

	handover := []Block{l1a, l1b, final} // sources still present.
	compacted := []Block{final}          // sources already deleted.

	require.Equal(t, SumAll(compacted), SumSkipSources(handover))
}

func TestSumSkipSources_TiedLineageKeepsExactlyOne(t *testing.T) {
	// Two blocks with identical source-sets (an overlapping or retried
	// compaction can produce this): each must not supersede the other
	// into nothing, but exactly one of them must still survive.
	blocks := []Block{
		{ID: id(1), Level: 4, Sources: srcs(10, 11), SeriesByName: map[string][]uint64{"m": {1, 2}}},
		{ID: id(2), Level: 4, Sources: srcs(10, 11), SeriesByName: map[string][]uint64{"m": {1, 2}}},
	}
	require.Equal(t, 2, SumSkipSources(blocks), "exactly one of the tied blocks must survive")
}

func TestSumMinLevel_DropsBelowThreshold(t *testing.T) {
	blocks := []Block{
		{ID: id(1), Level: 1, SeriesByName: map[string][]uint64{"m": {1, 2}}},
		{ID: id(2), Level: 2, SeriesByName: map[string][]uint64{"m": {3, 4, 5}}},
	}
	require.Equal(t, 3, SumMinLevel(blocks, 2))
	require.Equal(t, 5, SumMinLevel(blocks, 1))
	require.Equal(t, 0, SumMinLevel(blocks, 3))
}
