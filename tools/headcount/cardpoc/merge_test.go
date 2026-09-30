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
	// Blocks 1 and 2 are listed as sources of block 3, so a merge that
	// skips superseded sources should count only block 3's series.
	require.Equal(t, 3, SumSkipSources(blocks))
	require.Equal(t, 6, SumAll(blocks), "sanity: without the skip, all three blocks' series are counted")
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
