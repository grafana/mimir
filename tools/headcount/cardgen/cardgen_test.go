// SPDX-License-Identifier: AGPL-3.0-only

package cardgen

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/tools/headcount/model"
)

func testPopulation(t *testing.T) *model.Model {
	m, err := model.New(model.Config{
		Seed:        1,
		Start:       time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC),
		End:         time.Date(2026, 9, 20, 4, 0, 0, 0, time.UTC), // 4 hours
		MetricNames: 20,
		SeriesZipfS: 1.2,
		SeriesFloor: 1,
		SeriesCap:   50,
		SpikeMetric: -1,
	})
	require.NoError(t, err)
	return m
}

func TestGenerate_SeriesCountMatchesTruth(t *testing.T) {
	pop := testPopulation(t)
	dir := t.TempDir()

	metas, err := Generate(pop, dir, Config{Partitions: 3, Seed: 7})
	require.NoError(t, err)
	require.NotEmpty(t, metas)

	hourMs := time.Hour.Milliseconds()
	var totalFromBlocks int
	for _, meta := range metas {
		totalFromBlocks += int(meta.Stats.NumSeries)
		// Every block must fall within one hour, matching the block-builder
		// layout the compactor expects downstream.
		require.LessOrEqual(t, meta.MaxTime-meta.MinTime, hourMs)
	}

	// Sum truth per hour rather than over the whole range: a series live in
	// two consecutive hours is one distinct series overall but appears in
	// two different (partition, hour) blocks, so the per-hour sum is what
	// should match the block count, not the whole-range distinct count.
	var totalTruth int
	for h := pop.Config().Start.UnixMilli(); h < pop.Config().End.UnixMilli(); h += hourMs {
		totalTruth += pop.Truth(nil, h, h+hourMs)
	}
	require.Equal(t, totalTruth, totalFromBlocks)
}

func TestGenerate_DeterministicBlockIDs(t *testing.T) {
	pop := testPopulation(t)
	cfg := Config{Partitions: 4, Seed: 7}

	metasA, err := Generate(pop, t.TempDir(), cfg)
	require.NoError(t, err)
	metasB, err := Generate(pop, t.TempDir(), cfg)
	require.NoError(t, err)

	require.Equal(t, len(metasA), len(metasB))
	for i := range metasA {
		require.Equal(t, metasA[i].ULID, metasB[i].ULID, "same seed must produce the same block IDs")
	}
}

// TestGenerate_NoSeriesSplitWithinAnHour checks that a series live in a given
// hour appears in exactly one of that hour's partition blocks, which is what
// makes summing partition counts for an hour exact.
func TestGenerate_NoSeriesSplitWithinAnHour(t *testing.T) {
	pop := testPopulation(t)
	dir := t.TempDir()

	metas, err := Generate(pop, dir, Config{Partitions: 4, Seed: 7})
	require.NoError(t, err)

	byHour := map[int64]map[string]bool{}
	for _, meta := range metas {
		seen := byHour[meta.MinTime]
		if seen == nil {
			seen = map[string]bool{}
			byHour[meta.MinTime] = seen
		}

		r, err := index.NewFileReader(blockDir(dir, meta.ULID)+"/index", index.DecodePostingsRaw)
		require.NoError(t, err)
		p, err := r.Postings(t.Context(), "", "")
		require.NoError(t, err)

		var b labels.ScratchBuilder
		for p.Next() {
			require.NoError(t, r.Series(p.At(), &b, nil))
			sig := b.Labels().String()
			require.False(t, seen[sig], "series %s appears in two partition blocks for the same hour", sig)
			seen[sig] = true
		}
		require.NoError(t, r.Close())
	}
}
