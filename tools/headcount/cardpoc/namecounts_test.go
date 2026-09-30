// SPDX-License-Identifier: AGPL-3.0-only

package cardpoc

import (
	"os"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/tools/headcount/cardgen"
	"github.com/grafana/mimir/tools/headcount/model"
)

func nameCountsTestPopulation(t *testing.T) *model.Model {
	pop, err := model.New(model.Config{
		Seed:        1,
		Start:       time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC),
		End:         time.Date(2026, 9, 20, 3, 0, 0, 0, time.UTC),
		MetricNames: 20,
		SeriesZipfS: 1.2, SeriesFloor: 1, SeriesCap: 40,
		ChurnFraction: 0.2, ChurnPeriod: time.Hour,
		SpikeMetric: -1,
	})
	require.NoError(t, err)
	return pop
}

func TestNameCountsForRange_ExactPerNameOnSingleBlockRanges(t *testing.T) {
	pop := nameCountsTestPopulation(t)
	bucket := t.TempDir()
	require.NoError(t, os.MkdirAll(bucket+"/anonymous", 0o755))
	_, err := cardgen.Generate(pop, bucket+"/anonymous", cardgen.Config{Partitions: 1, Seed: 1})
	require.NoError(t, err)

	ranges, err := BlockRanges(bucket)
	require.NoError(t, err)
	require.Len(t, ranges, 3, "one block per hour")

	for _, r := range ranges {
		nc, err := NameCountsForRange(bucket, r)
		require.NoError(t, err)
		require.Len(t, nc.Blocks, 1)
		require.Positive(t, nc.IndexHeaderBytes)
		require.Equal(t, pop.Truth(nil, r.MinT, r.MaxT), nc.Total())
		for name, got := range nc.Counts {
			m := labels.MustNewMatcher(labels.MatchEqual, "__name__", name)
			require.Equal(t, pop.Truth([]*labels.Matcher{m}, r.MinT, r.MaxT), got, "name %s in [%d, %d)", name, r.MinT, r.MaxT)
		}
	}
}

func TestNameCountsForRange_RejectsUnshardedBlocksForOneRange(t *testing.T) {
	pop := nameCountsTestPopulation(t)
	bucket := t.TempDir()
	require.NoError(t, os.MkdirAll(bucket+"/anonymous", 0o755))
	_, err := cardgen.Generate(pop, bucket+"/anonymous", cardgen.Config{Partitions: 3, Seed: 1})
	require.NoError(t, err)

	ranges, err := BlockRanges(bucket)
	require.NoError(t, err)
	_, err = NameCountsForRange(bucket, ranges[0])
	require.ErrorContains(t, err, "no shard ID")
}

func TestCheckDisjointShards(t *testing.T) {
	meta := func(shard string) *block.Meta {
		m := &block.Meta{}
		m.Thanos.Labels = map[string]string{}
		if shard != "" {
			m.Thanos.Labels[block.CompactorShardIDExternalLabel] = shard
		}
		return m
	}
	require.NoError(t, checkDisjointShards([]*block.Meta{meta("")}), "a single block needs no shard ID")
	require.NoError(t, checkDisjointShards([]*block.Meta{meta("1_of_2"), meta("2_of_2")}))
	require.ErrorContains(t, checkDisjointShards([]*block.Meta{meta("1_of_2"), meta("1_of_2")}), "appears twice")
	require.ErrorContains(t, checkDisjointShards([]*block.Meta{meta("1_of_2"), meta("2_of_4")}), "shard counts")
	require.ErrorContains(t, checkDisjointShards([]*block.Meta{meta("1_of_2"), meta("")}), "no shard ID")
}
