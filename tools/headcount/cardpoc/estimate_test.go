// SPDX-License-Identifier: AGPL-3.0-only

package cardpoc

import (
	"os"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/tools/headcount/cardgen"
	"github.com/grafana/mimir/tools/headcount/model"
)

func TestCardinalityEstimate(t *testing.T) {
	start := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	pop, err := model.New(model.Config{
		Seed:        1,
		Start:       start,
		End:         start.Add(3 * time.Hour),
		MetricNames: 20,
		SeriesZipfS: 1.2, SeriesFloor: 1, SeriesCap: 30,
		ChurnFraction: 0.5, ChurnPeriod: time.Hour,
		SpikeMetric: 0, SpikeDay: 0, SpikeStartHour: 1, SpikeHours: 1, SpikeBaseValues: 2, SpikePeakValues: 40,
	})
	require.NoError(t, err)
	bucket := t.TempDir()
	require.NoError(t, os.MkdirAll(bucket+"/anonymous", 0o755))
	_, err = cardgen.Generate(pop, bucket+"/anonymous", cardgen.Config{Partitions: 1, Seed: 1})
	require.NoError(t, err)

	ranges, err := BlockRanges(bucket)
	require.NoError(t, err)
	require.Len(t, ranges, 3, "one block range per hour")
	r0, r1 := ranges[0], ranges[1]
	spike := pop.Series[0].Labels.Get("__name__")
	bySpike := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", spike)}

	flat := func(e Estimate) map[string]int {
		out := map[string]int{}
		for v, c := range e.Counts {
			out[v] = c[0]
		}
		return out
	}

	t.Run("every name over one block range reads index-headers", func(t *testing.T) {
		e, err := CardinalityEstimate(bucket, CardinalityEstimateRequest{MinT: r1.MinT, MaxT: r1.MaxT})
		require.NoError(t, err)
		assert.Equal(t, ReadIndexHeader, e.Read)
		assert.Equal(t, pop.TruthBy(nil, "__name__", r1.MinT, r1.MaxT), flat(e))
	})

	t.Run("a matcher and another label read the full index", func(t *testing.T) {
		e, err := CardinalityEstimate(bucket, CardinalityEstimateRequest{Matchers: bySpike, MinT: r1.MinT, MaxT: r1.MaxT, GroupBy: "pod"})
		require.NoError(t, err)
		assert.Equal(t, ReadFullIndex, e.Read)
		assert.False(t, e.Dedup)
		assert.Equal(t, pop.TruthBy(bySpike, "pod", r1.MinT, r1.MaxT), flat(e))
	})

	t.Run("across block ranges it dedups", func(t *testing.T) {
		e, err := CardinalityEstimate(bucket, CardinalityEstimateRequest{MinT: r0.MinT, MaxT: r1.MaxT})
		require.NoError(t, err)
		assert.Equal(t, ReadFullIndex, e.Read)
		assert.True(t, e.Dedup)
		assert.Equal(t, pop.TruthBy(nil, "__name__", r0.MinT, r1.MaxT), flat(e))

		_, err = CardinalityEstimate(bucket, CardinalityEstimateRequest{MinT: r0.MinT, MaxT: r1.MaxT, Step: r0.MaxT - r0.MinT})
		require.ErrorContains(t, err, "inside one block range")
	})

	t.Run("buckets inside one block range", func(t *testing.T) {
		e, err := CardinalityEstimate(bucket, CardinalityEstimateRequest{Matchers: bySpike, MinT: r1.MinT, MaxT: r1.MaxT, Step: r1.MaxT - r1.MinT})
		require.NoError(t, err)
		assert.Equal(t, []int{pop.Truth(bySpike, r1.MinT, r1.MaxT)}, e.Counts[spike])
	})

	t.Run("snap widens a window to its block range", func(t *testing.T) {
		e, err := CardinalityEstimate(bucket, CardinalityEstimateRequest{MinT: r1.MinT, MaxT: r1.MinT + 10*60*1000, Snap: true})
		require.NoError(t, err)
		assert.True(t, e.Snapped)
		assert.Equal(t, ReadIndexHeader, e.Read)
		assert.Equal(t, r1.MaxT, e.MaxT)

		_, err = CardinalityEstimate(bucket, CardinalityEstimateRequest{MinT: r0.MaxT - 1, MaxT: r1.MinT + 1, Snap: true})
		require.ErrorContains(t, err, "more than one block range")
	})

	t.Run("the budget gives a lower bound", func(t *testing.T) {
		e, err := CardinalityEstimate(bucket, CardinalityEstimateRequest{Matchers: bySpike, MinT: r1.MinT, MaxT: r1.MaxT, GroupBy: "pod", MaxSeries: 5})
		require.NoError(t, err)
		assert.True(t, e.LowerBound)
		assert.Equal(t, 5, e.SeriesRead)
		truth := pop.TruthBy(bySpike, "pod", r1.MinT, r1.MaxT)
		for v, n := range flat(e) {
			assert.LessOrEqual(t, n, truth[v])
		}
	})
}

func TestCardinalityEstimate_BlocksThatMayShareSeries(t *testing.T) {
	start := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	pop, err := model.New(model.Config{
		Seed: 1, Start: start, End: start.Add(time.Hour), MetricNames: 10,
		SeriesZipfS: 1.2, SeriesFloor: 1, SeriesCap: 20, SpikeMetric: -1,
	})
	require.NoError(t, err)
	bucket := t.TempDir()
	require.NoError(t, os.MkdirAll(bucket+"/anonymous", 0o755))
	// Two partitions give two level-1 blocks for the hour, with no shard IDs
	// to prove they share no series.
	_, err = cardgen.Generate(pop, bucket+"/anonymous", cardgen.Config{Partitions: 2, Seed: 1})
	require.NoError(t, err)
	ranges, err := BlockRanges(bucket)
	require.NoError(t, err)
	require.Len(t, ranges, 1)

	e, err := CardinalityEstimate(bucket, CardinalityEstimateRequest{MinT: ranges[0].MinT, MaxT: ranges[0].MaxT})
	require.NoError(t, err)
	assert.Equal(t, ReadFullIndex, e.Read, "index-header counts can't be added for these blocks")
	assert.True(t, e.Dedup)
	assert.Equal(t, 2, e.Blocks)
	total := 0
	for _, c := range e.Counts {
		total += c[0]
	}
	assert.Equal(t, pop.Truth(nil, ranges[0].MinT, ranges[0].MaxT), total)
}
