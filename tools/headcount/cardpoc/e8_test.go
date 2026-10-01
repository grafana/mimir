// SPDX-License-Identifier: AGPL-3.0-only

package cardpoc

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/tools/headcount/cardgen"
	"github.com/grafana/mimir/tools/headcount/model"
)

func TestRunE8_BucketsExactWithoutInChunkGaps(t *testing.T) {
	start := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	pop, err := model.New(model.Config{
		Seed:        1,
		Start:       start,
		End:         start.Add(2 * time.Hour),
		MetricNames: 20,
		SeriesZipfS: 1.2, SeriesFloor: 1, SeriesCap: 30,
		// Churn every 20 minutes and stale series start and end series
		// inside an hour, so the 15-minute buckets differ from each other.
		ChurnFraction: 0.5, ChurnPeriod: 20 * time.Minute,
		StaleFraction: 0.3,
		SpikeMetric:   0, SpikeDay: 0, SpikeStartHour: 1, SpikeHours: 1, SpikeBaseValues: 2, SpikePeakValues: 40,
	})
	require.NoError(t, err)
	bucket := t.TempDir()
	require.NoError(t, os.MkdirAll(bucket+"/anonymous", 0o755))
	_, err = cardgen.Generate(pop, bucket+"/anonymous", cardgen.Config{Partitions: 1, Seed: 1})
	require.NoError(t, err)

	ranges, err := BlockRanges(bucket)
	require.NoError(t, err)
	require.Len(t, ranges, 2)
	spike := pop.Series[0].Labels.Get("__name__")

	for _, r := range ranges {
		for _, metric := range []string{"", spike} {
			res, err := RunE8(bucket, pop, r, 15*time.Minute, metric)
			require.NoError(t, err)
			require.Len(t, res.Counts, 4)
			require.True(t, res.Pass(), "range %v metric %q: counts %v truth %v", r, metric, res.Counts, res.Truth)
		}
	}

	before, err := RunE8(bucket, pop, ranges[0], 15*time.Minute, spike)
	require.NoError(t, err)
	during, err := RunE8(bucket, pop, ranges[1], 15*time.Minute, spike)
	require.NoError(t, err)
	require.Equal(t, 2, before.Counts[0], "before the spike only the base values exist")
	require.Equal(t, 40, during.Counts[0], "the spike starts with every peak value")

	_, err = CountBuckets(bucket, ranges[0], 25*time.Minute, "")
	require.ErrorContains(t, err, "must divide")
}

func TestRunE8_InChunkGapCountsAsPresent(t *testing.T) {
	start := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	pop, err := model.New(model.Config{
		Seed:        1,
		Start:       start,
		End:         start.Add(3 * time.Hour),
		MetricNames: 10,
		SeriesZipfS: 1.2, SeriesFloor: 1, SeriesCap: 20,
		// A 30-minute gap can sit inside one hourly chunk, which spans it.
		GapFraction: 1, GapDuration: 30 * time.Minute,
		SpikeMetric: -1,
	})
	require.NoError(t, err)
	bucket := t.TempDir()
	require.NoError(t, os.MkdirAll(bucket+"/anonymous", 0o755))
	_, err = cardgen.Generate(pop, bucket+"/anonymous", cardgen.Config{Partitions: 1, Seed: 1})
	require.NoError(t, err)

	ranges, err := BlockRanges(bucket)
	require.NoError(t, err)
	over := 0
	for _, r := range ranges {
		hourly, err := RunE8(bucket, pop, r, time.Hour, "")
		require.NoError(t, err)
		require.True(t, hourly.Pass(), "hour buckets match the chunks: %v vs %v", hourly.Counts, hourly.Truth)

		fine, err := RunE8(bucket, pop, r, 10*time.Minute, "")
		require.NoError(t, err)
		for i := range fine.Counts {
			require.GreaterOrEqual(t, fine.Counts[i], fine.Truth[i], "chunk metas never undercount")
			over += fine.Counts[i] - fine.Truth[i]
		}
	}
	require.Positive(t, over, "a bucket inside a gap must be counted from the chunk that spans it")
}
