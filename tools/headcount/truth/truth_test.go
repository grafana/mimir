// SPDX-License-Identifier: AGPL-3.0-only

package truth

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/tools/headcount/cardgen"
	"github.com/grafana/mimir/tools/headcount/model"
)

func TestCountDistinctSeries_MatchesModelTruth(t *testing.T) {
	pop, err := model.New(model.Config{
		Seed:        1,
		Start:       time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC),
		End:         time.Date(2026, 9, 20, 6, 0, 0, 0, time.UTC), // 6 hours, so most series span several l1 blocks.
		MetricNames: 30,
		SeriesZipfS: 1.2, SeriesFloor: 1, SeriesCap: 40,
		ChurnFraction: 0.2, ChurnPeriod: 2 * time.Hour,
		GapFraction: 0.1, GapDuration: time.Hour,
		StaleFraction: 0.1,
		SpikeMetric:   -1,
	})
	require.NoError(t, err)

	bucket := t.TempDir()
	require.NoError(t, os.MkdirAll(bucket+"/anonymous", 0o755))
	_, err = cardgen.Generate(pop, bucket+"/anonymous", cardgen.Config{Partitions: 3, Seed: 1})
	require.NoError(t, err)

	got, err := CountDistinctSeries(bucket)
	require.NoError(t, err)

	want := pop.Truth(nil, pop.Config().Start.UnixMilli(), pop.Config().End.UnixMilli())
	require.Equal(t, want, got, "a hash-deduplicated read of the on-disk blocks must match the population's own distinct-series count")
}

func TestCountDistinctSeries_EmptyBucket(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(dir+"/anonymous", 0o755))

	got, err := CountDistinctSeries(dir)
	require.NoError(t, err)
	require.Zero(t, got)
}
