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

func TestRunE7_SnappedExactAndCheckedWithinBound(t *testing.T) {
	start := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	pop, err := model.New(model.Config{
		Seed:        1,
		Start:       start,
		End:         start.Add(3 * time.Hour),
		MetricNames: 30,
		SeriesZipfS: 1.2, SeriesFloor: 1, SeriesCap: 40,
		// Churn every 20 minutes and stale series make series start and end
		// inside an hour, so an unaligned window really cuts some of them.
		ChurnFraction: 0.5, ChurnPeriod: 20 * time.Minute,
		StaleFraction: 0.3,
		SpikeMetric:   -1,
	})
	require.NoError(t, err)
	bucket := t.TempDir()
	require.NoError(t, os.MkdirAll(bucket+"/anonymous", 0o755))
	_, err = cardgen.Generate(pop, bucket+"/anonymous", cardgen.Config{Partitions: 1, Seed: 1})
	require.NoError(t, err)

	ms := func(d time.Duration) int64 { return start.Add(d).UnixMilli() }
	windows := [][2]int64{
		{ms(time.Hour + 10*time.Minute), ms(time.Hour + 40*time.Minute)},
		{ms(2*time.Hour + 5*time.Minute), ms(2*time.Hour + 6*time.Minute)},
		{ms(time.Hour), ms(2 * time.Hour)}, // aligned: snapped and asked-for agree.
	}
	results, err := RunE7(bucket, pop, windows)
	require.NoError(t, err)
	require.Len(t, results, len(windows))
	for i, r := range results {
		require.True(t, r.Pass(), "window %d: %+v", i, r)
	}
	require.Less(t, results[0].Truth, results[0].TruthSnapped, "the unaligned window must cut some series, or the test is not exercising snapping")
	require.Equal(t, results[2].Truth, results[2].SnappedCount)

	_, err = CountWindow(bucket, ms(30*time.Minute), ms(90*time.Minute))
	require.ErrorContains(t, err, "not inside one block range")
}
