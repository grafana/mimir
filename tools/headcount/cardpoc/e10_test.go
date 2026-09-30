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

func TestRunE10_BudgetTripsAndUndercounts(t *testing.T) {
	// Metric 0 is the spike metric: 5 pods for the whole range, 200 during
	// the first day, so its pod breakdown has many values to cut short.
	pop, err := model.New(model.Config{
		Seed:        1,
		Start:       time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC),
		End:         time.Date(2026, 9, 20, 2, 0, 0, 0, time.UTC),
		MetricNames: 10,
		SeriesZipfS: 1.2, SeriesFloor: 1, SeriesCap: 20,
		SpikeMetric: 0, SpikeDay: 0, SpikeBaseValues: 5, SpikePeakValues: 200,
	})
	require.NoError(t, err)
	bucket := t.TempDir()
	require.NoError(t, os.MkdirAll(bucket+"/anonymous", 0o755))
	_, err = cardgen.Generate(pop, bucket+"/anonymous", cardgen.Config{Partitions: 1, Seed: 1})
	require.NoError(t, err)

	ranges, err := BlockRanges(bucket)
	require.NoError(t, err)
	metric := pop.Series[0].Labels.Get("__name__")

	res, err := RunE10(bucket, pop, ranges[0], metric, "pod", Budget{MaxSeries: 50})
	require.NoError(t, err)
	require.True(t, res.Pass(), "exact=%d budgeted=%d lowerBound=%v", res.Exact.Total(), res.Budgeted.Total(), res.Budgeted.LowerBound)
	require.Len(t, res.Exact.Counts, 200)
	require.True(t, res.Budgeted.LowerBound)
	require.Less(t, len(res.Budgeted.Counts), 200, "a tripped breakdown is missing values")

	_, err = RunE10(bucket, pop, ranges[0], metric, "pod", Budget{MaxSeries: 200})
	require.Error(t, err, "a budget at or above the series count cannot trip")
}

func TestE10Result_Pass(t *testing.T) {
	truth := map[string]int{"a": 2, "b": 1}
	ok := E10Result{
		Budget:   Budget{MaxSeries: 2},
		Exact:    Breakdown{Counts: map[string]int{"a": 2, "b": 1}},
		Budgeted: Breakdown{Counts: map[string]int{"a": 2}, SeriesTouched: 2, LowerBound: true},
		Truth:    truth,
	}
	require.True(t, ok.Pass())

	over := ok
	over.Budgeted = Breakdown{Counts: map[string]int{"a": 1, "b": 2}, SeriesTouched: 3, LowerBound: true}
	over.Budget = Budget{MaxSeries: 3}
	require.False(t, over.Pass(), "a count above the truth must fail")

	notTripped := ok
	notTripped.Budgeted.LowerBound = false
	require.False(t, notTripped.Pass(), "a budgeted run that did not trip must fail")

	wrongExact := ok
	wrongExact.Exact = Breakdown{Counts: map[string]int{"a": 1, "b": 1}}
	require.False(t, wrongExact.Pass(), "an unbudgeted run that disagrees with the truth must fail")
}
