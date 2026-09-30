// SPDX-License-Identifier: AGPL-3.0-only

package cardpoc

import (
	"math"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/tools/headcount/cardgen"
	"github.com/grafana/mimir/tools/headcount/model"
)

func TestRunE6_ExactBelowThreshold(t *testing.T) {
	// Every metric name's cardinality here is well under a threshold of
	// 1000, so RunE6 must report the exact count for all of them.
	pop, err := model.New(model.Config{
		Seed:  1,
		Start: time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC), End: time.Date(2026, 9, 20, 2, 0, 0, 0, time.UTC),
		MetricNames: 10, SeriesZipfS: 1.2, SeriesFloor: 1, SeriesCap: 50, SpikeMetric: -1,
	})
	require.NoError(t, err)
	bucket := t.TempDir()
	require.NoError(t, os.MkdirAll(bucket+"/anonymous", 0o755))
	_, err = cardgen.Generate(pop, bucket+"/anonymous", cardgen.Config{Partitions: 2, Seed: 1})
	require.NoError(t, err)

	res, err := RunE6(bucket, pop, 1000)
	require.NoError(t, err)
	require.NotEmpty(t, res.Groups)
	for _, g := range res.Groups {
		require.False(t, g.UsedHLL, "name %s: cardinality %d is under the threshold", g.Name, g.Exact)
		require.Equal(t, g.Truth, g.Estimate, "name %s", g.Name)
	}
	require.True(t, res.Pass())
}

func TestRunE6_HLLAboveThreshold_BoundedError(t *testing.T) {
	// One metric name with high cardinality, forced above a low
	// threshold, so RunE6 must fall back to the HLL estimate for it -
	// and that estimate must still be within HLL's expected error bound.
	pop, err := model.New(model.Config{
		Seed:  1,
		Start: time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC), End: time.Date(2026, 9, 20, 1, 0, 0, 0, time.UTC),
		MetricNames: 1, SeriesZipfS: 1.2, SeriesFloor: 20000, SeriesCap: 20000, SpikeMetric: -1,
	})
	require.NoError(t, err)
	bucket := t.TempDir()
	require.NoError(t, os.MkdirAll(bucket+"/anonymous", 0o755))
	_, err = cardgen.Generate(pop, bucket+"/anonymous", cardgen.Config{Partitions: 1, Seed: 1})
	require.NoError(t, err)

	res, err := RunE6(bucket, pop, 10_000)
	require.NoError(t, err)
	require.Len(t, res.Groups, 1)

	g := res.Groups[0]
	require.True(t, g.UsedHLL)
	require.Equal(t, g.Truth, g.Exact, "the exact count computed for comparison must still be right")
	require.Less(t, math.Abs(g.RelativeError()), 0.1, "a single p=11 sketch should not be off by more than ~10%% at this cardinality")
	require.Greater(t, g.PayloadBytes, 0)
}

func TestP95(t *testing.T) {
	require.InDelta(t, 0.05, p95([]float64{0.01, 0.02, 0.03, 0.04, 0.05}), 1e-9)
	require.InDelta(t, 0.9, p95([]float64{0.1, 0.9}), 1e-9, "nearest-rank at n=2 takes the larger value")
}

func TestE6Result_HLLErrorStats(t *testing.T) {
	r := E6Result{Threshold: 10, Groups: []NameGroupResult{
		{Name: "exact", Truth: 5, Estimate: 5},
		{Name: "a", Truth: 100, Estimate: 102, UsedHLL: true},
		{Name: "b", Truth: 200, Estimate: 190, UsedHLL: true},
	}}
	p95Err, maxErr := r.HLLErrorStats()
	require.InDelta(t, 0.05, p95Err, 1e-9)
	require.InDelta(t, 0.05, maxErr, 1e-9)

	groups := r.HLLGroups()
	require.Len(t, groups, 2)
	require.Equal(t, "b", groups[0].Name, "largest truth first")
	require.Contains(t, r.Details(), "error=-5.00%")

	p95Err, maxErr = E6Result{}.HLLErrorStats()
	require.Zero(t, p95Err)
	require.Zero(t, maxErr)
}

func TestE6Result_PassBar(t *testing.T) {
	result := func(estimate int) E6Result {
		return E6Result{Threshold: 10, Groups: []NameGroupResult{{Truth: 100, Estimate: estimate, UsedHLL: true}}}
	}
	require.True(t, result(104).Pass(), "4% error is inside the 5% p95 bar")
	require.True(t, result(95).Pass(), "5% error is on the bar")
	require.False(t, result(106).Pass(), "6% error is outside the bar")
}
