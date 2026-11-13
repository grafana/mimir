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

func TestRunE9_AboveThresholdMatchesTruth(t *testing.T) {
	// One metric (index 0) is made permanently high-cardinality via the
	// spike mechanism with Base==Peak (so it's a flat 2000 series, not an
	// actual spike): large enough that even split across 12 simulated
	// gateways, its true count clears the protocol's threshold, unlike
	// every other, ordinary-sized metric here.
	pop, err := model.New(model.Config{
		Seed:        1,
		Start:       time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC),
		End:         time.Date(2026, 9, 20, 4, 0, 0, 0, time.UTC),
		MetricNames: 40, SeriesZipfS: 1.3, SeriesFloor: 1, SeriesCap: 50,
		SpikeMetric: 0, SpikeDay: 0, SpikeBaseValues: 2000, SpikePeakValues: 2000,
	})
	require.NoError(t, err)
	bucket := t.TempDir()
	require.NoError(t, os.MkdirAll(bucket+"/anonymous", 0o755))
	_, err = cardgen.Generate(pop, bucket+"/anonymous", cardgen.Config{Partitions: 8, Seed: 1})
	require.NoError(t, err)

	for _, k := range []int{3, 12} {
		res, err := RunE9(bucket, pop, k, 5)
		require.NoError(t, err, "K=%d", k)
		require.NotEmpty(t, res.Above, "K=%d: this population must produce at least one name above threshold, or the test isn't exercising the interesting case", k)
		require.True(t, res.Pass(), "K=%d: above=%v truth=%v", k, res.Above, res.Truth)
		require.NotEmpty(t, res.NearBelow, "K=%d", k)
		for _, e := range res.NearBelow {
			require.LessOrEqual(t, e.Count, res.Threshold, "K=%d: near-below names must be at or below the threshold", k)
		}
		require.Contains(t, res.Details(), "near below", "K=%d", k)
	}
}

func TestLocalTopN(t *testing.T) {
	blocks := []Block{
		{SeriesByName: map[string][]uint64{"a": {1, 2, 3}, "b": {4, 5}, "c": {6}}},
	}
	top, cutoff := localTopN(blocks, 2)
	require.Equal(t, []TopNEntry{{"a", 3}, {"b", 2}}, top)
	require.Equal(t, 2, cutoff)

	// n larger than the number of names: cutoff is the smallest present.
	top, cutoff = localTopN(blocks, 10)
	require.Len(t, top, 3)
	require.Equal(t, 1, cutoff)
}

func TestAssignGateways_EveryBlockAssignedOnce(t *testing.T) {
	blocks := []Block{{ID: id(1)}, {ID: id(2)}, {ID: id(3)}, {ID: id(4)}, {ID: id(5)}}
	gateways := assignGateways(blocks, 3)

	require.Len(t, gateways, 3)
	var total int
	for _, gw := range gateways {
		total += len(gw)
	}
	require.Equal(t, len(blocks), total)
}

func TestE9Result_Pass(t *testing.T) {
	require.True(t, E9Result{
		Above: []TopNEntry{{"a", 10}, {"b", 5}},
		Truth: []TopNEntry{{"a", 10}, {"b", 5}},
	}.Pass())
	require.False(t, E9Result{
		Above: []TopNEntry{{"a", 10}},
		Truth: []TopNEntry{{"a", 10}, {"b", 5}},
	}.Pass(), "must fail when Above is missing a name Truth has")
	require.False(t, E9Result{
		Above: []TopNEntry{{"a", 9}},
		Truth: []TopNEntry{{"a", 10}},
	}.Pass(), "must fail when a name's count disagrees")
}
