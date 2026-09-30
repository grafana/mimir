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

// e9TPUTBucket generates hours hourly blocks per partition for a population
// with several names close together at the top, so top-5 is not decided by
// one dominant name.
func e9TPUTBucket(t *testing.T, hours, partitions int) (string, *model.Model) {
	start := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	pop, err := model.New(model.Config{
		Seed:        1,
		Start:       start,
		End:         start.Add(time.Duration(hours) * time.Hour),
		MetricNames: 60,
		SeriesZipfS: 1.2, SeriesFloor: 1, SeriesCap: 30,
		FixedSeries: []uint64{400, 390, 380, 370, 360, 350, 340},
		SpikeMetric: -1,
	})
	require.NoError(t, err)
	bucket := t.TempDir()
	require.NoError(t, os.MkdirAll(bucket+"/anonymous", 0o755))
	_, err = cardgen.Generate(pop, bucket+"/anonymous", cardgen.Config{Partitions: partitions, Seed: 1})
	require.NoError(t, err)
	return bucket, pop
}

func TestRunE9TPUT_SumIsExactWhenGatewaysShareNoSeries(t *testing.T) {
	// One hour, eight partitions: eight series-disjoint blocks.
	bucket, pop := e9TPUTBucket(t, 1, 8)
	for _, k := range []int{3, 12} {
		res, err := RunE9TPUT(bucket, pop, k, 5, false)
		require.NoError(t, err)
		require.True(t, res.Pass(), "K=%d: answer=%v truth=%v", k, res.Answer, res.Truth)
		require.Zero(t, res.ExactNames)
	}
}

func TestRunE9TPUT_DedupIsExactWhenGatewaysShareSeries(t *testing.T) {
	// Four hours, one partition: every long-lived series is in four blocks,
	// which the gateways split between them.
	bucket, pop := e9TPUTBucket(t, 4, 1)
	for _, k := range []int{3, 12} {
		res, err := RunE9TPUT(bucket, pop, k, 5, true)
		require.NoError(t, err)
		require.True(t, res.Pass(), "K=%d: answer=%v truth=%v", k, res.Answer, res.Truth)
		require.Positive(t, res.ExactNames)
	}

	// Without dedup the round-2 sums count a series once per gateway that
	// holds one of its blocks, so the counts come out above the truth.
	res, err := RunE9TPUT(bucket, pop, 3, 5, false)
	require.NoError(t, err)
	require.False(t, res.Pass())
	require.Greater(t, res.Answer[0].Count, res.Truth[0].Count)
}

func TestE9TPUTResult_PassAndMissed(t *testing.T) {
	r := E9TPUTResult{
		Answer: []TopNEntry{{"a", 10}, {"c", 4}},
		Truth:  []TopNEntry{{"a", 10}, {"b", 5}},
	}
	require.False(t, r.Pass())
	require.Equal(t, []string{"b"}, r.Missed())
	r.Answer = r.Truth
	require.True(t, r.Pass())
	require.Empty(t, r.Missed())
}
