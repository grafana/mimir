// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/series"
)

func TestCountSeriesSamples(t *testing.T) {
	at := func(ts ...int64) []model.SamplePair {
		out := make([]model.SamplePair, len(ts))
		for i, t := range ts {
			out[i] = model.SamplePair{Timestamp: model.Time(t), Value: 1}
		}
		return out
	}
	newSet := func() storage.SeriesSet {
		return series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
			series.NewConcreteSeries(labels.FromStrings("__name__", "up", "pod", "a"), at(5, 15), nil),
			series.NewConcreteSeries(labels.FromStrings("__name__", "up", "pod", "b"), at(12), nil),
			// Samples only outside the window: a chunk can overlap the window
			// with none of its samples inside it, and this path doesn't count it.
			series.NewConcreteSeries(labels.FromStrings("__name__", "up", "pod", "c"), at(1, 25), nil),
			series.NewConcreteSeries(labels.FromStrings("__name__", "errors_total", "pod", "a"), at(19), nil),
		})
	}

	counts, err := countSeriesSamples(newSet(), SeriesCountsRequest{MinT: 2, MaxT: 22, Step: 10})
	require.NoError(t, err)
	// Buckets [2, 12) and [12, 22).
	assert.Equal(t, map[string][]int64{"up": {1, 2}, "errors_total": {0, 1}}, counts)

	counts, err = countSeriesSamples(newSet(), SeriesCountsRequest{MinT: 2, MaxT: 22, GroupBy: "pod"})
	require.NoError(t, err)
	assert.Equal(t, map[string][]int64{"a": {2}, "b": {1}}, counts)
}

func TestCountsDiff(t *testing.T) {
	examples, n := countsDiff(map[string][]int64{"a": {1}, "b": {2}}, map[string][]int64{"a": {1}, "b": {2}}, 5)
	assert.Zero(t, n)
	assert.Empty(t, examples)

	examples, n = countsDiff(map[string][]int64{"a": {1}, "b": {2}, "c": {1}}, map[string][]int64{"a": {1}, "b": {3}, "d": {1}}, 2)
	assert.Equal(t, 3, n, "b differs, c is only in index, d only in chunks")
	assert.Equal(t, []string{"b: index [2], chunks [3]", "c: index [1], chunks []"}, examples)
}
