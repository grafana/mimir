// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"fmt"
	"math"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestDeterministicSeriesMerge(t *testing.T) {
	lbls := labels.FromStrings(model.MetricNameLabel, "test_metric")

	type sample struct {
		t  int64
		f  float64
		h  *histogram.Histogram
		fh *histogram.FloatHistogram
	}

	// Each element of a test case's input is the chunks for the series from one store-gateway response.
	testCases := map[string]struct {
		input    [][]storepb.AggrChunk
		expected []sample
		// If true, the result must also be the same as when all chunks are merged inside one store-gateway response.
		sameAsSingleResponse bool
	}{
		"float samples with the same timestamp and different values": {
			input: [][]storepb.AggrChunk{
				{createAggrChunkWithSamples(promql.FPoint{T: 1, F: 1}, promql.FPoint{T: 2, F: 0}, promql.FPoint{T: 3, F: 3})},
				{createAggrChunkWithSamples(promql.FPoint{T: 2, F: 5})},
			},
			expected:             []sample{{t: 1, f: 1}, {t: 2, f: 5}, {t: 3, f: 3}},
			sameAsSingleResponse: true,
		},
		"float samples with the same timestamp in three series": {
			input: [][]storepb.AggrChunk{
				{createAggrChunkWithSamples(promql.FPoint{T: 1, F: 1}, promql.FPoint{T: 2, F: 7}, promql.FPoint{T: 4, F: 4})},
				{createAggrChunkWithSamples(promql.FPoint{T: 2, F: 8}, promql.FPoint{T: 3, F: 3}, promql.FPoint{T: 4, F: 0})},
				{createAggrChunkWithSamples(promql.FPoint{T: 1, F: 1}, promql.FPoint{T: 2, F: -1}, promql.FPoint{T: 4, F: 2}, promql.FPoint{T: 5, F: 5})},
			},
			expected:             []sample{{t: 1, f: 1}, {t: 2, f: 8}, {t: 3, f: 3}, {t: 4, f: 4}, {t: 5, f: 5}},
			sameAsSingleResponse: true,
		},
		"float sample and NaN with the same timestamp": {
			input: [][]storepb.AggrChunk{
				{createAggrChunkWithSamples(promql.FPoint{T: 1, F: math.NaN()}, promql.FPoint{T: 2, F: math.Float64frombits(value.StaleNaN)})},
				{createAggrChunkWithSamples(promql.FPoint{T: 1, F: 2}, promql.FPoint{T: 2, F: math.NaN()})},
			},
			// The value is preferred over NaN. Between two NaNs, the one with the higher bits is preferred.
			expected: []sample{{t: 1, f: 2}, {t: 2, f: math.NaN()}},
		},
		"histogram samples with the same timestamp and different values": {
			input: [][]storepb.AggrChunk{
				{createAggrHistogramChunkWithSamples(t, 1, test.GenerateTestHistogram(1), 2, test.GenerateTestHistogram(2))},
				{createAggrHistogramChunkWithSamples(t, 2, test.GenerateTestHistogram(5))},
			},
			expected:             []sample{{t: 1, h: test.GenerateTestHistogram(1)}, {t: 2, h: test.GenerateTestHistogram(5)}},
			sameAsSingleResponse: true,
		},
		"float histogram samples with the same timestamp and different values": {
			input: [][]storepb.AggrChunk{
				{createAggrChunkWithFloatHistogramSamples(promql.HPoint{T: 1, H: test.GenerateTestFloatHistogram(1)}, promql.HPoint{T: 2, H: test.GenerateTestFloatHistogram(2)})},
				{createAggrChunkWithFloatHistogramSamples(promql.HPoint{T: 2, H: test.GenerateTestFloatHistogram(5)})},
			},
			expected:             []sample{{t: 1, fh: test.GenerateTestFloatHistogram(1)}, {t: 2, fh: test.GenerateTestFloatHistogram(5)}},
			sameAsSingleResponse: true,
		},
		"float and float histogram samples with the same timestamp": {
			input: [][]storepb.AggrChunk{
				{createAggrChunkWithSamples(promql.FPoint{T: 1, F: 100})},
				{createAggrChunkWithFloatHistogramSamples(promql.HPoint{T: 1, H: test.GenerateTestFloatHistogram(1)})},
			},
			expected:             []sample{{t: 1, fh: test.GenerateTestFloatHistogram(1)}},
			sameAsSingleResponse: true,
		},
	}

	readAll := func(t *testing.T, it chunkenc.Iterator, seekTo int64) []sample {
		var out []sample
		vt := it.Seek(seekTo)
		for ; vt != chunkenc.ValNone; vt = it.Next() {
			s := sample{t: it.AtT()}
			switch vt {
			case chunkenc.ValFloat:
				_, s.f = it.At()
			case chunkenc.ValHistogram:
				_, s.h = it.AtHistogram(nil)
				// Counter reset hints depend on which chunks the samples come from, and we only care about values here.
				s.h.CounterResetHint = histogram.UnknownCounterReset
			case chunkenc.ValFloatHistogram:
				_, s.fh = it.AtFloatHistogram(nil)
				s.fh.CounterResetHint = histogram.UnknownCounterReset
			default:
				require.FailNow(t, "unexpected value type", vt)
			}
			out = append(out, s)
		}
		require.NoError(t, it.Err())
		return out
	}

	requireSamplesEqual := func(t *testing.T, expected, actual []sample) {
		require.Len(t, actual, len(expected))
		for i := range expected {
			e, a := expected[i], actual[i]
			require.Equal(t, e.t, a.t, "sample %d", i)
			if math.IsNaN(e.f) {
				// NaN != NaN, so compare the bits.
				require.Equal(t, math.Float64bits(e.f), math.Float64bits(a.f), "sample %d", i)
			} else {
				require.Equal(t, e.f, a.f, "sample %d", i)
			}
			if e.h != nil {
				e.h = e.h.Copy()
				e.h.CounterResetHint = histogram.UnknownCounterReset
			}
			if e.fh != nil {
				e.fh = e.fh.Copy()
				e.fh.CounterResetHint = histogram.UnknownCounterReset
			}
			require.Equal(t, e.h, a.h, "sample %d", i)
			require.Equal(t, e.fh, a.fh, "sample %d", i)
		}
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			for _, perm := range permutations(len(tc.input)) {
				series := make([]storage.Series, 0, len(tc.input))
				for _, i := range perm {
					series = append(series, newTestBlockSeries(lbls, tc.input[i]))
				}

				t.Run(fmt.Sprintf("order %v", perm), func(t *testing.T) {
					merged := deterministicSeriesMerge(series...)
					require.Equal(t, lbls, merged.Labels())

					t.Run("Next", func(t *testing.T) {
						requireSamplesEqual(t, tc.expected, readAll(t, merged.Iterator(nil), math.MinInt64))
					})

					t.Run("Seek to each timestamp", func(t *testing.T) {
						for i, s := range tc.expected {
							requireSamplesEqual(t, tc.expected[i:], readAll(t, merged.Iterator(nil), s.t))
						}
					})
				})
			}

			if tc.sameAsSingleResponse {
				var allChunks []storepb.AggrChunk
				for _, chks := range tc.input {
					allChunks = append(allChunks, chks...)
				}
				requireSamplesEqual(t, tc.expected, readAll(t, newBlockQuerierSeriesIterator(nil, lbls, allChunks), math.MinInt64))
			}
		})
	}
}

// newTestBlockSeries returns a series with the same iterator as a series from a store-gateway response.
func newTestBlockSeries(lbls labels.Labels, chks []storepb.AggrChunk) storage.Series {
	return &storage.SeriesEntry{
		Lset: lbls,
		SampleIteratorFn: func(it chunkenc.Iterator) chunkenc.Iterator {
			return newBlockQuerierSeriesIterator(it, lbls, chks)
		},
	}
}

// createAggrHistogramChunkWithSamples takes pairs of timestamp (int64) and *histogram.Histogram.
func createAggrHistogramChunkWithSamples(t *testing.T, pairs ...any) storepb.AggrChunk {
	chunk := chunkenc.NewHistogramChunk()
	appender, err := chunk.Appender()
	require.NoError(t, err)

	var minT, maxT int64
	for i := 0; i < len(pairs); i += 2 {
		ts := int64(pairs[i].(int))
		if i == 0 {
			minT = ts
		}
		maxT = ts
		_, _, appender, err = appender.AppendHistogram(nil, 0, ts, pairs[i+1].(*histogram.Histogram), true)
		require.NoError(t, err)
	}

	return storepb.AggrChunk{
		MinTime: minT,
		MaxTime: maxT,
		Raw: storepb.Chunk{
			Type: storepb.Chunk_Histogram,
			Data: chunk.Bytes(),
		},
	}
}

// permutations returns all orders of the indexes 0 to n-1.
func permutations(n int) [][]int {
	if n == 0 {
		return [][]int{{}}
	}
	var out [][]int
	for _, p := range permutations(n - 1) {
		for i := 0; i <= len(p); i++ {
			q := make([]int, 0, n)
			q = append(q, p[:i]...)
			q = append(q, n-1)
			q = append(q, p[i:]...)
			out = append(out, q)
		}
	}
	return out
}
