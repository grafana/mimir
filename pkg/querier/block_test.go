// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/block_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestBlockQuerierSeries(t *testing.T) {
	t.Parallel()

	// Init some test fixtures
	minTimestamp := time.Unix(1, 0)
	maxTimestamp := time.Unix(10, 0)

	tests := map[string]struct {
		series          *storepb.Series
		expectedMetric  labels.Labels
		expectedSamples int64
		assertSample    func(t *testing.T, i int64, iter chunkenc.Iterator, valueType chunkenc.ValueType)
		expectedErr     string
	}{
		"empty series": {
			series:         &storepb.Series{},
			expectedMetric: labels.EmptyLabels(),
			expectedErr:    "no chunks",
		},
		"should return float series on success": {
			series: &storepb.Series{
				Labels: []mimirpb.LabelAdapter{
					{Name: "foo", Value: "bar"},
				},
				Chunks: []storepb.AggrChunk{
					{
						MinTime: minTimestamp.Unix() * 1000,
						MaxTime: maxTimestamp.Unix() * 1000,
						Raw:     storepb.Chunk{Type: storepb.Chunk_XOR, Data: mockTSDBXorChunkData(t)},
					},
				},
			},
			expectedMetric:  labels.FromStrings("foo", "bar"),
			expectedSamples: 2,
			assertSample: func(t *testing.T, i int64, iter chunkenc.Iterator, valueType chunkenc.ValueType) {
				test.RequireIteratorFloat(t, time.Unix(i, 0).UnixMilli(), float64(i), iter, valueType)
			},
		},
		"should return histogram series on success": {
			series: &storepb.Series{
				Labels: []mimirpb.LabelAdapter{
					{Name: "foo", Value: "bar"},
				},
				Chunks: []storepb.AggrChunk{
					{
						MinTime: minTimestamp.Unix() * 1000,
						MaxTime: maxTimestamp.Unix() * 1000,
						Raw:     storepb.Chunk{Type: storepb.Chunk_Histogram, Data: mockTSDBHistogramChunkData(t)},
					},
				},
			},
			expectedMetric:  labels.FromStrings("foo", "bar"),
			expectedSamples: 2,
			assertSample: func(t *testing.T, i int64, iter chunkenc.Iterator, valueType chunkenc.ValueType) {
				expH := test.GenerateTestHistogram(int(i))
				if i > 1 { // 1 because the chunk contains samples starting from timestamp 1, not 0
					expH.CounterResetHint = histogram.NotCounterReset
				}
				test.RequireIteratorHistogram(t, time.Unix(i, 0).UnixMilli(), expH, iter, valueType)
			},
		},
		"should return float histogram series on success": {
			series: &storepb.Series{
				Labels: []mimirpb.LabelAdapter{
					{Name: "foo", Value: "bar"},
				},
				Chunks: []storepb.AggrChunk{
					{
						MinTime: minTimestamp.Unix() * 1000,
						MaxTime: maxTimestamp.Unix() * 1000,
						Raw:     storepb.Chunk{Type: storepb.Chunk_FloatHistogram, Data: mockTSDBFloatHistogramChunkData(t)},
					},
				},
			},
			expectedMetric:  labels.FromStrings("foo", "bar"),
			expectedSamples: 2,
			assertSample: func(t *testing.T, i int64, iter chunkenc.Iterator, valueType chunkenc.ValueType) {
				expFH := test.GenerateTestFloatHistogram(int(i))
				if i > 1 { // 1 because the chunk contains samples starting from timestamp 1, not 0
					expFH.CounterResetHint = histogram.NotCounterReset
				}
				test.RequireIteratorFloatHistogram(t, time.Unix(i, 0).UnixMilli(), expFH, iter, valueType)
			},
		},
		"should return error on failure while reading encoded chunk data": {
			series: &storepb.Series{
				Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar"}},
				Chunks: []storepb.AggrChunk{
					{MinTime: minTimestamp.Unix() * 1000, MaxTime: maxTimestamp.Unix() * 1000, Raw: storepb.Chunk{Type: storepb.Chunk_XOR, Data: []byte{0, 1}}},
				},
			},
			expectedMetric: labels.FromStrings("foo", "bar"),
			expectedErr:    `error reading chunks for series {foo="bar"}: EOF`,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			series := newBlockQuerierSeries(mimirpb.FromLabelAdaptersToLabels(testData.series.Labels), testData.series.Chunks)

			assert.True(t, labels.Equal(testData.expectedMetric, series.Labels()))

			sampleIx := int64(0)

			it := series.Iterator(nil)
			for valType := it.Next(); valType != chunkenc.ValNone; valType = it.Next() {
				require.True(t, sampleIx < testData.expectedSamples)
				testData.assertSample(t, sampleIx+1, it, valType) // +1 because the chunk contains samples starting from timestamp 1, not 0
				sampleIx++
			}
			// make sure we've got all expected samples
			require.Equal(t, sampleIx, testData.expectedSamples)

			if testData.expectedErr != "" {
				require.EqualError(t, it.Err(), testData.expectedErr)
			} else {
				require.NoError(t, it.Err())
			}
		})
	}
}

func mockTSDBXorChunkData(t *testing.T) []byte {
	chunk := chunkenc.NewXORChunk()
	appender, err := chunk.Appender()
	require.NoError(t, err)

	appender.Append(time.Unix(1, 0).Unix()*1000, 1)
	appender.Append(time.Unix(2, 0).Unix()*1000, 2)

	return chunk.Bytes()
}

func mockTSDBHistogramChunkData(t *testing.T) []byte {
	chunk := chunkenc.NewHistogramChunk()
	appender, err := chunk.Appender()
	require.NoError(t, err)

	_, _, _, err = appender.AppendHistogram(nil, time.Unix(1, 0).Unix()*1000, test.GenerateTestHistogram(1), true)
	require.NoError(t, err)
	_, _, _, err = appender.AppendHistogram(nil, time.Unix(2, 0).Unix()*1000, test.GenerateTestHistogram(2), true)
	require.NoError(t, err)

	return chunk.Bytes()
}

func mockTSDBFloatHistogramChunkData(t *testing.T) []byte {
	chunk := chunkenc.NewFloatHistogramChunk()
	appender, err := chunk.Appender()
	require.NoError(t, err)

	_, _, _, err = appender.AppendFloatHistogram(nil, time.Unix(1, 0).Unix()*1000, test.GenerateTestFloatHistogram(1), true)
	require.NoError(t, err)
	_, _, _, err = appender.AppendFloatHistogram(nil, time.Unix(2, 0).Unix()*1000, test.GenerateTestFloatHistogram(2), true)
	require.NoError(t, err)

	return chunk.Bytes()
}

type timeRange struct {
	minT time.Time
	maxT time.Time
}

func TestBlockQuerierSeriesSet(t *testing.T) {
	now := time.Now()

	// It would be possible to split this test into smaller parts, but I prefer to keep
	// it as is, to also test transitions between series.

	getSeriesSet := func() *blockQuerierSeriesSet {
		return &blockQuerierSeriesSet{
			series: []*storepb.Series{
				// first, with one chunk.
				{
					Labels: mkZLabels("__name__", "first", "a", "a"),
					Chunks: []storepb.AggrChunk{
						createAggrChunkWithSineSamples(now, now.Add(100*time.Second-time.Millisecond), 3*time.Millisecond), // ceil(100 / 0.003) samples (= 33334)
					},
				},
				// continuation of previous series. Must have exact same labels.
				{
					Labels: mkZLabels("__name__", "first", "a", "a"),
					Chunks: []storepb.AggrChunk{
						createAggrChunkWithSineSamples(now.Add(100*time.Second), now.Add(200*time.Second-time.Millisecond), 3*time.Millisecond), // ceil(100 / 0.003) samples (= 33334) samples more, 66668 in total
					},
				},
				// second, with multiple chunks
				{
					Labels: mkZLabels("__name__", "second"),
					Chunks: []storepb.AggrChunk{
						// unordered chunks
						createAggrChunkWithSineSamples(now.Add(400*time.Second), now.Add(600*time.Second-5*time.Millisecond), 5*time.Millisecond), // 200 / 0.005 (= 40000 samples, = 120000 in total)
						createAggrChunkWithSineSamples(now.Add(200*time.Second), now.Add(400*time.Second-5*time.Millisecond), 5*time.Millisecond), // 200 / 0.005 (= 40000 samples)
						createAggrChunkWithSineSamples(now, now.Add(200*time.Second-5*time.Millisecond), 5*time.Millisecond),                      // 200 / 0.005 (= 40000 samples)
					},
				},
				// overlapping
				{
					Labels: mkZLabels("__name__", "overlapping"),
					Chunks: []storepb.AggrChunk{
						createAggrChunkWithSineSamples(now, now.Add(10*time.Second-5*time.Millisecond), 5*time.Millisecond), // 10 / 0.005 = 2000 samples
					},
				},
				{
					Labels: mkZLabels("__name__", "overlapping"),
					Chunks: []storepb.AggrChunk{
						// 10 / 0.005 = 2000 samples, but first 1000 are overlapping with previous series, so this chunk only contributes 1000
						createAggrChunkWithSineSamples(now.Add(5*time.Second), now.Add(15*time.Second-5*time.Millisecond), 5*time.Millisecond),
					},
				},
				// overlapping 2. Chunks here come in wrong order.
				{
					Labels: mkZLabels("__name__", "overlapping2"),
					Chunks: []storepb.AggrChunk{
						// entire range overlaps with the next chunk, so this chunks contributes 0 samples (it will be sorted as second)
						createAggrChunkWithSineSamples(now.Add(3*time.Second), now.Add(7*time.Second-5*time.Millisecond), 5*time.Millisecond),
					},
				},
				{
					Labels: mkZLabels("__name__", "overlapping2"),
					Chunks: []storepb.AggrChunk{
						// this chunk has completely overlaps previous chunk. Since its minTime is lower, it will be sorted as first.
						createAggrChunkWithSineSamples(now, now.Add(10*time.Second-5*time.Millisecond), 5*time.Millisecond), // 10 / 0.005 = 2000 samples
					},
				},
				{
					Labels: mkZLabels("__name__", "overlapping2"),
					Chunks: []storepb.AggrChunk{
						// no samples
						createAggrChunkWithSineSamples(now, now.Add(-5*time.Millisecond), 5*time.Millisecond),
					},
				},
				{
					Labels: mkZLabels("__name__", "overlapping2"),
					Chunks: []storepb.AggrChunk{
						// 2000 samples more (10 / 0.005)
						createAggrChunkWithSineSamples(now.Add(20*time.Second), now.Add(30*time.Second-5*time.Millisecond), 5*time.Millisecond),
					},
				},
				// many_empty_chunks is a series which contains many empty chunks and only a few that have data
				{
					Labels: mkZLabels("__name__", "many_empty_chunks"),
					Chunks: []storepb.AggrChunk{
						createAggrChunkWithSineSamples(now, now.Add(-5*time.Millisecond), 5*time.Millisecond),                                   // empty
						createAggrChunkWithSineSamples(now, now.Add(10*time.Second-5*time.Millisecond), 5*time.Millisecond),                     // 10 / 0.005 (= 2000 samples)
						createAggrChunkWithSineSamples(now.Add(10*time.Second), now.Add(10*time.Second-5*time.Millisecond), 5*time.Millisecond), // empty
						createAggrChunkWithSineSamples(now.Add(10*time.Second), now.Add(10*time.Second-5*time.Millisecond), 5*time.Millisecond), // empty
						createAggrChunkWithSineSamples(now.Add(10*time.Second), now.Add(20*time.Second-5*time.Millisecond), 5*time.Millisecond), // 10 / 0.005 (= 2000 samples, = 4000 in total)
						createAggrChunkWithSineSamples(now.Add(20*time.Second), now.Add(20*time.Second-5*time.Millisecond), 5*time.Millisecond), // empty
						createAggrChunkWithSineSamples(now.Add(20*time.Second), now.Add(20*time.Second-5*time.Millisecond), 5*time.Millisecond), // empty
						createAggrChunkWithSineSamples(now.Add(20*time.Second), now.Add(20*time.Second-5*time.Millisecond), 5*time.Millisecond), // empty
						createAggrChunkWithSineSamples(now.Add(20*time.Second), now.Add(30*time.Second-5*time.Millisecond), 5*time.Millisecond), // 10 / 0.005 (= 2000 samples, = 6000 in total)
						createAggrChunkWithSineSamples(now.Add(30*time.Second), now.Add(30*time.Second-5*time.Millisecond), 5*time.Millisecond), // empty
					},
				},
				// Two adjacent ranges with overlapping chunks in each range. Each overlapping chunk in a
				// range have +1 sample at +1ms timestamp compared to the previous one.
				{
					Labels: mkZLabels("__name__", "overlapping_chunks_with_additional_samples_in_sequence"),
					Chunks: []storepb.AggrChunk{
						// Range #1: [now, now+4ms]
						createAggrChunkWithSineSamples(now, now.Add(1*time.Millisecond), time.Millisecond),
						createAggrChunkWithSineSamples(now, now.Add(2*time.Millisecond), time.Millisecond),
						createAggrChunkWithSineSamples(now, now.Add(3*time.Millisecond), time.Millisecond),
						createAggrChunkWithSineSamples(now, now.Add(4*time.Millisecond), time.Millisecond),
						// Range #2: [now+5ms, now+7ms]
						createAggrChunkWithSineSamples(now.Add(5*time.Millisecond), now.Add(5*time.Millisecond), time.Millisecond),
						createAggrChunkWithSineSamples(now.Add(5*time.Millisecond), now.Add(6*time.Millisecond), time.Millisecond),
						createAggrChunkWithSineSamples(now.Add(5*time.Millisecond), now.Add(7*time.Millisecond), time.Millisecond),
					},
				},
			},
		}
	}

	// Test while calling .At() after varying numbers of samples have been consumed
	for _, callAtEvery := range []uint32{1, 3, 100, 971, 1000} {
		t.Run(fmt.Sprintf("consume with .Next() method, perform .At() after every %dth call to .Next()", callAtEvery), func(t *testing.T) {
			t.Parallel()

			advance := func(it chunkenc.Iterator, _ int64) chunkenc.ValueType { return it.Next() }
			ss := getSeriesSet()

			verifyNextSeries(t, ss, labels.FromStrings("__name__", "first", "a", "a"), 3*time.Millisecond, []timeRange{
				{now, now.Add(100*time.Second - time.Millisecond)},
				{now.Add(100 * time.Second), now.Add(200*time.Second - time.Millisecond)},
			}, 66668, callAtEvery, advance)
			verifyNextSeries(t, ss, labels.FromStrings("__name__", "second"), 5*time.Millisecond, []timeRange{
				{now, now.Add(600*time.Second - 5*time.Millisecond)},
			}, 120000, callAtEvery, advance)
			verifyNextSeries(t, ss, labels.FromStrings("__name__", "overlapping"), 5*time.Millisecond, []timeRange{
				{now, now.Add(15*time.Second - 5*time.Millisecond)},
			}, 3000, callAtEvery, advance)
			verifyNextSeries(t, ss, labels.FromStrings("__name__", "overlapping2"), 5*time.Millisecond, []timeRange{
				{now, now.Add(10*time.Second - 5*time.Millisecond)},
				{now.Add(20 * time.Second), now.Add(30*time.Second - 5*time.Millisecond)},
			}, 4000, callAtEvery, advance)
			verifyNextSeries(t, ss, labels.FromStrings("__name__", "many_empty_chunks"), 5*time.Millisecond, []timeRange{
				{now, now.Add(30*time.Second - 5*time.Millisecond)},
			}, 6000, callAtEvery, advance)
			verifyNextSeries(t, ss, labels.FromStrings("__name__", "overlapping_chunks_with_additional_samples_in_sequence"), time.Millisecond, []timeRange{
				{now, now.Add(7 * time.Millisecond)},
			}, 8, callAtEvery, advance)

			require.False(t, ss.Next())
		})

		t.Run(fmt.Sprintf("consume with .Seek() method, perform .At() after every %dth call to .Seek()", callAtEvery), func(t *testing.T) {
			t.Parallel()

			advance := func(it chunkenc.Iterator, wantTs int64) chunkenc.ValueType { return it.Seek(wantTs) }
			ss := getSeriesSet()

			verifyNextSeries(t, ss, labels.FromStrings("__name__", "first", "a", "a"), 3*time.Millisecond, []timeRange{
				{now, now.Add(100*time.Second - time.Millisecond)},
				{now.Add(100 * time.Second), now.Add(200*time.Second - time.Millisecond)},
			}, 66668, callAtEvery, advance)
			verifyNextSeries(t, ss, labels.FromStrings("__name__", "second"), 5*time.Millisecond, []timeRange{
				{now, now.Add(600*time.Second - 5*time.Millisecond)},
			}, 120000, callAtEvery, advance)
			verifyNextSeries(t, ss, labels.FromStrings("__name__", "overlapping"), 5*time.Millisecond, []timeRange{
				{now, now.Add(15*time.Second - 5*time.Millisecond)},
			}, 3000, callAtEvery, advance)
			verifyNextSeries(t, ss, labels.FromStrings("__name__", "overlapping2"), 5*time.Millisecond, []timeRange{
				{now, now.Add(10*time.Second - 5*time.Millisecond)},
				{now.Add(20 * time.Second), now.Add(30*time.Second - 5*time.Millisecond)},
			}, 4000, callAtEvery, advance)
			verifyNextSeries(t, ss, labels.FromStrings("__name__", "many_empty_chunks"), 5*time.Millisecond, []timeRange{
				{now, now.Add(30*time.Second - 5*time.Millisecond)},
			}, 6000, callAtEvery, advance)
			verifyNextSeries(t, ss, labels.FromStrings("__name__", "overlapping_chunks_with_additional_samples_in_sequence"), time.Millisecond, []timeRange{
				{now, now.Add(7 * time.Millisecond)},
			}, 8, callAtEvery, advance)

			require.False(t, ss.Next())
		})

		t.Run(fmt.Sprintf("consume with alternating calls to .Seek() and .Next() method, perform .At() after every %dth call to .Seek() or .Next()", callAtEvery), func(t *testing.T) {
			t.Parallel()

			var seek bool
			advance := func(it chunkenc.Iterator, wantTs int64) chunkenc.ValueType {
				seek = !seek
				if seek {
					return it.Seek(wantTs)
				}
				return it.Next()
			}
			ss := getSeriesSet()

			verifyNextSeries(t, ss, labels.FromStrings("__name__", "first", "a", "a"), 3*time.Millisecond, []timeRange{
				{now, now.Add(100*time.Second - time.Millisecond)},
				{now.Add(100 * time.Second), now.Add(200*time.Second - time.Millisecond)},
			}, 66668, callAtEvery, advance)
			verifyNextSeries(t, ss, labels.FromStrings("__name__", "second"), 5*time.Millisecond, []timeRange{
				{now, now.Add(600*time.Second - 5*time.Millisecond)},
			}, 120000, callAtEvery, advance)
			verifyNextSeries(t, ss, labels.FromStrings("__name__", "overlapping"), 5*time.Millisecond, []timeRange{
				{now, now.Add(15*time.Second - 5*time.Millisecond)},
			}, 3000, callAtEvery, advance)
			verifyNextSeries(t, ss, labels.FromStrings("__name__", "overlapping2"), 5*time.Millisecond, []timeRange{
				{now, now.Add(10*time.Second - 5*time.Millisecond)},
				{now.Add(20 * time.Second), now.Add(30*time.Second - 5*time.Millisecond)},
			}, 4000, callAtEvery, advance)
			verifyNextSeries(t, ss, labels.FromStrings("__name__", "many_empty_chunks"), 5*time.Millisecond, []timeRange{
				{now, now.Add(30*time.Second - 5*time.Millisecond)},
			}, 6000, callAtEvery, advance)
			verifyNextSeries(t, ss, labels.FromStrings("__name__", "overlapping_chunks_with_additional_samples_in_sequence"), time.Millisecond, []timeRange{
				{now, now.Add(7 * time.Millisecond)},
			}, 8, callAtEvery, advance)

			require.False(t, ss.Next())
		})
	}
}

// verifyNextSeries verifies a series by consuming it via a given consumer function.
// "step" is the time distance between samples.
// "ranges" is a slice of timeRanges where each timeRange consists of {minT,maxT}.
// "samples" is the expected total number of samples.
// "callAtEvery" defines after every how many samples we want to call .At().
// "advance" is a function which takes an iterator and advances its position.
func verifyNextSeries(t *testing.T, ss storage.SeriesSet, labels labels.Labels, step time.Duration, ranges []timeRange, samples, callAtEvery uint32, advance func(chunkenc.Iterator, int64) chunkenc.ValueType) {
	require.True(t, ss.Next())

	s := ss.At()
	require.Equal(t, labels, s.Labels())

	var count uint32
	it := s.Iterator(nil)
	for _, r := range ranges {
		for wantTs := r.minT.UnixNano() / 1000000; wantTs <= r.maxT.UnixNano()/1000000; wantTs += step.Milliseconds() {
			require.Equal(t, chunkenc.ValFloat, advance(it, wantTs))

			if count%callAtEvery == 0 {
				gotTs, v := it.At()
				require.Equal(t, wantTs, gotTs)
				require.Equal(t, math.Sin(float64(wantTs)), v)
			}

			count++
		}
	}

	require.Equal(t, samples, count)
}

// createAggrChunkWithSineSamples takes a min/maxTime and a step duration, it generates a chunk given these specs.
// The minTime and maxTime are both inclusive.
func createAggrChunkWithSineSamples(minTime, maxTime time.Time, step time.Duration) storepb.AggrChunk {
	var samples []promql.FPoint

	minT := minTime.UnixNano() / 1000000
	maxT := maxTime.UnixNano() / 1000000
	stepMillis := step.Milliseconds()

	for t := minT; t <= maxT; t += stepMillis {
		samples = append(samples, promql.FPoint{T: t, F: math.Sin(float64(t))})
	}

	return createAggrChunk(minT, maxT, samples...)
}

func createAggrChunkWithSamples(samples ...promql.FPoint) storepb.AggrChunk {
	return createAggrChunk(samples[0].T, samples[len(samples)-1].T, samples...)
}

func createAggrChunk(minTime, maxTime int64, samples ...promql.FPoint) storepb.AggrChunk {
	// Ensure samples are sorted by timestamp.
	sort.Slice(samples, func(i, j int) bool {
		return samples[i].T < samples[j].T
	})

	chunk := chunkenc.NewXORChunk()
	appender, err := chunk.Appender()
	if err != nil {
		panic(err)
	}

	for _, s := range samples {
		appender.Append(s.T, s.F)
	}

	return storepb.AggrChunk{
		MinTime: minTime,
		MaxTime: maxTime,
		Raw: storepb.Chunk{
			Type: storepb.Chunk_XOR,
			Data: chunk.Bytes(),
		},
	}
}

func mkZLabels(s ...string) []mimirpb.LabelAdapter {
	var result []mimirpb.LabelAdapter

	for i := 0; i+1 < len(s); i = i + 2 {
		result = append(result, mimirpb.LabelAdapter{
			Name:  s[i],
			Value: s[i+1],
		})
	}

	return result
}

func Benchmark_newBlockQuerierSeries(b *testing.B) {
	lbls := labels.FromStrings(
		"__name__", "test",
		"label_1", "value_1",
		"label_2", "value_2",
		"label_3", "value_3",
		"label_4", "value_4",
		"label_5", "value_5",
		"label_6", "value_6",
		"label_7", "value_7",
		"label_8", "value_8",
		"label_9", "value_9")

	chunks := []storepb.AggrChunk{
		createAggrChunkWithSineSamples(time.Now(), time.Now().Add(-time.Hour), time.Minute),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newBlockQuerierSeries(lbls, chunks)
	}
}

func Benchmark_blockQuerierSeriesSet_iteration(b *testing.B) {
	const (
		numSeries          = 8000
		numSamplesPerChunk = 240
		numChunksPerSeries = 24
	)

	// Generate series.
	series := make([]*storepb.Series, 0, numSeries)
	for seriesID := 0; seriesID < numSeries; seriesID++ {
		lbls := mkZLabels("__name__", "test", "series_id", strconv.Itoa(seriesID))
		chunks := make([]storepb.AggrChunk, 0, numChunksPerSeries)

		// Create chunks with 1 sample per second.
		for minT := int64(0); minT < numChunksPerSeries*numSamplesPerChunk; minT += numSamplesPerChunk {
			chunks = append(chunks, createAggrChunkWithSineSamples(util.TimeFromMillis(minT), util.TimeFromMillis(minT+numSamplesPerChunk), time.Millisecond))
		}

		series = append(series, &storepb.Series{
			Labels: lbls,
			Chunks: chunks,
		})
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		set := blockQuerierSeriesSet{series: series}

		var t chunkenc.Iterator
		for set.Next() {
			for t = set.At().Iterator(t); t.Next() != chunkenc.ValNone; {
				t.At()
			}
		}
	}
}

func Benchmark_blockQuerierSeriesSet_seek(b *testing.B) {
	const (
		numSeries          = 100
		numSamplesPerChunk = 120
		numChunksPerSeries = 500
		samplesPerStep     = 720 // equal to querying 15sec interval data with a "step" of 3h
	)

	// Generate series.
	series := make([]*storepb.Series, 0, numSeries)
	for seriesID := 0; seriesID < numSeries; seriesID++ {
		lbls := mkZLabels("__name__", "test", "series_id", strconv.Itoa(seriesID))
		chunks := make([]storepb.AggrChunk, 0, numChunksPerSeries)

		// Create chunks with 1 sample per second.
		for minT := int64(0); minT < numChunksPerSeries*numSamplesPerChunk; minT += numSamplesPerChunk {
			chunks = append(chunks, createAggrChunkWithSineSamples(util.TimeFromMillis(minT), util.TimeFromMillis(minT+numSamplesPerChunk), time.Millisecond))
		}

		series = append(series, &storepb.Series{
			Labels: lbls,
			Chunks: chunks,
		})
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		set := blockQuerierSeriesSet{series: series}

		var t chunkenc.Iterator
		for set.Next() {
			seekT := int64(0)
			for t = set.At().Iterator(t); t.Seek(seekT) != chunkenc.ValNone; seekT += samplesPerStep {
				t.At()
			}
		}
	}
}

func TestBlockQuerierSeriesIterator_ShouldMergeOverlappingChunks(t *testing.T) {
	// Create 3 chunks with overlapping timeranges, but each sample is only in 1 chunk.
	chunk1 := createAggrChunkWithSamples(promql.FPoint{T: 1, F: 1}, promql.FPoint{T: 3, F: 3}, promql.FPoint{T: 7, F: 7})
	chunk2 := createAggrChunkWithSamples(promql.FPoint{T: 2, F: 2}, promql.FPoint{T: 5, F: 5}, promql.FPoint{T: 9, F: 9})
	chunk3 := createAggrChunkWithSamples(promql.FPoint{T: 4, F: 4}, promql.FPoint{T: 6, F: 6}, promql.FPoint{T: 8, F: 8})

	permutations := [][]storepb.AggrChunk{
		{chunk1, chunk2, chunk3},
		{chunk1, chunk3, chunk2},
		{chunk2, chunk1, chunk3},
		{chunk2, chunk3, chunk1},
		{chunk3, chunk1, chunk2},
		{chunk3, chunk2, chunk1},
	}

	for idx, permutation := range permutations {
		t.Run(fmt.Sprintf("permutation %d", idx), func(t *testing.T) {
			it := newBlockQuerierSeriesIterator(nil, labels.EmptyLabels(), permutation)

			var actual []promql.FPoint
			for it.Next() != chunkenc.ValNone {
				ts, value := it.At()
				actual = append(actual, promql.FPoint{T: ts, F: value})

				require.Equal(t, ts, it.AtT())
			}

			require.NoError(t, it.Err())

			require.Equal(t, []promql.FPoint{
				{T: 1, F: 1},
				{T: 2, F: 2},
				{T: 3, F: 3},
				{T: 4, F: 4},
				{T: 5, F: 5},
				{T: 6, F: 6},
				{T: 7, F: 7},
				{T: 8, F: 8},
				{T: 9, F: 9},
			}, actual)
		})
	}
}

func TestBlockQuerierSeriesIterator_ShouldMergeNonOverlappingChunks(t *testing.T) {
	chunk1 := createAggrChunkWithSamples(promql.FPoint{T: 1, F: 1}, promql.FPoint{T: 2, F: 2}, promql.FPoint{T: 3, F: 3})
	chunk2 := createAggrChunkWithSamples(promql.FPoint{T: 4, F: 4}, promql.FPoint{T: 5, F: 5}, promql.FPoint{T: 6, F: 6})

	it := newBlockQuerierSeriesIterator(nil, labels.EmptyLabels(), []storepb.AggrChunk{chunk1, chunk2})

	var actual []promql.FPoint
	for it.Next() != chunkenc.ValNone {
		ts, value := it.At()
		actual = append(actual, promql.FPoint{T: ts, F: value})

		require.Equal(t, ts, it.AtT())
	}

	require.NoError(t, it.Err())

	require.Equal(t, []promql.FPoint{
		{T: 1, F: 1},
		{T: 2, F: 2},
		{T: 3, F: 3},
		{T: 4, F: 4},
		{T: 5, F: 5},
		{T: 6, F: 6},
	}, actual)
}

func TestBlockQuerierSeriesIterator_SeekWithNonOverlappingChunks(t *testing.T) {
	chunk1 := createAggrChunkWithSamples(promql.FPoint{T: 1, F: 1}, promql.FPoint{T: 2, F: 2}, promql.FPoint{T: 3, F: 3})
	chunk2 := createAggrChunkWithSamples(promql.FPoint{T: 4, F: 4}, promql.FPoint{T: 5, F: 5}, promql.FPoint{T: 6, F: 6})
	chunk3 := createAggrChunkWithSamples(promql.FPoint{T: 7, F: 7}, promql.FPoint{T: 8, F: 8}, promql.FPoint{T: 9, F: 9})

	it := newBlockQuerierSeriesIterator(nil, labels.EmptyLabels(), []storepb.AggrChunk{chunk1, chunk2, chunk3})

	// Seek to middle of first chunk.
	require.Equal(t, chunkenc.ValFloat, it.Seek(2))
	ts, value := it.At()
	require.Equal(t, int64(2), ts)
	require.Equal(t, 2.0, value)

	// Seek to end of first chunk.
	require.Equal(t, chunkenc.ValFloat, it.Seek(3))
	ts, value = it.At()
	require.Equal(t, int64(3), ts)
	require.Equal(t, 3.0, value)

	// Seek to the start of the second chunk.
	require.Equal(t, chunkenc.ValFloat, it.Seek(4))
	ts, value = it.At()
	require.Equal(t, int64(4), ts)
	require.Equal(t, 4.0, value)

	// Seek to the middle of the last chunk, and check all the remaining points are as we expect.
	require.Equal(t, chunkenc.ValFloat, it.Seek(8))

	var actual []promql.FPoint
	for {
		ts, value := it.At()
		actual = append(actual, promql.FPoint{T: ts, F: value})

		require.Equal(t, ts, it.AtT())

		// Keep consuming points until we've exhausted all remaining points.
		if it.Next() == chunkenc.ValNone {
			break
		}
	}

	require.NoError(t, it.Err())

	require.Equal(t, []promql.FPoint{
		{T: 8, F: 8},
		{T: 9, F: 9},
	}, actual)
}

func TestBlockQuerierSeriesIterator_SeekPastEnd(t *testing.T) {
	chunk1 := createAggrChunkWithSamples(promql.FPoint{T: 1, F: 1}, promql.FPoint{T: 2, F: 2}, promql.FPoint{T: 3, F: 3})
	chunk2 := createAggrChunkWithSamples(promql.FPoint{T: 4, F: 4}, promql.FPoint{T: 5, F: 5}, promql.FPoint{T: 6, F: 6})
	chunk3 := createAggrChunkWithSamples(promql.FPoint{T: 7, F: 7}, promql.FPoint{T: 8, F: 8}, promql.FPoint{T: 9, F: 9})

	it := newBlockQuerierSeriesIterator(nil, labels.EmptyLabels(), []storepb.AggrChunk{chunk1, chunk2, chunk3})
	require.Equal(t, chunkenc.ValNone, it.Seek(10))
}
