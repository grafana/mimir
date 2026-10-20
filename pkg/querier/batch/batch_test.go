// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/batch_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/util/test"
)

func BenchmarkNewChunkMergeIterator_CreateAndIterate(b *testing.B) {
	scenarios := []struct {
		numChunks          int
		numSamplesPerChunk int
		duplicationFactor  int
	}{
		{numChunks: 1000, numSamplesPerChunk: 100, duplicationFactor: 1},
		{numChunks: 1000, numSamplesPerChunk: 100, duplicationFactor: 3},
		{numChunks: 100, numSamplesPerChunk: 100, duplicationFactor: 1},
		{numChunks: 100, numSamplesPerChunk: 100, duplicationFactor: 3},
		{numChunks: 1, numSamplesPerChunk: 100, duplicationFactor: 1},
		{numChunks: 1, numSamplesPerChunk: 100, duplicationFactor: 3},
	}

	lbls := labels.EmptyLabels()

	for _, scenario := range scenarios {
		for _, encoding := range []chunk.Encoding{chunk.PrometheusXorChunk, chunk.PrometheusHistogramChunk, chunk.PrometheusFloatHistogramChunk} {
			name := fmt.Sprintf("chunks: %d samples per chunk: %d duplication factor: %d encoding: %s", scenario.numChunks, scenario.numSamplesPerChunk, scenario.duplicationFactor, encoding)
			chunks := createChunks(b, scenario.numChunks, scenario.numSamplesPerChunk, scenario.duplicationFactor, encoding)
			var it chunkenc.Iterator
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()

				var (
					h  *histogram.Histogram
					fh *histogram.FloatHistogram
				)
				for n := 0; n < b.N; n++ {
					it = NewChunkMergeIterator(it, lbls, chunks, IteratorOptions{})
					for valType := it.Next(); valType != chunkenc.ValNone; valType = it.Next() {
						switch valType {
						case chunkenc.ValFloat:
							it.At()
						case chunkenc.ValHistogram:
							_, h = it.AtHistogram(h)
						case chunkenc.ValFloatHistogram:
							_, fh = it.AtFloatHistogram(fh)
						default:
							panic(fmt.Sprintf("Unknown type detected %v", valType))
						}
					}

					// Ensure no error occurred.
					if it.Err() != nil {
						b.Fatal(it.Err().Error())
					}
				}
			})
		}
	}
}

func TestSeekCorrectlyDealWithSinglePointChunks(t *testing.T) {
	chunkOne := mkChunk(t, model.Time(1*step/time.Millisecond), 1, chunk.PrometheusXorChunk)
	chunkTwo := mkChunk(t, model.Time(10*step/time.Millisecond), 1, chunk.PrometheusXorChunk)
	chunks := []chunk.Chunk{chunkOne, chunkTwo}

	sut := NewChunkMergeIterator(nil, labels.EmptyLabels(), chunks, IteratorOptions{})

	// Following calls mimics Prometheus's query engine behaviour for VectorSelector.
	require.Equal(t, chunkenc.ValFloat, sut.Next())
	require.Equal(t, chunkenc.ValFloat, sut.Seek(0))

	actual, val := sut.At()
	require.Equal(t, float64(1*time.Second/time.Millisecond), val) // since mkChunk use ts as value.
	require.Equal(t, int64(1*time.Second/time.Millisecond), actual)
}

func createChunks(b *testing.B, numChunks, numSamplesPerChunk, duplicationFactor int, enc chunk.Encoding) []chunk.Chunk {
	result := make([]chunk.Chunk, 0, numChunks)

	for d := 0; d < duplicationFactor; d++ {
		for c := 0; c < numChunks; c++ {
			minTime := step * time.Duration(c*numSamplesPerChunk)
			result = append(result, mkChunk(b, model.Time(minTime.Milliseconds()), numSamplesPerChunk, enc))
		}
	}

	return result
}

func TestNewChunkMergeIterator_ShouldGuaranteeDeterminismIteratingTwoSamplesWithSameTimestamp(t *testing.T) {
	metric := labels.FromStrings(model.MetricNameLabel, "test")

	first := NewChunkMergeIterator(nil, metric, []chunk.Chunk{
		createEncodedChunk(t, metric,
			model.SamplePair{Timestamp: 1720588148418, Value: 23084.411222},
			model.SamplePair{Timestamp: 1720588152848, Value: 169084.077208}, // Clashing sample.
		),
		createEncodedChunk(t, metric,
			model.SamplePair{Timestamp: 1720588092946, Value: 875741.198983},
			model.SamplePair{Timestamp: 1720588152848, Value: 30455667.651284}, // Clashing sample.
		),
	}, IteratorOptions{})

	second := NewChunkMergeIterator(nil, metric, []chunk.Chunk{
		createEncodedChunk(t, metric,
			model.SamplePair{Timestamp: 1720588152848, Value: 169084.077208}, // Clashing sample.
		),
		createEncodedChunk(t, metric,
			model.SamplePair{Timestamp: 1720588092946, Value: 875741.198983},
			model.SamplePair{Timestamp: 1720588148418, Value: 23084.411222},
			model.SamplePair{Timestamp: 1720588152848, Value: 30455667.651284}, // Clashing sample.
		),
	}, IteratorOptions{})

	assert.Equal(t, iterateEncodedChunks(t, first), iterateEncodedChunks(t, second))
}

func createEncodedChunk(t *testing.T, metric labels.Labels, samples ...model.SamplePair) chunk.Chunk {
	promChunk, err := chunk.NewForEncoding(chunk.PrometheusXorChunk)
	require.NoError(t, err)

	for _, sample := range samples {
		_, err = promChunk.Add(sample)
		require.NoError(t, err)
	}

	return chunk.NewChunk(metric, promChunk, samples[0].Timestamp, samples[len(samples)-1].Timestamp)
}

func iterateEncodedChunks(t *testing.T, it chunkenc.Iterator) []model.SamplePair {
	var actualSamples []model.SamplePair

	for it.Next() != chunkenc.ValNone {
		ts, value := it.At()
		actualSamples = append(actualSamples, model.SamplePair{
			Timestamp: model.Time(ts),
			Value:     model.SampleValue(value),
		})
	}

	require.NoError(t, it.Err())

	return actualSamples
}

func TestNewChunkMergeIterator_HistogramStartTimestamps(t *testing.T) {
	stepMs := step.Milliseconds()

	startTimestamp := func(ts int64) int64 {
		return ts - stepMs/2
	}

	mkHistogramChunk := func(t *testing.T, encoding chunk.Encoding, promEncoding chunkenc.Encoding, firstStep, lastStep int) chunk.Chunk {
		promChunk, err := chunkenc.NewEmptyChunk(promEncoding)
		require.NoError(t, err)
		app, err := promChunk.Appender()
		require.NoError(t, err)

		floatHistograms := promEncoding == chunkenc.EncFloatHistogram || promEncoding == chunkenc.EncFloatHistogramST
		for n := firstStep; n <= lastStep; n++ {
			ts := int64(n) * stepMs
			var newChunk chunkenc.Chunk
			if floatHistograms {
				newChunk, _, app, err = app.AppendFloatHistogram(nil, startTimestamp(ts), ts, test.GenerateTestFloatHistogram(n), true)
			} else {
				newChunk, _, app, err = app.AppendHistogram(nil, startTimestamp(ts), ts, test.GenerateTestHistogram(n), true)
			}
			require.NoError(t, err)
			require.Nil(t, newChunk)
		}

		encoded, err := chunk.NewForEncoding(encoding)
		require.NoError(t, err)
		require.NoError(t, encoded.UnmarshalFromBuf(promChunk.Bytes()))
		return chunk.NewChunk(labels.FromStrings(model.MetricNameLabel, "foo"), encoded, model.Time(int64(firstStep)*stepMs), model.Time(int64(lastStep)*stepMs))
	}

	for _, tc := range []struct {
		encoding     chunk.Encoding
		promEncoding chunkenc.Encoding
		supportsST   bool
	}{
		{encoding: chunk.PrometheusHistogramChunk, promEncoding: chunkenc.EncHistogram},
		{encoding: chunk.PrometheusHistogramSTChunk, promEncoding: chunkenc.EncHistogramST, supportsST: true},
		{encoding: chunk.PrometheusFloatHistogramChunk, promEncoding: chunkenc.EncFloatHistogram},
		{encoding: chunk.PrometheusFloatHistogramSTChunk, promEncoding: chunkenc.EncFloatHistogramST, supportsST: true},
	} {
		t.Run(tc.encoding.String(), func(t *testing.T) {
			// Overlapping chunks, so samples go through both the chunk iterator and the merge.
			chunks := []chunk.Chunk{
				mkHistogramChunk(t, tc.encoding, tc.promEncoding, 1, 60),
				mkHistogramChunk(t, tc.encoding, tc.promEncoding, 31, 90),
			}

			for _, collect := range []bool{true, false} {
				it := NewChunkMergeIterator(nil, labels.EmptyLabels(), chunks, IteratorOptions{CollectStartTimestamps: collect})

				for n := 1; n <= 90; n++ {
					ts := int64(n) * stepMs
					expectedST := int64(0)
					if collect && tc.supportsST {
						expectedST = startTimestamp(ts)
					}

					require.NotEqual(t, chunkenc.ValNone, it.Next(), "collect=%v: expected a sample at %d", collect, ts)
					require.Equal(t, ts, it.AtT(), "collect=%v", collect)
					require.Equal(t, expectedST, it.AtST(), "collect=%v: start timestamp of the sample at %d", collect, ts)
				}
				require.Equal(t, chunkenc.ValNone, it.Next(), "collect=%v: expected no more samples", collect)
				require.NoError(t, it.Err())
			}
		})
	}
}
