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
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/chunk"
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
					it = NewChunkMergeIterator(it, lbls, chunks)
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

	sut := NewChunkMergeIterator(nil, labels.EmptyLabels(), chunks)

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
