// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/merge_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

func TestMergeIter(t *testing.T) {
	for _, enc := range []chunk.Encoding{/*chunk.PrometheusXorChunk,*/ chunk.PrometheusHistogramChunk/*, chunk.PrometheusFloatHistogramChunk*/} {
		t.Run(enc.String(), func(t *testing.T) {
			chunk1 := mkGenericChunk(t, 0, 100, enc)
			chunk2 := mkGenericChunk(t, model.TimeFromUnix(25), 100, enc)
			chunk3 := mkGenericChunk(t, model.TimeFromUnix(50), 100, enc)
			chunk4 := mkGenericChunk(t, model.TimeFromUnix(75), 100, enc)
			chunk5 := mkGenericChunk(t, model.TimeFromUnix(100), 100, enc)

			iter := NewGenericChunkMergeIterator(nil, labels.EmptyLabels(), []GenericChunk{chunk1, chunk2, chunk3, chunk4, chunk5})
			testIter(t, 200, iter, enc)
			iter = NewGenericChunkMergeIterator(nil, labels.EmptyLabels(), []GenericChunk{chunk1, chunk2, chunk3, chunk4, chunk5})
			testSeek(t, 200, iter, enc)

			// Re-use iterator.
			iter = NewGenericChunkMergeIterator(iter, labels.EmptyLabels(), []GenericChunk{chunk1, chunk2, chunk3, chunk4, chunk5})
			testIter(t, 200, iter, enc)
			iter = NewGenericChunkMergeIterator(iter, labels.EmptyLabels(), []GenericChunk{chunk1, chunk2, chunk3, chunk4, chunk5})
			testSeek(t, 200, iter, enc)
		})
	}
}

func TestMergeHarder(t *testing.T) {
	var (
		numChunks = 24 * 15
		chunks    = make([]GenericChunk, 0, numChunks)
		offset    = 30
		samples   = 100
	)
	for _, enc := range []chunk.Encoding{chunk.PrometheusXorChunk, chunk.PrometheusHistogramChunk, chunk.PrometheusFloatHistogramChunk} {
		t.Run(enc.String(), func(t *testing.T) {
			chunks = chunks[:0]
			from := model.Time(0)
			for i := 0; i < numChunks; i++ {
				chunks = append(chunks, mkGenericChunk(t, from, samples, enc))
				from = from.Add(time.Duration(offset) * time.Second)
			}
			iter := newMergeIterator(nil, chunks)
			testIter(t, offset*numChunks+samples-offset, newIteratorAdapter(nil, iter, labels.EmptyLabels()), enc)

			iter = newMergeIterator(nil, chunks)
			testSeek(t, offset*numChunks+samples-offset, newIteratorAdapter(nil, iter, labels.EmptyLabels()), enc)
		})
	}
}

// TestMergeIteratorSeek tests a bug while calling Seek() on mergeIterator.
func TestMergeIteratorSeek(t *testing.T) {
	// Samples for 3 chunks.
	chunkSamples := [][]int64{
		{10, 20, 30, 40},
		{50, 60, 70, 80, 90, 100},
		{110, 120},
	}

	var genericChunks []GenericChunk
	for _, samples := range chunkSamples {
		ch := chunkenc.NewXORChunk()
		app, err := ch.Appender()
		require.NoError(t, err)
		for _, ts := range samples {
			app.Append(ts, 1)
		}

		genericChunks = append(genericChunks, NewGenericChunk(samples[0], samples[len(samples)-1], func(reuse chunk.Iterator) chunk.Iterator {
			chk, err := chunk.NewForEncoding(chunk.PrometheusXorChunk)
			require.NoError(t, err)
			require.NoError(t, chk.UnmarshalFromBuf(ch.Bytes()))
			return chk.NewIterator(reuse)
		}))
	}

	c3It := NewGenericChunkMergeIterator(nil, labels.EmptyLabels(), genericChunks)

	c3It.Seek(15)
	// These Next() calls are necessary to reproduce the bug.
	c3It.Next()
	c3It.Next()
	c3It.Next()
	c3It.Seek(75)
	// Without the bug fix, this Seek() call skips an additional sample than desired.
	// Instead of stopping at 100, which is the last sample of chunk 2,
	// it would go to 110, which is the first sample of chunk 3.
	// This was happening because in the Seek() method we were discarding the current
	// batch from mergeIterator if the batch's first sample was after the seek time.
	c3It.Seek(95)

	require.Equal(t, int64(100), c3It.AtT())
}
