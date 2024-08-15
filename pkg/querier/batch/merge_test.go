// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/merge_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
	"fmt"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

func TestMergeIter(t *testing.T) {
	for _, enc := range []chunk.Encoding{chunk.PrometheusXorChunk, chunk.PrometheusHistogramChunk, chunk.PrometheusFloatHistogramChunk} {
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

// TestBugInMergeIterator tests a bug while calling Seek() on mergeIterator.
// The samples used for testing is from a live Mimir cluster where the bug was first observed.
func TestBugInMergeIteratorSeek(t *testing.T) {
	// Samples for 3 chunks.
	chunkSamples := [][]int64{
		{1723419966795, 1723420626795, 1723420686795, 1723420746795},
		{1723420806795, 1723420866795, 1723420926795, 1723427346795, 1723427406795, 1723427946795},
		{1723428006795, 1723428546795},
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

	c3It.Seek(1723420500000)
	// These Next() calls are necessary to reproduce the bug.
	c3It.Next()
	c3It.Next()
	c3It.Next()
	c3It.Seek(1723421700000)
	// Without the bug fix, this Seek() call skips an additional sample than desired.
	// Instead of stopping at 1723427946795, which is the last sample of chunk 2,
	// it would go to 1723428006795, which is the first sample of chunk 3.
	// This was happening because in the Seek() method we were discarding the current
	// batch from mergeIterator if the batch's first sample was after the seek time.
	c3It.Seek(1723427700000)

	require.Equal(t, int64(1723427946795), c3It.AtT())
}

func readFloats(t *testing.T, chunks ...storepb.AggrChunk) []promql.FPoint {
	fmt.Println("READ FLOATS")
	it := newBlockQuerierSeriesIterator(nil, labels.EmptyLabels(), chunks)
	var firstIteratorPoints []promql.FPoint
	for it.Next() != chunkenc.ValNone {
		ts, value := it.At()
		firstIteratorPoints = append(firstIteratorPoints, promql.FPoint{T: ts, F: value})
		fmt.Println(ts)

		require.Equal(t, ts, it.AtT())
	}

	require.NoError(t, it.Err())
	return firstIteratorPoints
}

func newBlockQuerierSeriesIterator(reuse chunkenc.Iterator, lbls labels.Labels, chunks []storepb.AggrChunk) chunkenc.Iterator {
	genericChunks := make([]GenericChunk, 0, len(chunks))

	for _, c := range chunks {
		c := c

		genericChunk := NewGenericChunk(c.MinTime, c.MaxTime, func(reuse chunk.Iterator) chunk.Iterator {
			encoding, ok := c.GetChunkEncoding()
			if !ok {
				return chunk.ErrorIterator(fmt.Sprintf("cannot create new chunk for series %s: unknown encoded raw data type %v", lbls, c.Raw.Type))
			}

			ch, err := chunk.NewForEncoding(encoding)
			if err != nil {
				return chunk.ErrorIterator(fmt.Sprintf("cannot create new chunk for series %s: %s", lbls.String(), err.Error()))
			}

			if err := ch.UnmarshalFromBuf(c.Raw.Data); err != nil {
				return chunk.ErrorIterator(fmt.Sprintf("cannot unmarshal chunk for series %s: %s", lbls.String(), err.Error()))
			}

			return ch.NewIterator(reuse)
		})

		genericChunks = append(genericChunks, genericChunk)
	}

	return NewGenericChunkMergeIterator(reuse, lbls, genericChunks)
}
