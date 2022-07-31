// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/batch.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

// GenericChunk is a generic chunk used by the batch iterator, in order to make the batch
// iterator general purpose.
type GenericChunk struct {
	MinTime int64
	MaxTime int64

	iterator func(reuse chunk.Iterator) chunk.Iterator
}

func NewGenericChunk(minTime, maxTime int64, iterator func(reuse chunk.Iterator) chunk.Iterator) GenericChunk {
	return GenericChunk{
		MinTime:  minTime,
		MaxTime:  maxTime,
		iterator: iterator,
	}
}

func (c GenericChunk) Iterator(reuse chunk.Iterator) chunk.Iterator {
	return c.iterator(reuse)
}

// iterator iterates over batches.
type iterator interface {
	// Seek to the batch at (or after) time t.
	Seek(t int64, size int) chunkenc.ValueType

	// Next moves to the next batch.
	Next(size int) chunkenc.ValueType

	// AtTime returns the start time of the next batch.  Must only be called after
	// Seek or Next have returned true.
	AtTime() int64

	// Batch returns the current batch.  Must only be called after Seek or Next
	// have returned true.
	Batch() chunk.Batch

	Err() error
}

// NewChunkMergeIterator returns a chunkenc.Iterator that merges Mimir chunks together.
func NewChunkMergeIterator(chunks []chunk.Chunk, _, _ model.Time) chunkenc.Iterator {
	converted := make([]GenericChunk, len(chunks))
	for i, c := range chunks {
		converted[i] = NewGenericChunk(int64(c.From), int64(c.Through), c.Data.NewIterator)
	}

	return NewGenericChunkMergeIterator(converted)
}

// NewGenericChunkMergeIterator returns a chunkenc.Iterator that merges generic chunks together.
func NewGenericChunkMergeIterator(chunks []GenericChunk) chunkenc.Iterator {
	iter := newMergeIterator(chunks)
	return newIteratorAdapter(iter)
}

// iteratorAdapter turns a batchIterator into a chunkenc.Iterator.
// It fetches ever increasing batchSizes (up to promchunk.BatchSize) on each
// call to Next; on calls to Seek, resets batch size to 1.
type iteratorAdapter struct {
	batchSize  int
	curr       chunk.Batch
	underlying iterator
}

func newIteratorAdapter(underlying iterator) chunkenc.Iterator {
	return &iteratorAdapter{
		batchSize:  1,
		underlying: underlying,
	}
}

// Seek implements chunkenc.Iterator.
func (a *iteratorAdapter) Seek(t int64) chunkenc.ValueType {

	// Optimisation: fulfill the seek using current batch if possible.
	if a.curr.Length > 0 && a.curr.Index < a.curr.Length {
		if t <= a.curr.Timestamps[a.curr.Index] {
			//In this case, the interface's requirement is met, so state of this
			//iterator does not need any change.
			return a.curr.ValueTypes
		} else if t <= a.curr.Timestamps[a.curr.Length-1] {
			//In this case, some timestamp between current sample and end of batch can fulfill
			//the seek. Let's find it.
			for a.curr.Index < a.curr.Length && t > a.curr.Timestamps[a.curr.Index] {
				a.curr.Index++
			}
			return a.curr.ValueTypes
		}
	}

	a.curr.Length = -1
	a.batchSize = 1
	if typ := a.underlying.Seek(t, a.batchSize); typ != chunkenc.ValNone {
		a.curr = a.underlying.Batch()
		if a.curr.Index < a.curr.Length {
			return typ
		}
	}
	return chunkenc.ValNone
}

// Next implements chunkenc.Iterator.
func (a *iteratorAdapter) Next() chunkenc.ValueType {
	a.curr.Index++
	for a.curr.Index >= a.curr.Length && a.underlying.Next(a.batchSize) != chunkenc.ValNone {
		a.curr = a.underlying.Batch()
		a.batchSize = a.batchSize * 2
		if a.batchSize > chunk.BatchSize {
			a.batchSize = chunk.BatchSize
		}
	}

	if a.curr.Index < a.curr.Length {
		return a.curr.ValueTypes
	}
	return chunkenc.ValNone
}

// At implements chunkenc.Iterator.
func (a *iteratorAdapter) At() (int64, float64) {
	return a.curr.Timestamps[a.curr.Index], a.curr.SampleValues[a.curr.Index]
}

// AtHistogram implements chunkenc.Iterator.
func (a *iteratorAdapter) AtHistogram() (int64, *histogram.Histogram) {
	return a.curr.Timestamps[a.curr.Index], a.curr.HistogramValues[a.curr.Index]
}

// AtFloatHistogram implements chunkenc.Iterator.
func (a *iteratorAdapter) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	// It seems this method is sometimes called when there aren't any float histograms
	// but there are regular histograms, so we can get of those instead.
	var h *histogram.FloatHistogram
	if a.curr.FloatHistogramValues != nil {
		h = a.curr.FloatHistogramValues[a.curr.Index]
	}
	if h == nil {
		h = a.curr.HistogramValues[a.curr.Index].ToFloat()
	}
	return a.curr.Timestamps[a.curr.Index], h
}

// AtT implements chunkenc.Iterator.
func (a *iteratorAdapter) AtT() int64 {
	return a.curr.Timestamps[a.curr.Index]
}

// Err implements chunkenc.Iterator.
func (a *iteratorAdapter) Err() error {
	return nil
}
