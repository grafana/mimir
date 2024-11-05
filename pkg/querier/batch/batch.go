// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/batch.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
	"fmt"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

// GenericChunk is a generic chunk used by the batch iterator, in order to make the batch
// iterator general purpose.
type GenericChunk struct {
	MinTime int64
	MaxTime int64
	// The maximum time of the previous chunk from the same stream/array.
	PrevMaxTime int64

	iterator func(reuse chunk.Iterator) chunk.Iterator
}

func NewGenericChunk(minTime, maxTime, prevMaxTime int64, iterator func(reuse chunk.Iterator) chunk.Iterator) GenericChunk {
	return GenericChunk{
		MinTime:     minTime,
		MaxTime:     maxTime,
		PrevMaxTime: prevMaxTime,
		iterator:    iterator,
	}
}

func (c GenericChunk) Iterator(reuse chunk.Iterator) chunk.Iterator {
	return c.iterator(reuse)
}

// iterator iterates over batches.
type iterator interface {
	// Seek advances the iterator forward to batch containing the sample at or after the given timestamp.
	// If the current batch contains a sample at or after the given timestamp, then Seek retains the current batch.
	//
	// The batch's Index is advanced to point to the sample at or after the given timestamp.
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
func NewChunkMergeIterator(it chunkenc.Iterator, lbls labels.Labels, chunks []chunk.Chunk) chunkenc.Iterator {
	converted := make([]GenericChunk, len(chunks))
	for i, c := range chunks {
		converted[i] = NewGenericChunk(int64(c.From), int64(c.Through), c.PrevMaxTime, c.Data.NewIterator)
	}

	return NewGenericChunkMergeIterator(it, lbls, converted)
}

// NewGenericChunkMergeIterator returns a chunkenc.Iterator that merges generic chunks together.
func NewGenericChunkMergeIterator(it chunkenc.Iterator, lbls labels.Labels, chunks []GenericChunk) chunkenc.Iterator {
	var iter *mergeIterator

	adapter, ok := it.(*iteratorAdapter)
	if ok {
		iter = newMergeIterator(adapter.underlying, chunks)
	} else {
		iter = newMergeIterator(nil, chunks)
	}

	return newIteratorAdapter(adapter, iter, lbls)
}

// iteratorAdapter turns a batchIterator into a chunkenc.Iterator.
// It fetches ever increasing batchSizes (up to promchunk.BatchSize) on each
// call to Next; on calls to Seek, resets batch size to 1.
type iteratorAdapter struct {
	batchSize  int
	curr       chunk.Batch
	underlying iterator
	labels     labels.Labels
}

func newIteratorAdapter(it *iteratorAdapter, underlying iterator, lbls labels.Labels) chunkenc.Iterator {
	if it != nil {
		it.batchSize = 1
		it.underlying = underlying
		it.curr = chunk.Batch{}
		it.labels = lbls
		return it
	}
	return &iteratorAdapter{
		batchSize:  1,
		underlying: underlying,
		labels:     lbls,
	}
}

// Seek implements chunkenc.Iterator.
func (a *iteratorAdapter) Seek(t int64) chunkenc.ValueType {
	// Optimisation: fulfill the seek using current batch if possible.
	if a.curr.Length > 0 && a.curr.Index < a.curr.Length {
		if t <= a.curr.Timestamps[a.curr.Index] {
			//In this case, the interface's requirement is met, so state of this
			//iterator does not need any change.
			return a.curr.ValueType
		} else if t <= a.curr.Timestamps[a.curr.Length-1] {
			//In this case, some timestamp between current sample and end of batch can fulfill
			//the seek. Let's find it.
			for a.curr.Index < a.curr.Length && t > a.curr.Timestamps[a.curr.Index] {
				a.curr.Index++
			}
			return a.curr.ValueType
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
		return a.curr.ValueType
	}
	return chunkenc.ValNone
}

// At implements chunkenc.Iterator.
func (a *iteratorAdapter) At() (int64, float64) {
	if a.curr.ValueType != chunkenc.ValFloat {
		panic(fmt.Sprintf("Cannot read float from batch %v", a.curr.ValueType))
	}
	return a.curr.Timestamps[a.curr.Index], a.curr.Values[a.curr.Index]
}

// AtHistogram implements chunkenc.Iterator. It copies and returns the underlying histogram.
// If a pointer to a histogram is passed as parameter, the underlying histogram is copied there.
// Otherwise, a new histogram is created.
func (a *iteratorAdapter) AtHistogram(h *histogram.Histogram) (int64, *histogram.Histogram) {
	if a.curr.ValueType != chunkenc.ValHistogram {
		panic(fmt.Sprintf("Cannot read histogram from batch %v", a.curr.ValueType))
	}
	if h == nil {
		h = &histogram.Histogram{}
	}
	fromH := (*histogram.Histogram)(a.curr.PointerValues[a.curr.Index])
	fromH.CopyTo(h)
	return a.curr.Timestamps[a.curr.Index], h
}

// AtFloatHistogram implements chunkenc.Iterator. It copies and returns the underlying float histogram.
// If a pointer to a float histogram is passed as parameter, the underlying float histogram is copied there.
// Otherwise, a new float histogram is created.
func (a *iteratorAdapter) AtFloatHistogram(fh *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	if fh == nil {
		fh = &histogram.FloatHistogram{}
	}
	// The promQL engine works on Float Histograms even if the underlying data is an integer histogram
	// and will call AtFloatHistogram on a Histogram
	if a.curr.ValueType == chunkenc.ValFloatHistogram {
		fromFH := (*histogram.FloatHistogram)(a.curr.PointerValues[a.curr.Index])
		fromFH.CopyTo(fh)
		return a.curr.Timestamps[a.curr.Index], fh
	}
	if a.curr.ValueType == chunkenc.ValHistogram {
		fromH := (*histogram.Histogram)(a.curr.PointerValues[a.curr.Index])
		fromH.ToFloat(fh)
		return a.curr.Timestamps[a.curr.Index], fh
	}
	panic(fmt.Sprintf("Cannot read floathistogram from batch %v", a.curr.ValueType))
}

// AtT implements chunkenc.Iterator.
func (a *iteratorAdapter) AtT() int64 {
	return a.curr.Timestamps[a.curr.Index]
}

// Err implements chunkenc.Iterator.
func (a *iteratorAdapter) Err() error {
	if err := a.underlying.Err(); err != nil {
		return fmt.Errorf("error reading chunks for series %s: %w", a.labels, err)
	}

	return nil
}
