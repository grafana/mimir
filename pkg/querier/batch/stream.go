// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/stream.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/zeropool"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

// batchStream deals with iterating through multiple, non-overlapping batches,
// and building new slices of non-overlapping batches.  Designed to be used
// without allocations.
type batchStream struct {
	batches    []chunk.Batch
	batchesBuf []chunk.Batch

	prevIteratorID int

	hPool  *zeropool.Pool[*histogram.Histogram]
	fhPool *zeropool.Pool[*histogram.FloatHistogram]
}

func newBatchStream(size int, hPool *zeropool.Pool[*histogram.Histogram], fhPool *zeropool.Pool[*histogram.FloatHistogram]) *batchStream {
	batches := make([]chunk.Batch, 0, size)
	batchesBuf := make([]chunk.Batch, size)
	return &batchStream{
		batches:        batches,
		batchesBuf:     batchesBuf,
		prevIteratorID: -1,
		hPool:          hPool,
		fhPool:         fhPool,
	}
}

func (bs *batchStream) putPointerValuesToThePool(batch *chunk.Batch) {
	if batch.ValueType == chunkenc.ValHistogram && bs.hPool != nil {
		for i := 0; i < batch.Length; i++ {
			bs.hPool.Put((*histogram.Histogram)(batch.PointerValues[i]))
		}
	} else if batch.ValueType == chunkenc.ValFloatHistogram && bs.fhPool != nil {
		for i := 0; i < batch.Length; i++ {
			bs.fhPool.Put((*histogram.FloatHistogram)(batch.PointerValues[i]))
		}
	}
}

func (bs *batchStream) removeFirst() {
	bs.putPointerValuesToThePool(bs.curr())
	copy(bs.batches, bs.batches[1:])
	bs.batches = bs.batches[:len(bs.batches)-1]
}

func (bs *batchStream) empty() {
	for i := range bs.batches {
		bs.putPointerValuesToThePool(&bs.batches[i])
	}
	bs.batches = bs.batches[:0]
	bs.prevIteratorID = -1
}

func (bs *batchStream) len() int {
	return len(bs.batches)
}

func (bs *batchStream) reset() {
	for i := range bs.batches {
		bs.batches[i].Index = 0
	}
}

func (bs *batchStream) hasNext() chunkenc.ValueType {
	if bs.len() > 0 {
		return bs.curr().ValueType
	}
	return chunkenc.ValNone
}

func (bs *batchStream) next() {
	b := bs.curr()
	b.Index++
	if b.Index >= b.Length {
		bs.batches = bs.batches[1:]
	}
}

func (bs *batchStream) curr() *chunk.Batch {
	return &bs.batches[0]
}

// merge merges this streams of chunk.Batch objects and the given chunk.Batch of the same series over time.
// Samples are simply merged by time when they are the same type (float/histogram/...), with the left stream taking precedence if the timestamps are equal.
// When sample are different type, batches are not merged. In case of equal timestamps, histograms take precedence since they have more information.
func (bs *batchStream) merge(batch *chunk.Batch, size int, iteratorID int) {
	// We store this at the beginning to avoid additional allocations.
	// Namely, the merge method will go through all the batches from bs.batch,
	// check whether their elements should be kept (and copy them to the result)
	// or discarded (and put them in the pool in order to reuse them), and then
	// remove the batches from bs.batch.
	// Eventually, at the end of the merge method, the resulting merged batches
	// will be appended to the previously emptied bs.batches. At that point
	// the cap(bs.batches) will be 0, so in order to save some allocations,
	// we will use origBatches, i.e., bs.bathces' capacity from the beginning of
	// the merge method.
	origBatches := bs.batches[:0]

	// Reset the Index and Length of existing batches.
	for i := range bs.batchesBuf {
		bs.batchesBuf[i].Index = 0
		bs.batchesBuf[i].Length = 0
	}

	resultLen := 1 // Number of batches in the final result.
	b := &bs.batchesBuf[0]

	// Step to the next Batch in the result, create it if it does not exist
	nextBatch := func(valueType chunkenc.ValueType) {
		// The Index is the place at which new sample
		// has to be appended, hence it tells the length.
		b.Length = b.Index
		resultLen++
		if resultLen > len(bs.batchesBuf) {
			// It is possible that result can grow longer
			// then the one provided.
			bs.batchesBuf = append(bs.batchesBuf, chunk.Batch{})
		}
		b = &bs.batchesBuf[resultLen-1]
		b.ValueType = valueType
	}

	prevIteratorID := bs.prevIteratorID

	populate := func(batch *chunk.Batch, valueType chunkenc.ValueType, itID int) {
		if b.Index == 0 {
			// Starting to write this Batch, it is safe to set the value type
			b.ValueType = valueType
		} else if b.Index == size || b.ValueType != valueType {
			// The batch reached its intended size or is of a different value type
			// Add another batch to the result and use it for further appending.
			nextBatch(valueType)
		}

		switch valueType {
		case chunkenc.ValFloat:
			b.Timestamps[b.Index], b.Values[b.Index] = batch.At()
		case chunkenc.ValHistogram:
			b.Timestamps[b.Index], b.PointerValues[b.Index] = batch.AtHistogram()
			if itID == -1 {
				itID = batch.GetIteratorID()
			}
			if prevIteratorID != itID && prevIteratorID != -1 {
				// We switched non overlapping iterators, so if the next sample coming
				// from a different place or time and we should reset the hint.
				h := (*histogram.Histogram)(b.PointerValues[b.Index])
				if h.CounterResetHint != histogram.GaugeType && h.CounterResetHint != histogram.UnknownCounterReset {
					h.CounterResetHint = histogram.UnknownCounterReset
				}
			}
			b.SetIteratorId(itID)
		case chunkenc.ValFloatHistogram:
			b.Timestamps[b.Index], b.PointerValues[b.Index] = batch.AtFloatHistogram()
			if itID == -1 {
				itID = batch.GetIteratorID()
			}
			if prevIteratorID != itID && prevIteratorID != -1 {
				// We switched non overlapping iterators, so if the next sample coming
				// from a different place or time and we should reset the hint.
				h := (*histogram.FloatHistogram)(b.PointerValues[b.Index])
				if h.CounterResetHint != histogram.GaugeType && h.CounterResetHint != histogram.UnknownCounterReset {
					h.CounterResetHint = histogram.UnknownCounterReset
				}
			}
			b.SetIteratorId(itID)
		}
		prevIteratorID = itID
		b.Index++
	}

	for lt, rt := bs.hasNext(), batch.HasNext(); lt != chunkenc.ValNone && rt != chunkenc.ValNone; lt, rt = bs.hasNext(), batch.HasNext() {
		t1, t2 := bs.curr().AtTime(), batch.AtTime()
		if t1 < t2 {
			populate(bs.curr(), lt, -1)
			bs.next()
		} else if t1 > t2 {
			populate(batch, rt, iteratorID)
			batch.Next()
		} else {
			if (rt == chunkenc.ValHistogram || rt == chunkenc.ValFloatHistogram) && lt == chunkenc.ValFloat {
				// Prefer histograms than floats. Take left side if both have histograms.
				populate(batch, rt, iteratorID)
			} else {
				populate(bs.curr(), lt, -1)
				// if bs.hPool is not nil, we put there the discarded histogram.Histogram object from batch, so it can be reused.
				if rt == chunkenc.ValHistogram && bs.hPool != nil {
					_, h := batch.AtHistogram()
					bs.hPool.Put((*histogram.Histogram)(h))
				}
				// if bs.fhPool is not nil, we put there the discarded histogram.FloatHistogram object from batch, so it can be reused.
				if rt == chunkenc.ValFloatHistogram && bs.fhPool != nil {
					_, fh := batch.AtFloatHistogram()
					bs.fhPool.Put((*histogram.FloatHistogram)(fh))
				}
			}
			bs.next()
			batch.Next()
		}
	}

	for t := bs.hasNext(); t != chunkenc.ValNone; t = bs.hasNext() {
		populate(bs.curr(), t, -1)
		bs.next()
	}

	for t := batch.HasNext(); t != chunkenc.ValNone; t = batch.HasNext() {
		populate(batch, t, iteratorID)
		batch.Next()
	}

	// The Index is the place at which new sample
	// has to be appended, hence it tells the length.
	b.Length = b.Index

	// Store the last iterator id.
	bs.prevIteratorID = prevIteratorID

	bs.batches = append(origBatches, bs.batchesBuf[:resultLen]...)
	bs.reset()
}
