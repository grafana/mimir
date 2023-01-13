// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/stream.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
	"unsafe"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

// batchStream deals with iteratoring through multiple, non-overlapping batches,
// and building new slices of non-overlapping batches.  Designed to be used
// without allocations.
type batchStream []chunk.Batch

// reset, hasNext, next, atTime etc are all inlined in go1.11.

func (bs *batchStream) reset() {
	for i := range *bs {
		(*bs)[i].Index = 0
	}
}

func (bs *batchStream) hasNext() chunkenc.ValueType {
	if len(*bs) > 0 {
		return (*bs)[0].ValueType
	}
	return chunkenc.ValNone
}

func (bs *batchStream) next() {
	(*bs)[0].Index++
	if (*bs)[0].Index >= (*bs)[0].Length {
		*bs = (*bs)[1:]
	}
}

func (bs *batchStream) atTime() int64 {
	return (*bs)[0].Timestamps[(*bs)[0].Index]
}

func (bs *batchStream) at() (int64, float64) {
	b := &(*bs)[0]
	return b.Timestamps[b.Index], b.Values[b.Index]
}

func (bs *batchStream) atHistogram() (int64, *histogram.Histogram) {
	b := &(*bs)[0]
	return b.Timestamps[b.Index], (*histogram.Histogram)(b.PointerValues[b.Index])
}

func (bs *batchStream) atFloatHistogram() (int64, *histogram.FloatHistogram) {
	b := &(*bs)[0]
	return b.Timestamps[b.Index], (*histogram.FloatHistogram)(b.PointerValues[b.Index])
}

// mergeStreams merges streams of Batches of the same series over time.
// Samples are simply merged by time when they are the same type (float/histogram/...), with the left stream taking precedence if the timestamps are equal.
// When sample are different type, batches are not merged. In case of equal timestamps, histograms take precedence since they have more information.
func mergeStreams(left, right batchStream, result batchStream, size int) batchStream {

	// Reset the Index and Length of existing batches.
	for i := range result {
		result[i].ValueType = chunkenc.ValFloat // assume that most metrics are simple floats
		result[i].Index = 0
		result[i].Length = 0
	}

	resultLen := 1 // Number of batches in the final result.
	b := &result[0]

	// Step to the next Batch in the result, create it if it does not exist
	nextBatch := func(valueType chunkenc.ValueType) {
		// The Index is the place at which new sample
		// has to be appended, hence it tells the length.
		b.Length = b.Index
		resultLen++
		if resultLen > len(result) {
			// It is possible that result can grow longer
			// then the one provided.
			result = append(result, chunk.Batch{})
		}
		b = &result[resultLen-1]
	}

	// Ensure that the batch we're writing to is not full and of the right type.
	ensureBatch := func(valueType chunkenc.ValueType) {
		if b.Index == size {
			// The batch reached its intended size.
			// Add another batch the the result
			// and use it for further appending.
			nextBatch(valueType)
		}
		for b.ValueType != valueType {
			if b.ValueType == chunkenc.ValNone || b.Index == 0 {
				// Uninitialized or unstarted Batch
				b.ValueType = valueType
				return
			}
			// Batch already started, finish and start new
			// We don't know its type so let the loop run one more time
			nextBatch(valueType)
		}
	}

	for {
		if lt, rt := left.hasNext(), right.hasNext(); lt != chunkenc.ValNone && rt != chunkenc.ValNone {
			t1, t2 := left.atTime(), right.atTime()
			if t1 < t2 {
				ensureBatch(lt)
				populate(b, left, lt)
				left.next()
			} else if t1 > t2 {
				ensureBatch(rt)
				populate(b, right, rt)
				right.next()
			} else {
				if (rt == chunkenc.ValHistogram || rt == chunkenc.ValFloatHistogram) && lt == chunkenc.ValFloat {
					// Prefer historgrams over floats. Take left side if both has histograms.
					ensureBatch(rt)
					populate(b, right, rt)
				} else {
					ensureBatch(lt)
					populate(b, left, lt)
				}
				left.next()
				right.next()
			}
		} else {
			break
		}
	}

	// This function adds all the samples from the provided
	// batchStream into the result in the same order.
	addToResult := func(bs batchStream) {
		for {
			if t := bs.hasNext(); t != chunkenc.ValNone {
				ensureBatch(t)
				populate(b, bs, t)
				b.Length++
				bs.next()
			} else {
				break
			}
		}
	}

	addToResult(left)
	addToResult(right)

	// The Index is the place at which new sample
	// has to be appended, hence it tells the length.
	b.Length = b.Index

	// The provided 'result' slice might be bigger
	// than the actual result, hence return the subslice.
	result = result[:resultLen]
	result.reset()
	return result
}

func populate(b *chunk.Batch, s batchStream, valueTypes chunkenc.ValueType) {
	switch valueTypes {
	case chunkenc.ValFloat:
		b.Timestamps[b.Index], b.Values[b.Index] = s.at()
	case chunkenc.ValHistogram:
		t, v := s.atHistogram()
		b.Timestamps[b.Index], b.PointerValues[b.Index] = t, unsafe.Pointer(v)
	case chunkenc.ValFloatHistogram:
		t, v := s.atFloatHistogram()
		b.Timestamps[b.Index], b.PointerValues[b.Index] = t, unsafe.Pointer(v)
	}
	b.Index++
}
