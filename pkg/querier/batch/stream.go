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
	return b.Timestamps[b.Index], b.Histograms[b.Index]
}

func (bs *batchStream) atFloatHistogram() (int64, *histogram.FloatHistogram) {
	b := &(*bs)[0]
	return b.Timestamps[b.Index], b.FloatHistograms[b.Index]
}

// mergeStreams merges streams of Batches of the same series over time.
// Samples are simply merged by time when they are the same type (float/histogram/...), with the left stream taking precedence if the timestamps are equal.
// When sample are different type, batches are not merged. In case of equal timestamps, histograms take precedence since they have more information.
// If copyPointerValuesLeft or copyPointerValuesRight are true, pointers of the copies of the histograms and float histograms from left or right batchStream
// will be stored in the result batchStream.
// hPool and fhPool are pools of pointers to histogram.Histogram and histogram.FloatHistogram that are used for creating copies.
// If they are nil, new instances of histogram.Histogram and histogram.FloatHistogram are used for creating copies.
func mergeStreams(left, right, result batchStream, size int, copyPointerValuesLeft, copyPointerValuesRight bool, hPool *zeropool.Pool[*histogram.Histogram], fhPool *zeropool.Pool[*histogram.FloatHistogram]) batchStream {

	// Reset the Index and Length of existing batches.
	for i := range result {
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
		b.ValueType = valueType
	}

	populate := func(s batchStream, valueType chunkenc.ValueType, copyPointerValues bool) {
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
			b.Timestamps[b.Index], b.Values[b.Index] = s.at()
		case chunkenc.ValHistogram:
			t, fromH := s.atHistogram()
			if copyPointerValues {
				if fromH != nil {
					var toH *histogram.Histogram
					if hPool == nil {
						toH = &histogram.Histogram{}
					} else {
						toH = hPool.Get()
					}
					fromH.CopyTo(toH)
					b.Timestamps[b.Index], b.Histograms[b.Index] = t, toH
				}
			} else {
				b.Timestamps[b.Index], b.Histograms[b.Index] = t, fromH
			}
		case chunkenc.ValFloatHistogram:
			t, fromFH := s.atFloatHistogram()
			if copyPointerValues {
				if fromFH != nil {
					var toFH *histogram.FloatHistogram
					if fhPool == nil {
						toFH = &histogram.FloatHistogram{}
					} else {
						toFH = fhPool.Get()
					}
					fromFH.CopyTo(toFH)
					b.Timestamps[b.Index], b.FloatHistograms[b.Index] = t, toFH
				}
			} else {
				b.Timestamps[b.Index], b.FloatHistograms[b.Index] = t, fromFH
			}
		}
		b.Index++
	}

	for lt, rt := left.hasNext(), right.hasNext(); lt != chunkenc.ValNone && rt != chunkenc.ValNone; lt, rt = left.hasNext(), right.hasNext() {
		t1, t2 := left.atTime(), right.atTime()
		if t1 < t2 {
			populate(left, lt, copyPointerValuesLeft)
			left.next()
		} else if t1 > t2 {
			populate(right, rt, copyPointerValuesRight)
			right.next()
		} else {
			if (rt == chunkenc.ValHistogram || rt == chunkenc.ValFloatHistogram) && lt == chunkenc.ValFloat {
				// Prefer histograms to floats. Take left side if both have histograms.
				populate(right, rt, copyPointerValuesRight)
			} else {
				populate(left, lt, copyPointerValuesLeft)
			}
			left.next()
			right.next()
		}
	}

	// This function adds all the samples from the provided
	// batchStream into the result in the same order.
	addToResult := func(bs batchStream, reusePointerValues bool) {
		for t := bs.hasNext(); t != chunkenc.ValNone; t = bs.hasNext() {
			populate(bs, t, reusePointerValues)
			bs.next()
		}
	}

	addToResult(left, copyPointerValuesLeft)
	addToResult(right, copyPointerValuesRight)

	// The Index is the place at which new sample
	// has to be appended, hence it tells the length.
	b.Length = b.Index

	// The provided 'result' slice might be bigger
	// than the actual result, hence return the subslice.
	result = result[:resultLen]
	result.reset()
	return result
}
