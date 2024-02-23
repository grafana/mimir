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

func (bs *batchStream) swapAtHistogram(replacement *histogram.Histogram) (int64, *histogram.Histogram) {
	b := &(*bs)[0]
	t, h := b.Timestamps[b.Index], b.Histograms[b.Index]
	b.Timestamps[b.Index], b.Histograms[b.Index] = 0, replacement
	return t, h
}

func (bs *batchStream) atFloatHistogram() (int64, *histogram.FloatHistogram) {
	b := &(*bs)[0]
	return b.Timestamps[b.Index], b.FloatHistograms[b.Index]
}

func (bs *batchStream) swapAtFloatHistogram(replacement *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	b := &(*bs)[0]
	t, h := b.Timestamps[b.Index], b.FloatHistograms[b.Index]
	b.Timestamps[b.Index], b.FloatHistograms[b.Index] = 0, replacement
	return t, h
}

// mergeStreams merges streams of Batches of the same series over time.
// Samples are simply merged by time when they are the same type (float/histogram/...), with the left stream taking precedence if the timestamps are equal.
// When sample are different type, batches are not merged. In case of equal timestamps, histograms take precedence since they have more information.
// If copyPointerValuesLeft or copyPointerValuesRight are true, pointers of the copies of the histograms and float histograms from left or right batchStream
// will be stored in the result batchStream.
// hPool and fhPool are pools of pointers to histogram.Histogram and histogram.FloatHistogram that are used for creating copies.
// If they are nil, new instances of histogram.Histogram and histogram.FloatHistogram are used for creating copies.
func mergeStreams(left batchStream, right *chunk.Batch, result batchStream, size int, hPool *zeropool.Pool[*histogram.Histogram], fhPool *zeropool.Pool[*histogram.FloatHistogram]) batchStream {
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

	populateLeft := func(s batchStream, valueType chunkenc.ValueType) {
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
			b.Timestamps[b.Index], b.Histograms[b.Index] = s.atHistogram()
		case chunkenc.ValFloatHistogram:
			b.Timestamps[b.Index], b.FloatHistograms[b.Index] = s.atFloatHistogram()
		}
		b.Index++
	}

	populateRight := func(c *chunk.Batch, valueType chunkenc.ValueType) {
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
			b.Timestamps[b.Index], b.Values[b.Index] = c.Timestamps[c.Index], c.Values[c.Index]
		case chunkenc.ValHistogram:
			var replacement *histogram.Histogram
			if hPool == nil {
				replacement = &histogram.Histogram{}
			} else {
				replacement = hPool.Get()
			}
			b.Timestamps[b.Index], b.Histograms[b.Index] = c.Timestamps[c.Index], c.Histograms[c.Index]
			c.Timestamps[c.Index], c.Histograms[c.Index] = 0, replacement
		case chunkenc.ValFloatHistogram:
			var replacement *histogram.FloatHistogram
			if fhPool == nil {
				replacement = &histogram.FloatHistogram{}
			} else {
				replacement = fhPool.Get()
			}
			b.Timestamps[b.Index], b.FloatHistograms[b.Index] = c.Timestamps[c.Index], c.FloatHistograms[c.Index]
			c.Timestamps[c.Index], c.FloatHistograms[c.Index] = 0, replacement
		}
		b.Index++
	}

	for lt, rt := left.hasNext(), right.ValueType; lt != chunkenc.ValNone && rt != chunkenc.ValNone && right.Index < right.Length; lt, rt = left.hasNext(), right.ValueType {
		t1, t2 := left.atTime(), right.Timestamps[right.Index]
		if t1 < t2 {
			populateLeft(left, lt)
			left.next()
		} else if t1 > t2 {
			populateRight(right, rt)
			right.Index++
		} else {
			if (rt == chunkenc.ValHistogram || rt == chunkenc.ValFloatHistogram) && lt == chunkenc.ValFloat {
				// Prefer histograms to floats. Take left side if both have histograms.
				populateRight(right, rt)
			} else {
				populateLeft(left, lt)
			}
			left.next()
			right.Index++
		}
	}

	// Add the remaining samples from the left
	for t := left.hasNext(); t != chunkenc.ValNone; t = left.hasNext() {
		populateLeft(left, t)
		left.next()
	}

	// Add the remaining samples from the right
	for right.Index < right.Length {
		populateRight(right, right.ValueType)
		right.Index++
	}

	// The Index is the place at which new sample
	// has to be appended, hence it tells the length.
	b.Length = b.Index

	// The provided 'result' slice might be bigger
	// than the actual result, hence return the subslice.
	result = result[:resultLen]
	result.reset()
	return result
}
