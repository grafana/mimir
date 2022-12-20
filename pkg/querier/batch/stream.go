// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/stream.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
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
		return (*bs)[0].ValueTypes
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
	return b.Timestamps[b.Index], b.SampleValues[b.Index]
}

func (bs *batchStream) atHistogram() (int64, *histogram.Histogram) {
	b := &(*bs)[0]
	return b.Timestamps[b.Index], b.HistogramValues[b.Index]
}

func (bs *batchStream) atFloatHistogram() (int64, *histogram.FloatHistogram) {
	b := &(*bs)[0]
	return b.Timestamps[b.Index], b.FloatHistogramValues[b.Index]
}

// mergeStreams merges streams of Batches of the same series over time.
// Samples are simply merged by time when they are the same type (float/histogram/...), with the left stream taking precedence if the timestamps are equal.
// When sample are different type, batches are not merged. In case of equal timestamps, histograms take precedence since they have more information.
func mergeStreams(left, right batchStream, result batchStream, size int) batchStream {

	// Reset the Index and Length of existing batches.
	for i := range result {
		result[i].Index = 0
		result[i].Length = 0
	}

	resultLen := 1 // Number of batches in the final result.
	b := &result[0]

	// Step to the next Batch in the result, create it if it does not exist
	nextBatch := func(valueTypes chunkenc.ValueType) {
		// The Index is the place at which new sample
		// has to be appended, hence it tells the length.
		b.Length = b.Index
		resultLen++
		if resultLen > len(result) {
			// It is possible that result can grow longer
			// then the one provided.
			result = append(result, createBatch(valueTypes))
		}
		b = &result[resultLen-1]
	}

	ensureBatchType := func(valueTypes chunkenc.ValueType) {
		for b.ValueTypes != valueTypes {
			if b.ValueTypes == chunkenc.ValNone {
				// Uninitialized Batch
				initBatchType(b, valueTypes)
				return
			}
			// These should be rare, means that we're merging into something where the chunk type changed
			if b.Index == 0 {
				// Batch not written yet
				result[resultLen-1] = createBatch(valueTypes)
				b = &result[resultLen-1]
				return
			}
			// Batch already started, finish and start new
			// We don't know its type so let the loop run one more time
			nextBatch(valueTypes)
		}
	}

	// Ensure that the batch we're writing to is not full and of the right type.
	ensureBatch := func(valueTypes chunkenc.ValueType) {
		if b.Index == size {
			// The batch reached its intended size.
			// Add another batch the the result
			// and use it for further appending.
			nextBatch(valueTypes)
		}
		ensureBatchType(valueTypes)
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
			b.Index++
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
				b.Index++
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

func createBatch(valueTypes chunkenc.ValueType) (batch chunk.Batch) {
	switch valueTypes {
	case chunkenc.ValFloat:
		batch.ValueTypes = chunkenc.ValFloat
		batch.SampleValues = &[chunk.BatchSize]float64{}
	case chunkenc.ValHistogram:
		batch.ValueTypes = chunkenc.ValHistogram
		batch.HistogramValues = &[chunk.BatchSize]*histogram.Histogram{}
	case chunkenc.ValFloatHistogram:
		batch.ValueTypes = chunkenc.ValFloatHistogram
		batch.FloatHistogramValues = &[chunk.BatchSize]*histogram.FloatHistogram{}
	}
	return
}

// Call on uninitialized Batch to allocate the correct valueType array
func initBatchType(batch *chunk.Batch, valueTypes chunkenc.ValueType) {
	batch.ValueTypes = valueTypes
	switch valueTypes {
	case chunkenc.ValFloat:
		batch.SampleValues = &[chunk.BatchSize]float64{}
	case chunkenc.ValHistogram:
		batch.HistogramValues = &[chunk.BatchSize]*histogram.Histogram{}
	case chunkenc.ValFloatHistogram:
		batch.FloatHistogramValues = &[chunk.BatchSize]*histogram.FloatHistogram{}
	}
}

func populate(b *chunk.Batch, s batchStream, valueTypes chunkenc.ValueType) {
	switch valueTypes {
	case chunkenc.ValFloat:
		b.Timestamps[b.Index], b.SampleValues[b.Index] = s.at()
	case chunkenc.ValHistogram:
		b.Timestamps[b.Index], b.HistogramValues[b.Index] = s.atHistogram()
	case chunkenc.ValFloatHistogram:
		b.Timestamps[b.Index], b.FloatHistogramValues[b.Index] = s.atFloatHistogram()
	}
}
