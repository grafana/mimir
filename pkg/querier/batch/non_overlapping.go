// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/non_overlapping.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/zeropool"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

type nonOverlappingIterator struct {
	curr    int
	chunks  []chunk.Chunk
	iter    chunkIterator
	options IteratorOptions
	// id is used to detect when the iterator has changed when merging
	id int
}

// newNonOverlappingIterator returns a single iterator over a slice of sorted,
// non-overlapping iterators.
func newNonOverlappingIterator(it *nonOverlappingIterator, id int, chunks []chunk.Chunk, hPool *zeropool.Pool[*histogram.Histogram], fhPool *zeropool.Pool[*histogram.FloatHistogram], stPool *zeropool.Pool[*[chunk.BatchSize]int64], options IteratorOptions) *nonOverlappingIterator {
	if it == nil {
		it = &nonOverlappingIterator{}
	} else {
		// Release through the old pool before replacing the configuration.
		it.iter.invalidateBatch()
	}
	it.id = id
	it.chunks = chunks
	it.curr = 0
	it.options = options
	it.iter.hPool = hPool
	it.iter.fhPool = fhPool
	it.iter.stPool = stPool
	it.iter.reset(it.chunks[0], options)
	return it
}

func (it *nonOverlappingIterator) Seek(t int64, size int) chunkenc.ValueType {
	for {
		if typ := it.iter.Seek(t, size); typ != chunkenc.ValNone {
			return typ
		} else if it.iter.Err() != nil {
			return chunkenc.ValNone
		} else if !it.next() {
			return chunkenc.ValNone
		}
	}
}

func (it *nonOverlappingIterator) Next(size int) chunkenc.ValueType {
	for {
		if typ := it.iter.Next(size); typ != chunkenc.ValNone {
			return typ
		} else if it.iter.Err() != nil {
			return chunkenc.ValNone
		} else if !it.next() {
			return chunkenc.ValNone
		}
	}
}

func (it *nonOverlappingIterator) next() bool {
	it.curr++
	if it.curr < len(it.chunks) {
		it.iter.reset(it.chunks[it.curr], it.options)
	}
	return it.curr < len(it.chunks)
}

func (it *nonOverlappingIterator) AtTime() int64 {
	return it.iter.AtTime()
}

func (it *nonOverlappingIterator) Batch() chunk.Batch {
	return it.iter.Batch()
}

func (it *nonOverlappingIterator) Err() error {
	if it.curr < len(it.chunks) {
		return it.iter.Err()
	}
	return nil
}
