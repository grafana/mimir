// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/non_overlapping.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
	"github.com/grafana/mimir/pkg/storage/chunk"
)

type nonOverlappingIterator struct {
	curr   int
	chunks []GenericChunk
	iter   chunkIterator
}

// newNonOverlappingIterator returns a single iterator over an slice of sorted,
// non-overlapping iterators.
func newNonOverlappingIterator(chunks []GenericChunk) *nonOverlappingIterator {
	it := &nonOverlappingIterator{
		chunks: chunks,
	}
	it.iter.reset(it.chunks[0])
	return it
}

func (it *nonOverlappingIterator) Seek(t int64, size int) bool {
	for {
		if it.iter.Seek(t, size) {
			return true
		} else if it.iter.Err() != nil {
			return false
		} else if !it.next() {
			return false
		}
	}
}

func (it *nonOverlappingIterator) Next(size int) bool {
	for {
		if it.iter.Next(size) {
			return true
		} else if it.iter.Err() != nil {
			return false
		} else if !it.next() {
			return false
		}
	}
}

func (it *nonOverlappingIterator) next() bool {
	it.curr++
	if it.curr < len(it.chunks) {
		it.iter.reset(it.chunks[it.curr])
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
