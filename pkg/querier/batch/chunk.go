// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/chunk.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/zeropool"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

// chunkIterator implement batchIterator over a chunk.  Its is designed to be
// reused by calling reset() with a fresh chunk.
type chunkIterator struct {
	chunk GenericChunk
	it    chunk.Iterator
	batch chunk.Batch
	lastT int64

	hPool  *zeropool.Pool[*histogram.Histogram]
	fhPool *zeropool.Pool[*histogram.FloatHistogram]
}

func (i *chunkIterator) reset(chunk GenericChunk) {
	i.chunk = chunk
	i.it = chunk.Iterator(i.it)
	i.batch.Length = 0
	i.batch.Index = 0
}

func (i *chunkIterator) Seek(t int64, size int) chunkenc.ValueType {
	// We assume seeks only care about a specific window; if this chunk doesn't
	// contain samples in that window, we can shortcut.
	if i.chunk.MaxTime < t {
		return chunkenc.ValNone
	}

	// If the seek is to the middle of the current batch, and size fits, we can
	// shortcut.
	if i.batch.Length > 0 && t <= i.batch.Timestamps[i.batch.Length-1] {
		i.batch.Index = 0
		for i.batch.Index < i.batch.Length && t > i.batch.Timestamps[i.batch.Index] {
			i.batch.Index++
		}
		if i.batch.Index+size < i.batch.Length {
			// Set the previous timestamp to 0 (unknown) to be consistent with the
			// non-fast path Seek below.
			i.batch.SetPrevT(0)
			return i.batch.ValueType
		}
	}
	if typ := i.it.FindAtOrAfter(model.Time(t)); typ != chunkenc.ValNone {
		if typ == chunkenc.ValFloat {
			i.batch = i.it.BatchFloats(size)
		} else {
			// We set the previous timestamp to 0 (unknown) because FindAtOrAfter,
			// Seek does not return the information. However it doesn't matter for
			// the first sample anyway.
			i.batch = i.it.Batch(size, typ, 0, i.hPool, i.fhPool)
		}
		if i.batch.Length > 0 {
			return typ
		}
	}
	return chunkenc.ValNone
}

func (i *chunkIterator) Next(size int) chunkenc.ValueType {
	if typ := i.it.Scan(); typ != chunkenc.ValNone {
		if typ == chunkenc.ValFloat {
			i.batch = i.it.BatchFloats(size)
		} else {
			if i.batch.Length == 0 {
				i.batch = i.it.Batch(size, typ, i.chunk.PrevMaxTime, i.hPool, i.fhPool)
			} else {
				i.batch = i.it.Batch(size, typ, i.lastT, i.hPool, i.fhPool)
			}
			i.lastT = i.batch.Timestamps[i.batch.Length-1]
		}
		if i.batch.Length > 0 {
			return typ
		}
	}
	return chunkenc.ValNone
}

func (i *chunkIterator) AtTime() int64 {
	return i.batch.Timestamps[0]
}

func (i *chunkIterator) Batch() chunk.Batch {
	return i.batch
}

func (i *chunkIterator) Err() error {
	return i.it.Err()
}
