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
	chunk chunk.Chunk
	it    chunk.Iterator
	batch chunk.Batch

	hPool  *zeropool.Pool[*histogram.Histogram]
	fhPool *zeropool.Pool[*histogram.FloatHistogram]

	// stPool is the pool from which per-batch start timestamp sidecars are
	// acquired. It is nil when start timestamp collection is disabled.
	stPool *zeropool.Pool[*[chunk.BatchSize]int64]
	// collectStartTimestamps is set only when both start timestamps are enabled
	// and the current chunk's encoding can contain non-zero start timestamps.
	collectStartTimestamps bool
}

func (i *chunkIterator) reset(c chunk.Chunk, options IteratorOptions) {
	i.invalidateBatch()
	i.chunk = c
	i.it = c.Data.NewIterator(i.it)
	i.collectStartTimestamps = options.CollectStartTimestamps && encodingSupportsStartTimestamps(c.Data.Encoding())
}

// encodingSupportsStartTimestamps reports whether a chunk encoding can produce
// non-zero start timestamps via its iterator's AtST method. Encodings that
// cannot are skipped by the decoder to avoid an unnecessary per-sample call.
func encodingSupportsStartTimestamps(encoding chunk.Encoding) bool {
	//TODO(carrieedwards): Add histogramST and floatHistogramST encoding types
	switch encoding {
	case chunk.PrometheusXor2Chunk:
		return true
	default:
		return false
	}
}

// startTimestampPoolForBatch returns nil when collection is disabled for the
// current chunk, without changing the owning pool used to release sidecars.
func (i *chunkIterator) startTimestampPoolForBatch() *zeropool.Pool[*[chunk.BatchSize]int64] {
	if !i.collectStartTimestamps {
		return nil
	}
	return i.stPool
}

func (i *chunkIterator) Seek(t int64, size int) chunkenc.ValueType {
	// We assume seeks only care about a specific window; if this chunk doesn't
	// contain samples in that window, we can shortcut.
	if int64(i.chunk.Through) < t {
		i.invalidateBatch()
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
			return i.batch.ValueType
		}
	}

	// Release the outgoing sidecar before the decoder acquires a new one. Every
	// other field of i.batch is replaced wholesale by the Batch call below, so
	// there is nothing else to invalidate on this path.
	chunk.ReleaseStartTimestampSidecar(&i.batch, i.stPool)
	if typ := i.it.FindAtOrAfter(model.Time(t)); typ != chunkenc.ValNone {
		i.batch = i.it.Batch(size, typ, i.hPool, i.fhPool, i.startTimestampPoolForBatch())
		if i.batch.Length > 0 {
			return typ
		}
	}
	i.invalidateBatch()
	return chunkenc.ValNone
}

func (i *chunkIterator) Next(size int) chunkenc.ValueType {
	// Release the outgoing sidecar before the decoder acquires a new one. Every
	// other field of i.batch is replaced wholesale by the Batch call below, so
	// there is nothing else to invalidate on this path.
	chunk.ReleaseStartTimestampSidecar(&i.batch, i.stPool)
	if typ := i.it.Scan(); typ != chunkenc.ValNone {
		i.batch = i.it.Batch(size, typ, i.hPool, i.fhPool, i.startTimestampPoolForBatch())
		if i.batch.Length > 0 {
			return typ
		}
	}
	i.invalidateBatch()
	return chunkenc.ValNone
}

// invalidateBatch marks the cached batch as holding no samples and returns its
// start timestamp sidecar to the pool. Every path that reports chunkenc.ValNone
// leaves the batch in this state, as does reset. Callers must finish reading
// borrowed Batch copies first. Histogram pointers are not returned here:
// merging manages their ownership.
func (i *chunkIterator) invalidateBatch() {
	chunk.ReleaseStartTimestampSidecar(&i.batch, i.stPool)
	i.batch.Length = 0
	i.batch.Index = 0
	i.batch.ValueType = chunkenc.ValNone
}

func (i *chunkIterator) AtTime() int64 {
	return i.batch.Timestamps[0]
}

// Batch returns a copy that borrows the iterator's start timestamp sidecar.
// The caller must finish reading it before calling Seek, Next, or reset.
func (i *chunkIterator) Batch() chunk.Batch {
	return i.batch
}

func (i *chunkIterator) Err() error {
	return i.it.Err()
}
