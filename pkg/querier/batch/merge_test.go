// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/merge_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

func TestMergeIter(t *testing.T) {
	chunk1 := mkGenericChunk(t, 0, 100, chunk.PrometheusXorChunk)
	chunk2 := mkGenericChunk(t, model.TimeFromUnix(25), 100, chunk.PrometheusXorChunk)
	chunk3 := mkGenericChunk(t, model.TimeFromUnix(50), 100, chunk.PrometheusXorChunk)
	chunk4 := mkGenericChunk(t, model.TimeFromUnix(75), 100, chunk.PrometheusXorChunk)
	chunk5 := mkGenericChunk(t, model.TimeFromUnix(100), 100, chunk.PrometheusXorChunk)

	iter := newMergeIterator([]GenericChunk{chunk1, chunk2, chunk3, chunk4, chunk5})
	testIter(t, 200, newIteratorAdapter(iter))

	iter = newMergeIterator([]GenericChunk{chunk1, chunk2, chunk3, chunk4, chunk5})
	testSeek(t, 200, newIteratorAdapter(iter))
}

func TestMergeHarder(t *testing.T) {
	var (
		numChunks = 24 * 15
		chunks    = make([]GenericChunk, 0, numChunks)
		from      = model.Time(0)
		offset    = 30
		samples   = 100
	)
	for i := 0; i < numChunks; i++ {
		chunks = append(chunks, mkGenericChunk(t, from, samples, chunk.PrometheusXorChunk))
		from = from.Add(time.Duration(offset) * time.Second)
	}
	iter := newMergeIterator(chunks)
	testIter(t, offset*numChunks+samples-offset, newIteratorAdapter(iter))

	iter = newMergeIterator(chunks)
	testSeek(t, offset*numChunks+samples-offset, newIteratorAdapter(iter))
}
