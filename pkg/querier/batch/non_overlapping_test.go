// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/non_overlapping_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
	"testing"

	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

func TestNonOverlappingIter(t *testing.T) {
	cs := []GenericChunk(nil)
	for i := int64(0); i < 100; i++ {
		cs = append(cs, mkGenericChunk(t, model.TimeFromUnix(i*10), 10, chunk.PrometheusXorChunk))
	}
	testIter(t, 10*100, newIteratorAdapter(nil, newNonOverlappingIterator(nil, cs, nil, nil)), chunk.PrometheusXorChunk)
	it := newNonOverlappingIterator(nil, cs, nil, nil)
	adapter := newIteratorAdapter(nil, it)
	testSeek(t, 10*100, adapter, chunk.PrometheusXorChunk)

	// Do the same operations while re-using the iterators.
	it = newNonOverlappingIterator(it, cs, nil, nil)
	adapter = newIteratorAdapter(adapter.(*iteratorAdapter), it)
	testIter(t, 10*100, adapter, chunk.PrometheusXorChunk)
	it = newNonOverlappingIterator(it, cs, nil, nil)
	adapter = newIteratorAdapter(adapter.(*iteratorAdapter), it)
	testSeek(t, 10*100, adapter, chunk.PrometheusXorChunk)
}

func TestNonOverlappingIterSparse(t *testing.T) {
	cs := []GenericChunk{
		mkGenericChunk(t, model.TimeFromUnix(0), 1, chunk.PrometheusXorChunk),
		mkGenericChunk(t, model.TimeFromUnix(1), 3, chunk.PrometheusXorChunk),
		mkGenericChunk(t, model.TimeFromUnix(4), 1, chunk.PrometheusXorChunk),
		mkGenericChunk(t, model.TimeFromUnix(5), 90, chunk.PrometheusXorChunk),
		mkGenericChunk(t, model.TimeFromUnix(95), 1, chunk.PrometheusXorChunk),
		mkGenericChunk(t, model.TimeFromUnix(96), 4, chunk.PrometheusXorChunk),
	}
	testIter(t, 100, newIteratorAdapter(nil, newNonOverlappingIterator(nil, cs, nil, nil)), chunk.PrometheusXorChunk)
	testSeek(t, 100, newIteratorAdapter(nil, newNonOverlappingIterator(nil, cs, nil, nil)), chunk.PrometheusXorChunk)
}
