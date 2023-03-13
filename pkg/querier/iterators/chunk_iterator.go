// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/iterators/chunk_iterator.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package iterators

import (
	"errors"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

type chunkIterator struct {
	chunk.Chunk
	it chunk.Iterator

	isExhausted bool

	// At() is called often in the heap code, so caching its result seems like
	// a good idea.
	cacheValid  bool
	cachedTime  int64
	cachedValue float64
}

// Seek advances the iterator forward to the value at or after
// the given timestamp.
func (i *chunkIterator) Seek(t int64) chunkenc.ValueType {
	i.cacheValid = false

	// We assume seeks only care about a specific window; if this chunk doesn't
	// contain samples in that window, we can shortcut.
	if int64(i.Through) < t || i.isExhausted {
		i.isExhausted = true
		return chunkenc.ValNone
	}

	return i.it.FindAtOrAfter(model.Time(t))
}

func (i *chunkIterator) AtTime() int64 {
	if i.cacheValid {
		return i.cachedTime
	}

	v := i.it.Value()
	i.cachedTime, i.cachedValue = int64(v.Timestamp), float64(v.Value)
	i.cacheValid = true
	return i.cachedTime
}

func (i *chunkIterator) At() (int64, float64) {
	if i.cacheValid {
		return i.cachedTime, i.cachedValue
	}

	v := i.it.Value()
	i.cachedTime, i.cachedValue = int64(v.Timestamp), float64(v.Value)
	i.cacheValid = true
	return i.cachedTime, i.cachedValue
}

func (i *chunkIterator) AtHistogram() (int64, *histogram.Histogram) {
	panic(errors.New("chunkIterator: AtHistogram not implemented"))
}

func (i *chunkIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	panic(errors.New("chunkIterator: AtFloatHistogram not implemented"))
}

func (i *chunkIterator) AtT() int64 {
	t, _ := i.At()
	return t
}

func (i *chunkIterator) Next() chunkenc.ValueType {
	if i.isExhausted {
		return chunkenc.ValNone
	}
	i.cacheValid = false
	return i.it.Scan()
}

func (i *chunkIterator) Err() error {
	return i.it.Err()
}
