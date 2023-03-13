// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/iterators/chunk_iterator.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package iterators

import (
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

type chunkIterator struct {
	chunk.Chunk
	it chunk.Iterator

	valType chunkenc.ValueType

	// AtT() is called often in the heap code, so caching its result seems like
	// a good idea.
	cacheValid           bool
	cachedTime           int64
	cachedValue          float64
	cachedHistogram      *histogram.Histogram
	cachedFloatHistogram *histogram.FloatHistogram
}

// Seek advances the iterator forward to the value at or after
// the given timestamp.
func (i *chunkIterator) Seek(t int64) chunkenc.ValueType {
	i.cacheValid = false

	// We assume seeks only care about a specific window; if this chunk doesn't
	// contain samples in that window, we can shortcut.
	if int64(i.Through) < t {
		i.valType = chunkenc.ValNone
		return chunkenc.ValNone
	}

	i.valType = i.it.FindAtOrAfter(model.Time(t))
	return i.valType
}

func (i *chunkIterator) At() (int64, float64) {
	if i.valType != chunkenc.ValFloat {
		panic(fmt.Errorf("chunkIterator: calling At when chunk is of different type %v", i.valType))
	}

	if i.cacheValid {
		return i.cachedTime, i.cachedValue
	}

	v := i.it.Value()
	i.cachedTime, i.cachedValue = int64(v.Timestamp), float64(v.Value)
	i.cacheValid = true
	return i.cachedTime, i.cachedValue
}

func (i *chunkIterator) AtHistogram() (int64, *histogram.Histogram) {
	if i.valType != chunkenc.ValHistogram {
		panic(fmt.Errorf("chunkIterator: calling AtHistogram when chunk is of different type %v", i.valType))
	}

	if i.cacheValid {
		return i.cachedTime, i.cachedHistogram
	}

	i.cachedTime, i.cachedHistogram = i.it.AtHistogram()
	i.cacheValid = true
	return i.cachedTime, i.cachedHistogram
}

func (i *chunkIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	if i.valType == chunkenc.ValHistogram {
		// use cache as histogram
		t, h := i.AtHistogram()
		return t, h.ToFloat()
	}
	if i.valType != chunkenc.ValFloatHistogram {
		panic(fmt.Errorf("chunkIterator: calling AtFloatHistogram when chunk is of different type %v", i.valType))
	}

	if i.cacheValid {
		return i.cachedTime, i.cachedFloatHistogram
	}

	i.cachedTime, i.cachedFloatHistogram = i.it.AtFloatHistogram()
	i.cacheValid = true
	return i.cachedTime, i.cachedFloatHistogram
}

func (i *chunkIterator) AtT() int64 {
	if i.cacheValid {
		return i.cachedTime
	}
	switch i.valType {
	case chunkenc.ValFloat:
		t, v := i.At()
		i.cacheValid = true
		i.cachedValue = v
		return t
	case chunkenc.ValHistogram:
		t, h := i.AtHistogram()
		i.cacheValid = true
		i.cachedHistogram = h
		return t
	case chunkenc.ValFloatHistogram:
		t, fh := i.AtFloatHistogram()
		i.cacheValid = true
		i.cachedFloatHistogram = fh
		return t
	default:
		panic(fmt.Errorf("chunkIterator: calling AtT with unknown chunk encoding %v", i.valType))
	}
}

func (i *chunkIterator) AtType() chunkenc.ValueType {
	return i.valType
}

func (i *chunkIterator) Next() chunkenc.ValueType {
	i.cacheValid = false
	i.valType = i.it.Scan()
	return i.valType
}

func (i *chunkIterator) Err() error {
	return i.it.Err()
}
